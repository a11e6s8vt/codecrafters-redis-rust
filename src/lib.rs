mod cli;
mod cmds;
mod connection;
mod database;
mod global;
mod parse;
mod resp;

use core::str;
use std::{
    any::Any, collections::HashMap, future::Future, io, net::SocketAddr, ops::Deref, pin::Pin,
    process::Output, sync::Arc,
};

use bytes::BytesMut;
pub use cli::Cli;
use cmds::{Command, CommandError, InfoSubCommand, SubCommand};
use connection::Connection;
use database::Shared;
pub use database::{load_from_rdb, KeyValueStore};
pub use global::STATE;

use parse::parse_command;
use rand::{distributions::Alphanumeric, Rng};
use resp::{parse_handshake_response, RespData, RespError, Tokenizer};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

const CHUNK_SIZE: usize = 16 * 1024;
const CRLF: &str = "\r\n";
trait RedisInstance: Any + Send + Sync {
    fn run(&self) -> Pin<Box<dyn Future<Output = ()> + '_>>;
}

pub struct Follower {
    pub bind_address: String,
    pub listening_port: u16,
    pub leader_addr: String,
}

impl Follower {
    pub fn new(
        bind_address: Option<String>,
        listening_port: Option<u16>,
        leader_addr: Option<String>,
    ) -> Self {
        let bind_address = if bind_address.is_some() {
            bind_address.unwrap()
        } else {
            "127.0.0.1".to_string()
        };

        let listening_port = if listening_port.is_some() {
            listening_port.unwrap()
        } else {
            panic!("Port cannot be empty!");
        };

        let leader_addr = if leader_addr.is_some() {
            leader_addr.unwrap()
        } else {
            panic!("Leader address (--replicaof) cannot be empty");
        };

        Self {
            bind_address,
            listening_port,
            leader_addr,
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get(String),
    Set(String, String),
    Delete(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Value(Option<String>),
    Acknowledged,
    Error(String),
}

impl<'a> RedisInstance for Follower {
    fn run(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async {
            // initialise the DB
            //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
            let kv_store: KeyValueStore<String, String> = KeyValueStore::new();
            let state = Arc::new(Mutex::new(Shared::new()));

            // Create TCP Listener
            let listener_addr = format!("{}:{}", self.bind_address, self.listening_port);
            let listener = TcpListener::bind(listener_addr.to_owned())
                .await
                .expect("Binding to listener address failed!");
            log::info!("Follower running on {}...", listener_addr);

            let kv_store_follower = kv_store.clone();

            let leader_addr = self.leader_addr.clone();
            let kv_store_follower = kv_store_follower.clone();
            tokio::spawn(async { follower_thread(leader_addr, kv_store_follower).await });

            let kv_store_client = kv_store.clone();
            // Handle Multiple Clients in a loop
            loop {
                // it's a follower instance

                let (tcp_stream, socket_addr) = listener
                    .accept()
                    .await
                    .expect("Accepting connection failed");
                log::info!(
                    "Follower: Accepted connection from {}",
                    socket_addr.ip().to_string()
                );
                let kv_store_client = kv_store_client.clone();
                let state = Arc::clone(&state);
                // tokio::spawn(handle_client(tcp_stream, socket_addr, kv_store, None));
                tokio::spawn(async move {
                    let mut conn = Connection::new(state, tcp_stream, socket_addr);
                    let _ = conn.handle(kv_store_client).await;
                });
            }
        })
    }
}

pub struct Leader {
    pub bind_address: String,
    pub listening_port: u16,
    pub dir_name: Option<String>,
    pub dbfilename: Option<String>,
    pub peers: Vec<Follower>,
}

impl Leader {
    pub fn new(
        bind_address: Option<String>,
        listening_port: Option<u16>,
        dir_name: Option<String>,
        dbfilename: Option<String>,
    ) -> Self {
        let bind_address = if bind_address.is_some() {
            bind_address.unwrap()
        } else {
            "127.0.0.1".to_string()
        };

        let listening_port = if listening_port.is_some() {
            listening_port.unwrap()
        } else {
            6379u16
        };

        Self {
            bind_address,
            listening_port,
            dir_name,
            dbfilename,
            peers: Vec::new(),
        }
    }
}

impl RedisInstance for Leader {
    fn run(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async {
            // initialise the DB
            //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
            let kv_store: KeyValueStore<String, String> = KeyValueStore::new();
            let state = Arc::new(Mutex::new(Shared::new()));

            if self.dir_name.is_some() && self.dbfilename.is_some() {
                log::info!(
                    "initialising database from rdb file {}/{}..",
                    self.dir_name.clone().unwrap(),
                    self.dbfilename.clone().unwrap()
                );
                load_from_rdb(kv_store.clone())
                    .await
                    .expect("RDB file read failed");
            }

            // Create TCP Listener
            let bind_address = STATE.get_val(&"bind_address".to_string()).unwrap();
            let listening_port = STATE.get_val(&"listening_port".to_string()).unwrap();
            let listener_addr = format!("{}:{}", bind_address, listening_port);
            let listener = TcpListener::bind(listener_addr.to_owned())
                .await
                .expect("Binding to listener address failed!");
            log::info!("Redis running on {}...", listener_addr);

            // Handle Multiple Clients in a loop
            loop {
                let master_replid: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(40) // 40 character long
                    .map(char::from) // `u8` values to `char`
                    .collect();

                STATE.push(("master_replid".into(), master_replid));

                let master_repl_offset: u64 = 0;
                STATE.push(("master_repl_offset".into(), master_repl_offset.to_string()));

                // listener
                let (tcp_stream, socket_addr) = listener
                    .accept()
                    .await
                    .expect("Accepting connection failed");
                log::info!("Accepted connection from {}", socket_addr.ip().to_string());

                let kv_store_client = kv_store.clone();
                let state = Arc::clone(&state);

                tokio::spawn(async move {
                    let mut conn = Connection::new(state, tcp_stream, socket_addr);
                    let _ = conn.handle(kv_store_client).await;
                });
            }
        })
    }
}

pub async fn start_server(
    bind_address: Option<String>,
    listening_port: Option<u16>,
    dir_name: Option<String>,
    dbfilename: Option<String>,
    replicaof: Option<String>,
) {
    // Start logging.
    femme::start();
    if bind_address.is_some() {
        STATE.push(("bind_address".to_string(), bind_address.clone().unwrap()));
    }

    if listening_port.is_some() {
        STATE.push((
            "listening_port".to_string(),
            listening_port.unwrap().to_string(),
        ));
    }

    if dir_name.is_some() {
        STATE.push(("dir".to_string(), dir_name.clone().unwrap()));
    }

    if dbfilename.is_some() {
        STATE.push(("dbfilename".to_string(), dbfilename.clone().unwrap()));
    }

    let leader_addr = if replicaof.is_some() {
        let leader_addr = if let Some(val) = replicaof {
            let ip_and_port: Vec<&str> = val.split_whitespace().collect();
            if ip_and_port.len() > 2 {
                panic!("Wrong number of arguments in leader connection string");
            }
            format!("{}:{}", ip_and_port[0], ip_and_port[1])
        } else {
            panic!("Leader address is not valid");
        };
        STATE.push(("LEADER".to_string(), leader_addr.clone()));
        Some(leader_addr)
    } else {
        None
    };

    if leader_addr.is_none() {
        // it's a Leader instance
        let leader: Box<dyn RedisInstance> = Box::new(Leader::new(
            bind_address.clone(),
            listening_port,
            dir_name.clone(),
            dbfilename.clone(),
        ));
        leader.run().await;
    } else {
        // it's a follower instance
        let follower: Box<dyn RedisInstance> = Box::new(Follower::new(
            bind_address.clone(),
            listening_port,
            leader_addr.clone(),
        ));
        follower.run().await;
    }
}

async fn follower_thread(
    leader_addr: String,
    store: KeyValueStore<String, String>,
) -> anyhow::Result<(), String> {
    let stream = Arc::new(Mutex::new(TcpStream::connect(leader_addr).await.unwrap()));
    let mut buffer = BytesMut::with_capacity(16 * 1024);

    match follower_handshake(stream.clone()).await {
        Ok(_) => {
            loop {
                dbg!("After handshake");
                let mut stream = stream.lock().await;
                if let Ok(n) = stream.read_buf(&mut buffer).await {
                    if n == 0 {
                        if buffer.is_empty() {
                            return Ok(());
                        } else {
                            return Err("Follower thread failed!".to_string());
                        }
                    }
                    dbg!(&buffer);
                    let cmd_from_leader = buffer[..n].to_vec();

                    let s = String::from_utf8_lossy(&cmd_from_leader).to_string();
                    // if let Ok(tk) = Tokenizer::new(&cmd_from_leader) {
                    if let Ok(resp_parsed) = RespData::parse(&s) {
                        let mut resp_parsed_iter = resp_parsed.iter();
                        while let Some(parsed) = resp_parsed_iter.next() {
                            // let cmd_from_leader = cmd_from_leader.clone();
                            match parsed {
                                RespData::Array(v) => match parse_command(v.to_vec()) {
                                    Ok(res) => match res {
                                        Command::Set(o) => {
                                            let key = o.key;
                                            let value = o.value;
                                            let expiry = o.expiry;
                                            let mut kv_store = store.clone();
                                            kv_store
                                                .insert(key.clone(), value.clone(), expiry)
                                                .await;
                                            drop(kv_store);
                                        }
                                        _ => {}
                                    },
                                    Err(_) => todo!(),
                                },
                                RespData::String(_) => todo!(),
                                RespData::ErrorStr(_) => todo!(),
                                RespData::Integer(_) => todo!(),
                                RespData::BulkStr(_) => todo!(),
                                RespData::Null => todo!(),
                                RespData::Boolean(_) => todo!(),
                                RespData::Double(_) => todo!(),
                                RespData::BulkError(_) => todo!(),
                                RespData::VerbatimStr(_) => todo!(),
                                RespData::Map(_) => todo!(),
                                RespData::Set(_) => todo!(),
                            }
                        }
                    } //
                    buffer.clear();
                }
            }
        }
        Err(e) => {
            eprintln!("{}", e);
            return Err(e);
        }
    }
}

async fn follower_handshake(stream: Arc<Mutex<TcpStream>>) -> anyhow::Result<(), String> {
    // Hashshake
    let mut stream = stream.lock().await;
    let mut buffer = BytesMut::with_capacity(1 * 512);
    let handshake_messages = vec![
        vec!["*1\r\n$4\r\nPING\r\n".to_string(), "+PONG\r\n".to_string()],
        vec![
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".to_string(),
            "+OK\r\n".to_string(),
        ],
        vec![
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".to_string(),
            "+OK\r\n".to_string(),
        ],
        vec![
            "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".to_string(),
            "+FULLRESYNC".to_string(),
        ],
    ];
    let mut handshake_messages_iter = handshake_messages.iter().enumerate();
    while let Some((i, message)) = handshake_messages_iter.next() {
        let _ = stream.write_all(message[0].as_bytes()).await;
        if let Ok(n) = stream.read_buf(&mut buffer).await {
            if n == 0 {
                if buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err("Handshake failed!".to_string());
                }
            }

            let cmd_from_leader = buffer[..n].to_vec();

            let s = String::from_utf8_lossy(&cmd_from_leader).to_string();
            // Sometimes the response to `PSYNC ? -1` comes in a single network read
            // In that case, the `+FULLRESYNC` and the database contents will be in the same buffer
            // Our resp parser will parse it correctly, but the result will have two items.
            let parsed = &parse_handshake_response(&s)[0];
            let parsed_len = parsed.len();
            if parsed_len == 1 {
                if !message[1].contains(parsed[0].split(" ").collect::<Vec<&str>>()[0]) {
                    return Err("Handshake failed!".to_string());
                }
            } else if parsed_len == 2 {
                if !parsed[0].starts_with("FULLRESYNC") && !parsed[1].starts_with("REDIS") {
                    return Err("Handshake failed!".to_string());
                } else {
                    log::info!("Handshake Completed!!!");
                    return Ok(());
                }
            }
        }
        buffer.clear();
    }
    if let Ok(n) = stream.read_buf(&mut buffer).await {
        let response = String::from_utf8_lossy(&buffer[..n]).to_string();
        if !response.contains("REDIS") {
            return Err("Handshake Failed!".into());
        } else {
            log::info!("Handshake Completed!!!");
        }
    }
    buffer.clear();
    drop(stream);

    Ok(())
}
