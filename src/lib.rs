mod cli;
mod cmds;
mod connection;
mod database;
mod global;
mod parse;
mod resp;

use std::{
    any::Any,
    collections::VecDeque,
    future::Future,
    pin::Pin,
    str,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
pub use cli::Cli;
use cmds::Command;
use connection::Connection;
pub use database::{load_from_rdb, KeyValueStore};
use database::{RadixTreeStore, Shared};
pub use global::STATE;

use parse::parse_command;
use rand::{distributions::Alphanumeric, Rng};
use resp::RespData;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{self, Duration},
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
    pub bytes_received: Arc<AtomicUsize>,
    pub commands_processed: Arc<Mutex<VecDeque<String>>>,
}

pub struct Leader {
    pub bind_address: String,
    pub listening_port: u16,
    pub dir_name: Option<String>,
    pub dbfilename: Option<String>,
    pub peers: Vec<Follower>,
}

impl Follower {
    pub fn new(
        bind_address: Option<String>,
        listening_port: Option<u16>,
        leader_addr: Option<String>,
    ) -> Self {
        let bind_address = if let Some(bind_address) = bind_address {
            bind_address
        } else {
            "127.0.0.1".to_string()
        };

        let listening_port = if let Some(listening_port) = listening_port {
            listening_port
        } else {
            panic!("Port cannot be empty!");
        };

        let leader_addr = if let Some(leader_addr) = leader_addr {
            leader_addr
        } else {
            panic!("Leader address (--replicaof) cannot be empty");
        };

        Self {
            bind_address,
            listening_port,
            leader_addr,
            bytes_received: Arc::new(AtomicUsize::new(0)),
            commands_processed: Arc::new(Mutex::new(VecDeque::new())),
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
            let stream_store: RadixTreeStore = RadixTreeStore::new();

            let kv_store_follower = kv_store.clone();
            let kv_store_client = kv_store.clone();

            // Handle the follower thread
            let leader_addr = self.leader_addr.clone();
            let bytes_received = self.bytes_received.clone();
            let kv_store_follower = kv_store_follower.clone();
            tokio::spawn(async {
                follower_thread(leader_addr, bytes_received, kv_store_follower).await
            });

            // Redis clients for the Follower instance
            let state = Arc::new(Mutex::new(Shared::new()));
            // Create TCP Listener
            let listener_addr = format!("{}:{}", self.bind_address, self.listening_port);
            let listener = TcpListener::bind(listener_addr.to_owned())
                .await
                .expect("Binding to listener address failed!");
            log::info!("Follower running on {}...", listener_addr);
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

                // Handle clients
                let kv_store_client = kv_store_client.clone();
                let stream_store_client = stream_store.clone();
                let state = Arc::clone(&state);

                tokio::spawn(async move {
                    let mut conn = Connection::new(state, tcp_stream, socket_addr);
                    let _ = conn.handle(kv_store_client, stream_store_client).await;
                });
            }
        })
    }
}

impl Leader {
    pub fn new(
        bind_address: Option<String>,
        listening_port: Option<u16>,
        dir_name: Option<String>,
        dbfilename: Option<String>,
    ) -> Self {
        let bind_address = if let Some(bind_address) = bind_address {
            bind_address
        } else {
            "127.0.0.1".to_string()
        };

        let listening_port = listening_port.unwrap_or(6379u16);

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
            let stream_store: RadixTreeStore = RadixTreeStore::new();
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

                // Handle Clients
                let kv_store_client = kv_store.clone();
                let stream_store_client = stream_store.clone();
                let state_client_handles = Arc::clone(&state);
                tokio::spawn(async move {
                    let mut conn = Connection::new(state_client_handles, tcp_stream, socket_addr);
                    let _ = conn.handle(kv_store_client, stream_store_client).await;
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
    bytes_received: Arc<AtomicUsize>,
    store: KeyValueStore<String, String>,
) -> anyhow::Result<()> {
    let stream = match follower_connect(leader_addr).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("{}", e);
            return Err(e.into());
        }
    };

    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let mut stream = stream.lock().await;
    loop {
        if let Ok(n) = stream.read_buf(&mut buffer).await {
            if n == 0 {
                if buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err(anyhow::format_err!("Follower thread failed!".to_string()));
                }
            }
            // check if the buffer contains `getack` command. We will need to omit length of one `getack`
            // from the total_bytes as each getack calculates length of commands processed so far excluding the
            // current get ack
            let cmd_from_leader = buffer[..n].to_vec();

            let s = String::from_utf8_lossy(&cmd_from_leader).to_string();

            if let Ok(resp_parsed) = RespData::parse(&s) {
                let total_bytes = calculate_bytes(bytes_received.clone(), &resp_parsed);
                // let resp_parsed_clone = resp_parsed.clone();
                let mut resp_parsed_iter = resp_parsed.iter();
                while let Some(parsed) = resp_parsed_iter.next() {
                    match parsed {
                        RespData::Array(v) => match parse_command(v.to_vec()) {
                            Ok(res) => match res {
                                Command::Set(o) => {
                                    let key = o.key;
                                    let value = o.value;
                                    let expiry = o.expiry;
                                    let mut kv_store = store.clone();
                                    kv_store.insert(key.clone(), value.clone(), expiry).await;
                                    drop(kv_store);
                                }
                                Command::Replconf(o) => {
                                    let args = o.args;
                                    let mut args_iter = args.iter();
                                    let first = args_iter.next().expect("First cannot be empty");
                                    match first.to_ascii_lowercase().as_str() {
                                        "getack" => {
                                            let opt = args_iter
                                                .next()
                                                .expect("Expect a valid port number");
                                            match opt.to_ascii_lowercase().as_str() {
                                                "*" => {
                                                    let response = format!(
                                                        "*3{}$8{}REPLCONF{}$3{}ACK{}${}{}{}{}",
                                                        CRLF,
                                                        CRLF,
                                                        CRLF,
                                                        CRLF,
                                                        CRLF,
                                                        total_bytes.to_string().len(),
                                                        CRLF,
                                                        total_bytes,
                                                        CRLF
                                                    )
                                                    .as_bytes()
                                                    .to_vec();
                                                    let _ = stream.write_all(&response).await;
                                                }
                                                _ => {}
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            },
                            Err(e) => log::error!("{:?}", e),
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
            }
            buffer.clear();
        }
    }
}

async fn follower_connect(leader_addr: String) -> anyhow::Result<Arc<Mutex<TcpStream>>> {
    let mut backoff = 1;

    loop {
        match TcpStream::connect(leader_addr.clone()).await {
            Ok(socket) => {
                let stream = Arc::new(Mutex::new(socket));
                match follower_handshake(stream.clone()).await {
                    Ok(_) => return Ok(stream),
                    Err(err) => {
                        if backoff > 64 {
                            // Accept has failed too many times. Return the error.
                            return Err(anyhow::format_err!(err));
                        }
                    }
                }
            }
            Err(err) => {
                if backoff > 64 {
                    // Accept has failed too many times. Return the error.
                    return Err(err.into());
                }
            }
        }

        // Pause execution until the back off period elapses.
        time::sleep(Duration::from_secs(backoff)).await;

        // Double the back off
        backoff *= 2;
    }
}

async fn follower_handshake(stream: Arc<Mutex<TcpStream>>) -> anyhow::Result<(), String> {
    // Hashshake
    let mut stream = stream.lock().await;
    let mut buffer = BytesMut::with_capacity(1 * 512);
    let handshake_messages_part1 = vec![
        "*1\r\n$4\r\nPING\r\n".to_string(),
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".to_string(),
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".to_string(),
    ];
    let handshake_messages_part1_responses = vec![
        "+PONG\r\n".to_string(),
        "+OK\r\n".to_string(),
        "+OK\r\n".to_string(),
    ];
    let handshake_messages_part2 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".to_string();

    // Handshake first part
    for (msg, response) in handshake_messages_part1
        .iter()
        .zip(handshake_messages_part1_responses.iter())
    {
        let _ = stream.write_all(msg.as_bytes()).await;
        if let Ok(n) = stream.read_buf(&mut buffer).await {
            if n == 0 {
                if buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err("Handshake failed!".to_string());
                }
            }
            if !std::str::from_utf8(&buffer[..n])
                .expect("Utf8Error")
                .contains(response.as_str())
            {
                return Err("Handshake failed!".to_string());
            }
        }
        buffer.clear();
    }

    // Handshake Second part
    let _ = stream.write_all(handshake_messages_part2.as_bytes()).await;
    // Leader response `+FULLRESYNC <REPL_ID> 0\r\n` is 56 bytes
    let mut buffer = [0; 56];
    let _ = stream.read_exact(&mut buffer).await;
    if let Ok(leader_response) = std::str::from_utf8(&buffer) {
        if !leader_response.to_ascii_lowercase().contains("fullresync") {
            return Err("Handshake failed!".to_string());
        }
    }

    // Leader send the empty RDB file
    // Next five byte indicates is the length of the RDB. It ends with '\r\n'
    let mut buffer: Vec<u8> = Vec::new();
    while let Ok(byte) = stream.read_u8().await {
        if byte as char != '\n' {
            buffer.push(byte);
        } else {
            buffer.push(byte);
            break;
        }
    }

    let rdb_len = if let Ok(parsed) = RespData::parse(&String::from_utf8_lossy(&buffer).to_string())
    {
        let rdb_len = match parsed[0] {
            RespData::Integer(num) => num as usize,
            _ => return Err("Handshake failed!".to_string()),
        };
        rdb_len
    } else {
        return Err("Handshake failed!".to_string());
    };

    // Read `rdb_len` bytes
    let mut buffer: Vec<u8> = vec![0; rdb_len];
    if let Ok(_) = stream.read_exact(&mut buffer).await {
        let magic_string = &buffer[..5];
        if let Ok(magic_string) = std::str::from_utf8(magic_string) {
            if !magic_string.to_ascii_lowercase().contains("redis") {
                return Err("Handshake failed!".to_string());
            }
        }
    }

    drop(stream);

    Ok(())
}

fn calculate_bytes(bytes_received: Arc<AtomicUsize>, parsed: &Vec<RespData>) -> usize {
    let mut total_bytes: usize = 0;
    for data in parsed {
        match data {
            RespData::Array(vec) => {
                let mut cmd = String::from(&format!("*{}\r\n", vec.len()));
                for item in vec {
                    match item {
                        RespData::String(s) => {
                            cmd.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
                        }
                        RespData::ErrorStr(_) => todo!(),
                        RespData::Integer(num) => {
                            cmd.push_str(&format!("${}\r\n{}\r\n", num.to_string().len(), num));
                        }
                        _ => todo!(),
                    }
                }
                total_bytes = bytes_received.fetch_add(cmd.len(), Ordering::Relaxed);
            }
            _ => todo!(),
        }
    }

    total_bytes
}
