use crate::{
    cmds::{Command, CommandError, InfoSubCommand, SubCommand},
    database::{self, KeyValueStore, Shared},
    parse::parse_command,
    resp::{RespError, Tokenizer},
    Request,
};
use bytes::{BufMut, BytesMut};
use core::net::SocketAddr;
use std::{sync::Arc, thread, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
    sync::{mpsc, Mutex},
};

use crate::global::STATE;
use crate::resp::RespData;

const CHUNK_SIZE: usize = 16 * 1024;
const CRLF: &str = "\r\n";

pub struct Connection {
    state: Arc<Mutex<Shared>>,
    pub socket_addr: SocketAddr,
    stream: TcpStream,
    // reader: Arc<Mutex<BufReader<ReadHalf<'a>>>>,
    // writer: Arc<Mutex<BufWriter<WriteHalf<'a>>>>,
    buffer: BytesMut,
    is_a_peer: bool,
}

impl Connection {
    pub fn new(
        state: Arc<Mutex<Shared>>,
        stream: TcpStream,
        socket_addr: SocketAddr,
    ) -> Connection {
        // let (reader, writer) = stream.split();
        // let reader = Arc::new(Mutex::new(BufReader::new(reader)));
        // let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
        Self {
            state,
            socket_addr,
            stream,
            // reader,
            // writer,
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
            is_a_peer: false,
        }
    }

    pub async fn handle(
        &mut self,
        kv_store: KeyValueStore<String, String>,
    ) -> anyhow::Result<(), RespError> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

        loop {
            tokio::select! {
                //while let Ok(num_bytes) = self.stream.read_buf(&mut self.buffer).await {
                Some(msg) = rx.recv() => {
                    self.write(vec![msg]).await;
                }
                network_read_result = self.stream.read_buf(&mut self.buffer) => {
                    if let Ok(num_bytes_read) = network_read_result {
                        if num_bytes_read == 0 {
                            if self.buffer.is_empty() {
                                return Ok(());
                            } else {
                                return Err(RespError::Invalid);
                            }
                        }
                        let str_from_network = self.buffer[..num_bytes_read].to_vec();
                        let mut responses: Vec<Vec<u8>> = Vec::new();
                        let s = String::from_utf8_lossy(&str_from_network).to_string();
                        if let Ok(resp_parsed) = RespData::parse(&s) {
                            let mut resp_parsed_iter = resp_parsed.iter();
                            while let Some(parsed) = resp_parsed_iter.next() {
                                let str_from_network = str_from_network.clone();
                                match parsed {
                                    RespData::Array(v) => match parse_command(v.to_vec()) {
                                        Ok(res) => match res {
                                            Command::Ping(o) => {
                                                if o.value.is_some() {
                                                    responses.push(
                                                        format!("+{}{}", o.value.unwrap(), CRLF)
                                                            .as_bytes()
                                                            .to_vec(),
                                                    );
                                                } else {
                                                    responses.push(format!("+PONG{}", CRLF).as_bytes().to_vec());
                                                }
                                            }
                                            Command::Echo(o) => {
                                                if o.value.is_some() {
                                                    responses.push(
                                                        format!("+{}{}", o.value.unwrap(), CRLF)
                                                            .as_bytes()
                                                            .to_vec(),
                                                    );
                                                } else {
                                                    responses.push(
                                                        format!(
                                                        "-Error ERR wrong number of arguments for 'echo' command{}",
                                                        CRLF
                                                    )
                                                        .as_bytes()
                                                        .to_vec(),
                                                    );
                                                }
                                            }
                                            Command::Get(o) => {
                                                let key = o.key;
                                                let mut kv_store = kv_store.clone();
                                                if let Some(value) = kv_store.get(&key).await {
                                                    responses.push(
                                                        format!(
                                                            "${}{}{}{}",
                                                            &value.len().to_string(),
                                                            CRLF,
                                                            &value,
                                                            CRLF
                                                        )
                                                        .as_bytes()
                                                        .to_vec(),
                                                    );
                                                } else {
                                                    responses.push(format!("$-1{}", CRLF).as_bytes().to_vec());
                                                }
                                            }
                                            Command::Set(o) => {
                                                let key = o.key;
                                                let value = o.value;
                                                let expiry = o.expiry;
                                                let mut kv_store = kv_store.clone();
                                                kv_store.insert(key.clone(), value.clone(), expiry).await;
                                                responses.push(format!("+OK{}", CRLF).as_bytes().to_vec());
                                                drop(kv_store);
                                                // replicate data to peers
                                                self.state.lock().await.broadcast(str_from_network).await;
                                                // replicate_to_peers(&replication_message, peers.clone()).await;
                                            }
                                            Command::Config(o) => {
                                                dbg!(o.clone());
                                                match o.sub_command {
                                                    SubCommand::Get(pattern) => {
                                                        if let Some(res) = STATE.get_val(&pattern) {
                                                            dbg!(res);
                                                            // *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
                                                            responses.push(
                                                                format!(
                                                                    "*2{}${}{}{}{}${}{}{}{}",
                                                                    CRLF,
                                                                    pattern.len(),
                                                                    CRLF,
                                                                    pattern,
                                                                    CRLF,
                                                                    res.len(),
                                                                    CRLF,
                                                                    res,
                                                                    CRLF
                                                                )
                                                                .as_bytes()
                                                                .to_vec(),
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Command::Save(_o) => {
                                                responses.push(format!("+OK{}", CRLF).as_bytes().to_vec());
                                                database::write_to_disk(kv_store.clone())
                                                    .await
                                                    .expect("Write failed")
                                            }
                                            Command::Keys(o) => {
                                                let _arg = o.arg;
                                                // *1\r\n$3\r\nfoo\r\n
                                                let mut response =
                                                    format!("*{}{}", kv_store.get_ht_size().await, CRLF);
                                                let mut kv_store = kv_store.clone();
                                                for (key, _) in kv_store.iter().await {
                                                    response.push_str(&format!(
                                                        "${}{}{}{}",
                                                        key.len(),
                                                        CRLF,
                                                        key,
                                                        CRLF
                                                    ));
                                                }
                                                responses.push(response.as_bytes().to_vec());
                                            }
                                            Command::Info(o) => match o.sub_command {
                                                Some(InfoSubCommand::Replication) => {
                                                    if let Some(_leader_addr) =
                                                        STATE.get_val(&"LEADER".to_string())
                                                    {
                                                        responses.push(
                                                            format!(
                                                                "${}{}{}{}",
                                                                "role:slave".len(),
                                                                CRLF,
                                                                "role:slave",
                                                                CRLF,
                                                            )
                                                            .as_bytes()
                                                            .to_vec(),
                                                        );
                                                    } else {
                                                        let master_replid = if let Some(master_replid) =
                                                            STATE.get_val(&"master_replid".into())
                                                        {
                                                            master_replid.to_owned()
                                                        } else {
                                                            "".to_string()
                                                        };

                                                        let master_repl_offset = if let Some(master_repl_offset) =
                                                            STATE.get_val(&"master_repl_offset".into())
                                                        {
                                                            master_repl_offset.to_owned()
                                                        } else {
                                                            "".to_string()
                                                        };

                                                        let data = format!("role:master{CRLF}master_replid:{master_replid}{CRLF}master_repl_offset:{master_repl_offset}");

                                                        responses.push(
                                                            format!("${}{}{}{}", data.len(), CRLF, data, CRLF,)
                                                                .as_bytes()
                                                                .to_vec(),
                                                        );
                                                    }
                                                }
                                                None => {}
                                            },
                                            Command::Replconf(o) => {
                                                let args = o.args;
                                                let mut args_iter = args.iter();
                                                let first = args_iter.next().expect("First cannot be empty");
                                                dbg!(&first);
                                                match first.as_str() {
                                                    "capa" => {
                                                        if args_iter.next() == Some(&"psync2".to_string()) {
                                                            responses.push(format!("+OK{}", CRLF).as_bytes().to_vec())
                                                        }
                                                    }
                                                    "listening-port" => {
                                                        let port = args_iter.next().expect("Expect a valid port number");
                                                        if let Ok(_port) = port.parse::<u16>() {
                                                            responses.push(format!("+OK{}", CRLF).as_bytes().to_vec());
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                            Command::Psync(o) => {
                                                let args = o.args;
                                                let mut args_iter = args.iter();
                                                if args_iter.next() == Some(&"?".to_string())
                                                    && args_iter.next() == Some(&"-1".to_string())
                                                {
                                                    let repl_id = STATE
                                                        .get_val(&"master_replid".to_string())
                                                        .expect("Expect a valid replication id");

                                                    responses.push(
                                                        format!("+FULLRESYNC {} 0{}", repl_id, CRLF)
                                                            .as_bytes()
                                                            .to_vec(),
                                                    );
                                                    let rdb_contents = [
                                                        82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100,
                                                        105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250,
                                                        10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
                                                        64, 250, 5, 99, 116, 105, 109, 101, 194, 5, 28, 228, 102,
                                                        250, 8, 117, 115, 101, 100, 45, 109, 101, 109, 194, 184,
                                                        75, 14, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192,
                                                        0, 255, 187, 243, 46, 0, 102, 82, 8, 22,
                                                    ];
                                                    let mut res = format!("${}{}", rdb_contents.len(), CRLF)
                                                        .as_bytes()
                                                        .to_vec();
                                                    res.extend(rdb_contents);
                                                    responses.push(res);
                                                    let tx = tx.clone();
                                                    self.state.lock().await.peers.insert(self.socket_addr, tx);
                                                    self.is_a_peer = true;
                                                }
                                            },
                                            Command::Wait(o) => {
                                                let args = o.args;
                                                let mut args_iter = args.iter();
                                                let n = self.state.lock().await.peers.len();
                                                let res = format!(":{}{}", n, CRLF);
                                                responses.push(res.as_bytes().to_vec());
                                            }
                                        },
                                        Err(e) => match e.clone() {
                                            CommandError::SyntaxError(_n) => {
                                                responses
                                                    .push(format!("-{}{}", &e.message(), CRLF).as_bytes().to_vec());
                                            }
                                            CommandError::WrongNumberOfArguments(_n) => {
                                                responses
                                                    .push(format!("-{}{}", &e.message(), CRLF).as_bytes().to_vec());
                                            }
                                            CommandError::NotSupported => {
                                                responses
                                                    .push(format!("-{}{}", &e.message(), CRLF).as_bytes().to_vec());
                                            }
                                            CommandError::NotValidType(_x) => {
                                                responses
                                                    .push(format!("-{}{}", &e.message(), CRLF).as_bytes().to_vec());
                                            }
                                            CommandError::UnknownSubCommand(_x) => {
                                                responses
                                                    .push(format!("-{}{}", &e.message(), CRLF).as_bytes().to_vec());
                                            }
                                        },
                                    },
                                    _ => {}
                                };
                            }
                        } else {
                            // todo: it must be parse error
                            return Err(RespError::Invalid);
                        }
                        self.write(responses).await;
                        self.buffer.clear();
                        // let state_replication_acks = Arc::clone(&self.state);
                        // thread::sleep(Duration::from_millis(1000));
                        // tokio::spawn(follower_replication_acks(state_replication_acks));
                    }
                }
            }
        }
    }

    pub async fn write(&mut self, message: Vec<Vec<u8>>) {
        for content in message {
            if let Err(e) = self.stream.write_all(&content).await {
                log::error!("Writing to TCP stream failed! {}", e);
            } else {
                if let Err(e) = self.stream.flush().await {
                    log::error!("Writing to TCP stream failed! {}", e);
                }
            }
        }
    }
}

async fn follower_replication_acks(state: Arc<Mutex<Shared>>) {
    let get_ack_cmd = b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n";
    state.lock().await.broadcast(get_ack_cmd.to_vec()).await;
}
