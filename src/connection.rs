use crate::{
    cmds::{Command, CommandError, InfoSubCommand, SubCommand},
    db::{self, ExpiringHashMap},
    parse::parse_command,
    resp::RespError,
    token::Tokenizer,
};
use bytes::BytesMut;
use core::net::SocketAddr;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
    sync::Mutex,
};

use crate::global::CONFIG_LIST;
use crate::resp::RespData;

const CHUNK_SIZE: usize = 16 * 1024;
const CRLF: &str = "\r\n";

pub struct Connection<'a> {
    pub socket_addr: SocketAddr,
    reader: Arc<Mutex<BufReader<ReadHalf<'a>>>>,
    writer: Arc<Mutex<BufWriter<WriteHalf<'a>>>>,
    buffer: BytesMut,
}

impl<'a> Connection<'a> {
    pub fn new(stream: &'a mut TcpStream, socket_addr: SocketAddr) -> Connection<'a> {
        let (reader, writer) = stream.split();
        let reader = Arc::new(Mutex::new(BufReader::new(reader)));
        let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
        let c = Self {
            socket_addr,
            reader,
            writer,
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
        };
        c
    }

    pub async fn handle(
        &mut self,
        db: ExpiringHashMap<String, String>,
    ) -> anyhow::Result<(), RespError> {
        let mut guard = self.reader.lock().await;
        while let Ok(num_bytes) = guard.read_buf(&mut self.buffer).await {
            if num_bytes == 0 {
                if self.buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err(RespError::Invalid);
                }
            }
            let tk = Tokenizer::new(&self.buffer[..num_bytes]);
            dbg!(&tk);
            let mut responses: Vec<Vec<u8>> = Vec::new();
            if let Ok(data) = RespData::try_from(tk) {
                dbg!(&data);
                match data {
                    RespData::Array(v) => match parse_command(v) {
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
                                let mut db = db.clone();
                                if let Some(value) = db.get(&key).await {
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
                                let mut db = db.clone();
                                db.insert(key, value, expiry).await;
                                responses.push(format!("+OK{}", CRLF).as_bytes().to_vec());
                                drop(db);
                            }
                            Command::Config(o) => {
                                dbg!(o.clone());
                                match o.sub_command {
                                    SubCommand::Get(pattern) => {
                                        if let Some(res) = CONFIG_LIST.get_val(&pattern) {
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
                                db::write_to_disk(db.clone()).await.expect("Write failed")
                            }
                            Command::Keys(o) => {
                                let _arg = o.arg;
                                // *1\r\n$3\r\nfoo\r\n
                                let mut response = format!("*{}{}", db.get_ht_size().await, CRLF);
                                let mut db = db.clone();
                                for (key, _) in db.iter().await {
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
                                    if let Some(_replicaof) =
                                        CONFIG_LIST.get_val(&"replicaof".to_string())
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
                                            CONFIG_LIST.get_val(&"master_replid".into())
                                        {
                                            master_replid.to_owned()
                                        } else {
                                            "".to_string()
                                        };

                                        let master_repl_offset = if let Some(master_repl_offset) =
                                            CONFIG_LIST.get_val(&"master_repl_offset".into())
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
                                dbg!(args.clone());
                                let mut args_iter = args.iter();
                                let first = args_iter.next().expect("First cannot be empty");
                                match first.as_str() {
                                    "capa" => {
                                        if args_iter.next() == Some(&"psync2".to_string()) {
                                            responses
                                                .push(format!("+OK{}", CRLF).as_bytes().to_vec())
                                        }
                                    }
                                    "listening-port" => {
                                        let port =
                                            args_iter.next().expect("Expect a valid port number");
                                        if let Ok(port) = port.parse::<u16>() {
                                            responses
                                                .push(format!("+OK{}", CRLF).as_bytes().to_vec());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Command::Psync(o) => {
                                let args = o.args;
                                dbg!(&args);
                                let mut args_iter = args.iter();
                                if args_iter.next() == Some(&"?".to_string())
                                    && args_iter.next() == Some(&"-1".to_string())
                                {
                                    let repl_id = CONFIG_LIST
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
                                }
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
            } else {
                // todo: it must be parse error
                return Err(RespError::Invalid);
            }
            let mut guard = self.writer.lock().await;
            for content in responses {
                let _ = guard.write_all(&content).await;
            }
            let _ = guard.flush().await;
            drop(guard);
            self.buffer.clear();
        }

        Ok(())
    }
}
