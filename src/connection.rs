use crate::{
    cmds::{Command, CommandError, SubCommand},
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

    pub async fn apply(
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
            let mut response = String::new();
            if let Ok(data) = RespData::try_from(tk) {
                match data {
                    RespData::Array(v) => match parse_command(v) {
                        Ok(res) => match res {
                            Command::Ping(o) => {
                                if o.value.is_some() {
                                    response.push_str(&format!("+{}{}", o.value.unwrap(), CRLF));
                                } else {
                                    response.push_str(&format!("+PONG{}", CRLF));
                                }
                            }
                            Command::Echo(o) => {
                                if o.value.is_some() {
                                    response.push_str(&format!("+{}{}", o.value.unwrap(), CRLF));
                                } else {
                                    response.push_str(&format!(
                                        "-Error ERR wrong number of arguments for 'echo' command{}",
                                        CRLF
                                    ));
                                }
                            }
                            Command::Get(o) => {
                                let key = o.key;
                                let mut db = db.clone();
                                if let Some(value) = db.get(&key).await {
                                    response.push_str(&format!(
                                        "${}{}{}{}",
                                        &value.len().to_string(),
                                        CRLF,
                                        &value,
                                        CRLF
                                    ));
                                } else {
                                    response.push_str(&format!("$-1{}", CRLF));
                                }
                            }
                            Command::Set(o) => {
                                let key = o.key;
                                let value = o.value;
                                let expiry = o.expiry;
                                let mut db = db.clone();
                                db.insert(key, value, expiry).await;
                                response.push_str(&format!("+OK{}", CRLF));
                                drop(db);
                            }
                            Command::Config(o) => {
                                dbg!(o.clone());
                                match o.sub_command {
                                    SubCommand::Get(pattern) => {
                                        if let Some(res) = CONFIG_LIST.get_val(&pattern) {
                                            dbg!(res);
                                            // *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
                                            response.push_str(&format!(
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
                                            ));
                                        }
                                    }
                                }
                            }
                            Command::Save(_o) => {
                                response.push_str(&format!("+OK{}", CRLF));
                                db::write_to_disk(db.clone()).await.expect("Write failed")
                            }
                            Command::Keys(o) => {
                                let _arg = o.arg;
                                // *1\r\n$3\r\nfoo\r\n
                                response.push_str(&format!("*{}{}", db.get_ht_size().await, CRLF));
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
                            }
                        },
                        Err(e) => match e.clone() {
                            CommandError::SyntaxError(n) => {
                                response.push_str(&format!("-{}{}", &e.message(), CRLF));
                            }
                            CommandError::WrongNumberOfArguments(n) => {
                                response.push_str(&format!("-{}{}", &e.message(), CRLF));
                            }
                            CommandError::NotSupported => {
                                response.push_str(&format!("-{}{}", &e.message(), CRLF));
                            }
                            CommandError::NotValidType(x) => {
                                response.push_str(&format!("-{}{}", &e.message(), CRLF));
                            }
                            CommandError::UnknownSubCommand(x) => {
                                response.push_str(&format!("-{}{}", &e.message(), CRLF));
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
            let _ = guard.write_all(response.as_bytes()).await;
            let _ = guard.flush().await;
            drop(guard);
            self.buffer.clear();
        }

        Ok(())
    }
}
