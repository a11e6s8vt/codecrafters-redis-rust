use crate::{
    command::{Command, CommandError},
    database::ExpiringHashMap,
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
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use crate::resp::RespData;

const CHUNK_SIZE: usize = 16 * 1024;
const CRLF: &str = "\r\n";

// pub struct Connection<'a> {
//     pub socket_addr: SocketAddr,
//     reader: FramedRead<ReadHalf<'a>, LinesCodec>,
//     writer: FramedWrite<WriteHalf<'a>, LinesCodec>,
//     buffer: BytesMut,
// }

pub struct Connection<'a> {
    pub socket_addr: SocketAddr,
    reader: Arc<Mutex<BufReader<ReadHalf<'a>>>>,
    writer: Arc<Mutex<BufWriter<WriteHalf<'a>>>>,
    buffer: BytesMut,
}

impl<'a> Connection<'a> {
    // pub fn new(stream: &'a mut TcpStream, socket_addr: SocketAddr) -> Connection<'a> {
    //     let (reader, writer) = stream.split();
    //     let reader = FramedRead::new(reader, LinesCodec::new());
    //     let writer = FramedWrite::new(writer, LinesCodec::new());
    //     let c = Self {
    //         socket_addr,
    //         reader,
    //         writer,
    //         buffer: BytesMut::with_capacity(CHUNK_SIZE),
    //     };
    //     c
    // }
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

    // pub async fn read(&mut self) -> anyhow::Result<Option<RespData>, RespError> {
    //     loop {
    //         let mut guard = self.reader.lock().await;
    //         let num_bytes = guard
    //             .read_buf(&mut self.buffer)
    //             .await
    //             .expect("failed to read from socket");
    //         drop(guard);
    //         if num_bytes == 0 {
    //             if self.buffer.is_empty() {
    //                 return Ok(None);
    //             } else {
    //                 return Err(RespError::Invalid);
    //             }
    //         }

    //         let tk = Tokenizer::new(&self.buffer[..num_bytes]);
    //         if let Ok(data) = RespData::try_from(tk) {
    //             return Ok(Some(data));
    //         } else {
    //             // todo: it must be parse error
    //             return Err(RespError::Invalid);
    //         }
    //     }
    // }

    // pub async fn write(&mut self, buf: &[u8]) {
    //     let mut guard = self.writer.lock().await;
    //     let _ = guard.write_all(buf).await;
    //     let _ = guard.flush().await;
    //     drop(guard);
    // }

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
            println!("{:?}", &tk);
            let mut response = String::new();
            if let Ok(data) = RespData::try_from(tk) {
                println!("{:?}", &data);
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
                                // let guard = db.lock().await;
                                if db.contains_key(&key).await {
                                    if let Some(value) = db.get(&key).await {
                                        response.push_str(&format!(
                                            "${}{}{}{}",
                                            &value.len().to_string(),
                                            CRLF,
                                            &value,
                                            CRLF
                                        ));
                                    }
                                } else {
                                    response.push_str(&format!("$-1{}", CRLF));
                                }
                                // drop(guard);
                            }
                            Command::Set(o) => {
                                dbg!(o.clone());
                                let key = o.key;
                                let value = o.value;
                                let expiry = o.expiry;
                                // let mut guard = db.lock().await;
                                let mut db = db.clone();
                                db.insert(key, value, expiry).await;
                                response.push_str(&format!("+OK{}", CRLF));
                                drop(db);
                                dbg!("response = {}", response.clone());
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
                            CommandError::NotValidType { cmd, arg } => {
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
