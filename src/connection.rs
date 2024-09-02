use crate::{resp::RespError, token::Tokenizer};
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

use crate::resp::RespData;

const CHUNK_SIZE: usize = 16 * 1024;

pub struct Connection {
    pub socket_addr: SocketAddr,
    //reader: Arc<Mutex<BufReader<ReadHalf<'a>>>>,
    writer: Arc<Mutex<BufWriter<TcpStream>>>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream, socket_addr: SocketAddr) -> Connection {
        // let (reader, writer) = stream.split();
        // let reader = Arc::new(Mutex::new(BufReader::new(reader)));
        let writer = Arc::new(Mutex::new(BufWriter::new(stream)));
        let c = Self {
            socket_addr,
            // reader,
            writer,
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
        };
        c
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<RespData>, RespError> {
        loop {
            let mut guard = self.writer.lock().await;
            if let Ok(num_bytes) = guard.read_buf(&mut self.buffer).await {
                if num_bytes == 0 {
                    if self.buffer.is_empty() {
                        return Ok(None);
                    } else {
                        return Err(RespError::Invalid);
                    }
                }
                drop(guard);
                let tk = Tokenizer::new(&self.buffer[..num_bytes]);
                if let Ok(data) = RespData::try_from(tk) {
                    return Ok(Some(data));
                } else {
                    // todo: it must be parse error
                    return Err(RespError::Invalid);
                }
            }
        }
    }

    pub async fn write(&mut self, buf: &[u8]) {
        let mut guard = self.writer.lock().await;
        let _ = guard.write_all(buf).await;
        let _ = guard.flush().await;
        drop(guard);
    }
}
