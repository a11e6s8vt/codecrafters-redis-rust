use crate::{resp::RespError, token::Tokenizer};
use bytes::BytesMut;
use core::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
};

use crate::resp::RespData;

const CHUNK_SIZE: usize = 16 * 1024;

pub struct Connection<'a> {
    pub socket_addr: SocketAddr,
    reader: BufReader<ReadHalf<'a>>,
    writer: BufWriter<WriteHalf<'a>>,
    buffer: BytesMut,
}

impl<'a> Connection<'a> {
    pub fn new(stream: &'a mut TcpStream, socket_addr: SocketAddr) -> Connection<'a> {
        let (reader, writer) = stream.split();
        let reader = BufReader::new(reader);
        let writer = BufWriter::new(writer);
        let c = Self {
            socket_addr,
            reader,
            writer,
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
        };
        c
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<RespData>, RespError> {
        loop {
            if let Ok(num_bytes) = self.reader.read_buf(&mut self.buffer).await {
                if num_bytes == 0 {
                    if self.buffer.is_empty() {
                        return Ok(None);
                    } else {
                        return Err(RespError::Invalid);
                    }
                }

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
        let _ = self.writer.write_all(buf).await;
        let _ = self.writer.flush().await;
    }
}
