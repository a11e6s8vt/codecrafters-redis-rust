use std::collections::VecDeque;
use std::net::SocketAddr;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

pub use kv::KeyValueStore;
pub use rdb::{load_from_rdb, write_to_disk};
pub use stream::{RadixTreeStore, StreamEntry};
use tokio::sync::mpsc;

mod kv;
mod rdb;
mod stream;

type Tx = mpsc::UnboundedSender<Vec<u8>>;
type Rx = mpsc::UnboundedReceiver<Vec<u8>>;

pub struct Peer {
    pub sender: Tx,
    pub bytes_sent: AtomicUsize,
    pub bytes_written: AtomicUsize,
    // Stores last 10 commands sent to replica excluding `REPLCONF GETACK *`
    pub commands_processed: VecDeque<String>,
}

pub struct Shared {
    pub peers: HashMap<SocketAddr, Peer>,
}

impl Shared {
    pub fn new() -> Self {
        Shared {
            peers: HashMap::new(),
        }
    }

    pub async fn broadcast(&mut self, message: Vec<u8>) {
        for peer in self.peers.iter_mut() {
            let p = peer.1;
            let _ = p.sender.send(message.clone());
            p.bytes_sent
                .fetch_add(message.len(), std::sync::atomic::Ordering::Relaxed);
            let msg_str = String::from_utf8_lossy(&message).to_string();
            if !msg_str.to_ascii_lowercase().contains("getack") {
                p.commands_processed.push_back(msg_str);
            }
        }
    }

    pub async fn update_bytes_written(&mut self, sender: SocketAddr, bytes_written: usize) {
        for peer in self.peers.iter_mut() {
            let p = peer.1;
            if *peer.0 == sender {
                p.bytes_written
                    .store(bytes_written, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    pub async fn verify_propagation(&mut self, offset_len: usize) -> usize {
        let mut count: usize = 0;
        for peer in self.peers.iter_mut() {
            let p = peer.1;
            let bytes_sent = p.bytes_sent.load(Ordering::Relaxed) - offset_len;
            let bytes_written = p.bytes_written.load(Ordering::Relaxed);
            if bytes_sent == bytes_written {
                count += 1;
            }
        }
        count
    }

    pub async fn count_commands_processed(&self) -> usize {
        let mut count: usize = 0;
        for peer in self.peers.iter() {
            let p = peer.1;
            if !p.commands_processed.is_empty() {
                count += 1;
            }
        }
        count
    }
}
