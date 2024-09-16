use std::collections::HashMap;
use std::net::SocketAddr;

pub use kv::KeyValueStore;
pub use rdb::{load_from_rdb, write_to_disk};
use tokio::sync::mpsc;

mod kv;
mod rdb;
type Tx = mpsc::UnboundedSender<Vec<u8>>;
type Rx = mpsc::UnboundedReceiver<Vec<u8>>;

pub struct Shared {
    pub peers: HashMap<SocketAddr, Tx>,
}

impl Shared {
    pub fn new() -> Self {
        Shared {
            peers: HashMap::new(),
        }
    }

    pub async fn broadcast(&mut self, sender: SocketAddr, message: Vec<u8>) {
        for peer in self.peers.iter_mut() {
            let _ = peer.1.send(message.clone());
        }
    }
}
