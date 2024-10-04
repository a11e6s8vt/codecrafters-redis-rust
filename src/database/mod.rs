use std::collections::VecDeque;
use std::net::SocketAddr;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;

use anyhow::Result;

pub use kv::KeyValueStore;
pub use rdb::{load_from_rdb, write_to_disk};
pub use stream::{RadixTreeStore, StreamEntry};
use tokio::sync::{mpsc, RwLock};

use crate::cmds::Command;

mod kv;
mod rdb;
mod stream;

type Tx = mpsc::UnboundedSender<Vec<u8>>;
type _Rx = mpsc::UnboundedReceiver<Vec<u8>>;

pub struct Peer {
    pub sender: Tx,
    pub bytes_sent: AtomicUsize,
    pub bytes_written: AtomicUsize,
    // Stores last 10 commands sent to replica excluding `REPLCONF GETACK *`
    pub commands_processed: VecDeque<String>,
}

#[derive(Debug, Default)]
pub struct Client {
    pub multi_lock: AtomicBool,
    pub multi_queue: Arc<Mutex<VecDeque<Command>>>,
}

pub struct SharedState {
    // a connection may be either a client or a replica (follower)
    pub peers: Arc<RwLock<HashMap<SocketAddr, Peer>>>,
    pub clients: Arc<RwLock<HashMap<SocketAddr, Client>>>,
    pub stream_store: RadixTreeStore,
    pub kv_store: KeyValueStore<String, String>,
}

impl SharedState {
    pub fn new() -> Self {
        SharedState {
            peers: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            stream_store: RadixTreeStore::new(),
            kv_store: KeyValueStore::new(),
        }
    }

    /// insert into the stream store
    pub async fn stream_store_insert(
        &self,
        key: &str,
        entry_id: &str,
        data: Vec<(String, String)>,
    ) -> Result<String> {
        self.stream_store.insert(key, entry_id, data).await
    }

    /// Insert into the kv_store
    pub async fn kv_store_insert(&self, k: String, v: String, expiry: Option<Duration>) {
        self.kv_store.insert(k, v, expiry).await;
    }

    pub async fn kv_store_get(&self, k: &String) -> Option<String> {
        self.kv_store.get(k).await
    }

    /// Insert a new peer
    pub async fn insert_peer(&self, socket_addr: SocketAddr, peer: Peer) {
        self.peers.write().await.entry(socket_addr).or_insert(peer);
    }

    pub async fn insert_client(&self, socket_addr: SocketAddr, client: Client) {
        self.clients
            .write()
            .await
            .entry(socket_addr)
            .or_insert(client);
    }

    pub async fn broadcast_peers(&self, message: Vec<u8>) {
        let mut peers = self.peers.write().await;
        for peer in peers.iter_mut() {
            let p = peer.1;
            let _ = p.sender.send(message.clone());
            p.bytes_sent
                .fetch_add(message.len(), std::sync::atomic::Ordering::Relaxed);
            let msg_str = String::from_utf8_lossy(&message).to_string();
            if !msg_str.to_ascii_lowercase().contains("getack") {
                p.commands_processed.push_back(msg_str);
            }
        }
        drop(peers);
    }

    pub async fn update_peers_bytes_written(&self, sender: SocketAddr, bytes_written: usize) {
        let mut peers = self.peers.write().await;
        for peer in peers.iter_mut() {
            let p = peer.1;
            if *peer.0 == sender {
                p.bytes_written
                    .store(bytes_written, std::sync::atomic::Ordering::Relaxed);
            }
        }
        drop(peers);
    }

    pub async fn verify_peers_propagation(&self, offset_len: usize) -> usize {
        let mut count: usize = 0;
        let mut peers = self.peers.write().await;
        for peer in peers.iter_mut() {
            let p = peer.1;
            let bytes_sent = p.bytes_sent.load(Ordering::Relaxed) - offset_len;
            let bytes_written = p.bytes_written.load(Ordering::Relaxed);
            if bytes_sent == bytes_written {
                count += 1;
            }
        }
        drop(peers);
        count
    }

    pub async fn count_peers_commands_processed(&self) -> usize {
        let mut count: usize = 0;
        let peers = self.peers.read().await;
        for peer in peers.iter() {
            let p = peer.1;
            if !p.commands_processed.is_empty() {
                count += 1;
            }
        }
        drop(peers);
        count
    }
}
