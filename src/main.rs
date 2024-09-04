use client_handler::handle_client;
use database::{prune_database, ExpiringHashMap};
use std::collections::HashMap;
use std::io::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

mod client_handler;
mod command;
mod connection;
mod database;
mod parse;
mod resp;
mod token;

#[tokio::main]
pub async fn main() -> Result<()> {
    // Start logging.
    femme::start();

    // Create TCP Listener
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    log::info!("Listening on port 6379");

    // initialise the DB
    //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let db: ExpiringHashMap<String, String> = ExpiringHashMap::new();

    // Handle Multiple Clients in a loop
    loop {
        let (tcp_stream, socket_addr) = listener.accept().await?;
        log::info!("Accepted connection from {}", socket_addr.ip().to_string());
        let db = db.clone();

        tokio::spawn(handle_client(tcp_stream, socket_addr, db));
    }
}
