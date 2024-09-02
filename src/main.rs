use bytes::Bytes;
use client_handler::handle_client;
use connection::Connection;
use std::collections::HashMap;
use std::io::Result;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

mod client_handler;
mod command;
mod connection;
mod execute;
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
    let db: Arc<Mutex<HashMap<String, Bytes>>> = Arc::new(Mutex::new(HashMap::new()));

    // Handle Multiple Clients in a loop
    loop {
        let (mut tcp_stream, socket_addr) = listener.accept().await?;
        log::info!("Accepted connection from {}", socket_addr.ip().to_string());
        let db = Arc::clone(&db);

        tokio::spawn(async move {
            let mut conn = Connection::new(&mut tcp_stream, socket_addr);
            handle_client(&mut conn, db).await;
        });
    }
}
