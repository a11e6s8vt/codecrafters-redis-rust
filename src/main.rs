use client_handler::handle_client;
use std::io::Result;
use tokio::io::BufReader;
use tokio::net::TcpListener;

mod client_handler;

#[tokio::main]
pub async fn main() -> Result<()> {
    // Start logging.
    femme::start();

    // Create TCP Listener
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    log::info!("Listening on port 6379");

    // Handle Multiple Clients in a loop
    loop {
        let (tcp_stream, socket_addr) = listener.accept().await?;
        log::info!("Accepted connection from {}", socket_addr.ip().to_string());
        tokio::spawn(async move {
            match handle_client(BufReader::new(tcp_stream), socket_addr).await {
                Ok(ip) => {
                    log::info!("{} handle_client terminated gracefully", ip)
                }
                Err(error) => log::error!("handle_client encountered error: {}", error),
            }
        });
    }
}
