use std::io::Result;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
pub async fn main() -> Result<()> {
    // Start logging.
    femme::start();

    // Create TCP Listener
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    // Handle Multiple Clients in a loop
    loop {
        let (tcp_stream, socket_addr) = listener.accept().await?;
        tokio::spawn(async move {
            match handle_client(tcp_stream, socket_addr).await {
                Ok(ip) => {
                    log::info!("{} handle_client terminated gracefully", ip)
                }
                Err(error) => log::error!("handle_client encountered error: {}", error),
            }
        });
    }
}

async fn handle_client(mut stream: TcpStream, socket_addr: SocketAddr) -> Result<String> {
    let mut buffer = [0; 1024];
    loop {
        let num_bytes = stream.read(&mut buffer).await?;
        let response = "+PONG\r\n";

        stream.write_all(response.as_bytes()).await?;
        if num_bytes == 0 {
            break;
        }
    }
    Ok(socket_addr.ip().to_string())
}
