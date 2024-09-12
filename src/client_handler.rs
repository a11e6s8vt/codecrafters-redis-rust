use crate::db::ExpiringHashMap;
use anyhow::Error;
use core::net::SocketAddr;
use tokio::net::TcpStream;

use crate::connection::Connection;

pub async fn handle_client(
    mut tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    db: ExpiringHashMap<String, String>,
) -> anyhow::Result<(), Error> {
    let mut conn = Connection::new(&mut tcp_stream, socket_addr);
    let _ = conn.handle(db).await;
    Ok(())
}
