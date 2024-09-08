use crate::db::ExpiringHashMap;
use core::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};

use crate::connection::Connection;

pub async fn handle_client(
    mut tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    db: ExpiringHashMap<String, String>,
) -> anyhow::Result<()> {
    let mut conn = Connection::new(&mut tcp_stream, socket_addr);
    conn.apply(db).await;
    // while let Some(Ok(mut msg)) = stream.next().await {
    //     if msg.starts_with("/help") {
    //         sink.send(HELP_MSG).await?;
    //     } else if msg.starts_with("/quit") {
    //         break;
    //     } else {
    //         msg.push_str(" ❤️");
    //         sink.send(msg).await?;
    //     }
    // }
    Ok(())
}
