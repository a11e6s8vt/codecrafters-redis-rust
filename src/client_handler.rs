use crate::command::{Command, CommandError};
use core::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::connection::Connection;
use crate::execute::execute_command;
use crate::resp::RespData;

pub async fn handle_client(
    mut tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    db: Arc<Mutex<HashMap<String, String>>>,
) -> anyhow::Result<()> {
    let mut conn = Connection::new(&mut tcp_stream, socket_addr);
    conn.apply(db.clone()).await;
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
