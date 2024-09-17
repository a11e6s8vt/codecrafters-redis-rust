use anyhow::Error;
use redis_starter_rust::start_server;
use redis_starter_rust::Cli;
use redis_starter_rust::Request;

mod cli;
mod cmds;
mod connection;
mod database;
mod global;
mod parse;
mod resp;

#[tokio::main]
pub async fn main() -> anyhow::Result<(), Error> {
    let config_params = Cli::new(std::env::args());
    let bind_address = config_params.bind_address.clone();
    let listening_port = config_params.listening_port;
    let dir_name = config_params.dir_name.clone();
    let dbfilename = config_params.db_filename.clone();
    let replicaof = config_params.replicaof.clone();

    let _ = start_server(
        bind_address,
        listening_port,
        dir_name,
        dbfilename,
        replicaof,
    )
    .await;
    Ok(())
}
