use anyhow::Error;
use cli::Cli;
use redis_starter_rust::start_server;

mod cli;
mod client_handler;
mod cmds;
mod connection;
mod db;
mod global;
mod parse;
mod resp;
mod token;

#[tokio::main]
pub async fn main() -> anyhow::Result<(), Error> {
    let config_params = Cli::new(std::env::args());

    dbg!(config_params.clone());

    let bind_address = config_params.bind_address;
    let listening_port = config_params.listening_port;
    let dir_name = config_params.dir_name;
    let dbfilename = config_params.db_filename;
    let replicaof = config_params.replicaof;
    start_server(
        bind_address,
        listening_port,
        dir_name,
        dbfilename,
        replicaof,
    )
    .await?;
    Ok(())
}
