use anyhow::Error;
use global::CONFIG_LIST;
use rand::{distributions::Alphanumeric, Rng};
use redis_starter_rust::start_server;
use redis_starter_rust::Cli;

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
    // tokio::spawn(start_server());

    // let replicaof = CONFIG_LIST.get_val(&"replicaof".to_string());
    // match replicaof {
    //     Some(val) => {
    //         // Follower
    //         let ip_and_port: Vec<&str> = val.split_whitespace().collect();
    //         if ip_and_port.len() > 2 {
    //             panic!("Wrong number of arguments in leader connection string");
    //         }
    //     }
    //     None => {
    //         // Leader
    //         // Set the replication_id and offset
    //         let master_replid: String = rand::thread_rng()
    //             .sample_iter(&Alphanumeric)
    //             .take(40) // 40 character long
    //             .map(char::from) // `u8` values to `char`
    //             .collect();

    //         CONFIG_LIST.push(("master_replid".into(), master_replid));

    //         let master_repl_offset: u64 = 0;
    //         CONFIG_LIST.push(("master_repl_offset".into(), master_repl_offset.to_string()));
    //     }
    // }

    Ok(())
}
