use cli::Cli;
use client_handler::handle_client;
use db::{load_from_rdb, ExpiringHashMap};
use global::CONFIG_LIST;
use std::io::Result;
use tokio::net::TcpListener;

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
pub async fn main() -> Result<()> {
    // Start logging.
    femme::start();

    log::info!("initialising database files...");
    let config_params = Cli::new(std::env::args());
    CONFIG_LIST.push((
        "bind_address".to_string(),
        config_params.bind_address.to_string(),
    ));
    CONFIG_LIST.push((
        "listening_port".to_string(),
        config_params.listening_port.to_string(),
    ));
    CONFIG_LIST.push(("dir".to_string(), config_params.dir_name.clone()));
    CONFIG_LIST.push(("dbfilename".to_string(), config_params.db_filename.clone()));

    // Create TCP Listener
    let ip_address = format!(
        "{}:{}",
        config_params.bind_address, config_params.listening_port
    );
    let listener = TcpListener::bind(ip_address).await?;
    log::info!("Listening on port 6379");

    // initialise the DB
    //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let db: ExpiringHashMap<String, String> = ExpiringHashMap::new();
    if !(config_params.dir_name.is_empty() && config_params.db_filename.is_empty()) {
        load_from_rdb(db.clone())
            .await
            .expect("RDB file read failed");
    }
    // Handle Multiple Clients in a loop
    loop {
        let (tcp_stream, socket_addr) = listener.accept().await?;
        log::info!("Accepted connection from {}", socket_addr.ip().to_string());
        let db = db.clone();

        tokio::spawn(handle_client(tcp_stream, socket_addr, db));
    }
}
