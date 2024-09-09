use cli::Cli;
use client_handler::handle_client;
use db::ExpiringHashMap;
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
    CONFIG_LIST.push(("dir".to_string(), config_params.dir_name));
    CONFIG_LIST.push(("dbfilename".to_string(), config_params.db_filename));

    // Create TCP Listener
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    log::info!("Listening on port 6379");

    // initialise the DB
    //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let db: ExpiringHashMap<String, String> = ExpiringHashMap::new();
    db::load_from_rdb(db.clone())
        .await
        .expect("RDB file read failed");
    // Handle Multiple Clients in a loop
    loop {
        let (tcp_stream, socket_addr) = listener.accept().await?;
        log::info!("Accepted connection from {}", socket_addr.ip().to_string());
        let db = db.clone();

        tokio::spawn(handle_client(tcp_stream, socket_addr, db));
    }
}
