mod cli;
mod client_handler;
mod cmds;
mod connection;
mod db;
mod global;
mod parse;
mod resp;
mod token;

pub use cli::Cli;
use client_handler::handle_client;
pub use db::{load_from_rdb, ExpiringHashMap};
pub use global::CONFIG_LIST;
use tokio::net::TcpListener;

pub async fn start_server(
    bind_address: Option<String>,
    listening_port: Option<u16>,
    dir_name: Option<String>,
    dbfilename: Option<String>,
    replicaof: Option<String>,
) -> anyhow::Result<()> {
    // Start logging.
    femme::start();

    log::info!("initialising database files...");
    if bind_address.is_some() {
        CONFIG_LIST.push(("bind_address".to_string(), bind_address.clone().unwrap()));
    }

    if listening_port.is_some() {
        CONFIG_LIST.push((
            "listening_port".to_string(),
            listening_port.unwrap().to_string(),
        ));
    }

    if dir_name.is_some() {
        CONFIG_LIST.push(("dir".to_string(), dir_name.clone().unwrap()));
    }

    if dbfilename.is_some() {
        CONFIG_LIST.push(("dbfilename".to_string(), dbfilename.clone().unwrap()));
    }

    if replicaof.is_some() {
        CONFIG_LIST.push(("replicaof".to_string(), replicaof.clone().unwrap()));
    }

    // Create TCP Listener
    if bind_address.clone().is_some() && listening_port.is_some() {
        let listener_addr = format!("{}:{}", bind_address.unwrap(), listening_port.unwrap());
        let listener = TcpListener::bind(listener_addr.to_owned()).await?;
        log::info!("Redis running on {}...", listener_addr);

        // initialise the DB
        //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let db: ExpiringHashMap<String, String> = ExpiringHashMap::new();
        if dir_name.is_some() && dbfilename.is_some() {
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
    } else {
        panic!("Bind address and port cannot be empty!");
    }
}
