mod command;
mod value_entry;
mod handler;
mod command_parser;
mod errors;
mod protocol_constants;
mod rdb_parser;
mod env_parser;

use crate::handler::{handle_client, handle_configure, handle_env};
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task;

type Db = Arc<RwLock<HashMap<String, ValueEntry>>>;
type Config = Arc<RwLock<HashMap<String, String>>>;


#[tokio::main]
async fn main() {
    let db = Arc::new(RwLock::new(HashMap::new()));
    let config = Arc::new(RwLock::new(HashMap::new()));
    let args: Vec<String> = env::args().collect();

    if let Err(e) = handle_env(args.clone(), Arc::clone(&config)).await {
        eprintln!("Failed to handle environment configuration: {}", e);
        return;
    }

    handle_configure(db.clone(), config.clone()).await;

    let config_read = config.read().await;
    let port = config_read.get("port").cloned().unwrap_or_else(|| "6379".to_string());

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();

    println!("Listening on port {}", port);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let db_clone = Arc::clone(&db);
                let config_clone = Arc::clone(&config);
                task::spawn(async move {
                    handle_client(stream, db_clone, config_clone).await;
                });
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}



