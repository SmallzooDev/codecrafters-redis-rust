mod command;
mod value_entry;
mod handler;
mod command_parser;
mod errors;
mod protocol_constants;
mod rdb_parser;
mod debug_handler;

use crate::debug_handler::debug_file_structure;
use crate::handler::{handle_client, handle_env};
use crate::rdb_parser::run;
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
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(RwLock::new(HashMap::new()));
    let config = Arc::new(RwLock::new(HashMap::new()));
    let args: Vec<String> = env::args().collect();

    if let Err(e) = handle_env(args.clone(), Arc::clone(&config)).await {
        eprintln!("Failed to handle environment configuration: {}", e);
        return;
    }

    // todo: add args for debug flag and exec below line optionally
    debug_file_structure(config.clone()).await;
    run(db.clone(), config.clone()).await;

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
                println!("Error accepting connection : {}", e);
            }
        }
    }
}



