mod command;
mod value_entry;
mod handler;
mod command_parser;
mod errors;
mod protocol_constants;
mod rdb_parser;
mod state_manager;
mod config_handler;
mod replication_config;
mod util;
mod ClientManager;
mod Client;

use crate::config_handler::ConfigHandler;
use crate::handler::handle_client;
use crate::state_manager::StateManager;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task;

type Db = Arc<RwLock<HashMap<String, ValueEntry>>>;
type Config = Arc<RwLock<HashMap<String, String>>>;


#[tokio::main]
async fn main() {
    let state = StateManager::new();
    let config_handler = ConfigHandler::new(state.get_db(), state.get_config(), state.get_replication_config());

    config_handler.load_config().await;
    config_handler.configure_db().await;
    config_handler.configure_replication().await;

    let port = config_handler.get_port().await;
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();

    println!("Listening on port {}", port);
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let db = state.get_db();
                let config = state.get_config();
                let replication_config = state.get_replication_config();
                task::spawn(async move {
                    handle_client(stream, db, config, replication_config).await;
                });
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}



