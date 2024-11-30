mod command;
mod value_entry;
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
mod event;
mod event_handler;

use crate::config_handler::ConfigHandler;
use crate::event::RedisEvent;
use crate::event_handler::EventHandler;
use crate::state_manager::StateManager;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

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
    let (tx, mut rx) = mpsc::channel::<RedisEvent>(32);

    let mut event_handler = EventHandler::new(
        state.get_db(),
        state.get_config(),
        state.get_replication_config(),
    );

    println!("Listening on port {}", port);

    let event_handler_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            event_handler.handle_event(event).await;
        }
    });

    let tx_clone = tx.clone();
    let accept_task = tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            let client_id = addr.port() as u64;
            if let Err(e) = tx_clone.send(RedisEvent::ClientConnected {
                client_id,
                stream,
                addr,
            }).await {
                eprintln!("Failed to send client connected event: {}", e);
            }
        }
    });

    tokio::try_join!(event_handler_task, accept_task).unwrap();
}



