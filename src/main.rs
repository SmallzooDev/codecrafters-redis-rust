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
mod client_manager;
mod redis_client;
mod event;
mod event_handler;
mod event_publisher;

use crate::command_parser::CommandParser;
use crate::config_handler::ConfigHandler;
use crate::event::RedisEvent;
use crate::event_handler::EventHandler;
use crate::event_publisher::EventPublisher;
use crate::state_manager::StateManager;
use crate::value_entry::ValueEntry;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let state = StateManager::new();

    let (tx, mut rx) = mpsc::channel::<RedisEvent>(32);
    let tx_clone = tx.clone();
    let publisher = EventPublisher::new(tx_clone.clone());

    let mut config_handler = ConfigHandler::new(
        state.get_db(),
        state.get_config(),
        state.get_replication_config(),
        publisher.clone(),
    );
    config_handler.load_config().await;
    config_handler.configure_db().await;

    let port = {
        let config_lock = state.get_config();
        let config = config_lock.read().await;
        config.get("port")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(6379)
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
    println!("Listening on port {}", port);

    let mut event_handler = EventHandler::new(
        state.get_db(),
        state.get_config(),
        state.get_replication_config(),
        publisher.clone(),
    );

    let event_handler_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            event_handler.handle_event(event).await;
        }
    });

    let accept_task = tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            //TODO : client_id 리팩토링
            let client_id = addr.port() as u64;
            let (mut read_stream, write_stream) = stream.into_split();

            let publisher = publisher.clone();
            if let Err(e) = publisher.publish_client_connected(client_id, write_stream, addr).await {
                eprintln!("Failed to send client connected event: {}", e);
                continue;
            }

            tokio::spawn(async move {
                // TODO: buffer 읽기가 끝나는 것을 보장하도록 수정
                let mut buffer = [0u8; 512];
                loop {
                    match read_stream.read(&mut buffer).await {
                        Ok(n) if n > 0 => {
                            let command = String::from_utf8_lossy(&buffer[..n]).to_string();
                            let parsed_command = CommandParser::parse_message(&command).unwrap();
                            if let Err(e) = publisher.publish_command(client_id, parsed_command).await {
                                eprintln!("Failed to publish command: {}", e);
                                break;
                            }
                        }
                        _ => break,
                    }
                }
            });
        }
    });

    config_handler.configure_replication().await;

    tokio::try_join!(event_handler_task, accept_task).unwrap();
}



