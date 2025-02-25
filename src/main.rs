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
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use std::process;

#[tokio::main]
async fn main() {
    let mut state = StateManager::new();

    let (tx, mut rx) = mpsc::channel::<RedisEvent>(32);
    let tx_clone = tx.clone();
    let publisher = EventPublisher::new(tx_clone.clone());

    let mut config_handler = ConfigHandler::new(
        state.take_db(),
        state.take_config(),
        state.take_replication_config(),
        publisher.clone(),
    );
    config_handler.load_config();
    config_handler.configure_db().await;

    println!("Configuring replication...");
    config_handler.configure_replication().await;

    println!("Current config before restore:");
    for (key, value) in config_handler.get_config() {
        println!("  {} = {}", key, value);
    }

    state.restore_db(config_handler.take_db());
    state.restore_config(config_handler.take_config());
    state.restore_replication_config(config_handler.take_replication_config());

    println!("State config after restore:");
    if let Some(config) = state.get_config() {
        for (key, value) in config {
            println!("  {} = {}", key, value);
        }
    } else {
        println!("  No config found in state!");
    }

    let port = state.get_config_value("port")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6379);

    println!("Selected port for binding: {}", port);

    let bind_addr = format!("127.0.0.1:{}", port);
    println!("Attempting to bind to {}", bind_addr);
    
    let listener = match TcpListener::bind(&bind_addr).await {
        Ok(listener) => {
            println!("Successfully listening on port {}", port);
            listener
        }
        Err(e) => {
            eprintln!("Failed to bind to {}: {}", bind_addr, e);
            eprintln!("Failed to bind to port {}: {}", port, e);
            process::exit(1);
        }
    };

    let mut event_handler = EventHandler::new(
        state.take_db(),
        state.take_config(),
        state.take_replication_config(),
        publisher.clone(),
    );

    let event_handler_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            event_handler.handle_event(event).await;
        }
    });

    let accept_task = tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            let client_id = addr.port() as u64;
            let (mut read_stream, write_stream) = stream.into_split();

            let publisher = publisher.clone();
            if let Err(e) = publisher.publish_client_connected(client_id, write_stream, addr).await {
                eprintln!("Failed to send client connected event: {}", e);
                continue;
            }

            tokio::spawn(async move {
                let mut buffer = [0u8; 512];
                loop {
                    match read_stream.read(&mut buffer).await {
                        Ok(n) if n > 0 => {
                            let command = String::from_utf8_lossy(&buffer[..n]).to_string();
                            let parsed_command = match CommandParser::parse_message(&command) {
                                Ok(cmd) => cmd,
                                Err(e) => {
                                    eprintln!("Failed to parse command: {}", e);
                                    continue;
                                }
                            };
                            if let Err(e) = publisher.publish_command(client_id, parsed_command).await {
                                eprintln!("Failed to publish command: {}", e);
                                break;
                            }
                        }
                        Ok(_) => break,
                        Err(e) => {
                            eprintln!("Failed to read from client: {}", e);
                            break;
                        }
                    }
                }
            });
        }
    });

    if let Err(e) = tokio::try_join!(event_handler_task, accept_task) {
        eprintln!("Error in tasks: {}", e);
        process::exit(1);
    }
}



