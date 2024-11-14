use crate::command_parser::parse_message;
use crate::env_parser::parse_env;
use crate::rdb_parser::run_rdb_handler;
use crate::{Config, Db};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

pub async fn handle_client(mut stream: TcpStream, db: Db, config: Config) {
    let mut buffer = [0; 512];
    loop {
        buffer.fill(0);
        match stream.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                let message = match std::str::from_utf8(&buffer[..n]) {
                    Ok(msg) => msg,
                    Err(_) => {
                        println!("Failed to parse message as UTF-8");
                        continue;
                    }
                };

                println!("Received message: {:?}", message);
                match parse_message(message) {
                    Ok(command) => {
                        if let Err(e) = command.handle_command(&mut stream, Arc::clone(&db), Arc::clone(&config)).await {
                            println!("Failed to send response: {}", e);
                        }
                    }
                    Err(e) => {
                        println!("Failed to parse command: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Error reading from stream: {}", e);
                break;
            }
        }
    }
}

pub async fn handle_env(args: Vec<String>, config: Config) -> Result<(), String> {
    if args.len() <= 1 {
        println!("No configuration arguments provided. Using default settings.");
        return Ok(());
    }

    let result = parse_env(args.clone())?;
    let mut config_guard = config.write().await;

    result.iter().for_each(|(key, value)| {
        config_guard.insert(key.clone(), value.clone());
    });

    println!("Environment configuration applied.");
    Ok(())
}

pub async fn handle_configure(db: Db, config: Config) {
    let mut config_read = config.read().await;

    let dir = config_read.get("dir").cloned().unwrap_or_else(|| "".to_string());
    let db_file_name = config_read.get("file_name").cloned().unwrap_or_else(|| "".to_string());
    let mut rdb_file_path = "".to_string();

    if !dir.is_empty() && !db_file_name.is_empty() {
        println!("Initiating Redis with Data File");
        rdb_file_path = format!("{}/{}", dir, db_file_name);
        if let Err(e) = run_rdb_handler(db.clone(), rdb_file_path).await {
            eprintln!("Error during run rdb_parser: {}", e);
        }
    }
}
