use crate::command_parser::parse_message;
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

    let mut dir: Option<String> = None;
    let mut file_name: Option<String> = None;
    let mut arg_index = 1;

    while arg_index < args.len() {
        match args[arg_index].as_str() {
            "--dir" => {
                if arg_index + 1 < args.len() {
                    dir = Some(args[arg_index + 1].clone());
                    arg_index += 2;
                } else {
                    return Err("Argument Error: --dir option requires an argument".into());
                }
            }
            "--dbfilename" => {
                if arg_index + 1 < args.len() {
                    file_name = Some(args[arg_index + 1].clone());
                    arg_index += 2;
                } else {
                    return Err("Argument Error: --dbfilename option requires an argument".into());
                }
            }
            _ => return Err(format!("Argument Error: '{}' is an unknown option", args[arg_index])),
        }
    }

    match (dir, file_name) {
        (Some(dir), Some(path_name)) => {
            let mut config_guard = config.write().await;
            config_guard.insert("dir".to_string(), dir);
            config_guard.insert("dbfilename".to_string(), path_name);
            println!("Environment configuration applied.");
        }
        (None, None) => {
            println!("No configuration arguments provided. Using default settings.");
        }
        _ => {
            return Err("Argument Error: Both --dir and --dbfilename must be provided together.".into());
        }
    }

    Ok(())
}
