use regex::Regex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

enum Command {
    PING,
    ECHO(String),
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                task::spawn(async move {
                    handle_client(stream).await;
                });
            }
            Err(e) => {
                println!("Error accepting connection : {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream) {
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
                        if let Err(e) = handle_command(command, &mut stream).await {
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
fn parse_message(message: &str) -> Result<Command, String> {
    let re_ping = Regex::new(r"^\*1\r\n\$4\r\nPING\r\n$").unwrap();
    let re_echo = Regex::new(r"^\*2\r\n\$4\r\nECHO\r\n\$(\d+)\r\n(.+)\r\n$").unwrap();

    if re_ping.is_match(message) {
        Ok(Command::PING)
    } else if let Some(captures) = re_echo.captures(message) {
        let length: usize = captures[1].parse().unwrap_or(0);
        let echo_message = &captures[2];

        if echo_message.len() == length {
            Ok(Command::ECHO(echo_message.to_string()))
        } else {
            Err("Invalid ECHO command format: length mismatch".to_string())
        }
    } else {
        Err("Unknown command".to_string())
    }
}

async fn handle_command(command: Command, stream: &mut TcpStream) -> std::io::Result<()> {
    match command {
        Command::PING => {
            stream.write_all(b"+PONG\r\n").await?;
        }
        Command::ECHO(echo_message) => {
            let response_message = format!("${}\r\n{}\r\n", echo_message.len(), echo_message);
            stream.write_all(response_message.as_bytes()).await?;
        }
    }
    Ok(())
}
