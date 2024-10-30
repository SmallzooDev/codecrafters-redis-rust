use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

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
                        println!("Failed to parse message");
                        continue;
                    }
                };

                if message.starts_with("*1\r\n$4\r\nPING\r\n") {
                    stream.write_all(b"+PONG\r\n").await.unwrap();
                }
            },
            Err(_) => break,
        }
    }
}