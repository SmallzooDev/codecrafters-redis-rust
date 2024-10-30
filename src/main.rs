use std::io::{Read, Write};
use std::net::TcpListener;
use std::str::from_utf8;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buffer = [0; 512];

                loop {
                    buffer.fill(0);
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(_) => {
                            let message = from_utf8(&buffer).unwrap_or("");

                            if message.starts_with("*1\r\n$4\r\nPING\r\n") {
                                stream.write_all(b"+PONG\r\n").unwrap();
                            }
                        },
                        Err(_) => break,
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
