use crate::command::Command;
use crate::command_parser::CommandParser;
use crate::event_publisher::EventPublisher;
use crate::protocol_constants::*;
use crate::rdb_parser::RdbParser;
use crate::replication_config::ReplicationConfig;
use crate::util::construct_redis_command;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}};
use tokio::sync::RwLock;
use std::sync::Arc;

pub type Db = HashMap<String, ValueEntry>;
pub type Config = HashMap<String, String>;

pub struct ConfigHandler {
    db: Arc<RwLock<HashMap<String, ValueEntry>>>,
    config: Arc<RwLock<HashMap<String, String>>>,
    replication_config: Arc<RwLock<ReplicationConfig>>,
    publisher: EventPublisher,
}

impl ConfigHandler {
    pub fn new(
        db: Arc<RwLock<HashMap<String, ValueEntry>>>,
        config: Arc<RwLock<HashMap<String, String>>>,
        replication_config: Arc<RwLock<ReplicationConfig>>,
        publisher: EventPublisher,
    ) -> Self {
        Self { 
            db, 
            config, 
            replication_config,
            publisher,
        }
    }

    pub async fn load_config(&self) {
        let args: Vec<String> = env::args().collect();
        match ConfigHandler::parse_env(args) {
            Ok(result) => {
                let mut config = self.config.write().await;
                for (key, value) in result {
                    config.insert(key, value);
                }
                println!("Configuration loaded.");
            }
            Err(e) => {
                eprintln!("Failed to parse configuration: {}", e);
            }
        }
    }

    pub async fn configure_db(&mut self) {
        let dir = self.config.read().await.get("dir").cloned().unwrap_or_default();
        let db_file_name = self.config.read().await.get("file_name").cloned().unwrap_or_default();

        if !dir.is_empty() && !db_file_name.is_empty() {
            let rdb_file_path = format!("{}/{}", dir, db_file_name);
            let mut db_guard = self.db.write().await;
            if let Ok(mut parser) = RdbParser::new(&mut *db_guard, &rdb_file_path) {
                if let Err(e) = parser.parse().await {
                    eprintln!("Error during RDB parsing: {}", e);
                }
            }
        }
    }

    pub async fn configure_replication(&self) {
        let replica_of_host = self.config.read().await.get("replica_of_host").cloned().unwrap_or_default();
        let replica_of_port = self.config.read().await.get("replica_of_port").cloned().unwrap_or_default();

        if !replica_of_host.is_empty() && !replica_of_port.is_empty() {
            self.replication_config.write().await.set_replica_of(replica_of_host.clone(), replica_of_port.parse::<u16>().expect("none")).await;
            if let Err(e) = self.handshake_with_master(replica_of_host.clone(), replica_of_port.clone()).await {
                eprintln!("configure failure with : {}", e);
                return;
            }
        }
    }

    pub async fn get_port(&self) -> u16 {
        self.config.read().await.get("port")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(6379)
    }

    fn parse_env(args: Vec<String>) -> Result<Vec<(String, String)>, String> {
        if args.len() <= 1 {
            return Err("No configuration arguments provided to parse".into());
        }

        let mut result = Vec::new();
        let mut arg_index = 1;

        while arg_index < args.len() {
            match args[arg_index].as_str() {
                "--dir" => {
                    if arg_index + 1 < args.len() {
                        result.push(("dir".into(), args[arg_index + 1].clone()));
                        arg_index += 2;
                    } else {
                        return Err("Argument Error: --dir option requires an argument".into());
                    }
                }
                "--dbfilename" => {
                    if arg_index + 1 < args.len() {
                        result.push(("file_name".into(), args[arg_index + 1].clone()));
                        arg_index += 2;
                    } else {
                        return Err("Argument Error: --dbfilename option requires an argument".into());
                    }
                }
                "--port" => {
                    if arg_index + 1 < args.len() {
                        result.push(("port".into(), args[arg_index + 1].clone()));
                        arg_index += 2;
                    } else {
                        return Err("Argument Error: --port option requires an argument".into());
                    }
                }
                "--replicaof" => {
                    if arg_index + 1 < args.len() {
                        let replica_location = args[arg_index + 1].clone();
                        let replica_location_split: Vec<&str> = replica_location.split_whitespace().collect();

                        if replica_location_split.len() == 2 {
                            result.push(("replica_of_host".into(), replica_location_split[0].into()));
                            result.push(("replica_of_port".into(), replica_location_split[1].into()));
                            arg_index += 2;
                        } else {
                            return Err("Argument Error: --replicaof requires a host and port (e.g., 'localhost 6379')".into());
                        }
                    } else {
                        return Err("Argument Error: --replicaof requires a host and port (e.g., 'localhost 6379')".into());
                    }
                }
                _ => return Err(format!("Argument Error: '{}' is an unknown option", args[arg_index])),
            }
        }

        Ok(result)
    }

    pub async fn handshake_with_master(&self, master_host: String, master_port: String) -> Result<(), String> {
        let master_address = format!("{}:{}", master_host, master_port);
        let port = self.get_port().await;

        let stream = TcpStream::connect(&master_address).await.map_err(|e| format!("Failed to connect to master: {}", e))?;
        let (mut read_stream, mut write_stream) = stream.into_split();

        self.send_command_with_writer(&mut write_stream, &[PING_COMMAND]).await?;
        self.expect_pong_response(&mut read_stream).await?;

        self.send_command_with_writer(&mut write_stream, &[REPLCONF_COMMAND, "listening-port", &port.to_string()]).await?;
        self.expect_ok_response(&mut read_stream).await?;

        self.send_command_with_writer(&mut write_stream, &[REPLCONF_COMMAND, "capa", "psync2"]).await?;
        self.expect_ok_response(&mut read_stream).await?;

        self.send_command_with_writer(&mut write_stream, &[PSYNC_COMMAND, "?", "-1"]).await?;
        self.expect_fullresync_response(&mut read_stream).await?;

        let mut buffer = [0u8; 1024];
        let mut size_buffer = String::new();
        let mut byte = [0u8; 1];

        // $ 문자 읽기
        read_stream.read_exact(&mut byte).await.map_err(|e| format!("Failed to read RDB size prefix: {}", e))?;
        if byte[0] != b'$' {
            return Err("Expected $ prefix for RDB size".to_string());
        }

        // 크기 읽기
        loop {
            read_stream.read_exact(&mut byte).await.map_err(|e| format!("Failed to read RDB size: {}", e))?;
            if byte[0] == b'\r' {
                read_stream.read_exact(&mut byte).await.map_err(|e| format!("Failed to read RDB size: {}", e))?;
                if byte[0] == b'\n' {
                    break;
                }
            }
            size_buffer.push(byte[0] as char);
        }

        let rdb_size: usize = size_buffer.parse()
            .map_err(|e| format!("Failed to parse RDB size: {}", e))?;

        println!("Reading RDB file of size: {}", rdb_size);

        let mut rdb_data = vec![0u8; rdb_size];
        let mut total_bytes = 0;
        while total_bytes < rdb_size {
            let bytes_read = read_stream.read(&mut rdb_data[total_bytes..]).await
                .map_err(|e| format!("Failed to read RDB data: {}", e))?;
            if bytes_read == 0 {
                return Err("Unexpected EOF while reading RDB data".to_string());
            }
            total_bytes += bytes_read;
        }

        println!("Read {} bytes of RDB data", total_bytes);

        self.replication_config.write().await.set_replica_of(master_host.clone(), master_port.parse::<u16>().expect("none")).await;

        let publisher = self.publisher.clone();
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut temp_buffer = [0u8; 1024];
            
            loop {
                match read_stream.read(&mut temp_buffer).await {
                    Ok(n) if n > 0 => {
                        buffer.extend_from_slice(&temp_buffer[..n]);
                        
                        let mut pos = 0;
                        while pos < buffer.len() {
                            if buffer[pos] == b'*' {
                                let mut array_end = pos;
                                let mut elements = 0;
                                let mut expected_elements = 0;
                                let mut is_complete = false;

                                if let Some(size_end) = buffer[pos + 1..].iter().position(|&b| b == b'\r') {
                                    if let Ok(size) = String::from_utf8_lossy(&buffer[pos + 1..pos + 1 + size_end]).parse::<usize>() {
                                        expected_elements = size;
                                        array_end = pos + 1 + size_end + 2;

                                        while elements < expected_elements && array_end < buffer.len() {
                                            if buffer[array_end] != b'$' {
                                                break;
                                            }

                                            if let Some(len_end) = buffer[array_end + 1..].iter().position(|&b| b == b'\r') {
                                                if let Ok(len) = String::from_utf8_lossy(&buffer[array_end + 1..array_end + 1 + len_end]).parse::<usize>() {
                                                    array_end = array_end + 1 + len_end + 2 + len + 2;
                                                    elements += 1;

                                                    if elements == expected_elements && array_end <= buffer.len() {
                                                        is_complete = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                if is_complete {
                                    let command_data = buffer[pos..array_end].to_vec();
                                    if let Ok(command) = String::from_utf8(command_data) {
                                        if let Ok(parsed_command) = CommandParser::parse_message(&command) {
                                            if let Err(e) = publisher.publish_command(0, parsed_command).await {
                                                eprintln!("Failed to publish command from master: {}", e);
                                            }
                                        }
                                    }
                                    pos = array_end;
                                } else {
                                    break;
                                }
                            } else {
                                pos += 1;
                            }
                        }

                        if pos > 0 {
                            buffer = buffer[pos..].to_vec();
                        }
                    }
                    Ok(0) | Err(_) => break,
                    _ => continue,
                }
            }
        });

        Ok(())
    }

    async fn send_command_with_writer(&self, stream: &mut OwnedWriteHalf, args: &[&str]) -> Result<(), String> {
        let command = construct_redis_command(args);
        stream.write_all(command.as_bytes()).await.map_err(|e| format!("Failed to send command to master: {}", e))
    }

    async fn expect_pong_response(&self, stream: &mut OwnedReadHalf) -> Result<(), String> {
        let mut buffer = [0u8; 512];
        let bytes_read = stream.read(&mut buffer).await.map_err(|e| format!("Failed to read PONG response from master: {}", e))?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if response.contains(SIMPLE_STRING_PREFIX) && response.contains("PONG") {
            println!("Master responded with PONG");
            Ok(())
        } else {
            Err(format!("Unexpected response from master: {}", response))
        }
    }

    async fn expect_ok_response(&self, stream: &mut OwnedReadHalf) -> Result<(), String> {
        let mut buffer = [0u8; 512];
        let bytes_read = stream.read(&mut buffer).await.map_err(|e| format!("Failed to read OK response from master: {}", e))?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if response.contains(SIMPLE_STRING_PREFIX) && response.contains("OK") {
            println!("Master acknowledged command with OK");
            Ok(())
        } else {
            Err(format!("Unexpected response from master: {}", response))
        }
    }

    async fn expect_fullresync_response(&self, stream: &mut OwnedReadHalf) -> Result<(), String> {
        let mut buffer = [0u8; 512];
        let bytes_read = stream.read(&mut buffer).await.map_err(|e| format!("Failed to read FULLRESYNC response from master: {}", e))?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if response.contains(SIMPLE_STRING_PREFIX) && response.contains("FULLRESYNC") {
            println!("Master responded with FULLRESYNC");
            Ok(())
        } else {
            Err(format!("Unexpected response from master: {}", response))
        }
    }
}