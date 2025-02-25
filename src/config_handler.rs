use crate::command_parser::CommandParser;
use crate::event_publisher::EventPublisher;
use crate::protocol_constants::*;
use crate::rdb_parser::RdbParser;
use crate::replication_config::ReplicationConfig;
use crate::util::construct_redis_command;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::env;
use std::io::Cursor;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use std::mem;

pub struct ConfigHandler {
    db: HashMap<String, ValueEntry>,
    config: HashMap<String, String>,
    replication_config: ReplicationConfig,
    publisher: EventPublisher,
}

impl ConfigHandler {
    pub fn new(
        db: HashMap<String, ValueEntry>,
        config: HashMap<String, String>,
        replication_config: ReplicationConfig,
        publisher: EventPublisher,
    ) -> Self {
        Self {
            db,
            config,
            replication_config,
            publisher,
        }
    }

    pub fn load_config(&mut self) {
        let args: Vec<String> = env::args().collect();
        match ConfigHandler::parse_env(args) {
            Ok(result) => {
                for (key, value) in result {
                    self.config.insert(key, value);
                }
                println!("Configuration loaded.");
            }
            Err(e) => {
                eprintln!("Failed to parse configuration: {}", e);
            }
        }
    }

    pub async fn configure_db(&mut self) {
        let dir = self.config.get("dir").cloned().unwrap_or_default();
        let db_file_name = self.config.get("file_name").cloned().unwrap_or_default();

        if !dir.is_empty() && !db_file_name.is_empty() {
            let rdb_file_path = format!("{}/{}", dir, db_file_name);

            match File::open(&rdb_file_path).await {
                Ok(file) => {
                    let reader = BufReader::new(file);
                    let parser = RdbParser::new(reader, mem::take(&mut self.db));

                    match parser.parse().await {
                        Ok(parsed_db) => {
                            self.db = parsed_db;
                        }
                        Err(e) => {
                            eprintln!("Error during RDB parsing: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to open Rdb file: {}", e);
                }
            }
        }
    }

    pub async fn configure_replication(&mut self) {
        let replica_of_host = self.config.get("replica_of_host").cloned().unwrap_or_default();
        let replica_of_port = self.config.get("replica_of_port").cloned().unwrap_or_default();

        if !replica_of_host.is_empty() && !replica_of_port.is_empty() {
            println!("Starting replication configuration...");
            println!("Configuring as replica of {}:{}", replica_of_host, replica_of_port);
            
            let port = match replica_of_port.parse::<u16>() {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Invalid master port {}: {}", replica_of_port, e);
                    return;
                }
            };

            println!("Setting replica configuration...");
            self.replication_config.set_replica_of(replica_of_host.clone(), port);
            
            println!("Attempting to connect to master...");
            match self.handshake_with_master(replica_of_host.clone(), replica_of_port.clone()).await {
                Ok(_) => println!("Successfully connected to master and completed handshake"),
                Err(e) => {
                    eprintln!("Failed to connect to master: {}", e);
                    println!("Promoting to master mode due to connection failure");
                    self.replication_config.promote_to_master();
                }
            }
        } else {
            println!("No replication configuration found, running in standalone mode");
        }
    }

    pub async fn get_port(&self) -> u16 {
        self.config
            .get("port")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(6379)
    }

    fn parse_env(args: Vec<String>) -> Result<Vec<(String, String)>, String> {
        println!("Received arguments:");
        for (i, arg) in args.iter().enumerate() {
            println!("  arg[{}] = {:?}", i, arg);
        }

        let mut result = Vec::new();
        let mut arg_index = 1;

        while arg_index < args.len() {
            println!("Processing argument at index {}: {:?}", arg_index, args[arg_index]);
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
                        println!("Found --replicaof option");
                        println!("  Next argument: {:?}", args.get(arg_index + 1));
                        println!("  Following argument: {:?}", args.get(arg_index + 2));
                        
                        let host_port = args[arg_index + 1].trim_matches('"').split_whitespace().collect::<Vec<_>>();
                        println!("  Split host_port: {:?}", host_port);
                        
                        if host_port.len() == 2 {
                            result.push(("replica_of_host".into(), host_port[0].to_string()));
                            result.push(("replica_of_port".into(), host_port[1].to_string()));
                            arg_index += 2;
                        } else {
                            return Err("Argument Error: --replicaof option requires host and port".into());
                        }
                    } else {
                        return Err("Argument Error: --replicaof option requires two arguments".into());
                    }
                }
                _ => {
                    println!("Skipping unknown argument: {:?}", args[arg_index]);
                    arg_index += 1;
                }
            }
        }

        println!("Parsed configuration:");
        for (key, value) in &result {
            println!("  {} = {}", key, value);
        }

        Ok(result)
    }

    async fn handshake_with_master(
        &mut self,
        master_host: String,
        master_port: String,
    ) -> Result<(), String> {
        println!("Initiating handshake with master at {}:{}", master_host, master_port);
        
        let addr = format!("{}:{}", master_host, master_port);
        println!("Connecting to master at {}", addr);
        
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Failed to connect to master: {}", e))?;

        println!("Successfully established TCP connection to master");
        let (mut read_stream, mut write_stream) = stream.into_split();

        // Send initial PING
        println!("Sending initial PING to master");
        self.send_command_with_writer(&mut write_stream, &["PING"])
            .await?;

        // Wait for PONG response
        println!("Waiting for PONG response from master");
        let mut buffer = [0u8; 512];
        let bytes_read = read_stream
            .read(&mut buffer)
            .await
            .map_err(|e| format!("Failed to read PONG response: {}", e))?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if !response.contains("+PONG") {
            return Err(format!("Unexpected response to PING: {}", response));
        }
        println!("Received PONG from master");

        // Send REPLCONF listening-port
        let local_port = self.config.get("port")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(6379)
            .to_string();
        println!("Sending REPLCONF listening-port {}", local_port);
        self.send_command_with_writer(&mut write_stream, &["REPLCONF", "listening-port", &local_port])
            .await?;

        // Wait for OK response
        println!("Waiting for OK response from master");
        let bytes_read = read_stream
            .read(&mut buffer)
            .await
            .map_err(|e| format!("Failed to read OK response: {}", e))?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if !response.contains("+OK") {
            return Err(format!("Unexpected response to REPLCONF: {}", response));
        }
        println!("Received OK from master");

        println!("Sending PSYNC command to master");
        self.send_command_with_writer(&mut write_stream, &[PSYNC_COMMAND, "?", "-1"])
            .await?;

        println!("Waiting for FULLRESYNC response from master");
        self.expect_fullresync_response(&mut read_stream).await?;

        println!("Starting RDB transfer process");
        let mut size_str = String::new();
        let mut reading_size = true;
        let mut rdb_size = 0;

        while reading_size {
            let mut byte = [0u8; 1];
            read_stream
                .read_exact(&mut byte)
                .await
                .map_err(|e| format!("Failed to read RDB size byte: {}", e))?;

            match byte[0] {
                b'$' => {
                    println!("Found RDB size marker");
                    continue;
                }
                b'\r' => {
                    let mut lf = [0u8; 1];
                    read_stream
                        .read_exact(&mut lf)
                        .await
                        .map_err(|e| format!("Failed to read LF after CR: {}", e))?;

                    if lf[0] == b'\n' {
                        rdb_size = size_str
                            .parse::<usize>()
                            .map_err(|e| format!("Failed to parse RDB size: {}", e))?;
                        println!("RDB size determined: {} bytes", rdb_size);
                        reading_size = false;
                    } else {
                        return Err("Invalid RDB size format".to_string());
                    }
                }
                _ => size_str.push(byte[0] as char),
            }
        }

        println!("Reading RDB data...");
        let mut rdb_buffer = vec![0u8; rdb_size];
        read_stream
            .read_exact(&mut rdb_buffer)
            .await
            .map_err(|e| format!("Failed to read RDB data: {}", e))?;

        println!("Parsing RDB data...");
        {
            let cursor = Cursor::new(rdb_buffer);
            let reader = tokio::io::BufReader::new(cursor);
            let parser = RdbParser::new(reader, mem::take(&mut self.db));

            match parser.parse().await {
                Ok(parsed_db) => {
                    println!("Successfully parsed RDB data");
                    self.db = parsed_db;
                }
                Err(e) => {
                    return Err(format!("Failed to parse RDB data: {}", e));
                }
            }
        }

        println!("Sending ACK to master");
        if let Err(e) = self.respond_with_ack(&mut write_stream).await {
            return Err(format!("Failed to send ACK response: {}", e));
        }

        println!("Starting command processing loop");
        let publisher = self.publisher.clone();
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut temp_buffer = [0u8; 1024];

            loop {
                match read_stream.read(&mut temp_buffer).await {
                    Ok(n) if n > 0 => {
                        buffer.extend_from_slice(&temp_buffer[..n]);

                        while let Some(command_end) = buffer.windows(2).position(|w| w == b"\r\n") {
                            let command_bytes = buffer[..command_end + 2].to_vec();
                            buffer = buffer[command_end + 2..].to_vec();

                            let command = String::from_utf8_lossy(&command_bytes).to_string();
                            println!("Received command from master: {}", command);
                            if let Ok(parsed_command) = CommandParser::parse_message(&command) {
                                if let Err(e) = publisher.publish_command(0, parsed_command).await {
                                    eprintln!("Failed to publish command from master: {}", e);
                                    return;
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        println!("Master connection closed gracefully");
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error reading from master: {}", e);
                        break;
                    }
                }
            }
            println!("Command processing loop ended");
        });

        println!("Handshake completed successfully");
        Ok(())
    }

    async fn send_command_with_writer(
        &self,
        writer: &mut OwnedWriteHalf,
        args: &[&str],
    ) -> Result<(), String> {
        let command = construct_redis_command(args);
        writer
            .write_all(command.as_bytes())
            .await
            .map_err(|e| format!("Failed to send command: {}", e))
    }

    async fn expect_fullresync_response(&self, stream: &mut OwnedReadHalf) -> Result<(), String> {
        let mut buffer = [0u8; 512];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .map_err(|e| format!("Failed to read FULLRESYNC response from master: {}", e))?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if response.contains(SIMPLE_STRING_PREFIX) && response.contains("FULLRESYNC") {
            println!("Master responded with FULLRESYNC");
            Ok(())
        } else {
            Err(format!("Unexpected response from master: {}", response))
        }
    }

    async fn respond_with_ack(&self, write_stream: &mut OwnedWriteHalf) -> Result<(), String> {
        let ack_response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
        write_stream
            .write_all(ack_response.as_bytes())
            .await
            .map_err(|e| format!("Failed to send ACK: {}", e))
    }

    pub fn take_db(&mut self) -> HashMap<String, ValueEntry> {
        mem::take(&mut self.db)
    }

    pub fn take_config(&mut self) -> HashMap<String, String> {
        mem::take(&mut self.config)
    }

    pub fn take_replication_config(&mut self) -> ReplicationConfig {
        mem::take(&mut self.replication_config)
    }

    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
    }
}
