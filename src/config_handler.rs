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
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio::sync::RwLock;

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
        let dir = self
            .config
            .read()
            .await
            .get("dir")
            .cloned()
            .unwrap_or_default();
        let db_file_name = self
            .config
            .read()
            .await
            .get("file_name")
            .cloned()
            .unwrap_or_default();

        if !dir.is_empty() && !db_file_name.is_empty() {
            let rdb_file_path = format!("{}/{}", dir, db_file_name);

            match File::open(&rdb_file_path).await {
                Ok(file) => {
                    let reader = BufReader::new(file);
                    let mut db_guard = self.db.write().await;
                    let mut parser = RdbParser::new(reader, &mut *db_guard);

                    if let Err(e) = parser.parse().await {
                        eprintln!("Error during RDB parsing: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to open Rdb file: {}", e);
                }
            }
        }
    }

    pub async fn configure_replication(&self) {
        let replica_of_host = self
            .config
            .read()
            .await
            .get("replica_of_host")
            .cloned()
            .unwrap_or_default();
        let replica_of_port = self
            .config
            .read()
            .await
            .get("replica_of_port")
            .cloned()
            .unwrap_or_default();

        if !replica_of_host.is_empty() && !replica_of_port.is_empty() {
            self.replication_config
                .write()
                .await
                .set_replica_of(
                    replica_of_host.clone(),
                    replica_of_port.parse::<u16>().expect("none"),
                )
                .await;
            if let Err(e) = self
                .handshake_with_master(replica_of_host.clone(), replica_of_port.clone())
                .await
            {
                eprintln!("configure failure with : {}", e);
                return;
            }
        }
    }

    pub async fn get_port(&self) -> u16 {
        self.config
            .read()
            .await
            .get("port")
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
                        return Err(
                            "Argument Error: --dbfilename option requires an argument".into()
                        );
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
                        let replica_location_split: Vec<&str> =
                            replica_location.split_whitespace().collect();

                        if replica_location_split.len() == 2 {
                            result
                                .push(("replica_of_host".into(), replica_location_split[0].into()));
                            result
                                .push(("replica_of_port".into(), replica_location_split[1].into()));
                            arg_index += 2;
                        } else {
                            return Err("Argument Error: --replicaof requires a host and port (e.g., 'localhost 6379')".into());
                        }
                    } else {
                        return Err("Argument Error: --replicaof requires a host and port (e.g., 'localhost 6379')".into());
                    }
                }
                _ => {
                    return Err(format!(
                        "Argument Error: '{}' is an unknown option",
                        args[arg_index]
                    ))
                }
            }
        }

        Ok(result)
    }

    pub async fn handshake_with_master(
        &self,
        master_host: String,
        master_port: String,
    ) -> Result<(), String> {
        let master_address = format!("{}:{}", master_host, master_port);
        let port = self.get_port().await;

        let stream = TcpStream::connect(&master_address)
            .await
            .map_err(|e| format!("Failed to connect to master: {}", e))?;
        let (mut read_stream, mut write_stream) = stream.into_split();

        // 초기 핸드셰이크 단계는 그대로 유지
        self.send_command_with_writer(&mut write_stream, &[PING_COMMAND])
            .await?;
        self.expect_pong_response(&mut read_stream).await?;

        self.send_command_with_writer(
            &mut write_stream,
            &[REPLCONF_COMMAND, "listening-port", &port.to_string()],
        )
        .await?;
        self.expect_ok_response(&mut read_stream).await?;

        self.send_command_with_writer(&mut write_stream, &[REPLCONF_COMMAND, "capa", "psync2"])
            .await?;
        self.expect_ok_response(&mut read_stream).await?;

        self.send_command_with_writer(&mut write_stream, &[PSYNC_COMMAND, "?", "-1"])
            .await?;
        self.expect_fullresync_response(&mut read_stream).await?;

        // RDB 사이즈 읽기
        let mut size_str = String::new();
        let mut reading_size = true;
        let mut rdb_size = 0;

        // $ 마커 이후 크기 정보 읽기
        while reading_size {
            let mut byte = [0u8; 1];
            read_stream
                .read_exact(&mut byte)
                .await
                .map_err(|e| format!("Failed to read RDB size byte: {}", e))?;

            match byte[0] {
                b'$' => continue, // $ 마커는 건너뜀
                b'\r' => {
                    // \r\n 확인
                    let mut lf = [0u8; 1];
                    read_stream
                        .read_exact(&mut lf)
                        .await
                        .map_err(|e| format!("Failed to read LF after CR: {}", e))?;

                    if lf[0] == b'\n' {
                        // 크기 문자열을 숫자로 파싱
                        rdb_size = size_str
                            .parse::<usize>()
                            .map_err(|e| format!("Failed to parse RDB size: {}", e))?;
                        println!("RDB size: {} bytes", rdb_size);
                        reading_size = false;
                    } else {
                        return Err("Invalid RDB size format".to_string());
                    }
                }
                // 숫자 문자 추가
                _ => size_str.push(byte[0] as char),
            }
        }

        // RDB 데이터를 모두 읽음
        let mut rdb_buffer = vec![0u8; rdb_size];
        read_stream
            .read_exact(&mut rdb_buffer)
            .await
            .map_err(|e| format!("Failed to read RDB data: {}", e))?;

        // 버퍼를 처리
        {
            let cursor = Cursor::new(rdb_buffer);
            let reader = tokio::io::BufReader::new(cursor);
            let mut db_guard = self.db.write().await;
            let mut parser = RdbParser::new(reader, &mut *db_guard);

            if let Err(e) = parser.parse().await {
                return Err(format!("Failed to parse RDB data: {}", e));
            }
        }

        self.respond_with_ack(&mut write_stream).await;

        // 복제 설정 업데이트
        self.replication_config
            .write()
            .await
            .set_replica_of(
                master_host.clone(),
                master_port.parse::<u16>().expect("none"),
            )
            .await;

        // 복제 스트림 모니터링을 위한 별도 태스크 생성
        let publisher = self.publisher.clone();
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut temp_buffer = [0u8; 1024];

            loop {
                // 기존 복제 스트림 모니터링 코드 유지
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

                                if let Some(size_end) =
                                    buffer[pos + 1..].iter().position(|&b| b == b'\r')
                                {
                                    if let Ok(size) = String::from_utf8_lossy(
                                        &buffer[pos + 1..pos + 1 + size_end],
                                    )
                                    .parse::<usize>()
                                    {
                                        expected_elements = size;
                                        array_end = pos + 1 + size_end + 2;

                                        while elements < expected_elements
                                            && array_end < buffer.len()
                                        {
                                            if buffer[array_end] != b'$' {
                                                break;
                                            }

                                            if let Some(len_end) = buffer[array_end + 1..]
                                                .iter()
                                                .position(|&b| b == b'\r')
                                            {
                                                if let Ok(len) = String::from_utf8_lossy(
                                                    &buffer[array_end + 1..array_end + 1 + len_end],
                                                )
                                                .parse::<usize>()
                                                {
                                                    array_end =
                                                        array_end + 1 + len_end + 2 + len + 2;
                                                    elements += 1;

                                                    if elements == expected_elements
                                                        && array_end <= buffer.len()
                                                    {
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
                                        if let Ok(parsed_command) =
                                            CommandParser::parse_message(&command)
                                        {
                                            if let Err(e) =
                                                publisher.publish_command(0, parsed_command).await
                                            {
                                                eprintln!(
                                                    "Failed to publish command from master: {}",
                                                    e
                                                );
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

    async fn send_command_with_writer(
        &self,
        stream: &mut OwnedWriteHalf,
        args: &[&str],
    ) -> Result<(), String> {
        let command = construct_redis_command(args);
        stream
            .write_all(command.as_bytes())
            .await
            .map_err(|e| format!("Failed to send command to master: {}", e))
    }

    async fn expect_pong_response(&self, stream: &mut OwnedReadHalf) -> Result<(), String> {
        let mut buffer = [0u8; 512];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .map_err(|e| format!("Failed to read PONG response from master: {}", e))?;
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
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .map_err(|e| format!("Failed to read OK response from master: {}", e))?;
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
}
