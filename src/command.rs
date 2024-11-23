use crate::protocol_constants::*;
use crate::replication_config::ReplicationConfig;
use crate::{Config, Db, ValueEntry};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub enum Command {
    PING,
    ECHO(String),
    GET(String),
    SET { key: String, value: String, px: Option<u64>, ex: Option<u64> },
    CONFIG(ConfigCommand),
    KEYS(String),
    INFO(String),
    REPLCONF(Vec<String>),
    PSYNC(Vec<String>),
}

pub enum ConfigCommand {
    GET(String),
}

pub enum CommandResponse {
    Simple(String),
    Bulk(Vec<u8>),
    EndStream,
}

impl Command {
    pub async fn handle_command(&self, stream: &mut TcpStream, db: Db, config: Config, replication_config: ReplicationConfig) -> std::io::Result<()> {
        let responses = self.execute(db, config, replication_config).await;
        for response in responses {
            match response {
                CommandResponse::Simple(response) => {
                    stream.write_all(response.as_bytes()).await?;
                }
                CommandResponse::Bulk(data) => {
                    let header = format!("${}{}", data.len(), CRLF);
                    stream.write_all(header.as_bytes()).await?;
                    stream.write_all(&data).await?;
                }
                CommandResponse::EndStream => {
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn execute(&self, db: Db, config: Config, replication_config: ReplicationConfig) -> Vec<CommandResponse> {
        match self {
            Command::PING => { vec![CommandResponse::Simple(format!("{}PONG{}", SIMPLE_STRING_PREFIX, CRLF))] }
            Command::ECHO(echo_message) => { vec![CommandResponse::Simple(format!("{}{}{}{}{}", BULK_STRING_PREFIX, echo_message.len(), CRLF, echo_message, CRLF))] }
            Command::GET(key) => { vec![CommandResponse::Simple(Self::execute_get(key, db).await)] }
            Command::SET { key, value, ex, px } => { vec![CommandResponse::Simple(Self::execute_set(key, value, *ex, *px, db).await)] }
            Command::CONFIG(command) => { vec![CommandResponse::Simple(Self::execute_config(command, config).await)] }
            Command::KEYS(_pattern) => { vec![CommandResponse::Simple(Self::execute_keys(db).await)] }
            Command::INFO(section) => { vec![CommandResponse::Simple(Self::execute_info(section, replication_config).await)] }
            Command::REPLCONF(args) => {
                println!("REPLCONF received with arguments: {:?}", args);
                vec![CommandResponse::Simple(format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF))]
            }
            Command::PSYNC(args) => { Self::execute_psync(args, replication_config).await }
        }
    }

    async fn execute_get(key: &String, db: Db) -> String {
        match db.read().await.get(key) {
            Some(value_entry) => {
                if value_entry.is_expired() {
                    format!("{}-1{}", BULK_STRING_PREFIX, CRLF)
                } else {
                    format!("{}{}{}{}{}", BULK_STRING_PREFIX, value_entry.value.len(), CRLF, value_entry.value, CRLF)
                }
            }
            None => format!("{}-1{}", BULK_STRING_PREFIX, CRLF),
        }
    }

    async fn execute_set(key: &String, value: &String, ex: Option<u64>, px: Option<u64>, db: Db) -> String {
        let expiration_ms = match (px, ex) {
            (Some(ms), _) => Some(ms),
            (None, Some(s)) => Some(s * 1000),
            _ => None,
        };

        db.write().await.insert(key.clone(), ValueEntry::new_relative(value.clone(), expiration_ms));
        format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF)
    }

    async fn execute_config(command: &ConfigCommand, config: Config) -> String {
        match command {
            ConfigCommand::GET(key) => {
                match config.read().await.get(key.as_str()) {
                    Some(value) => {
                        format!("{}2{}{}{}{}{}{}{}{}{}{}{}", ARRAY_PREFIX, CRLF, BULK_STRING_PREFIX, key.len(), CRLF, key, CRLF, BULK_STRING_PREFIX, value.len(), CRLF, value, CRLF)
                    }
                    None => format!("{}-1{}", BULK_STRING_PREFIX, CRLF),
                }
            }
        }
    }

    async fn execute_keys(db: Db) -> String {
        let keys: Vec<String> = db.read().await.keys().cloned().collect();
        let mut response = format!("*{}\r\n", keys.len());
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        response
    }

    async fn execute_info(section: &String, replication_config: ReplicationConfig) -> String {
        if section.to_lowercase() == "replication" {
            let replication_info = replication_config.get_replication_info().await;
            format!("${}\r\n{}\r\n", replication_info.len(), replication_info)
        } else {
            format!("{}-1{}", BULK_STRING_PREFIX, CRLF)
        }
    }

    async fn execute_psync(args: &Vec<String>, replication_config: ReplicationConfig) -> Vec<CommandResponse> {
        let master_repl_id = replication_config.get_repl_id().await;

        let requested_offset: i64 = args
            .get(1)
            .and_then(|offset| offset.parse::<i64>().ok())
            .unwrap_or(-1);

        let master_offset = 0;

        if requested_offset == -1 || requested_offset < master_offset {
            let full_resync_response = format!(
                "{}FULLRESYNC {} {}{}",
                SIMPLE_STRING_PREFIX, master_repl_id, master_offset, CRLF
            );

            const EMPTY_RDB_FILE: &[u8] = &[
                0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39,
                0xFF,
            ];

            vec![
                CommandResponse::Simple(full_resync_response),
                CommandResponse::Bulk(EMPTY_RDB_FILE.to_vec()),
                CommandResponse::EndStream,
            ]
        } else {
            vec![CommandResponse::Simple(format!(
                "{}TODO{}",
                SIMPLE_STRING_PREFIX, CRLF
            ))]
        }
    }
}