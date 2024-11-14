use crate::protocol_constants::*;
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
}

pub enum ConfigCommand {
    GET(String),
}

impl Command {
    pub async fn handle_command(&self, stream: &mut TcpStream, db: Db, config: Config) -> std::io::Result<()> {
        let response = self.execute(db, config).await;
        stream.write_all(response.as_bytes()).await?;
        Ok(())
    }

    pub async fn execute(&self, db: Db, config: Config) -> String {
        match self {
            Command::PING => format!("{}PONG{}", SIMPLE_STRING_PREFIX, CRLF),
            Command::ECHO(echo_message) => format!("{}{}{}{}{}", BULK_STRING_PREFIX, echo_message.len(), CRLF, echo_message, CRLF),
            Command::GET(key) => Self::execute_get(key, db).await,
            Command::SET { key, value, ex, px } => Self::execute_set(key, value, *ex, *px, db).await,
            Command::CONFIG(command) => Self::execute_config(command, config).await,
            Command::KEYS(pattern) => Self::execute_keys(db).await,
            Command::INFO(section) => Self::execute_info(section).await, // Handling INFO command
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

    async fn execute_info(section: &String) -> String {
        if section.to_lowercase() == "replication" {
            let replication_info = "role:master";
            format!("${}\r\n{}\r\n", replication_info.len(), replication_info)
        } else {
            format!("{}-1{}", BULK_STRING_PREFIX, CRLF)
        }
    }
}