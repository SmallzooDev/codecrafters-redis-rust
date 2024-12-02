use crate::event_publisher::EventPublisher;
use crate::protocol_constants::*;
use crate::replication_config::ReplicationConfig;
use crate::ValueEntry;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::RwLock;

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
    pub async fn handle_command(
        &self,
        writer: &mut OwnedWriteHalf,
        db: &Arc<RwLock<HashMap<String, ValueEntry>>>,
        config: &Arc<RwLock<HashMap<String, String>>>,
        replication_config: &Arc<RwLock<ReplicationConfig>>,
        peer_addr: SocketAddr,
        publisher: &EventPublisher,
    ) -> std::io::Result<()> {
        match self.execute(db, config, replication_config, peer_addr, publisher).await {
            Ok(responses) => {
                for response in responses {
                    match response {
                        CommandResponse::Simple(response) => {
                            writer.write_all(response.as_bytes()).await?;
                        }
                        CommandResponse::Bulk(data) => {
                            let header = format!("${}{}", data.len(), CRLF);
                            writer.write_all(header.as_bytes()).await?;
                            writer.write_all(&data).await?;
                        }
                        CommandResponse::EndStream => break,
                    }
                }
            }
            Err(e) => {
                let err_response = format!("-ERR {}\r\n", e);
                writer.write_all(err_response.as_bytes()).await?;
            }
        }
        Ok(())
    }

    pub async fn execute(
        &self,
        db: &Arc<RwLock<HashMap<String, ValueEntry>>>,
        config: &Arc<RwLock<HashMap<String, String>>>,
        replication_config: &Arc<RwLock<ReplicationConfig>>,
        peer_addr: SocketAddr,
        publisher: &EventPublisher,
    ) -> Result<Vec<CommandResponse>, String> {
        match self {
            Command::PING => Ok(vec![CommandResponse::Simple(format!(
                "{}PONG{}",
                SIMPLE_STRING_PREFIX, CRLF
            ))]),
            Command::ECHO(echo_message) => Ok(vec![CommandResponse::Simple(format!(
                "{}{}{}{}{}",
                BULK_STRING_PREFIX,
                echo_message.len(),
                CRLF,
                echo_message,
                CRLF
            ))]),
            Command::GET(key) => {
                let db = db.read().await;
                Ok(vec![CommandResponse::Simple(
                    Self::execute_get(key, &db).await,
                )])
            }
            Command::SET { key, value, ex, px } => {
                let role = replication_config.read().await.get_role().await;
                let mut db = db.write().await;

                if role == "slave" {
                    let response = Self::execute_set(key, value, *ex, *px, &mut db).await;
                    return Ok(vec![CommandResponse::Simple(response)]);
                }

                let response = Self::execute_set(key, value, *ex, *px, &mut db).await;

                let replicated_command = format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                );

                publisher.publish_propagate_slave(peer_addr, replicated_command).await
                    .map_err(|e| format!("Failed to propagate command to slaves: {}", e))?;

                Ok(vec![CommandResponse::Simple(response)])
            }
            Command::CONFIG(command) => Ok(vec![CommandResponse::Simple(
                Self::execute_config(command, config).await,
            )]),
            Command::KEYS(_pattern) => Ok(vec![CommandResponse::Simple(Self::execute_keys(db).await)]),
            Command::INFO(section) => Ok(vec![CommandResponse::Simple(
                Self::execute_info(section, replication_config).await,
            )]),
            Command::REPLCONF(args) => Ok(vec![CommandResponse::Simple(
                Self::execute_replconf(args, peer_addr, publisher).await,
            )]),
            Command::PSYNC(args) => Ok(Self::execute_psync(args, replication_config).await),
        }
    }

    async fn execute_get(key: &String, db: &HashMap<String, ValueEntry>) -> String {
        match db.get(key) {
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

    async fn execute_set(key: &String, value: &String, ex: Option<u64>, px: Option<u64>, db: &mut HashMap<String, ValueEntry>) -> String {
        let expiration_ms = match (px, ex) {
            (Some(ms), _) => Some(ms),
            (None, Some(s)) => Some(s * 1000),
            _ => None,
        };

        db.insert(key.clone(), ValueEntry::new_relative(value.clone(), expiration_ms));
        format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF)
    }

    async fn execute_config(command: &ConfigCommand, config: &Arc<RwLock<HashMap<String, String>>>) -> String {
        match command {
            ConfigCommand::GET(key) => {
                let config = config.read().await;
                match config.get(key.as_str()) {
                    Some(value) => {
                        format!("{}2{}{}{}{}{}{}{}{}{}{}{}", ARRAY_PREFIX, CRLF, BULK_STRING_PREFIX, key.len(), CRLF, key, CRLF, BULK_STRING_PREFIX, value.len(), CRLF, value, CRLF)
                    }
                    None => format!("{}-1{}", BULK_STRING_PREFIX, CRLF),
                }
            }
        }
    }

    async fn execute_keys(db: &Arc<RwLock<HashMap<String, ValueEntry>>>) -> String {
        let db = db.read().await;
        let keys: Vec<String> = db.keys().cloned().collect();
        let mut response = format!("*{}\r\n", keys.len());
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        response
    }

    async fn execute_info(section: &String, replication_config: &Arc<RwLock<ReplicationConfig>>) -> String {
        if section.to_lowercase() == "replication" {
            let replication_config = replication_config.read().await;
            let replication_info = replication_config.get_replication_info().await;
            format!("${}\r\n{}\r\n", replication_info.len(), replication_info)
        } else {
            format!("{}-1{}", BULK_STRING_PREFIX, CRLF)
        }
    }
    pub async fn execute_replconf(
        args: &Vec<String>,
        peer_addr: SocketAddr,
        publisher: &EventPublisher,
    ) -> String {
        // TODO: 요구사항에는, --listening-port로 전파하는 것처럼 되어있지만 실제로는 그렇지 않아 리팩토링 필요
        if args[0] == "listening-port" {
            if let Err(e) = publisher.publish_slave_connected(peer_addr).await {
                return format!("-ERR Failed to register slave: {}{}", e, CRLF);
            }
            return format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF);
        } else if args[0] == "capa" {
            return format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF);
        }
        format!("-ERR Invalid REPLCONF arguments{}", CRLF)
    }

    async fn execute_psync(
        args: &Vec<String>,
        replication_config: &Arc<RwLock<ReplicationConfig>>,
    ) -> Vec<CommandResponse> {
        let master_repl_id = replication_config.read().await.get_repl_id().await;
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

            // TODO : give real rdb file if needed
            const EMPTY_RDB_FILE: &[u8] = &[
                0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39,
                0xFF,
            ];

            vec![
                CommandResponse::Simple(full_resync_response),
                CommandResponse::Bulk(EMPTY_RDB_FILE.to_vec()),
            ]
        } else {
            vec![CommandResponse::Simple(format!(
                "{}CONTINUE{}",
                SIMPLE_STRING_PREFIX, CRLF
            ))]
        }
    }
}