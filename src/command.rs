use crate::event_publisher::EventPublisher;
use crate::protocol_constants::*;
use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;

pub enum Command {
    PING,
    ECHO(String),
    GET(String),
    SET {
        key: String,
        value: String,
        px: Option<u64>,
        ex: Option<u64>,
    },
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
        db: &mut HashMap<String, ValueEntry>,
        config: &mut HashMap<String, String>,
        replication_config: &mut ReplicationConfig,
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
        db: &mut HashMap<String, ValueEntry>,
        config: &mut HashMap<String, String>,
        replication_config: &mut ReplicationConfig,
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
            Command::GET(key) => Ok(vec![CommandResponse::Simple(Self::execute_get(key, db))]),
            Command::SET { key, value, ex, px } => {
                let role = replication_config.get_role();
                if role == "slave" {
                    let response = Self::execute_set(key, value, *ex, *px, db);
                    return Ok(vec![CommandResponse::Simple(response)]);
                }

                let response = Self::execute_set(key, value, *ex, *px, db);

                let replicated_command = format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                );

                publisher
                    .publish_propagate_slave(replicated_command)
                    .await
                    .map_err(|e| format!("Failed to propagate command to slaves: {}", e))?;

                Ok(vec![CommandResponse::Simple(response)])
            }
            Command::CONFIG(command) => Ok(vec![CommandResponse::Simple(
                Self::execute_config(command, config),
            )]),
            Command::KEYS(_pattern) => Ok(vec![CommandResponse::Simple(Self::execute_keys(db))]),
            Command::INFO(section) => Ok(vec![CommandResponse::Simple(
                Self::execute_info(section, replication_config),
            )]),
            Command::REPLCONF(args) => Ok(vec![CommandResponse::Simple(
                Self::execute_replconf(args, peer_addr, publisher).await,
            )]),
            Command::PSYNC(args) => Ok(Self::execute_psync(args, replication_config)),
        }
    }

    fn execute_get(key: &str, db: &HashMap<String, ValueEntry>) -> String {
        match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    format!("{}$-1{}", BULK_STRING_PREFIX, CRLF)
                } else {
                    format!(
                        "{}{}{}{}{}",
                        BULK_STRING_PREFIX,
                        entry.value.len(),
                        CRLF,
                        entry.value,
                        CRLF
                    )
                }
            }
            None => format!("{}$-1{}", BULK_STRING_PREFIX, CRLF),
        }
    }

    fn execute_set(
        key: &str,
        value: &str,
        ex: Option<u64>,
        px: Option<u64>,
        db: &mut HashMap<String, ValueEntry>,
    ) -> String {
        let expiration_ms = if let Some(ex) = ex {
            Some(ex * 1000)
        } else {
            px
        };

        db.insert(
            key.to_string(),
            ValueEntry::new_relative(value.to_string(), expiration_ms),
        );
        format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF)
    }

    fn execute_config(command: &ConfigCommand, config: &HashMap<String, String>) -> String {
        match command {
            ConfigCommand::GET(key) => match config.get(key.as_str()) {
                Some(value) => format!(
                    "{}2{}{}{}{}{}{}{}{}{}{}{}",
                    ARRAY_PREFIX,
                    CRLF,
                    BULK_STRING_PREFIX,
                    key.len(),
                    CRLF,
                    key,
                    CRLF,
                    BULK_STRING_PREFIX,
                    value.len(),
                    CRLF,
                    value,
                    CRLF
                ),
                None => format!("{}-1{}", BULK_STRING_PREFIX, CRLF),
            },
        }
    }

    fn execute_keys(db: &HashMap<String, ValueEntry>) -> String {
        let keys: Vec<String> = db.keys().cloned().collect();
        let mut response = format!("*{}\r\n", keys.len());
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        response
    }

    fn execute_info(section: &str, replication_config: &ReplicationConfig) -> String {
        if section.to_lowercase() == "replication" {
            let replication_info = replication_config.get_info();
            format!("${}\r\n{}\r\n", replication_info.len(), replication_info)
        } else {
            format!("{}-1{}", BULK_STRING_PREFIX, CRLF)
        }
    }

    async fn execute_replconf(
        args: &[String],
        peer_addr: SocketAddr,
        publisher: &EventPublisher,
    ) -> String {
        if args[0] == "listening-port" {
            if let Err(e) = publisher.publish_slave_connected(peer_addr).await {
                return format!("-ERR Failed to register slave: {}{}", e, CRLF);
            }
            return format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF);
        } else if args[0] == "capa" {
            return format!("{}OK{}", SIMPLE_STRING_PREFIX, CRLF);
        } else if args[0].to_lowercase() == "getack" {
            return format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n");
        }
        format!("-ERR Invalid REPLCONF arguments{}", CRLF)
    }

    fn execute_psync(args: &[String], replication_config: &ReplicationConfig) -> Vec<CommandResponse> {
        let master_repl_id = replication_config.get_master_replid();
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

            const EMPTY_RDB_FILE: &[u8] =
                &[0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39, 0xFF];

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
