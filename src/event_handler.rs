use crate::client::Client;
use crate::command_parser::CommandParser;
use crate::event::RedisEvent;
use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct EventHandler {
    db: Arc<RwLock<HashMap<String, ValueEntry>>>,
    config: Arc<RwLock<HashMap<String, String>>>,
    replication_config: Arc<RwLock<ReplicationConfig>>,
    clients: HashMap<u64, Client>,
}

impl EventHandler {
    pub fn new(
        db: Arc<RwLock<HashMap<String, ValueEntry>>>,
        config: Arc<RwLock<HashMap<String, String>>>,
        replication_config: Arc<RwLock<ReplicationConfig>>,
    ) -> Self {
        Self {
            db,
            config,
            replication_config,
            clients: HashMap::new(),
        }
    }

    pub async fn handle_event(&mut self, event: RedisEvent) {
        match event {
            RedisEvent::ClientConnected { client_id, stream, addr } => {
                println!("New client connected: {}", client_id);
                let client = Client::new(client_id, stream, addr);
                self.clients.insert(client_id, client);
            }

            RedisEvent::ClientDisconnected { client_id } => {
                println!("Client disconnected: {}", client_id);
                self.clients.remove(&client_id);
            }

            RedisEvent::CommandReceived { client_id, command } => {
                if let Some(client) = self.clients.get_mut(&client_id) {
                    let addr = client.get_addr();
                    match CommandParser::parse_message(&command) {
                        Ok(cmd) => {
                            let writer = client.get_writer();
                            if let Err(e) = cmd.handle_command(
                                writer,
                                &self.db,
                                &self.config,
                                &self.replication_config,
                                addr,
                            ).await {
                                eprintln!("Error handling command: {}", e);
                            }
                        }
                        Err(e) => eprintln!("Error parsing command: {}", e),
                    }
                }
            }

            RedisEvent::SlaveConnected { addr } => {
                self.replication_config.write().await.register_slave(addr).await;
            }

            RedisEvent::SlaveDisconnected { addr } => {
                println!("Slave disconnected: {}", addr);
            }

            RedisEvent::SlaveOffsetUpdated { addr, offset } => {
                self.replication_config.write().await.update_slave_offset(addr, offset).await;
            }
        }
    }
} 