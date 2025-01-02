use crate::client_manager::ClientManager;
use crate::event::RedisEvent;
use crate::event_publisher::EventPublisher;
use crate::redis_client::Client;
use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

pub struct EventHandler {
    db: Arc<RwLock<HashMap<String, ValueEntry>>>,
    config: Arc<RwLock<HashMap<String, String>>>,
    replication_config: Arc<RwLock<ReplicationConfig>>,
    client_manager: ClientManager,
    publisher: EventPublisher,
}

impl EventHandler {
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
            client_manager: ClientManager::new(),
            publisher,
        }
    }

    pub async fn handle_event(&mut self, event: RedisEvent) {
        match event {
            RedisEvent::ClientConnected { client_id, writer, addr } => {
                println!("New client connected: {}", client_id);
                let client = Client::new(client_id, writer, addr);
                self.client_manager.add_client(client_id, client);
            }

            RedisEvent::ClientDisconnected { client_id } => {
                println!("Client disconnected: {}", client_id);
                self.client_manager.remove_client(client_id);
            }

            RedisEvent::CommandReceived { client_id, command } => {
                if client_id == 0 {
                    let mut db = self.db.write().await;
                    if let Err(e) = command.execute_without_response(&mut db).await {
                        eprintln!("Failed to execute command from master: {}", e);
                    }
                } else if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                    if let Err(e) = command.handle_command(
                        &mut client.writer,
                        &self.db,
                        &self.config,
                        &self.replication_config,
                        client.addr,
                        &self.publisher,
                    ).await {
                        eprintln!("Failed to handle command: {}", e);
                    }
                }
            }

            RedisEvent::SlaveConnected { addr } => {
                println!("New slave connected: {}", addr);
                let client_id = addr.port() as u64;

                if let Some(_) = self.client_manager.get_client_mut(&client_id) {
                    self.replication_config.write().await.register_slave(addr).await;
                }
            }

            RedisEvent::SlaveDisconnected { addr } => {
                println!("Slave disconnected: {}", addr);
            }

            RedisEvent::PropagateSlave { message } => {
                let repl_guard = self.replication_config.read().await;
                let slaves = repl_guard.list_slaves().await;

                for slave in slaves.iter() {
                    let client_id = slave.addr.port() as u64;

                    if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                        if let Err(e) = client.get_writer().write_all(message.as_bytes()).await {
                            eprintln!("Failed to propagate message to slave {}: {}", slave.addr, e);
                        }
                    } else {
                        println!("No client found for slave addr: {}", slave.addr);
                    }
                }
            }
        }
    }
} 