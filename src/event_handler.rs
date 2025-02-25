use crate::client_manager::ClientManager;
use crate::event::RedisEvent;
use crate::event_publisher::EventPublisher;
use crate::redis_client::Client;
use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;

pub struct EventHandler {
    db: HashMap<String, ValueEntry>,
    config: HashMap<String, String>,
    replication_config: ReplicationConfig,
    client_manager: ClientManager,
    publisher: EventPublisher,
}

impl EventHandler {
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
            client_manager: ClientManager::new(),
            publisher,
        }
    }

    pub async fn handle_event(&mut self, event: RedisEvent) {
        match event {
            RedisEvent::Command { client_id, command } => {
                if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                    let addr = client.get_addr();
                    if let Err(e) = command
                        .handle_command(
                            client.get_writer_mut(),
                            &mut self.db,
                            &mut self.config,
                            &mut self.replication_config,
                            addr,
                            &self.publisher,
                        )
                        .await
                    {
                        eprintln!("Failed to handle command: {}", e);
                    }
                }
            }

            RedisEvent::ClientConnected {
                client_id,
                writer,
                addr,
            } => {
                let client = Client::new(writer, addr);
                self.client_manager.add_client(client_id, client);
                if let Some(_) = self.client_manager.get_client_mut(&client_id) {
                    self.replication_config.register_slave(addr);
                }
            }

            RedisEvent::ClientDisconnected { client_id } => {
                self.client_manager.remove_client(client_id);
            }

            RedisEvent::CommandReceived { client_id, command } => {
                if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                    let addr = client.get_addr();
                    if let Err(e) = command
                        .handle_command(
                            client.get_writer_mut(),
                            &mut self.db,
                            &mut self.config,
                            &mut self.replication_config,
                            addr,
                            &self.publisher,
                        )
                        .await
                    {
                        eprintln!("Failed to handle received command: {}", e);
                    }
                }
            }

            RedisEvent::SlaveConnected { addr } => {
                println!("Slave connected: {}", addr);
            }

            RedisEvent::SlaveDisconnected { addr } => {
                println!("Slave disconnected: {}", addr);
            }

            RedisEvent::PropagateSlave { message } => {
                let slaves = self.replication_config.get_slaves().clone();
                for slave in slaves.iter() {
                    let client_id = slave.addr.port() as u64;
                    if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                        if let Err(e) = client.get_writer_mut().write_all(message.as_bytes()).await {
                            eprintln!("Failed to propagate message to slave {}: {}", slave.addr, e);
                        }
                    }
                }
            }
        }
    }
} 