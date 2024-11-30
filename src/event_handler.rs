use crate::command_parser::CommandParser;
use crate::event::RedisEvent;
use crate::replication_config::ReplicationConfig;
use crate::Client::Client;
use crate::{Config, Db};
use std::collections::HashMap;

pub struct EventHandler {
    db: Db,
    config: Config,
    replication_config: ReplicationConfig,
    clients: HashMap<u64, Client>,
}

impl EventHandler {
    pub fn new(
        db: Db,
        config: Config,
        replication_config: ReplicationConfig,
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
                                self.db.clone(),
                                self.config.clone(),
                                self.replication_config.clone(),
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
                self.replication_config.register_slave(addr).await;
            }

            RedisEvent::SlaveDisconnected { addr } => {
                println!("Slave disconnected: {}", addr);
                println!("And impl TODO");
            }

            RedisEvent::SlaveOffsetUpdated { addr, offset } => {
                self.replication_config.update_slave_offset(addr, offset).await;
            }
        }
    }
} 