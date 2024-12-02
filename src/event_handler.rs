use crate::event::RedisEvent;
use crate::event_publisher::EventPublisher;
use crate::redis_client::Client;
use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::io::AsyncWriteExt;
use crate::client_manager::ClientManager;
use std::net::TcpStream;

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
                if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                    let addr = client.get_addr();
                    let writer = client.get_writer();
                    if let Err(e) = command.handle_command(
                        writer,
                        &self.db,
                        &self.config,
                        &self.replication_config,
                        addr,
                        &self.publisher,
                    ).await {
                        eprintln!("Error handling command: {}", e);
                    }
                }
            }

            RedisEvent::SlaveConnected { addr } => {
                println!("New slave connected: {}", addr);
                
                if self.client_manager.get_client_mut(&(addr.port() as u64)).is_some() {
                    println!("Client already exists for slave: {}", addr);
                    self.replication_config.write().await.register_slave(addr).await;
                    return;
                }
                
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(stream) => {
                        let (_, writer) = stream.into_split();
                        let client = Client::new(addr.port() as u64, writer, addr);
                        self.client_manager.add_client(addr.port() as u64, client);
                        self.replication_config.write().await.register_slave(addr).await;
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to slave {}: {}", addr, e);
                    }
                }
            }

            RedisEvent::SlaveDisconnected { addr } => {
                println!("Slave disconnected: {}", addr);
            }

            RedisEvent::PropagateSlave { addr, message } => {
                println!("Propagating message to slaves: {}", message);
                let repl_guard = self.replication_config.read().await;
                let slaves = repl_guard.list_slaves().await;
                
                for slave in slaves.iter() {
                    let client_id = slave.addr.port() as u64;
                    println!("Trying to propagate to slave with client_id: {}", client_id);
                    
                    if let Some(client) = self.client_manager.get_client_mut(&client_id) {
                        println!("Found client, sending message");
                        if let Err(e) = client.get_writer().write_all(message.as_bytes()).await {
                            eprintln!("Failed to propagate message to slave {}: {}", slave.addr, e);
                        }
                    } else {
                        println!("No client found for slave: {}", slave.addr);
                    }
                }
            }
        }
    }
} 