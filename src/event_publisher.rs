use tokio::sync::mpsc::Sender;
use crate::event::RedisEvent;
use std::net::SocketAddr;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct EventPublisher {
    tx: Sender<RedisEvent>,
}

impl EventPublisher {
    pub fn new(tx: Sender<RedisEvent>) -> Self {
        Self { tx }
    }

    pub async fn publish_command(&self, client_id: u64, command: String) -> Result<(), String> {
        self.tx.send(RedisEvent::CommandReceived {
            client_id,
            command,
        })
        .await
        .map_err(|e| format!("Failed to send command event: {}", e))
    }

    pub async fn publish_client_connected(&self, client_id: u64, stream: TcpStream, addr: SocketAddr) -> Result<(), String> {
        self.tx.send(RedisEvent::ClientConnected {
            client_id,
            stream,
            addr,
        })
        .await
        .map_err(|e| format!("Failed to send client connected event: {}", e))
    }

    pub async fn publish_client_disconnected(&self, client_id: u64) -> Result<(), String> {
        self.tx.send(RedisEvent::ClientDisconnected {
            client_id,
        })
        .await
        .map_err(|e| format!("Failed to send client disconnected event: {}", e))
    }
} 