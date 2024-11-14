use crate::protocol_constants::{BULK_STRING_PREFIX, CRLF};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ReplicationConfig {
    role: Arc<RwLock<String>>,
    master_host: Arc<RwLock<Option<String>>>,
    master_port: Arc<RwLock<Option<u16>>>,
}

impl ReplicationConfig {
    pub fn new() -> Self {
        Self {
            role: Arc::new(RwLock::new("master".to_string())),
            master_host: Arc::new(RwLock::new(None)),
            master_port: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn set_replica_of(&self, host: String, port: u16) {
        let mut role_guard = self.role.write().await;
        *role_guard = "slave".to_string();
        let mut master_host = self.master_host.write().await;
        *master_host = Some(host);
        let mut master_port = self.master_port.write().await;
        *master_port = Some(port);
    }

    pub async fn promote_to_master(&self) {
        let mut role_guard = self.role.write().await;
        *role_guard = "master".to_string();
        let mut master_host = self.master_host.write().await;
        *master_host = None;
        let mut master_port = self.master_port.write().await;
        *master_port = None;
    }

    pub async fn get_role(&self) -> String {
        self.role.read().await.clone()
    }

    pub async fn get_replication_info(&self) -> String {
        let role = self.get_role().await;
        let mut info = format!("{}role:{}{}", BULK_STRING_PREFIX, role, CRLF);
        info
    }
}