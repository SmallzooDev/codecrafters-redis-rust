use crate::protocol_constants::{BULK_STRING_PREFIX, CRLF};
use rand::distr::Alphanumeric;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ReplicationConfig {
    role: Arc<RwLock<String>>,
    master_host: Arc<RwLock<Option<String>>>,
    master_port: Arc<RwLock<Option<u16>>>,
    master_replid: Arc<RwLock<String>>,
    master_repl_offset: Arc<RwLock<u64>>,
}

impl ReplicationConfig {
    pub fn new() -> Self {
        let replid = Self::generate_replication_id();
        Self {
            role: Arc::new(RwLock::new("master".to_string())),
            master_host: Arc::new(RwLock::new(None)),
            master_port: Arc::new(RwLock::new(None)),
            master_replid: Arc::new(RwLock::new(replid)),
            master_repl_offset: Arc::new(RwLock::new(0)),
        }
    }

    fn generate_replication_id() -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect()
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
        let mut master_repl_offset = self.master_repl_offset.write().await;
        *master_repl_offset = 0;
    }

    pub async fn get_role(&self) -> String {
        self.role.read().await.clone()
    }

    pub async fn get_replication_info(&self) -> String {
        let role = self.get_role().await;
        let mut info = format!("{}role:{}{}", BULK_STRING_PREFIX, role, CRLF);

        if role == "master" {
            let master_replid = self.master_replid.read().await.clone();
            let master_repl_offset = *self.master_repl_offset.read().await;
            info.push_str(&format!("master_replid:{}{}", master_replid, CRLF));
            info.push_str(&format!("master_repl_offset:{}{}", master_repl_offset, CRLF));
        }

        info
    }
}