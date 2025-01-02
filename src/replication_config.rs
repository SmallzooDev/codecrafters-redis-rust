use crate::protocol_constants::{BULK_STRING_PREFIX, CRLF};
use rand::distr::Alphanumeric;
use rand::Rng;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ReplicationConfig {
    role: Arc<RwLock<String>>,
    master_host: Arc<RwLock<Option<String>>>,
    master_port: Arc<RwLock<Option<u16>>>,
    master_replid: Arc<RwLock<String>>,
    master_repl_offset: Arc<RwLock<u64>>,
    slaves: Arc<RwLock<Vec<SlaveInfo>>>,
}

#[derive(Debug)]
pub struct SlaveInfo {
    pub addr: SocketAddr,
    pub offset: i64,
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
            slaves: Arc::new(RwLock::new(Vec::new())),
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

    #[allow(dead_code)]
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

    pub async fn get_repl_id(&self) -> String {
        self.master_replid.read().await.clone()
    }

    pub async fn get_replication_info(&self) -> String {
        let role = self.get_role().await;
        let mut info = format!("{}role:{}{}", BULK_STRING_PREFIX, role, CRLF);

        if role == "master" {
            let master_replid = self.master_replid.read().await.clone();
            let master_repl_offset = *self.master_repl_offset.read().await;
            info.push_str(&format!("master_replid:{}{}", master_replid, CRLF));
            info.push_str(&format!("master_repl_offset:{}{}", master_repl_offset, CRLF));
            let slaves = self.list_slaves().await;
            info.push_str(&format!("connected_slaves:{}\r\n", slaves.len()));
            for (i, slave) in slaves.iter().enumerate() {
                info.push_str(&format!(
                    "slave{}:ip={},port={},state=online,offset={}\r\n",
                    i,
                    slave.addr.ip(),
                    slave.addr.port(),
                    slave.offset
                ));
            }
        } else if role == "slave" {
            let master_host = self.master_host.read().await;
            let master_port = self.master_port.read().await;
            if let (Some(host), Some(port)) = (master_host.as_ref(), master_port.as_ref()) {
                info.push_str(&format!("master_host:{}{}", host, CRLF));
                info.push_str(&format!("master_port:{}{}", port, CRLF));
                info.push_str(&format!("master_link_status:up{}", CRLF));
            }
        }

        info
    }
    pub async fn register_slave(&self, addr: SocketAddr) {
        let mut slaves = self.slaves.write().await;
        if !slaves.iter().any(|slave| slave.addr == addr) {
            slaves.push(SlaveInfo {
                addr,
                offset: 0,
            });
        }
    }

    pub async fn update_slave_offset(&self, addr: SocketAddr, offset: i64) {
        let mut slaves = self.slaves.write().await;
        if let Some(slave) = slaves.iter_mut().find(|slave| slave.addr == addr) {
            slave.offset = offset;
        }
    }


    pub async fn update_slave_state(&self, addr: SocketAddr, offset: i64) {
        let mut slaves = self.slaves.write().await;
        if let Some(slave) = slaves.iter_mut().find(|slave| slave.addr == addr) {
            slave.offset = offset;
        }
    }

    pub async fn list_slaves(&self) -> tokio::sync::RwLockReadGuard<'_, Vec<SlaveInfo>> {
        self.slaves.read().await
    }

    pub async fn get_slaves_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, Vec<SlaveInfo>> {
        self.slaves.write().await
    }
}