use crate::protocol_constants::{BULK_STRING_PREFIX, CRLF};
use rand::distr::Alphanumeric;
use rand::Rng;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct ReplicationConfig {
    role: String,
    master_host: Option<String>,
    master_port: Option<u16>,
    master_replid: String,
    master_repl_offset: u64,
    slaves: Vec<SlaveInfo>,
}

#[derive(Debug, Clone)]
pub struct SlaveInfo {
    pub addr: SocketAddr,
    pub offset: i64,
}

impl ReplicationConfig {
    pub fn new() -> Self {
        let replid = Self::generate_replication_id();
        Self {
            role: "master".to_string(),
            master_host: None,
            master_port: None,
            master_replid: replid,
            master_repl_offset: 0,
            slaves: Vec::new(),
        }
    }

    fn generate_replication_id() -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect()
    }

    pub fn set_replica_of(&mut self, host: String, port: u16) {
        self.role = "slave".to_string();
        self.master_host = Some(host);
        self.master_port = Some(port);
    }

    pub fn promote_to_master(&mut self) {
        self.role = "master".to_string();
        self.master_host = None;
        self.master_port = None;
        self.master_repl_offset = 0;
    }

    pub fn get_role(&self) -> &str {
        &self.role
    }

    pub fn get_info(&self) -> String {
        let mut info = String::new();
        let role = self.get_role();
        info.push_str(&format!("role:{}{}", role, CRLF));

        if role == "master" {
            info.push_str(&format!("master_replid:{}{}", self.master_replid, CRLF));
            info.push_str(&format!("master_repl_offset:{}{}", self.master_repl_offset, CRLF));
            info.push_str(&format!("connected_slaves:{}\r\n", self.slaves.len()));
            for (i, slave) in self.slaves.iter().enumerate() {
                info.push_str(&format!(
                    "slave{}:ip={},port={},state=online,offset={}\r\n",
                    i,
                    slave.addr.ip(),
                    slave.addr.port(),
                    slave.offset
                ));
            }
        } else if role == "slave" {
            if let (Some(host), Some(port)) = (&self.master_host, &self.master_port) {
                info.push_str(&format!("master_host:{}{}", host, CRLF));
                info.push_str(&format!("master_port:{}{}", port, CRLF));
                info.push_str(&format!("master_link_status:up{}", CRLF));
            }
        }

        info
    }

    pub fn register_slave(&mut self, addr: SocketAddr) {
        if !self.slaves.iter().any(|slave| slave.addr == addr) {
            self.slaves.push(SlaveInfo {
                addr,
                offset: 0,
            });
        }
    }

    pub fn update_slave_offset(&mut self, addr: SocketAddr, offset: i64) {
        if let Some(slave) = self.slaves.iter_mut().find(|slave| slave.addr == addr) {
            slave.offset = offset;
        }
    }

    pub fn get_slaves(&self) -> &Vec<SlaveInfo> {
        &self.slaves
    }

    pub fn get_slaves_mut(&mut self) -> &mut Vec<SlaveInfo> {
        &mut self.slaves
    }

    pub fn get_master_replid(&self) -> &str {
        &self.master_replid
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self::new()
    }
}