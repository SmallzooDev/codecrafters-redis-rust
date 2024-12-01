use crate::replication_config::ReplicationConfig;
use std::collections::HashMap;
use crate::value_entry::ValueEntry;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct StateManager {
    db: Arc<RwLock<HashMap<String, ValueEntry>>>,
    config: Arc<RwLock<HashMap<String, String>>>,
    replication_config: Arc<RwLock<ReplicationConfig>>,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(HashMap::new())),
            replication_config: Arc::new(RwLock::new(ReplicationConfig::new())),
        }
    }

    pub fn get_db(&self) -> Arc<RwLock<HashMap<String, ValueEntry>>> {
        self.db.clone()
    }

    pub fn get_config(&self) -> Arc<RwLock<HashMap<String, String>>> {
        self.config.clone()
    }

    pub fn get_replication_config(&self) -> Arc<RwLock<ReplicationConfig>> {
        self.replication_config.clone()
    }
}