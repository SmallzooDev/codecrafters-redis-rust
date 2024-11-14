use crate::replication_config::ReplicationConfig;
use crate::{Config, Db};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct StateManager {
    db: Db,
    config: Config,
    replication_config: ReplicationConfig,
}

impl StateManager {
    pub fn new() -> Self {
        let db = Arc::new(RwLock::new(HashMap::new()));
        let config = Arc::new(RwLock::new(HashMap::new()));
        let replication_config = ReplicationConfig::new();
        Self { db, config, replication_config }
    }

    pub fn get_db(&self) -> Db {
        self.db.clone()
    }

    pub fn get_config(&self) -> Config {
        self.config.clone()
    }

    pub fn get_replication_config(&self) -> ReplicationConfig {
        self.replication_config.clone()
    }
}