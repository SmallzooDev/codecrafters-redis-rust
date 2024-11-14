use crate::{Config, Db};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct StateManager {
    db: Db,
    config: Config,
}

impl StateManager {
    pub fn new() -> Self {
        let db = Arc::new(RwLock::new(HashMap::new()));
        let config = Arc::new(RwLock::new(HashMap::new()));
        Self { db, config }
    }

    pub fn get_db(&self) -> Db {
        self.db.clone()
    }

    pub fn get_config(&self) -> Config {
        self.config.clone()
    }
}