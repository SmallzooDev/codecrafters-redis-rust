use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::mem;

pub struct StateManager {
    db: HashMap<String, ValueEntry>,
    config: HashMap<String, String>,
    replication_config: ReplicationConfig,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            db: HashMap::new(),
            config: HashMap::new(),
            replication_config: ReplicationConfig::new(),
        }
    }

    pub fn take_db(&mut self) -> HashMap<String, ValueEntry> {
        mem::take(&mut self.db)
    }

    pub fn restore_db(&mut self, db: HashMap<String, ValueEntry>) {
        self.db = db;
    }

    pub fn set_value(&mut self, key: String, value: ValueEntry) {
        self.db.insert(key, value);
    }

    pub fn get_value(&self, key: &str) -> Option<&ValueEntry> {
        self.db.get(key)
    }

    pub fn delete_value(&mut self, key: &str) -> Option<ValueEntry> {
        self.db.remove(key)
    }

    pub fn take_config(&mut self) -> HashMap<String, String> {
        mem::take(&mut self.config)
    }

    pub fn restore_config(&mut self, config: HashMap<String, String>) {
        self.config = config;
    }

    pub fn set_config(&mut self, key: String, value: String) {
        self.config.insert(key, value);
    }

    pub fn get_config_value(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    pub fn take_replication_config(&mut self) -> ReplicationConfig {
        mem::take(&mut self.replication_config)
    }

    pub fn restore_replication_config(&mut self, config: ReplicationConfig) {
        self.replication_config = config;
    }

    pub fn get_config(&self) -> Option<&HashMap<String, String>> {
        if self.config.is_empty() {
            None
        } else {
            Some(&self.config)
        }
    }
}