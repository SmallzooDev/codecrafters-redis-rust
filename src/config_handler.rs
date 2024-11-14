use crate::rdb_parser::RdbParser;
use crate::replication_config::ReplicationConfig;
use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type Db = Arc<RwLock<HashMap<String, ValueEntry>>>;
pub type Config = Arc<RwLock<HashMap<String, String>>>;

pub struct ConfigHandler {
    db: Db,
    config: Config,
    replication_config: ReplicationConfig,
}

impl ConfigHandler {
    pub fn new(db: Db, config: Config, replication_config: ReplicationConfig) -> Self {
        Self { db, config, replication_config }
    }

    pub async fn load_config(&self) {
        let args: Vec<String> = env::args().collect();
        match ConfigHandler::parse_env(args) {
            Ok(result) => {
                let mut config_guard = self.config.write().await;
                for (key, value) in result {
                    config_guard.insert(key, value);
                }
                println!("Configuration loaded.");
            }
            Err(e) => {
                eprintln!("Failed to parse configuration: {}", e);
            }
        }
    }

    pub async fn configure_db(&self) {
        let config_guard = self.config.read().await;
        let dir = config_guard.get("dir").cloned().unwrap_or_default();
        let db_file_name = config_guard.get("file_name").cloned().unwrap_or_default();

        if !dir.is_empty() && !db_file_name.is_empty() {
            let rdb_file_path = format!("{}/{}", dir, db_file_name);
            if let Ok(mut parser) = RdbParser::new(self.db.clone(), &rdb_file_path) {
                if let Err(e) = parser.parse().await {
                    eprintln!("Error during RDB parsing: {}", e);
                }
            } else {
                eprintln!("Failed to initialize RDB parser.");
            }
        }
    }

    pub async fn configure_replication(&self) {
        let config_guard = self.config.read().await;
        let replica_of_host = config_guard.get("replica_of_host").cloned().unwrap_or_default();
        let replica_of_port = config_guard.get("replica_of_port").cloned().unwrap_or_default();

        if !replica_of_host.is_empty() && !replica_of_port.is_empty() {
            self.replication_config.set_replica_of(replica_of_host, replica_of_port.parse::<u16>().expect("none")).await;
        }
    }

    pub async fn get_port(&self) -> String {
        self.config.read().await.get("port").cloned().unwrap_or_else(|| "6379".to_string())
    }

    fn parse_env(args: Vec<String>) -> Result<Vec<(String, String)>, String> {
        if args.len() <= 1 {
            return Err("No configuration arguments provided to parse".into());
        }

        let mut result = Vec::new();
        let mut arg_index = 1;

        while arg_index < args.len() {
            match args[arg_index].as_str() {
                "--dir" => {
                    if arg_index + 1 < args.len() {
                        result.push(("dir".into(), args[arg_index + 1].clone()));
                        arg_index += 2;
                    } else {
                        return Err("Argument Error: --dir option requires an argument".into());
                    }
                }
                "--dbfilename" => {
                    if arg_index + 1 < args.len() {
                        result.push(("file_name".into(), args[arg_index + 1].clone()));
                        arg_index += 2;
                    } else {
                        return Err("Argument Error: --dbfilename option requires an argument".into());
                    }
                }
                "--port" => {
                    if arg_index + 1 < args.len() {
                        result.push(("port".into(), args[arg_index + 1].clone()));
                        arg_index += 2;
                    } else {
                        return Err("Argument Error: --port option requires an argument".into());
                    }
                }
                "--replicaof" => {
                    if arg_index + 1 < args.len() {
                        let replica_location = args[arg_index + 1].clone();
                        let replica_location_split: Vec<&str> = replica_location.split_whitespace().collect();

                        if replica_location_split.len() == 2 {
                            result.push(("replica_of_host".into(), replica_location_split[0].into()));
                            result.push(("replica_of_port".into(), replica_location_split[1].into()));
                            arg_index += 2;
                        } else {
                            return Err("Argument Error: --replicaof requires a host and port (e.g., 'localhost 6379')".into());
                        }
                    } else {
                        return Err("Argument Error: --replicaof requires a host and port (e.g., 'localhost 6379')".into());
                    }
                }
                _ => return Err(format!("Argument Error: '{}' is an unknown option", args[arg_index])),
            }
        }

        Ok(result)
    }
}