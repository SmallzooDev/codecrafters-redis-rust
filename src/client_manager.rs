use crate::redis_client::Client;
use std::collections::HashMap;


pub struct ClientManager {
    clients: HashMap<u64, Client>,
}

impl ClientManager {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
    pub fn add_client(&mut self, client_id: u64, client: Client) {
        self.clients.insert(client_id, client);
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.clients.remove(&client_id);
    }

    pub fn get_client(&self, client_id: u64) -> Option<&Client> {
        self.clients.get(&client_id)
    }
}