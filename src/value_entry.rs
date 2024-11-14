use std::time::{Duration, SystemTime, UNIX_EPOCH};


#[derive(Clone)]
pub struct ValueEntry {
    pub(crate) value: String,
    expiration: Option<SystemTime>,
}

impl ValueEntry {
    pub fn new_absolute(value: String, expiration_ms: Option<u64>) -> ValueEntry {
        let expiration = expiration_ms.map(|ms| UNIX_EPOCH + Duration::from_millis(ms));
        ValueEntry { value, expiration }
    }

    pub fn new_relative(value: String, duration_ms: Option<u64>) -> ValueEntry {
        let expiration = duration_ms.map(|ms| SystemTime::now() + Duration::from_millis(ms));
        ValueEntry { value, expiration }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expiration) = self.expiration {
            SystemTime::now() > expiration
        } else {
            false
        }
    }
}