use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct ValueEntry {
    pub(crate) value: String,
    expiration: Option<Instant>,
}

impl ValueEntry {
    pub fn new(value: String, ex: Option<u64>, px: Option<u64>) -> ValueEntry {
        let expiration = match (px, ex) {
            (Some(ms), _) => Some(Instant::now() + Duration::from_millis(ms)),
            (_, Some(s)) => Some(Instant::now() + Duration::from_secs(s)),
            _ => None,
        };
        ValueEntry { value, expiration }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expiration) = self.expiration {
            Instant::now() > expiration
        } else {
            false
        }
    }
}