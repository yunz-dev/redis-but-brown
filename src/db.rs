use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq)]
pub struct DbValue {
    pub data: Bytes,
    pub expiry: Option<Instant>,
}

pub type Db = Arc<RwLock<HashMap<String, DbValue>>>;

pub fn new_db() -> Db {
    Arc::new(RwLock::new(HashMap::new()))
}

impl DbValue {
    pub fn new(data: Bytes) -> Self {
        Self { data, expiry: None }
    }

    pub fn is_expired(&self) -> bool {
        self.expiry.map_or(false, |exp| Instant::now() > exp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db_operations() {
        let db = new_db();

        // SET
        {
            let mut map = db.write().await;
            map.insert("key".to_string(), DbValue::new(Bytes::from("value")));
        }

        // GET
        {
            let map = db.read().await;
            assert_eq!(map.get("key").map(|v| &v.data), Some(&Bytes::from("value")));
        }

        // DEL
        {
            let mut map = db.write().await;
            map.remove("key");
        }

        // GET after del
        {
            let map = db.read().await;
            assert_eq!(map.get("key"), None);
        }
    }

    #[tokio::test]
    async fn test_expiry() {
        let db = new_db();
        let mut val = DbValue::new(Bytes::from("value"));
        val.expiry = Some(Instant::now() - Duration::from_secs(1)); // Expired

        {
            let mut map = db.write().await;
            map.insert("key".to_string(), val);
        }

        // GET should return null and remove
        {
            let mut map = db.write().await;
            if let Some(db_val) = map.get("key") {
                if db_val.is_expired() {
                    map.remove("key");
                }
            }
            assert_eq!(map.get("key"), None);
        }
    }
}