use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc::Sender};
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    String(Bytes),
    List(Vec<Bytes>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DbValue {
    pub data: DataType,
    pub expiry: Option<Instant>,
}

pub struct Database {
    pub data: HashMap<String, DbValue>,
    pub channels: HashMap<String, Vec<Sender<Bytes>>>,
}

pub type Db = Arc<RwLock<Database>>;

pub fn new_db() -> Db {
    Arc::new(RwLock::new(Database {
        data: HashMap::new(),
        channels: HashMap::new(),
    }))
}

impl DbValue {
    pub fn new_string(data: Bytes) -> Self {
        Self { data: DataType::String(data), expiry: None }
    }

    pub fn new_list(data: Vec<Bytes>) -> Self {
        Self { data: DataType::List(data), expiry: None }
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
            let mut db_lock = db.write().await;
            db_lock.data.insert("key".to_string(), DbValue::new_string(Bytes::from("value")));
        }

        // GET
        {
            let db_lock = db.read().await;
            match db_lock.data.get("key").map(|v| &v.data) {
                Some(DataType::String(bs)) => assert_eq!(bs, &Bytes::from("value")),
                _ => panic!("Expected string"),
            }
        }

        // DEL
        {
            let mut db_lock = db.write().await;
            db_lock.data.remove("key");
        }

        // GET after del
        {
            let db_lock = db.read().await;
            assert_eq!(db_lock.data.get("key"), None);
        }
    }

    #[tokio::test]
    async fn test_expiry() {
        let db = new_db();
        let mut val = DbValue::new_string(Bytes::from("value"));
        val.expiry = Some(Instant::now() - Duration::from_secs(1)); // Expired

        {
            let mut db_lock = db.write().await;
            db_lock.data.insert("key".to_string(), val);
        }

        // GET should return null and remove
        {
            let mut db_lock = db.write().await;
            if let Some(db_val) = db_lock.data.get("key") {
                if db_val.is_expired() {
                    db_lock.data.remove("key");
                }
            }
            assert_eq!(db_lock.data.get("key"), None);
        }
    }
}