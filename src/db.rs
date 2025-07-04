use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use bytes::Bytes;

pub type Db = Arc<RwLock<HashMap<String, Bytes>>>;

pub fn new_db() -> Db {
    Arc::new(RwLock::new(HashMap::new()))
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
            map.insert("key".to_string(), Bytes::from("value"));
        }

        // GET
        {
            let map = db.read().await;
            assert_eq!(map.get("key"), Some(&Bytes::from("value")));
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
}