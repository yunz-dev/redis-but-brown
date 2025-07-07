use crate::resp::Value;
use crate::db::{Db, DbValue};
use bytes::Bytes;

pub async fn handle_command(db: &Db, cmd: &[Value]) -> Option<Value> {
    if cmd.is_empty() {
        return None;
    }

    match &cmd[0] {
        Value::BulkString(bs) => {
            let cmd_str = std::str::from_utf8(bs.as_ref()).ok()?;
            match cmd_str.to_uppercase().as_str() {
                "PING" => Some(Value::SimpleString("PONG".to_string())),
                "SET" => handle_set(db, &cmd[1..]).await,
                "GET" => handle_get(db, &cmd[1..]).await,
                "DEL" => handle_del(db, &cmd[1..]).await,
                _ => None,
            }
        }
        _ => None,
    }
}

async fn handle_set(db: &Db, args: &[Value]) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    {
        let mut map = db.write().await;
        map.insert(key, DbValue::new(value));
    }
    Some(Value::SimpleString("OK".to_string()))
}

async fn handle_get(db: &Db, args: &[Value]) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let key = extract_string(&args[0])?;
    let mut map = db.write().await; // Need write to remove if expired
    if let Some(db_val) = map.get(&key) {
        if db_val.is_expired() {
            map.remove(&key);
            return Some(Value::Null);
        }
        Some(Value::BulkString(db_val.data.clone()))
    } else {
        Some(Value::Null)
    }
}

async fn handle_del(db: &Db, args: &[Value]) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let key = extract_string(&args[0])?;
    let mut map = db.write().await;
    let count = if map.remove(&key).is_some() { 1 } else { 0 };
    Some(Value::Integer(count))
}

fn extract_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bs) => std::str::from_utf8(bs.as_ref()).ok().map(|s| s.to_string()),
        _ => None,
    }
}

fn extract_bytes(value: &Value) -> Option<Bytes> {
    match value {
        Value::BulkString(bs) => Some(bs.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::new_db;

    #[tokio::test]
    async fn test_set_get() {
        let db = new_db();
        let cmd = vec![
            Value::BulkString(Bytes::from("SET")),
            Value::BulkString(Bytes::from("key")),
            Value::BulkString(Bytes::from("value")),
        ];
        let resp = handle_command(&db, &cmd).await;
        assert_eq!(resp, Some(Value::SimpleString("OK".to_string())));

        let cmd_get = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp_get = handle_command(&db, &cmd_get).await;
        assert_eq!(resp_get, Some(Value::BulkString(Bytes::from("value"))));
    }

    #[tokio::test]
    async fn test_get_expired() {
        let db = new_db();
        // Manually insert expired value
        {
            let mut map = db.write().await;
            let mut val = DbValue::new(Bytes::from("value"));
            val.expiry = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
            map.insert("key".to_string(), val);
        }

        let cmd_get = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp_get = handle_command(&db, &cmd_get).await;
        assert_eq!(resp_get, Some(Value::Null));
    }

    #[tokio::test]
    async fn test_del() {
        let db = new_db();
        // Set first
        let cmd_set = vec![
            Value::BulkString(Bytes::from("SET")),
            Value::BulkString(Bytes::from("key")),
            Value::BulkString(Bytes::from("value")),
        ];
        handle_command(&db, &cmd_set).await;

        let cmd_del = vec![
            Value::BulkString(Bytes::from("DEL")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp = handle_command(&db, &cmd_del).await;
        assert_eq!(resp, Some(Value::Integer(1)));

        // Get after del
        let cmd_get = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp_get = handle_command(&db, &cmd_get).await;
        assert_eq!(resp_get, Some(Value::Null));
    }

    #[tokio::test]
    async fn test_ping() {
        let db = new_db();
        let cmd = vec![Value::BulkString(Bytes::from("PING"))];
        let resp = handle_command(&db, &cmd).await;
        assert_eq!(resp, Some(Value::SimpleString("PONG".to_string())));
    }
}