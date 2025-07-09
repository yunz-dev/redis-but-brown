use crate::resp::Value;
use crate::db::{Db, DbValue};
use bytes::Bytes;

#[derive(Debug, PartialEq)]
pub enum CommandResult {
    Value(Value),
    Subscribe(String),
}

pub async fn handle_command(db: &Db, cmd: &[Value]) -> Option<CommandResult> {
    if cmd.is_empty() {
        return None;
    }

    match &cmd[0] {
        Value::BulkString(bs) => {
            let cmd_str = std::str::from_utf8(bs.as_ref()).ok()?;
            match cmd_str.to_uppercase().as_str() {
                "PING" => Some(CommandResult::Value(Value::SimpleString("PONG".to_string()))),
                "SET" => handle_set(db, &cmd[1..]).await.map(CommandResult::Value),
                "GET" => handle_get(db, &cmd[1..]).await.map(CommandResult::Value),
                "DEL" => handle_del(db, &cmd[1..]).await.map(CommandResult::Value),
                "SUBSCRIBE" => handle_subscribe(db, &cmd[1..]).await,
                "PUBLISH" => handle_publish(db, &cmd[1..]).await.map(CommandResult::Value),
                _ => None,
            }
        }
        _ => None,
    }
}

async fn handle_set(db: &Db, args: &[Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    let mut expiry = None;
    if args.len() >= 4 && matches!(&args[2], Value::BulkString(bs) if bs.as_ref() == b"EX") {
        if let Value::BulkString(ex_str) = &args[3] {
            if let Ok(ex_secs) = std::str::from_utf8(ex_str.as_ref()).ok()?.parse::<u64>() {
                expiry = Some(std::time::Instant::now() + std::time::Duration::from_secs(ex_secs));
            }
        }
    }
    let mut db_val = DbValue::new(value);
    db_val.expiry = expiry;
    {
        let mut db_lock = db.write().await;
        db_lock.data.insert(key, db_val);
    }
    Some(Value::SimpleString("OK".to_string()))
}

async fn handle_get(db: &Db, args: &[Value]) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let key = extract_string(&args[0])?;
    let mut db_lock = db.write().await; // Need write to remove if expired
    if let Some(db_val) = db_lock.data.get(&key) {
        if db_val.is_expired() {
            db_lock.data.remove(&key);
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
    let mut db_lock = db.write().await;
    let count = if db_lock.data.remove(&key).is_some() { 1 } else { 0 };
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

async fn handle_subscribe(db: &Db, args: &[Value]) -> Option<CommandResult> {
    if args.len() != 1 {
        return None;
    }
    let channel = extract_string(&args[0])?;
    Some(CommandResult::Subscribe(channel))
}

async fn handle_publish(db: &Db, args: &[Value]) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let channel = extract_string(&args[0])?;
    let message = extract_bytes(&args[1])?;
    let mut db_lock = db.write().await;
    let count = if let Some(senders) = db_lock.channels.get_mut(&channel) {
        let initial_count = senders.len();
        senders.retain(|sender| {
            // Try to send, remove if failed
            sender.try_send(message.clone()).is_ok()
        });
        initial_count
    } else {
        0
    };
    Some(Value::Integer(count as i64))
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
        assert_eq!(resp, Some(CommandResult::Value(Value::SimpleString("OK".to_string()))));

        let cmd_get = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp_get = handle_command(&db, &cmd_get).await;
        assert_eq!(resp_get, Some(CommandResult::Value(Value::BulkString(Bytes::from("value")))));
    }

    #[tokio::test]
    async fn test_set_with_ttl() {
        let db = new_db();
        let cmd = vec![
            Value::BulkString(Bytes::from("SET")),
            Value::BulkString(Bytes::from("key")),
            Value::BulkString(Bytes::from("value")),
            Value::BulkString(Bytes::from("EX")),
            Value::BulkString(Bytes::from("1")),
        ];
        let resp = handle_command(&db, &cmd).await;
        assert_eq!(resp, Some(CommandResult::Value(Value::SimpleString("OK".to_string()))));

        // Check expiry is set
        {
            let db_lock = db.read().await;
            if let Some(db_val) = db_lock.data.get("key") {
                assert!(db_val.expiry.is_some());
            } else {
                panic!("Key not found");
            }
        }
    }

    #[tokio::test]
    async fn test_get_expired() {
        let db = new_db();
        // Manually insert expired value
        {
            let mut db_lock = db.write().await;
            let mut val = DbValue::new(Bytes::from("value"));
            val.expiry = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
            db_lock.data.insert("key".to_string(), val);
        }

        let cmd_get = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp_get = handle_command(&db, &cmd_get).await;
        assert_eq!(resp_get, Some(CommandResult::Value(Value::Null)));
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
        assert_eq!(resp, Some(CommandResult::Value(Value::Integer(1))));

        // Get after del
        let cmd_get = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("key")),
        ];
        let resp_get = handle_command(&db, &cmd_get).await;
        assert_eq!(resp_get, Some(CommandResult::Value(Value::Null)));
    }

    #[tokio::test]
    async fn test_ping() {
        let db = new_db();
        let cmd = vec![Value::BulkString(Bytes::from("PING"))];
        let resp = handle_command(&db, &cmd).await;
        assert_eq!(resp, Some(CommandResult::Value(Value::SimpleString("PONG".to_string()))));
    }
}