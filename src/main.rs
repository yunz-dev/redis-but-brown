use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{interval, Duration};
use bytes::{Bytes, BytesMut};
use rand::seq::SliceRandom;
use redust::resp::{parse_value, Value, serialize_value};
use redust::db::new_db;
use redust::commands::{handle_command, CommandResult};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db = new_db();
    println!("Server listening on 127.0.0.1:6379");

    // Spawn active expiration task
    let db_clone = db.clone();
    tokio::spawn(async move {
        active_expiration(db_clone).await;
    });

    loop {
        let (socket, _) = listener.accept().await?;
        let db_clone = db.clone();
        tokio::spawn(async move {
            handle_connection(socket, db_clone).await;
        });
    }
}

async fn handle_subscribe_mode(mut socket: TcpStream, db: &redust::db::Db, channel: String) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    {
        let mut db_lock = db.write().await;
        db_lock.channels.entry(channel.clone()).or_insert_with(Vec::new).push(tx);
    }

    // Send subscribe confirmation
    let response = serialize_value(&Value::Array(vec![
        Value::BulkString(Bytes::from("subscribe")),
        Value::BulkString(Bytes::from(channel.clone())),
        Value::Integer(1),
    ]));
    if socket.write(&response).await.is_err() {
        return;
    }

    loop {
        match rx.recv().await {
            Some(message) => {
                let msg = serialize_value(&Value::Array(vec![
                    Value::BulkString(Bytes::from("message")),
                    Value::BulkString(Bytes::from(channel.clone())),
                    Value::BulkString(message),
                ]));
                if socket.write(&msg).await.is_err() {
                    return;
                }
            }
            None => return, // Channel closed
        }
    }
}

async fn active_expiration(db: redust::db::Db) {
    let mut interval = interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        let mut db_lock = db.write().await;
        let keys: Vec<String> = db_lock.data.keys().cloned().collect();
        let sample: Vec<_> = keys.choose_multiple(&mut rand::thread_rng(), 20.min(keys.len())).collect();
        for key in sample {
            if let Some(val) = db_lock.data.get(key) {
                if val.is_expired() {
                    db_lock.data.remove(key);
                }
            }
        }
    }
}

async fn handle_connection(mut socket: TcpStream, db: redust::db::Db) {
    let mut buf = BytesMut::with_capacity(1024);

    loop {
        // Read data
        socket.readable().await.unwrap();
        match socket.try_read_buf(&mut buf) {
            Ok(0) => return, // Connection closed
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => {
                eprintln!("Error reading: {}", e);
                return;
            }
        }

        // Try to parse
        match parse_value(&mut buf) {
            Ok(Value::Array(arr)) => {
                match handle_command(&db, &arr).await {
                    Some(CommandResult::Value(response)) => {
                        let serialized = serialize_value(&response);
                        if socket.write(&serialized).await.is_err() {
                            return;
                        }
                    }
                    Some(CommandResult::Subscribe(channel)) => {
                        handle_subscribe_mode(socket, &db, channel).await;
                        return; // End connection after subscribe
                    }
                    None => {}
                }
            }
            Ok(_) => {} // Ignore non-array
            Err(_) => {} // Incomplete, wait for more data
        }
    }
}