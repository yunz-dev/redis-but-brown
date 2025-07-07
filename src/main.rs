use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{interval, Duration};
use bytes::BytesMut;
use rand::seq::SliceRandom;
use redust::resp::{parse_value, Value, serialize_value};
use redust::db::new_db;
use redust::commands::handle_command;

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

async fn active_expiration(db: redust::db::Db) {
    let mut interval = interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        let mut map = db.write().await;
        let keys: Vec<String> = map.keys().cloned().collect();
        let sample: Vec<_> = keys.choose_multiple(&mut rand::thread_rng(), 20.min(keys.len())).collect();
        for key in sample {
            if let Some(val) = map.get(key) {
                if val.is_expired() {
                    map.remove(key);
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
                if let Some(response) = handle_command(&db, &arr).await {
                    let serialized = serialize_value(&response);
                    if socket.write(&serialized).await.is_err() {
                        return;
                    }
                }
            }
            Ok(_) => {} // Ignore non-array
            Err(_) => {} // Incomplete, wait for more data
        }
    }
}