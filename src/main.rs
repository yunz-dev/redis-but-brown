use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;
use redust::resp::{parse_value, Value};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Server listening on 127.0.0.1:6379");

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(socket).await;
        });
    }
}

async fn handle_connection(mut socket: TcpStream) {
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
            Ok(Value::Array(arr)) if arr.len() == 1 => {
                if let Value::BulkString(cmd) = &arr[0] {
                    if cmd.as_ref() == b"PING" {
                        let response = b"+PONG\r\n";
                        if socket.write(response).await.is_err() {
                            return;
                        }
                    }
                }
            }
            Ok(_) => {} // Ignore other commands for now
            Err(_) => {} // Incomplete, wait for more data
        }
    }
}