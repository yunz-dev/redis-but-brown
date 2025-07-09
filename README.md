# Redis: but its brown

![Redis but brown](assets/redis-but-brown.jpg)

Yeah, I wrote it in rust for fun :D

## Features

- **Async I/O**: Tokio-powered, no blocking BS.
- **Thread-Safe**: RwLock for 10k readers, 1 writer.
- **Zero-Copy**: `bytes` crate keeps it lean.
- **Expiration**: Passive/active TTL – data cleans itself.
- **Pub/Sub**: Channels for real-time vibes.
- **Commands**: SET, GET, DEL, INCR, DECR, EXISTS, KEYS (globs), and growing.
- **Data Types**: Strings, Lists, Sets, Hashes incoming.
- **TDD**: Tests first, bugs last.

## Quick Start

```bash
git clone <your-repo>
cd redust
cargo build --release
cargo run  # Listens on 127.0.0.1:6379
```

Just like Redis:

```bash
redis-cli -p 6379
SET key "value"
GET key
```

## Examples

- **TTL**: `SET temp "gone" EX 5`
- **Math**: `INCR counter` → 1, 2, 3...
- **Keys**: `KEYS *` → all keys
- **Pub/Sub**: Subscribe in one terminal, publish in another.
