# HAUL - Transport Layer

> **⚠️ DISCLAIMER:** `haul` is currently under active, early-stage development. APIs are subject to breaking changes without notice, and backwards compatibility or formal migrations are not yet supported. Use in production at your own risk.

An asynchronous networking library for Rust.

## Features

- **Length-Prefixed Framing**: Uses 4-byte big-endian length prefixes via `tokio_util::codec::LengthDelimitedCodec`.
- **Trait-Based Design**: Separated traits for decoding (`DecoderTrait`), encoding (`EncoderTrait`), and transport orchestration (`Transport`).
- **Concurrent I/O**: Handles inbound and outbound packets concurrently using `tokio::select!`.
- **Protocol Agnostic**: Designed to support custom application protocols.
- **Low Latency**: `TCP_NODELAY` is on by default, since small framed messages suffer under Nagle's algorithm. Turn it off with `TcpTransport::new(..).nodelay(false)`.
- **Slow Client Queue**: Each client queues up to 256 outbound messages by default. Change it with `TcpTransport::new(..).outbound_capacity(n)`.

## Quick Start

1. Implement Codecs:

```rust
use haul::{DecoderTrait, EncoderTrait};

#[derive(Clone)]
struct MyDecoder;
impl DecoderTrait for MyDecoder {
    type Inbound = String;
    type Error = std::string::FromUtf8Error;

    fn decode(&mut self, data: &[u8]) -> Result<Self::Inbound, Self::Error> {
        String::from_utf8(data.to_vec())
    }
}

#[derive(Clone)]
struct MyEncoder;
impl EncoderTrait for MyEncoder {
    type Outbound = String;
    type Error = std::convert::Infallible;

    fn encode(&mut self, packet: Self::Outbound) -> Result<Vec<u8>, Self::Error> {
        Ok(packet.into_bytes())
    }
}
```

2. Start the Server:

```rust
use haul::tcp::TcpTransport;
use haul::{Event, Transport};

#[tokio::main]
async fn main() {
    let transport = TcpTransport::new("127.0.0.1:8080", MyDecoder, MyEncoder);
    let mut inbound_rx = transport.listen::<u64>().await.unwrap();

    while let Some(event) = inbound_rx.recv().await {
        match event {
            Event::Connected(client, address, _tx) => println!("Client connected: {:?} at {}", client, address),
            Event::Message(client, msg) => println!("Received from {:?}: {}", client, msg),
            Event::Disconnected(client, address) => println!("Client disconnected: {:?} at {}", client, address),
            Event::Error(client, address, err) => eprintln!("Error on {:?} ({}): {}", client, address, err),
        }
    }
}
```

## License

This project is open-source under the MIT License.