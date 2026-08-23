# HAUL - Transport Layer

> **⚠️ DISCLAIMER:** `haul` is currently under active, early-stage development. APIs are subject to breaking changes without notice, and backwards compatibility or formal migrations are not yet supported. Use in production at your own risk.

An asynchronous networking library for Rust.

## Features

- **Length-Prefixed Framing**: Uses 4-byte big-endian length prefixes via `tokio_util::codec::LengthDelimitedCodec`.
- **Trait-Based Design**: Separated traits for decoding (`DecoderTrait`), encoding (`EncoderTrait`), and transport orchestration (`Transport`).
- **Concurrent I/O**: Handles inbound and outbound packets concurrently using `tokio::select!`.
- **Protocol Agnostic**: Designed to support custom application protocols.

## Quick Start

1. Implement Codecs:

```rust
use haul::{DecoderTrait, EncoderTrait};

struct MyDecoder;
impl DecoderTrait for MyDecoder {
    type Inbound = String;
    type Error = String;

    fn decode(&mut self, data: &[u8]) -> Result<Self::Inbound, Self::Error> {
        String::from_utf8(data.to_vec()).map_err(|e| e.to_string())
    }
}

struct MyEncoder;
impl EncoderTrait for MyEncoder {
    type Outbound = String;
    type Error = String;

    fn encode(&mut self, packet: Self::Outbound) -> Result<Vec<u8>, Self::Error> {
        Ok(packet.into_bytes())
    }
}
```

2. Start the Server:

```rust
use haul::{TcpTransport, Transport, Event};

#[tokio::main]
async fn main() {
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(256);
    let transport = TcpTransport::new(MyDecoder, MyEncoder);

    tokio::spawn(async move {
        transport.listen("127.0.0.1:8080", inbound_tx).await;
    });

    while let Some(event) = inbound_rx.recv().await {
        match event {
            Event::Connected(client, _tx) => println!("Client connected: {:?}", client),
            Event::Message(client, msg) => println!("Received from {:?}: {}", client, msg),
            Event::Disconnected(client, address) => println!("Client disconnected: {:?} at {}", client, address),
            Event::Error(client, address, err) => eprintln!("Error on {:?} ({address}): {}", client, address, err),
        }
    }
}
```

## License

This project is open-source under the MIT License.