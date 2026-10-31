//! TCP transport: accepts incoming connections and spawns a worker task per client.

use crate::client::Client;
use crate::error::TransportError;
use crate::tcp::client_handler::ClientHandler;
use crate::types::{EventReceiver, EventSender};
use crate::{DecoderTrait, EncoderTrait, Transport};
use async_trait::async_trait;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::channel;
use tokio::time::sleep;

/// A TCP-specific implementation of the [`Transport`] trait.
pub struct TcpTransport<D, E> {
    address: String,
    decoder: D,
    encoder: E,
    nodelay: bool,
}

impl<D, E> TcpTransport<D, E> {
    pub fn new(address: impl Into<String>, decoder: D, encoder: E) -> Self {
        Self {
            address: address.into(),
            decoder,
            encoder,
            nodelay: true,
        }
    }

    /// Sets `TCP_NODELAY` on accepted connections. Defaults to `true`, since
    /// haul sends small framed messages where Nagle's algorithm adds latency.
    pub fn nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = nodelay;
        self
    }
}

#[async_trait]
impl<D: DecoderTrait, E: EncoderTrait> Transport for TcpTransport<D, E> {
    type Decoder = D;
    type Encoder = E;

    async fn listen<C: Client>(&self) -> Result<EventReceiver<D, E, C>, TransportError> {
        let listener = TcpListener::bind(&self.address).await?;
        let (inbound_tx, inbound_rx) = channel(128);

        tokio::spawn(run(
            listener,
            self.decoder.clone(),
            self.encoder.clone(),
            self.nodelay,
            inbound_tx,
        ));

        Ok(inbound_rx)
    }
}

/// Accepts connections until the event receiver is dropped.
async fn run<D: DecoderTrait, E: EncoderTrait, C: Client>(
    listener: TcpListener,
    decoder: D,
    encoder: E,
    nodelay: bool,
    inbound_tx: EventSender<D, E, C>,
) {
    let mut client_id = 1;

    loop {
        let (stream, address) = tokio::select! {
            accepted = accept(&listener) => accepted,
            _ = inbound_tx.closed() => return,
        };

        // On failure the connection still works, just with Nagle's algorithm on.
        let _ = stream.set_nodelay(nodelay);

        ClientHandler::new(
            client_id.into(),
            stream,
            address,
            decoder.clone(),
            encoder.clone(),
            inbound_tx.clone(),
        )
        .start();

        client_id += 1;
    }
}

/// Waits for the next connection, retrying past accept errors.
async fn accept(listener: &TcpListener) -> (TcpStream, SocketAddr) {
    loop {
        match listener.accept().await {
            Ok((stream, address)) => {
                return (stream, address);
            }
            // haul has no logger, so accept errors are retried silently.
            Err(error) if is_connection_error(&error) => continue,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
    }
}

/// Errors that concern only the connection being accepted, not the listener.
fn is_connection_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}
