//! TCP listener module for accepting and spawning incoming client connections.

use crate::client::Client;
use crate::codec::{DecoderTrait, EncoderTrait};
use crate::error::TransportError;
use crate::tcp::client_handler::ClientHandler;
use crate::types::{EventReceiver, EventSender};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::channel;
use tokio::time::sleep;

/// Accepts incoming TCP connections and spawns dedicated client worker tasks.
pub struct Listener<D, E> {
    decoder: D,
    encoder: E,
    nodelay: bool,
}

impl<D: DecoderTrait, E: EncoderTrait> Listener<D, E> {
    pub fn new(decoder: D, encoder: E, nodelay: bool) -> Self {
        Listener {
            decoder,
            encoder,
            nodelay,
        }
    }

    /// Binds to the address and spawns the connection acceptance loop.
    pub async fn listen<C: Client>(
        self,
        address: &str,
    ) -> Result<EventReceiver<D, E, C>, TransportError> {
        let listener = TcpListener::bind(address).await?;
        let (inbound_tx, inbound_rx) = channel(128);

        tokio::spawn(self.run(listener, inbound_tx));

        Ok(inbound_rx)
    }

    /// Accepts connections until the event receiver is dropped.
    async fn run<C: Client>(self, listener: TcpListener, inbound_tx: EventSender<D, E, C>) {
        let mut client_id = 1;

        loop {
            let (stream, address) = tokio::select! {
                accepted = accept(&listener) => accepted,
                _ = inbound_tx.closed() => return,
            };

            // On failure the connection still works, just with Nagle's algorithm on.
            let _ = stream.set_nodelay(self.nodelay);

            ClientHandler::new(
                client_id.into(),
                stream,
                address,
                self.decoder.clone(),
                self.encoder.clone(),
                inbound_tx.clone(),
            )
            .start();

            client_id += 1;
        }
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
