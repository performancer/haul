//! TCP listener module for accepting and spawning incoming client connections.

use crate::client::Client;
use crate::codec::{DecoderTrait, EncoderTrait};
use crate::error::TransportError;
use crate::event::Event;
use crate::tcp::client_handler::ClientHandler;
use crate::types::EventSender;
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::channel;

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

    /// Binds to the address and runs the connection acceptance loop.
    pub async fn listen<C: Client>(
        &self,
        inbound_tx: EventSender<D, E, C>,
        address: &str,
    ) -> Result<(), TransportError> {
        let listener = Self::bind(address).await?;
        let mut client_id = 1;

        loop {
            let (stream, address) = self.accept(&listener).await?;
            let (outbound_tx, outbound_rx) = channel(256);
            let client: C = client_id.into();

            if inbound_tx
                .send(Event::Connected(client.clone(), address, outbound_tx))
                .await
                .is_err()
            {
                return Err(TransportError::ChannelClosed);
            }

            ClientHandler {
                client,
                stream,
                address,
                decoder: self.decoder.clone(),
                encoder: self.encoder.clone(),
                inbound_tx: inbound_tx.clone(),
                outbound_rx,
            }
            .start();

            client_id += 1;
        }
    }

    async fn bind(address: &str) -> io::Result<TcpListener> {
        let listener = TcpListener::bind(address).await?;
        Ok(listener)
    }

    async fn accept(&self, listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, address) = listener.accept().await?;
        // On failure the connection still works, just with Nagle's algorithm on.
        let _ = stream.set_nodelay(self.nodelay);

        Ok((stream, address))
    }
}
