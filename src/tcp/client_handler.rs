//! Client handler module for the haul layer.
//!
//! Manages the asynchronous lifecycle and bidirectional I/O multiplexing for a
//! single connected TCP client using [`tokio_util::codec::Framed`].

use crate::client::Client;
use crate::error::ClientError;
use crate::types::EventSender;
use crate::{DecoderTrait, EncoderTrait, Event};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Manages the concurrent read/write loop for an individual client connection.
pub struct ClientHandler<D: DecoderTrait, E: EncoderTrait, C: Client> {
    client: C,
    stream: TcpStream,
    address: SocketAddr,
    decoder: D,
    encoder: E,
    inbound_tx: EventSender<D, E, C>,
}

impl<D: DecoderTrait, E: EncoderTrait, C: Client> ClientHandler<D, E, C> {
    pub fn new(
        client: C,
        stream: TcpStream,
        address: SocketAddr,
        decoder: D,
        encoder: E,
        inbound_tx: EventSender<D, E, C>,
    ) -> Self {
        ClientHandler {
            client,
            stream,
            address,
            decoder,
            encoder,
            inbound_tx,
        }
    }

    /// Spawns the handler loop into a detached background Tokio task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(mut self) {
        let client = self.client.clone();
        let address = self.address;
        let inbound_tx = self.inbound_tx.clone();

        let Some(mut outbound_rx) = Self::connect(&client, address, &inbound_tx).await else {
            return;
        };

        let mut framed = Framed::new(self.stream, LengthDelimitedCodec::new());

        loop {
            tokio::select! {
                incoming = framed.next() => {
                    match Self::process_inbound(incoming, &mut self.decoder, &client, &inbound_tx).await {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(err) => {
                            let _ = inbound_tx.send(Event::Error(client.clone(), address, err)).await;
                            break;
                        }
                    }
                }
                outbound = outbound_rx.recv() => {
                    match Self::process_outbound(outbound, &mut self.encoder, &mut framed).await {
                        Ok(()) => {}
                        Err(err) => {
                            let _ = inbound_tx.send(Event::Error(client.clone(), address, err)).await;
                            break;
                        }
                    }
                }
            }
        }

        let _ = inbound_tx.send(Event::Disconnected(client, address)).await;
    }

    /// Announces the client upstream, returning the receiver for its outbound packets,
    /// or `None` once the event receiver is dropped.
    async fn connect(
        client: &C,
        address: SocketAddr,
        inbound_tx: &EventSender<D, E, C>,
    ) -> Option<Receiver<E::Outbound>> {
        let (outbound_tx, outbound_rx) = channel(256);

        inbound_tx
            .send(Event::Connected(client.clone(), address, outbound_tx))
            .await
            .ok()?;

        Some(outbound_rx)
    }

    async fn process_inbound(
        incoming: Option<Result<bytes::BytesMut, std::io::Error>>,
        decoder: &mut D,
        client: &C,
        inbound_tx: &EventSender<D, E, C>,
    ) -> Result<bool, ClientError<D::Error, E::Error>> {
        match incoming {
            Some(Ok(bytes)) => {
                let packet = decoder.decode(&bytes).map_err(ClientError::Decode)?;
                inbound_tx
                    .send(Event::Message(client.clone(), packet))
                    .await
                    .map_err(|_| ClientError::InboundChannelClosed)?;
                Ok(true)
            }
            Some(Err(e)) => Err(ClientError::Io(e)),
            None => Ok(false),
        }
    }

    async fn process_outbound(
        outbound: Option<E::Outbound>,
        encoder: &mut E,
        framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    ) -> Result<(), ClientError<D::Error, E::Error>> {
        let packet = outbound.ok_or(ClientError::OutboundChannelClosed)?;
        let raw = encoder.encode(packet).map_err(ClientError::Encode)?;

        framed.send(Bytes::from(raw)).await.map_err(ClientError::Io)
    }
}
