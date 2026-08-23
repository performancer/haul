//! Client handler module for the haul layer.
//!
//! Manages the asynchronous lifecycle and bidirectional I/O multiplexing for a
//! single connected TCP client using [`tokio_util::codec::Framed`].

use crate::client::Client;
use crate::types::EventSender;
use crate::{DecoderTrait, EncoderTrait, Event};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crate::error::ClientError;

/// Manages the concurrent read/write loop for an individual client connection.
pub struct ClientHandler<D: DecoderTrait, E: EncoderTrait, C: Client> {
    pub client: C,
    pub address: SocketAddr,
    pub stream: TcpStream,
    pub decoder: D,
    pub encoder: E,
    pub inbound_tx: EventSender<D, E, C>,
    pub outbound_rx: Receiver<E::Outbound>,
}

impl<D: DecoderTrait, E: EncoderTrait, C: Client> ClientHandler<D, E, C> {
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
                outbound = self.outbound_rx.recv() => {
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

        framed
            .send(Bytes::from(raw))
            .await
            .map_err(ClientError::Io)
    }
}
