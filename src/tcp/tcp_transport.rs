use crate::client::Client;
use crate::error::TransportError;
use crate::tcp::listener::Listener;
use crate::types::EventSender;
use crate::{DecoderTrait, EncoderTrait, Transport};
use async_trait::async_trait;

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
impl<D: DecoderTrait, E: EncoderTrait, C: Client> Transport<D, E, C> for TcpTransport<D, E> {
    async fn listen(&self, inbound_tx: EventSender<D, E, C>) -> Result<(), TransportError> {
        Listener::new(self.decoder.clone(), self.encoder.clone(), self.nodelay)
            .listen(inbound_tx, &self.address)
            .await
    }
}
