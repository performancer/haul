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
}

impl<D, E> TcpTransport<D, E> {
    pub fn new(address: impl Into<String>, decoder: D, encoder: E) -> Self {
        Self {
            address: address.into(),
            decoder,
            encoder,
        }
    }
}

#[async_trait]
impl<D: DecoderTrait, E: EncoderTrait, C: Client> Transport<D, E, C> for TcpTransport<D, E> {
    async fn listen(&self, inbound_tx: EventSender<D, E, C>) -> Result<(), TransportError> {
        Listener::new(self.decoder.clone(), self.encoder.clone())
            .listen(inbound_tx, &self.address)
            .await
    }
}
