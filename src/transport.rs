use crate::client::Client;
use crate::error::TransportError;
use crate::types::EventReceiver;
use crate::{DecoderTrait, EncoderTrait};
use async_trait::async_trait;

/// A transport layer capable of dispatching inbound events and managing communication channels.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    type Decoder: DecoderTrait;
    type Encoder: EncoderTrait;

    /// Starts the transport in the background, returning the receiver for its events.
    /// Dropping the receiver stops accepting new connections.
    async fn listen<C: Client>(
        &self,
    ) -> Result<EventReceiver<Self::Decoder, Self::Encoder, C>, TransportError>;
}
