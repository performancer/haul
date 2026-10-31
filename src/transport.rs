use crate::error::TransportError;
use crate::types::EventReceiver;
use crate::{DecoderTrait, EncoderTrait};
use async_trait::async_trait;

/// A transport layer capable of dispatching inbound events and managing communication channels.
#[async_trait]
pub trait Transport<D: DecoderTrait, E: EncoderTrait, C>: Send + Sync + 'static {
    /// Starts the transport in the background, returning the receiver for its events.
    /// Dropping the receiver stops accepting new connections.
    async fn listen(&self) -> Result<EventReceiver<D, E, C>, TransportError>;
}
