use crate::error::TransportError;
use crate::types::EventSender;
use crate::{DecoderTrait, EncoderTrait};
use async_trait::async_trait;

/// A transport layer capable of dispatching inbound events and managing communication channels.
#[async_trait]
pub trait Transport<D: DecoderTrait, E: EncoderTrait, C>: Send + Sync + 'static {
    /// Starts the transport, routing events through the provided sender.
    async fn listen(
        &self,
        inbound_tx: EventSender<D, E, C>,
    ) -> Result<(), TransportError>;
}
