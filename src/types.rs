use crate::error::ClientError;
use crate::{DecoderTrait, EncoderTrait, Event};
use tokio::sync::mpsc::Sender;

/// A type alias for the multi-producer channel sender used to dispatch transport events upstream.
pub type EventSender<D, E, C> = Sender<
    Event<
        <D as DecoderTrait>::Inbound,
        <E as EncoderTrait>::Outbound,
        C,
        ClientError<<D as DecoderTrait>::Error, <E as EncoderTrait>::Error>,
    >,
>;
