use crate::error::ClientError;
use crate::{DecoderTrait, EncoderTrait, Event};
use tokio::sync::mpsc::{Receiver, Sender};

/// The transport event produced for a given decoder, encoder, and client type.
type TransportEvent<D, E, C> = Event<
    <D as DecoderTrait>::Inbound,
    <E as EncoderTrait>::Outbound,
    C,
    ClientError<<D as DecoderTrait>::Error, <E as EncoderTrait>::Error>,
>;

/// A type alias for the multi-producer channel sender used to dispatch transport events upstream.
pub type EventSender<D, E, C> = Sender<TransportEvent<D, E, C>>;

/// A type alias for the channel receiver through which transport events are consumed.
pub type EventReceiver<D, E, C> = Receiver<TransportEvent<D, E, C>>;
