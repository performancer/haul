use std::error::Error;
use std::fmt;
use std::io;

/// Errors that can occur while managing a connected client's I/O loop.
#[derive(Debug)]
pub enum ClientError<D, E> {
    Io(io::Error),
    Decode(D),
    Encode(E),
    InboundChannelClosed,
    OutboundChannelClosed,
}

impl<D, E> fmt::Display for ClientError<D, E>
where
    D: Error,
    E: Error,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Decode(err) => write!(f, "decoding error: {err}"),
            Self::Encode(err) => write!(f, "encoding error: {err}"),
            Self::InboundChannelClosed => write!(f, "inbound channel closed"),
            Self::OutboundChannelClosed => write!(f, "outbound channel closed"),
        }
    }
}

impl<D, E> Error for ClientError<D, E>
where
    D: Error + 'static,
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Decode(err) => Some(err),
            Self::Encode(err) => Some(err),
            Self::InboundChannelClosed | Self::OutboundChannelClosed => None,
        }
    }
}

impl<D, E> From<io::Error> for ClientError<D, E> {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
