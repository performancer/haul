use std::error::Error;
use std::fmt;
use std::io;

/// Errors that can occur within the transport layer's main listener.
#[derive(Debug)]
pub enum TransportError {
    Io(io::Error),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "network I/O error: {err}"),
        }
    }
}

impl Error for TransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
