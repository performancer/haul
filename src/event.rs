use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;

/// Lifecycle and data events emitted by the transport layer.
pub enum Event<Inbound, Outbound, Client, Error> {
    /// A client has successfully connected, providing a channel for outbound messages.
    Connected(Client, SocketAddr, Sender<Outbound>),

    /// An inbound packet has been received and decoded.
    Message(Client, Inbound),

    /// A client connection has terminated.
    Disconnected(Client, SocketAddr),

    /// A transport, codec, or I/O error occurred for a client.
    Error(Client, SocketAddr, Error),
}