/// Converts protocol frames into application packets.
pub trait DecoderTrait: Clone + Send + Sync + 'static {
    type Inbound: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Deserializes a framed byte payload into an inbound packet.
    fn decode(&mut self, data: &[u8]) -> Result<Self::Inbound, Self::Error>;
}

/// Converts application packets into protocol frames.
pub trait EncoderTrait: Clone + Send + Sync + 'static {
    type Outbound: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Serializes an outbound packet into raw bytes for transmission.
    fn encode(&mut self, packet: Self::Outbound) -> Result<Vec<u8>, Self::Error>;
}