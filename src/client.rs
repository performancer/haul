/// A unique client session identifier convertible from a 64-bit integer.
pub trait Client: From<u64> + Clone + Send + Sync + 'static {}

impl<T> Client for T where T: From<u64> + Clone + Send + Sync + 'static {}