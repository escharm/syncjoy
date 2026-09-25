#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("relay unreachable: {0}")]
    Connect(String),
    #[error("invalid frame: {0}")]
    Protocol(#[from] syncjoy_protocol::ProtocolError),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("relay link closed")]
    Closed,
    #[error("webrtc failure: {0}")]
    Rtc(String),
    #[error("dial timed out")]
    Timeout,
}
