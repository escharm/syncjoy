use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 6;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Frame<T> {
    pub v: u32,
    pub msg: T,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("unsupported protocol version: {found}")]
    UnsupportedVersion { found: u32 },
    #[error("invalid message: {0}")]
    Invalid(#[from] serde_json::Error),
}

pub fn encode<T: Serialize>(msg: &T) -> String {
    let frame = Frame {
        v: PROTOCOL_VERSION,
        msg,
    };
    serde_json::to_string(&frame).expect("protocol messages are representable in json")
}

pub fn decode<T: DeserializeOwned>(text: &str) -> Result<T, ProtocolError> {
    let value: Value = serde_json::from_str(text)?;
    let frame: Frame<T> = serde_json::from_value(value)?;
    if frame.v != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion { found: frame.v });
    }
    Ok(frame.msg)
}
