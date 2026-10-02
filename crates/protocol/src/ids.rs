use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

const MAX_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerId(pub String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Serialize for PeerId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PeerId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        validate(&value).map_err(D::Error::custom)?;
        Ok(Self(value))
    }
}

fn validate(value: &str) -> Result<(), String> {
    if value.len() > MAX_LEN {
        return Err(format!("peer id exceeds {MAX_LEN} bytes"));
    }
    if !value.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err("peer id must be printable ascii without spaces".to_owned());
    }
    Ok(())
}

impl From<&str> for PeerId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for PeerId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
