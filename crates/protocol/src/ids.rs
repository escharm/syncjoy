use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(pub String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
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
