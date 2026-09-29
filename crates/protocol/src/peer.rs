use crate::ids::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PeerToRelay {
    Hello { peer_id: PeerId },
    Offer { to: PeerId, sdp: String },
    Answer { to: PeerId, sdp: String },
    Ice { to: PeerId, candidate: IceCandidate },
    Decline { to: PeerId },
    Pong,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelayToPeer {
    Ack,
    Ping,
    Turn {
        urls: Vec<String>,
        username: String,
        credential: String,
    },
    PeerOnline {
        peer_id: PeerId,
    },
    PeerOffline {
        peer_id: PeerId,
    },
    OfferRelay {
        from: PeerId,
        sdp: String,
    },
    AnswerRelay {
        from: PeerId,
        sdp: String,
    },
    IceRelay {
        from: PeerId,
        candidate: IceCandidate,
    },
    Rejected {
        to: PeerId,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: String,
    pub credential: String,
}

impl IceServer {
    pub fn stun(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            username: String::new(),
            credential: String::new(),
        }
    }

    pub fn turn(
        url: impl Into<String>,
        username: impl Into<String>,
        credential: impl Into<String>,
    ) -> Self {
        Self {
            urls: vec![url.into()],
            username: username.into(),
            credential: credential.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}
