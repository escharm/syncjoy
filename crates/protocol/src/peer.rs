use crate::ids::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PeerToRelay {
    Hello { peer_id: PeerId },
    Offer { to: PeerId, sdp: String },
    Answer { to: PeerId, sdp: String },
    Ice { to: PeerId, candidate: IceCandidate },
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
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}
