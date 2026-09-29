use std::sync::Arc;

use crate::registry::PeerRegistry;
use crate::turn::TurnConfig;
use syncjoy_protocol::{PeerId, PeerToRelay, RelayToPeer};

#[derive(Clone, Default)]
pub struct Relay {
    pub(crate) registry: Arc<PeerRegistry>,
    pub(crate) turn: Option<TurnConfig>,
}

impl Relay {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_turn(turn: TurnConfig) -> Self {
        Self {
            registry: Arc::new(PeerRegistry::new()),
            turn: Some(turn),
        }
    }

    pub fn registry(&self) -> &PeerRegistry {
        &self.registry
    }

    pub(crate) fn route(&self, from: &PeerId, msg: PeerToRelay) {
        match msg {
            PeerToRelay::Hello { .. } => {}
            PeerToRelay::Offer { to, sdp } => {
                if !self.registry.route(
                    &to,
                    RelayToPeer::OfferRelay {
                        from: from.clone(),
                        sdp,
                    },
                ) {
                    self.registry.route(
                        from,
                        RelayToPeer::Rejected {
                            to: to.clone(),
                            reason: format!("peer {to} is not online"),
                        },
                    );
                }
            }
            PeerToRelay::Answer { to, sdp } => {
                self.registry.route(
                    &to,
                    RelayToPeer::AnswerRelay {
                        from: from.clone(),
                        sdp,
                    },
                );
            }
            PeerToRelay::Ice { to, candidate } => {
                self.registry.route(
                    &to,
                    RelayToPeer::IceRelay {
                        from: from.clone(),
                        candidate,
                    },
                );
            }
            PeerToRelay::Decline { to } => {
                self.registry.route(
                    &to,
                    RelayToPeer::Rejected {
                        to: from.clone(),
                        reason: "declined by peer".to_owned(),
                    },
                );
            }
            PeerToRelay::Pong => {}
        }
    }
}
