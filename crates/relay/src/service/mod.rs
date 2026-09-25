mod ws_peer;

use std::sync::Arc;

use crate::registry::PeerRegistry;
use crate::turn::TurnConfig;
use axum::routing::get;
use axum::Router;
use syncjoy_protocol::{PeerId, PeerToRelay, RelayToPeer};

#[derive(Clone, Default)]
pub struct Relay {
    registry: Arc<PeerRegistry>,
    turn: Option<TurnConfig>,
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

    pub fn router(&self) -> Router {
        Router::new()
            .route("/syncjoy/healthz", get(health))
            .route("/syncjoy/v1/peer", get(ws_peer::upgrade))
            .with_state(self.clone())
    }

    fn route(&self, from: &PeerId, msg: PeerToRelay) {
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
            PeerToRelay::Pong => {}
        }
    }
}

async fn health() -> &'static str {
    "ok"
}
