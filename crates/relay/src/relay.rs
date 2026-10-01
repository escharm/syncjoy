use std::sync::Arc;

use crate::heartbeat::HeartbeatConfig;
use crate::registry::PeerRegistry;
use crate::turn::TurnConfig;
use syncjoy_protocol::{PeerId, PeerToRelay, RelayToPeer};

#[derive(Clone, Default)]
pub struct Relay {
    pub(crate) registry: Arc<PeerRegistry>,
    pub(crate) turn: Option<TurnConfig>,
    pub(crate) heartbeat: HeartbeatConfig,
    pub(crate) guard: Option<Arc<dyn Fn(&PeerId, Option<&str>) -> bool + Send + Sync>>,
}

impl Relay {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_turn(turn: TurnConfig) -> Self {
        Self {
            registry: Arc::new(PeerRegistry::new()),
            turn: Some(turn),
            heartbeat: HeartbeatConfig::default(),
            guard: None,
        }
    }

    /// Registration gate consulted with the hello peer_id and its optional
    /// token before the peer enters the registry; a false verdict rejects
    /// the connection. Without a guard every hello is admitted.
    pub fn with_guard(
        mut self,
        guard: impl Fn(&PeerId, Option<&str>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.guard = Some(Arc::new(guard));
        self
    }

    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.heartbeat = heartbeat;
        self
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
