use crate::error::PeerError;
use crate::relay_link::RelayLink;
use crate::signaling;
use syncjoy_protocol::{IceServer, PeerId};

pub struct Peer {
    pub(crate) id: PeerId,
    pub(crate) ice_servers: Vec<IceServer>,
    pub(crate) relay_token: Option<String>,
}

#[derive(Default)]
pub struct PeerBuilder {
    id: Option<String>,
    ice_servers: Vec<IceServer>,
    relay_token: Option<String>,
}

impl PeerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Seed the ICE configuration with your own STUN/TURN servers.
    /// Providing any replaces the built-in default STUN; the relay's TURN
    /// credentials (latest issued) are merged in at dial time.
    pub fn ice_server(mut self, server: IceServer) -> Self {
        self.ice_servers.push(server);
        self
    }

    /// Credential presented in the hello frame when the relay runs an
    /// admission guard. Sent inside the signaling channel, never in the
    /// dial URL.
    pub fn relay_token(mut self, token: impl Into<String>) -> Self {
        self.relay_token = Some(token.into());
        self
    }

    pub fn build(self) -> Peer {
        Peer {
            id: PeerId::new(self.id.expect("id is required")),
            ice_servers: self.ice_servers,
            relay_token: self.relay_token,
        }
    }
}

impl Peer {
    pub fn builder() -> PeerBuilder {
        PeerBuilder::new()
    }

    pub fn id(&self) -> &PeerId {
        &self.id
    }

    pub async fn connect_relay(&self, relay_url: &str) -> Result<RelayLink, PeerError> {
        signaling::connect_relay(self, relay_url).await
    }
}
