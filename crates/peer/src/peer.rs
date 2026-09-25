use crate::error::PeerError;
use crate::relay_link::RelayLink;
use crate::signaling;
use syncjoy_protocol::PeerId;

pub struct Peer {
    pub(crate) id: PeerId,
}

#[derive(Default)]
pub struct PeerBuilder {
    id: Option<String>,
}

impl PeerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn build(self) -> Peer {
        Peer {
            id: PeerId::new(self.id.expect("id is required")),
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
