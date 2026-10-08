use std::collections::HashMap;
use std::sync::Mutex;

use syncjoy_protocol::{PeerId, RelayToPeer};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::lock;

#[derive(Default)]
pub struct PeerRegistry {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    generation: u64,
    peers: HashMap<PeerId, PeerSlot>,
}

struct PeerSlot {
    generation: u64,
    sender: UnboundedSender<RelayToPeer>,
}

pub struct RegisteredPeer {
    pub generation: u64,
    pub inbox: UnboundedReceiver<RelayToPeer>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, peer: &PeerId) -> RegisteredPeer {
        let (sender, inbox) = unbounded_channel();
        let mut inner = lock(&self.inner);
        inner.generation += 1;
        let generation = inner.generation;
        inner
            .peers
            .insert(peer.clone(), PeerSlot { generation, sender });
        RegisteredPeer { generation, inbox }
    }

    pub fn unregister(&self, peer: &PeerId, generation: u64) -> bool {
        let mut inner = lock(&self.inner);
        let is_current = inner
            .peers
            .get(peer)
            .is_some_and(|slot| slot.generation == generation);
        if is_current {
            inner.peers.remove(peer);
        }
        is_current
    }

    pub fn route(&self, to: &PeerId, msg: RelayToPeer) -> bool {
        let inner = lock(&self.inner);
        match inner.peers.get(to) {
            Some(slot) => slot.sender.send(msg).is_ok(),
            None => false,
        }
    }
}
