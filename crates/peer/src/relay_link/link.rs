use std::sync::Arc;

use bytes::Bytes;

use crate::error::PeerError;
use crate::peer_link::PeerLink;
use syncjoy_protocol::PeerId;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{Command, ConnectOptions, Core, Event, IncomingHandler};

#[derive(Clone)]
pub struct LinkHandle {
    pub(crate) core: Arc<Core>,
}

impl LinkHandle {
    pub async fn connect_peer(&self, target: impl Into<PeerId>) -> Result<PeerLink, PeerError> {
        self.connect_peer_with(target, ConnectOptions::default())
            .await
    }

    pub async fn connect_peer_with(
        &self,
        target: impl Into<PeerId>,
        options: ConnectOptions,
    ) -> Result<PeerLink, PeerError> {
        self.core.connect_peer(target.into(), options).await
    }

    pub fn on_incoming(&self, handler: impl IncomingHandler) {
        *self.core.incoming.lock().unwrap() = Some(Arc::new(handler));
    }

    pub fn close_peer(&self, target: impl Into<PeerId>) {
        let peer = target.into();
        if self.core.link_alive.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = self.core.commands.send(Command::ClosePeer { peer });
        } else {
            self.core.teardown_all(&peer);
        }
    }

    pub async fn send(
        &self,
        to: impl Into<PeerId>,
        payload: impl AsRef<[u8]>,
    ) -> Result<(), PeerError> {
        self.core
            .send(to.into(), Bytes::copy_from_slice(payload.as_ref()))
            .await
    }
}

pub struct RelayLink {
    core: Arc<Core>,
    events: UnboundedReceiver<Event>,
}

impl RelayLink {
    pub(crate) fn new(core: Arc<Core>, events: UnboundedReceiver<Event>) -> Self {
        Self { core, events }
    }

    pub fn handle(&self) -> LinkHandle {
        LinkHandle {
            core: self.core.clone(),
        }
    }

    pub async fn connect_peer(&self, target: impl Into<PeerId>) -> Result<PeerLink, PeerError> {
        self.handle().connect_peer(target).await
    }

    pub async fn connect_peer_with(
        &self,
        target: impl Into<PeerId>,
        options: ConnectOptions,
    ) -> Result<PeerLink, PeerError> {
        self.handle().connect_peer_with(target, options).await
    }

    /// Close the signaling link deliberately: existing direct channels keep
    /// working, in-flight dials fail immediately, and later dials return
    /// `Closed` right away. No `RelayClosed` event is emitted (the host asked
    /// for this); rejoining the relay requires a fresh `connect_relay`.
    pub fn close(&self) {
        let _ = self.core.shutdown.send(true);
    }

    pub fn on_incoming(&self, handler: impl IncomingHandler) {
        *self.core.incoming.lock().unwrap() = Some(Arc::new(handler));
    }

    pub fn close_peer(&self, target: impl Into<PeerId>) {
        self.handle().close_peer(target)
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        self.events.recv().await
    }
}
