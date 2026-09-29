use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::error::PeerError;
use crate::peer_link::{PeerLink, PeerLinkInner};
use crate::transport::{self, DataChannel, PeerConnection};
use async_trait::async_trait;
use bytes::Bytes;
use syncjoy_protocol::IceServer;
use syncjoy_protocol::PeerId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

pub enum Event {
    Channel(PeerLink),
}

#[async_trait]
pub trait MsgHandler: Send + Sync + 'static {
    async fn on_msg(&self, link: LinkHandle, from: PeerId, payload: Bytes);
}

#[async_trait]
pub trait CloseHandler: Send + Sync + 'static {
    async fn on_close(&self, link: LinkHandle, peer: PeerId);
}

/// Decides whether an incoming connection request is accepted.
/// Unset means accept everything; returning `false` declines with a wire-level
/// `decline`, which the dialer observes as `Rejected("declined by peer")`.
#[async_trait]
pub trait IncomingHandler: Send + Sync + 'static {
    async fn incoming(&self, from: &PeerId) -> bool;
}

pub(crate) enum Command {
    Signal(syncjoy_protocol::PeerToRelay),
    Dial {
        req: u64,
        to: PeerId,
        ice: Vec<IceServer>,
    },
    Close {
        peer: PeerId,
    },
}

/// Per-dial options; `ice_servers` are merged over the link-level ICE
/// configuration (builder seeds + relay-pushed TURN) for this dial only.
#[derive(Default)]
pub struct ConnectOptions {
    pub ice_servers: Vec<IceServer>,
}

pub(crate) struct ChannelEntry {
    pub(crate) dc: DataChannel,
    pub(crate) inbound: UnboundedSender<Bytes>,
    pub(crate) link: Weak<PeerLinkInner>,
}

pub(crate) struct DialWaiter {
    pub(crate) to: PeerId,
    pub(crate) sender: oneshot::Sender<Result<PeerLink, String>>,
}

pub(crate) struct Core {
    pub(crate) commands: UnboundedSender<Command>,
    pub(crate) incoming: Mutex<Option<Arc<dyn IncomingHandler>>>,
    pub(crate) pending: Mutex<HashMap<PeerId, Arc<PeerLinkInner>>>,
    pub(crate) events: UnboundedSender<Event>,
    pub(crate) dials: Mutex<HashMap<u64, DialWaiter>>,
    pub(crate) conns: Mutex<HashMap<PeerId, PeerConnection>>,
    pub(crate) channels: Mutex<HashMap<PeerId, ChannelEntry>>,
    pub(crate) ice: Mutex<Vec<IceServer>>,
    pub(crate) next_req: AtomicU64,
}

const DIAL_TIMEOUT: Duration = Duration::from_secs(15);

impl Core {
    pub(crate) async fn connect_peer(
        &self,
        target: PeerId,
        options: ConnectOptions,
    ) -> Result<PeerLink, PeerError> {
        if let Some(existing) = self.live_link(&target) {
            return Ok(existing);
        }
        let req = self.next_req.fetch_add(1, Ordering::Relaxed);
        let (sender, opened) = oneshot::channel();
        self.dials.lock().unwrap().insert(
            req,
            DialWaiter {
                to: target.clone(),
                sender,
            },
        );
        if self
            .commands
            .send(Command::Dial {
                req,
                to: target.clone(),
                ice: options.ice_servers,
            })
            .is_err()
        {
            self.dials.lock().unwrap().remove(&req);
            return Err(PeerError::Closed);
        }

        match transport::timeout(DIAL_TIMEOUT, opened).await {
            Ok(Ok(result)) => result.map_err(PeerError::Rejected),
            Ok(Err(_)) => Err(PeerError::Closed),
            Err(_) => {
                self.dials.lock().unwrap().remove(&req);
                let _ = self.commands.send(Command::Close { peer: target });
                Err(PeerError::Timeout)
            }
        }
    }

    pub(crate) async fn accept_incoming(&self, from: &PeerId) -> bool {
        let handler = self.incoming.lock().unwrap().clone();
        match handler {
            None => true,
            Some(handler) => handler.incoming(from).await,
        }
    }

    fn live_link(&self, target: &PeerId) -> Option<PeerLink> {
        let channels = self.channels.lock().unwrap();
        let entry = channels.get(target)?;
        let inner = entry.link.upgrade()?;
        if !inner.dc.is_open() {
            return None;
        }
        Some(PeerLink::new(inner))
    }

    pub(crate) async fn send(&self, to: PeerId, bytes: Bytes) -> Result<(), PeerError> {
        let dc = self
            .channels
            .lock()
            .unwrap()
            .get(&to)
            .map(|entry| entry.dc.clone());
        match dc {
            Some(dc) if dc.is_open() => dc.send(bytes).await.map_err(PeerError::Rtc),
            _ => Err(PeerError::Rejected(format!("not connected to {to}"))),
        }
    }

    pub(crate) fn retire(&self, peer: &PeerId) {
        self.pending.lock().unwrap().remove(peer);
        let Some(entry) = self.channels.lock().unwrap().remove(peer) else {
            return;
        };
        if let Some(inner) = entry.link.upgrade() {
            let _ = inner.closed.send(true);
        }
    }

    /// Variant for a DataChannel close callback: a rebuilt connection for the
    /// same peer must survive the old channel's late close event.
    pub(crate) fn retire_channel(&self, peer: &PeerId, dc: &DataChannel) {
        let current = {
            let channels = self.channels.lock().unwrap();
            channels.get(peer).map(|entry| entry.dc.clone())
        };
        if let Some(current) = current {
            if !current.same(dc) {
                return;
            }
        }
        self.retire(peer);
    }
}

pub(crate) fn dispatch(core: &Arc<Core>, from: &PeerId, bytes: Bytes) {
    let channels = core.channels.lock().unwrap();
    if let Some(entry) = channels.get(from) {
        let _ = entry.inbound.send(bytes);
    }
}

pub(crate) fn complete_dial(core: &Arc<Core>, req: u64, result: Result<PeerLink, String>) {
    let Some(waiter) = core.dials.lock().unwrap().remove(&req) else {
        if let Ok(link) = &result {
            crate::rtc::teardown(core, link.peer());
        }
        return;
    };
    let _ = waiter.sender.send(result);
}

pub(crate) fn fail_dials_to(core: &Core, to: &PeerId, reason: String) {
    let mut dials = core.dials.lock().unwrap();
    let reqs: Vec<u64> = dials
        .iter()
        .filter(|(_, waiter)| &waiter.to == to)
        .map(|(req, _)| *req)
        .collect();
    for req in reqs {
        if let Some(waiter) = dials.remove(&req) {
            let _ = waiter.sender.send(Err(reason.clone()));
        }
    }
}

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
        let _ = self.core.commands.send(Command::Close {
            peer: target.into(),
        });
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
