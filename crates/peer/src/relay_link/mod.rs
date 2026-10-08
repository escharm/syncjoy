mod link;

pub use link::{LinkHandle, RelayLink};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::error::PeerError;
use crate::lock;
use crate::peer_link::{PeerLink, PeerLinkInner};
use crate::transport::{DataChannel, PeerConnection};
use async_trait::async_trait;
use bytes::Bytes;
use syncjoy_protocol::IceServer;
use syncjoy_protocol::PeerId;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, watch};

pub(crate) const DIAL_TIMED_OUT: &str = "dial timed out";
pub(crate) const RELAY_LINK_CLOSED: &str = "relay link closed";
pub(crate) const HOST_CLOSED: &str = "closed by host";

pub enum Event {
    Channel(PeerLink),
    /// The signaling link to the relay died; `reason` names the cause (socket
    /// closed, invalid frame, send failure) so the host can decide whether to
    /// reconnect. Existing direct channels keep working, in-flight dials fail
    /// immediately, and reconnecting requires a fresh `connect_relay`.
    RelayClosed { reason: String },
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
        to: PeerId,
        ice: Vec<IceServer>,
    },
    /// Tear down the connection for `peer` only if it is still the one
    /// identified by `generation` (a stale handle must not kill a rebuilt one).
    Close {
        peer: PeerId,
        generation: u64,
    },
    /// Tear down whatever connection to `peer` currently exists.
    ClosePeer {
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
    pub(crate) generation: u64,
    pub(crate) dc: DataChannel,
    pub(crate) inbound: UnboundedSender<Bytes>,
    pub(crate) link: Weak<PeerLinkInner>,
}

pub(crate) struct ConnEntry {
    pub(crate) generation: u64,
    pub(crate) pc: PeerConnection,
}

pub(crate) struct DialWaiter {
    pub(crate) generation: u64,
    pub(crate) senders: Vec<oneshot::Sender<Result<PeerLink, String>>>,
}

pub(crate) struct Core {
    pub(crate) commands: UnboundedSender<Command>,
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) link_alive: AtomicBool,
    pub(crate) incoming: Mutex<Option<Arc<dyn IncomingHandler>>>,
    pub(crate) pending: Mutex<HashMap<PeerId, Arc<PeerLinkInner>>>,
    pub(crate) events: UnboundedSender<Event>,
    pub(crate) dials: Mutex<HashMap<PeerId, DialWaiter>>,
    pub(crate) conns: Mutex<HashMap<PeerId, ConnEntry>>,
    pub(crate) channels: Mutex<HashMap<PeerId, ChannelEntry>>,
    pub(crate) ice: Vec<IceServer>,
    pub(crate) relay_turn: Mutex<Option<IceServer>>,
    pub(crate) next_gen: AtomicU64,
}

impl Core {
    /// Concurrent dials to one target coalesce into the in-flight dial and
    /// share its outcome; a decline or timeout fails every coalesced caller.
    pub(crate) async fn connect_peer(
        &self,
        target: PeerId,
        options: ConnectOptions,
    ) -> Result<PeerLink, PeerError> {
        if let Some(existing) = self.live_link(&target) {
            return Ok(existing);
        }
        if !self.link_alive.load(Ordering::Relaxed) {
            return Err(PeerError::Closed);
        }
        let (sender, opened) = oneshot::channel();
        {
            let mut dials = lock(&self.dials);
            match dials.get_mut(&target) {
                Some(waiter) => waiter.senders.push(sender),
                None => {
                    dials.insert(
                        target.clone(),
                        DialWaiter {
                            generation: 0,
                            senders: vec![sender],
                        },
                    );
                    if self
                        .commands
                        .send(Command::Dial {
                            to: target.clone(),
                            ice: options.ice_servers,
                        })
                        .is_err()
                    {
                        dials.remove(&target);
                        return Err(PeerError::Closed);
                    }
                    if !self.link_alive.load(Ordering::Relaxed) {
                        fail_dials_to(self, &target, RELAY_LINK_CLOSED.to_owned());
                        return Err(PeerError::Closed);
                    }
                }
            }
        }
        match opened.await {
            Ok(Ok(link)) => Ok(link),
            Ok(Err(reason)) if reason == DIAL_TIMED_OUT => Err(PeerError::Timeout),
            Ok(Err(reason)) => Err(PeerError::Rejected(reason)),
            Err(_) => Err(PeerError::Closed),
        }
    }

    pub(crate) async fn accept_incoming(&self, from: &PeerId) -> bool {
        let handler = lock(&self.incoming).clone();
        match handler {
            None => true,
            Some(handler) => handler.incoming(from).await,
        }
    }

    fn live_link(&self, target: &PeerId) -> Option<PeerLink> {
        let channels = lock(&self.channels);
        let entry = channels.get(target)?;
        let inner = entry.link.upgrade()?;
        if !inner.dc.is_open() {
            return None;
        }
        Some(PeerLink::new(inner))
    }

    pub(crate) fn current_ice(&self) -> Vec<IceServer> {
        let mut servers = self.ice.clone();
        if let Some(turn) = lock(&self.relay_turn).clone() {
            servers.push(turn);
        }
        servers
    }

    pub(crate) async fn send(&self, to: PeerId, bytes: Bytes) -> Result<(), PeerError> {
        let dc = lock(&self.channels)
            .get(&to)
            .map(|entry| entry.dc.clone());
        match dc {
            Some(dc) if dc.is_open() => dc.send(bytes).await.map_err(PeerError::Rtc),
            _ => Err(PeerError::Rejected(format!("not connected to {to}"))),
        }
    }

    pub(crate) fn retire(&self, peer: &PeerId) {
        lock(&self.pending).remove(peer);
        let Some(entry) = lock(&self.channels).remove(peer) else {
            return;
        };
        if let Some(inner) = entry.link.upgrade() {
            let _ = inner.closed.send(true);
        }
    }

    pub(crate) fn teardown_all(&self, peer: &PeerId) {
        let pc = lock(&self.conns).remove(peer).map(|entry| entry.pc);
        self.retire(peer);
        if let Some(pc) = pc {
            pc.close();
        }
    }

    pub(crate) fn teardown_if(&self, peer: &PeerId, generation: u64) {
        let pc = {
            let mut conns = lock(&self.conns);
            match conns.get(peer) {
                Some(entry) if entry.generation == generation => {
                    conns.remove(peer).map(|entry| entry.pc)
                }
                _ => None,
            }
        };
        let retired = {
            let mut channels = lock(&self.channels);
            match channels.get(peer) {
                Some(entry) if entry.generation == generation => channels.remove(peer),
                _ => None,
            }
        };
        if let Some(entry) = retired {
            lock(&self.pending).remove(peer);
            if let Some(inner) = entry.link.upgrade() {
                let _ = inner.closed.send(true);
            }
        }
        if let Some(pc) = pc {
            pc.close();
        }
    }

    /// Variant for a DataChannel close callback: a rebuilt connection for the
    /// same peer must survive the old channel's late close event.
    pub(crate) fn retire_channel(&self, peer: &PeerId, dc: &DataChannel) {
        let current = {
            let channels = lock(&self.channels);
            channels.get(peer).map(|entry| (entry.generation, entry.dc.clone()))
        };
        match current {
            Some((generation, current)) => {
                if !current.same(dc) {
                    return;
                }
                self.retire(peer);
                let pc = {
                    let mut conns = lock(&self.conns);
                    match conns.get(peer) {
                        Some(entry) if entry.generation == generation => {
                            conns.remove(peer).map(|entry| entry.pc)
                        }
                        _ => None,
                    }
                };
                if let Some(pc) = pc {
                    pc.close();
                }
            }
            None => self.retire(peer),
        }
    }
}

pub(crate) fn dispatch(core: &Arc<Core>, from: &PeerId, bytes: Bytes) {
        let channels = lock(&core.channels);
        if let Some(entry) = channels.get(from) {
        let _ = entry.inbound.send(bytes);
    }
}

pub(crate) fn fail_dials_to(core: &Core, to: &PeerId, reason: String) {
    let mut dials = lock(&core.dials);
    if let Some(waiter) = dials.remove(to) {
        for sender in waiter.senders {
            let _ = sender.send(Err(reason.clone()));
        }
    }
}

pub(crate) fn fail_all_dials(core: &Core, reason: String) {
    let mut dials = lock(&core.dials);
    for (_, waiter) in dials.drain() {
        for sender in waiter.senders {
            let _ = sender.send(Err(reason.clone()));
        }
    }
}
