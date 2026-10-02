use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use syncjoy_protocol::PeerId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Mutex};

use crate::error::PeerError;
use crate::relay_link::{CloseHandler, Command, LinkHandle, MsgHandler};
use crate::transport::{self, DataChannel};

#[derive(Clone)]
pub struct PeerLink {
    pub(crate) inner: Arc<PeerLinkInner>,
}

pub(crate) struct PeerLinkInner {
    pub(crate) peer: PeerId,
    pub(crate) generation: u64,
    pub(crate) announced: AtomicBool,
    pub(crate) dc: DataChannel,
    pub(crate) link: LinkHandle,
    pub(crate) closed: watch::Sender<bool>,
    close_slot: Arc<std::sync::Mutex<Option<Arc<dyn CloseHandler>>>>,
    close_watched: AtomicBool,
    commands: UnboundedSender<Command>,
    inbound: Mutex<Option<UnboundedReceiver<Bytes>>>,
}

impl Drop for PeerLinkInner {
    fn drop(&mut self) {
        let _ = self.closed.send(true);
        if self.link.core.link_alive.load(Ordering::Relaxed) {
            let _ = self.commands.send(Command::Close {
                peer: self.peer.clone(),
                generation: self.generation,
            });
        } else {
            self.link.core.teardown_if(&self.peer, self.generation);
        }
    }
}

impl PeerLinkInner {
    pub(crate) fn new(
        peer: PeerId,
        generation: u64,
        dc: DataChannel,
        link: LinkHandle,
        commands: UnboundedSender<Command>,
        inbound: UnboundedReceiver<Bytes>,
    ) -> Self {
        let (closed, _) = watch::channel(false);
        Self {
            peer,
            generation,
            announced: AtomicBool::new(false),
            dc,
            link,
            closed,
            close_slot: Arc::new(std::sync::Mutex::new(None)),
            close_watched: AtomicBool::new(false),
            commands,
            inbound: Mutex::new(Some(inbound)),
        }
    }
}

impl PeerLink {
    pub(crate) fn new(inner: Arc<PeerLinkInner>) -> Self {
        Self { inner }
    }

    pub fn peer(&self) -> &PeerId {
        &self.inner.peer
    }

    pub async fn send(&self, payload: impl AsRef<[u8]>) -> Result<(), PeerError> {
        if !self.inner.dc.is_open() {
            return Err(PeerError::Closed);
        }
        self.inner
            .dc
            .send(Bytes::copy_from_slice(payload.as_ref()))
            .await
            .map_err(PeerError::Rtc)
    }

    /// Consume this link's incoming queue through `handler`, in arrival order.
    /// Exclusive with `recv`: after this call `recv` yields `None`.
    pub async fn on_msg(&mut self, handler: impl MsgHandler) {
        let Some(mut inbound) = self.inner.inbound.lock().await.take() else {
            return;
        };
        let handler = Arc::new(handler);
        let link = self.inner.link.clone();
        let from = self.inner.peer.clone();
        let keeper = self.clone();
        transport::spawn(async move {
            while let Some(bytes) = inbound.recv().await {
                handler.on_msg(link.clone(), from.clone(), bytes).await;
            }
            drop(keeper);
        });
    }

    pub fn close(&self) {
        if self.inner.link.core.link_alive.load(Ordering::Relaxed) {
            let _ = self.inner.commands.send(Command::Close {
                peer: self.inner.peer.clone(),
                generation: self.inner.generation,
            });
        } else {
            self.inner
                .link
                .core
                .teardown_if(&self.inner.peer, self.inner.generation);
        }
    }

    /// Invoke the registered handler once this connection closes, whatever the
    /// cause: remote close, local `close`/`close_peer`, teardown, or the last
    /// handle dropping. One handler slot per link: re-registering replaces,
    /// the callback fires at most once.
    pub fn on_close(&self, handler: impl CloseHandler) {
        *self.inner.close_slot.lock().unwrap() = Some(Arc::new(handler));
        if self.inner.close_watched.swap(true, Ordering::Relaxed) {
            return;
        }
        let mut closed = self.inner.closed.subscribe();
        let slot = Arc::clone(&self.inner.close_slot);
        let link = self.inner.link.clone();
        let peer = self.inner.peer.clone();
        transport::spawn(async move {
            while !*closed.borrow() {
                if closed.changed().await.is_err() {
                    break;
                }
            }
            let handler = slot.lock().unwrap().clone();
            if let Some(handler) = handler {
                handler.on_close(link, peer).await;
            }
        });
    }

    pub async fn recv(&mut self) -> Option<Bytes> {
        let mut inbound = self.inner.inbound.lock().await;
        match inbound.as_mut() {
            Some(receiver) => receiver.recv().await,
            None => None,
        }
    }
}
