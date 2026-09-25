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
    pub(crate) dc: DataChannel,
    pub(crate) link: LinkHandle,
    pub(crate) closed: watch::Sender<bool>,
    commands: UnboundedSender<Command>,
    inbound: Mutex<Option<UnboundedReceiver<Bytes>>>,
}

impl Drop for PeerLinkInner {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Close {
            peer: self.peer.clone(),
        });
    }
}

impl PeerLinkInner {
    pub(crate) fn new(
        peer: PeerId,
        dc: DataChannel,
        link: LinkHandle,
        commands: UnboundedSender<Command>,
        inbound: UnboundedReceiver<Bytes>,
    ) -> Self {
        let (closed, _) = watch::channel(false);
        Self {
            peer,
            dc,
            link,
            closed,
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
        let _ = self.inner.commands.send(Command::Close {
            peer: self.inner.peer.clone(),
        });
    }

    /// Invoke `handler` once this connection closes, whatever the cause:
    /// remote close, local `close`/`close_peer`, teardown, or the last handle dropping.
    pub fn on_close(&self, handler: impl CloseHandler) {
        let mut closed = self.inner.closed.subscribe();
        let link = self.inner.link.clone();
        let peer = self.inner.peer.clone();
        let handler = Arc::new(handler);
        transport::spawn(async move {
            while !*closed.borrow() {
                if closed.changed().await.is_err() {
                    break;
                }
            }
            handler.on_close(link, peer).await;
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
