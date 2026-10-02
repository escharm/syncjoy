use std::sync::Arc;

use syncjoy_protocol::PeerId;
use tokio::sync::mpsc::unbounded_channel;

use crate::peer_link::{PeerLink, PeerLinkInner};
use crate::relay_link::{dispatch, ChannelEntry, Core, Event, LinkHandle};
use crate::transport::DataChannel;

#[derive(Clone, Copy)]
pub(crate) enum OpenAction {
    Dial,
    Incoming,
}

/// Resolve every coalesced caller of the dial identified by `generation`.
/// A late success with no caller left tears the fresh channel down.
pub(crate) fn settle_dial(
    core: &Arc<Core>,
    to: &PeerId,
    generation: u64,
    result: Result<PeerLink, String>,
) {
    let senders = {
        let mut dials = core.dials.lock().unwrap();
        let matches = dials
            .get(to)
            .is_some_and(|waiter| waiter.generation == generation);
        if matches {
            dials
                .remove(to)
                .map(|waiter| waiter.senders)
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    };
    if senders.is_empty() {
        if let Ok(link) = &result {
            core.teardown_if(link.peer(), generation);
        }
        return;
    }
    match result {
        Ok(link) => {
            for sender in senders {
                let _ = sender.send(Ok(link.clone()));
            }
        }
        Err(reason) => {
            core.teardown_if(to, generation);
            for sender in senders {
                let _ = sender.send(Err(reason.clone()));
            }
        }
    }
}

pub(crate) async fn attach_incoming(
    core: Arc<Core>,
    peer: PeerId,
    generation: u64,
    dc: DataChannel,
) {
    let already_open = dc.is_open();
    let inner = wire_data_channel(&core, &peer, generation, dc, OpenAction::Incoming);
    if already_open {
        if let Some(inner) = inner {
            fire_open(&core, &peer, inner, OpenAction::Incoming);
        }
    }
}

pub(crate) fn wire_data_channel(
    core: &Arc<Core>,
    peer: &PeerId,
    generation: u64,
    dc: DataChannel,
    action: OpenAction,
) -> Option<Arc<PeerLinkInner>> {
    let (inbound_tx, inbound_rx) = unbounded_channel();
    let inner = {
        let mut channels = core.channels.lock().unwrap();
        if channels.contains_key(peer) {
            return None;
        }
        let inner = Arc::new(PeerLinkInner::new(
            peer.clone(),
            generation,
            dc.clone(),
            LinkHandle {
                core: Arc::clone(core),
            },
            core.commands.clone(),
            inbound_rx,
        ));
        channels.insert(
            peer.clone(),
            ChannelEntry {
                generation,
                dc,
                inbound: inbound_tx,
                link: Arc::downgrade(&inner),
            },
        );
        core.pending
            .lock()
            .unwrap()
            .insert(peer.clone(), Arc::clone(&inner));
        inner
    };

    let core_for_msg = Arc::clone(core);
    let peer_for_msg = peer.clone();
    inner.dc.on_message(Box::new(move |bytes| {
        dispatch(&core_for_msg, &peer_for_msg, bytes);
    }));

    let core_for_open = Arc::clone(core);
    let peer_for_open = peer.clone();
    let open_inner = Arc::downgrade(&inner);
    inner.dc.on_open(Box::new(move || {
        if let Some(inner) = open_inner.upgrade() {
            fire_open(&core_for_open, &peer_for_open, inner, action);
        }
    }));

    let peer_for_close = peer.clone();
    let close_inner = Arc::downgrade(&inner);
    let close_core = Arc::clone(core);
    inner.dc.on_close(Box::new(move || {
        if let Some(inner) = close_inner.upgrade() {
            close_core.retire_channel(&peer_for_close, &inner.dc);
        }
    }));

    Some(inner)
}

fn fire_open(core: &Arc<Core>, peer: &PeerId, inner: Arc<PeerLinkInner>, action: OpenAction) {
    if inner
        .announced
        .swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        return;
    }
    core.pending.lock().unwrap().remove(peer);
    let peer_link = PeerLink::new(inner.clone());
    match action {
        OpenAction::Dial => {
            settle_dial(core, peer, inner.generation, Ok(peer_link));
        }
        OpenAction::Incoming => {
            let _ = core.events.send(Event::Channel(peer_link));
        }
    }
}
