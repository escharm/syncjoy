use std::sync::atomic::Ordering;
use std::sync::Arc;

use syncjoy_protocol::{PeerId, PeerToRelay};
use tokio::sync::mpsc::unbounded_channel;

use crate::peer_link::{PeerLink, PeerLinkInner};
use crate::relay_link::{complete_dial, dispatch, ChannelEntry, Command, Core, Event, LinkHandle};
use crate::transport::{self, DataChannel, PeerConnection};

const DATA_CHANNEL_LABEL: &str = "syncjoy";

#[derive(Clone, Copy)]
enum OpenAction {
    Dial(u64),
    Incoming,
}

pub(crate) async fn dial(
    core: &Arc<Core>,
    req: u64,
    to: &PeerId,
    extra_ice: Vec<syncjoy_protocol::IceServer>,
) {
    let mut ice = core.ice.lock().unwrap().clone();
    ice.extend(extra_ice);
    let pc = match transport::new_peer_connection(ice).await {
        Ok(pc) => pc,
        Err(error) => {
            complete_dial(core, req, Err(error));
            return;
        }
    };
    core.conns.lock().unwrap().insert(to.clone(), pc.clone());
    forward_ice(core, &pc, to);

    let dc = match pc.create_data_channel(DATA_CHANNEL_LABEL).await {
        Ok(dc) => dc,
        Err(error) => {
            teardown(core, to);
            complete_dial(core, req, Err(error));
            return;
        }
    };
    wire_data_channel(core, to, dc, OpenAction::Dial(req));

    let sdp = match pc.create_offer().await {
        Ok(sdp) => sdp,
        Err(error) => {
            teardown(core, to);
            complete_dial(core, req, Err(error));
            return;
        }
    };
    let _ = core.commands.send(Command::Signal(PeerToRelay::Offer {
        to: to.clone(),
        sdp,
    }));
}

pub(crate) async fn answer(core: &Arc<Core>, from: &PeerId, sdp: &str) {
    if connection_alive(core, from) {
        return;
    }
    if !core.accept_incoming(from).await {
        let _ = core
            .commands
            .send(Command::Signal(PeerToRelay::Decline { to: from.clone() }));
        return;
    }
    teardown(core, from);
    let ice = core.ice.lock().unwrap().clone();
    let pc = match transport::new_peer_connection(ice).await {
        Ok(pc) => pc,
        Err(error) => {
            tracing::warn!(peer = %from, %error, "create peer connection failed");
            return;
        }
    };
    core.conns.lock().unwrap().insert(from.clone(), pc.clone());
    forward_ice(core, &pc, from);

    let core_for_dc = Arc::clone(core);
    let peer_for_dc = from.clone();
    pc.on_data_channel(Box::new(move |dc| {
        let core = Arc::clone(&core_for_dc);
        let peer = peer_for_dc.clone();
        transport::spawn(async move {
            attach_incoming(core, peer, dc).await;
        });
    }));

    if pc.set_remote_offer(sdp).await.is_err() {
        teardown(core, from);
        return;
    }
    let sdp = match pc.create_answer().await {
        Ok(sdp) => sdp,
        Err(error) => {
            tracing::warn!(peer = %from, %error, "create answer failed");
            teardown(core, from);
            return;
        }
    };
    let _ = core.commands.send(Command::Signal(PeerToRelay::Answer {
        to: from.clone(),
        sdp,
    }));
}

pub(crate) async fn complete(core: &Arc<Core>, from: &PeerId, sdp: &str) {
    let pc = core.conns.lock().unwrap().get(from).cloned();
    if let Some(pc) = pc {
        let _ = pc.set_remote_answer(sdp).await;
    }
}

pub(crate) async fn add_ice(
    core: &Arc<Core>,
    from: &PeerId,
    candidate: &syncjoy_protocol::IceCandidate,
) {
    let pc = core.conns.lock().unwrap().get(from).cloned();
    if let Some(pc) = pc {
        pc.add_ice_candidate(candidate).await;
    }
}

pub(crate) fn teardown(core: &Arc<Core>, peer: &PeerId) {
    let pc = core.conns.lock().unwrap().remove(peer);
    core.retire(peer);
    if let Some(pc) = pc {
        pc.close();
    }
}

fn forward_ice(core: &Arc<Core>, pc: &PeerConnection, to: &PeerId) {
    let commands = core.commands.clone();
    let to = to.clone();
    pc.on_ice_candidate(Box::new(move |candidate| {
        let _ = commands.send(Command::Signal(PeerToRelay::Ice {
            to: to.clone(),
            candidate,
        }));
    }));
}

fn connection_alive(core: &Arc<Core>, peer: &PeerId) -> bool {
    let channels = core.channels.lock().unwrap();
    channels.get(peer).is_some_and(|entry| entry.dc.is_open())
}

async fn attach_incoming(core: Arc<Core>, peer: PeerId, dc: DataChannel) {
    let already_open = dc.is_open();
    let inner = wire_data_channel(&core, &peer, dc, OpenAction::Incoming);
    if already_open {
        if let Some(inner) = inner {
            fire_open(&core, &peer, inner, OpenAction::Incoming);
        }
    }
}

fn wire_data_channel(
    core: &Arc<Core>,
    peer: &PeerId,
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
    let msg_dc = inner.dc.clone();
    msg_dc.on_message(Box::new(move |bytes| {
        dispatch(&core_for_msg, &peer_for_msg, bytes);
    }));

    let core_for_open = Arc::clone(core);
    let peer_for_open = peer.clone();
    let open_inner = Arc::clone(&inner);
    let open_dc = inner.dc.clone();
    open_dc.on_open(Box::new(move || {
        let handed = Arc::clone(&open_inner);
        fire_open(&core_for_open, &peer_for_open, handed, action);
    }));

    let peer_for_close = peer.clone();
    let close_dc = inner.dc.clone();
    let close_core = Arc::clone(core);
    inner.dc.on_close(Box::new(move || {
        close_core.retire_channel(&peer_for_close, &close_dc);
    }));

    Some(inner)
}

fn fire_open(core: &Arc<Core>, peer: &PeerId, inner: Arc<PeerLinkInner>, action: OpenAction) {
    if inner.announced.swap(true, Ordering::Relaxed) {
        return;
    }
    core.pending.lock().unwrap().remove(peer);
    let peer_link = PeerLink::new(inner);
    match action {
        OpenAction::Dial(req) => complete_dial(core, req, Ok(peer_link)),
        OpenAction::Incoming => {
            let _ = core.events.send(Event::Channel(peer_link));
        }
    }
}
