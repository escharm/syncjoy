mod channel;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use syncjoy_protocol::{IceServer, PeerId, PeerToRelay};

use crate::lock;
use crate::relay_link::{Command, ConnEntry, Core, DIAL_TIMED_OUT};
use crate::transport::{self, PeerConnection};
use channel::{settle_dial, wire_data_channel, OpenAction};

const DATA_CHANNEL_LABEL: &str = "syncjoy";
const DIAL_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) async fn dial(core: &Arc<Core>, to: &PeerId, extra_ice: Vec<IceServer>) {
    let generation = core.next_gen.fetch_add(1, Ordering::Relaxed);
    {
        let mut dials = lock(&core.dials);
        match dials.get_mut(to) {
            Some(waiter) => waiter.generation = generation,
            None => return,
        }
    }

    let mut ice = core.current_ice();
    ice.extend(extra_ice);
    let pc = match transport::new_peer_connection(ice).await {
        Ok(pc) => pc,
        Err(error) => {
            settle_dial(core, to, generation, Err(error));
            return;
        }
    };
    lock(&core.conns).insert(
        to.clone(),
        ConnEntry {
            generation,
            pc: pc.clone(),
        },
    );
    forward_ice(core, &pc, to);

    let dc = match pc.create_data_channel(DATA_CHANNEL_LABEL).await {
        Ok(dc) => dc,
        Err(error) => {
            core.teardown_if(to, generation);
            settle_dial(core, to, generation, Err(error));
            return;
        }
    };
    wire_data_channel(core, to, generation, dc, OpenAction::Dial);

    let sdp = match pc.create_offer().await {
        Ok(sdp) => sdp,
        Err(error) => {
            core.teardown_if(to, generation);
            settle_dial(core, to, generation, Err(error));
            return;
        }
    };
    let _ = core.commands.send(Command::Signal(PeerToRelay::Offer {
        to: to.clone(),
        sdp,
    }));

    let watchdog = Arc::clone(core);
    let watched = to.clone();
    transport::spawn(async move {
        transport::sleep(DIAL_TIMEOUT).await;
        settle_dial(
            &watchdog,
            &watched,
            generation,
            Err(DIAL_TIMED_OUT.to_owned()),
        );
    });
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
    core.teardown_all(from);
    let generation = core.next_gen.fetch_add(1, Ordering::Relaxed);
    let ice = core.current_ice();
    let pc = match transport::new_peer_connection(ice).await {
        Ok(pc) => pc,
        Err(error) => {
            tracing::warn!(peer = %from, %error, "create peer connection failed");
            return;
        }
    };
    lock(&core.conns).insert(
        from.clone(),
        ConnEntry {
            generation,
            pc: pc.clone(),
        },
    );
    forward_ice(core, &pc, from);

    let core_for_dc = Arc::clone(core);
    let peer_for_dc = from.clone();
    pc.on_data_channel(Box::new(move |dc| {
        let core = Arc::clone(&core_for_dc);
        let peer = peer_for_dc.clone();
        transport::spawn(async move {
            channel::attach_incoming(core, peer, generation, dc).await;
        });
    }));

    if pc.set_remote_offer(sdp).await.is_err() {
        core.teardown_if(from, generation);
        return;
    }
    let sdp = match pc.create_answer().await {
        Ok(sdp) => sdp,
        Err(error) => {
            tracing::warn!(peer = %from, %error, "create answer failed");
            core.teardown_if(from, generation);
            return;
        }
    };
    let _ = core.commands.send(Command::Signal(PeerToRelay::Answer {
        to: from.clone(),
        sdp,
    }));
}

pub(crate) async fn complete(core: &Arc<Core>, from: &PeerId, sdp: &str) {
    let pc = lock(&core.conns)
        .get(from)
        .map(|entry| entry.pc.clone());
    if let Some(pc) = pc {
        let _ = pc.set_remote_answer(sdp).await;
    }
}

pub(crate) async fn add_ice(
    core: &Arc<Core>,
    from: &PeerId,
    candidate: &syncjoy_protocol::IceCandidate,
) {
    let pc = lock(&core.conns)
        .get(from)
        .map(|entry| entry.pc.clone());
    if let Some(pc) = pc {
        pc.add_ice_candidate(candidate).await;
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
    let channels = lock(&core.channels);
    channels.get(peer).is_some_and(|entry| entry.dc.is_open())
}
