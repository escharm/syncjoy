use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use syncjoy_protocol::{decode, encode, PeerToRelay, RelayToPeer};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use crate::error::PeerError;
use crate::lock;
use crate::peer::Peer;
use crate::relay_link::{
    fail_all_dials, fail_dials_to, Command, Core, Event, RelayLink, HOST_CLOSED, RELAY_LINK_CLOSED,
};
use crate::rtc;
use crate::transport;
use syncjoy_protocol::IceServer;
use tokio::sync::watch;

const RELAY_PEER_PATH: &str = "/syncjoy/v1/peer";
const STUN_URL: &str = "stun:coturn.idealjs.com:3478";
const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn connect_relay(peer: &Peer, relay_url: &str) -> Result<RelayLink, PeerError> {
    let url = format!("{}{RELAY_PEER_PATH}", relay_url.trim_end_matches('/'));
    let (socket_tx, mut socket_rx) = transport::timeout(
        RELAY_CONNECT_TIMEOUT,
        transport::connect_signaling(url),
    )
    .await
    .map_err(|_| PeerError::Connect("relay handshake timed out".to_owned()))?
    .map_err(PeerError::Connect)?;

    let hello = encode(&PeerToRelay::Hello {
        peer_id: peer.id.clone(),
        token: peer.relay_token.clone(),
    });
    socket_tx.send(hello).map_err(|_| PeerError::Closed)?;
    expect_ack(&mut socket_rx).await?;

    let (commands, command_rx) = unbounded_channel::<Command>();
    let (events, event_rx) = unbounded_channel::<Event>();
    let (shutdown, _) = watch::channel(false);
    let core = Arc::new(Core {
        commands,
        shutdown,
        link_alive: AtomicBool::new(true),
        incoming: Mutex::new(None),
        pending: Mutex::new(HashMap::new()),
        events,
        dials: Mutex::new(HashMap::new()),
        conns: Mutex::new(HashMap::new()),
        channels: Mutex::new(HashMap::new()),
        ice: if peer.ice_servers.is_empty() {
            vec![IceServer::stun(STUN_URL)]
        } else {
            peer.ice_servers.clone()
        },
        relay_turn: Mutex::new(None),
        next_gen: AtomicU64::new(1),
    });
    transport::spawn(pump(socket_tx, socket_rx, core.clone(), command_rx));
    Ok(RelayLink::new(core, event_rx))
}

async fn expect_ack(socket_rx: &mut UnboundedReceiver<String>) -> Result<(), PeerError> {
    let text = transport::timeout(RELAY_CONNECT_TIMEOUT, socket_rx.recv())
        .await
        .map_err(|_| PeerError::Connect("relay handshake timed out".to_owned()))?;
    let Some(text) = text else {
        return Err(PeerError::Rejected("relay closed before ack".to_owned()));
    };
    match decode::<RelayToPeer>(&text)? {
        RelayToPeer::Ack => Ok(()),
        RelayToPeer::Rejected { reason, .. } => Err(PeerError::Rejected(reason)),
        other => Err(PeerError::Rejected(format!("{other:?}"))),
    }
}

async fn pump(
    socket_tx: transport::SignalingSender,
    mut socket_rx: UnboundedReceiver<String>,
    core: Arc<Core>,
    mut commands: UnboundedReceiver<Command>,
) {
    let (signals_tx, mut signals_rx) = unbounded_channel::<RelayToPeer>();
    let signal_core = Arc::clone(&core);
    transport::spawn(async move {
        while let Some(msg) = signals_rx.recv().await {
            handle_signal(&signal_core, msg).await;
        }
    });
    let mut shutdown_rx = core.shutdown.subscribe();
    let (reason, notify_host): (String, bool) = loop {
        tokio::select! {
            _ = watch_shutdown(&mut shutdown_rx) => {
                break (HOST_CLOSED.to_owned(), false);
            }
            text = socket_rx.recv() => match text {
                Some(text) => match decode::<RelayToPeer>(&text) {
                    Ok(RelayToPeer::Ping) => {
                        if socket_tx.send(encode(&PeerToRelay::Pong)).is_err() {
                            break ("signaling send failed".to_owned(), true);
                        }
                    }
                    Ok(msg) => {
                        if signals_tx.send(msg).is_err() {
                            break ("signaling processor stopped".to_owned(), true);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "invalid relay frame");
                        break (format!("invalid relay frame: {error}"), true);
                    }
                },
                None => break ("signaling socket closed".to_owned(), true),
            },
            command = commands.recv() => match command {
                Some(Command::Signal(msg)) => {
                    if socket_tx.send(encode(&msg)).is_err() {
                        break ("signaling send failed".to_owned(), true);
                    }
                }
                Some(Command::Dial { to, ice }) => {
                    rtc::dial(&core, &to, ice).await;
                }
                Some(Command::Close { peer, generation }) => {
                    core.teardown_if(&peer, generation);
                }
                Some(Command::ClosePeer { peer }) => {
                    core.teardown_all(&peer);
                }
                None => break ("link handles released".to_owned(), true),
            },
        }
    };
    core.link_alive.store(false, Ordering::Relaxed);
    fail_all_dials(&core, format!("{RELAY_LINK_CLOSED}: {reason}"));
    if notify_host {
        let _ = core.events.send(Event::RelayClosed { reason });
    }
}

async fn watch_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

async fn handle_signal(core: &Arc<Core>, msg: RelayToPeer) {
    match msg {
        RelayToPeer::Ack => {}
        RelayToPeer::Ping => {}
        RelayToPeer::Turn {
            urls,
            username,
            credential,
        } => {
            *lock(&core.relay_turn) = Some(IceServer {
                urls,
                username,
                credential,
            });
        }
        RelayToPeer::PeerOnline { .. } | RelayToPeer::PeerOffline { .. } => {}
        RelayToPeer::OfferRelay { from, sdp } => {
            rtc::answer(core, &from, &sdp).await;
        }
        RelayToPeer::AnswerRelay { from, sdp } => {
            rtc::complete(core, &from, &sdp).await;
        }
        RelayToPeer::IceRelay { from, candidate } => {
            rtc::add_ice(core, &from, &candidate).await;
        }
        RelayToPeer::Rejected { to, reason } => {
            core.teardown_all(&to);
            fail_dials_to(core, &to, reason);
        }
    }
}
