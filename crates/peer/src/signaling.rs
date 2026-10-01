use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use syncjoy_protocol::{decode, encode, PeerToRelay, RelayToPeer};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use crate::error::PeerError;
use crate::peer::Peer;
use crate::relay_link::{fail_dials_to, Command, Core, Event, RelayLink};
use crate::rtc;
use crate::transport;
use syncjoy_protocol::IceServer;

const RELAY_PEER_PATH: &str = "/syncjoy/v1/peer";
const STUN_URL: &str = "stun:stun.l.google.com:19302";

pub(crate) async fn connect_relay(peer: &Peer, relay_url: &str) -> Result<RelayLink, PeerError> {
    let url = format!("{}{RELAY_PEER_PATH}", relay_url.trim_end_matches('/'));
    let (socket_tx, mut socket_rx) = transport::connect_signaling(url)
        .await
        .map_err(PeerError::Connect)?;

    let hello = encode(&PeerToRelay::Hello {
        peer_id: peer.id.clone(),
        token: peer.relay_token.clone(),
    });
    socket_tx.send(hello).map_err(|_| PeerError::Closed)?;
    expect_ack(&mut socket_rx).await?;

    let (commands, command_rx) = unbounded_channel::<Command>();
    let (events, event_rx) = unbounded_channel::<Event>();
    let core = Arc::new(Core {
        commands,
        incoming: Mutex::new(None),
        pending: Mutex::new(HashMap::new()),
        events,
        dials: Mutex::new(HashMap::new()),
        conns: Mutex::new(HashMap::new()),
        channels: Mutex::new(HashMap::new()),
        ice: Mutex::new(if peer.ice_servers.is_empty() {
            vec![IceServer::stun(STUN_URL)]
        } else {
            peer.ice_servers.clone()
        }),
        next_req: AtomicU64::new(1),
    });
    transport::spawn(pump(socket_tx, socket_rx, core.clone(), command_rx));
    Ok(RelayLink::new(core, event_rx))
}

async fn expect_ack(socket_rx: &mut UnboundedReceiver<String>) -> Result<(), PeerError> {
    while let Some(text) = socket_rx.recv().await {
        return match decode::<RelayToPeer>(&text)? {
            RelayToPeer::Ack => Ok(()),
            RelayToPeer::Rejected { reason, .. } => Err(PeerError::Rejected(reason)),
            other => Err(PeerError::Rejected(format!("{other:?}"))),
        };
    }
    Err(PeerError::Rejected("relay closed before ack".to_owned()))
}

async fn pump(
    socket_tx: transport::SignalingSender,
    mut socket_rx: UnboundedReceiver<String>,
    core: Arc<Core>,
    mut commands: UnboundedReceiver<Command>,
) {
    loop {
        tokio::select! {
            text = socket_rx.recv() => match text {
                Some(text) => match decode::<RelayToPeer>(&text) {
                    Ok(msg) => {
                        if !handle_signal(&core, &socket_tx, msg).await {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "invalid relay frame");
                        break;
                    }
                },
                None => break,
            },
            command = commands.recv() => match command {
                Some(Command::Signal(msg)) => {
                    if socket_tx.send(encode(&msg)).is_err() {
                        break;
                    }
                }
                Some(Command::Dial { req, to, ice }) => {
                    rtc::dial(&core, req, &to, ice).await;
                }
                Some(Command::Close { peer }) => {
                    rtc::teardown(&core, &peer);
                }
                None => break,
            },
        }
    }
}

async fn handle_signal(
    core: &Arc<Core>,
    socket_tx: &transport::SignalingSender,
    msg: RelayToPeer,
) -> bool {
    match msg {
        RelayToPeer::Ack => {}
        RelayToPeer::Turn {
            urls,
            username,
            credential,
        } => {
            core.ice.lock().unwrap().push(IceServer {
                urls,
                username,
                credential,
            });
        }
        RelayToPeer::Ping => {
            return socket_tx.send(encode(&PeerToRelay::Pong)).is_ok();
        }
        RelayToPeer::PeerOnline { peer_id } => {
            let _ = core.events.send(Event::PeerOnline { peer_id });
        }
        RelayToPeer::PeerOffline { peer_id } => {
            rtc::teardown(core, &peer_id);
            let _ = core.events.send(Event::PeerOffline { peer_id });
        }
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
            rtc::teardown(core, &to);
            fail_dials_to(core, &to, reason);
        }
    }
    true
}
