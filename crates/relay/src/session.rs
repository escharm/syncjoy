use syncjoy_protocol::{decode, encode, PeerToRelay, RelayToPeer};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::heartbeat::{Heartbeat, Probe};
use crate::registry::RegisteredPeer;
use crate::relay::Relay;
use crate::turn::generate_credentials;
use std::sync::Arc;

const TURN_TTL_SECS: u64 = 3600;
const REGISTRATION_DENIED: &str = "registration denied";

/// Drive one signaling connection over a frame pipe: inbound yields peer text
/// frames, outbound accepts relay text frames. Framework-free; the axum adapter
/// and any custom-framework host both funnel a WebSocket through this.
pub async fn run(
    relay: Relay,
    mut inbound: UnboundedReceiver<String>,
    outbound: UnboundedSender<String>,
) {
    let registry = Arc::clone(&relay.registry);
    let Some(PeerToRelay::Hello { peer_id, token }) = read_hello(&mut inbound).await else {
        return;
    };

    if let Some(guard) = &relay.guard {
        if !guard(&peer_id, token.as_deref()) {
            tracing::info!(%peer_id, "registration denied");
            let _ = send(&outbound, &RelayToPeer::Rejected {
                to: peer_id.clone(),
                reason: REGISTRATION_DENIED.to_owned(),
            });
            return;
        }
    }

    let RegisteredPeer {
        generation,
        mut inbox,
    } = registry.register(&peer_id);
    if !send(&outbound, &RelayToPeer::Ack) {
        unregister(&relay, &peer_id, generation);
        return;
    }
    if let Some(turn) = &relay.turn {
        let credentials = generate_credentials(turn, &peer_id.0, TURN_TTL_SECS);
        if !send(&outbound, &credentials) {
            unregister(&relay, &peer_id, generation);
            return;
        }
    }
    registry.broadcast(
        RelayToPeer::PeerOnline {
            peer_id: peer_id.clone(),
        },
        &peer_id,
    );
    tracing::info!(%peer_id, "peer online");

    let heartbeat = Heartbeat::new(relay.heartbeat.clone());
    loop {
        tokio::select! {
            text = inbound.recv() => match text {
                Some(text) => match decode::<PeerToRelay>(&text) {
                    Ok(PeerToRelay::Pong) => heartbeat.pong(),
                    Ok(msg) => relay.route(&peer_id, msg),
                    Err(error) => {
                        tracing::warn!(%error, "invalid signal frame");
                        break;
                    }
                },
                None => break,
            },
            outgoing = inbox.recv() => match outgoing {
                Some(msg) => {
                    if !send(&outbound, &msg) {
                        break;
                    }
                }
                None => break,
            },
            probe = heartbeat.wait() => match probe {
                Probe::Send => {
                    if !send(&outbound, &RelayToPeer::Ping) {
                        break;
                    }
                    heartbeat.probe_sent();
                }
                Probe::Expired => {
                    tracing::info!(%peer_id, "heartbeat timeout");
                    break;
                }
            },
        }
    }

    unregister(&relay, &peer_id, generation);
    tracing::info!(%peer_id, "peer offline");
}

fn unregister(relay: &Relay, peer_id: &syncjoy_protocol::PeerId, generation: u64) {
    if relay.registry.unregister(peer_id, generation) {
        relay.registry.broadcast(
            RelayToPeer::PeerOffline {
                peer_id: peer_id.clone(),
            },
            peer_id,
        );
    }
}

async fn read_hello(inbound: &mut UnboundedReceiver<String>) -> Option<PeerToRelay> {
    while let Some(text) = inbound.recv().await {
        return decode::<PeerToRelay>(&text)
            .ok()
            .filter(|msg| matches!(msg, PeerToRelay::Hello { .. }));
    }
    None
}

fn send(outbound: &UnboundedSender<String>, msg: &RelayToPeer) -> bool {
    outbound.send(encode(msg)).is_ok()
}
