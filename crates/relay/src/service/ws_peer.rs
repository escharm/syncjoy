use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use syncjoy_protocol::{decode, encode, PeerToRelay, RelayToPeer};

use super::Relay;
use crate::registry::RegisteredPeer;
use crate::turn::generate_credentials;

const TURN_TTL_SECS: u64 = 3600;

pub(super) async fn upgrade(ws: WebSocketUpgrade, State(relay): State<Relay>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run(socket, relay))
}

async fn run(socket: WebSocket, relay: Relay) {
    let (mut sink, mut stream) = socket.split();
    let Some(PeerToRelay::Hello { peer_id }) = read_hello(&mut stream).await else {
        return;
    };

    let RegisteredPeer {
        generation,
        mut inbox,
    } = relay.registry.register(&peer_id);
    if sink.send(text(&RelayToPeer::Ack)).await.is_err() {
        relay.registry.unregister(&peer_id, generation);
        return;
    }
    if let Some(turn) = &relay.turn {
        let credentials = generate_credentials(turn, &peer_id.0, TURN_TTL_SECS);
        if sink.send(text(&credentials)).await.is_err() {
            relay.registry.unregister(&peer_id, generation);
            return;
        }
    }
    relay.registry.broadcast(
        RelayToPeer::PeerOnline {
            peer_id: peer_id.clone(),
        },
        &peer_id,
    );
    tracing::info!(%peer_id, "peer online");

    loop {
        tokio::select! {
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(text))) => match decode::<PeerToRelay>(&text) {
                    Ok(msg) => relay.route(&peer_id, msg),
                    Err(error) => {
                        tracing::warn!(%error, "invalid signal frame");
                        break;
                    }
                },
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    tracing::warn!(%error, "peer socket error");
                    break;
                }
                None => break,
            },
            outgoing = inbox.recv() => match outgoing {
                Some(msg) => {
                    if sink.send(text(&msg)).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
        }
    }

    if relay.registry.unregister(&peer_id, generation) {
        relay.registry.broadcast(
            RelayToPeer::PeerOffline {
                peer_id: peer_id.clone(),
            },
            &peer_id,
        );
    }
    tracing::info!(%peer_id, "peer offline");
}

async fn read_hello(stream: &mut SplitStream<WebSocket>) -> Option<PeerToRelay> {
    while let Some(item) = stream.next().await {
        match item {
            Ok(Message::Text(text)) => {
                return decode::<PeerToRelay>(&text)
                    .ok()
                    .filter(|msg| matches!(msg, PeerToRelay::Hello { .. }));
            }
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
    None
}

fn text(msg: &RelayToPeer) -> Message {
    Message::Text(encode(msg).into())
}
