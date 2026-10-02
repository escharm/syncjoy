mod common;

use common::serve;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use syncjoy_peer::{IncomingHandler, Peer};
use syncjoy_protocol::{decode, encode, PeerId, PeerToRelay, RelayToPeer};
use syncjoy_relay::{HeartbeatConfig, Relay};
use tokio_tungstenite::tungstenite::Message;

struct SlowAccept {
    delay: Duration,
    ran: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl IncomingHandler for SlowAccept {
    async fn incoming(&self, _from: &PeerId) -> bool {
        tokio::time::sleep(self.delay).await;
        self.ran.store(true, Ordering::SeqCst);
        true
    }
}

#[tokio::test]
async fn slow_incoming_handler_does_not_stall_heartbeat_pongs() {
    let relay = Relay::new().with_heartbeat(HeartbeatConfig {
        interval: Duration::from_millis(150),
        timeout: Duration::from_millis(400),
        hello_timeout: Duration::from_secs(5),
    });
    let relay_url = serve(relay).await.expect("relay starts");

    let ran = Arc::new(AtomicBool::new(false));
    let bob = Peer::builder().id("bob").build();
    let bob_link = bob.connect_relay(&relay_url).await.expect("bob registers");
    bob_link.on_incoming(SlowAccept {
        delay: Duration::from_millis(800),
        ran: Arc::clone(&ran),
    });

    let (ws, _) = tokio_tungstenite::connect_async(format!("{relay_url}/syncjoy/v1/peer"))
        .await
        .expect("carol connects");
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Text(encode(&PeerToRelay::Hello {
        peer_id: PeerId::new("carol"),
        token: None,
    })))
    .await
    .unwrap();

    let mut acked = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
    while let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now()) {
        let message = match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(message))) => message,
            _ => break,
        };
        let Ok(text) = message.into_text() else {
            break;
        };
        match decode::<RelayToPeer>(text.as_str()) {
            Ok(RelayToPeer::Ack) => {
                acked = true;
                sink.send(Message::Text(encode(&PeerToRelay::Offer {
                    to: PeerId::new("bob"),
                    sdp: "not-an-sdp".to_owned(),
                })))
                .await
                .unwrap();
            }
            Ok(RelayToPeer::Ping) => {
                sink.send(Message::Text(encode(&PeerToRelay::Pong)))
                    .await
                    .unwrap();
            }
            Ok(RelayToPeer::PeerOffline { peer_id }) => {
                panic!("peer {peer_id} was evicted while bob's handler was running");
            }
            Ok(_) => {}
            Err(_) => {}
        }
    }

    assert!(acked, "carol completed the hello handshake");
    assert!(
        ran.load(Ordering::SeqCst),
        "the offer reached bob and its handler completed"
    );
}
