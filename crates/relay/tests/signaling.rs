use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use syncjoy_protocol::{decode, encode, PeerId, PeerToRelay, RelayToPeer};
mod common;

use common::serve;
use syncjoy_relay::Relay;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start_relay() -> Result<String, Box<dyn std::error::Error>> {
    serve(Relay::new()).await
}

async fn dial(relay_url: &str, id: &str) -> Result<Ws, Box<dyn std::error::Error>> {
    let url = format!("{relay_url}/syncjoy/v1/peer");
    let (mut ws, _) = connect_async(&url).await?;
    ws.send(Message::Text(encode(&PeerToRelay::Hello {
        peer_id: PeerId::new(id),
    })))
    .await?;
    while let Some(item) = ws.next().await {
        if let Message::Text(text) = item? {
            if matches!(decode::<RelayToPeer>(&text)?, RelayToPeer::Ack) {
                break;
            }
        }
    }
    Ok(ws)
}

async fn next_matching(
    ws: &mut Ws,
    check: impl Fn(&RelayToPeer) -> bool,
) -> Result<RelayToPeer, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let item = tokio::time::timeout_at(deadline, ws.next())
            .await?
            .ok_or("ws closed")??;
        if let Message::Text(text) = item {
            let msg = decode::<RelayToPeer>(&text)?;
            if check(&msg) {
                return Ok(msg);
            }
        }
    }
}

#[tokio::test]
async fn offer_and_answer_route_between_peers() {
    let relay_url = start_relay().await.expect("relay starts");
    let mut alice = dial(&relay_url, "alice").await.expect("alice registers");
    let mut bob = dial(&relay_url, "bob").await.expect("bob registers");

    alice
        .send(Message::Text(encode(&PeerToRelay::Offer {
            to: PeerId::new("bob"),
            sdp: "v=0.offer".to_owned(),
        })))
        .await
        .expect("offer sent");

    let arrived = next_matching(&mut bob, |msg| {
        matches!(msg, RelayToPeer::OfferRelay { .. })
    })
    .await
    .expect("bob gets the relayed offer");
    assert!(matches!(
        arrived,
        RelayToPeer::OfferRelay { ref from, ref sdp } if from == &PeerId::new("alice") && sdp == "v=0.offer"
    ));

    bob.send(Message::Text(encode(&PeerToRelay::Answer {
        to: PeerId::new("alice"),
        sdp: "v=0.answer".to_owned(),
    })))
    .await
    .expect("answer sent");

    let arrived = next_matching(&mut alice, |msg| {
        matches!(msg, RelayToPeer::AnswerRelay { .. })
    })
    .await
    .expect("alice gets the relayed answer");
    assert!(
        matches!(arrived, RelayToPeer::AnswerRelay { ref from, ref sdp } if from == &PeerId::new("bob"))
    );
}

#[tokio::test]
async fn offer_to_offline_peer_is_rejected() {
    let relay_url = start_relay().await.expect("relay starts");
    let mut alice = dial(&relay_url, "alice").await.expect("alice registers");

    alice
        .send(Message::Text(encode(&PeerToRelay::Offer {
            to: PeerId::new("ghost"),
            sdp: String::new(),
        })))
        .await
        .expect("offer sent");

    let arrived = next_matching(&mut alice, |msg| {
        matches!(msg, RelayToPeer::Rejected { .. })
    })
    .await
    .expect("alice is told the target is offline");
    assert!(matches!(
        arrived,
        RelayToPeer::Rejected { ref to, .. } if to == &PeerId::new("ghost")
    ));
}

#[tokio::test]
async fn peer_online_offline_broadcasts_to_others() {
    let relay_url = start_relay().await.expect("relay starts");
    let mut alice = dial(&relay_url, "alice").await.expect("alice registers");

    let bob = dial(&relay_url, "bob").await.expect("bob registers");
    let online = next_matching(&mut alice, |msg| {
        matches!(msg, RelayToPeer::PeerOnline { .. })
    })
    .await
    .expect("alice sees bob coming online");
    assert!(matches!(
        online,
        RelayToPeer::PeerOnline { ref peer_id } if peer_id == &PeerId::new("bob")
    ));

    drop(bob);
    let offline = next_matching(&mut alice, |msg| {
        matches!(msg, RelayToPeer::PeerOffline { .. })
    })
    .await
    .expect("alice sees bob going offline");
    assert!(matches!(
        offline,
        RelayToPeer::PeerOffline { ref peer_id } if peer_id == &PeerId::new("bob")
    ));
}
