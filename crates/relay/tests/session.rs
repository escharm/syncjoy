use std::time::Duration;

use syncjoy_protocol::{decode, encode, PeerId, PeerToRelay, RelayToPeer};
use syncjoy_relay::{session, Relay, TurnConfig};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

struct Endpoint {
    to_relay: UnboundedSender<String>,
    from_relay: UnboundedReceiver<String>,
}

fn spawn_session(relay: &Relay) -> Endpoint {
    let (to_relay, inbound_rx) = unbounded_channel::<String>();
    let (outbound_tx, from_relay) = unbounded_channel::<String>();
    let relay = relay.clone();
    tokio::spawn(session::run(relay, inbound_rx, outbound_tx));
    Endpoint {
        to_relay,
        from_relay,
    }
}

fn hello(id: &str) -> String {
    encode(&PeerToRelay::Hello {
        peer_id: PeerId::new(id),
        token: None,
    })
}

async fn next_frame(from_relay: &mut UnboundedReceiver<String>) -> RelayToPeer {
    let text = tokio::time::timeout(Duration::from_secs(5), from_relay.recv())
        .await
        .expect("frame within timeout")
        .expect("session stays alive");
    decode(&text).expect("frame decodes")
}

async fn until(
    from_relay: &mut UnboundedReceiver<String>,
    check: impl Fn(&RelayToPeer) -> bool,
) -> RelayToPeer {
    loop {
        let frame = next_frame(from_relay).await;
        if check(&frame) {
            return frame;
        }
    }
}

#[tokio::test]
async fn handshake_routes_offer_and_answer() {
    let relay = Relay::new();
    let mut alice = spawn_session(&relay);
    let mut bob = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    let frame = until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));

    bob.to_relay.send(hello("bob")).unwrap();
    let frame = until(&mut bob.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));

    alice
        .to_relay
        .send(encode(&PeerToRelay::Offer {
            to: PeerId::new("bob"),
            sdp: "offer-sdp".to_owned(),
        }))
        .unwrap();
    let frame = until(&mut bob.from_relay, |f| {
        matches!(f, RelayToPeer::OfferRelay { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::OfferRelay { ref from, ref sdp }
            if *from == PeerId::new("alice") && sdp == "offer-sdp"
    ));

    bob.to_relay
        .send(encode(&PeerToRelay::Answer {
            to: PeerId::new("alice"),
            sdp: "answer-sdp".to_owned(),
        }))
        .unwrap();
    let frame = until(&mut alice.from_relay, |f| {
        matches!(f, RelayToPeer::AnswerRelay { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::AnswerRelay { ref from, ref sdp }
            if *from == PeerId::new("bob") && sdp == "answer-sdp"
    ));
}

#[tokio::test]
async fn offer_to_offline_peer_is_rejected() {
    let relay = Relay::new();
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;

    alice
        .to_relay
        .send(encode(&PeerToRelay::Offer {
            to: PeerId::new("ghost"),
            sdp: String::new(),
        }))
        .unwrap();
    let frame = until(&mut alice.from_relay, |f| {
        matches!(f, RelayToPeer::Rejected { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::Rejected { ref to, .. } if *to == PeerId::new("ghost")
    ));
}

#[tokio::test]
async fn answer_ice_and_decline_to_offline_peers_are_rejected() {
    let relay = Relay::new();
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;

    for frame in [
        encode(&PeerToRelay::Answer {
            to: PeerId::new("ghost"),
            sdp: String::new(),
        }),
        encode(&PeerToRelay::Ice {
            to: PeerId::new("ghost"),
            candidate: syncjoy_protocol::IceCandidate {
                candidate: String::new(),
                sdp_mid: None,
                sdp_mline_index: None,
            },
        }),
        encode(&PeerToRelay::Decline {
            to: PeerId::new("ghost"),
        }),
    ] {
        alice.to_relay.send(frame).unwrap();
        let rejected = until(&mut alice.from_relay, |f| {
            matches!(f, RelayToPeer::Rejected { .. })
        })
        .await;
        assert!(
            matches!(
                rejected,
                RelayToPeer::Rejected { ref to, ref reason }
                    if *to == PeerId::new("ghost") && reason.contains("not online")
            ),
            "routing failure must notify the sender, got {rejected:?}"
        );
    }
}

#[tokio::test]
async fn decline_routes_rejection_back_to_the_dialer() {
    let relay = Relay::new();
    let mut alice = spawn_session(&relay);
    let mut bob = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    bob.to_relay.send(hello("bob")).unwrap();
    until(&mut bob.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;

    bob.to_relay
        .send(encode(&PeerToRelay::Decline {
            to: PeerId::new("alice"),
        }))
        .unwrap();
    let frame = until(&mut alice.from_relay, |f| {
        matches!(f, RelayToPeer::Rejected { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::Rejected { ref to, ref reason }
            if *to == PeerId::new("bob") && reason.contains("declined")
    ));
}

#[tokio::test]
async fn turn_credentials_follow_the_ack() {
    let relay = Relay::with_turn(TurnConfig {
        urls: vec!["turn:turn.example.com:3478".to_owned()],
        secret: "secret".to_owned(),
    });
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    let frame = until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));
    let frame = until(&mut alice.from_relay, |f| {
        matches!(f, RelayToPeer::Turn { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::Turn { ref urls, .. } if urls == &vec!["turn:turn.example.com:3478".to_owned()]
    ));
}

#[tokio::test]
async fn silent_connection_is_dropped_after_hello_timeout() {
    let relay = Relay::new().with_heartbeat(syncjoy_relay::HeartbeatConfig {
        interval: Duration::from_secs(30),
        timeout: Duration::from_secs(10),
        hello_timeout: Duration::from_millis(100),
    });
    let (to_relay, inbound_rx) = unbounded_channel::<String>();
    let (outbound_tx, mut from_relay) = unbounded_channel::<String>();

    tokio::time::timeout(Duration::from_secs(2), session::run(relay, inbound_rx, outbound_tx))
        .await
        .expect("silent session ends within hello timeout");

    drop(to_relay);
    assert!(from_relay.try_recv().is_err(), "no frame before hello");
}
