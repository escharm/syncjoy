use std::sync::{Arc, Mutex};
use std::time::Duration;

use syncjoy_protocol::{decode, encode, PeerId, PeerToRelay, RelayToPeer};
use syncjoy_relay::{session, Relay};
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

fn hello(id: &str, token: Option<&str>) -> String {
    encode(&PeerToRelay::Hello {
        peer_id: PeerId::new(id),
        token: token.map(str::to_owned),
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

async fn expect_closed(from_relay: &mut UnboundedReceiver<String>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while from_relay.recv().await.is_some() {}
    })
    .await
    .expect("relay closes the connection after denial");
}

#[tokio::test]
async fn guard_sees_peer_id_and_valid_token_registers() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let probe = Arc::clone(&seen);
    let relay = Relay::new().with_guard(move |peer_id, token| {
        probe
            .lock()
            .unwrap()
            .push((peer_id.to_string(), token.map(str::to_owned)));
        token == Some("secret")
    });
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice", Some("secret"))).unwrap();
    let frame = until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![("alice".to_owned(), Some("secret".to_owned()))]
    );
    assert!(
        relay.registry().route(&PeerId::new("alice"), RelayToPeer::Ack),
        "admitted peer is registered"
    );
}

#[tokio::test]
async fn invalid_token_is_rejected_and_disconnected() {
    let relay = Relay::new().with_guard(|_, token| token == Some("secret"));
    let mut intruder = spawn_session(&relay);

    intruder
        .to_relay
        .send(hello("intruder", Some("wrong")))
        .unwrap();
    let frame = until(&mut intruder.from_relay, |f| {
        matches!(f, RelayToPeer::Rejected { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::Rejected {
            ref to,
            ref reason
        } if *to == PeerId::new("intruder") && reason == "registration denied"
    ));
    expect_closed(&mut intruder.from_relay).await;
    assert!(
        !relay.registry().route(&PeerId::new("intruder"), RelayToPeer::Ack),
        "denied peer never enters the registry"
    );
}

#[tokio::test]
async fn missing_token_is_rejected_when_guarded() {
    let relay = Relay::new().with_guard(|_, token| token == Some("secret"));
    let mut stranger = spawn_session(&relay);

    stranger.to_relay.send(hello("stranger", None)).unwrap();
    until(&mut stranger.from_relay, |f| {
        matches!(f, RelayToPeer::Rejected { .. })
    })
    .await;
    expect_closed(&mut stranger.from_relay).await;
    assert!(!relay.registry().route(&PeerId::new("stranger"), RelayToPeer::Ack));
}

#[tokio::test]
async fn unguarded_relay_admits_tokenless_hello() {
    let relay = Relay::new();
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice", None)).unwrap();
    let frame = until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));
    assert!(relay.registry().route(&PeerId::new("alice"), RelayToPeer::Ack));
}

#[tokio::test]
async fn unguarded_relay_admits_hello_with_token() {
    let relay = Relay::new();
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice", Some("anything"))).unwrap();
    let frame = until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));
}
