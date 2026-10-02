use std::time::Duration;

use syncjoy_protocol::{decode, encode, PeerId, PeerToRelay, RelayToPeer};
use syncjoy_relay::{session, HeartbeatConfig, Relay};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

struct Endpoint {
    to_relay: UnboundedSender<String>,
    from_relay: UnboundedReceiver<String>,
}

struct Responder {
    to_relay: UnboundedSender<String>,
}

fn short_heartbeat() -> HeartbeatConfig {
    HeartbeatConfig {
        interval: Duration::from_millis(30),
        timeout: Duration::from_millis(60),
        hello_timeout: Duration::from_secs(5),
    }
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

fn spawn_responder(relay: &Relay, id: &str) -> Responder {
    let endpoint = spawn_session(relay);
    endpoint.to_relay.send(hello(id)).unwrap();
    let mut from_relay = endpoint.from_relay;
    let reply = endpoint.to_relay.clone();
    tokio::spawn(async move {
        while let Some(text) = from_relay.recv().await {
            if matches!(decode::<RelayToPeer>(&text), Ok(RelayToPeer::Ping)) {
                let _ = reply.send(encode(&PeerToRelay::Pong));
            }
        }
    });
    Responder {
        to_relay: endpoint.to_relay,
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
async fn half_open_peer_is_evicted_and_its_id_can_reregister() {
    let relay = Relay::new().with_heartbeat(short_heartbeat());
    let mut ghost = spawn_session(&relay);
    ghost.to_relay.send(hello("ghost")).unwrap();
    until(&mut ghost.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    until(&mut ghost.from_relay, |f| matches!(f, RelayToPeer::Ping)).await;

    let watcher = spawn_responder(&relay, "watcher");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !relay
                .registry()
                .route(&PeerId::new("ghost"), RelayToPeer::Ack)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("ghost is evicted within timeout");

    tokio::time::timeout(Duration::from_secs(2), async {
        while ghost.from_relay.recv().await.is_some() {}
    })
    .await
    .expect("relay drops the half-open session");

    let mut revived = spawn_session(&relay);
    revived.to_relay.send(hello("ghost")).unwrap();
    let frame = until(&mut revived.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    assert!(matches!(frame, RelayToPeer::Ack));

    watcher
        .to_relay
        .send(encode(&PeerToRelay::Offer {
            to: PeerId::new("ghost"),
            sdp: "v=0.revived".to_owned(),
        }))
        .unwrap();
    let frame = until(&mut revived.from_relay, |f| {
        matches!(f, RelayToPeer::OfferRelay { .. })
    })
    .await;
    assert!(matches!(
        frame,
        RelayToPeer::OfferRelay { ref from, ref sdp }
            if *from == PeerId::new("watcher") && sdp == "v=0.revived"
    ));
}

#[tokio::test]
async fn ponging_peer_survives_repeated_probes() {
    let relay = Relay::new().with_heartbeat(short_heartbeat());
    let _steady = spawn_responder(&relay, "steady");

    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        relay.registry().route(&PeerId::new("steady"), RelayToPeer::Ack),
        "peer answering pongs stays registered across many probe intervals"
    );
}
