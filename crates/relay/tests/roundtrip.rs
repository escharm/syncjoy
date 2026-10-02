use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use syncjoy_peer::{
    Bytes, CloseHandler, ConnectOptions, Event, IceServer, IncomingHandler, LinkHandle, MsgHandler,
    Peer, PeerError, PeerLink,
};
use syncjoy_protocol::PeerId;
use syncjoy_relay::Relay;
mod common;

use common::serve;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

async fn start_relay() -> Result<String, Box<dyn std::error::Error>> {
    serve(Relay::new()).await
}

async fn within_timeout<T>(future: impl Future<Output = T>) -> T {
    match tokio::time::timeout(Duration::from_secs(30), future).await {
        Ok(value) => value,
        Err(_) => panic!("operation did not finish within timeout"),
    }
}

#[tokio::test]
async fn peer_reaches_peer_over_webrtc() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let mut peer_link: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("webrtc channel opens");

    peer_link.send("ping").await.expect("send succeeds");
    let reply = within_timeout(peer_link.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("channel stays open")),
        "ping"
    );

    peer_link.close();
    let closed = within_timeout(peer_link.recv()).await;
    assert!(closed.is_none(), "channel closes after close()");
}

struct Refuser;

#[async_trait]
impl IncomingHandler for Refuser {
    async fn incoming(&self, _from: &PeerId) -> bool {
        false
    }
}

#[tokio::test]
async fn declined_dial_reports_rejection() {
    let relay_url = start_relay().await.expect("relay starts");

    let bob = Peer::builder().id("bob").build();
    let bob_relay_link = bob.connect_relay(&relay_url).await.expect("bob registers");
    bob_relay_link.on_incoming(Refuser);

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let refused = within_timeout(alice_relay_link.connect_peer("bob")).await;
    match refused {
        Err(PeerError::Rejected(reason)) => assert!(
            reason.contains("declined"),
            "expected a decline reason, got {reason}"
        ),
        Ok(_) => panic!("expected a decline, got a channel"),
        Err(other) => panic!("expected a decline, got {other}"),
    }
}

struct EchoLink;

#[async_trait]
impl MsgHandler for EchoLink {
    async fn on_msg(&self, link: LinkHandle, from: PeerId, payload: Bytes) {
        let _ = link.send(from, payload).await;
    }
}

struct Collect {
    msgs: UnboundedSender<(PeerId, Bytes)>,
}

#[async_trait]
impl MsgHandler for Collect {
    async fn on_msg(&self, _link: LinkHandle, from: PeerId, payload: Bytes) {
        let _ = self.msgs.send((from, payload));
    }
}

#[tokio::test]
async fn on_msg_roundtrip() {
    let relay_url = start_relay().await.expect("relay starts");

    let bob = Peer::builder().id("bob").build();
    let mut bob_relay_link = bob.connect_relay(&relay_url).await.expect("bob registers");
    tokio::spawn(async move {
        while let Some(event) = bob_relay_link.next_event().await {
            if let Event::Channel(mut peer_link) = event {
                peer_link.on_msg(EchoLink).await;
            }
        }
    });

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");
    let mut peer_link: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("webrtc channel opens");
    let (msgs, mut inbox) = unbounded_channel();
    peer_link.on_msg(Collect { msgs }).await;

    peer_link.send("ping").await.expect("send delivers");

    let (from, payload) = within_timeout(inbox.recv()).await.expect("message arrives");
    assert_eq!(from, PeerId::new("bob"));
    assert_eq!(String::from_utf8_lossy(&payload), "ping");
}

async fn start_echo_peer(relay_url: &str, id: &str) -> LinkHandle {
    let peer = Peer::builder().id(id).build();
    let mut relay_link = peer.connect_relay(relay_url).await.expect("registers");
    let handle = relay_link.handle();
    tokio::spawn(async move {
        while let Some(event) = relay_link.next_event().await {
            if let Event::Channel(mut peer_link) = event {
                peer_link.on_msg(EchoLink).await;
            }
        }
    });
    handle
}

#[tokio::test]
async fn custom_ice_still_connects_over_loopback() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder()
        .id("alice")
        .ice_server(IceServer::stun("stun:unreachable.example:3478"))
        .build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let mut peer_link: PeerLink = within_timeout(alice_relay_link.connect_peer_with(
        "bob",
        ConnectOptions {
            ice_servers: vec![IceServer::turn(
                "turn:unreachable.example:3478",
                "user",
                "secret",
            )],
        },
    ))
    .await
    .expect("host candidates connect despite dead stun/turn entries");

    peer_link.send("ping").await.expect("send delivers");
    let reply = within_timeout(peer_link.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("echo returns")),
        "ping"
    );
}

#[tokio::test]
async fn connect_peer_returns_existing_link() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let mut first: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("first dial opens");
    let second: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("second dial returns the existing link");

    second.send("ping").await.expect("send delivers");
    let reply = within_timeout(first.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("same connection echoes back")),
        "ping"
    );
}

#[tokio::test]
async fn dropping_last_link_recycles_the_connection() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let peer_link: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("dial opens");
    drop(peer_link);

    let mut redialed: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("redial after drop opens a fresh link");
    redialed.send("still alive").await.expect("send delivers");
    let reply = within_timeout(redialed.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("redialed connection echoes back")),
        "still alive"
    );
}

struct CloseProbe {
    closed: UnboundedSender<PeerId>,
}

#[async_trait]
impl CloseHandler for CloseProbe {
    async fn on_close(&self, _link: LinkHandle, peer: PeerId) {
        let _ = self.closed.send(peer);
    }
}

#[tokio::test]
async fn on_close_fires_when_the_link_closes() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let peer_link: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("dial opens");
    let (first_tx, mut first_rx) = unbounded_channel();
    peer_link.on_close(CloseProbe { closed: first_tx });
    let (second_tx, mut second_rx) = unbounded_channel();
    peer_link.on_close(CloseProbe { closed: second_tx });

    peer_link.close();
    let peer = within_timeout(second_rx.recv())
        .await
        .expect("the replacement handler is notified");
    assert_eq!(peer, PeerId::new("bob"));
    let stale = tokio::time::timeout(Duration::from_secs(2), first_rx.recv()).await;
    assert!(
        matches!(&stale, Ok(None)) || stale.is_err(),
        "the replaced handler must not fire"
    );
}

#[tokio::test]
async fn on_close_fires_when_the_last_handle_drops() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice.connect_relay(&relay_url).await.expect("alice registers");

    let peer_link: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("dial opens");
    let (closed_tx, mut closed_rx) = unbounded_channel();
    peer_link.on_close(CloseProbe { closed: closed_tx });

    drop(peer_link);

    let peer = within_timeout(closed_rx.recv())
        .await
        .expect("dropping the last handle fires on_close");
    assert_eq!(peer, PeerId::new("bob"));
}

#[tokio::test]
async fn concurrent_dials_coalesce_into_one_channel() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice.connect_relay(&relay_url).await.expect("alice registers");
    let handle = alice_relay_link.handle();

    let (first, second) =
        within_timeout(async { tokio::join!(handle.connect_peer("bob"), handle.connect_peer("bob")) }).await;
    let first: PeerLink = first.expect("first dial opens");
    let mut second: PeerLink = second.expect("coalesced dial shares the same outcome");

    first.send("via first").await.expect("send delivers");
    let reply = within_timeout(second.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("both handles are the same live channel")),
        "via first"
    );
}

#[tokio::test]
async fn decline_fails_every_coalesced_caller() {
    let relay_url = start_relay().await.expect("relay starts");

    let bob = Peer::builder().id("bob").build();
    let bob_relay_link = bob.connect_relay(&relay_url).await.expect("bob registers");
    bob_relay_link.on_incoming(Refuser);

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice.connect_relay(&relay_url).await.expect("alice registers");
    let handle = alice_relay_link.handle();

    let (first, second) =
        within_timeout(async { tokio::join!(handle.connect_peer("bob"), handle.connect_peer("bob")) }).await;
    for (label, outcome) in [("first", first), ("coalesced", second)] {
        match outcome {
            Err(PeerError::Rejected(reason)) => assert!(
                reason.contains("declined"),
                "{label} caller got {reason}"
            ),
            Err(other) => panic!("{label} caller got {other}"),
            Ok(_) => panic!("{label} caller got a channel despite the decline"),
        }
    }
}

#[tokio::test]
async fn stale_handle_drop_keeps_the_rebuilt_connection() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let alice_relay_link = alice.connect_relay(&relay_url).await.expect("alice registers");

    let mut stale: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("first dial opens");
    alice_relay_link.close_peer("bob");
    let drained = within_timeout(stale.recv()).await;
    assert!(drained.is_none(), "close_peer tears the first connection down");

    let mut fresh: PeerLink = within_timeout(alice_relay_link.connect_peer("bob"))
        .await
        .expect("redial opens a fresh connection");
    drop(stale);

    fresh.send("still alive").await.expect("send delivers");
    let reply = within_timeout(fresh.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("the rebuilt connection survives the stale drop")),
        "still alive"
    );
}

#[tokio::test]
async fn closing_the_relay_link_keeps_direct_channels_and_fails_new_dials() {
    let relay_url = start_relay().await.expect("relay starts");
    let _bob = start_echo_peer(&relay_url, "bob").await;

    let alice = Peer::builder().id("alice").build();
    let mut relay_link = alice.connect_relay(&relay_url).await.expect("alice registers");
    let mut peer_link: PeerLink = within_timeout(relay_link.connect_peer("bob"))
        .await
        .expect("dial opens before the close");

    relay_link.close();

    peer_link
        .send("after close")
        .await
        .expect("direct channels survive the signaling close");
    let reply = within_timeout(peer_link.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("the echo still returns")),
        "after close"
    );

    match tokio::time::timeout(Duration::from_secs(3), relay_link.connect_peer("carol")).await {
        Ok(Err(PeerError::Closed)) => {}
        Ok(Err(other)) => panic!("new dials must fail with Closed, got {other}"),
        Ok(Ok(_)) => panic!("new dials must fail once the relay link is closed"),
        Err(_) => panic!("new dials must fail immediately, not wait"),
    }

    let stray_event = tokio::time::timeout(Duration::from_millis(300), relay_link.next_event()).await;
    assert!(
        stray_event.is_err(),
        "a deliberate close must not emit RelayClosed"
    );
}
