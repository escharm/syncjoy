use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use syncjoy_peer::{Bytes, CloseHandler, Event, LinkHandle, MsgHandler, Peer, PeerLink};
use syncjoy_protocol::PeerId;
use syncjoy_relay::Relay;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

async fn start_relay() -> Result<String, Box<dyn std::error::Error>> {
    let relay = Relay::new();
    let app = relay.router();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("ws://{addr}"))
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
        while let Some(Event::Channel(mut peer_link)) = bob_relay_link.next_event().await {
            peer_link.on_msg(EchoLink).await;
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
        while let Some(Event::Channel(mut peer_link)) = relay_link.next_event().await {
            peer_link.on_msg(EchoLink).await;
        }
    });
    handle
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
    let (closed_tx, mut closed_rx) = unbounded_channel();
    peer_link.on_close(CloseProbe { closed: closed_tx });

    peer_link.close();
    let peer = within_timeout(closed_rx.recv())
        .await
        .expect("close is notified");
    assert_eq!(peer, PeerId::new("bob"));
}
