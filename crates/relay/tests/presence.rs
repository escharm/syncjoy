use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use syncjoy_peer::{Bytes, Event, LinkHandle, MsgHandler, Peer, PeerLink, RelayLink};
use syncjoy_protocol::{decode, encode, PeerId, PeerToRelay, RelayToPeer};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
mod common;

use common::serve;
use syncjoy_relay::Relay;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start_relay() -> Result<String, Box<dyn std::error::Error>> {
    serve(Relay::new()).await
}

async fn within_timeout<T>(future: impl Future<Output = T>) -> T {
    match tokio::time::timeout(Duration::from_secs(30), future).await {
        Ok(value) => value,
        Err(_) => panic!("operation did not finish within timeout"),
    }
}

async fn dial_raw(relay_url: &str, id: &str) -> Ws {
    let url = format!("{relay_url}/syncjoy/v1/peer");
    let (mut ws, _) = connect_async(&url).await.expect("raw client connects");
    ws.send(Message::Text(encode(&PeerToRelay::Hello {
        peer_id: PeerId::new(id),
        token: None,
    })))
    .await
    .expect("hello sent");
    while let Some(item) = ws.next().await {
        if let Message::Text(text) = item.expect("ws stays healthy") {
            if matches!(decode::<RelayToPeer>(&text), Ok(RelayToPeer::Ack)) {
                return ws;
            }
        }
    }
    panic!("relay closed before ack");
}

struct Echo;

#[async_trait]
impl MsgHandler for Echo {
    async fn on_msg(&self, link: LinkHandle, from: PeerId, payload: Bytes) {
        let _ = link.send(from, payload).await;
    }
}

async fn start_echo_peer(relay_url: &str, id: &str) -> LinkHandle {
    let peer = Peer::builder().id(id).build();
    let mut relay_link = peer.connect_relay(relay_url).await.expect("registers");
    let handle = relay_link.handle();
    tokio::spawn(async move {
        while let Some(event) = relay_link.next_event().await {
            if let Event::Channel(mut peer_link) = event {
                peer_link.on_msg(Echo).await;
            }
        }
    });
    handle
}

async fn expect_online(observer: &mut RelayLink, peer: &str) {
    let event = within_timeout(observer.next_event())
        .await
        .expect("event stream stays alive");
    assert!(
        matches!(event, Event::PeerOnline { ref peer_id } if peer_id == &PeerId::new(peer)),
        "expected PeerOnline({peer})"
    );
}

async fn expect_offline(observer: &mut RelayLink, peer: &str) {
    let event = within_timeout(observer.next_event())
        .await
        .expect("event stream stays alive");
    assert!(
        matches!(event, Event::PeerOffline { ref peer_id } if peer_id == &PeerId::new(peer)),
        "expected PeerOffline({peer})"
    );
}

#[tokio::test]
async fn peer_online_and_offline_surface_as_sdk_events() {
    let relay_url = start_relay().await.expect("relay starts");

    let alice = Peer::builder().id("alice").build();
    let mut alice_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let _bob = start_echo_peer(&relay_url, "bob").await;
    expect_online(&mut alice_link, "bob").await;

    let carol = dial_raw(&relay_url, "carol").await;
    expect_online(&mut alice_link, "carol").await;

    drop(carol);
    expect_offline(&mut alice_link, "carol").await;
}

#[tokio::test]
async fn presence_events_flow_alongside_live_channels() {
    let relay_url = start_relay().await.expect("relay starts");

    let alice = Peer::builder().id("alice").build();
    let mut alice_link = alice
        .connect_relay(&relay_url)
        .await
        .expect("alice registers");

    let _bob = start_echo_peer(&relay_url, "bob").await;
    expect_online(&mut alice_link, "bob").await;

    let mut channel: PeerLink = within_timeout(alice_link.connect_peer("bob"))
        .await
        .expect("webrtc channel opens");

    let carol = dial_raw(&relay_url, "carol").await;
    expect_online(&mut alice_link, "carol").await;
    drop(carol);
    expect_offline(&mut alice_link, "carol").await;

    channel.send("still here").await.expect("send delivers");
    let reply = within_timeout(channel.recv()).await;
    assert_eq!(
        String::from_utf8_lossy(&reply.expect("channel survives unrelated presence events")),
        "still here"
    );
}
