use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use syncjoy_peer::{Event, Peer, PeerError};
use syncjoy_protocol::{encode, RelayToPeer};
use tokio::net::TcpListener;

/// A signaling server that registers the peer and then, after receiving one
/// more frame (the dial's offer), closes the connection.
async fn fake_relay_that_dies_after_the_offer() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accepts");
        let mut ws = tokio_tungstenite::accept_async(stream)
            .await
            .expect("handshake");
        let _hello = ws.next().await.expect("hello frame");
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            encode(&RelayToPeer::Ack).into(),
        ))
        .await
        .expect("ack sent");
        let _offer = ws.next().await.expect("offer frame");
        let _ = ws.close(None).await;
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn link_death_fails_pending_dials_and_emits_relay_closed() {
    let relay_url = fake_relay_that_dies_after_the_offer().await;
    let bob = Peer::builder().id("bob").build();
    let mut link = bob
        .connect_relay(&relay_url)
        .await
        .expect("registers before the link dies");

    let dial_handle = link.handle();
    let dial = tokio::time::timeout(Duration::from_secs(8), dial_handle.connect_peer("carol")).await;

    let outcome = dial.expect("the pending dial settles instead of waiting the full 15s");
    match outcome {
        Err(PeerError::Rejected(reason)) => {
            assert!(
                reason.contains("relay link closed") && reason.contains("socket closed"),
                "got {reason}"
            );
        }
        Err(other) => panic!("expected the link-closed rejection, got {other}"),
        Ok(_) => panic!("the dial must fail when the signaling link dies"),
    }

    match tokio::time::timeout(Duration::from_secs(5), link.next_event()).await {
        Ok(Some(Event::RelayClosed { reason })) => {
            assert!(reason.contains("socket closed"), "got {reason}");
        }
        Ok(Some(_)) => panic!("expected Event::RelayClosed, got another event"),
        Ok(None) => panic!("event stream ended without Event::RelayClosed"),
        Err(_) => panic!("Event::RelayClosed did not arrive within 5s"),
    }
}

#[tokio::test]
async fn ack_blackhole_times_out_instead_of_hanging() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accepts");
        let mut ws = tokio_tungstenite::accept_async(stream)
            .await
            .expect("handshake");
        let _hello = ws.next().await.expect("hello frame");
        tokio::time::sleep(Duration::from_secs(30)).await;
        let _ = ws;
    });

    let bob = Peer::builder().id("bob").build();
    let started = std::time::Instant::now();
    match bob.connect_relay(&format!("ws://{addr}")).await {
        Err(PeerError::Connect(reason)) => {
            assert!(reason.contains("timed out"), "got {reason}");
        }
        Err(other) => panic!("expected the handshake timeout, got {other}"),
        Ok(_) => panic!("the silent relay must not register the peer"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "the ack wait must give up long before the dial timeout"
    );
}
