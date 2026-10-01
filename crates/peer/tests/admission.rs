mod common;

use common::serve;
use syncjoy_peer::{Peer, PeerError};
use syncjoy_relay::Relay;

#[tokio::test]
async fn relay_token_registers_against_guarded_relay() {
    let relay_url = serve(Relay::new().with_guard(|_, token| token == Some("secret")))
        .await
        .expect("relay starts");
    let peer = Peer::builder()
        .id("alice")
        .relay_token("secret")
        .build();

    peer.connect_relay(&relay_url)
        .await
        .expect("valid credential registers");
}

#[tokio::test]
async fn wrong_token_fails_with_the_relay_reason() {
    let relay_url = serve(Relay::new().with_guard(|_, token| token == Some("secret")))
        .await
        .expect("relay starts");
    let peer = Peer::builder()
        .id("alice")
        .relay_token("wrong")
        .build();

    match peer.connect_relay(&relay_url).await {
        Err(PeerError::Rejected(reason)) => {
            assert!(reason.contains("registration denied"), "got {reason}")
        }
        Ok(_) => panic!("expected a registration rejection"),
        Err(other) => panic!("expected a registration rejection, got {other}"),
    }
}

#[tokio::test]
async fn missing_token_fails_against_guarded_relay() {
    let relay_url = serve(Relay::new().with_guard(|_, token| token == Some("secret")))
        .await
        .expect("relay starts");
    let peer = Peer::builder().id("alice").build();

    match peer.connect_relay(&relay_url).await {
        Err(PeerError::Rejected(reason)) => {
            assert!(reason.contains("registration denied"), "got {reason}")
        }
        Ok(_) => panic!("expected a registration rejection"),
        Err(other) => panic!("expected a registration rejection, got {other}"),
    }
}

#[tokio::test]
async fn tokenless_peer_still_registers_against_unguarded_relay() {
    let relay_url = serve(Relay::new()).await.expect("relay starts");
    let peer = Peer::builder().id("alice").build();

    peer.connect_relay(&relay_url)
        .await
        .expect("legacy handshake keeps working");
}
