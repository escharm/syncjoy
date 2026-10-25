use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use syncjoy_protocol::{decode, encode, IceServer, PeerId, PeerToRelay, RelayToPeer};
use syncjoy_relay::{session, GrantedTurn, Relay, TurnProvisioner};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

struct Endpoint {
    to_relay: UnboundedSender<String>,
    from_relay: UnboundedReceiver<String>,
}

struct Recorder {
    refresh_after: Option<Duration>,
    fail: bool,
    issued: Mutex<Vec<String>>,
    revoked: Mutex<Vec<String>>,
}

impl Recorder {
    fn fresh() -> Arc<Self> {
        Arc::new(Self {
            refresh_after: None,
            fail: false,
            issued: Mutex::new(Vec::new()),
            revoked: Mutex::new(Vec::new()),
        })
    }

    fn failing() -> Arc<Self> {
        Arc::new(Self {
            refresh_after: None,
            fail: true,
            issued: Mutex::new(Vec::new()),
            revoked: Mutex::new(Vec::new()),
        })
    }

    fn refreshing(every: Duration) -> Arc<Self> {
        Arc::new(Self {
            refresh_after: Some(every),
            fail: false,
            issued: Mutex::new(Vec::new()),
            revoked: Mutex::new(Vec::new()),
        })
    }

    fn revoked(&self) -> Vec<String> {
        self.revoked.lock().unwrap().clone()
    }
}

#[async_trait]
impl TurnProvisioner for Recorder {
    async fn issue(&self, peer: &PeerId) -> Result<GrantedTurn, String> {
        if self.fail {
            return Err("out of accounts".to_owned());
        }
        let username = format!("{peer}-{}", self.issued.lock().unwrap().len());
        self.issued.lock().unwrap().push(username.clone());
        Ok(GrantedTurn {
            server: IceServer::turn("turn:turn.example.com:3478", username, "pw"),
            refresh_after: self.refresh_after,
        })
    }

    async fn revoke(&self, granted: &GrantedTurn) {
        self.revoked.lock().unwrap().push(granted.server.username.clone());
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

async fn eventually<F>(mut probe: F)
where
    F: FnMut() -> bool,
{
    tokio::time::timeout(Duration::from_secs(2), async {
        while !probe() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition met within timeout");
}

fn turn_username(frame: &RelayToPeer) -> &str {
    match frame {
        RelayToPeer::Turn { username, .. } => username,
        other => panic!("expected turn frame, got {other:?}"),
    }
}

#[tokio::test]
async fn grant_reaches_peer_and_revocation_follows_disconnect() {
    let recorder = Recorder::fresh();
    let relay = Relay::new()
        .with_guard(|_, _| true)
        .with_turn_provisioner(recorder.clone());
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    let frame = until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Turn { .. })).await;
    assert_eq!(turn_username(&frame), "alice-0");

    drop(alice.to_relay);
    eventually(|| recorder.revoked() == vec!["alice-0".to_owned()]).await;
}

#[tokio::test]
async fn provisioner_error_degrades_to_signaling_only() {
    let recorder = Recorder::failing();
    let relay = Relay::new()
        .with_guard(|_, _| true)
        .with_turn_provisioner(recorder.clone())
        .with_heartbeat(syncjoy_relay::HeartbeatConfig {
            interval: Duration::from_millis(30),
            timeout: Duration::from_millis(60),
            hello_timeout: Duration::from_secs(5),
        });
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    loop {
        let frame = next_frame(&mut alice.from_relay).await;
        assert!(
            !matches!(frame, RelayToPeer::Turn { .. }),
            "failed issuance must not reach the peer"
        );
        if matches!(frame, RelayToPeer::Ping) {
            break;
        }
    }
    assert!(
        relay.registry().route(&PeerId::new("alice"), RelayToPeer::Ack),
        "session survives a failed issuance"
    );
}

#[tokio::test]
async fn takeover_revokes_only_the_superseded_grant() {
    let recorder = Recorder::fresh();
    let relay = Relay::new()
        .with_guard(|_, _| true)
        .with_turn_provisioner(recorder.clone());
    let mut original = spawn_session(&relay);
    let successor = spawn_session(&relay);

    original.to_relay.send(hello("alice")).unwrap();
    until(&mut original.from_relay, |f| matches!(f, RelayToPeer::Turn { .. })).await;

    successor.to_relay.send(hello("alice")).unwrap();
    let mut successor_rx = successor.from_relay;
    let frame = until(&mut successor_rx, |f| matches!(f, RelayToPeer::Turn { .. })).await;
    assert_eq!(turn_username(&frame), "alice-1");

    eventually(|| recorder.revoked() == vec!["alice-0".to_owned()]).await;
    assert!(
        relay.registry().route(&PeerId::new("alice"), RelayToPeer::Ack),
        "successor stays registered"
    );
}

#[tokio::test]
async fn refresh_reissues_and_session_end_revokes_the_latest_grant() {
    let recorder = Recorder::refreshing(Duration::from_millis(30));
    let relay = Relay::new()
        .with_guard(|_, _| true)
        .with_turn_provisioner(recorder.clone());
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Turn { .. })).await;
    let frame = until(&mut alice.from_relay, |f| {
        matches!(f, RelayToPeer::Turn { username, .. } if username.ends_with("-1"))
    })
    .await;
    assert_eq!(turn_username(&frame), "alice-1");

    drop(alice.to_relay);
    eventually(|| recorder.revoked() == vec!["alice-1".to_owned()]).await;
}

#[tokio::test]
async fn provisioner_is_withheld_without_a_guard() {
    let recorder = Recorder::fresh();
    let relay = Relay::new()
        .with_turn_provisioner(recorder.clone())
        .with_heartbeat(syncjoy_relay::HeartbeatConfig {
            interval: Duration::from_millis(30),
            timeout: Duration::from_millis(60),
            hello_timeout: Duration::from_secs(5),
        });
    let mut alice = spawn_session(&relay);

    alice.to_relay.send(hello("alice")).unwrap();
    until(&mut alice.from_relay, |f| matches!(f, RelayToPeer::Ack)).await;
    loop {
        let frame = next_frame(&mut alice.from_relay).await;
        assert!(
            !matches!(frame, RelayToPeer::Turn { .. }),
            "unguarded relay must not consult any turn source"
        );
        if matches!(frame, RelayToPeer::Ping) {
            break;
        }
    }
    assert!(
        recorder.issued.lock().unwrap().is_empty(),
        "issue is never called without a guard"
    );
}
