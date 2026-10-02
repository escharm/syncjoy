use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use syncjoy_protocol::{decode, encode, IceCandidate, PeerId, PeerToRelay, RelayToPeer};
use tungstenite::{client, Message as WsMessage, WebSocket};

use crate::harness::stringify;

/// A scripted signaling client speaking the real protocol against a live relay.
/// Outgoing frames queue through `send`; inbound frames land in `log`.
pub struct WsPeer {
    log: Arc<Mutex<Vec<String>>>,
    out: Option<std::sync::mpsc::Sender<PeerToRelay>>,
    id: String,
    seq: Arc<AtomicU64>,
}

impl WsPeer {
    pub fn spawn(port: u16, id: &str) -> Result<Self, String> {
        let url = format!("ws://127.0.0.1:{port}/syncjoy/v1/peer");
        let stream = TcpStream::connect(("127.0.0.1", port)).map_err(stringify)?;
        let (mut ws, _) = client(url.as_str(), stream).map_err(stringify)?;
        handshake(&mut ws, id)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let (out_tx, out_rx) = std::sync::mpsc::channel::<PeerToRelay>();
        let sink = Arc::clone(&log);
        thread::spawn(move || pump(ws, out_rx, sink));
        Ok(Self {
            log,
            out: Some(out_tx),
            id: id.to_owned(),
            seq: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn send(&self, msg: PeerToRelay) -> Result<(), String> {
        let out = self.out.as_ref().ok_or_else(|| format!("{} is gone", self.id))?;
        out.send(msg).map_err(stringify)
    }

    pub fn wait_log(&self, needle: &str, secs: u64) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if self
                .log
                .lock()
                .unwrap()
                .iter()
                .any(|frame| frame.contains(needle))
            {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(50));
        }
        Err(format!(
            "{} never received {needle:?}; log: {:?}",
            self.id,
            self.log.lock().unwrap()
        ))
    }

    pub fn log_len(&self) -> usize {
        self.log.lock().unwrap().len()
    }

    pub fn next_seq(&self) -> u32 {
        self.seq.fetch_add(1, Ordering::Relaxed) as u32
    }
}

fn handshake(ws: &mut WebSocket<TcpStream>, id: &str) -> Result<(), String> {
    ws.send(WsMessage::Text(
        encode(&PeerToRelay::Hello {
            peer_id: PeerId::new(id),
            token: None,
        })
        .into(),
    ))
    .map_err(stringify)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match ws.read() {
            Ok(WsMessage::Text(text)) => match decode::<RelayToPeer>(text.as_str()) {
                Ok(RelayToPeer::Ack) => return Ok(()),
                Ok(other) => return Err(format!("{id}: expected ack, got {other:?}")),
                Err(error) => return Err(format!("{id}: bad ack: {error}")),
            },
            Ok(_) => {}
            Err(tungstenite::Error::Io(ref error))
                if error.kind() == std::io::ErrorKind::WouldBlock =>
            {
                if Instant::now() > deadline {
                    return Err(format!("{id}: no ack within 5s"));
                }
            }
            Err(error) => return Err(format!("{id}: {error}")),
        }
    }
}

fn pump(
    mut ws: WebSocket<TcpStream>,
    out: std::sync::mpsc::Receiver<PeerToRelay>,
    sink: Arc<Mutex<Vec<String>>>,
) {
    ws.get_ref()
        .set_read_timeout(Some(Duration::from_millis(50)))
        .ok();
    loop {
        match ws.read() {
            Ok(WsMessage::Text(text)) => sink.lock().unwrap().push(text.as_str().to_owned()),
            Ok(_) => {}
            Err(tungstenite::Error::Io(ref error))
                if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => break,
        }
        match out.try_recv() {
            Ok(msg) => {
                if ws.send(WsMessage::Text(encode(&msg).into())).is_err() {
                    break;
                }
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }
    }
}

/// Regression: routing failures notify the sender for answer/ice/decline the
/// same way offer always did. The routing-failure receipt doubles as the
/// sync point for "the target is gone" now that presence broadcasts are gone.
pub fn scenario_routing_silence(port: u16) -> Result<(), String> {
    let carol = WsPeer::spawn(port, "carol")?;
    let bob = WsPeer::spawn(port, "bob")?;

    bob.send(offer("nobody", bob.next_seq()))?;
    bob.wait_log("is not online", 5)?;

    bob.send(offer("carol", bob.next_seq()))?;
    carol.wait_log("offer_relay", 5)?;

    drop(bob);
    await_unroutable(&carol, "bob")?;
    thread::sleep(Duration::from_millis(300));

    let baseline = count_log(&carol, "is not online");
    let quiet_from = carol.log_len();

    carol.send(PeerToRelay::Answer {
        to: PeerId::new("bob"),
        sdp: "v=0.answer".to_owned(),
    })?;
    carol.send(PeerToRelay::Decline {
        to: PeerId::new("bob"),
    })?;
    carol.send(PeerToRelay::Ice {
        to: PeerId::new("bob"),
        candidate: IceCandidate {
            candidate: "candidate:1 1 udp 1 127.0.0.1 1 typ host".to_owned(),
            sdp_mid: None,
            sdp_mline_index: None,
        },
    })?;

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if count_log(&carol, "is not online") >= baseline + 3 {
            if carol.log_len() != quiet_from + 3 {
                return Err(format!(
                    "expected exactly three rejections, log: {:?}",
                    carol.log
                ));
            }
            drop(carol);
            println!("verify: answer/ice/decline routing failures notify the sender (fixed)");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "not all three routing failures notified the sender, log: {:?}",
        carol.log
    ))
}

fn count_log(peer: &WsPeer, needle: &str) -> usize {
    peer.log
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| frame.contains(needle))
        .count()
}

fn await_unroutable(peer: &WsPeer, id: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        peer.send(PeerToRelay::Ice {
            to: PeerId::new(id),
            candidate: IceCandidate {
                candidate: String::new(),
                sdp_mid: None,
                sdp_mline_index: None,
            },
        })?;
        if count_log(peer, "is not online") > 0 {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!("{id} is still routable within 5s"))
}

fn offer(to: &str, seq: u32) -> PeerToRelay {
    PeerToRelay::Offer {
        to: PeerId::new(to),
        sdp: format!("v=0.offer-{seq}"),
    }
}
