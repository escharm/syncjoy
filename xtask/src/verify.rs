use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::harness::{occurrences, read_relay_port, spawn_relay, stringify, PeerPty};
use crate::ws_verify;

pub fn verify() -> Result<(), String> {
    let build = Command::new("cargo")
        .args(["build", "-p", "relay-host", "-p", "peer-demo"])
        .status()
        .map_err(stringify)?;
    if !build.success() {
        return Err("cargo build failed".into());
    }
    let demo_bin = std::env::current_dir()
        .map_err(stringify)?
        .join("target/debug/peer-demo");

    let mut relay = spawn_relay()?;
    let port = read_relay_port(&mut relay)?;
    let routed = ws_verify::scenario_routing_silence(port);
    let _ = relay.kill();
    let _ = relay.wait();
    routed?;

    scenario_connect_hang(&demo_bin)?;
    scenario_link_death_silent(&demo_bin)?;
    Ok(())
}

/// Regression: `connect_relay` gives up on a relay that accepts the
/// connection but never answers, instead of hanging forever.
fn scenario_connect_hang(demo_bin: &Path) -> Result<(), String> {
    let tcp_hole = Blackhole::tcp()?;
    let mut pty = PeerPty::spawn(demo_bin, &["--id", "hang", "--to", "bob"], tcp_hole.port)?;
    assert_demo_times_out(&mut pty, 16, "tcp-level black hole")?;
    pty.kill_and_wait()?;
    drop(tcp_hole);

    let ws_hole = Blackhole::ws()?;
    let mut pty = PeerPty::spawn(demo_bin, &["--id", "hang", "--to", "bob"], ws_hole.port)?;
    assert_demo_times_out(&mut pty, 16, "ws-level black hole (no ack)")?;
    pty.kill_and_wait()?;
    drop(ws_hole);
    println!("verify: connect_relay gives up on silent relays with a handshake timeout (fixed)");
    Ok(())
}

fn assert_demo_times_out(pty: &mut PeerPty, secs: u64, label: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if let Some(status) = pty.child.try_wait().map_err(stringify)? {
            if status.success() {
                return Err(format!("{label}: peer-demo exited cleanly, expected the timeout error"));
            }
            let output = pty.output();
            if !output.contains("relayhandshaketimedout") {
                return Err(format!(
                    "{label}: peer-demo exited without the handshake-timeout error"
                ));
            }
            return Ok(());
        }
        thread::sleep(Duration::from_millis(200));
    }
    Err(format!(
        "{label}: peer-demo kept hanging past the connect timeout"
    ))
}

struct Blackhole {
    port: u16,
    _held: Option<Arc<Mutex<Vec<TcpStream>>>>,
}

impl Blackhole {
    /// Accepts TCP connections and never writes a byte back.
    fn tcp() -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(stringify)?;
        let port = listener.local_addr().map_err(stringify)?.port();
        let held = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&held);
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                sink.lock().unwrap().push(stream);
            }
        });
        Ok(Self {
            port,
            _held: Some(held),
        })
    }

    /// Completes the WebSocket handshake and reads frames forever, but never
    /// writes a byte: the peer SDK's hello is swallowed in silence.
    fn ws() -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(stringify)?;
        let port = listener.local_addr().map_err(stringify)?.port();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                thread::spawn(move || swallow(stream));
            }
        });
        Ok(Self {
            port,
            _held: None,
        })
    }
}

fn swallow(stream: TcpStream) {
    let Ok(mut ws) = tungstenite::accept(stream) else {
        return;
    };
    while ws.read().is_ok() {}
}

fn assert_link_death_surfaced(name: &str, pty: &mut PeerPty, before: &str) -> Result<(), String> {
    pty.wait_for("relay link closed", 10)?;
    let after = pty.output();
    for marker in ["channelclosed", "disconnected"] {
        if occurrences(&after, marker) > occurrences(before, marker) {
            return Err(format!("{name}: the direct channel died with the relay via {marker:?}"));
        }
    }
    if !pty.is_alive()? {
        return Err(format!("{name}: peer-demo died with the relay"));
    }
    Ok(())
}

/// Regression: when the signaling link dies, the host learns about it via the
/// relay-closed event while the direct data channel keeps working.
fn scenario_link_death_silent(demo_bin: &PathBuf) -> Result<(), String> {
    let mut relay = spawn_relay()?;
    let port = read_relay_port(&mut relay)?;

    let mut bob = PeerPty::spawn(demo_bin, &["--id", "bob", "--to", "alice"], port)?;
    let mut alice = PeerPty::spawn(demo_bin, &["--id", "alice", "--to", "bob"], port)?;
    thread::sleep(Duration::from_millis(1000));
    alice.write(&[0x0e]);
    alice.wait_for("connected to bob", 20)?;
    bob.wait_for("channel open: alice", 20)?;
    alice.write(b"ping\r");
    bob.wait_for("[alice] ping", 20)?;

    let before_alice = alice.output();
    let before_bob = bob.output();
    relay.kill().map_err(stringify)?;
    let _ = relay.wait();

    assert_link_death_surfaced("alice", &mut alice, &before_alice)?;
    assert_link_death_surfaced("bob", &mut bob, &before_bob)?;

    bob.write(b"post relay\r");
    alice.wait_for("[bob] post relay", 20)?;

    alice.quit_and_wait()?;
    bob.quit_and_wait()?;
    println!("verify: link death is surfaced while the data channel survives (fixed)");
    Ok(())
}
