use std::process::Command;
use std::thread;
use std::time::Duration;

use crate::harness::{read_relay_port, spawn_relay, stringify, PeerPty};

pub fn e2e() -> Result<(), String> {
    let build = Command::new("cargo")
        .args(["build", "-p", "relay-host", "-p", "peer-demo"])
        .status()
        .map_err(stringify)?;
    if !build.success() {
        return Err("cargo build failed".into());
    }

    let mut relay = spawn_relay()?;
    let relay_port = read_relay_port(&mut relay)?;
    let demo_bin = std::env::current_dir()
        .map_err(stringify)?
        .join("target/debug/peer-demo");

    let mut bob = PeerPty::spawn(&demo_bin, &["--id", "bob", "--to", "alice"], relay_port)?;
    let mut alice = PeerPty::spawn(&demo_bin, &["--id", "alice", "--to", "bob"], relay_port)?;

    thread::sleep(Duration::from_millis(1000));
    alice.write(&[0x0e]);
    alice.wait_for("connecting to bob", 20)?;
    alice.wait_for("connected to bob", 20)?;
    bob.wait_for("channel open: alice", 20)?;

    alice.write(&[0x0e]);
    alice.wait_for("already connected to bob", 20)?;

    alice.write(b"hello bob\r");
    bob.wait_for("[alice] hello bob", 20)?;

    bob.write(b"hi alice\r");
    alice.wait_for("[bob] hi alice", 20)?;

    alice.quit_and_wait()?;
    bob.quit_and_wait()?;
    let _ = relay.kill();
    Ok(())
}
