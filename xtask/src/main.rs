use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use portable_pty::{native_pty_system, Child, CommandBuilder, PtySize};

/// Workspace orchestration tasks.
#[derive(Parser)]
#[command(name = "xtask")]
enum Cli {
    /// Spawn relay-host plus two peer-demo TUIs in ptys and verify a chat round trip.
    E2e,
}

fn main() {
    match Cli::parse() {
        Cli::E2e => {
            if let Err(error) = e2e() {
                eprintln!("e2e failed: {error}");
                std::process::exit(1);
            }
            println!("e2e passed");
        }
    }
}

fn e2e() -> Result<(), String> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let root = Path::new(manifest)
        .parent()
        .ok_or("no workspace root")?
        .to_path_buf();
    std::env::set_current_dir(&root).map_err(|e| e.to_string())?;

    let build = Command::new("cargo")
        .args(["build", "-p", "relay-host", "-p", "peer-demo"])
        .status()
        .map_err(|e| e.to_string())?;
    if !build.success() {
        return Err("cargo build failed".into());
    }

    let mut relay = spawn_relay()?;
    let relay_port = read_relay_port(&mut relay)?;
    let demo_bin = std::env::current_dir()
        .map_err(|e| e.to_string())?
        .join("target/debug/peer-demo");

    let mut bob = PeerPty::spawn(&demo_bin, &["--id", "bob", "--to", "alice"], relay_port)?;
    let mut alice = PeerPty::spawn(&demo_bin, &["--id", "alice", "--to", "bob"], relay_port)?;

    thread::sleep(Duration::from_millis(1000));
    alice.write(&[0x0e]);
    alice.wait_for("connected to bob", 20)?;
    bob.wait_for("channel open: alice", 20)?;

    alice.write(b"hello bob\r");
    bob.wait_for("[alice] hello bob", 20)?;

    bob.write(b"hi alice\r");
    alice.wait_for("[bob] hi alice", 20)?;

    alice.quit_and_wait()?;
    bob.quit_and_wait()?;
    let _ = relay.kill();
    Ok(())
}

fn spawn_relay() -> Result<std::process::Child, String> {
    Command::new("target/debug/relay-host")
        .env("SYNCJOY_LISTEN", "127.0.0.1:0")
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())
}

fn read_relay_port(relay: &mut std::process::Child) -> Result<u16, String> {
    let stdout = relay.stdout.take().ok_or("relay stdout missing")?;
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .map_err(|e| e.to_string())?;
    line.trim()
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok())
        .ok_or_else(|| format!("cannot parse relay port from {line:?}"))
}

struct PeerPty {
    child: Box<dyn Child + Send>,
    writer: Box<dyn Write + Send>,
    output: Arc<Mutex<Vec<u8>>>,
}

impl PeerPty {
    fn spawn(bin: &Path, args: &[&str], relay_port: u16) -> Result<Self, String> {
        let pty_system = native_pty_system();
        let size = PtySize {
            rows: 24,
            cols: 80,
            ..Default::default()
        };
        let pair = pty_system.openpty(size).map_err(stringify)?;
        let mut command = CommandBuilder::new(bin);
        command.args(args);
        command.env("SYNCJOY_RELAY", format!("ws://127.0.0.1:{relay_port}"));
        command.env("TERM", "xterm-256color");
        let child = pair.slave.spawn_command(command).map_err(stringify)?;
        let mut reader = pair.master.try_clone_reader().map_err(stringify)?;
        let writer = pair.master.take_writer().map_err(stringify)?;
        drop(pair.slave);

        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&output);
        thread::spawn(move || {
            let mut chunk = [0u8; 4096];
            loop {
                match reader.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => sink.lock().unwrap().extend_from_slice(&chunk[..n]),
                }
            }
        });
        Ok(Self {
            child,
            writer,
            output,
        })
    }

    fn write(&mut self, bytes: &[u8]) {
        let _ = self.writer.write_all(bytes);
        let _ = self.writer.flush();
    }

    fn wait_for(&self, needle: &str, secs: u64) -> Result<(), String> {
        let needle = normalize(needle.as_bytes());
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            let haystack = normalize(&self.output.lock().unwrap());
            if haystack.contains(&needle) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        let tail: String = normalize(&self.output.lock().unwrap())
            .chars()
            .rev()
            .take(400)
            .collect();
        let tail: String = tail.chars().rev().collect();
        Err(format!(
            "timed out waiting for {needle:?}; output tail: {tail}"
        ))
    }

    fn quit_and_wait(&mut self) -> Result<(), String> {
        self.write(b"/quit\r");
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            match self.child.try_wait().map_err(stringify)? {
                Some(status) if status.success() => return Ok(()),
                Some(status) => return Err(format!("exit status {status}")),
                None => thread::sleep(Duration::from_millis(100)),
            }
        }
        self.child.kill().map_err(stringify)?;
        Err("did not exit after /quit".into())
    }
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// ratatui redraws incrementally and skips unchanged cells (including spaces)
/// with cursor moves, so matching ignores ANSI escapes and all whitespace.
fn normalize(bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == 0x1b {
            i += 1;
            if i < bytes.len() && bytes[i] == b'[' {
                i += 1;
                while i < bytes.len() && !bytes[i].is_ascii_alphabetic() {
                    i += 1;
                }
            }
        } else if !byte.is_ascii_whitespace() {
            out.push(byte as char);
        }
        i += 1;
    }
    out
}
