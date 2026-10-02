use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use portable_pty::{native_pty_system, Child, CommandBuilder, PtySize};

pub fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

pub fn spawn_relay() -> Result<std::process::Child, String> {
    Command::new("target/debug/relay-host")
        .env("SYNCJOY_LISTEN", "127.0.0.1:0")
        .stdout(Stdio::piped())
        .spawn()
        .map_err(stringify)
}

pub fn read_relay_port(relay: &mut std::process::Child) -> Result<u16, String> {
    let stdout = relay.stdout.take().ok_or("relay stdout missing")?;
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .map_err(stringify)?;
    line.trim()
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok())
        .ok_or_else(|| format!("cannot parse relay port from {line:?}"))
}

pub struct PeerPty {
    pub child: Box<dyn Child + Send>,
    writer: Box<dyn Write + Send>,
    output: Arc<Mutex<Vec<u8>>>,
}

impl PeerPty {
    pub fn spawn(bin: &Path, args: &[&str], relay_port: u16) -> Result<Self, String> {
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

    pub fn write(&mut self, bytes: &[u8]) {
        let _ = self.writer.write_all(bytes);
        let _ = self.writer.flush();
    }

    pub fn output(&self) -> String {
        normalize(&self.output.lock().unwrap())
    }

    pub fn wait_for(&self, needle: &str, secs: u64) -> Result<(), String> {
        let needle = normalize(needle.as_bytes());
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if self.output().contains(&needle) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(format!(
            "timed out waiting for {needle:?}; output tail: {}",
            tail(&self.output())
        ))
    }

    pub fn is_alive(&mut self) -> Result<bool, String> {
        Ok(self.child.try_wait().map_err(stringify)?.is_none())
    }

    pub fn quit_and_wait(&mut self) -> Result<(), String> {
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

    pub fn kill_and_wait(&mut self) -> Result<(), String> {
        if self.child.try_wait().map_err(stringify)?.is_some() {
            return Ok(());
        }
        let _ = self.child.kill();
        let _ = self.child.try_wait();
        Ok(())
    }
}

fn tail(output: &str) -> String {
    let reversed: String = output.chars().rev().take(400).collect();
    reversed.chars().rev().collect()
}

/// ratatui redraws incrementally and skips unchanged cells (including spaces)
/// with cursor moves, so matching ignores ANSI escapes and all whitespace.
pub fn normalize(bytes: &[u8]) -> String {
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

pub fn occurrences(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}
