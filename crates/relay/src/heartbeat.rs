use std::sync::Mutex;
use std::time::Duration;

use tokio::time::{sleep_until, Instant};

#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub timeout: Duration,
    pub hello_timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(10),
            hello_timeout: Duration::from_secs(10),
        }
    }
}

pub(crate) enum Probe {
    Send,
    Expired,
}

struct State {
    next_probe: Instant,
    probe_deadline: Option<Instant>,
}

pub(crate) struct Heartbeat {
    config: HeartbeatConfig,
    state: Mutex<State>,
}

impl Heartbeat {
    pub(crate) fn new(config: HeartbeatConfig) -> Self {
        let next_probe = Instant::now() + config.interval;
        Self {
            config,
            state: Mutex::new(State {
                next_probe,
                probe_deadline: None,
            }),
        }
    }

    pub(crate) async fn wait(&self) -> Probe {
        loop {
            let (wake_at, probing) = {
                let state = self.state.lock().unwrap();
                (
                    state.probe_deadline.unwrap_or(state.next_probe),
                    state.probe_deadline.is_some(),
                )
            };
            sleep_until(wake_at).await;
            let probing_now = self.state.lock().unwrap().probe_deadline.is_some();
            if probing != probing_now {
                continue;
            }
            if probing {
                return Probe::Expired;
            }
            return Probe::Send;
        }
    }

    pub(crate) fn probe_sent(&self) {
        let mut state = self.state.lock().unwrap();
        state.probe_deadline = Some(Instant::now() + self.config.timeout);
    }

    pub(crate) fn pong(&self) {
        let mut state = self.state.lock().unwrap();
        state.probe_deadline = None;
        state.next_probe = Instant::now() + self.config.interval;
    }
}
