use std::time::Duration;

use tokio::time::Instant;

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

pub(crate) struct Heartbeat {
    config: HeartbeatConfig,
    next_probe: Instant,
    probe_deadline: Option<Instant>,
}

impl Heartbeat {
    pub(crate) fn new(config: HeartbeatConfig) -> Self {
        Self {
            next_probe: Instant::now() + config.interval,
            probe_deadline: None,
            config,
        }
    }

    pub(crate) fn wake_at(&self) -> Instant {
        self.probe_deadline.unwrap_or(self.next_probe)
    }

    pub(crate) fn fired(&self) -> Probe {
        if self.probe_deadline.is_some() {
            Probe::Expired
        } else {
            Probe::Send
        }
    }

    pub(crate) fn probe_sent(&mut self) {
        self.probe_deadline = Some(Instant::now() + self.config.timeout);
    }

    pub(crate) fn pong(&mut self) {
        self.probe_deadline = None;
        self.next_probe = Instant::now() + self.config.interval;
    }
}
