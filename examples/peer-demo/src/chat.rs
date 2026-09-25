use std::sync::Arc;

use async_trait::async_trait;
use syncjoy_peer::{Bytes, CloseHandler, LinkHandle, MsgHandler};
use syncjoy_protocol::PeerId;

use crate::screen::Screen;

/// Logs every incoming message onto the screen.
pub(crate) struct Receiver {
    pub(crate) screen: Arc<Screen>,
}

#[async_trait]
impl MsgHandler for Receiver {
    async fn on_msg(&self, _link: LinkHandle, from: PeerId, payload: Bytes) {
        self.screen
            .log(format!("[{from}] {}", String::from_utf8_lossy(&payload)));
        self.screen.set_connected(true);
    }
}

/// Logs the connection loss and marks the screen offline.
pub(crate) struct CloseNotice {
    pub(crate) screen: Arc<Screen>,
}

#[async_trait]
impl CloseHandler for CloseNotice {
    async fn on_close(&self, _link: LinkHandle, peer: PeerId) {
        self.screen.set_connected(false);
        self.screen.log(format!("channel closed: {peer}"));
    }
}

/// What a submitted input line asks the session to do.
pub(crate) enum Submission {
    Quit,
    Skip,
    Unknown(String),
    Send(String),
}

impl Submission {
    pub(crate) fn parse(line: &str) -> Self {
        let line = line.trim();
        if line.is_empty() {
            Self::Skip
        } else if line == "/quit" {
            Self::Quit
        } else if let Some(command) = line.strip_prefix('/') {
            Self::Unknown(command.to_owned())
        } else {
            Self::Send(line.to_owned())
        }
    }
}
