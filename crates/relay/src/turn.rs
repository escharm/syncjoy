use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use syncjoy_protocol::{IceServer, PeerId, RelayToPeer};

use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

const REST_TTL_SECS: u64 = 3600;

#[derive(Debug, Clone)]
pub struct TurnConfig {
    pub urls: Vec<String>,
    pub secret: String,
}

/// One grant of TURN credentials. `refresh_after` tells the relay when to
/// ask the provisioner again; `None` keeps this grant for the whole session.
#[derive(Debug, Clone)]
pub struct GrantedTurn {
    pub server: IceServer,
    pub refresh_after: Option<Duration>,
}

/// Host-supplied TURN credential lifecycle. `issue` is awaited after
/// admission and at each refresh point; an `Err` degrades that session to
/// signaling-only. `revoke` is called once for the latest successful grant
/// when its session ends, dispatched without blocking session teardown.
#[async_trait]
pub trait TurnProvisioner: Send + Sync + 'static {
    async fn issue(&self, peer: &PeerId) -> Result<GrantedTurn, String>;
    async fn revoke(&self, granted: &GrantedTurn);
}

pub(crate) struct RestSecret {
    config: TurnConfig,
    ttl: Duration,
}

impl RestSecret {
    pub(crate) fn new(config: TurnConfig) -> Self {
        Self {
            config,
            ttl: Duration::from_secs(REST_TTL_SECS),
        }
    }
}

#[async_trait]
impl TurnProvisioner for RestSecret {
    async fn issue(&self, peer: &PeerId) -> Result<GrantedTurn, String> {
        Ok(GrantedTurn {
            server: rest_server(&self.config, &peer.0, self.ttl.as_secs()),
            refresh_after: Some(self.ttl / 2),
        })
    }

    async fn revoke(&self, _granted: &GrantedTurn) {}
}

pub fn generate_credentials(config: &TurnConfig, user: &str, ttl_secs: u64) -> RelayToPeer {
    let server = rest_server(config, user, ttl_secs);
    RelayToPeer::Turn {
        urls: server.urls,
        username: server.username,
        credential: server.credential,
    }
}

fn rest_server(config: &TurnConfig, user: &str, ttl_secs: u64) -> IceServer {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after the unix epoch")
        .as_secs()
        + ttl_secs;
    let username = format!("{expiry}:{user}");
    IceServer {
        urls: config.urls.clone(),
        credential: sign(&config.secret, &username),
        username,
    }
}

fn sign(secret: &str, username: &str) -> String {
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("hmac takes any key length");
    mac.update(username.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
