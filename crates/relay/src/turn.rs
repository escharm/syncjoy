use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use syncjoy_protocol::RelayToPeer;

use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

#[derive(Debug, Clone)]
pub struct TurnConfig {
    pub urls: Vec<String>,
    pub secret: String,
}

pub fn generate_credentials(config: &TurnConfig, user: &str, ttl_secs: u64) -> RelayToPeer {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after the unix epoch")
        .as_secs()
        + ttl_secs;
    let username = format!("{expiry}:{user}");
    let credential = sign(&config.secret, &username);
    RelayToPeer::Turn {
        urls: config.urls.clone(),
        username,
        credential,
    }
}

fn sign(secret: &str, username: &str) -> String {
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("hmac takes any key length");
    mac.update(username.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
