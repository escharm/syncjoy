#[cfg(all(feature = "native", feature = "web"))]
compile_error!("features \"native\" and \"web\" are mutually exclusive transports");

mod error;
mod peer;
mod peer_link;
mod relay_link;
mod rtc;
mod signaling;
mod transport;

pub use bytes::Bytes;
pub use error::PeerError;
pub use peer::{Peer, PeerBuilder};
pub use peer_link::PeerLink;
pub use relay_link::{
    CloseHandler, ConnectOptions, Event, IncomingHandler, LinkHandle, MsgHandler, RelayLink,
};
pub use syncjoy_protocol::{IceServer, PeerId};
