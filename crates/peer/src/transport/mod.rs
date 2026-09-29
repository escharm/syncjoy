#[cfg(feature = "native")]
pub(crate) mod native;

#[cfg(feature = "web")]
pub(crate) mod web;

#[cfg(feature = "native")]
pub(crate) use native::*;

#[cfg(feature = "web")]
pub(crate) use web::*;

pub(crate) use syncjoy_protocol::IceServer;
