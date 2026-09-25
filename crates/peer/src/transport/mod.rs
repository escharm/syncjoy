#[cfg(feature = "native")]
pub(crate) mod native;

#[cfg(feature = "web")]
pub(crate) mod web;

#[cfg(feature = "native")]
pub(crate) use native::*;

#[cfg(feature = "web")]
pub(crate) use web::*;

#[derive(Clone)]
pub(crate) struct IceServer {
    pub(crate) urls: Vec<String>,
    pub(crate) username: String,
    pub(crate) credential: String,
}
