pub mod heartbeat;
pub mod registry;
pub mod session;
pub mod turn;

mod relay;

pub use heartbeat::HeartbeatConfig;
pub use registry::PeerRegistry;
pub use relay::Relay;
pub use turn::{generate_credentials, GrantedTurn, TurnConfig, TurnProvisioner};

pub(crate) fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
