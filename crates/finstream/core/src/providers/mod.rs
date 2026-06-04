use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{reconnect::ReconnectPolicy, types::{MarketEvent, ProviderKind}};

#[cfg(feature = "alpaca")]
pub mod alpaca;

#[cfg(feature = "finnhub")]
pub mod finnhub;

#[cfg(feature = "massive")]
pub mod massive;

#[cfg(feature = "yahoo")]
pub mod yahoo;

/// A self-contained streaming driver for a single financial data provider instance.
///
/// Implementors are responsible for managing a WebSocket connection, handling
/// provider-specific protocols (auth, subscription), and translating raw messages
/// into normalized [`MarketEvent`]s.
pub trait ProviderDriver: Send + 'static {
    /// Returns the general kind of provider this driver handles (e.g., Alpaca).
    fn kind(&self) -> ProviderKind;

    /// Returns the unique name assigned to this specific driver instance (e.g., "alpaca_paper").
    fn name(&self) -> &str;

    /// Validates the driver's configuration (e.g., checks if required API keys are present).
    ///
    /// # Errors
    ///
    /// Returns `FinStreamError::Config` if the driver is not correctly configured.
    fn validate(&self) -> Result<(), crate::error::FinStreamError> {
        Ok(())
    }

    /// Consumes the driver and spawns a long-running background task.
    ///
    /// The task is responsible for establishing the initial connection and
    /// automatically reconnecting according to the provided `policy` if the
    /// stream is interrupted.
    fn spawn(
        self: Box<Self>,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> JoinHandle<()>;
}
