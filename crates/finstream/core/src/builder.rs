use tokio::sync::mpsc;

use crate::{
    client::FinStreamClient,
    error::FinStreamError,
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::MarketEvent,
};

/// Default buffer capacity for the internal MPSC channel.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// A fluent builder for configuring and connecting a unified financial data stream.
///
/// The builder allows you to add multiple provider drivers (Alpaca, Yahoo, etc.),
/// set subscription symbols, and customize reconnection policies before
/// initiating the asynchronous connections.
pub struct FinStreamBuilder {
    providers:        Vec<(Box<dyn ProviderDriver>, ReconnectPolicy)>,
    symbols:          Vec<String>,
    channel_capacity: usize,
    default_policy:   ReconnectPolicy,
}

impl Default for FinStreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FinStreamBuilder {
    /// Creates a new, empty builder with default channel capacity and reconnection policy.
    pub fn new() -> Self {
        // Initialize with default values
        Self {
            providers:        Vec::new(),
            symbols:          Vec::new(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            default_policy:   ReconnectPolicy::default(),
        }
    }

    /// Sets a default reconnect policy applied to all subsequent `.provider()` calls.
    pub fn default_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.default_policy = policy;
        self
    }

    /// Adds a provider driver using the current default reconnect policy.
    pub fn provider(mut self, driver: impl ProviderDriver + 'static) -> Self {
        // Use the current default policy for this provider
        let policy = self.default_policy.clone();
        self.providers.push((Box::new(driver), policy));
        self
    }

    /// Adds a provider driver with an explicit reconnect policy.
    pub fn provider_with_policy(
        mut self,
        driver: impl ProviderDriver + 'static,
        policy: ReconnectPolicy,
    ) -> Self {
        // Store the driver and its specific policy
        self.providers.push((Box::new(driver), policy));
        self
    }

    /// Sets the symbols all providers will subscribe to.
    pub fn symbols(mut self, symbols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        // Extend the internal symbol list from the iterator
        self.symbols.extend(symbols.into_iter().map(Into::into));
        self
    }

    /// Customizes the internal MPSC channel buffer size (default: 1024).
    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    /// Spawns background tasks for all configured providers and returns the client and receiver.
    ///
    /// # Errors
    ///
    /// Returns `FinStreamError::Config` if no providers were added or if any driver
    /// validation fails (e.g., missing API keys).
    pub fn connect(
        self,
    ) -> Result<(FinStreamClient, mpsc::Receiver<MarketEvent>), FinStreamError> {
        // Ensure at least one provider is configured
        if self.providers.is_empty() {
            return Err(FinStreamError::Config("no providers added".into()));
        }

        // Create the MPSC channel for unified event streaming
        let (tx, rx) = mpsc::channel(self.channel_capacity);
        let mut handles = Vec::with_capacity(self.providers.len());

        // Validate each driver and spawn its background task
        for (driver, policy) in self.providers {
            // Check for missing credentials or invalid config
            driver.validate()?;
            // Each provider task gets its own clone of the sender and symbols
            let handle = driver.spawn(self.symbols.clone(), tx.clone(), policy);
            handles.push(handle);
        }

        // Return the client (for management) and the receiver (for consumption)
        Ok((FinStreamClient { handles }, rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::yahoo::YahooDriver;
    #[cfg(feature = "alpaca")]
    use crate::providers::alpaca::{AlpacaDriver, AlpacaFeed};

    #[tokio::test]
    async fn test_builder_multiple_providers_allowed() {
        let builder = FinStreamBuilder::new()
            .provider(YahooDriver { 
                name: "y1".into(), 
                silence_secs: 60, 
                ping_interval_secs: 30 
            })
            .provider(YahooDriver { 
                name: "y2".into(), 
                silence_secs: 60, 
                ping_interval_secs: 30 
            });

        let result = builder.connect();
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_no_provider_error() {
        let builder = FinStreamBuilder::new();
        let result = builder.connect();
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "alpaca")]
    fn test_alpaca_validation_missing_key() {
        let builder = FinStreamBuilder::new()
            .provider(AlpacaDriver {
                name: "alpaca".into(),
                api_key: "".into(),
                api_secret: "secret".into(),
                feed: AlpacaFeed::Iex,
            });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Alpaca API key is missing"));
        } else {
            panic!("Expected config error for missing Alpaca key");
        }
    }

    #[test]
    #[cfg(feature = "alpaca")]
    fn test_alpaca_validation_missing_secret() {
        let builder = FinStreamBuilder::new()
            .provider(AlpacaDriver {
                name: "alpaca".into(),
                api_key: "key".into(),
                api_secret: "".into(),
                feed: AlpacaFeed::Iex,
            });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Alpaca API secret is missing"));
        } else {
            panic!("Expected config error for missing Alpaca secret");
        }
    }

    #[test]
    #[cfg(feature = "finnhub")]
    fn test_finnhub_validation_missing_token() {
        use crate::providers::finnhub::FinnhubDriver;
        let builder = FinStreamBuilder::new()
            .provider(FinnhubDriver { name: "finnhub".into(), api_token: "".into() });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Finnhub API token is missing"));
        } else {
            panic!("Expected config error for missing Finnhub token");
        }
    }

    #[test]
    #[cfg(feature = "massive")]
    fn test_massive_validation_missing_key() {
        use crate::providers::massive::MassiveDriver;
        let builder = FinStreamBuilder::new()
            .provider(MassiveDriver { name: "massive".into(), api_key: "".into() });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Massive API key is missing"));
        } else {
            panic!("Expected config error for missing Massive key");
        }
    }
}
