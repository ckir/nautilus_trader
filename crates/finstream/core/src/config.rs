use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::collections::HashMap;

use crate::reconnect::ReconnectPolicy;

/// Top-level application configuration — loaded from finstream.toml + env + CLI.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    /// Server-specific configuration (port, log level).
    pub server:    ServerConfig,
    /// Global reconnection policy defaults.
    pub reconnect: ReconnectConfig,
    /// A map of provider instance names to their specific configurations.
    pub providers: HashMap<String, ProviderInstanceConfig>,
    /// Default symbols to subscribe to if none are provided.
    pub symbols:   SymbolsConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        // Build a default config with standard server and reconnect settings
        Self {
            server:    ServerConfig::default(),
            reconnect: ReconnectConfig::default(),
            providers: HashMap::new(),
            symbols:   SymbolsConfig::default(),
        }
    }
}

/// Configuration for the WebSocket gateway server.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// The port the gateway server will listen on.
    pub port:      u16,
    /// The default log level (e.g., "info", "debug").
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        // Default to port 9001 and info logging
        Self { port: 9001, log_level: "info".into() }
    }
}

/// Configuration for the automatic reconnection logic.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReconnectConfig {
    /// Initial delay before the first retry.
    pub initial_delay_secs:      u64,
    /// Maximum delay between retries.
    pub max_delay_secs:          u64,
    /// Whether to apply random jitter to the retry delay.
    pub jitter:                  bool,
    /// Optional limit on the number of retry attempts.
    pub max_retries:             Option<u32>,
    /// Wall-clock retry limit in seconds. `None` or absent = use default (3600s).
    pub max_retry_duration_secs: Option<u64>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        // Standard backoff defaults
        Self {
            initial_delay_secs:      1,
            max_delay_secs:          60,
            jitter:                  true,
            max_retries:             None,
            max_retry_duration_secs: Some(3600), // 1 hour limit by default
        }
    }
}

impl From<ReconnectConfig> for ReconnectPolicy {
    fn from(c: ReconnectConfig) -> Self {
        // Transform the human-friendly config into the internal policy struct
        Self {
            max_retries:   c.max_retries,
            max_duration:  c.max_retry_duration_secs.map(Duration::from_secs),
            initial_delay: Duration::from_secs(c.initial_delay_secs),
            max_delay:     Duration::from_secs(c.max_delay_secs),
            jitter:        c.jitter,
        }
    }
}

/// A polymorphic configuration variant for a specific provider instance.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderInstanceConfig {
    /// Alpaca Markets configuration.
    Alpaca(AlpacaConfig),
    /// Finnhub.io configuration.
    Finnhub(FinnhubConfig),
    /// Massive (Polygon.io) configuration.
    Massive(MassiveConfig),
    /// Yahoo Finance streamer configuration.
    Yahoo(YahooConfig),
}

/// Specific configuration for an Alpaca provider instance.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlpacaConfig {
    /// Whether this instance is active.
    pub enabled:    bool,
    /// The data feed to use ("iex" or "sip").
    pub feed:       String,
    /// The Alpaca API Key ID.
    #[serde(default)]
    pub api_key:    String,
    /// The Alpaca API Secret Key.
    #[serde(default)]
    pub api_secret: String,
}

impl Default for AlpacaConfig {
    fn default() -> Self {
        // Default to disabled and IEX feed
        Self {
            enabled:    false,
            feed:       "iex".into(),
            api_key:    String::new(),
            api_secret: String::new(),
        }
    }
}

/// Specific configuration for a Finnhub provider instance.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FinnhubConfig {
    /// Whether this instance is active.
    pub enabled:   bool,
    /// The Finnhub API token.
    #[serde(default)]
    pub api_token: String,
}

impl Default for FinnhubConfig {
    fn default() -> Self {
        // Default to disabled
        Self { enabled: false, api_token: String::new() }
    }
}

/// Specific configuration for a Massive provider instance.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MassiveConfig {
    /// Whether this instance is active.
    pub enabled: bool,
    /// The Massive (Polygon.io) API key.
    #[serde(default)]
    pub api_key: String,
}

impl Default for MassiveConfig {
    fn default() -> Self {
        // Default to disabled
        Self { enabled: false, api_key: String::new() }
    }
}

/// Specific configuration for a Yahoo Finance provider instance.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct YahooConfig {
    /// Whether this instance is active.
    pub enabled:            bool,
    /// Reconnect if no data is received for this many seconds.
    pub silence_secs:       u32,
    /// How often to send a WebSocket ping to keep the connection alive.
    pub ping_interval_secs: u32,
}

impl Default for YahooConfig {
    fn default() -> Self {
        // Default to disabled with 60s silence timeout
        Self { enabled: false, silence_secs: 60, ping_interval_secs: 30 }
    }
}

/// Default symbol configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SymbolsConfig {
    /// The list of ticker symbols to use by default.
    pub default: Vec<String>,
}

impl Default for SymbolsConfig {
    fn default() -> Self {
        // Default to major tech stocks
        Self { default: vec!["AAPL".into(), "MSFT".into(), "GOOGL".into()] }
    }
}
