mod gateway;
mod stdout;
mod rotating_file;

use std::collections::HashMap;
use std::io::Write;
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use tokio::sync::mpsc;

use nautilus_finstream::{
    config::{AppConfig, ProviderInstanceConfig},
    providers::{
        alpaca::{AlpacaDriver, AlpacaFeed},
        finnhub::FinnhubDriver,
        massive::MassiveDriver,
        yahoo::YahooDriver,
    },
    reconnect::ReconnectPolicy,
    FinStreamBuilder, MarketEvent,
};

use crate::rotating_file::SynchronizedRotator;

#[derive(Parser, Debug)]
#[command(name = "finstream", about = "Unified financial data WebSocket gateway")]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(long, default_value = "finstream.toml")]
    config: String,

    /// Comma-separated list of ticker symbols to subscribe to (overrides config).
    #[arg(long)]
    symbols: Option<String>,

    /// Specific provider instance name to use (overrides config enabled states).
    #[arg(long)]
    provider: Option<String>,

    /// Stream ndjson market data to stdout instead of starting the gateway server.
    /// In this mode, only one provider can be active to ensure clean data flow.
    #[arg(long)]
    stdout: bool,

    /// The port the WebSocket gateway server will listen on (overrides config).
    #[arg(long)]
    port: Option<u16>,

    /// Log level: trace|debug|info|warn|error (overrides config).
    #[arg(long)]
    log_level: Option<String>,

    /// Maximum total retry duration in seconds before stopping the driver (overrides config).
    /// Use 0 for an unlimited retry duration.
    #[arg(long)]
    retry_timeout: Option<u64>,

    /// Enable rotating application logs in the specified directory.
    #[arg(long, default_missing_value = "logs")]
    logs: Option<String>,

    /// Enable rotating market data and status events to files in the logs directory.
    #[arg(long)]
    output: bool,

    /// Maximum size of an individual log file in MB before it is rotated.
    #[arg(long, default_value = "100")]
    max_log_size: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load secrets and settings from .env file into environment variables
    let _ = dotenvy::dotenv();

    // Parse command line arguments
    let cli = Cli::parse();

    // --- Load layered config -----------------------------------------------
    // 1. Base defaults (in code)
    // 2. TOML file (e.g. finstream.toml)
    // 3. Environment variables (FINSTREAM__* prefix)
    let cfg: AppConfig = {
        use config::{Config, Environment, File};
        Config::builder()
            .add_source(File::with_name(&cli.config).required(false))
            .add_source(Environment::with_prefix("FINSTREAM").separator("__"))
            .build()
            .and_then(|c| c.try_deserialize())
            .unwrap_or_default()
    };

    // --- Apply CLI overrides ------------------------------------------------
    // Priority: CLI flag > Config file > Default value
    let log_level = cli
        .log_level
        .as_deref()
        .unwrap_or(&cfg.server.log_level)
        .to_string();

    let port = cli.port.unwrap_or(cfg.server.port);

    // Symbols are normalized to uppercase and trimmed
    let symbols: Vec<String> = cli
        .symbols
        .as_deref()
        .map(|s| s.split(',').map(str::trim).map(str::to_uppercase).collect())
        .unwrap_or_else(|| cfg.symbols.default.clone());

    // --- Logging -----------------------------------------------------------
    // Configure log filter for both the gateway binary and the core library
    let filter = match log_level.as_str() {
        "trace" | "debug" => format!("warn,nautilus_finstream={log_level},finstream={log_level}"),
        other => other.to_string(),
    };

    // Initialize the synchronized file rotator if logging is enabled
    let rotator = if let Some(dir) = &cli.logs {
        Some(SynchronizedRotator::new(dir, cli.max_log_size)?)
    } else {
        None
    };

    // Build the tracing subscriber with multiple layers
    let env_filter = EnvFilter::new(&filter);
    // Layer 1: Stderr for real-time monitoring without polluting stdout
    let stderr_layer = fmt::layer().with_writer(std::io::stderr);
    
    let registry = tracing_subscriber::registry().with(env_filter);

    if let Some(ref rot) = rotator {
        // Layer 2: Rotating file for application logs (info, debug, etc.)
        let app_writer = rot.writer("app").unwrap();
        let file_layer = fmt::layer().with_writer(app_writer).with_ansi(false);
        registry.with(stderr_layer).with(file_layer).init();
    } else {
        registry.with(stderr_layer).init();
    }

    // --- Build the stream --------------------------------------------------
    // Convert configuration into a reconnect policy
    let mut policy: ReconnectPolicy = cfg.reconnect.clone().into();
    // CLI override for max retry duration
    if let Some(secs) = cli.retry_timeout {
        policy.max_duration = if secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(secs))
        };
    }

    // Initialize the builder with shared settings
    let mut builder = FinStreamBuilder::new()
        .default_policy(policy)
        .symbols(symbols);

    // Filter which providers should be instantiated based on config and CLI
    let providers_to_enable: HashMap<String, ProviderInstanceConfig> = if let Some(p_name) = &cli.provider {
        // Explicit provider selected via CLI
        if let Some(p_cfg) = cfg.providers.get(p_name) {
            let mut map = HashMap::new();
            map.insert(p_name.clone(), p_cfg.clone());
            map
        } else {
            return Err(anyhow::anyhow!("Provider instance '{}' not found in config", p_name));
        }
    } else {
        // Use all providers enabled in the configuration file
        cfg.providers.iter()
            .filter(|(_, p)| is_enabled(p))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    // Validation: stdout mode only supports a single provider for consistency
    if cli.stdout && providers_to_enable.len() > 1 {
        return Err(anyhow::anyhow!("Only one provider can be active when using --stdout. Active providers: {:?}", providers_to_enable.keys().collect::<Vec<_>>()));
    }

    // Configure each enabled provider and add it to the builder
    for (name, p_cfg) in providers_to_enable {
        match p_cfg {
            ProviderInstanceConfig::Alpaca(c) => {
                let api_key    = env_or(&c.api_key,    "ALPACA_API_KEY");
                let api_secret = env_or(&c.api_secret, "ALPACA_API_SECRET");
                let feed       = AlpacaFeed::from_str(&c.feed);
                builder = builder.provider(AlpacaDriver { name, api_key, api_secret, feed });
            }
            ProviderInstanceConfig::Finnhub(c) => {
                let api_token = env_or(&c.api_token, "FINNHUB_API_TOKEN");
                builder = builder.provider(FinnhubDriver { name, api_token });
            }
            ProviderInstanceConfig::Massive(c) => {
                let api_key = env_or(&c.api_key, "MASSIVE_API_KEY");
                builder = builder.provider(MassiveDriver { name, api_key });
            }
            ProviderInstanceConfig::Yahoo(c) => {
                builder = builder.provider(YahooDriver {
                    name,
                    silence_secs:       c.silence_secs,
                    ping_interval_secs: c.ping_interval_secs,
                });
            }
        }
    }

    // Establish connections and spawn background driver tasks
    let (_client, mut rx) = builder.connect()?;

    // --- Event Routing -----------------------------------------------------
    // Create an internal bridge to forward events to multiple concurrent consumers
    let (tx_main, rx_main) = mpsc::channel::<MarketEvent>(2048);
    let (tx_gw, rx_gw) = mpsc::channel::<MarketEvent>(2048);
    
    // Prepare writers for data and status log rotation
    let mut data_writer = rotator.as_ref().and_then(|r| r.writer("data"));
    let mut status_writer = rotator.as_ref().and_then(|r| r.writer("status"));
    let enable_output = cli.output;

    // Background task to fan-out events from the core library to binary consumers
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            // 1. Log to files if rotation is enabled
            if enable_output {
                match &event {
                    MarketEvent::Trade { .. } | MarketEvent::Quote { .. } => {
                        if let Some(w) = &mut data_writer {
                            if let Ok(json) = serde_json::to_string(&event) {
                                let _ = writeln!(w, "{}", json);
                            }
                        }
                    }
                    MarketEvent::Status { source, status } => {
                        if let Some(w) = &mut status_writer {
                            if let Ok(status_json) = serde_json::to_string(status) {
                                let _ = writeln!(w, "[{}] {}", source, status_json);
                            }
                        }
                    }
                }
            }

            // 2. Forward to the main execution mode (stdout or gateway)
            let _ = tx_main.send(event.clone()).await;
            let _ = tx_gw.send(event).await;
        }
    });

    // Final dispatch to the selected operation mode
    if cli.stdout {
        stdout::run(rx_main).await;
    } else {
        gateway::run(rx_gw, port).await;
    }

    Ok(())
}

/// Returns true if the given provider configuration is explicitly enabled.
fn is_enabled(p: &ProviderInstanceConfig) -> bool {
    match p {
        ProviderInstanceConfig::Alpaca(c)  => c.enabled,
        ProviderInstanceConfig::Finnhub(c) => c.enabled,
        ProviderInstanceConfig::Massive(c) => c.enabled,
        ProviderInstanceConfig::Yahoo(c)   => c.enabled,
    }
}

/// Returns `override_val` if non-empty, otherwise attempts to read from the environment.
fn env_or(override_val: &str, env_key: &str) -> String {
    if !override_val.is_empty() {
        override_val.to_string()
    } else {
        std::env::var(env_key).unwrap_or_default()
    }
}
