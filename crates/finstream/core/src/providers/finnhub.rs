use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{FinnhubTradeExtras, MarketEvent, ProviderKind, ProviderStatus, Trade, TradeExtras},
};

/// Driver for the Finnhub.io real-time trade WebSocket.
pub struct FinnhubDriver {
    /// Unique name for this driver instance.
    pub name:      String,
    /// Finnhub API token.
    pub api_token: String,
}

impl ProviderDriver for FinnhubDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Finnhub
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> Result<(), crate::error::FinStreamError> {
        if self.api_token.is_empty() {
            return Err(crate::error::FinStreamError::Config("Finnhub API token is missing".into()));
        }
        Ok(())
    }

    fn spawn(
        self: Box<Self>,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            run_loop(*self, symbols, tx, policy).await;
        })
    }
}

// Internal run loop that handles automatic reconnection for Finnhub.
async fn run_loop(
    driver: FinnhubDriver,
    symbols: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    policy: ReconnectPolicy,
) {
    let mut attempt = 0u32;
    let mut first_failure: Option<std::time::Instant> = None;

    loop {
        // Start a new WebSocket session
        match ws_session(&driver, &symbols, &tx).await {
            SessionResult::Stopped => {
                // Clean shutdown requested
                break;
            }
            SessionResult::Failed(reason) => {
                // Retryable error (e.g. network timeout)
                let elapsed = first_failure.get_or_insert_with(std::time::Instant::now).elapsed();
                
                // Check if we should stop retrying based on policy
                if policy.is_exceeded(attempt, elapsed) {
                    let _ = tx
                        .send(MarketEvent::Status {
                            source: driver.name.clone(),
                            status: ProviderStatus::Error {
                                provider: ProviderKind::Finnhub,
                                message:  format!("retry limit reached: {reason}"),
                            }
                        })
                        .await;
                    return;
                }
                
                // Calculate backoff delay
                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "finnhub", name = %driver.name, attempt, ?delay, %reason, "reconnecting");
                
                // Notify listeners about the reconnection attempt
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Reconnecting {
                            provider: ProviderKind::Finnhub,
                            attempt,
                            delay_ms: delay.as_millis() as u64,
                        }
                    })
                    .await;
                
                // Wait before next attempt
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[allow(dead_code)]
enum SessionResult {
    Stopped,
    Failed(String),
}

// Internal session handler that performs Finnhub-specific subscription.
async fn ws_session(
    driver: &FinnhubDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
) -> SessionResult {
    // Note: The '/' path is required by Finnhub's LB. 
    // omitting it results in 400 Bad Request.
    let url = format!("wss://ws.finnhub.io/?token={}", driver.api_token);

    // Establish WebSocket connection
    let (ws_stream, _) = match connect_async(&url).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "finnhub", name = %driver.name, %e, "connect failed");
            return SessionResult::Failed(e.to_string());
        }
    };

    info!(provider = "finnhub", name = %driver.name, "connected");
    // Notify listeners about successful connection
    let _ = tx
        .send(MarketEvent::Status {
            source: driver.name.clone(),
            status: ProviderStatus::Connected { provider: ProviderKind::Finnhub },
        })
        .await;

    let (mut write, mut read) = ws_stream.split();

    // Finnhub requires one subscription message per symbol
    for symbol in symbols {
        let msg = serde_json::json!({ "type": "subscribe", "symbol": symbol }).to_string();
        debug!(provider = "finnhub", name = %driver.name, out = %msg, "→ send");
        if write.send(Message::Text(msg.into())).await.is_err() {
            return SessionResult::Failed("subscribe send failed".into());
        }
    }

    loop {
        // Main loop to receive and process market data messages
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                // Process incoming message
                handle_message(text.as_str(), tx, &driver.name).await;
            }
            Some(Ok(Message::Close(frame))) => {
                // Handle clean connection closure
                let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Disconnected {
                            provider: ProviderKind::Finnhub,
                            reason:   reason.clone(),
                        }
                    })
                    .await;
                return SessionResult::Failed(reason);
            }
            Some(Ok(_)) => {} // Ignore binary/ping/pong frames
            Some(Err(e)) => {
                // Handle network/IO errors
                error!(provider = "finnhub", name = %driver.name, %e, "ws error");
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Disconnected {
                            provider: ProviderKind::Finnhub,
                            reason:   e.to_string(),
                        }
                    })
                    .await;
                return SessionResult::Failed(e.to_string());
            }
            None => {
                // Stream ended unexpectedly
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Disconnected {
                            provider: ProviderKind::Finnhub,
                            reason:   "stream ended".into(),
                        }
                    })
                    .await;
                return SessionResult::Failed("stream ended".into());
            }
        }
    }
}

// Parses a Finnhub message (which may contain multiple trades) and emits events.
async fn handle_message(text: &str, tx: &mpsc::Sender<MarketEvent>, source: &str) {
    // Parse the JSON message
    let obj: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    // Route based on message type
    match obj["type"].as_str() {
        Some("trade") => {} // Proceed to trade processing
        Some("ping")  => { 
            // Finnhub sends pings which require no response
            trace!(provider = "finnhub", name = source, "ping"); 
            return; 
        }
        Some(t)       => { 
            // Ignore other message types
            debug!(provider = "finnhub", name = source, msg_type = t, msg = text, "ignored"); 
            return; 
        }
        None          => return,
    }

    // Extract trade data array
    let data = match obj["data"].as_array() {
        Some(a) => a,
        None    => return,
    };

    // Iterate through individual trade items in the batch
    for item in data {
        // Extract required fields with validation
        let ticker     = match item["s"].as_str() { Some(s) => s.to_string(), None => continue };
        let price      = match item["p"].as_f64()  { Some(p) => p, None => continue };
        let volume     = item["v"].as_f64().unwrap_or(0.0);
        let time_ms    = item["t"].as_i64().unwrap_or(0);
        
        // Convert timestamp from milliseconds
        let timestamp  = Utc.timestamp_millis_opt(time_ms).single().unwrap_or_else(Utc::now);
        
        // Optional condition codes
        let conditions = item["c"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect())
            .unwrap_or_default();

        // Send normalized trade event
        let _ = tx
            .send(MarketEvent::Trade {
                source: source.to_string(),
                data: Trade {
                    ticker,
                    timestamp,
                    price,
                    extras: TradeExtras::Finnhub(FinnhubTradeExtras { volume, conditions }),
                    raw:    Some(item.to_string()),
                },
            })
            .await;
    }
}
