use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{
        MarketEvent, MassiveQuoteExtras, MassiveTradeExtras, ProviderKind, ProviderStatus,
        Quote, QuoteExtras, Trade, TradeExtras,
    },
};

/// The WebSocket URL for the Massive (Polygon.io) stocks feed.
const WS_URL: &str = "wss://socket.massive.com/stocks";

/// Driver for the Massive (Polygon.io) real-time data WebSocket.
pub struct MassiveDriver {
    /// Unique name for this driver instance.
    pub name:    String,
    /// Massive (Polygon.io) API key.
    pub api_key: String,
}

impl ProviderDriver for MassiveDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Massive
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> Result<(), crate::error::FinStreamError> {
        if self.api_key.is_empty() {
            return Err(crate::error::FinStreamError::Config("Massive API key is missing".into()));
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

enum SessionResult {
    #[allow(dead_code)]
    Stopped,
    Failed(String),
    Fatal(String),
}

// Internal run loop that handles automatic reconnection for Massive.
async fn run_loop(
    driver: MassiveDriver,
    symbols: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    policy: ReconnectPolicy,
) {
    let mut attempt = 0u32;
    let mut first_failure: Option<Instant> = None;

    loop {
        // Start a new WebSocket session
        match ws_session(&driver, &symbols, &tx).await {
            SessionResult::Stopped => {
                // Clean shutdown requested
                break;
            }

            SessionResult::Fatal(reason) => {
                // Non-retryable error (e.g. invalid API key)
                error!(provider = "massive", name = %driver.name, %reason, "fatal error — not retrying");
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Error {
                            provider: ProviderKind::Massive,
                            message:  reason,
                        }
                    })
                    .await;
                return;
            }

            SessionResult::Failed(reason) => {
                // Retryable error (e.g. network timeout)
                let elapsed = first_failure.get_or_insert_with(Instant::now).elapsed();
                
                // Check if we should stop retrying based on policy
                if policy.is_exceeded(attempt, elapsed) {
                    let _ = tx
                        .send(MarketEvent::Status {
                            source: driver.name.clone(),
                            status: ProviderStatus::Error {
                                provider: ProviderKind::Massive,
                                message:  format!("retry limit reached: {reason}"),
                            }
                        })
                        .await;
                    return;
                }
                
                // Calculate backoff delay
                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "massive", name = %driver.name, attempt, ?delay, %reason, "reconnecting");
                
                // Notify listeners about the reconnection attempt
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Reconnecting {
                            provider: ProviderKind::Massive,
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

// Internal session handler that performs Massive-specific auth and subscription.
async fn ws_session(
    driver: &MassiveDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
) -> SessionResult {
    // Establish WebSocket connection
    let (ws_stream, _) = match connect_async(WS_URL).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "massive", name = %driver.name, %e, "connect failed");
            return SessionResult::Failed(e.to_string());
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // ── Step 1: Expect connected status ──────────────────────────────────────
    match read.next().await {
        Some(Ok(Message::Text(text))) => {
            // Massive sends an initial status message upon connection
            let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            let first = &msgs[0];
            if first["ev"] != "status" || first["status"] != "connected" {
                return SessionResult::Fatal(format!("unexpected connected message: {text}"));
            }
            debug!(provider = "massive", name = %driver.name, "received connected");
        }
        other => {
            return SessionResult::Fatal(format!("expected connected message, got: {other:?}"));
        }
    }

    // ── Step 2: Send authentication ──────────────────────────────────────────
    let auth = serde_json::json!({ "action": "auth", "params": driver.api_key }).to_string();
    if write.send(Message::Text(auth.into())).await.is_err() {
        return SessionResult::Failed("auth send failed".into());
    }

    // ── Step 3: Expect auth response ─────────────────────────────────────────
    match read.next().await {
        Some(Ok(Message::Text(text))) => {
            // Parse authentication response
            let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            let first = &msgs[0];
            let status = first["status"].as_str().unwrap_or("");
            // Handle explicit auth failures
            if status == "auth_failed" || status == "auth_timeout" {
                let msg = first["message"].as_str().unwrap_or("auth failed").to_string();
                return SessionResult::Fatal(msg);
            }
            // Verify success
            if status != "auth_success" {
                return SessionResult::Fatal(format!("unexpected auth response: {text}"));
            }
            info!(provider = "massive", name = %driver.name, "authenticated");
        }
        other => {
            return SessionResult::Fatal(format!("expected auth response, got: {other:?}"));
        }
    }

    // Notify listeners that we are connected and authenticated
    let _ = tx
        .send(MarketEvent::Status {
            source: driver.name.clone(),
            status: ProviderStatus::Connected { provider: ProviderKind::Massive },
        })
        .await;

    // ── Step 4: Send subscription ────────────────────────────────────────────
    if !symbols.is_empty() {
        // Construct a comma-separated list of T.SYMBOL and Q.SYMBOL
        let params: String = symbols
            .iter()
            .flat_map(|s| [format!("T.{s}"), format!("Q.{s}")])
            .collect::<Vec<_>>()
            .join(",");
        let sub = serde_json::json!({ "action": "subscribe", "params": params }).to_string();
        // Send subscription request
        if write.send(Message::Text(sub.into())).await.is_err() {
            return SessionResult::Failed("subscribe send failed".into());
        }

        // ── Step 5: Expect subscription confirmation ─────────────────────────
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                let first = &msgs[0];
                let status = first["status"].as_str().unwrap_or("");
                if status != "success" {
                    return SessionResult::Fatal(format!("subscription failed: {text}"));
                }
                info!(provider = "massive", name = %driver.name, ?symbols, "subscribed");
            }
            other => {
                return SessionResult::Fatal(format!("expected subscription confirmation, got: {other:?}"));
            }
        }
    }

    loop {
        // ── Step 6: Message loop ─────────────────────────────────────────────
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                // Process batch of messages
                handle_messages(text.as_str(), tx, &driver.name).await;
            }
            Some(Ok(Message::Ping(d))) => {
                // Respond to WebSocket pings
                let _ = write.send(Message::Pong(d)).await;
            }
            Some(Ok(Message::Close(frame))) => {
                // Handle clean closure
                let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Disconnected {
                            provider: ProviderKind::Massive,
                            reason:   reason.clone(),
                        }
                    })
                    .await;
                return SessionResult::Failed(reason);
            }
            Some(Err(e)) => {
                // Handle network/IO errors
                error!(provider = "massive", name = %driver.name, %e, "ws error");
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Disconnected {
                            provider: ProviderKind::Massive,
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
                            provider: ProviderKind::Massive,
                            reason:   "stream ended".into(),
                        }
                    })
                    .await;
                return SessionResult::Failed("stream ended".into());
            }
            _ => {} // Ignore other frames
        }
    }
}

// Dispatches a Massive message batch to appropriate parsers.
async fn handle_messages(text: &str, tx: &mpsc::Sender<MarketEvent>, source: &str) {
    // Parse JSON array of messages
    let msgs: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    let arr = match msgs.as_array() {
        Some(a) => a,
        None    => return,
    };

    // Route each message in the batch
    for msg in arr {
        match msg["ev"].as_str() {
            Some("T") => {
                // Parse normalized trade
                if let Some(mut event) = parse_trade(msg) {
                    event.raw = Some(msg.to_string());
                    let _ = tx.send(MarketEvent::Trade {
                        source: source.to_string(),
                        data: event,
                    }).await;
                }
            }
            Some("Q") => {
                // Parse normalized quote
                if let Some(mut event) = parse_quote(msg) {
                    event.raw = Some(msg.to_string());
                    let _ = tx.send(MarketEvent::Quote {
                        source: source.to_string(),
                        data: event,
                    }).await;
                }
            }
            Some("status") => {
                // Log non-auth status updates at debug level
                debug!(
                    provider = "massive",
                    name = source,
                    status = msg["status"].as_str().unwrap_or(""),
                    message = msg["message"].as_str().unwrap_or(""),
                    "status event"
                );
            }
            _ => {}
        }
    }
}

// Parses a single Massive 'T' (trade) message into a normalized Trade struct.
fn parse_trade(msg: &serde_json::Value) -> Option<Trade> {
    // Extract required fields
    let ticker     = msg["sym"].as_str()?.to_string();
    let price      = msg["p"].as_f64()?;
    let size       = msg["s"].as_f64().unwrap_or(0.0);
    let time_ms    = msg["t"].as_i64().unwrap_or(0);
    // Convert timestamp
    let timestamp  = Utc.timestamp_millis_opt(time_ms).single().unwrap_or_else(Utc::now);
    // Convert integer condition codes to strings
    let conditions = msg["c"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_i64().map(|n| n.to_string())).collect())
        .unwrap_or_default();

    Some(Trade {
        ticker,
        timestamp,
        price,
        extras: TradeExtras::Massive(MassiveTradeExtras { size, conditions }),
        raw:    None,
    })
}

// Parses a single Massive 'Q' (quote) message into a normalized Quote struct.
fn parse_quote(msg: &serde_json::Value) -> Option<Quote> {
    // Extract required fields
    let ticker = msg["sym"].as_str()?.to_string();
    let bid    = msg["bp"].as_f64().unwrap_or(0.0);
    let ask    = msg["ap"].as_f64().unwrap_or(0.0);
    // Quote sizes are in round lots (100 shares), multiply to get actual count
    let bid_size = msg["bs"].as_f64().unwrap_or(0.0) * 100.0;
    let ask_size = msg["as"].as_f64().unwrap_or(0.0) * 100.0;
    let time_ms  = msg["t"].as_i64().unwrap_or(0);
    let timestamp = Utc.timestamp_millis_opt(time_ms).single().unwrap_or_else(Utc::now);
    // Compute mid price
    let price = (bid + ask) / 2.0;

    Some(Quote {
        ticker,
        timestamp,
        price,
        extras: QuoteExtras::Massive(MassiveQuoteExtras { bid, ask, bid_size, ask_size }),
        raw:    None,
    })
}
