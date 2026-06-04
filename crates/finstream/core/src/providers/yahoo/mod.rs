pub mod proto_handler;

use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{
        MarketEvent, ProviderKind, ProviderStatus, Quote, QuoteExtras, Trade, TradeExtras,
        YahooQuoteExtras, YahooTradeExtras,
    },
};
use proto_handler::{decode_yahoo_message, YahooPricing};

/// The WebSocket URL for the Yahoo Finance streamer feed.
const WS_URL: &str = "wss://streamer.finance.yahoo.com/?version=2";

// ── Control messages sent to the running driver task ──────────────────────────

/// Commands that can be sent to a live Yahoo driver task to modify its behavior.
pub enum YahooControl {
    /// Subscribe to additional symbols on the active connection.
    Subscribe(Vec<String>),
    /// Unsubscribe from symbols. Uses `{"unsubscribe":[...]}` — verified to
    /// stop data without closing the connection.
    Unsubscribe(Vec<String>),
}

// ── Driver ────────────────────────────────────────────────────────────────────

/// Driver for the Yahoo Finance real-time data WebSocket.
pub struct YahooDriver {
    /// Unique name for this driver instance.
    pub name:               String,
    /// Reconnect if no data is received for this many seconds.
    pub silence_secs:       u32,
    /// How often to send a WebSocket ping to keep the connection alive.
    pub ping_interval_secs: u32,
}

impl ProviderDriver for YahooDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Yahoo
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn spawn(
        self: Box<Self>,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            run_loop(*self, symbols, tx, policy, None).await;
        })
    }
}

impl YahooDriver {
    /// Spawns the driver and returns a control sender for dynamic subscribe/unsubscribe.
    pub fn spawn_with_control(
        self,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> (JoinHandle<()>, mpsc::Sender<YahooControl>) {
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<YahooControl>(32);
        let handle = tokio::spawn(async move {
            run_loop(self, symbols, tx, policy, Some(ctrl_rx)).await;
        });
        (handle, ctrl_tx)
    }
}

// ── Internal run loop ─────────────────────────────────────────────────────────

// Internal run loop that handles automatic reconnection for Yahoo.
async fn run_loop(
    driver: YahooDriver,
    symbols: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    policy: ReconnectPolicy,
    mut ctrl_rx: Option<mpsc::Receiver<YahooControl>>,
) {
    let mut attempt = 0u32;
    let mut first_failure: Option<std::time::Instant> = None;
    // Carry subscriptions across reconnects so they survive a connection drop.
    let mut active_symbols = symbols;

    loop {
        // Start a new WebSocket session with the current set of active symbols
        match ws_session(&driver, &active_symbols, &tx, ctrl_rx.as_mut()).await {
            SessionResult::Stopped => {
                // Clean shutdown requested
                break;
            }
            SessionResult::Failed { reason, symbols_at_close } => {
                // Preserve whatever symbols were active when the session died.
                // This ensures we resubscribe to dynamically added symbols after reconnect.
                active_symbols = symbols_at_close;

                let elapsed = first_failure.get_or_insert_with(std::time::Instant::now).elapsed();
                
                // Check retry policy
                if policy.is_exceeded(attempt, elapsed) {
                    let _ = tx
                        .send(MarketEvent::Status {
                            source: driver.name.clone(),
                            status: ProviderStatus::Error {
                                provider: ProviderKind::Yahoo,
                                message:  format!("retry limit reached: {reason}"),
                            }
                        })
                        .await;
                    return;
                }
                
                // Calculate backoff
                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "yahoo", name = %driver.name, attempt, ?delay, "reconnecting");
                
                // Notify status
                let _ = tx
                    .send(MarketEvent::Status {
                        source: driver.name.clone(),
                        status: ProviderStatus::Reconnecting {
                            provider: ProviderKind::Yahoo,
                            attempt,
                            delay_ms: delay.as_millis() as u64,
                        }
                    })
                    .await;
                
                // Wait before reconnecting
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[allow(dead_code)]
enum SessionResult {
    /// Graceful stop requested (reserved for future shutdown signal).
    Stopped,
    /// Connection lost or error; caller should reconnect.
    Failed {
        reason:          String,
        /// Symbol set active at the time of failure, preserved for reconnect.
        symbols_at_close: Vec<String>,
    },
}

// Internal session handler that performs Yahoo-specific Protobuf decoding.
async fn ws_session(
    driver: &YahooDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
    ctrl_rx: Option<&mut mpsc::Receiver<YahooControl>>,
) -> SessionResult {
    // Establish WebSocket connection
    let (ws_stream, _) = match connect_async(WS_URL).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "yahoo", name = %driver.name, %e, "connect failed");
            return SessionResult::Failed {
                reason:          e.to_string(),
                symbols_at_close: symbols.to_vec(),
            };
        }
    };

    info!(provider = "yahoo", name = %driver.name, "connected");
    // Notify successful connection
    let _ = tx
        .send(MarketEvent::Status {
            source: driver.name.clone(),
            status: ProviderStatus::Connected { provider: ProviderKind::Yahoo },
        })
        .await;

    let (mut write, mut read) = ws_stream.split();

    // Track active subscriptions so they can be preserved on reconnect.
    let mut active: Vec<String> = symbols.to_vec();

    // Send initial subscription request if symbols are provided
    if !active.is_empty() {
        let payload = serde_json::json!({ "subscribe": active }).to_string();
        debug!(provider = "yahoo", name = %driver.name, out = %payload, "→ send");
        if write.send(Message::Text(payload.into())).await.is_err() {
            return failed("subscribe send failed", &active);
        }
    }

    // Configure timers for keep-alive and activity monitoring
    let ping_interval = std::time::Duration::from_secs(driver.ping_interval_secs as u64);
    let silence_limit = std::time::Duration::from_secs(driver.silence_secs as u64);

    let mut ping_timer    = tokio::time::interval(ping_interval);
    let mut silence_timer = tokio::time::interval(silence_limit);
    // Skip initial ticks
    ping_timer.tick().await;
    silence_timer.tick().await;

    // We need to handle the optional control receiver polymorphically.
    // Use a dummy channel when none is provided so the select arm compiles uniformly.
    let (dummy_tx, mut dummy_rx) = mpsc::channel::<YahooControl>(1);
    drop(dummy_tx); // close it immediately so recv() always returns None
    let ctrl = match ctrl_rx {
        Some(r) => r as &mut mpsc::Receiver<YahooControl>,
        None    => &mut dummy_rx,
    };

    loop {
        // Multi-plex between WebSocket data, control messages, and timers
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Data received, reset the silence watchdog
                        silence_timer.reset();
                        // Parse and emit events from the pricing message
                        handle_text(text.as_str(), tx, &driver.name).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        // Server closed connection
                        let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                        let _ = tx.send(MarketEvent::Status {
                            source: driver.name.clone(),
                            status: ProviderStatus::Disconnected {
                                provider: ProviderKind::Yahoo,
                                reason: reason.clone(),
                            }
                        }).await;
                        return failed(&reason, &active);
                    }
                    Some(Ok(_)) => {} // Ignore binary/pong frames
                    Some(Err(e)) => {
                        // Network/IO error
                        error!(provider = "yahoo", name = %driver.name, %e, "ws error");
                        let _ = tx.send(MarketEvent::Status {
                            source: driver.name.clone(),
                            status: ProviderStatus::Disconnected {
                                provider: ProviderKind::Yahoo,
                                reason: e.to_string(),
                            }
                        }).await;
                        return failed(&e.to_string(), &active);
                    }
                    None => {
                        // Stream terminated
                        let _ = tx.send(MarketEvent::Status {
                            source: driver.name.clone(),
                            status: ProviderStatus::Disconnected {
                                provider: ProviderKind::Yahoo,
                                reason: "stream ended".into(),
                            }
                        }).await;
                        return failed("stream ended", &active);
                    }
                }
            }
            ctrl_msg = ctrl.recv() => {
                // Handle dynamic subscription changes
                match ctrl_msg {
                    Some(YahooControl::Subscribe(syms)) => {
                        // Filter out already active symbols
                        let new: Vec<String> = syms.into_iter()
                            .filter(|s| !active.contains(s))
                            .collect();
                        if !new.is_empty() {
                            let payload = serde_json::json!({ "subscribe": new }).to_string();
                            debug!(provider = "yahoo", name = %driver.name, out = %payload, "→ send");
                            if write.send(Message::Text(payload.into())).await.is_err() {
                                return failed("control subscribe send failed", &active);
                            }
                            active.extend(new);
                        }
                    }
                    Some(YahooControl::Unsubscribe(syms)) => {
                        // Send unsubscribe message. 
                        // Note: uses {"unsubscribe":[]} format to avoid server disconnect.
                        let payload = serde_json::json!({ "unsubscribe": syms }).to_string();
                        debug!(provider = "yahoo", name = %driver.name, out = %payload, "→ send");
                        if write.send(Message::Text(payload.into())).await.is_err() {
                            return failed("control unsubscribe send failed", &active);
                        }
                        active.retain(|s| !syms.contains(s));
                    }
                    None => {
                        // Control channel closed; continue running with current symbols
                    }
                }
            }
            _ = ping_timer.tick() => {
                // Send periodic WebSocket ping to keep connection alive
                let _ = write.send(Message::Ping(vec![].into())).await;
            }
            _ = silence_timer.tick() => {
                // Silence timeout triggered - no data received for silence_secs
                warn!(provider = "yahoo", name = %driver.name, "silence timeout, reconnecting");
                let _ = tx.send(MarketEvent::Status {
                    source: driver.name.clone(),
                    status: ProviderStatus::Disconnected {
                        provider: ProviderKind::Yahoo,
                        reason: "silence timeout".into(),
                    }
                }).await;
                return failed("silence timeout", &active);
            }
        }
    }
}

fn failed(reason: &str, active: &[String]) -> SessionResult {
    // Helper to construct a Failed session result with preserved symbols
    SessionResult::Failed {
        reason:          reason.to_string(),
        symbols_at_close: active.to_vec(),
    }
}

// Decodes a Yahoo Finance JSON-wrapped base64 Protobuf message and emits events.
async fn handle_text(text: &str, tx: &mpsc::Sender<MarketEvent>, source: &str) {
    // Parse the JSON wrapper
    let obj: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    // Yahoo sends various message types; we only care about 'pricing'
    if obj["type"].as_str() != Some("pricing") {
        debug!(provider = "yahoo", name = source, msg = text, "non-pricing message");
        return;
    }

    // Extract the base64-encoded Protobuf payload
    let b64 = match obj["message"].as_str() {
        Some(v) => v,
        None => return,
    };

    // Decode Protobuf message
    match decode_yahoo_message(b64) {
        Ok(pricing) => {
            // Transform raw proto struct into normalized YahooPricing helper
            let p: YahooPricing = pricing.clone().into();
            let ts = Utc.timestamp_millis_opt(p.time_ms).single().unwrap_or_else(Utc::now);
            
            // Log decoded pricing at trace level for debugging
            trace!(
                provider = "yahoo",
                name      = source,
                symbol    = %p.symbol,
                price     = p.price,
                bid       = p.bid,
                ask       = p.ask,
                bid_size  = p.bid_size,
                ask_size  = p.ask_size,
                last_size = p.last_size,
                change    = p.change,
                change_pct = p.change_pct,
                volume    = p.day_volume,
                open      = p.open_price,
                day_high  = p.day_high,
                day_low   = p.day_low,
                prev_close = p.prev_close,
                market_cap = p.market_cap,
                exchange  = %p.exchange,
                currency  = %p.currency,
                market_hours = p.market_hours,
                short_name = %p.short_name,
                "pricing"
            );

            // If a last trade price is present, emit a Trade event
            if p.price > 0.0 {
                let _ = tx
                    .send(MarketEvent::Trade {
                        source: source.to_string(),
                        data: Trade {
                            ticker:    p.symbol.clone(),
                            timestamp: ts,
                            price:     p.price,
                            extras: TradeExtras::Yahoo(YahooTradeExtras {
                                exchange:     p.exchange.clone(),
                                currency:     p.currency.clone(),
                                market_hours: p.market_hours,
                                change:       p.change,
                                change_pct:   p.change_pct,
                                volume:       p.day_volume,
                                open:         p.open_price,
                                day_high:     p.day_high,
                                day_low:      p.day_low,
                                prev_close:   p.prev_close,
                                market_cap:   p.market_cap,
                                bid:          p.bid,
                                ask:          p.ask,
                                bid_size:     p.bid_size,
                                ask_size:     p.ask_size,
                                short_name:   p.short_name.clone(),
                            }),
                            raw: Some(format!("{:?}", pricing)),
                        },
                    })
                    .await;
            }

            // If bid/ask prices are present, emit a Quote event
            if p.bid > 0.0 || p.ask > 0.0 {
                let price = (p.bid + p.ask) / 2.0;
                let _ = tx
                    .send(MarketEvent::Quote {
                        source: source.to_string(),
                        data: Quote {
                            ticker:    p.symbol,
                            timestamp: ts,
                            price,
                            extras: QuoteExtras::Yahoo(YahooQuoteExtras {
                                bid:          p.bid,
                                ask:          p.ask,
                                bid_size:     p.bid_size,
                                ask_size:     p.ask_size,
                                exchange:     p.exchange,
                                currency:     p.currency,
                                market_hours: p.market_hours,
                                change:       p.change,
                                change_pct:   p.change_pct,
                            }),
                            raw: Some(format!("{:?}", pricing)),
                        },
                    })
                    .await;
            }
        }
        Err(e) => {
            error!(provider = "yahoo", %e, "decode failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::proto_handler::decode_yahoo_message;

    #[test]
    fn decode_known_tsla_message() {
        let b64 = "CgRUU0xBFYG9y0MYgM6B7JtnKgNOTVMwCDgCRbV+qr1lANStvtgBBA==";
        let pricing = decode_yahoo_message(b64).expect("decode should succeed");
        assert_eq!(pricing.id, "TSLA");
        assert!(pricing.price > 0.0);
    }
}
