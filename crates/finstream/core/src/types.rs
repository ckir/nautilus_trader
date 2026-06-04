use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

// ── MarketEvent ───────────────────────────────────────────────────────────────

/// A normalized event emitted by the unified financial data stream.
///
/// This enum wraps all possible message types (trades, quotes, and status updates)
/// and identifies their source at the root level of the JSON output.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// A completed transaction on an exchange.
    Trade { 
        /// The unique name of the provider instance that emitted this trade.
        source: String, 
        /// The normalized trade data.
        data: Trade 
    },
    /// A bid/ask price update.
    Quote { 
        /// The unique name of the provider instance that emitted this quote.
        source: String, 
        /// The normalized quote data.
        data: Quote 
    },
    /// A change in the connectivity or health state of a provider driver.
    Status { 
        /// The unique name of the provider instance this status refers to.
        source: String, 
        /// The status update details.
        status: ProviderStatus 
    },
}

impl Serialize for MarketEvent {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            MarketEvent::Trade { source, data } => {
                // Initialize a map for the JSON object
                let mut map = s.serialize_map(None)?;
                // Source field identifies which provider instance emitted this
                map.serialize_entry("source", source)?;
                // Inline the Trade fields into the same map for a flat structure
                map.serialize_entry("type", "trade")?;
                map.serialize_entry("ticker", &data.ticker)?;
                map.serialize_entry("timestamp", &data.timestamp)?;
                map.serialize_entry("price", &data.price)?;
                // Include raw data if available
                if let Some(raw) = &data.raw {
                    map.serialize_entry("raw", raw)?;
                }
                // Nest provider-specific extras under a key named after the provider
                match &data.extras {
                    TradeExtras::Finnhub(v) => map.serialize_entry("finnhub", v)?,
                    TradeExtras::Yahoo(v)   => map.serialize_entry("yahoo", v)?,
                    TradeExtras::Massive(v) => map.serialize_entry("massive", v)?,
                }
                map.end()
            }
            MarketEvent::Quote { source, data } => {
                // Initialize a map for the JSON object
                let mut map = s.serialize_map(None)?;
                // Source field identifies which provider instance emitted this
                map.serialize_entry("source", source)?;
                // Inline the Quote fields into the same map for a flat structure
                map.serialize_entry("type", "quote")?;
                map.serialize_entry("ticker", &data.ticker)?;
                map.serialize_entry("timestamp", &data.timestamp)?;
                map.serialize_entry("price", &data.price)?;
                // Include raw data if available
                if let Some(raw) = &data.raw {
                    map.serialize_entry("raw", raw)?;
                }
                // Nest provider-specific extras under a key named after the provider
                match &data.extras {
                    QuoteExtras::Alpaca(v)  => map.serialize_entry("alpaca", v)?,
                    QuoteExtras::Yahoo(v)   => map.serialize_entry("yahoo", v)?,
                    QuoteExtras::Massive(v) => map.serialize_entry("massive", v)?,
                }
                map.end()
            }
            MarketEvent::Status { source, status } => {
                // Initialize a map for the JSON object
                let mut map = s.serialize_map(None)?;
                // Source field identifies which provider instance emitted this
                map.serialize_entry("source", source)?;
                
                // Convert the status enum to a temporary Value to flatten its fields
                let status_val = serde_json::to_value(status).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(status_obj) = status_val {
                    // Inject all fields from the status object into the current map
                    for (k, v) in status_obj {
                        map.serialize_entry(&k, &v)?;
                    }
                }
                map.end()
            }
        }
    }
}

// ── Trade ─────────────────────────────────────────────────────────────────────

/// Normalized trade data.
#[derive(Debug, Clone)]
pub struct Trade {
    /// The ticker symbol (format is provider-specific).
    pub ticker:    String,
    /// The UTC timestamp of the trade.
    pub timestamp: DateTime<Utc>,
    /// The execution price.
    pub price:     f64,
    /// Provider-specific metadata and additional fields.
    pub extras:    TradeExtras,
    /// The raw payload received from the provider, if captured.
    pub raw:       Option<String>,
}

impl Trade {
    /// Returns the kind of provider that generated this trade.
    pub fn provider(&self) -> ProviderKind {
        // Map the internal extras variant back to the provider kind
        match &self.extras {
            TradeExtras::Finnhub(_) => ProviderKind::Finnhub,
            TradeExtras::Yahoo(_)   => ProviderKind::Yahoo,
            TradeExtras::Massive(_) => ProviderKind::Massive,
        }
    }
}

impl Serialize for Trade {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        // Start a map for the trade object
        let mut map = s.serialize_map(None)?;
        map.serialize_entry("type", "trade")?;
        map.serialize_entry("ticker", &self.ticker)?;
        map.serialize_entry("timestamp", &self.timestamp)?;
        map.serialize_entry("price", &self.price)?;
        // Serialize raw data only if present
        if let Some(raw) = &self.raw {
            map.serialize_entry("raw", raw)?;
        }
        // Serialize extras under the provider-specific key
        match &self.extras {
            TradeExtras::Finnhub(v) => map.serialize_entry("finnhub", v)?,
            TradeExtras::Yahoo(v)   => map.serialize_entry("yahoo", v)?,
            TradeExtras::Massive(v) => map.serialize_entry("massive", v)?,
        }
        map.end()
    }
}

// ── Quote ─────────────────────────────────────────────────────────────────────

/// Normalized quote (bid/ask) data.
#[derive(Debug, Clone)]
pub struct Quote {
    /// The ticker symbol (format is provider-specific).
    pub ticker:    String,
    /// The UTC timestamp of the quote.
    pub timestamp: DateTime<Utc>,
    /// The mid price: (bid + ask) / 2.0.
    pub price:     f64,
    /// Provider-specific metadata and additional fields.
    pub extras:    QuoteExtras,
    /// The raw payload received from the provider, if captured.
    pub raw:       Option<String>,
}

impl Quote {
    /// Returns the kind of provider that generated this quote.
    pub fn provider(&self) -> ProviderKind {
        // Map the internal extras variant back to the provider kind
        match &self.extras {
            QuoteExtras::Alpaca(_)  => ProviderKind::Alpaca,
            QuoteExtras::Yahoo(_)   => ProviderKind::Yahoo,
            QuoteExtras::Massive(_) => ProviderKind::Massive,
        }
    }
}

impl Serialize for Quote {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        // Start a map for the quote object
        let mut map = s.serialize_map(None)?;
        map.serialize_entry("type", "quote")?;
        map.serialize_entry("ticker", &self.ticker)?;
        map.serialize_entry("timestamp", &self.timestamp)?;
        map.serialize_entry("price", &self.price)?;
        // Serialize raw data only if present
        if let Some(raw) = &self.raw {
            map.serialize_entry("raw", raw)?;
        }
        // Serialize extras under the provider-specific key
        match &self.extras {
            QuoteExtras::Alpaca(v)  => map.serialize_entry("alpaca", v)?,
            QuoteExtras::Yahoo(v)   => map.serialize_entry("yahoo", v)?,
            QuoteExtras::Massive(v) => map.serialize_entry("massive", v)?,
        }
        map.end()
    }
}

// ── TradeExtras ───────────────────────────────────────────────────────────────

/// Provider-specific metadata for trades.
#[derive(Debug, Clone)]
pub enum TradeExtras {
    /// Extra fields for Finnhub trades.
    Finnhub(FinnhubTradeExtras),
    /// Extra fields for Yahoo trades.
    Yahoo(YahooTradeExtras),
    /// Extra fields for Massive trades.
    Massive(MassiveTradeExtras),
}

// ── QuoteExtras ───────────────────────────────────────────────────────────────

/// Provider-specific metadata for quotes.
#[derive(Debug, Clone)]
pub enum QuoteExtras {
    /// Extra fields for Alpaca quotes.
    Alpaca(AlpacaQuoteExtras),
    /// Extra fields for Yahoo quotes.
    Yahoo(YahooQuoteExtras),
    /// Extra fields for Massive quotes.
    Massive(MassiveQuoteExtras),
}

// ── Alpaca extras ─────────────────────────────────────────────────────────────

/// Extra fields provided by the Alpaca WebSocket feed for quotes.
#[derive(Debug, Clone, Serialize)]
pub struct AlpacaQuoteExtras {
    /// The best bid price.
    pub bid:      f64,
    /// The best ask price.
    pub ask:      f64,
    /// The number of shares available at the bid price.
    pub bid_size: f64,
    /// The number of shares available at the ask price.
    pub ask_size: f64,
    /// The exchange the bid originated from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bid_exchange: Option<String>,
    /// The exchange the ask originated from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ask_exchange: Option<String>,
    /// Condition codes for the quote.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
    /// The consolidated tape ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tape: Option<String>,
}

// ── Finnhub extras ────────────────────────────────────────────────────────────

/// Extra fields provided by the Finnhub WebSocket feed for trades.
#[derive(Debug, Clone, Serialize)]
pub struct FinnhubTradeExtras {
    /// The volume/size of the trade.
    pub volume: f64,
    /// Condition codes for the trade.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
}

// ── Yahoo extras ──────────────────────────────────────────────────────────────

/// Extra fields provided by the Yahoo Finance WebSocket feed for trades.
#[derive(Debug, Clone, Serialize)]
pub struct YahooTradeExtras {
    /// The exchange code.
    pub exchange:     String,
    /// The currency code.
    pub currency:     String,
    /// The current market hours state.
    pub market_hours: i32,
    /// The absolute change in price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change:       f64,
    /// The percentage change in price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change_pct:   f64,
    /// Cumulative day volume.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub volume:       i64,
    /// Opening price for the day.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub open:         f64,
    /// Day high price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub day_high:     f64,
    /// Day low price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub day_low:      f64,
    /// Previous close price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub prev_close:   f64,
    /// Current market capitalization.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub market_cap:   f64,
    /// Current bid price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub bid:          f64,
    /// Current ask price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub ask:          f64,
    /// Size at current bid.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub bid_size:     i64,
    /// Size at current ask.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub ask_size:     i64,
    /// Short name/description of the security.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub short_name:   String,
}

/// Extra fields provided by the Yahoo Finance WebSocket feed for quotes.
#[derive(Debug, Clone, Serialize)]
pub struct YahooQuoteExtras {
    /// Current bid price.
    pub bid:          f64,
    /// Current ask price.
    pub ask:          f64,
    /// Size at current bid.
    pub bid_size:     i64,
    /// Size at current ask.
    pub ask_size:     i64,
    /// The exchange code.
    pub exchange:     String,
    /// The currency code.
    pub currency:     String,
    /// The current market hours state.
    pub market_hours: i32,
    /// The absolute change in price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change:       f64,
    /// The percentage change in price.
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change_pct:   f64,
}

// ── Massive extras ────────────────────────────────────────────────────────────

/// Extra fields provided by the Massive (Polygon.io) WebSocket feed for trades.
#[derive(Debug, Clone, Serialize)]
pub struct MassiveTradeExtras {
    /// The size/volume of the trade.
    pub size: f64,
    /// Condition codes for the trade.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
}

/// Extra fields provided by the Massive (Polygon.io) WebSocket feed for quotes.
#[derive(Debug, Clone, Serialize)]
pub struct MassiveQuoteExtras {
    /// The best bid price.
    pub bid:      f64,
    /// The best ask price.
    pub ask:      f64,
    /// The number of shares at the bid.
    pub bid_size: f64,
    /// The number of shares at the ask.
    pub ask_size: f64,
}

// ── ProviderStatus ────────────────────────────────────────────────────────────

/// Infrastructure events describing the health of a provider connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderStatus {
    /// Successfully connected and authenticated.
    Connected    { provider: ProviderKind },
    /// Connection lost or closed.
    Disconnected { provider: ProviderKind, reason: String },
    /// An automatic reconnection attempt is scheduled.
    Reconnecting { provider: ProviderKind, attempt: u32, delay_ms: u64 },
    /// A fatal or retry-exhausted error occurred.
    Error        { provider: ProviderKind, message: String },
}

// ── ProviderKind ──────────────────────────────────────────────────────────────

/// Supported financial data providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    /// Alpaca Markets.
    Alpaca,
    /// Finnhub.io.
    Finnhub,
    /// Massive.com (formerly Polygon.io).
    Massive,
    /// Yahoo Finance streamer.
    Yahoo,
}

impl std::fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alpaca  => write!(f, "alpaca"),
            Self::Finnhub => write!(f, "finnhub"),
            Self::Massive => write!(f, "massive"),
            Self::Yahoo   => write!(f, "yahoo"),
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn is_zero_f64(v: &f64) -> bool { *v == 0.0 }
fn is_zero_i64(v: &i64) -> bool { *v == 0 }
