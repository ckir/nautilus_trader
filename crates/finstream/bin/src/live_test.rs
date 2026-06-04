use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};
use clap::Parser;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use chrono::{DateTime, NaiveDateTime, Utc, Datelike, Weekday};

use nautilus_finstream::{
    config::{AppConfig, ProviderInstanceConfig},
    providers::{
        alpaca::{AlpacaDriver, AlpacaFeed},
        finnhub::FinnhubDriver,
        massive::MassiveDriver,
        yahoo::YahooDriver,
        ProviderDriver,
    },
    reconnect::ReconnectPolicy,
    MarketEvent, ProviderKind,
};

// ── Nasdaq Market Info API Models ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct NasdaqResponse {
    data: Option<NasdaqData>,
    status: NasdaqStatus,
}

#[derive(Debug, Deserialize)]
struct NasdaqData {
    #[serde(rename = "mrktStatus")]
    mrkt_status: String,
    #[serde(rename = "marketIndicator")]
    indicator: String,
    #[serde(rename = "pmOpenRaw")]
    pm_open: String,
    #[serde(rename = "openRaw")]
    open: String,
    #[serde(rename = "closeRaw")]
    close: String,
    #[serde(rename = "ahCloseRaw")]
    ah_close: String,
}

#[derive(Debug, Deserialize)]
struct NasdaqStatus {
    #[serde(rename = "rCode")]
    r_code: i32,
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum MarketPhase {
    PreMarket,
    Open,
    AfterHours,
    Closed,
}

impl std::fmt::Display for MarketPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreMarket => write!(f, "Pre-Market"),
            Self::Open => write!(f, "Open"),
            Self::AfterHours => write!(f, "After-Hours"),
            Self::Closed => write!(f, "Closed"),
        }
    }
}

// ── CLI Arguments ─────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "finstream-live-test", about = "Live integration testing suite for multiple providers")]
struct Args {
    /// Duration to run the tests in minutes
    #[arg(short, long, default_value = "5")]
    minutes: u64,

    /// Clear existing log files in the test directory before starting
    #[arg(short, long)]
    clear: bool,

    /// Path to the log directory
    #[arg(short, long, default_value = "test_logs")]
    log_dir: String,
}

// ── Main Entry Point ──────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let args = Args::parse();

    // 1. Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();

    info!("Starting finstream live testing suite...");

    // 2. Load layered config
    let cfg: AppConfig = {
        use config::{Config, Environment, File};
        Config::builder()
            .add_source(File::with_name("finstream.toml").required(false))
            .add_source(Environment::with_prefix("FINSTREAM").separator("__"))
            .build()
            .and_then(|c| c.try_deserialize())
            .unwrap_or_default()
    };

    // 3. Setup log directory
    let log_path = PathBuf::from(&args.log_dir);
    if args.clear && log_path.exists() {
        info!("Clearing existing logs in {}...", args.log_dir);
        let _ = fs::remove_dir_all(&log_path);
    }
    fs::create_dir_all(&log_path)?;

    // 4. Check market status and detect phase overlap
    let market_data = check_market_status().await?;
    if let Some(ref data) = market_data {
        validate_test_duration(data, args.minutes);
    }

    // 5. Identify available providers based on API keys and config
    let available_providers = get_available_providers(&cfg);
    if available_providers.is_empty() {
        warn!("No providers found with valid API keys. Only Yahoo (unauthenticated) will be tested.");
    }

    // 6. Orchestrate tasks
    let total_duration = Duration::from_secs(args.minutes * 60);
    info!("Test suite scheduled to run for {} minutes.", args.minutes);

    run_suite(available_providers, total_duration, &log_path, &cfg.symbols.default).await?;

    info!("Live testing suite completed.");
    Ok(())
}

// ── Market Status Check ───────────────────────────────────────────────────────

async fn check_market_status() -> anyhow::Result<Option<NasdaqData>> {
    info!("Checking U.S. market status via Nasdaq API...");
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json, text/plain, */*".parse()?);
    headers.insert("origin", "https://www.nasdaq.com".parse()?);
    headers.insert("referer", "https://www.nasdaq.com/".parse()?);
    headers.insert("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36".parse()?);
    
    let response = client.get("https://api.nasdaq.com/api/market-info")
        .headers(headers)
        .send()
        .await;

    match response {
        Ok(res) => {
            let nasdaq: NasdaqResponse = res.json().await?;
            if nasdaq.status.r_code == 200 {
                if let Some(data) = nasdaq.data {
                    info!("Market Status: {} ({})", data.mrkt_status, data.indicator);
                    return Ok(Some(data));
                } else {
                    warn!("Nasdaq API returned rCode 200 but empty data.");
                }
            } else {
                warn!("Nasdaq API returned invalid rCode: {}", nasdaq.status.r_code);
            }
        }
        Err(e) => {
            error!("Failed to check market status: {}. Proceeding anyway...", e);
        }
    }
    
    Ok(None)
}

fn get_et_offset_hours(dt: DateTime<Utc>) -> i64 {
    // Basic US DST logic: 2nd Sunday of March to 1st Sunday of November
    let year = dt.year();
    
    // DST Start (2nd Sunday of March)
    let mut dst_start = NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(year, 3, 1).unwrap(),
        chrono::NaiveTime::from_hms_opt(2, 0, 0).unwrap()
    );
    let mut sundays = 0;
    while sundays < 2 {
        if dst_start.weekday() == Weekday::Sun { sundays += 1; }
        if sundays < 2 { dst_start += chrono::Duration::days(1); }
    }

    // DST End (1st Sunday of November)
    let mut dst_end = NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(year, 11, 1).unwrap(),
        chrono::NaiveTime::from_hms_opt(2, 0, 0).unwrap()
    );
    while dst_end.weekday() != Weekday::Sun {
        dst_end += chrono::Duration::days(1);
    }

    let naive_now = dt.naive_utc();
    // UTC-4 if between start and end, else UTC-5
    if naive_now >= (dst_start - chrono::Duration::hours(5)) && naive_now < (dst_end - chrono::Duration::hours(4)) {
        -4
    } else {
        -5
    }
}

fn parse_et_raw(raw: &str, offset_hours: i64) -> Option<DateTime<Utc>> {
    let naive = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S").ok()?;
    // Adjust for offset to get UTC
    Some(DateTime::from_naive_utc_and_offset(naive - chrono::Duration::hours(offset_hours), Utc))
}

fn validate_test_duration(data: &NasdaqData, duration_minutes: u64) {
    let now_utc = Utc::now();
    let offset = get_et_offset_hours(now_utc);
    
    let pm_open  = parse_et_raw(&data.pm_open, offset);
    let open     = parse_et_raw(&data.open, offset);
    let close    = parse_et_raw(&data.close, offset);
    let ah_close = parse_et_raw(&data.ah_close, offset);

    let phase = match &data.mrkt_status[..] {
        "Open" => MarketPhase::Open,
        _ if data.indicator.contains("Pre-Market") => MarketPhase::PreMarket,
        _ if data.indicator.contains("After-Hours") => MarketPhase::AfterHours,
        _ => MarketPhase::Closed,
    };

    // Determine the exact boundary of the CURRENT phase
    let next_transition = match phase {
        MarketPhase::PreMarket => open,
        MarketPhase::Open => close,
        MarketPhase::AfterHours => ah_close,
        MarketPhase::Closed => {
            // If closed, check if we are actually in the gap BEFORE pre-market
            if let Some(pmo) = pm_open {
                if now_utc < pmo {
                    Some(pmo) // Transition to Pre-Market
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    if let Some(transition) = next_transition {
        let remaining = transition.signed_duration_since(now_utc);
        let requested = chrono::Duration::minutes(duration_minutes as i64);
        let safety_buffer = chrono::Duration::seconds(60);

        if requested + safety_buffer > remaining {
            warn!(
                "ATTENTION: Test duration ({}m) exceeds remaining time in {} phase ({:?}).",
                duration_minutes,
                phase,
                remaining
            );
            warn!("The test will overlap into the next market phase, which may invalidate comparative analysis.");
        } else {
            info!(
                "Test duration validated: requested {}m, phase {} ends in {:?}",
                duration_minutes, phase, remaining
            );
        }
    }
}

// ── Provider Identification ───────────────────────────────────────────────────

fn get_available_providers(cfg: &AppConfig) -> Vec<ProviderDesc> {
    let mut providers = Vec::new();

    // Alpaca
    let alpaca_key = env_or_cfg("ALPACA_API_KEY", find_alpaca_key(cfg));
    let alpaca_secret = env_or_cfg("ALPACA_API_SECRET", find_alpaca_secret(cfg));
    if !alpaca_key.is_empty() && !alpaca_secret.is_empty() {
        providers.push(ProviderDesc {
            kind: ProviderKind::Alpaca,
            variants: vec![
                ProviderVariant::Alpaca(AlpacaFeed::Iex),
                ProviderVariant::Alpaca(AlpacaFeed::Sip),
            ],
            sequential: true,
        });
    } else {
        info!("Skipping Alpaca: API keys not found in ENV or config.");
    }

    // Finnhub
    let finnhub_token = env_or_cfg("FINNHUB_API_TOKEN", find_finnhub_token(cfg));
    if !finnhub_token.is_empty() {
        providers.push(ProviderDesc {
            kind: ProviderKind::Finnhub,
            variants: vec![ProviderVariant::Default],
            sequential: false,
        });
    } else {
        info!("Skipping Finnhub: API token not found in ENV or config.");
    }

    // Massive
    let massive_key = env_or_cfg("MASSIVE_API_KEY", find_massive_key(cfg));
    if !massive_key.is_empty() {
        providers.push(ProviderDesc {
            kind: ProviderKind::Massive,
            variants: vec![ProviderVariant::Default],
            sequential: false,
        });
    } else {
        info!("Skipping Massive: API key not found in ENV or config.");
    }

    // Yahoo (Always available)
    providers.push(ProviderDesc {
        kind: ProviderKind::Yahoo,
        variants: vec![ProviderVariant::Default],
        sequential: false,
    });

    providers
}

// Helper to find credentials in AppConfig map
fn find_alpaca_key(cfg: &AppConfig) -> String {
    cfg.providers.values().find_map(|p| match p {
        ProviderInstanceConfig::Alpaca(c) => Some(c.api_key.clone()),
        _ => None,
    }).unwrap_or_default()
}
fn find_alpaca_secret(cfg: &AppConfig) -> String {
    cfg.providers.values().find_map(|p| match p {
        ProviderInstanceConfig::Alpaca(c) => Some(c.api_secret.clone()),
        _ => None,
    }).unwrap_or_default()
}
fn find_finnhub_token(cfg: &AppConfig) -> String {
    cfg.providers.values().find_map(|p| match p {
        ProviderInstanceConfig::Finnhub(c) => Some(c.api_token.clone()),
        _ => None,
    }).unwrap_or_default()
}
fn find_massive_key(cfg: &AppConfig) -> String {
    cfg.providers.values().find_map(|p| match p {
        ProviderInstanceConfig::Massive(c) => Some(c.api_key.clone()),
        _ => None,
    }).unwrap_or_default()
}

fn env_or_cfg(env_key: &str, cfg_val: String) -> String {
    let e = std::env::var(env_key).unwrap_or_default();
    if !e.is_empty() { e } else { cfg_val }
}

struct ProviderDesc {
    kind: ProviderKind,
    variants: Vec<ProviderVariant>,
    sequential: bool,
}

#[derive(Clone)]
enum ProviderVariant {
    Default,
    Alpaca(AlpacaFeed),
}

// ── Suite Orchestration ───────────────────────────────────────────────────────

async fn run_suite(
    providers: Vec<ProviderDesc>,
    total_duration: Duration,
    log_dir: &PathBuf,
    default_symbols: &[String],
) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<MarketEvent>(10000);
    let mut tasks = Vec::new();

    // Reconnect policy for testing (fail fast-ish)
    let policy = ReconnectPolicy {
        max_retries: Some(2),
        max_duration: Some(Duration::from_secs(60)),
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(10),
        jitter: true,
    };

    // Symbols strategy
    let mut test_symbols = default_symbols.to_vec();
    if !test_symbols.contains(&"BTC-USD".into()) { test_symbols.push("BTC-USD".into()); }
    if !test_symbols.contains(&"AAPL".into()) { test_symbols.push("AAPL".into()); }
    
    let crypto_only = vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()];
    let finnhub_symbols = vec!["BINANCE:BTCUSDT".into(), "BINANCE:ETHUSDT".into()];

    for p in providers {
        let tx = tx.clone();
        let policy = policy.clone();
        let symbols = test_symbols.clone();
        let crypto = crypto_only.clone();
        let fh_symbols = finnhub_symbols.clone();

        if p.sequential {
            let p_kind = p.kind;
            let variants = p.variants.clone();
            let handle = tokio::spawn(async move {
                let count = variants.len() as u32;
                let variant_duration = total_duration / count;
                
                for variant in variants {
                    let name = match &variant {
                        ProviderVariant::Alpaca(f) => format!("alpaca_{:?}", f).to_lowercase(),
                        _ => format!("{:?}", p_kind).to_lowercase(),
                    };

                    let driver: Box<dyn ProviderDriver> = match variant {
                        ProviderVariant::Alpaca(f) => Box::new(AlpacaDriver {
                            name: name.clone(),
                            api_key: std::env::var("ALPACA_API_KEY").unwrap_or_default(),
                            api_secret: std::env::var("ALPACA_API_SECRET").unwrap_or_default(),
                            feed: f,
                        }),
                        _ => unreachable!(),
                    };

                    info!("[{}] Starting sequential variant for {:?}", name, variant_duration);
                    let jh = driver.spawn(symbols.clone(), tx.clone(), policy.clone());
                    tokio::time::sleep(variant_duration).await;
                    jh.abort();
                    info!("[{}] Finished sequential variant.", name);
                }
            });
            tasks.push(handle);
        } else {
            for _variant in p.variants {
                let name = format!("{:?}", p.kind).to_lowercase();
                let driver: Box<dyn ProviderDriver> = match p.kind {
                    ProviderKind::Yahoo => Box::new(YahooDriver {
                        name: "yahoo_test".into(),
                        silence_secs: 60,
                        ping_interval_secs: 30,
                    }),
                    ProviderKind::Finnhub => Box::new(FinnhubDriver {
                        name: "finnhub_test".into(),
                        api_token: std::env::var("FINNHUB_API_TOKEN").unwrap_or_default(),
                    }),
                    ProviderKind::Massive => Box::new(MassiveDriver {
                        name: "massive_test".into(),
                        api_key: std::env::var("MASSIVE_API_KEY").unwrap_or_default(),
                    }),
                    _ => continue,
                };

                let provider_symbols = match p.kind {
                    ProviderKind::Finnhub => fh_symbols.clone(),
                    ProviderKind::Yahoo => {
                        let mut s = symbols.clone();
                        s.extend(crypto.clone());
                        s
                    }
                    _ => symbols.clone(),
                };

                info!("[{}] Starting parallel task.", name);
                let jh = driver.spawn(provider_symbols, tx.clone(), policy.clone());
                tasks.push(jh);
            }
        }
    }

    drop(tx); // Close the original sender so the receiver loop ends when all drivers are dropped/aborted

    // ── Event Processing & Logging ─────────────────────────────────────────────

    let mut writers: HashMap<String, File> = HashMap::new();
    let start_time = Instant::now();
    let mut event_count = 0;

    info!("Recording events. Press Ctrl+C to stop early.");

    while start_time.elapsed() < total_duration {
        let remaining = total_duration.saturating_sub(start_time.elapsed());
        if remaining.is_zero() { break; }

        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                event_count += 1;
                let source = match &event {
                    MarketEvent::Trade { source, .. } => source.clone(),
                    MarketEvent::Quote { source, .. } => source.clone(),
                    MarketEvent::Status { source, .. } => source.clone(),
                };

                let writer = writers.entry(source.clone()).or_insert_with(|| {
                    let filename = format!("{}_data.ndjson", source);
                    let path = log_dir.join(filename);
                    File::create(path).expect("Failed to create log file")
                });

                if let Ok(json) = serde_json::to_string(&event) {
                    let _ = writeln!(writer, "{}", json);
                }

                if event_count % 100 == 0 {
                    info!("Processed {} events...", event_count);
                }
            }
            Ok(None) => break,
            Err(_) => continue, // Timeout, just loop to check elapsed time
        }
    }

    info!("Total events recorded: {}", event_count);
    
    // Abort any remaining tasks
    for task in tasks {
        task.abort();
    }

    Ok(())
}
