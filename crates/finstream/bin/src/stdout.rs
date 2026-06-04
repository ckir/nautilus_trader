use tokio::sync::mpsc;

use nautilus_finstream::MarketEvent;

/// Consumes unified market events and prints them as NDJSON to the terminal.
///
/// * `rx`: The MPSC receiver for market events from the core library.
///
/// This function follows a strict output convention:
/// - **Market Data (Trade, Quote)** is printed to **stdout**.
/// - **Status Events (Connected, Error, etc.)** are printed to **stderr**.
///
/// This separation allows operators to pipe stdout (e.g., to `jq` or a file) 
/// while still seeing infrastructure status updates on their console.
pub(crate) async fn run(mut rx: mpsc::Receiver<MarketEvent>) {
    while let Some(event) = rx.recv().await {
        match &event {
            MarketEvent::Status { status, .. } => {
                // Status events are infrastructure-related and go to stderr
                if let Ok(line) = serde_json::to_string(status) {
                    eprintln!("{line}");
                }
            }
            MarketEvent::Trade { .. } | MarketEvent::Quote { .. } => {
                // Market data events go to stdout for clean data piping
                match serde_json::to_string(&event) {
                    Ok(line) => println!("{line}"),
                    Err(e)   => eprintln!("serialize error: {e}"),
                }
            }
        }
    }
}
