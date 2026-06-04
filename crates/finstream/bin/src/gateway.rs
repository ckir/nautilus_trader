use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use serde::Deserialize;
use std::{collections::{HashMap, HashSet}, net::SocketAddr, sync::{Arc, Mutex}};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use nautilus_finstream::MarketEvent;

/// Shared state for the Axum WebSocket gateway.
#[derive(Clone)]
struct AppState {
    /// Map of source name -> broadcast channel.
    /// Used to route events from a specific provider to dedicated endpoints.
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<String>>>>,
    /// Global aggregator channel that broadcasts events from all sources.
    aggregator_tx: broadcast::Sender<String>,
}

/// Query parameters for WebSocket connection requests.
#[derive(Debug, Deserialize)]
struct WsQuery {
    /// Optional comma-separated symbol filter, e.g. `?symbols=AAPL,MSFT`.
    /// Only events matching these tickers will be sent to the client.
    symbols: Option<String>,
}

/// Starts the Axum WebSocket gateway server.
///
/// * `rx`: The MPSC receiver for unified market events from the core library.
/// * `port`: The TCP port to listen on.
pub(crate) async fn run(rx: mpsc::Receiver<MarketEvent>, port: u16) {
    // Create the global broadcast channel
    let (aggregator_tx, _) = broadcast::channel::<String>(2048);
    let state = Arc::new(AppState {
        channels: Arc::new(Mutex::new(HashMap::new())),
        aggregator_tx,
    });

    // Spawn a background task to fan-out unified MPSC events to broadcast channels
    tokio::spawn(forward_to_broadcast(rx, state.clone()));

    // Define the HTTP/WS routing table
    let app = Router::new()
        .route("/", get(aggregator_handler))
        .route("/ws/:source", get(source_handler))
        .with_state(state);

    // Bind to all interfaces on the specified port
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("gateway listening on ws://{addr}");
    info!("  - aggregator: ws://{addr}/");
    info!("  - per-source: ws://{addr}/ws/:source_name");

    // Start the Axum server
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}

/// Task that forwards events from the internal MPSC channel to public broadcast channels.
async fn forward_to_broadcast(
    mut rx: mpsc::Receiver<MarketEvent>,
    state: Arc<AppState>,
) {
    while let Some(event) = rx.recv().await {
        match &event {
            MarketEvent::Status { source, status } => {
                // Status events (connected, errors, etc.) are logged to stderr
                if let Ok(line) = serde_json::to_string(status) {
                    eprintln!("[{source}] {line}");
                }
            }
            MarketEvent::Trade { source, .. } | MarketEvent::Quote { source, .. } => {
                // Normalize the event to a JSON string once
                if let Ok(json) = serde_json::to_string(&event) {
                    // Send to the global aggregator (for clients at ws://host:port/)
                    let _ = state.aggregator_tx.send(json.clone());

                    // Send to the source-specific channel (for clients at ws://host:port/ws/source_name)
                    let mut channels = state.channels.lock().unwrap();
                    let tx = channels.entry(source.clone()).or_insert_with(|| {
                        // Create a new broadcast channel for this source on discovery
                        let (tx, _) = broadcast::channel::<String>(1024);
                        tx
                    });
                    let _ = tx.send(json);
                }
            }
        }
    }
}

/// Handler for the global aggregator WebSocket endpoint (`/`).
async fn aggregator_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WsQuery>,
) -> impl IntoResponse {
    // Parse symbol filter from query params
    let symbol_filter = parse_symbol_filter(query.symbols);
    // Subscribe to the global broadcast channel
    let rx = state.aggregator_tx.subscribe();
    // Upgrade connection to WebSocket
    ws.on_upgrade(move |socket| handle_socket(socket, rx, symbol_filter))
}

/// Handler for source-specific WebSocket endpoints (`/ws/:source`).
async fn source_handler(
    ws: WebSocketUpgrade,
    Path(source): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WsQuery>,
) -> impl IntoResponse {
    // Parse symbol filter from query params
    let symbol_filter = parse_symbol_filter(query.symbols);
    
    // Retrieve or create the broadcast channel for the requested source
    let tx = {
        let mut channels = state.channels.lock().unwrap();
        channels.entry(source).or_insert_with(|| {
            let (tx, _) = broadcast::channel::<String>(1024);
            tx
        }).clone()
    };

    // Subscribe to the source-specific channel
    let rx = tx.subscribe();
    // Upgrade connection to WebSocket
    ws.on_upgrade(move |socket| handle_socket(socket, rx, symbol_filter))
}

/// Parses a comma-separated symbol list into a HashSet for efficient filtering.
fn parse_symbol_filter(symbols: Option<String>) -> Option<HashSet<String>> {
    symbols.map(|s| {
        s.split(',').map(|sym| sym.trim().to_uppercase()).collect()
    })
}

/// Manages a single WebSocket client connection.
async fn handle_socket(
    mut socket: WebSocket,
    mut broadcast_rx: broadcast::Receiver<String>,
    symbol_filter: Option<HashSet<String>>,
) {
    loop {
        // Multi-plex between incoming broadcast events and client socket activity
        tokio::select! {
            result = broadcast_rx.recv() => {
                match result {
                    Ok(json) => {
                        // Apply symbol filter if requested by the client
                        if let Some(ref filter) = symbol_filter {
                            if !passes_filter(&json, filter) {
                                continue;
                            }
                        }
                        // Transmit the JSON message to the client
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            // Break loop if client disconnected
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Warn if the client is too slow and missing messages
                        warn!(skipped = n, "gateway client lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(_)) => {} // We ignore messages from the client (read-only stream)
                    _ => break,      // Connection closed or errored
                }
            }
        }
    }
}

/// Returns true if the event JSON contains a ticker that matches the symbol filter.
fn passes_filter(json: &str, filter: &HashSet<String>) -> bool {
    // Fast path: avoid full JSON parsing if possible (not yet implemented)
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return true, // Pass through invalid JSON to avoid dropping data
    };
    // Extract the ticker field and check against the allowed set
    match v["ticker"].as_str() {
        Some(sym) => filter.contains(&sym.to_uppercase()),
        None      => true, // Events without a ticker (e.g. status) are always passed
    }
}
