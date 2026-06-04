//! Live test: Yahoo Finance WebSocket unsubscribe behaviour.
//!
//! Tests whether {"unsubscribe":[...]} truly stops data on the same connection
//! by verifying the connection is still alive after (resubscribe and check).
//!
//! Run with:
//!   cargo run -p finstream-core --example yahoo_unsubscribe --features yahoo

use base64::{engine::general_purpose, Engine as _};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://streamer.finance.yahoo.com/?version=2";
const SYMBOLS: &[&str] = &["BTC-USD", "ETH-USD"];
const PRIME_COUNT: usize = 5;

#[derive(Clone, PartialEq, prost::Message)]
struct PricingData {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(float, tag = "2")]
    pub price: f32,
}

#[tokio::main]
async fn main() {
    println!("=== Yahoo Finance unsubscribe deep test ===\n");

    let (ws, _) = connect_async(WS_URL).await.expect("connect failed");
    let (mut write, mut read) = ws.split();

    // --- Subscribe ---
    send(&mut write, serde_json::json!({ "subscribe": SYMBOLS }).to_string()).await;
    println!("[1] Subscribed to {}", SYMBOLS.join(", "));

    // --- Prime: collect messages ---
    let mut count = 0;
    while count < PRIME_COUNT {
        if let Some(p) = next_pricing(&mut read).await {
            count += 1;
            println!("  [{count}/{PRIME_COUNT}] {} @ {:.2}", p.id, p.price);
        }
    }

    // --- Send {"unsubscribe": [...]} ---
    send(&mut write, serde_json::json!({ "unsubscribe": SYMBOLS }).to_string()).await;
    println!("\n[2] Sent {{\"unsubscribe\": [...]}}");

    // Check for 8s: does data stop, and does the connection stay open?
    println!("[3] Monitoring 8s for residual data …");
    let residual = count_messages_for(&mut read, 8).await;
    println!("    Residual messages in 8s: {residual}");

    // --- Attempt ping to check if connection is still alive ---
    let ping_ok = write.send(Message::Ping(b"probe".to_vec().into())).await.is_ok();
    println!("[4] Ping sent: {ping_ok}");

    // Wait a moment for Pong
    let pong = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        read.next(),
    )
    .await;
    let connection_alive = matches!(pong, Ok(Some(Ok(Message::Pong(_)))));
    println!("    Pong received (connection alive): {connection_alive}");

    // --- Re-subscribe on the same connection ---
    if connection_alive {
        send(&mut write, serde_json::json!({ "subscribe": SYMBOLS }).to_string()).await;
        println!("\n[5] Re-subscribed on same connection — waiting for data …");
        let resumed = count_messages_for(&mut read, 8).await;
        println!("    Messages after re-subscribe: {resumed}");

        println!("\n=== VERDICT ===");
        if residual == 0 && resumed > 0 {
            println!("  ✓ {{\"unsubscribe\":[...]}} works correctly.");
            println!("    Data stopped after unsub, connection stayed alive,");
            println!("    and data resumed after re-subscribe.");
        } else if residual > 0 {
            println!("  ✗ Unsubscribe had no effect — data kept flowing ({residual} messages).");
        } else if resumed == 0 {
            println!("  ~ Unsubscribe stopped data but re-subscribe also got nothing.");
            println!("    Connection may be in a broken state.");
        }
    } else {
        println!("\n=== VERDICT ===");
        if residual == 0 {
            println!("  ~ {{\"unsubscribe\":[...]}} stopped data BUT the connection also died");
            println!("    (no Pong response). Server likely silently closed the TCP socket.");
            println!("    Unsubscribe is not usable — reconnect is required to change symbols.");
        } else {
            println!("  ✗ Unsubscribe had no effect and connection is broken.");
        }
    }
}

async fn send(
    write: &mut (impl futures_util::Sink<Message, Error = impl std::fmt::Debug> + Unpin),
    msg: String,
) {
    write.send(Message::Text(msg.into())).await.unwrap();
}

async fn next_pricing(
    read: &mut (impl futures_util::Stream<Item = Result<Message, impl std::fmt::Debug>> + Unpin),
) -> Option<PricingData> {
    loop {
        match read.next().await? {
            Ok(Message::Text(text)) => {
                if let Some(p) = decode(text.as_str()) {
                    return Some(p);
                }
            }
            Ok(Message::Close(_)) => return None,
            Ok(_) => {}
            Err(_) => return None,
        }
    }
}

async fn count_messages_for(
    read: &mut (impl futures_util::Stream<Item = Result<Message, impl std::fmt::Debug>> + Unpin),
    secs: u64,
) -> usize {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut count = 0usize;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if decode(text.as_str()).is_some() {
                    count += 1;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(None) | Err(_) => break,
            _ => {}
        }
    }
    count
}

fn decode(text: &str) -> Option<PricingData> {
    let obj: serde_json::Value = serde_json::from_str(text).ok()?;
    if obj["type"].as_str() != Some("pricing") {
        return None;
    }
    let bytes = general_purpose::STANDARD
        .decode(obj["message"].as_str()?)
        .ok()?;
    PricingData::decode(&bytes[..]).ok()
}
