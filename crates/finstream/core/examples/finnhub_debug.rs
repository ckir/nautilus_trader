//! Raw Finnhub WebSocket debug — prints every frame received for 20s.
//!
//! cargo run -p finstream-core --example finnhub_debug --features finnhub

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    let token = std::env::var("FINNHUB_API_TOKEN").expect("FINNHUB_API_TOKEN not set");

    let url = format!("wss://ws.finnhub.io/?token={token}");
    println!("Connecting to {}", url.replace(&token, "***"));

    let (ws, _) = connect_async(&url).await.expect("connect failed");
    let (mut write, mut read) = ws.split();

    // Subscribe
    for sym in &["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"] {
        let msg = serde_json::json!({"type":"subscribe","symbol":sym}).to_string();
        println!("→ {msg}");
        write.send(Message::Text(msg.into())).await.unwrap();
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { break; }

        match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => println!("← TEXT  {text}"),
            Ok(Some(Ok(Message::Binary(b))))  => println!("← BIN   {} bytes", b.len()),
            Ok(Some(Ok(Message::Ping(d))))    => {
                println!("← PING  {} bytes — sending pong", d.len());
                let _ = write.send(Message::Pong(d)).await;
            }
            Ok(Some(Ok(Message::Pong(_))))   => println!("← PONG"),
            Ok(Some(Ok(Message::Close(f))))  => { println!("← CLOSE {f:?}"); break; }
            Ok(Some(Ok(Message::Frame(_))))  => println!("← FRAME"),
            Ok(Some(Err(e))) => { println!("← ERR   {e}"); break; }
            Ok(None)         => { println!("stream ended"); break; }
            Err(_)           => break, // timeout
        }
    }
    println!("Done.");
}
