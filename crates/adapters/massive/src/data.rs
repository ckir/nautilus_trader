// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::{json, Value};
use chrono::Utc;

use nautilus_common::clients::DataClient;
use nautilus_common::messages::DataEvent;
use nautilus_common::messages::data::{
    SubscribeQuotes, SubscribeTrades, UnsubscribeQuotes, UnsubscribeTrades,
};
use nautilus_model::identifiers::{ClientId, Venue, InstrumentId, Symbol};
use nautilus_model::data::{Data, QuoteTick, TradeTick};
use nautilus_model::types::{Price, Quantity};
use nautilus_core::UnixNanos;
use nautilus_common::live::runner::get_data_event_sender;

use crate::config::MassiveDataClientConfig;

const MASSIVE_PRICE_PRECISION: u8 = 4;
const MASSIVE_SIZE_PRECISION: u8 = 0;

enum MassiveCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

/// Data client for Massive.
pub struct MassiveDataClient {
    client_id: ClientId,
    config: MassiveDataClientConfig,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    cmd_sender: Option<mpsc::UnboundedSender<MassiveCommand>>,
    is_connected: AtomicBool,
    venue: Venue,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl MassiveDataClient {
    /// Creates a new `MassiveDataClient`.
    pub fn new(client_id: ClientId, config: MassiveDataClientConfig) -> Self {
        let data_sender = get_data_event_sender();
        Self {
            client_id,
            config,
            data_sender,
            cmd_sender: None,
            is_connected: AtomicBool::new(false),
            venue: Venue::new("MASSIVE"),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
        }
    }
}

#[async_trait(?Send)]
impl DataClient for MassiveDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Massive data client...");
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Massive data client...");
        self.cancellation_token.cancel();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::SeqCst)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        let url = "wss://socket.massive.com/stocks";
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // 1. Initial status message
        if let Some(Ok(Message::Text(text))) = read.next().await {
            let msgs: Value = serde_json::from_str(&text)?;
            if msgs[0]["ev"] != "status" || msgs[0]["status"] != "connected" {
                return Err(anyhow::anyhow!("Unexpected Massive connection message: {}", text));
            }
        }

        // 2. Authentication
        let auth = json!({
            "action": "auth",
            "params": self.config.api_key,
        });
        write.send(Message::Text(auth.to_string().into())).await?;

        if let Some(Ok(Message::Text(text))) = read.next().await {
            let msgs: Value = serde_json::from_str(&text)?;
            if msgs[0]["status"] != "auth_success" {
                return Err(anyhow::anyhow!("Massive authentication failed: {}", text));
            }
        }

        self.is_connected.store(true, Ordering::SeqCst);
        log::info!("Massive data client connected and authenticated.");

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<MassiveCommand>();
        self.cmd_sender = Some(cmd_tx);

        let cancellation_token = self.cancellation_token.clone();
        let data_sender = self.data_sender.clone();
        let venue = self.venue;

        let read_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Ok(msgs) = serde_json::from_str::<Value>(&text) {
                                    if let Some(arr) = msgs.as_array() {
                                        for m in arr {
                                            handle_message(m, &data_sender, venue);
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("Massive WS read error: {}", e);
                                break;
                            }
                            None => {
                                log::warn!("Massive WS stream ended.");
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        let cancellation_token_write = self.cancellation_token.clone();
        let write_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token_write.cancelled() => {
                        break;
                    }
                    cmd = cmd_rx.recv() => {
                        if let Some(command) = cmd {
                            let msg = match command {
                                MassiveCommand::Subscribe(symbols) => json!({
                                    "action": "subscribe",
                                    "params": symbols.join(","),
                                }),
                                MassiveCommand::Unsubscribe(symbols) => json!({
                                    "action": "unsubscribe",
                                    "params": symbols.join(","),
                                }),
                            };
                            if let Err(e) = write.send(Message::Text(msg.to_string().into())).await {
                                log::error!("Failed to send Massive subscription: {}", e);
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        });

        self.tasks.push(read_task);
        self.tasks.push(write_task);

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.is_connected.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let symbol = format!("Q.{}", cmd.instrument_id.symbol);
        if let Some(tx) = &self.cmd_sender {
            tx.send(MassiveCommand::Subscribe(vec![symbol]))?;
        }
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let symbol = format!("T.{}", cmd.instrument_id.symbol);
        if let Some(tx) = &self.cmd_sender {
            tx.send(MassiveCommand::Subscribe(vec![symbol]))?;
        }
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let symbol = format!("Q.{}", cmd.instrument_id.symbol);
        if let Some(tx) = &self.cmd_sender {
            tx.send(MassiveCommand::Unsubscribe(vec![symbol]))?;
        }
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let symbol = format!("T.{}", cmd.instrument_id.symbol);
        if let Some(tx) = &self.cmd_sender {
            tx.send(MassiveCommand::Unsubscribe(vec![symbol]))?;
        }
        Ok(())
    }
}

fn handle_message(msg: &Value, tx: &mpsc::UnboundedSender<DataEvent>, venue: Venue) {
    match msg["ev"].as_str() {
        Some("Q") => {
            if let Some(quote) = parse_quote(msg, venue) {
                let _ = tx.send(DataEvent::Data(Data::Quote(quote)));
            }
        }
        Some("T") => {
            if let Some(trade) = parse_trade(msg, venue) {
                let _ = tx.send(DataEvent::Data(Data::Trade(trade)));
            }
        }
        _ => {}
    }
}

fn parse_quote(msg: &Value, venue: Venue) -> Option<QuoteTick> {
    let symbol = msg["sym"].as_str()?;
    let bid_price = msg["bp"].as_f64()?;
    let ask_price = msg["ap"].as_f64()?;
    // Quote sizes are in round lots (100 shares)
    let bid_size = msg["bs"].as_f64().unwrap_or(0.0) * 100.0;
    let ask_size = msg["as"].as_f64().unwrap_or(0.0) * 100.0;
    let ts_ms = msg["t"].as_u64()?;
    
    let instrument_id = InstrumentId::new(Symbol::from(symbol), venue);

    Some(QuoteTick {
        instrument_id,
        bid_price: Price::new(bid_price, MASSIVE_PRICE_PRECISION),
        ask_price: Price::new(ask_price, MASSIVE_PRICE_PRECISION),
        bid_size: Quantity::new(bid_size, MASSIVE_SIZE_PRECISION),
        ask_size: Quantity::new(ask_size, MASSIVE_SIZE_PRECISION),
        ts_event: UnixNanos::new(ts_ms * 1_000_000),
        ts_init: UnixNanos::new(Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64),
    })
}

fn parse_trade(msg: &Value, venue: Venue) -> Option<TradeTick> {
    let symbol = msg["sym"].as_str()?;
    let price = msg["p"].as_f64()?;
    let size = msg["s"].as_f64()?;
    let ts_ms = msg["t"].as_u64()?;
    
    let instrument_id = InstrumentId::new(Symbol::from(symbol), venue);

    Some(TradeTick {
        instrument_id,
        price: Price::new(price, MASSIVE_PRICE_PRECISION),
        size: Quantity::new(size, MASSIVE_SIZE_PRECISION),
        ts_event: UnixNanos::new(ts_ms * 1_000_000),
        ts_init: UnixNanos::new(Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_quote() {
        let venue = Venue::new("MASSIVE");
        let msg = json!({
            "ev": "Q",
            "sym": "AAPL",
            "bp": 150.0,
            "bs": 1.0,
            "ap": 150.1,
            "as": 2.0,
            "t": 1717502401000u64
        });

        let quote = parse_quote(&msg, venue).unwrap();
        assert_eq!(quote.instrument_id.symbol.as_str(), "AAPL");
        assert_eq!(quote.bid_price.as_f64(), 150.0);
        assert_eq!(quote.ask_price.as_f64(), 150.1);
        assert_eq!(quote.bid_size.as_f64(), 100.0);
        assert_eq!(quote.ask_size.as_f64(), 200.0);
    }

    #[test]
    fn test_parse_trade() {
        let venue = Venue::new("MASSIVE");
        let msg = json!({
            "ev": "T",
            "sym": "AAPL",
            "p": 150.05,
            "s": 50.0,
            "t": 1717502401000u64
        });

        let trade = parse_trade(&msg, venue).unwrap();
        assert_eq!(trade.instrument_id.symbol.as_str(), "AAPL");
        assert_eq!(trade.price.as_f64(), 150.05);
        assert_eq!(trade.size.as_f64(), 50.0);
    }
}
