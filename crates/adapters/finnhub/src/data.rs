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
    SubscribeTrades, UnsubscribeTrades,
};
use nautilus_model::identifiers::{ClientId, Venue, InstrumentId, Symbol};
use nautilus_model::data::{Data, TradeTick};
use nautilus_model::types::{Price, Quantity};
use nautilus_core::UnixNanos;
use nautilus_common::live::runner::get_data_event_sender;

use crate::config::FinnhubDataClientConfig;

const FINNHUB_PRICE_PRECISION: u8 = 4;
const FINNHUB_SIZE_PRECISION: u8 = 8;

enum FinnhubCommand {
    Subscribe(String),
    Unsubscribe(String),
}

/// Data client for Finnhub.
pub struct FinnhubDataClient {
    client_id: ClientId,
    config: FinnhubDataClientConfig,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    cmd_sender: Option<mpsc::UnboundedSender<FinnhubCommand>>,
    is_connected: AtomicBool,
    venue: Venue,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl FinnhubDataClient {
    /// Creates a new `FinnhubDataClient`.
    pub fn new(client_id: ClientId, config: FinnhubDataClientConfig) -> Self {
        let data_sender = get_data_event_sender();
        Self {
            client_id,
            config,
            data_sender,
            cmd_sender: None,
            is_connected: AtomicBool::new(false),
            venue: Venue::new("FINNHUB"),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
        }
    }
}

#[async_trait(?Send)]
impl DataClient for FinnhubDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Finnhub data client...");
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Finnhub data client...");
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

        let url = format!("wss://ws.finnhub.io/?token={}", self.config.api_token);
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        self.is_connected.store(true, Ordering::SeqCst);
        log::info!("Finnhub data client connected.");

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<FinnhubCommand>();
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
                                    handle_message(&msgs, &data_sender, venue);
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("Finnhub WS read error: {}", e);
                                break;
                            }
                            None => {
                                log::warn!("Finnhub WS stream ended.");
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
                                FinnhubCommand::Subscribe(symbol) => json!({
                                    "type": "subscribe",
                                    "symbol": symbol,
                                }),
                                FinnhubCommand::Unsubscribe(symbol) => json!({
                                    "type": "unsubscribe",
                                    "symbol": symbol,
                                }),
                            };
                            if let Err(e) = write.send(Message::Text(msg.to_string().into())).await {
                                log::error!("Failed to send Finnhub subscription: {}", e);
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

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let symbol = cmd.instrument_id.symbol.to_string();
        if let Some(tx) = &self.cmd_sender {
            tx.send(FinnhubCommand::Subscribe(symbol))?;
        }
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let symbol = cmd.instrument_id.symbol.to_string();
        if let Some(tx) = &self.cmd_sender {
            tx.send(FinnhubCommand::Unsubscribe(symbol))?;
        }
        Ok(())
    }
}

fn handle_message(msg: &Value, tx: &mpsc::UnboundedSender<DataEvent>, venue: Venue) {
    match msg["type"].as_str() {
        Some("trade") => {
            if let Some(data) = msg["data"].as_array() {
                for t in data {
                    if let Some(trade) = parse_trade(t, venue) {
                        let _ = tx.send(DataEvent::Data(Data::Trade(trade)));
                    }
                }
            }
        }
        _ => {}
    }
}

fn parse_trade(msg: &Value, venue: Venue) -> Option<TradeTick> {
    let symbol = msg["s"].as_str()?;
    let price = msg["p"].as_f64()?;
    let size = msg["v"].as_f64()?;
    let ts_ms = msg["t"].as_u64()?;
    
    let instrument_id = InstrumentId::new(Symbol::from(symbol), venue);

    Some(TradeTick {
        instrument_id,
        price: Price::new(price, FINNHUB_PRICE_PRECISION),
        size: Quantity::new(size, FINNHUB_SIZE_PRECISION),
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
    fn test_parse_trade() {
        let venue = Venue::new("FINNHUB");
        let msg = json!({
            "s": "AAPL",
            "p": 150.05,
            "v": 50.0,
            "t": 1717502401000u64
        });

        let trade = parse_trade(&msg, venue).unwrap();
        assert_eq!(trade.instrument_id.symbol.as_str(), "AAPL");
        assert_eq!(trade.price.as_f64(), 150.05);
        assert_eq!(trade.size.as_f64(), 50.0);
        assert_eq!(trade.ts_event.as_u64(), 1717502401000000000u64);
    }
}
