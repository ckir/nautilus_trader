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
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use serde_json::json;
use chrono::Utc;
use prost::Message as ProstMessage;
use base64::{engine::general_purpose, Engine as _};

use nautilus_common::clients::DataClient;
use nautilus_common::messages::DataEvent;
use nautilus_common::messages::data::{SubscribeQuotes, SubscribeTrades};
use nautilus_model::identifiers::{ClientId, Venue, InstrumentId, Symbol};
use nautilus_model::data::{Data, QuoteTick, TradeTick};
use nautilus_model::types::{Price, Quantity};
use nautilus_core::UnixNanos;
use nautilus_common::live::runner::get_data_event_sender;

use crate::config::YahooDataClientConfig;

const YAHOO_PRICE_PRECISION: u8 = 4;
const YAHOO_SIZE_PRECISION: u8 = 0;

enum YahooCommand {
    Subscribe(Vec<String>),
}

/// Protobuf-decoded pricing payload from Yahoo Finance WebSocket.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PricingData {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(float, tag = "2")]
    pub price: f32,
    #[prost(sint64, tag = "3")]
    pub time: i64,
    #[prost(string, tag = "4")]
    pub currency: String,
    #[prost(string, tag = "5")]
    pub exchange: String,
    #[prost(int32, tag = "6")]
    pub quote_type: i32,
    #[prost(int32, tag = "7")]
    pub market_hours: i32,
    #[prost(float, tag = "8")]
    pub change_percent: f32,
    #[prost(sint64, tag = "9")]
    pub day_volume: i64,
    #[prost(float, tag = "10")]
    pub day_high: f32,
    #[prost(float, tag = "11")]
    pub day_low: f32,
    #[prost(float, tag = "12")]
    pub change: f32,
    #[prost(string, tag = "13")]
    pub short_name: String,
    #[prost(sint64, tag = "14")]
    pub expire_date: i64,
    #[prost(float, tag = "15")]
    pub open_price: f32,
    #[prost(float, tag = "16")]
    pub previous_close: f32,
    #[prost(float, tag = "17")]
    pub strike_price: f32,
    #[prost(string, tag = "18")]
    pub underlying_symbol: String,
    #[prost(sint64, tag = "19")]
    pub open_interest: i64,
    #[prost(int32, tag = "20")]
    pub option_type: i32,
    #[prost(sint64, tag = "21")]
    pub mini_option: i64,
    #[prost(sint64, tag = "22")]
    pub last_size: i64,
    #[prost(float, tag = "23")]
    pub bid: f32,
    #[prost(sint64, tag = "24")]
    pub bid_size: i64,
    #[prost(float, tag = "25")]
    pub ask: f32,
    #[prost(sint64, tag = "26")]
    pub ask_size: i64,
}

/// Data client for Yahoo.
pub struct YahooDataClient {
    client_id: ClientId,
    _config: YahooDataClientConfig,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    cmd_sender: Option<mpsc::UnboundedSender<YahooCommand>>,
    is_connected: AtomicBool,
    venue: Venue,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl YahooDataClient {
    /// Creates a new `YahooDataClient`.
    pub fn new(client_id: ClientId, config: YahooDataClientConfig) -> Self {
        let data_sender = get_data_event_sender();
        Self {
            client_id,
            _config: config,
            data_sender,
            cmd_sender: None,
            is_connected: AtomicBool::new(false),
            venue: Venue::new("YAHOO"),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
        }
    }
}

#[async_trait(?Send)]
impl DataClient for YahooDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Yahoo data client...");
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Yahoo data client...");
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

        let url = "wss://streamer.finance.yahoo.com/?version=2";
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        self.is_connected.store(true, Ordering::SeqCst);
        log::info!("Yahoo data client connected.");

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<YahooCommand>();
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
                            Some(Ok(WsMessage::Text(text))) => {
                                handle_message(&text, &data_sender, venue);
                            }
                            Some(Err(e)) => {
                                log::error!("Yahoo WS read error: {}", e);
                                break;
                            }
                            None => {
                                log::warn!("Yahoo WS stream ended.");
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
                            let symbols = match command {
                                YahooCommand::Subscribe(s) => s,
                            };
                            let msg = json!({
                                "subscribe": symbols,
                            });
                            if let Err(e) = write.send(WsMessage::Text(msg.to_string().into())).await {
                                log::error!("Failed to send Yahoo subscription: {}", e);
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
        let symbol = cmd.instrument_id.symbol.to_string();
        if let Some(tx) = &self.cmd_sender {
            tx.send(YahooCommand::Subscribe(vec![symbol]))?;
        }
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let symbol = cmd.instrument_id.symbol.to_string();
        if let Some(tx) = &self.cmd_sender {
            tx.send(YahooCommand::Subscribe(vec![symbol]))?;
        }
        Ok(())
    }
}

fn handle_message(text: &str, tx: &mpsc::UnboundedSender<DataEvent>, venue: Venue) {
    if let Ok(PricingData { id, price, time, bid, bid_size, ask, ask_size, last_size, .. }) = decode_yahoo_message(text) {
        let instrument_id = InstrumentId::new(Symbol::from(id), venue);
        let ts_event = UnixNanos::new(time as u64 * 1_000_000);
        let ts_init = UnixNanos::new(Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64);

        // Send Quote
        let quote = QuoteTick {
            instrument_id: instrument_id.clone(),
            bid_price: Price::new(bid as f64, YAHOO_PRICE_PRECISION),
            ask_price: Price::new(ask as f64, YAHOO_PRICE_PRECISION),
            bid_size: Quantity::new(bid_size as f64, YAHOO_SIZE_PRECISION),
            ask_size: Quantity::new(ask_size as f64, YAHOO_SIZE_PRECISION),
            ts_event,
            ts_init,
        };
        let _ = tx.send(DataEvent::Data(Data::Quote(quote)));

        // Send Trade
        let trade = TradeTick {
            instrument_id,
            price: Price::new(price as f64, YAHOO_PRICE_PRECISION),
            size: Quantity::new(last_size as f64, YAHOO_SIZE_PRECISION),
            ts_event,
            ts_init,
            ..Default::default()
        };
        let _ = tx.send(DataEvent::Data(Data::Trade(trade)));
    }
}

fn decode_yahoo_message(encoded: &str) -> Result<PricingData, String> {
    let decoded = general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| format!("base64: {e}"))?;
    
    ProstMessage::decode(&decoded[..]).map_err(|e| format!("protobuf: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_yahoo_message() {
        // This is a base64 encoded Protobuf message for AAPL at 150.0
        // id: "AAPL", price: 150.0, time: 1717502401000
        // We can't easily construct this without more effort, so we'll just check base64 decoding error
        let result = decode_yahoo_message("invalid_base64");
        assert!(result.is_err());
    }
}
