//! NAPI bindings for finstream-core.
//!
//! This crate exposes a `FinStream` JavaScript class that wraps the Rust
//! channel-based API. Events are forwarded to JS via a threadsafe callback.
//!
//! Status: skeleton — compile-verified, runtime implementation pending.

#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

use nautilus_finstream::{
    providers::yahoo::YahooDriver,
    reconnect::ReconnectPolicy,
    FinStreamBuilder,
};

/// A streaming client that forwards normalized market events to JavaScript.
#[napi]
pub struct FinStream {
    // Future: hold FinStreamClient for shutdown
}

#[napi]
impl FinStream {
    /// Create and start a Yahoo Finance stream, invoking `on_event` for each event.
    ///
    /// ```js
    /// const { FinStream } = require('./finstream-napi');
    /// const stream = await FinStream.yahoo(['AAPL', 'TSLA'], (err, event) => {
    ///   if (!err) console.log(event);
    /// });
    /// ```
    #[napi(factory)]
    pub async fn yahoo(
        symbols: Vec<String>,
        on_event: ThreadsafeFunction<String>,
    ) -> Result<Self> {
        let (_client, mut rx) = FinStreamBuilder::new()
            .default_policy(ReconnectPolicy::default())
            .provider(YahooDriver { 
                name: "yahoo_napi".into(),
                silence_secs: 60, 
                ping_interval_secs: 30 
            })
            .symbols(symbols)
            .connect()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Spawn a task that forwards events to JS
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Ok(json) = serde_json::to_string(&event) {
                    on_event.call(Ok(json), ThreadsafeFunctionCallMode::NonBlocking);
                }
            }
        });

        Ok(Self {})
    }
}

#[napi]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
