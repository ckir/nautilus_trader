//! # finstream-core
//!
//! A high-performance library for unified access to multiple financial market data providers.
//!
//! This crate provides normalized data structures ([`MarketEvent`], [`Trade`], [`Quote`])
//! and a fluent [`FinStreamBuilder`] to connect to multiple WebSocket-based data feeds
//! simultaneously.

pub mod builder;
pub mod client;
pub mod config;
pub mod error;
pub mod providers;
pub mod reconnect;
pub mod types;

pub use builder::FinStreamBuilder;
pub use client::FinStreamClient;
pub use error::FinStreamError;
pub use types::{
    AlpacaQuoteExtras, FinnhubTradeExtras, MarketEvent, MassiveQuoteExtras, MassiveTradeExtras,
    ProviderKind, ProviderStatus, Quote, QuoteExtras, Trade, TradeExtras, YahooQuoteExtras,
    YahooTradeExtras,
};
