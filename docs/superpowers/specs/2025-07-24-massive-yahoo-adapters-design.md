# Design: Massive and Yahoo Adapters

## Goal
Implement two new data adapters for the Nautilus Trader platform: **Massive** and **Yahoo**.

## Massive Adapter (`crates/adapters/massive`)

### Configuration
- `api_key`: String (required for authentication)

### Connection & Protocol
- **WS URL**: `wss://socket.massive.com/stocks`
- **Authentication**: `{"action": "auth", "params": "<API_KEY>"}`
- **Subscription**: `{"action": "subscribe", "params": "T.SYMBOL,Q.SYMBOL"}`
- **Heartbeat**: Standard WebSocket ping/pong if supported.

### Data Mapping
- **Trades (`ev: "T"`)**:
  - `p`: Price
  - `s`: Size (in shares)
  - `sym`: Symbol
  - `t`: Timestamp (milliseconds)
- **Quotes (`ev: "Q"`)**:
  - `bp`: Bid Price
  - `ap`: Ask Price
  - `bs`: Bid Size (round lots, multiply by 100)
  - `as`: Ask Size (round lots, multiply by 100)
  - `sym`: Symbol
  - `t`: Timestamp (milliseconds)

## Yahoo Adapter (`crates/adapters/yahoo`)

### Configuration
- `silence_secs`: u32 (Reconnect if no data received for this duration)
- `ping_interval_secs`: u32 (Interval for sending WS pings)

### Connection & Protocol
- **WS URL**: `wss://streamer.finance.yahoo.com/?version=2`
- **Subscription**: `{"subscribe": ["SYMBOL"]}`
- **Message Format**: JSON wrapper with base64-encoded Protobuf payload.
  - `{"type": "pricing", "message": "<BASE64_PROTOBUF>"}`

### Data Mapping
- Port logic from `crates/finstream/core/src/providers/yahoo/mod.rs` and `proto_handler.rs`.
- Decodes `PricingData` Protobuf message.
- Emits `TradeTick` if price > 0.
- Emits `QuoteTick` if bid > 0 or ask > 0.

## Implementation Details
- Both adapters will implement `DataClient` trait from `nautilus_common::clients`.
- Each will have `src/config.rs`, `src/data.rs`, and `src/lib.rs`.
- `Cargo.toml` will be based on `finnhub` adapter.
- Use Nautech Systems license header.

## Validation Plan
- Verify compilation with `cargo check -p nautilus-massive` and `cargo check -p nautilus-yahoo`.
- Unit tests in `data.rs` to verify parsing of sample messages.
