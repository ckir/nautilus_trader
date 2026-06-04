use crate::types::ProviderKind;

/// Errors that can occur during the setup or operation of the financial data stream.
#[derive(thiserror::Error, Debug)]
pub enum FinStreamError {
    /// A network or protocol error from the underlying WebSocket.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// An error occurred during JSON serialization or deserialization.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Failed to decode a Protobuf message (used by Yahoo).
    #[error("Protobuf decode error: {0}")]
    Protobuf(#[from] prost::DecodeError),

    /// Authentication with a provider was explicitly rejected.
    #[error("Auth failed for {provider}: {message}")]
    AuthFailed { 
        /// The provider that rejected authentication.
        provider: ProviderKind, 
        /// The error message returned by the provider.
        message: String 
    },

    /// The reconnection policy limit was reached for a provider.
    #[error("Max retries exceeded for {provider}")]
    MaxRetriesExceeded { 
        /// The provider that reached its retry limit.
        provider: ProviderKind 
    },

    /// The internal event channel was closed (e.g., during shutdown).
    #[error("Channel send error: receiver dropped")]
    ChannelClosed,

    /// An invalid configuration was provided (e.g., missing API keys).
    #[error("Config error: {0}")]
    Config(String),

    /// Failed to parse a WebSocket or API URL.
    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),
}
