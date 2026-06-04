use reqwest::{Client, header::{HeaderMap, HeaderValue}};
use serde::{Deserialize, Serialize};
use crate::config::AlpacaExecutionClientConfig;

pub struct AlpacaHttpClient {
    client: Client,
    base_url: String,
}

impl AlpacaHttpClient {
    pub fn new(config: &AlpacaExecutionClientConfig) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert("APCA-API-KEY-ID", HeaderValue::from_str(&config.api_key).unwrap());
        headers.insert("APCA-API-SECRET-KEY", HeaderValue::from_str(&config.api_secret).unwrap());

        let client = Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        let base_url = if config.paper {
            "https://paper-api.alpaca.markets".to_string()
        } else {
            "https://api.alpaca.markets".to_string()
        };

        Self { client, base_url }
    }

    pub async fn submit_order(&self, payload: &AlpacaOrderPayload) -> anyhow::Result<AlpacaOrderResponse> {
        let url = format!("{}/v2/orders", self.base_url);
        let resp = self.client.post(&url)
            .json(payload)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(resp.json().await?)
        } else {
            let err_text = resp.text().await?;
            Err(anyhow::anyhow!("Alpaca API error: {}", err_text))
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AlpacaOrderPayload {
    pub symbol: String,
    pub qty: Option<String>,
    pub notional: Option<String>,
    pub side: String,
    pub r#type: String,
    pub time_in_force: String,
    pub limit_price: Option<String>,
    pub stop_price: Option<String>,
    pub client_order_id: String,
    pub extended_hours: bool,
    // advanced types omitted for now
}

#[derive(Debug, Deserialize)]
pub struct AlpacaOrderResponse {
    pub id: String,
    pub client_order_id: String,
    pub status: String,
    // other fields omitted
}
