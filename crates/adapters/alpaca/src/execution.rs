use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;
use nautilus_common::clients::ExecutionClient;
use nautilus_common::messages::execution::SubmitOrder;
use nautilus_model::identifiers::{AccountId, ClientId, Venue};
use nautilus_model::enums::OmsType;
use crate::config::AlpacaExecutionClientConfig;
use crate::mapper::AlpacaIdMapper;
use crate::http::{AlpacaHttpClient, AlpacaOrderPayload};

/// Execution client for the Alpaca broker.
pub struct AlpacaExecutionClient {
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    config: AlpacaExecutionClientConfig,
    mapper: Arc<Mutex<AlpacaIdMapper>>,
    http_client: Arc<AlpacaHttpClient>,
    is_connected: bool,
}

impl AlpacaExecutionClient {
    /// Creates a new `AlpacaExecutionClient`.
    pub fn new(
        client_id: ClientId,
        account_id: AccountId,
        config: AlpacaExecutionClientConfig,
    ) -> Self {
        let http_client = Arc::new(AlpacaHttpClient::new(&config));
        Self {
            client_id,
            account_id,
            venue: Venue::new("ALPACA"),
            config,
            mapper: Arc::new(Mutex::new(AlpacaIdMapper::new())),
            http_client,
            is_connected: false,
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for AlpacaExecutionClient {
    fn is_connected(&self) -> bool {
        self.is_connected
    }

    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn venue(&self) -> Venue {
        self.venue
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<nautilus_model::accounts::AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        _balances: Vec<nautilus_model::types::AccountBalance>,
        _margins: Vec<nautilus_model::types::MarginBalance>,
        _reported: bool,
        _ts_event: nautilus_core::UnixNanos,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.is_connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.is_connected = false;
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let mapper = self.mapper.clone();
        let config = self.config.clone();

        // Basic mapping logic
        let symbol = cmd.instrument_id.symbol.to_string();
        let side = cmd.order_init.order_side.to_string().to_lowercase();
        let order_type = cmd.order_init.order_type.to_string().to_lowercase();
        
        // Time in Force
        let tif = cmd.order_init.time_in_force.to_string().to_lowercase();

        // Fractional vs Notional
        let mut qty = None;
        let mut notional = None;

        if cmd.order_init.quote_quantity {
            notional = Some(cmd.order_init.quantity.to_string());
        } else {
            qty = Some(cmd.order_init.quantity.to_string());
        }

        let payload = AlpacaOrderPayload {
            symbol,
            qty,
            notional,
            side,
            r#type: order_type,
            time_in_force: tif,
            limit_price: cmd.order_init.price.map(|p| p.to_string()),
            stop_price: cmd.order_init.trigger_price.map(|p| p.to_string()),
            client_order_id: cmd.order_init.client_order_id.to_string(),
            extended_hours: config.extended_hours,
        };

        tokio::spawn(async move {
            match http_client.submit_order(&payload).await {
                Ok(resp) => {
                    let mut m = mapper.lock().await;
                    m.insert(cmd.order_init.client_order_id, resp.id);
                }
                Err(e) => {
                    log::error!("Failed to submit order {}: {}", cmd.order_init.client_order_id, e);
                }
            }
        });

        Ok(())
    }
}
