use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;
use nautilus_common::clients::ExecutionClient;
use nautilus_model::identifiers::{AccountId, ClientId, Venue};
use nautilus_model::enums::OmsType;
use crate::config::AlpacaExecutionClientConfig;
use crate::mapper::AlpacaIdMapper;

/// Execution client for the Alpaca broker.
pub struct AlpacaExecutionClient {
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    config: AlpacaExecutionClientConfig,
    mapper: Arc<Mutex<AlpacaIdMapper>>,
    is_connected: bool,
}

impl AlpacaExecutionClient {
    /// Creates a new `AlpacaExecutionClient`.
    pub fn new(
        client_id: ClientId,
        account_id: AccountId,
        config: AlpacaExecutionClientConfig,
    ) -> Self {
        Self {
            client_id,
            account_id,
            venue: Venue::new("ALPACA"),
            config,
            mapper: Arc::new(Mutex::new(AlpacaIdMapper::new())),
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
}
