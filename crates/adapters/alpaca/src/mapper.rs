use std::collections::HashMap;
use nautilus_model::identifiers::ClientOrderId;

pub struct AlpacaIdMapper {
    client_to_alpaca: HashMap<ClientOrderId, String>,
    alpaca_to_client: HashMap<String, ClientOrderId>,
}

impl AlpacaIdMapper {
    pub fn new() -> Self {
        Self {
            client_to_alpaca: HashMap::new(),
            alpaca_to_client: HashMap::new(),
        }
    }
    pub fn insert(&mut self, client_id: ClientOrderId, alpaca_id: String) {
        self.client_to_alpaca.insert(client_id, alpaca_id.clone());
        self.alpaca_to_client.insert(alpaca_id, client_id);
    }
    pub fn get_alpaca_id(&self, client_id: &ClientOrderId) -> Option<&String> {
        self.client_to_alpaca.get(client_id)
    }
    pub fn get_client_id(&self, alpaca_id: &str) -> Option<&ClientOrderId> {
        self.alpaca_to_client.get(alpaca_id)
    }
}
