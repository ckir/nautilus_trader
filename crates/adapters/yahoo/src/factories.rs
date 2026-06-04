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

//! Factory functions for creating Yahoo clients and components.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    config::YahooDataClientConfig,
    data::YahooDataClient,
};

/// Factory for creating Yahoo data clients.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.yahoo", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.yahoo")
)]
#[derive(Debug, Clone)]
pub struct YahooDataClientFactory;

impl YahooDataClientFactory {
    /// Creates a new [`YahooDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl YahooDataClientFactory {
    #[new]
    #[must_use]
    fn py_new() -> Self {
        Self::new()
    }
}

impl Default for YahooDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for YahooDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let yahoo_config = config
            .as_any()
            .downcast_ref::<YahooDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for YahooDataClientFactory. Expected YahooDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = YahooDataClient::new(client_id, yahoo_config);
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "YAHOO"
    }

    fn config_type(&self) -> &'static str {
        stringify!(YahooDataClientConfig)
    }
}
