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

//! Factory functions for creating Massive clients and components.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    config::MassiveDataClientConfig,
    data::MassiveDataClient,
};

/// Factory for creating Massive data clients.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.massive", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.massive")
)]
#[derive(Debug, Clone)]
pub struct MassiveDataClientFactory;

impl MassiveDataClientFactory {
    /// Creates a new [`MassiveDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl MassiveDataClientFactory {
    #[new]
    #[must_use]
    fn py_new() -> Self {
        Self::new()
    }
}

impl Default for MassiveDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for MassiveDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let massive_config = config
            .as_any()
            .downcast_ref::<MassiveDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for MassiveDataClientFactory. Expected MassiveDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = MassiveDataClient::new(client_id, massive_config);
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "MASSIVE"
    }

    fn config_type(&self) -> &'static str {
        stringify!(MassiveDataClientConfig)
    }
}
