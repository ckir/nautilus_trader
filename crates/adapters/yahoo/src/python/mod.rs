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

//! Python bindings for the Yahoo adapter.

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    config::YahooDataClientConfig,
    factories::YahooDataClientFactory,
};

#[expect(clippy::needless_pass_by_value)]
fn extract_yahoo_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<YahooDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract YahooDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_yahoo_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<YahooDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract YahooDataClientConfig: {e}"
        ))),
    }
}

/// Yahoo adapter Python module.
///
/// Loaded as `nautilus_pyo3.yahoo`.
///
/// # Errors
///
/// Returns an error if module initialization fails.
#[pymodule]
pub fn yahoo(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<YahooDataClientConfig>()?;
    m.add_class::<YahooDataClientFactory>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor("YAHOO".to_string(), extract_yahoo_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Yahoo data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "YahooDataClientConfig".to_string(),
        extract_yahoo_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Yahoo data config extractor: {e}"
        )));
    }

    Ok(())
}
