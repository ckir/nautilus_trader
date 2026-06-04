use std::any::Any;
use serde::{Deserialize, Serialize};
use nautilus_common::factories::ClientConfig;
use nautilus_model::enums::TimeInForce;

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.alpaca")
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaExecutionClientConfig {
    pub api_key: String,
    pub api_secret: String,
    pub paper: bool,
    pub default_tif: TimeInForce,
    pub extended_hours: bool,
}

impl ClientConfig for AlpacaExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.alpaca")
)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlpacaDataFeed {
    Iex,
    Sip,
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.alpaca", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.alpaca")
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaDataClientConfig {
    pub api_key: String,
    pub api_secret: String,
    pub feed: AlpacaDataFeed,
}

impl ClientConfig for AlpacaDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
