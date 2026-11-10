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

//! Python bindings for OKX configuration.

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use pyo3::prelude::*;

use crate::{
    common::enums::{
        OKXEnvironment, OKXInstrumentType, OKXMarginMode, OKXPositionMode, OKXReconnectPolicy,
        OKXRegion, OKXVipLevel,
    },
    config::{OKXDataClientConfig, OKXExecClientConfig},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OKXDataClientConfig {
    /// Configuration for the OKX data client.
    #[new]
    #[pyo3(signature = (
        instrument_types = None,
        environment = None,
        region = None,
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        base_url_http = None,
        base_url_ws_public = None,
        base_url_ws_business = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        update_instruments_interval_mins = None,
        book_stale_check_interval_secs = None,
        book_stale_threshold_secs = None,
        book_snapshot_timeout_secs = None,
        vip_level = None,
        load_spreads = false,
        transport_backend = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        instrument_types: Option<Vec<OKXInstrumentType>>,
        environment: Option<OKXEnvironment>,
        region: Option<OKXRegion>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url_http: Option<String>,
        base_url_ws_public: Option<String>,
        base_url_ws_business: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
        book_stale_check_interval_secs: Option<u64>,
        book_stale_threshold_secs: Option<u64>,
        book_snapshot_timeout_secs: Option<u64>,
        vip_level: Option<OKXVipLevel>,
        load_spreads: bool,
        transport_backend: Option<TransportBackend>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            api_key,
            api_secret,
            api_passphrase,
            instrument_types: instrument_types.unwrap_or(defaults.instrument_types),
            contract_types: None,
            load_spreads,
            instrument_families: None,
            base_url_http,
            base_url_ws_public,
            base_url_ws_business,
            proxy_url,
            environment: environment.unwrap_or(defaults.environment),
            region: region.unwrap_or(defaults.region),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            retry_delay_initial_ms: retry_delay_initial_ms
                .unwrap_or(defaults.retry_delay_initial_ms),
            retry_delay_max_ms: retry_delay_max_ms.unwrap_or(defaults.retry_delay_max_ms),
            update_instruments_interval_mins: update_instruments_interval_mins
                .unwrap_or(defaults.update_instruments_interval_mins),
            book_stale_check_interval_secs: book_stale_check_interval_secs
                .unwrap_or(defaults.book_stale_check_interval_secs),
            book_stale_threshold_secs: book_stale_threshold_secs
                .unwrap_or(defaults.book_stale_threshold_secs),
            book_snapshot_timeout_secs: book_snapshot_timeout_secs
                .unwrap_or(defaults.book_snapshot_timeout_secs),
            vip_level,
            transport_backend: transport_backend.unwrap_or(defaults.transport_backend),
        }
    }

    #[getter]
    const fn has_proxy_url(&self) -> bool {
        self.proxy_url.is_some()
    }

    fn __repr__(&self) -> String {
        stringify!(OKXDataClientConfig).to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OKXExecClientConfig {
    /// Configuration for the OKX execution client.
    #[new]
    #[pyo3(signature = (
        trader_id,
        account_id,
        instrument_types = None,
        environment = None,
        region = None,
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        base_url_http = None,
        base_url_ws_private = None,
        base_url_ws_business = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        margin_mode = None,
        allow_leveraged_products = None,
        load_spreads = false,
        expected_uid = None,
        expected_main_uid = None,
        expected_account_type = None,
        expected_account_level = None,
        expected_position_mode = None,
        require_ip_binding = None,
        cancel_all_after_secs = None,
        cancel_all_after_heartbeat_secs = None,
        reconnect_policy = None,
        reconnect_reconcile_lookback_secs = None,
        auth_timeout_secs = None,
        transport_backend = None,
        use_spot_margin = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        trader_id: TraderId,
        account_id: AccountId,
        instrument_types: Option<Vec<OKXInstrumentType>>,
        environment: Option<OKXEnvironment>,
        region: Option<OKXRegion>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url_http: Option<String>,
        base_url_ws_private: Option<String>,
        base_url_ws_business: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        margin_mode: Option<OKXMarginMode>,
        allow_leveraged_products: Option<bool>,
        load_spreads: bool,
        expected_uid: Option<String>,
        expected_main_uid: Option<String>,
        expected_account_type: Option<u8>,
        expected_account_level: Option<u8>,
        expected_position_mode: Option<OKXPositionMode>,
        require_ip_binding: Option<bool>,
        cancel_all_after_secs: Option<u64>,
        cancel_all_after_heartbeat_secs: Option<u64>,
        reconnect_policy: Option<OKXReconnectPolicy>,
        reconnect_reconcile_lookback_secs: Option<u64>,
        auth_timeout_secs: Option<u64>,
        transport_backend: Option<TransportBackend>,
        use_spot_margin: Option<bool>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            trader_id,
            account_id,
            api_key,
            api_secret,
            api_passphrase,
            instrument_types: instrument_types.unwrap_or(defaults.instrument_types),
            contract_types: None,
            instrument_families: None,
            base_url_http,
            base_url_ws_private,
            base_url_ws_business,
            proxy_url,
            environment: environment.unwrap_or(defaults.environment),
            region: region.unwrap_or(defaults.region),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            use_fills_channel: defaults.use_fills_channel,
            use_mm_mass_cancel: defaults.use_mm_mass_cancel,
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            retry_delay_initial_ms: retry_delay_initial_ms
                .unwrap_or(defaults.retry_delay_initial_ms),
            retry_delay_max_ms: retry_delay_max_ms.unwrap_or(defaults.retry_delay_max_ms),
            margin_mode,
            allow_leveraged_products: allow_leveraged_products
                .unwrap_or(defaults.allow_leveraged_products),
            load_spreads,
            use_spot_margin: use_spot_margin.unwrap_or(defaults.use_spot_margin),
            expected_uid,
            expected_main_uid,
            expected_account_type,
            expected_account_level,
            expected_position_mode: expected_position_mode
                .unwrap_or(defaults.expected_position_mode),
            require_ip_binding: require_ip_binding.unwrap_or(defaults.require_ip_binding),
            cancel_all_after_secs,
            cancel_all_after_heartbeat_secs: cancel_all_after_heartbeat_secs
                .unwrap_or(defaults.cancel_all_after_heartbeat_secs),
            reconnect_policy: reconnect_policy.unwrap_or(defaults.reconnect_policy),
            reconnect_reconcile_lookback_secs: reconnect_reconcile_lookback_secs
                .unwrap_or(defaults.reconnect_reconcile_lookback_secs),
            auth_timeout_secs,
            transport_backend: transport_backend.unwrap_or(defaults.transport_backend),
        }
    }

    #[getter]
    const fn has_proxy_url(&self) -> bool {
        self.proxy_url.is_some()
    }

    /// Validates the exchange-facing execution safety matrix, raising `ValueError` on failure.
    ///
    /// Exposed so Python execution clients share the single Rust source of truth instead of
    /// duplicating the rules.
    #[pyo3(name = "validate_execution_safety")]
    fn py_validate_execution_safety(&self) -> PyResult<()> {
        self.validate_execution_safety().map_err(to_pyvalue_err)
    }

    fn __repr__(&self) -> String {
        stringify!(OKXExecClientConfig).to_string()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_data_config_py_new_load_spreads() {
        let config = OKXDataClientConfig::py_new(
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, true, None,
        );

        assert!(config.load_spreads);
        assert_eq!(config.environment, OKXEnvironment::Demo);
        assert_eq!(config.book_stale_check_interval_secs, 5);
        assert_eq!(config.book_stale_threshold_secs, 30);
        assert_eq!(config.book_snapshot_timeout_secs, 3);
    }

    #[rstest]
    fn test_exec_config_py_new_load_spreads() {
        let config = OKXExecClientConfig::py_new(
            TraderId::from("TRADER-001"),
            AccountId::from("OKX-001"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert!(config.load_spreads);
        assert_eq!(config.reconnect_policy, OKXReconnectPolicy::Reconcile);
        assert_eq!(config.reconnect_reconcile_lookback_secs, 60);
        assert_eq!(config.environment, OKXEnvironment::Demo);
        assert_eq!(config.auth_timeout_secs, None);
    }

    #[rstest]
    fn test_data_config_py_new_explicit_live_environment_is_preserved() {
        let config = OKXDataClientConfig::py_new(
            None,
            Some(OKXEnvironment::Live),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
        );

        assert_eq!(config.environment, OKXEnvironment::Live);
    }

    #[rstest]
    fn test_exec_config_py_new_explicit_live_environment_is_preserved() {
        let config = OKXExecClientConfig::py_new(
            TraderId::from("TRADER-001"),
            AccountId::from("OKX-001"),
            None,
            Some(OKXEnvironment::Live),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(OKXReconnectPolicy::Halt),
            Some(30),
            None,
            None,
            Some(true),
        );

        assert_eq!(config.environment, OKXEnvironment::Live);
        assert_eq!(config.reconnect_policy, OKXReconnectPolicy::Halt);
        assert_eq!(config.reconnect_reconcile_lookback_secs, 30);
        assert!(config.py_validate_execution_safety().is_err());
    }
}
