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

//! Configuration structures for the OKX adapter.

use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

use crate::{
    common::{
        credential::credential_env_vars,
        enums::{
            OKXContractType, OKXEnvironment, OKXInstrumentType, OKXMarginMode, OKXPositionMode,
            OKXReconnectPolicy, OKXRegion, OKXVipLevel,
        },
        urls::{
            get_http_base_url, get_ws_base_url_business, get_ws_base_url_private,
            get_ws_base_url_public,
        },
    },
    protection::CancelAllAfterPolicy,
};

/// Configuration for the OKX data client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.okx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.okx")
)]
pub struct OKXDataClientConfig {
    /// Optional API key for authenticated endpoints.
    pub api_key: Option<String>,
    /// Optional API secret for authenticated endpoints.
    pub api_secret: Option<String>,
    /// Optional API passphrase for authenticated endpoints.
    pub api_passphrase: Option<String>,
    /// Instrument types to load and subscribe to.
    #[builder(default = vec![OKXInstrumentType::Spot])]
    pub instrument_types: Vec<OKXInstrumentType>,
    /// Contract type filter applied to loaded instruments.
    pub contract_types: Option<Vec<OKXContractType>>,
    /// Whether to load spread trading instruments from the separate spread endpoint.
    #[builder(default)]
    pub load_spreads: bool,
    /// Instrument families to load (e.g., "BTC-USD", "ETH-USD").
    /// Required for OPTIONS. Optional for FUTURES/SWAP. Not applicable for SPOT/MARGIN.
    pub instrument_families: Option<Vec<String>>,
    /// Optional override for the HTTP base URL.
    pub base_url_http: Option<String>,
    /// Optional override for the public WebSocket URL.
    pub base_url_ws_public: Option<String>,
    /// Optional override for the business WebSocket URL.
    pub base_url_ws_business: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// The API environment (live or demo).
    #[builder(default = OKXEnvironment::Demo)]
    pub environment: OKXEnvironment,
    /// The API region (global, EEA, or US).
    #[builder(default)]
    pub region: OKXRegion,
    /// HTTP timeout in seconds.
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    /// Maximum retry attempts for requests.
    #[builder(default = 3)]
    pub max_retries: u32,
    /// Initial retry delay in milliseconds.
    #[builder(default = 1_000)]
    pub retry_delay_initial_ms: u64,
    /// Maximum retry delay in milliseconds.
    #[builder(default = 10_000)]
    pub retry_delay_max_ms: u64,
    /// Interval for refreshing instruments in minutes.
    #[builder(default = 60)]
    pub update_instruments_interval_mins: u64,
    /// Interval for checking order book feed staleness in seconds.
    #[builder(default = 5)]
    pub book_stale_check_interval_secs: u64,
    /// Maximum time without order book updates before emitting a stale signal in seconds.
    ///
    /// Set to 0 to disable. Quiet markets can idle without book changes.
    #[builder(default = 30)]
    pub book_stale_threshold_secs: u64,
    /// Maximum time to wait for a post-reconnect order book snapshot in seconds.
    #[builder(default = 3)]
    pub book_snapshot_timeout_secs: u64,
    /// Optional VIP level that unlocks additional subscriptions.
    pub vip_level: Option<OKXVipLevel>,
    /// WebSocket transport backend (defaults to `Tungstenite`).
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(OKXDataClientConfig {
    instrument_types: Vec<OKXInstrumentType>,
    environment: OKXEnvironment,
    region: OKXRegion,
    base_url_http: Option<String>,
    base_url_ws_public: Option<String>,
    base_url_ws_business: Option<String>,
    http_timeout_secs: u64,
    max_retries: u32,
    retry_delay_initial_ms: u64,
    retry_delay_max_ms: u64,
    update_instruments_interval_mins: u64,
    book_stale_check_interval_secs: u64,
    book_stale_threshold_secs: u64,
    book_snapshot_timeout_secs: u64,
    vip_level: Option<OKXVipLevel>,
    load_spreads: bool,
    transport_backend: TransportBackend,
});

impl Default for OKXDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl OKXDataClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` when all API credential fields are available (in config or env vars).
    #[must_use]
    pub fn has_api_credentials(&self) -> bool {
        let (key_var, secret_var, passphrase_var) = credential_env_vars();
        let has_key = self.api_key.is_some() || std::env::var(key_var).is_ok();
        let has_secret = self.api_secret.is_some() || std::env::var(secret_var).is_ok();
        let has_passphrase = self.api_passphrase.is_some() || std::env::var(passphrase_var).is_ok();
        has_key && has_secret && has_passphrase
    }

    /// Returns the HTTP base URL, falling back to the region default when unset.
    #[must_use]
    pub fn http_base_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| get_http_base_url(self.region).to_string())
    }

    /// Returns the public WebSocket URL, respecting the region, environment, and overrides.
    #[must_use]
    pub fn ws_public_url(&self) -> String {
        self.base_url_ws_public
            .clone()
            .unwrap_or_else(|| get_ws_base_url_public(self.region, self.environment).to_string())
    }

    /// Returns the business WebSocket URL, respecting the region, environment, and overrides.
    #[must_use]
    pub fn ws_business_url(&self) -> String {
        self.base_url_ws_business
            .clone()
            .unwrap_or_else(|| get_ws_base_url_business(self.region, self.environment).to_string())
    }

    /// Returns `true` when the business WebSocket should be instantiated.
    ///
    /// The business WebSocket carries public candle data and does not
    /// require authentication, so it is always needed.
    #[must_use]
    pub fn requires_business_ws(&self) -> bool {
        true
    }
}

/// Configuration for the OKX execution client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.okx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.okx")
)]
pub struct OKXExecClientConfig {
    /// The trader ID for the client.
    #[builder(default = TraderId::from("TRADER-001"))]
    pub trader_id: TraderId,
    /// The account ID for the client.
    #[builder(default = AccountId::from("OKX-001"))]
    pub account_id: AccountId,
    /// Optional API key for authenticated endpoints.
    pub api_key: Option<String>,
    /// Optional API secret for authenticated endpoints.
    pub api_secret: Option<String>,
    /// Optional API passphrase for authenticated endpoints.
    pub api_passphrase: Option<String>,
    /// Instrument types the execution client should support.
    #[builder(default = vec![OKXInstrumentType::Spot])]
    pub instrument_types: Vec<OKXInstrumentType>,
    /// Contract type filter applied to operations.
    pub contract_types: Option<Vec<OKXContractType>>,
    /// Instrument families to load (e.g., "BTC-USD", "ETH-USD").
    /// Required for OPTIONS. Optional for FUTURES/SWAP. Not applicable for SPOT/MARGIN.
    pub instrument_families: Option<Vec<String>>,
    /// Optional override for the HTTP base URL.
    pub base_url_http: Option<String>,
    /// Optional override for the private WebSocket URL.
    pub base_url_ws_private: Option<String>,
    /// Optional override for the business WebSocket URL.
    pub base_url_ws_business: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// The API environment (live or demo).
    #[builder(default = OKXEnvironment::Demo)]
    pub environment: OKXEnvironment,
    /// The API region (global, EEA, or US).
    #[builder(default)]
    pub region: OKXRegion,
    /// HTTP timeout in seconds.
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    /// Reserved for a future fills-channel implementation.
    ///
    /// Must remain false; execution client construction rejects true. The orders channel carries
    /// both order and fill lifecycle updates.
    #[builder(default)]
    pub use_fills_channel: bool,
    /// Whether to subscribe to spread order updates from the separate spread channel.
    #[builder(default)]
    pub load_spreads: bool,
    /// Enables mass-cancel support when true.
    #[builder(default)]
    pub use_mm_mass_cancel: bool,
    /// Maximum retry attempts for requests.
    #[builder(default = 3)]
    pub max_retries: u32,
    /// Initial retry delay in milliseconds.
    #[builder(default = 1_000)]
    pub retry_delay_initial_ms: u64,
    /// Maximum retry delay in milliseconds.
    #[builder(default = 10_000)]
    pub retry_delay_max_ms: u64,
    /// Optional margin mode (CROSS or ISOLATED) for margin/derivative accounts.
    pub margin_mode: Option<OKXMarginMode>,
    /// Enables margin/leverage for SPOT trading when true.
    #[builder(default)]
    pub use_spot_margin: bool,
    /// Explicitly permits margin and derivative order execution.
    ///
    /// Defaults to false so an API key with broad venue permissions cannot accidentally trade
    /// leveraged products. Market-data clients remain free to subscribe to derivatives.
    #[builder(default)]
    pub allow_leveraged_products: bool,
    /// Venue-issued OKX UID expected for live credentials.
    ///
    /// Required in live execution. The client verifies this against `GET /api/v5/account/config`
    /// before opening execution WebSockets or admitting orders.
    pub expected_uid: Option<String>,
    /// Expected OKX main-account UID. When set, it is verified in addition to `expected_uid`.
    pub expected_main_uid: Option<String>,
    /// Expected OKX account type (`type`) returned by the venue.
    pub expected_account_type: Option<u8>,
    /// Expected account mode (`acctLv`, 1 through 4) returned by OKX.
    pub expected_account_level: Option<u8>,
    /// Expected derivatives position mode. Defaults to net mode for live execution.
    #[builder(default = OKXPositionMode::NetMode)]
    pub expected_position_mode: OKXPositionMode,
    /// Require the API key to be bound to at least one IP address.
    #[builder(default = true)]
    pub require_ip_binding: bool,
    /// Cancel All After countdown in seconds. Required in live execution (10 through 120).
    pub cancel_all_after_secs: Option<u64>,
    /// CAA renewal cadence in seconds.
    ///
    /// Must leave room for at least one retried renewal before the countdown fires
    /// (`heartbeat + 2 x request timeout + 1s < countdown`, where the request timeout is a third
    /// of the countdown clamped to 1-5s). The OKX endpoint accepts one request per second, so
    /// the default of 5s keeps renewals well inside the venue rate limit.
    #[builder(default = 5)]
    pub cancel_all_after_heartbeat_secs: u64,
    /// How an execution WebSocket reconnect is handled (defaults to `Reconcile`).
    ///
    /// `Reconcile` fetches a venue snapshot for the disconnected window and reconciles it
    /// through the execution engine, so routine transport blips do not stop trading.
    /// `Halt` treats every reconnect as an unobserved event gap and fail-stops the node.
    #[builder(default)]
    pub reconnect_policy: OKXReconnectPolicy,
    /// Safety margin in seconds subtracted from the last observed WebSocket message time when
    /// requesting the post-reconnect venue snapshot (default 60).
    #[builder(default = 60)]
    pub reconnect_reconcile_lookback_secs: u64,
    /// Optional WebSocket authentication timeout (seconds), defaulting to
    /// `AUTHENTICATION_TIMEOUT_SECS` when unset.
    pub auth_timeout_secs: Option<u64>,
    /// WebSocket transport backend (defaults to `Tungstenite`).
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(OKXExecClientConfig {
    trader_id: TraderId,
    account_id: AccountId,
    instrument_types: Vec<OKXInstrumentType>,
    environment: OKXEnvironment,
    region: OKXRegion,
    base_url_http: Option<String>,
    base_url_ws_private: Option<String>,
    base_url_ws_business: Option<String>,
    http_timeout_secs: u64,
    max_retries: u32,
    retry_delay_initial_ms: u64,
    retry_delay_max_ms: u64,
    margin_mode: Option<OKXMarginMode>,
    allow_leveraged_products: bool,
    expected_uid: Option<String>,
    expected_main_uid: Option<String>,
    expected_account_type: Option<u8>,
    expected_account_level: Option<u8>,
    expected_position_mode: OKXPositionMode,
    require_ip_binding: bool,
    cancel_all_after_secs: Option<u64>,
    cancel_all_after_heartbeat_secs: u64,
    reconnect_policy: OKXReconnectPolicy,
    reconnect_reconcile_lookback_secs: u64,
    load_spreads: bool,
    use_spot_margin: bool,
    auth_timeout_secs: Option<u64>,
    transport_backend: TransportBackend,
});

impl Default for OKXExecClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl OKXExecClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates exchange-facing execution safety configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when live identity/CAA requirements are incomplete or any configured
    /// value cannot be proven safe before connecting.
    pub fn validate_execution_safety(&self) -> anyhow::Result<()> {
        let requests_leveraged_products = self.use_spot_margin
            || self.instrument_types.iter().any(|instrument_type| {
                matches!(
                    instrument_type,
                    OKXInstrumentType::Margin
                        | OKXInstrumentType::Swap
                        | OKXInstrumentType::Futures
                        | OKXInstrumentType::Option
                )
            });
        anyhow::ensure!(
            self.allow_leveraged_products || !requests_leveraged_products,
            "OKX margin and derivative execution requires allow_leveraged_products=true"
        );
        if let Some(level) = self.expected_account_level {
            anyhow::ensure!(
                (1..=4).contains(&level),
                "OKX expected_account_level must be between 1 and 4"
            );
        }
        if let Some(account_type) = self.expected_account_type {
            anyhow::ensure!(
                [0, 1, 2, 5, 9, 12].contains(&account_type),
                "unsupported OKX expected_account_type {account_type}"
            );
        }
        anyhow::ensure!(
            self.cancel_all_after_heartbeat_secs > 0,
            "OKX Cancel All After heartbeat must be greater than zero"
        );
        if let Some(timeout) = self.cancel_all_after_secs {
            anyhow::ensure!(
                (10..=120).contains(&timeout),
                "OKX Cancel All After timeout must be between 10 and 120 seconds"
            );
            CancelAllAfterPolicy::from_secs(timeout, self.cancel_all_after_heartbeat_secs)
                .validate()
                .map_err(|reason| anyhow::anyhow!("OKX {reason}"))?;
        }
        anyhow::ensure!(
            self.reconnect_reconcile_lookback_secs > 0,
            "OKX reconnect_reconcile_lookback_secs must be greater than zero"
        );

        if self.environment == OKXEnvironment::Live {
            anyhow::ensure!(
                self.expected_uid
                    .as_deref()
                    .is_some_and(|uid| !uid.trim().is_empty()),
                "live OKX execution requires expected_uid from the venue account configuration"
            );
            anyhow::ensure!(
                self.expected_main_uid
                    .as_deref()
                    .is_some_and(|uid| !uid.trim().is_empty()),
                "live OKX execution requires expected_main_uid from the venue account configuration"
            );
            anyhow::ensure!(
                self.expected_account_type.is_some(),
                "live OKX execution requires expected_account_type"
            );
            anyhow::ensure!(
                self.expected_account_level.is_some(),
                "live OKX execution requires expected_account_level"
            );
            anyhow::ensure!(
                self.require_ip_binding,
                "live OKX execution requires API-key IP binding"
            );
            anyhow::ensure!(
                self.cancel_all_after_secs.is_some(),
                "live OKX execution requires Cancel All After protection"
            );
            anyhow::ensure!(
                !self.load_spreads,
                "live OKX spread execution is disabled because order-book Cancel All After does not protect spread orders"
            );
        }
        Ok(())
    }

    /// Returns `true` when all API credential fields are available (in config or env vars).
    #[must_use]
    pub fn has_api_credentials(&self) -> bool {
        let (key_var, secret_var, passphrase_var) = credential_env_vars();
        let has_key = self.api_key.is_some() || std::env::var(key_var).is_ok();
        let has_secret = self.api_secret.is_some() || std::env::var(secret_var).is_ok();
        let has_passphrase = self.api_passphrase.is_some() || std::env::var(passphrase_var).is_ok();
        has_key && has_secret && has_passphrase
    }

    /// Returns the HTTP base URL, falling back to the region default when unset.
    #[must_use]
    pub fn http_base_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| get_http_base_url(self.region).to_string())
    }

    /// Returns the private WebSocket URL, respecting the region, environment, and overrides.
    #[must_use]
    pub fn ws_private_url(&self) -> String {
        self.base_url_ws_private
            .clone()
            .unwrap_or_else(|| get_ws_base_url_private(self.region, self.environment).to_string())
    }

    /// Returns the business WebSocket URL, respecting the region, environment, and overrides.
    #[must_use]
    pub fn ws_business_url(&self) -> String {
        self.base_url_ws_business
            .clone()
            .unwrap_or_else(|| get_ws_base_url_business(self.region, self.environment).to_string())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_data_config_toml_minimal() {
        let config: OKXDataClientConfig = toml::from_str(
            r#"
environment = "demo"
instrument_types = ["SPOT", "SWAP"]
http_timeout_secs = 90
"#,
        )
        .unwrap();

        assert_eq!(config.environment, OKXEnvironment::Demo);
        assert_eq!(
            config.instrument_types,
            vec![OKXInstrumentType::Spot, OKXInstrumentType::Swap]
        );
        assert_eq!(config.http_timeout_secs, 90);
        assert!(!config.load_spreads);
        assert_eq!(config.book_stale_check_interval_secs, 5);
        assert_eq!(config.book_stale_threshold_secs, 30);
        assert_eq!(config.book_snapshot_timeout_secs, 3);
    }

    #[rstest]
    fn test_data_config_toml_load_spreads() {
        let config: OKXDataClientConfig = toml::from_str(
            "
load_spreads = true
",
        )
        .unwrap();

        assert!(config.load_spreads);
    }

    #[rstest]
    fn test_data_config_toml_book_stale_settings() {
        let config: OKXDataClientConfig = toml::from_str(
            "
book_stale_check_interval_secs = 2
book_stale_threshold_secs = 7
book_snapshot_timeout_secs = 4
",
        )
        .unwrap();

        assert_eq!(config.book_stale_check_interval_secs, 2);
        assert_eq!(config.book_stale_threshold_secs, 7);
        assert_eq!(config.book_snapshot_timeout_secs, 4);
    }

    #[rstest]
    fn test_exec_config_toml_empty_uses_defaults() {
        let config: OKXExecClientConfig = toml::from_str("").unwrap();
        let expected = OKXExecClientConfig::default();

        assert_eq!(config.trader_id, expected.trader_id);
        assert_eq!(config.account_id, expected.account_id);
        assert_eq!(config.environment, expected.environment);
        assert_eq!(config.environment, OKXEnvironment::Demo);
        assert_eq!(config.instrument_types, expected.instrument_types);
        assert_eq!(config.http_timeout_secs, expected.http_timeout_secs);
        assert_eq!(config.use_fills_channel, expected.use_fills_channel);
        assert_eq!(config.load_spreads, expected.load_spreads);
        assert_eq!(config.use_mm_mass_cancel, expected.use_mm_mass_cancel);
        assert_eq!(config.transport_backend, expected.transport_backend);
        assert!(config.validate_execution_safety().is_ok());
    }

    #[rstest]
    fn test_live_execution_safety_requires_identity_ip_and_caa() {
        let config = OKXExecClientConfig::builder()
            .environment(OKXEnvironment::Live)
            .build();

        let error = config.validate_execution_safety().unwrap_err();
        assert!(error.to_string().contains("expected_uid"));
    }

    #[rstest]
    fn test_live_execution_safety_accepts_complete_order_book_profile() {
        let config = OKXExecClientConfig::builder()
            .environment(OKXEnvironment::Live)
            .expected_uid("123".to_string())
            .expected_main_uid("123".to_string())
            .expected_account_type(0)
            .expected_account_level(1)
            .cancel_all_after_secs(30)
            .build();

        config.validate_execution_safety().unwrap();
    }

    #[rstest]
    fn test_live_execution_safety_rejects_unbound_key_override() {
        let config = OKXExecClientConfig::builder()
            .environment(OKXEnvironment::Live)
            .expected_uid("123".to_string())
            .expected_main_uid("123".to_string())
            .expected_account_type(0)
            .expected_account_level(1)
            .require_ip_binding(false)
            .cancel_all_after_secs(30)
            .build();

        let error = config.validate_execution_safety().unwrap_err();
        assert!(error.to_string().contains("IP binding"));
    }

    #[rstest]
    fn test_execution_safety_rejects_heartbeat_without_retry_headroom() {
        let config = OKXExecClientConfig::builder()
            .cancel_all_after_secs(10)
            .cancel_all_after_heartbeat_secs(5)
            .build();

        let error = config.validate_execution_safety().unwrap_err();
        assert!(error.to_string().contains("retry"), "{error}");

        let config = OKXExecClientConfig::builder()
            .cancel_all_after_secs(10)
            .cancel_all_after_heartbeat_secs(2)
            .build();
        config.validate_execution_safety().unwrap();
    }

    #[rstest]
    fn test_exec_config_reconnect_defaults_and_toml() {
        let config = OKXExecClientConfig::default();
        assert_eq!(config.reconnect_policy, OKXReconnectPolicy::Reconcile);
        assert_eq!(config.reconnect_reconcile_lookback_secs, 60);
        assert_eq!(config.cancel_all_after_heartbeat_secs, 5);

        let config: OKXExecClientConfig = toml::from_str(
            r#"
reconnect_policy = "halt"
reconnect_reconcile_lookback_secs = 120
"#,
        )
        .unwrap();
        assert_eq!(config.reconnect_policy, OKXReconnectPolicy::Halt);
        assert_eq!(config.reconnect_reconcile_lookback_secs, 120);

        let config = OKXExecClientConfig::builder()
            .reconnect_reconcile_lookback_secs(0)
            .build();
        let error = config.validate_execution_safety().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("reconnect_reconcile_lookback_secs")
        );
    }

    #[rstest]
    fn test_exec_config_toml_load_spreads() {
        let config: OKXExecClientConfig = toml::from_str(
            "
load_spreads = true
",
        )
        .unwrap();

        assert!(config.load_spreads);
    }

    #[rstest]
    fn test_data_config_defaults_to_demo_global_urls() {
        let config = OKXDataClientConfig::default();

        assert_eq!(config.environment, OKXEnvironment::Demo);
        assert_eq!(config.region, OKXRegion::Global);
        assert_eq!(config.http_base_url(), "https://www.okx.com");
        assert_eq!(
            config.ws_public_url(),
            "wss://wspap.okx.com:8443/ws/v5/public"
        );
        assert_eq!(
            config.ws_business_url(),
            "wss://wspap.okx.com:8443/ws/v5/business"
        );
    }

    #[rstest]
    fn test_data_config_eea_region_urls() {
        let config = OKXDataClientConfig::builder()
            .region(OKXRegion::Eea)
            .build();

        assert_eq!(config.http_base_url(), "https://eea.okx.com");
        assert_eq!(
            config.ws_public_url(),
            "wss://wseeapap.okx.com:8443/ws/v5/public"
        );
        assert_eq!(
            config.ws_business_url(),
            "wss://wseeapap.okx.com:8443/ws/v5/business"
        );
    }

    #[rstest]
    fn test_exec_config_eea_region_urls() {
        let config = OKXExecClientConfig::builder()
            .region(OKXRegion::Eea)
            .build();

        assert_eq!(config.http_base_url(), "https://eea.okx.com");
        assert_eq!(
            config.ws_private_url(),
            "wss://wseeapap.okx.com:8443/ws/v5/private"
        );
        assert_eq!(
            config.ws_business_url(),
            "wss://wseeapap.okx.com:8443/ws/v5/business"
        );
    }

    #[rstest]
    fn test_data_config_explicit_live_environment_is_preserved() {
        let config = OKXDataClientConfig::builder()
            .environment(OKXEnvironment::Live)
            .build();

        assert_eq!(config.environment, OKXEnvironment::Live);
        assert_eq!(config.ws_public_url(), "wss://ws.okx.com:8443/ws/v5/public");
        assert_eq!(
            config.ws_business_url(),
            "wss://ws.okx.com:8443/ws/v5/business"
        );
    }

    #[rstest]
    fn test_exec_config_explicit_live_environment_is_preserved() {
        let config = OKXExecClientConfig::builder()
            .environment(OKXEnvironment::Live)
            .build();

        assert_eq!(config.environment, OKXEnvironment::Live);
        assert_eq!(
            config.ws_private_url(),
            "wss://ws.okx.com:8443/ws/v5/private"
        );
        assert_eq!(
            config.ws_business_url(),
            "wss://ws.okx.com:8443/ws/v5/business"
        );
    }

    #[rstest]
    fn test_config_region_override_takes_precedence() {
        let config = OKXDataClientConfig::builder()
            .region(OKXRegion::Eea)
            .base_url_http("https://custom.proxy".to_string())
            .build();

        assert_eq!(config.http_base_url(), "https://custom.proxy");
    }

    #[rstest]
    fn test_data_config_toml_region() {
        let config: OKXDataClientConfig = toml::from_str(
            r#"
region = "eea"
"#,
        )
        .unwrap();

        assert_eq!(config.region, OKXRegion::Eea);
    }

    #[rstest]
    fn test_exec_config_auth_timeout_secs() {
        assert_eq!(OKXExecClientConfig::default().auth_timeout_secs, None);

        let exec = OKXExecClientConfig::builder().auth_timeout_secs(4).build();
        assert_eq!(exec.auth_timeout_secs, Some(4));

        let exec: OKXExecClientConfig = toml::from_str("auth_timeout_secs = 8\n").unwrap();
        assert_eq!(exec.auth_timeout_secs, Some(8));
    }
}
