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

//! Provides a configuration for `RiskEngine` instances.

use ahash::AHashMap;
use nautilus_common::{
    config::{ConfigError, ConfigErrorCollector, ConfigResult},
    throttler::RateLimit,
};
use nautilus_core::datetime::NANOSECONDS_IN_SECOND;
use nautilus_model::identifiers::InstrumentId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Maximum configurable age for quote/trade evidence used to price market-style orders.
pub const MAX_MARKET_DATA_AGE_NS: u64 = 60 * NANOSECONDS_IN_SECOND;

/// Configuration for `RiskEngineConfig` instances.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.risk", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.risk")
)]
#[cfg_attr(
    feature = "python",
    expect(
        clippy::unsafe_derive_deserialize,
        reason = "config deserializes plain fields; unsafe methods come from generated PyO3 integration"
    )
)]
#[derive(Debug, Clone, Deserialize, Serialize, bon::Builder)]
#[builder(finish_fn(name = build_inner, vis = ""))]
#[serde(default, deny_unknown_fields)]
pub struct RiskEngineConfig {
    #[builder(default)]
    pub bypass: bool,
    #[builder(default = RateLimit::new(100, NANOSECONDS_IN_SECOND))]
    pub max_order_submit: RateLimit,
    #[builder(default = RateLimit::new(100, NANOSECONDS_IN_SECOND))]
    pub max_order_modify: RateLimit,
    /// Per-instrument maximum order notionals.
    ///
    /// An empty map configures no per-order capital ceiling; it must not be interpreted as a
    /// portfolio/account exposure limit. Account, collateral, and free-balance evidence remains
    /// mandatory when risk checks are not bypassed. Even when populated, this is only a hard
    /// pre-trade ceiling for orders with an explicit execution limit. A locally stressed market
    /// estimate is not a venue-enforced bound, so real-money market orders also require
    /// adapter/exchange price protection.
    #[builder(default)]
    pub max_notional_per_order: AHashMap<InstrumentId, Decimal>,
    /// Maximum local-receipt age for quote/trade evidence used in market-style valuation.
    ///
    /// This must be configured together with [`Self::market_order_price_bound_bps`]. When the
    /// pair is absent, orders without an explicit execution limit are denied. Enabling the pair
    /// only permits a conservative local estimate; it does not install or prove a venue-enforced
    /// price collar.
    pub max_market_data_age_ns: Option<u64>,
    /// Adverse local valuation stress, in basis points, for orders without an explicit limit.
    ///
    /// This must be configured together with [`Self::max_market_data_age_ns`]. It is not a hard
    /// execution-price bound and therefore cannot make `max_notional_per_order` a hard capital
    /// limit for market-style orders. Real-money deployments also need adapter/exchange price
    /// protection.
    pub market_order_price_bound_bps: Option<u32>,
    #[builder(default)]
    pub debug: bool,
}

impl<S: risk_engine_config_builder::IsComplete> RiskEngineConfigBuilder<S> {
    /// Validates and builds the [`RiskEngineConfig`].
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] if any field fails validation
    /// (see [`RiskEngineConfig::validate`]).
    pub fn build(self) -> ConfigResult<RiskEngineConfig> {
        let config = self.build_inner();
        config.validate()?;
        Ok(config)
    }
}

impl RiskEngineConfig {
    /// Validates the risk engine configuration, collecting every field violation.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] (a [`ConfigError::Multiple`] when more than one field is
    /// invalid) if any field fails validation.
    pub fn validate(&self) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();

        for (instrument_id, notional) in &self.max_notional_per_order {
            errors.check(
                *notional > Decimal::ZERO,
                ConfigError::range(
                    "max_notional_per_order",
                    format!("notional for {instrument_id} must be positive, was {notional}"),
                ),
            );
        }

        errors.check(
            self.max_market_data_age_ns.is_some()
                == self.market_order_price_bound_bps.is_some(),
            ConfigError::range(
                "market_order_valuation",
                "max_market_data_age_ns and market_order_price_bound_bps must be configured together",
            ),
        );
        if let Some(max_age_ns) = self.max_market_data_age_ns {
            errors.check(
                (1..=MAX_MARKET_DATA_AGE_NS).contains(&max_age_ns),
                ConfigError::range(
                    "max_market_data_age_ns",
                    format!("must be between 1 and {MAX_MARKET_DATA_AGE_NS} nanoseconds"),
                ),
            );
        }
        if let Some(bound_bps) = self.market_order_price_bound_bps {
            errors.check(
                (1..10_000).contains(&bound_bps),
                ConfigError::range(
                    "market_order_price_bound_bps",
                    "must be between 1 and 9999 basis points",
                ),
            );
        }

        errors.into_result()
    }
}

impl Default for RiskEngineConfig {
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default `RiskEngineConfig` should be valid")
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_default_config_is_valid() {
        assert!(RiskEngineConfig::builder().build().is_ok());
    }

    #[rstest]
    #[case(Decimal::ZERO)]
    #[case(Decimal::from(-1))]
    fn test_non_positive_notional_rejected(#[case] notional: Decimal) {
        let mut notionals = AHashMap::new();
        notionals.insert(InstrumentId::from("ESZ21.GLBX"), notional);
        let result = RiskEngineConfig::builder()
            .max_notional_per_order(notionals)
            .build();
        assert!(
            matches!(result, Err(ConfigError::Range { field, .. }) if field == "max_notional_per_order")
        );
    }

    #[rstest]
    fn test_positive_notional_accepted() {
        let mut notionals = AHashMap::new();
        notionals.insert(InstrumentId::from("ESZ21.GLBX"), Decimal::from(1_000_000));
        let result = RiskEngineConfig::builder()
            .max_notional_per_order(notionals)
            .build();
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_multiple_violations_collected() {
        let mut notionals = AHashMap::new();
        notionals.insert(InstrumentId::from("ESZ21.GLBX"), Decimal::ZERO);
        notionals.insert(InstrumentId::from("CLZ21.NYMEX"), Decimal::from(-1));
        let result = RiskEngineConfig::builder()
            .max_notional_per_order(notionals)
            .build();
        let ConfigError::Multiple { errors } = result.unwrap_err() else {
            panic!("expected ConfigError::Multiple");
        };
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().all(
            |e| matches!(e, ConfigError::Range { field, .. } if field == "max_notional_per_order")
        ));
    }

    #[rstest]
    #[case(Some(0), Some(500), "max_market_data_age_ns")]
    #[case(Some(MAX_MARKET_DATA_AGE_NS + 1), Some(500), "max_market_data_age_ns")]
    #[case(Some(NANOSECONDS_IN_SECOND), Some(0), "market_order_price_bound_bps")]
    #[case(
        Some(NANOSECONDS_IN_SECOND),
        Some(10_000),
        "market_order_price_bound_bps"
    )]
    #[case(Some(NANOSECONDS_IN_SECOND), None, "market_order_valuation")]
    #[case(None, Some(500), "market_order_valuation")]
    fn test_market_valuation_safety_config_rejects_invalid_values(
        #[case] max_market_data_age_ns: Option<u64>,
        #[case] market_order_price_bound_bps: Option<u32>,
        #[case] expected_field: &str,
    ) {
        let result = RiskEngineConfig::builder()
            .maybe_max_market_data_age_ns(max_market_data_age_ns)
            .maybe_market_order_price_bound_bps(market_order_price_bound_bps)
            .build();
        assert!(matches!(result, Err(ConfigError::Range { field, .. }) if field == expected_field));
    }

    #[rstest]
    fn test_market_valuation_safety_config_accepts_explicit_pair() {
        let result = RiskEngineConfig::builder()
            .max_market_data_age_ns(NANOSECONDS_IN_SECOND)
            .market_order_price_bound_bps(500)
            .build();

        assert!(result.is_ok());
    }
}
