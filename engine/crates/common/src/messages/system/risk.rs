// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::any::Any;

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::TradingState,
    identifiers::{InstrumentId, TraderId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Identifies the mutation which produced a complete risk-state checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskStateChangeReason {
    /// Initial state emitted when the risk engine starts.
    Initialized,
    /// The trading state changed.
    TradingStateChanged,
    /// A per-instrument maximum notional changed.
    MaxNotionalChanged,
    /// The risk engine reset to its configured defaults.
    Reset,
}

/// Canonical wire representation of a per-instrument maximum order notional.
///
/// Both values remain strings on the wire so an untrusted replay decoder can reject malformed
/// identifiers or decimals explicitly instead of accepting values constructed without validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskMaxNotional {
    /// Instrument identifier in canonical string form.
    pub instrument_id: String,
    /// Normalized decimal value in canonical string form.
    pub value: String,
}

impl RiskMaxNotional {
    /// Creates a canonical maximum-notional entry.
    #[must_use]
    pub fn new(instrument_id: InstrumentId, value: Decimal) -> Self {
        Self {
            instrument_id: instrument_id.to_string(),
            value: value.normalize().to_string(),
        }
    }
}

/// Complete, deterministic snapshot of operator-configurable risk policy state.
///
/// This message deliberately excludes transient runtime machinery such as throttler rolling
/// windows, counters, queues, buffers, and timers. It is a durable policy checkpoint, not a
/// byte-for-byte snapshot of the entire risk engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct RiskStateChanged {
    /// Trader associated with the risk engine.
    pub trader_id: TraderId,
    /// Current trading state.
    pub state: TradingState,
    /// Per-instrument maximum order notionals sorted by canonical instrument identifier.
    pub max_notional_per_order: Vec<RiskMaxNotional>,
    /// Mutation which produced this state checkpoint.
    pub reason: RiskStateChangeReason,
    /// Unique event identity.
    pub event_id: UUID4,
    /// UNIX timestamp (nanoseconds) when the state changed.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the instance was initialized.
    pub ts_init: UnixNanos,
}

impl RiskStateChanged {
    /// Creates a complete configurable risk-policy checkpoint with sorted notionals.
    #[must_use]
    pub fn new<I>(
        trader_id: TraderId,
        state: TradingState,
        max_notional_per_order: I,
        reason: RiskStateChangeReason,
        event_id: UUID4,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self
    where
        I: IntoIterator<Item = (InstrumentId, Decimal)>,
    {
        let mut max_notional_per_order = max_notional_per_order
            .into_iter()
            .map(|(instrument_id, value)| RiskMaxNotional::new(instrument_id, value))
            .collect::<Vec<_>>();
        max_notional_per_order.sort_unstable_by(|left, right| {
            left.instrument_id
                .cmp(&right.instrument_id)
                .then_with(|| left.value.cmp(&right.value))
        });

        Self {
            trader_id,
            state,
            max_notional_per_order,
            reason,
            event_id,
            ts_event,
            ts_init,
        }
    }

    /// Returns this message as [`Any`] for runtime message-bus dispatch.
    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::TradingState,
        identifiers::{InstrumentId, TraderId},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::{RiskMaxNotional, RiskStateChangeReason, RiskStateChanged};

    #[rstest]
    fn risk_max_notional_normalizes_wire_values() {
        let value = RiskMaxNotional::new(
            InstrumentId::from("BTC-USDT.OKX"),
            Decimal::new(1_000_000, 2),
        );

        assert_eq!(value.instrument_id, "BTC-USDT.OKX");
        assert_eq!(value.value, "10000");
    }

    #[rstest]
    fn risk_state_constructor_sorts_notionals_and_round_trips() {
        let event = RiskStateChanged::new(
            TraderId::from("TRADER-001"),
            TradingState::Halted,
            [
                (InstrumentId::from("ETH-USDT.OKX"), Decimal::new(250_000, 0)),
                (InstrumentId::from("BTC-USDT.OKX"), Decimal::new(500_000, 0)),
            ],
            RiskStateChangeReason::MaxNotionalChanged,
            UUID4::new(),
            UnixNanos::from(10),
            UnixNanos::from(11),
        );

        assert_eq!(
            event.max_notional_per_order[0].instrument_id,
            "BTC-USDT.OKX"
        );
        assert_eq!(
            event.max_notional_per_order[1].instrument_id,
            "ETH-USDT.OKX"
        );
        assert!(event.as_any().is::<RiskStateChanged>());

        let encoded = serde_json::to_vec(&event).expect("serialize risk state");
        let decoded: RiskStateChanged =
            serde_json::from_slice(&encoded).expect("deserialize risk state");
        assert_eq!(decoded, event);
    }
}
