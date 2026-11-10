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

//! Results from completed backtest runs.

use std::collections::BTreeMap;

use ahash::AHashMap;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_provenance::{ContentDigest, RunProvenance};
use nautilus_system::EventStoreSealReceipt;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug)]
pub(crate) struct EngineEvidenceSeal(ContentDigest);

/// Results from a completed backtest run.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.backtest",
        skip_from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.backtest")
)]
pub struct BacktestResult {
    pub trader_id: String,
    pub machine_id: String,
    pub instance_id: UUID4,
    pub run_config_id: Option<String>,
    pub run_id: Option<UUID4>,
    pub provenance: RunProvenance,
    pub run_started: Option<UnixNanos>,
    pub run_finished: Option<UnixNanos>,
    pub backtest_start: Option<UnixNanos>,
    pub backtest_end: Option<UnixNanos>,
    pub elapsed_time_secs: f64,
    pub iterations: usize,
    pub total_events: usize,
    pub total_orders: usize,
    pub total_positions: usize,
    pub summary: AHashMap<String, String>,
    pub stats_pnls: AHashMap<String, AHashMap<String, f64>>,
    pub stats_returns: AHashMap<String, f64>,
    pub stats_general: AHashMap<String, f64>,
    pub returns_series: BTreeMap<UnixNanos, f64>,
    /// Verified terminal commitment for the authoritative event-store run, when configured.
    pub event_store_receipt: Option<EventStoreSealReceipt>,
    #[serde(skip)]
    pub(crate) evidence_seal: Option<EngineEvidenceSeal>,
    #[serde(skip)]
    pub(crate) run_inputs_verified: bool,
}

/// Serializable canonical material required to independently recompute a backtest result digest.
///
/// The engine-only evidence seal is deliberately excluded: an external evaluator must recompute
/// the digest and verify the authoritative event store rather than trusting an in-process marker.
/// Unlike the public [`BacktestResult`] JSON representation, this type explicitly carries
/// `run_inputs_verified` because that bit participates in release eligibility and the canonical
/// result digest. This material is caller-serializable and therefore does not authenticate that
/// bit or any reported statistic. A matching digest and event-store receipt prove internal
/// consistency only; production release signing additionally requires trusted runner
/// authentication or evaluator-owned semantic reexecution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestResultEvidenceMaterial {
    pub trader_id: String,
    pub machine_id: String,
    pub instance_id: UUID4,
    pub run_config_id: Option<String>,
    pub run_id: Option<UUID4>,
    pub provenance: RunProvenance,
    pub run_started: Option<UnixNanos>,
    pub run_finished: Option<UnixNanos>,
    pub backtest_start: Option<UnixNanos>,
    pub backtest_end: Option<UnixNanos>,
    pub elapsed_time_secs: f64,
    pub iterations: usize,
    pub total_events: usize,
    pub total_orders: usize,
    pub total_positions: usize,
    pub summary: AHashMap<String, String>,
    pub stats_pnls: AHashMap<String, AHashMap<String, f64>>,
    pub stats_returns: AHashMap<String, f64>,
    pub stats_general: AHashMap<String, f64>,
    pub returns_series: BTreeMap<UnixNanos, f64>,
    pub event_store_receipt: Option<EventStoreSealReceipt>,
    pub run_inputs_verified: bool,
}

impl From<&BacktestResult> for BacktestResultEvidenceMaterial {
    fn from(result: &BacktestResult) -> Self {
        Self {
            trader_id: result.trader_id.clone(),
            machine_id: result.machine_id.clone(),
            instance_id: result.instance_id,
            run_config_id: result.run_config_id.clone(),
            run_id: result.run_id,
            provenance: result.provenance.clone(),
            run_started: result.run_started,
            run_finished: result.run_finished,
            backtest_start: result.backtest_start,
            backtest_end: result.backtest_end,
            elapsed_time_secs: result.elapsed_time_secs,
            iterations: result.iterations,
            total_events: result.total_events,
            total_orders: result.total_orders,
            total_positions: result.total_positions,
            summary: result.summary.clone(),
            stats_pnls: result.stats_pnls.clone(),
            stats_returns: result.stats_returns.clone(),
            stats_general: result.stats_general.clone(),
            returns_series: result.returns_series.clone(),
            event_store_receipt: result.event_store_receipt.clone(),
            run_inputs_verified: result.run_inputs_verified,
        }
    }
}

impl BacktestResultEvidenceMaterial {
    #[cfg(test)]
    pub(crate) fn to_result(&self) -> BacktestResult {
        BacktestResult {
            trader_id: self.trader_id.clone(),
            machine_id: self.machine_id.clone(),
            instance_id: self.instance_id,
            run_config_id: self.run_config_id.clone(),
            run_id: self.run_id,
            provenance: self.provenance.clone(),
            run_started: self.run_started,
            run_finished: self.run_finished,
            backtest_start: self.backtest_start,
            backtest_end: self.backtest_end,
            elapsed_time_secs: self.elapsed_time_secs,
            iterations: self.iterations,
            total_events: self.total_events,
            total_orders: self.total_orders,
            total_positions: self.total_positions,
            summary: self.summary.clone(),
            stats_pnls: self.stats_pnls.clone(),
            stats_returns: self.stats_returns.clone(),
            stats_general: self.stats_general.clone(),
            returns_series: self.returns_series.clone(),
            event_store_receipt: self.event_store_receipt.clone(),
            evidence_seal: None,
            run_inputs_verified: self.run_inputs_verified,
        }
    }
}

impl BacktestResult {
    pub(crate) fn issue_engine_evidence_seal(mut self) -> Self {
        if self.evidence_seal.is_none() {
            self.evidence_seal = Some(EngineEvidenceSeal(self.evidence_digest()));
        }
        self
    }

    pub(crate) const fn engine_evidence_seal_digest(&self) -> Option<ContentDigest> {
        match self.evidence_seal {
            Some(seal) => Some(seal.0),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap;
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn test_backtest_result_serializes_to_json() {
        let instance_id = UUID4::from("11111111-1111-4111-8111-111111111111");
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut summary = AHashMap::new();
        summary.insert("PnL (total)".to_string(), "10.00 USD".to_string());
        let mut usd_pnls = AHashMap::new();
        usd_pnls.insert("Returns Volatility (252 days)".to_string(), 1.25);
        let mut stats_pnls = AHashMap::new();
        stats_pnls.insert("USD".to_string(), usd_pnls);
        let mut stats_returns = AHashMap::new();
        stats_returns.insert("Sharpe Ratio (252 days)".to_string(), 0.75);
        let mut stats_general = AHashMap::new();
        stats_general.insert("Long Ratio".to_string(), 1.0);

        let result = BacktestResult {
            trader_id: "TRADER-001".to_string(),
            machine_id: "machine-1".to_string(),
            instance_id,
            run_config_id: Some("config-1".to_string()),
            run_id: Some(run_id),
            provenance: RunProvenance::default(),
            run_started: Some(UnixNanos::new(1)),
            run_finished: Some(UnixNanos::new(2)),
            backtest_start: Some(UnixNanos::new(3)),
            backtest_end: Some(UnixNanos::new(4)),
            elapsed_time_secs: 1.5,
            iterations: 10,
            total_events: 20,
            total_orders: 2,
            total_positions: 1,
            summary,
            stats_pnls,
            stats_returns,
            stats_general,
            returns_series: BTreeMap::from([(UnixNanos::new(3), 0.25)]),
            event_store_receipt: None,
            evidence_seal: None,
            run_inputs_verified: false,
        };

        let value = serde_json::to_value(&result).unwrap();
        let evidence_material =
            serde_json::to_value(BacktestResultEvidenceMaterial::from(&result)).unwrap();

        assert_eq!(value["trader_id"], json!("TRADER-001"));
        assert_eq!(value["machine_id"], json!("machine-1"));
        assert_eq!(value["instance_id"], json!(instance_id.to_string()));
        assert_eq!(value["run_id"], json!(run_id.to_string()));
        assert_eq!(value["run_started"], json!(1));
        assert_eq!(value["backtest_end"], json!(4));
        assert_eq!(value["elapsed_time_secs"], json!(1.5));
        assert_eq!(value["iterations"], json!(10));
        assert_eq!(value["summary"]["PnL (total)"], json!("10.00 USD"));
        assert_eq!(
            value["stats_pnls"]["USD"]["Returns Volatility (252 days)"],
            json!(1.25)
        );
        assert_eq!(
            value["stats_returns"]["Sharpe Ratio (252 days)"],
            json!(0.75)
        );
        assert_eq!(value["stats_general"]["Long Ratio"], json!(1.0));
        assert_eq!(value["returns_series"]["3"], json!(0.25));
        assert!(value.get("evidence_seal").is_none());
        assert_eq!(evidence_material["run_inputs_verified"], json!(false));
        assert!(evidence_material.get("evidence_seal").is_none());
    }
}
