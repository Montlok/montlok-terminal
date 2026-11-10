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

//! Python bindings for the [`BacktestResult`] type.

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use nautilus_analysis::{EvaluationGate, EvaluationSuite};
use nautilus_core::{UUID4, UnixNanos, python::to_pyvalue_err};
use pyo3::PyRef;

use crate::result::BacktestResult;

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pyo3::pymethods]
impl BacktestResult {
    #[getter]
    #[pyo3(name = "trader_id")]
    fn py_trader_id(&self) -> &str {
        &self.trader_id
    }

    #[getter]
    #[pyo3(name = "machine_id")]
    fn py_machine_id(&self) -> &str {
        &self.machine_id
    }

    #[getter]
    #[pyo3(name = "instance_id")]
    const fn py_instance_id(&self) -> UUID4 {
        self.instance_id
    }

    #[getter]
    #[pyo3(name = "run_config_id")]
    fn py_run_config_id(&self) -> Option<&str> {
        self.run_config_id.as_deref()
    }

    #[getter]
    #[pyo3(name = "run_id")]
    const fn py_run_id(&self) -> Option<UUID4> {
        self.run_id
    }

    #[getter]
    #[pyo3(name = "provenance_json")]
    fn py_provenance_json(&self) -> pyo3::PyResult<String> {
        serde_json::to_string(&self.provenance).map_err(nautilus_core::python::to_pyvalue_err)
    }

    #[getter]
    #[pyo3(name = "evidence_digest")]
    fn py_evidence_digest(&self) -> String {
        self.evidence_digest().to_string()
    }

    /// Returns native statistics using the stable promotion-gate metric namespace.
    #[pyo3(name = "evaluation_observations")]
    fn py_evaluation_observations(&self) -> HashMap<String, f64> {
        self.evaluation_observations().into_iter().collect()
    }

    /// Evaluates this result and returns a passing-only promotion attestation as JSON.
    #[pyo3(
        name = "promotion_attestation_json",
        signature = (suite_id, suite_version, gates_json, evaluated_at, baseline=None)
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 extracts optional Python references by value"
    )]
    fn py_promotion_attestation_json(
        &self,
        suite_id: &str,
        suite_version: &str,
        gates_json: &str,
        evaluated_at: u64,
        baseline: Option<PyRef<'_, Self>>,
    ) -> pyo3::PyResult<String> {
        let gates: Vec<EvaluationGate> =
            serde_json::from_str(gates_json).map_err(to_pyvalue_err)?;
        let suite = EvaluationSuite::new(suite_id, suite_version, gates).map_err(to_pyvalue_err)?;
        let baseline = baseline.as_deref();
        let attestation = self
            .promotion_attestation(&suite, baseline, UnixNanos::from(evaluated_at))
            .map_err(to_pyvalue_err)?;
        serde_json::to_string(&attestation).map_err(to_pyvalue_err)
    }

    /// Returns unsigned release claims bound to sealed run evidence as JSON.
    ///
    /// Production signing happens outside the backtest process. Use
    /// `release_promotion_signing_request_json` to obtain the exact payload for an HSM or
    /// evaluator service, then assemble the detached signature with
    /// `release_promotion_attestation_envelope_json`.
    #[pyo3(
        name = "release_promotion_attestation_json",
        signature = (suite_id, suite_version, gates_json, promotion_scope, evaluated_at, baseline=None)
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 extracts optional Python references by value"
    )]
    fn py_release_promotion_attestation_json(
        &self,
        suite_id: &str,
        suite_version: &str,
        gates_json: &str,
        promotion_scope: &str,
        evaluated_at: u64,
        baseline: Option<PyRef<'_, Self>>,
    ) -> pyo3::PyResult<String> {
        let gates: Vec<EvaluationGate> =
            serde_json::from_str(gates_json).map_err(to_pyvalue_err)?;
        let suite = EvaluationSuite::new(suite_id, suite_version, gates).map_err(to_pyvalue_err)?;
        let baseline = baseline.as_deref();
        let attestation = self
            .promotion_attestation_for_release(
                &suite,
                baseline,
                promotion_scope,
                UnixNanos::from(evaluated_at),
            )
            .map_err(to_pyvalue_err)?;
        serde_json::to_string(&attestation).map_err(to_pyvalue_err)
    }

    /// Returns the exact release attestation and signing payload for an external evaluator.
    ///
    /// The request is not proof by itself. The evaluator must authenticate the runner and verify
    /// the evidence through an independently trusted channel before authorizing a signature.
    #[pyo3(
        name = "release_promotion_signing_request_json",
        signature = (
            suite_id,
            suite_version,
            gates_json,
            promotion_scope,
            evaluated_at,
            evaluator_key_id,
            base_dir,
            baseline=None,
            baseline_dir=None
        )
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 extracts optional Python references by value"
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "the Python boundary names every externally signed evaluation input explicitly"
    )]
    fn py_release_promotion_signing_request_json(
        &self,
        suite_id: &str,
        suite_version: &str,
        gates_json: &str,
        promotion_scope: &str,
        evaluated_at: u64,
        evaluator_key_id: &str,
        base_dir: &str,
        baseline: Option<PyRef<'_, Self>>,
        baseline_dir: Option<&str>,
    ) -> pyo3::PyResult<String> {
        let gates: Vec<EvaluationGate> =
            serde_json::from_str(gates_json).map_err(to_pyvalue_err)?;
        let suite = EvaluationSuite::new(suite_id, suite_version, gates).map_err(to_pyvalue_err)?;
        let baseline = baseline.as_deref();
        let request = self
            .release_promotion_signing_request(
                &suite,
                baseline,
                Path::new(base_dir),
                baseline_dir.map(Path::new),
                promotion_scope,
                UnixNanos::from(evaluated_at),
                evaluator_key_id,
            )
            .map_err(to_pyvalue_err)?;
        serde_json::to_string(&request).map_err(to_pyvalue_err)
    }

    /// Assembles an externally produced detached evaluator signature with release claims.
    ///
    /// This validates the envelope shape only. The live trust policy performs cryptographic
    /// verification against the authorized evaluator public key.
    #[pyo3(
        name = "release_promotion_attestation_envelope_json",
        signature = (
            suite_id,
            suite_version,
            gates_json,
            promotion_scope,
            evaluated_at,
            evaluator_key_id,
            evaluator_signature,
            baseline=None
        )
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 extracts optional Python references by value"
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "the Python boundary names every externally signed evaluation input explicitly"
    )]
    fn py_release_promotion_attestation_envelope_json(
        &self,
        suite_id: &str,
        suite_version: &str,
        gates_json: &str,
        promotion_scope: &str,
        evaluated_at: u64,
        evaluator_key_id: &str,
        #[gen_stub(override_type(type_repr = "bytes"))] evaluator_signature: &[u8],
        baseline: Option<PyRef<'_, Self>>,
    ) -> pyo3::PyResult<String> {
        let gates: Vec<EvaluationGate> =
            serde_json::from_str(gates_json).map_err(to_pyvalue_err)?;
        let suite = EvaluationSuite::new(suite_id, suite_version, gates).map_err(to_pyvalue_err)?;
        let baseline = baseline.as_deref();
        let attestation = self
            .promotion_attestation_for_release(
                &suite,
                baseline,
                promotion_scope,
                UnixNanos::from(evaluated_at),
            )
            .map_err(to_pyvalue_err)?;
        let envelope = attestation
            .into_signed(evaluator_key_id, evaluator_signature)
            .map_err(to_pyvalue_err)?;
        serde_json::to_string(&envelope).map_err(to_pyvalue_err)
    }

    #[getter]
    #[pyo3(name = "run_started")]
    fn py_run_started(&self) -> Option<u64> {
        self.run_started.map(|timestamp| timestamp.as_u64())
    }

    #[getter]
    #[pyo3(name = "run_finished")]
    fn py_run_finished(&self) -> Option<u64> {
        self.run_finished.map(|timestamp| timestamp.as_u64())
    }

    #[getter]
    #[pyo3(name = "backtest_start")]
    fn py_backtest_start(&self) -> Option<u64> {
        self.backtest_start.map(|timestamp| timestamp.as_u64())
    }

    #[getter]
    #[pyo3(name = "backtest_end")]
    fn py_backtest_end(&self) -> Option<u64> {
        self.backtest_end.map(|timestamp| timestamp.as_u64())
    }

    #[getter]
    #[pyo3(name = "elapsed_time_secs")]
    const fn py_elapsed_time_secs(&self) -> f64 {
        self.elapsed_time_secs
    }

    #[getter]
    #[pyo3(name = "iterations")]
    const fn py_iterations(&self) -> usize {
        self.iterations
    }

    #[getter]
    #[pyo3(name = "total_events")]
    const fn py_total_events(&self) -> usize {
        self.total_events
    }

    #[getter]
    #[pyo3(name = "total_orders")]
    const fn py_total_orders(&self) -> usize {
        self.total_orders
    }

    #[getter]
    #[pyo3(name = "total_positions")]
    const fn py_total_positions(&self) -> usize {
        self.total_positions
    }

    #[getter]
    #[pyo3(name = "summary")]
    fn py_summary(&self) -> HashMap<String, String> {
        self.summary
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    #[getter]
    #[pyo3(name = "stats_pnls")]
    fn py_stats_pnls(&self) -> HashMap<String, HashMap<String, f64>> {
        self.stats_pnls
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter().map(|(k2, v2)| (k2.clone(), *v2)).collect(),
                )
            })
            .collect()
    }

    #[getter]
    #[pyo3(name = "stats_returns")]
    fn py_stats_returns(&self) -> HashMap<String, f64> {
        self.stats_returns
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    #[getter]
    #[pyo3(name = "stats_general")]
    fn py_stats_general(&self) -> HashMap<String, f64> {
        self.stats_general
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    #[getter]
    #[pyo3(name = "returns_series")]
    fn py_returns_series(&self) -> BTreeMap<u64, f64> {
        self.returns_series
            .iter()
            .map(|(timestamp, value)| (timestamp.as_u64(), *value))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "BacktestResult(trader_id='{}', elapsed={:.2}s, iterations={}, orders={}, positions={})",
            self.trader_id,
            self.elapsed_time_secs,
            self.iterations,
            self.total_orders,
            self.total_positions,
        )
    }
}
