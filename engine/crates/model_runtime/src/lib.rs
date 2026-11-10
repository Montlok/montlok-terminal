//! Model boundary used by the resident inference process, off the execution event loop.
//! A model only proposes a bounded long/cash target. The ordinary Nautilus risk and
//! execution engines still own position sizing, order admission and venue routing.

use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_PENDING: usize = 256;
const MAX_VALUES: usize = 262_144;

type Result<T> = std::result::Result<T, String>;

fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing_string:{key}"))
}
fn integer(v: &Value, key: &str) -> Result<u64> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("invalid_integer:{key}"))
}
fn finite(v: &Value) -> Result<f64> {
    v.as_f64()
        .filter(|v| v.is_finite())
        .ok_or_else(|| "nonfinite_or_nonnumeric".into())
}
fn identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_:./".contains(&c))
}

#[derive(Debug, Clone)]
struct Contract {
    names: Vec<String>,
    mean: Vec<f64>,
    scale: Vec<f64>,
    rows: usize,
    clip: (f64, f64),
}
impl Contract {
    fn parse(v: &Value) -> Result<Self> {
        let names: Vec<String> = v["names"]
            .as_array()
            .ok_or("missing_feature_names")?
            .iter()
            .map(|n| {
                n.as_str()
                    .filter(|s| identifier(s))
                    .map(str::to_owned)
                    .ok_or("invalid_feature_name".into())
            })
            .collect::<Result<_>>()?;
        if names.is_empty()
            || names.len() > 1024
            || names.iter().collect::<HashSet<_>>().len() != names.len()
        {
            return Err("invalid_feature_count_or_duplicates".into());
        }
        let mean = v["mean"]
            .as_array()
            .ok_or("missing_feature_mean")?
            .iter()
            .map(finite)
            .collect::<Result<Vec<_>>>()?;
        let scale = v["scale"]
            .as_array()
            .ok_or("missing_feature_scale")?
            .iter()
            .map(finite)
            .collect::<Result<Vec<_>>>()?;
        let rows =
            usize::try_from(integer(v, "sequenceBars")?).map_err(|_| "sequence_too_large")?;
        if rows == 0
            || rows > 4096
            || rows.saturating_mul(names.len()) > MAX_VALUES
            || mean.len() != names.len()
            || scale.len() != names.len()
            || scale.iter().any(|s| *s <= 0.0)
        {
            return Err("invalid_feature_shape_or_scaler".into());
        }
        let clip = v["clip"]
            .as_array()
            .filter(|v| v.len() == 2)
            .ok_or("invalid_clip")?;
        let clip = (finite(&clip[0])?, finite(&clip[1])?);
        if clip.0 >= clip.1 || clip.0 < -1e6 || clip.1 > 1e6 {
            return Err("invalid_clip".into());
        }
        Ok(Self {
            names,
            mean,
            scale,
            rows,
            clip,
        })
    }
    fn normalize(&self, request: &Value) -> Result<Value> {
        let expected = serde_json::to_value(&self.names).map_err(|_| "feature_encoding")?;
        if request["featureNames"] != expected {
            return Err("feature_order_mismatch".into());
        }
        if request.get("normalized").and_then(Value::as_bool) != Some(false) {
            return Err("raw_features_required".into());
        }
        let rows = request["inputs"]
            .as_array()
            .filter(|rows| rows.len() == self.rows)
            .ok_or("sequence_shape_mismatch")?;
        let mut output = Vec::with_capacity(self.rows);
        for row in rows {
            let row = row
                .as_array()
                .filter(|row| row.len() == self.names.len())
                .ok_or("feature_shape_mismatch")?;
            let mut values = Vec::with_capacity(row.len());
            for (i, value) in row.iter().enumerate() {
                let normalized = (finite(value)? - self.mean[i]) / self.scale[i];
                if !normalized.is_finite() {
                    return Err("normalization_overflow".into());
                }
                // Match the training contract's float32 network input after z-score and clipping.
                values.push(json!(normalized.clamp(self.clip.0, self.clip.1) as f32));
            }
            output.push(Value::Array(values));
        }
        Ok(Value::Array(output))
    }
}

#[derive(Debug, Clone)]
struct Pending {
    identity: Value,
    asof: u64,
    deadline: u64,
    instrument: String,
    scope: String,
    prepared: u64,
}

/// One instance per immutable release. Pending state is bounded; a process restart
/// discards in-flight requests. Strategy/run persistence owns execution watermarks.
pub struct ModelGuard {
    release: Value,
    contracts: HashMap<String, Contract>,
    modes: HashSet<String>,
    pending: HashMap<String, Pending>,
    consumed: HashMap<String, u64>,
    max_pending: usize,
    age_ns: u64,
    timeout_ns: u64,
    threshold: f64,
    max_fraction: f64,
    prepared: u64,
    accepted: u64,
    expired: u64,
}
impl ModelGuard {
    pub fn new(release: Value) -> Result<Self> {
        if integer(&release, "schemaVersion")? != 1 || !identifier(text(&release, "releaseId")?) {
            return Err("unsupported_release".into());
        }
        if !matches!(
            text(&release, "runnerId")?,
            "gru_v1" | "rdt4quant_v1" | "rdt4quant_cpu_v2"
        ) {
            return Err("unsupported_runner".into());
        }
        let digest = text(&release["model"], "sha256")?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid_model_digest".into());
        }
        let mut contracts = HashMap::new();
        if let Some(domains) = release.get("domainContracts").and_then(Value::as_object) {
            if domains.is_empty() || domains.len() > 256 {
                return Err("invalid_contract_count".into());
            }
            for (key, contract) in domains {
                contracts.insert(key.clone(), Contract::parse(contract)?);
            }
        } else {
            contracts.insert(
                "default".into(),
                Contract::parse(&release["featureContract"])?,
            );
        }
        let runtime = &release["runtime"];
        let max_pending =
            usize::try_from(integer(runtime, "maxQueueSize")?).map_err(|_| "queue_too_large")?;
        let age_ms = integer(runtime, "maxInputAgeMs")?;
        let timeout_ms = integer(runtime, "timeoutMs")?;
        if !(1..=MAX_PENDING).contains(&max_pending)
            || !(1..=172_800_000).contains(&age_ms)
            || !(1..=300_000).contains(&timeout_ms)
        {
            return Err("invalid_runtime_limits".into());
        }
        let modes = release["policy"]["allowedModes"]
            .as_array()
            .ok_or("missing_modes")?
            .iter()
            .map(|m| {
                m.as_str()
                    .filter(|v| matches!(*v, "shadow" | "sandbox" | "live"))
                    .map(str::to_owned)
                    .ok_or("unsupported_mode".into())
            })
            .collect::<Result<HashSet<_>>>()?;
        let threshold = finite(&release["policy"]["thresholdBps"])?;
        let max_fraction = finite(&release["policy"]["maxTargetFraction"])?;
        if modes.is_empty()
            || threshold < 0.0
            || threshold > 10_000.0
            || !(0.0..=1.0).contains(&max_fraction)
        {
            return Err("invalid_target_policy".into());
        }
        Ok(Self {
            release,
            contracts,
            modes,
            pending: HashMap::new(),
            consumed: HashMap::new(),
            max_pending,
            age_ns: age_ms * 1_000_000,
            timeout_ns: timeout_ms * 1_000_000,
            threshold,
            max_fraction,
            prepared: 0,
            accepted: 0,
            expired: 0,
        })
    }
    fn identity(&self, r: &Value) -> Result<Value> {
        let id = text(r, "requestId")?;
        if !identifier(id)
            || r["releaseId"] != self.release["releaseId"]
            || r["modelHash"] != self.release["model"]["sha256"]
        {
            return Err("request_identity_mismatch".into());
        }
        if !self.modes.contains(text(r, "mode")?) {
            return Err("execution_mode_not_released".into());
        }
        let instrument = text(r, "instrument")?;
        if !identifier(instrument) {
            return Err("invalid_instrument".into());
        }
        Ok(
            json!({"requestId":id,"releaseId":r["releaseId"],"modelHash":r["modelHash"],
            "instrument":instrument,"domain":r.get("domain").unwrap_or(&Value::Null),
            "mode":r["mode"],"asOfNs":r["asOfNs"],"deadlineNs":r["deadlineNs"]}),
        )
    }
    fn timing(&self, r: &Value, now: u64) -> Result<(u64, u64)> {
        let asof = integer(r, "asOfNs")?;
        let deadline = integer(r, "deadlineNs")?;
        if asof == 0 || asof > now || now - asof > self.age_ns {
            return Err("input_stale_or_future".into());
        }
        if deadline <= now || deadline - now > self.timeout_ns {
            return Err("invalid_inference_deadline".into());
        }
        Ok((asof, deadline))
    }
    pub fn prepare(&mut self, mut request: Value, now: u64) -> Result<Value> {
        let identity = self.identity(&request)?;
        let (asof, deadline) = self.timing(&request, now)?;
        let before = self.pending.len();
        self.pending.retain(|_, p| p.deadline > now);
        self.expired += (before - self.pending.len()) as u64;
        if self.pending.len() >= self.max_pending {
            return Err("model_queue_full".into());
        }
        let id = text(&request, "requestId")?.to_owned();
        if self.pending.contains_key(&id) {
            return Err("duplicate_request".into());
        }
        let instrument = text(&request, "instrument")?.to_owned();
        let scope = if self.contracts.contains_key("default") {
            if self.release["runnerId"] == "gru_v1" && instrument != "BTC-USDT" {
                return Err("unreleased_instrument".into());
            }
            "default".to_owned()
        } else {
            format!("{}:{}", text(&request, "domain")?, instrument)
        };
        if self
            .consumed
            .get(&scope)
            .is_some_and(|previous| asof <= *previous)
        {
            return Err("duplicate_or_out_of_order_window".into());
        }
        let contract = self
            .contracts
            .get(&scope)
            .ok_or("unreleased_domain_instrument")?;
        request["inputs"] = contract.normalize(&request)?;
        request["normalized"] = Value::Bool(true);
        request
            .as_object_mut()
            .ok_or("request_not_object")?
            .remove("op");
        request
            .as_object_mut()
            .ok_or("request_not_object")?
            .remove("nowNs");
        self.pending.insert(
            id,
            Pending {
                identity,
                asof,
                deadline,
                instrument,
                scope,
                prepared: now,
            },
        );
        self.prepared += 1;
        Ok(json!({"ok":true,"workerRequest":request,"pending":self.pending.len()}))
    }
    pub fn validate_prediction(
        &mut self,
        prediction: &Value,
        request: &Value,
        now: u64,
    ) -> Result<Value> {
        let identity = self.identity(request)?;
        let id = text(request, "requestId")?;
        let pending = self.pending.get(id).ok_or("no_pending_request")?;
        if pending.identity != identity {
            return Err("request_changed_after_prepare".into());
        }
        for key in [
            "requestId",
            "releaseId",
            "modelHash",
            "instrument",
            "asOfNs",
            "domain",
        ] {
            if prediction.get(key).unwrap_or(&Value::Null)
                != identity.get(key).unwrap_or(&Value::Null)
            {
                return Err(format!("prediction_identity_mismatch:{key}"));
            }
        }
        if prediction.get("error").is_some_and(|e| !e.is_null()) {
            return Err("worker_error".into());
        }
        if now < pending.prepared
            || now >= pending.deadline
            || now.saturating_sub(pending.asof) > self.age_ns
        {
            self.pending.remove(id);
            self.expired += 1;
            return Err("prediction_expired".into());
        }
        let completed = integer(prediction, "completedAtNs")?;
        if completed < pending.prepared || completed > now || completed >= pending.deadline {
            return Err("invalid_completion_time".into());
        }
        let value = finite(&prediction["prediction"])?;
        if !(-1.0..=1_000_000.0).contains(&value) {
            return Err("invalid_return_prediction".into());
        }
        for key in ["latencyMs", "queueMs", "inferenceMs"] {
            if prediction
                .get(key)
                .is_some_and(|v| finite(v).map_or(true, |n| n < 0.0))
            {
                return Err("invalid_latency".into());
            }
        }
        if let Some(rows) = prediction.get("quantilesBps") {
            for row in rows.as_array().ok_or("invalid_quantiles")? {
                let row = row
                    .as_array()
                    .filter(|r| r.len() == 3)
                    .ok_or("invalid_quantiles")?;
                let q = [finite(&row[0])?, finite(&row[1])?, finite(&row[2])?];
                if q[0] > q[1] || q[1] > q[2] {
                    return Err("unordered_quantiles".into());
                }
            }
        }
        let output = &self.release["outputContract"];
        if matches!(
            self.release["runnerId"].as_str(),
            Some("rdt4quant_v1" | "rdt4quant_cpu_v2")
        ) {
            let domain = text(request, "domain")?;
            let index = output["selectedHeadByDomain"][domain]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or("missing_selected_head")?;
            let horizons = output["horizons"][domain]
                .as_array()
                .ok_or("missing_horizons")?;
            let expected = horizons.get(index).ok_or("invalid_selected_head")?;
            if &prediction["horizon"] != expected
                || prediction["horizonUnit"] != output["horizonUnit"][domain]
            {
                return Err("prediction_horizon_mismatch".into());
            }
            let quantiles = prediction["quantilesBps"]
                .as_array()
                .filter(|rows| rows.len() == horizons.len())
                .ok_or("quantile_horizon_count_mismatch")?;
            let median_bps = finite(&quantiles[index][1])?;
            let expected_return = (median_bps / 10_000.0).exp_m1();
            if !expected_return.is_finite()
                || (value - expected_return).abs() > 1e-10 * expected_return.abs().max(1.0)
            {
                return Err("prediction_selected_head_value_mismatch".into());
            }
        } else if output.get("selectedHorizon").is_some()
            && (prediction["horizon"] != output["selectedHorizon"]
                || prediction["horizonUnit"] != output["selectedHorizonUnit"])
        {
            return Err("prediction_horizon_mismatch".into());
        }
        if self
            .consumed
            .get(&pending.scope)
            .is_some_and(|prior| pending.asof <= *prior)
        {
            self.pending.remove(id);
            return Err("prediction_out_of_order".into());
        }
        let target = if value * 10_000.0 >= self.threshold {
            self.max_fraction
        } else {
            0.0
        };
        let result = json!({"ok":true,"requestId":id,"releaseId":identity["releaseId"],"modelHash":identity["modelHash"],
            "instrument":pending.instrument,"asOfNs":pending.asof,"prediction":value,
            "targetFraction":target,"mode":identity["mode"],"ordersCreated":false});
        self.consumed.insert(pending.scope.clone(), pending.asof);
        self.pending.remove(id);
        self.accepted += 1;
        Ok(result)
    }
    pub fn metrics(&self) -> Value {
        json!({"prepared":self.prepared,"accepted":self.accepted,"expired":self.expired,"pending":self.pending.len()})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn release() -> Value {
        json!({"schemaVersion":1,"releaseId":"test-v1","runnerId":"gru_v1","model":{"sha256":"a".repeat(64)},
        "featureContract":{"names":["a","b"],"sequenceBars":2,"mean":[1,2],"scale":[2,4],"clip":[-8,8]},
        "runtime":{"maxQueueSize":2,"timeoutMs":2000,"maxInputAgeMs":10000},"policy":{"allowedModes":["shadow","sandbox"],"thresholdBps":12,"maxTargetFraction":0.5}})
    }
    fn request(id: &str, asof: u64) -> Value {
        json!({"requestId":id,"releaseId":"test-v1","modelHash":"a".repeat(64),"instrument":"BTC-USDT","mode":"shadow","normalized":false,"featureNames":["a","b"],"inputs":[[3,6],[100,-100]],"asOfNs":asof,"deadlineNs":12_000_000_000_u64})
    }
    fn result(r: &Value) -> Value {
        json!({"requestId":r["requestId"],"releaseId":r["releaseId"],"modelHash":r["modelHash"],"instrument":r["instrument"],"asOfNs":r["asOfNs"],"completedAtNs":10_500_000_000_u64,"prediction":0.002})
    }
    #[test]
    fn live_requests_require_an_explicitly_published_live_mode() {
        let mut input = request("live", 9_000_000_000);
        input["mode"] = json!("live");
        assert!(
            ModelGuard::new(release())
                .unwrap()
                .prepare(input.clone(), 10_000_000_000)
                .is_err()
        );
        let mut manifest = release();
        manifest["policy"]["allowedModes"] = json!(["live"]);
        assert!(
            ModelGuard::new(manifest)
                .unwrap()
                .prepare(input, 10_000_000_000)
                .is_ok()
        );
    }

    #[test]
    fn normalizes_and_clips_without_changing_feature_order() {
        let mut g = ModelGuard::new(release()).unwrap();
        let r = request("a", 9_000_000_000);
        let v = g.prepare(r, 10_000_000_000).unwrap();
        assert_eq!(v["workerRequest"]["inputs"], json!([[1., 1.], [8., -8.]]));
    }
    #[test]
    fn refuses_wrong_shape_order_and_boolean_values() {
        for (key, v) in [
            ("featureNames", json!(["b", "a"])),
            ("inputs", json!([[1, 2]])),
            ("inputs", json!([[true, 2], [3, 4]])),
            ("normalized", json!(true)),
        ] {
            let mut g = ModelGuard::new(release()).unwrap();
            let mut r = request("a", 9_000_000_000);
            r[key] = v;
            assert!(g.prepare(r, 10_000_000_000).is_err());
        }
    }
    #[test]
    fn rejects_stale_future_and_live_requests() {
        for (key, v) in [
            ("asOfNs", json!(11_000_000_000_u64)),
            ("asOfNs", json!(0)),
            ("deadlineNs", json!(10_000_000_000_u64)),
            ("mode", json!("live")),
            ("modelHash", json!("b".repeat(64))),
        ] {
            let mut g = ModelGuard::new(release()).unwrap();
            let mut r = request("a", 9_000_000_000);
            r[key] = v;
            assert!(g.prepare(r, 10_000_000_000).is_err());
        }
    }
    #[test]
    fn bounds_queue_and_rejects_duplicate_id() {
        let mut g = ModelGuard::new(release()).unwrap();
        g.prepare(request("a", 9_000_000_000), 10_000_000_000)
            .unwrap();
        assert!(
            g.prepare(request("a", 9_000_000_000), 10_000_000_000)
                .is_err()
        );
        g.prepare(request("b", 9_100_000_000), 10_000_000_000)
            .unwrap();
        assert!(
            g.prepare(request("c", 9_200_000_000), 10_000_000_000)
                .is_err()
        );
    }
    #[test]
    fn checks_identity_and_bounds_target_no_order_creation() {
        let mut g = ModelGuard::new(release()).unwrap();
        let r = request("a", 9_000_000_000);
        g.prepare(r.clone(), 10_000_000_000).unwrap();
        let mut p = result(&r);
        p["modelHash"] = json!("b".repeat(64));
        assert!(g.validate_prediction(&p, &r, 11_000_000_000).is_err());
        let v = g
            .validate_prediction(&result(&r), &r, 11_000_000_000)
            .unwrap();
        assert_eq!(v["targetFraction"], 0.5);
        assert_eq!(v["ordersCreated"], false);
        assert!(
            g.validate_prediction(&result(&r), &r, 11_000_000_000)
                .is_err()
        );
    }
    #[test]
    fn late_output_cannot_generate_a_target() {
        let mut g = ModelGuard::new(release()).unwrap();
        let r = request("a", 9_000_000_000);
        g.prepare(r.clone(), 10_000_000_000).unwrap();
        assert!(
            g.validate_prediction(&result(&r), &r, 12_000_000_000)
                .is_err()
        );
        assert_eq!(g.metrics()["pending"], 0);
    }
    #[test]
    fn out_of_order_completed_windows_are_discarded() {
        let mut g = ModelGuard::new(release()).unwrap();
        let a = request("a", 9_000_000_000);
        let b = request("b", 9_500_000_000);
        g.prepare(a.clone(), 10_000_000_000).unwrap();
        g.prepare(b.clone(), 10_000_000_000).unwrap();
        g.validate_prediction(&result(&b), &b, 11_000_000_000)
            .unwrap();
        assert!(
            g.validate_prediction(&result(&a), &a, 11_000_000_000)
                .is_err()
        );
    }
    #[test]
    fn clock_rollback_drops_pending_without_integer_underflow() {
        let mut g = ModelGuard::new(release()).unwrap();
        let r = request("rollback", 9_000_000_000);
        g.prepare(r.clone(), 10_000_000_000).unwrap();
        assert!(
            g.validate_prediction(&result(&r), &r, 8_000_000_000)
                .is_err()
        );
        assert_eq!(g.metrics()["pending"], 0);
    }
    #[test]
    fn rdt_binds_target_to_published_head_not_first_horizon() {
        for runner in ["rdt4quant_v1", "rdt4quant_cpu_v2"] {
            let mut manifest = release();
            let contract = manifest
                .as_object_mut()
                .unwrap()
                .remove("featureContract")
                .unwrap();
            manifest["runnerId"] = json!(runner);
            manifest["domainContracts"] = json!({"crypto:BTC-USDT":contract});
            manifest["outputContract"] = json!({"horizons":{"crypto":[15,60]},"horizonUnit":{"crypto":"minutes"},"selectedHeadByDomain":{"crypto":1}});
            let mut g = ModelGuard::new(manifest).unwrap();
            let mut r = request("rdt", 9_000_000_000);
            r["domain"] = json!("crypto");
            g.prepare(r.clone(), 10_000_000_000).unwrap();
            let mut p = result(&r);
            p["domain"] = json!("crypto");
            p["quantilesBps"] = json!([[-20, -10, 0], [10, 20, 30]]);
            p["horizon"] = json!(15);
            p["horizonUnit"] = json!("minutes");
            assert!(g.validate_prediction(&p, &r, 11_000_000_000).is_err());
            p["horizon"] = json!(60);
            p["prediction"] = json!((-10.0_f64 / 10_000.0).exp_m1());
            assert!(g.validate_prediction(&p, &r, 11_000_000_000).is_err());
            p["prediction"] = json!((20.0_f64 / 10_000.0).exp_m1());
            assert_eq!(
                g.validate_prediction(&p, &r, 11_000_000_000).unwrap()["targetFraction"],
                0.5
            );
        }
    }
}
