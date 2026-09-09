//! Read-side projection of the supervisor's immutable run directories.
//! No code here can issue a trading command or write into a run directory.
use crate::{ApiError, GatewayState};
use axum::{
    Json,
    extract::{Path as RoutePath, Query, State},
};
use bigdecimal::{BigDecimal, Zero};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_FILE: u64 = 8 * 1024 * 1024;
struct Cached {
    at: Instant,
    value: Arc<Value>,
}
pub struct LiveViews {
    root: Option<PathBuf>,
    cache: Mutex<HashMap<String, Cached>>,
}
impl LiveViews {
    pub fn disabled() -> Self {
        Self {
            root: None,
            cache: Mutex::new(HashMap::new()),
        }
    }
    pub fn open(root: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            root: Some(root.canonicalize()?),
            cache: Mutex::new(HashMap::new()),
        })
    }
    pub fn read(&self, group: &str, run: &str) -> Result<Arc<Value>, ApiError> {
        if !identifier(group) || !identifier(run) {
            return Err(ApiError::BadRequest("运行标识无效".into()));
        }
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| ApiError::Unavailable("运行数据连接尚未配置".into()))?;
        let key = format!("{group}/{run}");
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| ApiError::Unavailable("运行数据缓存暂不可用".into()))?;
        if let Some(value) = cache
            .get(&key)
            .filter(|entry| entry.at.elapsed() < Duration::from_millis(250))
        {
            return Ok(value.value.clone());
        }
        let path = root.join(run);
        let canonical = path
            .canonicalize()
            .map_err(|_| ApiError::NotFound("运行记录不存在".into()))?;
        if canonical.parent() != Some(root.as_path()) || canonical != path {
            return Err(ApiError::BadRequest("运行目录与所选记录不一致".into()));
        }
        let manifest = read_json(&path.join("manifest.json"))?
            .ok_or_else(|| ApiError::Unavailable("运行清单尚未同步".into()))?;
        let final_value = read_json(&path.join("final.json"))?;
        let ended = final_value.is_some();
        let status = if let Some(value) = final_value {
            value
        } else {
            read_json(&path.join("status.json"))?
                .ok_or_else(|| ApiError::Unavailable("运行状态尚未同步".into()))?
        };
        let view = read_json(&path.join("view.json"))?;
        let value = Arc::new(project(
            group,
            run,
            &manifest,
            &status,
            view.as_ref(),
            ended,
        )?);
        if cache.len() >= 32 {
            let oldest = cache
                .iter()
                .min_by_key(|(_, v)| v.at)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                cache.remove(&key);
            }
        }
        cache.insert(
            key,
            Cached {
                at: Instant::now(),
                value: value.clone(),
            },
        );
        Ok(value)
    }
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
}
fn read_json(path: &Path) -> Result<Option<Value>, ApiError> {
    let metadata = match path.symlink_metadata() {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ApiError::Unavailable("运行数据文件暂不可读".into())),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > MAX_FILE {
        return Err(ApiError::Unavailable("运行数据文件格式或大小异常".into()));
    }
    let file = std::fs::File::open(path)
        .map_err(|_| ApiError::Unavailable("运行数据文件暂不可读".into()))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_FILE + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ApiError::Unavailable("运行数据读取未完成".into()))?;
    if bytes.len() as u64 > MAX_FILE {
        return Err(ApiError::Unavailable("运行数据超过单次读取范围".into()));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| ApiError::Unavailable("运行快照正在更新".into()))
}
fn decimal(value: &Value) -> Option<BigDecimal> {
    match value {
        Value::String(value) => BigDecimal::from_str(value).ok(),
        Value::Number(value) => BigDecimal::from_str(&value.to_string()).ok(),
        _ => None,
    }
}
fn amount(value: Option<BigDecimal>) -> Value {
    value
        .map(|v| Value::String(v.normalized().to_string()))
        .unwrap_or(Value::Null)
}
fn timestamp(value: &Value) -> Option<i64> {
    value
        .as_str()
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .and_then(|v| v.timestamp_nanos_opt())
}
fn field_rows(rows: &Value, fields: &[&str]) -> Value {
    match rows.as_array() {
        Some(rows) => Value::Array(
            rows.iter()
                .take(5000)
                .map(|row| {
                    let mut values: serde_json::Map<String, Value> = fields
                        .iter()
                        .filter_map(|key| row.get(*key).map(|v| ((*key).to_string(), v.clone())))
                        .collect();
                    if let Some(raw) = values
                        .get("instrument")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        && let Some(canonical) = raw.strip_suffix(".OKX")
                    {
                        values.insert("instrumentSource".into(), json!(raw));
                        values.insert("instrument".into(), json!(canonical));
                    }
                    Value::Object(values)
                })
                .collect(),
        ),
        None => Value::Null,
    }
}
fn project(
    group: &str,
    run: &str,
    manifest: &Value,
    status: &Value,
    view: Option<&Value>,
    ended: bool,
) -> Result<Value, ApiError> {
    if manifest["run_id"] != run || manifest["group_id"] != group || manifest["mode"] != "live" {
        return Err(ApiError::Conflict("运行清单与所选实盘记录不一致".into()));
    }
    let account = manifest["account_id"]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| ApiError::Unavailable("运行记录缺少执行账户".into()))?;
    for (key, expected) in [("run_id", run), ("group_id", group), ("profileId", account)] {
        if status
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|v| v != expected)
        {
            return Err(ApiError::Conflict("状态快照与运行上下文不一致".into()));
        }
    }
    if view.is_some_and(|v| v["runId"] != run || v["groupId"] != group) {
        return Err(ApiError::Conflict("持仓快照与运行上下文不一致".into()));
    }
    let empty = Value::Null;
    let view = view.unwrap_or(&empty);
    let nav = decimal(&status["nav_usdt"]);
    let pnl = decimal(&status["pnl_usdt"]);
    let capital = decimal(&manifest["capital_usdt"])
        .or_else(|| nav.as_ref().zip(pnl.as_ref()).map(|(nav, pnl)| nav - pnl));
    let return_pct = pnl
        .as_ref()
        .zip(capital.as_ref())
        .filter(|(_, capital)| **capital > BigDecimal::zero())
        .map(|(pnl, capital)| ((pnl * BigDecimal::from(100)) / capital).round(8));
    let observed = timestamp(&status["observed_at"]);
    let view_observed = timestamp(&view["observed_at"]);
    let aligned = observed
        .zip(view_observed)
        .is_some_and(|(a, b)| a.abs_diff(b) <= 5_000_000_000);
    let positions = if aligned {
        field_rows(
            &view["positions"],
            &[
                "strategy",
                "instrument",
                "side",
                "quantity",
                "markPrice",
                "notional",
                "margin",
                "averagePrice",
                "unrealizedPnl",
            ],
        )
    } else {
        Value::Null
    };
    let mut universe = field_rows(
        &status["inventory"]["pairs"],
        &[
            "instrument",
            "base",
            "weight",
            "sector",
            "targetQuantity",
            "initialPosition",
            "bid",
            "ask",
            "tickSize",
            "lotSize",
            "minSize",
            "makerFeeBps",
            "takerFeeBps",
            "volume24hQuote",
        ],
    );
    if let Some(universe) = universe.as_array_mut() {
        for row in universe {
            if let Some(position) = positions
                .as_array()
                .into_iter()
                .flatten()
                .find(|position| position["instrument"] == row["instrument"])
            {
                for key in ["quantity", "notional", "unrealizedPnl"] {
                    if let Some(value) = position.get(key) {
                        row[key] = value.clone();
                    }
                }
            }
        }
    }
    let state = status["tradingState"]
        .as_str()
        .or_else(|| status["trading_state"].as_str())
        .unwrap_or("UNKNOWN");
    let state = match state {
        "ACTIVE" | "RUNNING" => "running",
        "STOPPED" => "stopped",
        "HALTED" => "halted",
        "REDUCING" => "reducing",
        "RECOVERING" => "recovering",
        "ERROR" | "FAULTED" | "FAILED" => "error",
        _ => "unknown",
    };
    Ok(
        json!({"id":group,"name":group,"runId":run,"accountId":account,"mode":"live","status":if ended&&state=="running"{"stopped"}else{state},
            "observedAt":observed.map(|v|v as f64/1e9),"observed_at_ns":observed.map(|v|v.to_string()),"view_observed_at_ns":view_observed.map(|v|v.to_string()),"ended":ended,
            "signalAsOf":manifest["signal_as_of"],"signalVersion":manifest["signals_sha256"],"executionVersion":manifest["worker_sha256"],"description":manifest["strategy_description"],
            "metrics":{"nav":amount(nav),"pnl":amount(pnl),"capital":amount(capital),"fees":amount(decimal(&status["fees_usdt"])),"returnPct":amount(return_pct),"maxDrawdownPct":amount(decimal(&status["max_drawdown"]).map(|v|v*BigDecimal::from(100)))},
            "ordersTotal":status["ordersTotal"],"fillsTotal":status["fillsTotal"],"positions":positions,
            "orders":if aligned{field_rows(&view["orders"],&["id","instrument","clientOrderId","venueOrderId","side","type","quantity","price","status","filledQty","filledQuantity","leavesQuantity","averagePrice","time","strategy"])}else{Value::Null},
            "fills":if aligned{field_rows(&view["fills"],&["id","tradeId","orderId","instrument","clientOrderId","venueOrderId","side","quantity","price","fee","feeCurrency","liquiditySide","source","time","strategy"])}else{Value::Null},
            "universe":universe,"execution":status["execution"],"connections":status["connected"],"model":status["model"],
            "sources":{"accounting":observed.is_some(),"positions":aligned&&view["positions"].is_array(),"orders":aligned&&view["orders"].is_array(),"fills":aligned&&view["fills"].is_array(),"aligned":aligned},
            "provenance":{"source":"nautilus.run_files","accounting_basis":"reported_run_mark_to_market","calculation_version":"read-view-decimal-v1","valuation_at_ns":observed.map(|v|v.to_string())}
        }),
    )
}

#[derive(Default, Deserialize)]
pub struct ObservationQuery {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    include: Option<String>,
}
pub async fn observation(
    State(state): State<GatewayState>,
    RoutePath(group): RoutePath<String>,
    Query(query): Query<ObservationQuery>,
) -> Result<Json<Value>, ApiError> {
    if !identifier(&group) {
        return Err(ApiError::BadRequest("策略组标识无效".into()));
    }
    let include = query
        .include
        .as_deref()
        .unwrap_or("universe,execution")
        .split(',')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if include.iter().any(|name| {
        !matches!(
            *name,
            "universe" | "execution" | "positions" | "orders" | "fills" | "model"
        )
    }) {
        return Err(ApiError::BadRequest("数据视图名称无效".into()));
    }
    let mut name = None;
    let run = if let Some(run) = query.run_id.filter(|v| !v.is_empty()) {
        run
    } else {
        let socket = state
            .control
            .socket
            .as_ref()
            .ok_or_else(|| ApiError::Unavailable("运行目录服务尚未连接".into()))?;
        let status =
            crate::socket_request(socket, json!({"command":"status","groupId":group})).await?;
        let registered = status["result"]["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|value| value["groupId"] == group)
            .ok_or_else(|| ApiError::NotFound("策略组不存在".into()))?;
        name = registered.get("name").cloned();
        registered["runId"]
            .as_str()
            .or_else(|| registered["runs"][0]["runId"].as_str())
            .ok_or_else(|| ApiError::NotFound("该策略组尚无运行记录".into()))?
            .to_string()
    };
    let reader = state.live_views.clone();
    let requested_group = group.clone();
    let snapshot = tokio::task::spawn_blocking(move || reader.read(&requested_group, &run))
        .await
        .map_err(|e| ApiError::Internal(e.into()))??;
    let mut result = snapshot
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::Unavailable("运行快照格式异常".into()))?;
    if let Some(name) = name {
        result.insert("name".into(), name);
    }
    for key in [
        "universe",
        "execution",
        "positions",
        "orders",
        "fills",
        "model",
    ] {
        if !include.contains(&key) {
            result.remove(key);
        }
    }
    let age = snapshot["observed_at_ns"]
        .as_str()
        .and_then(|v| v.parse::<i64>().ok())
        .map(|ns| crate::now_ns().saturating_sub(ns).max(0) / 1_000_000);
    let fresh = snapshot["ended"] == true || age.is_some_and(|ms| ms <= 5000);
    result.insert("fresh".into(), json!(fresh));
    result.insert("age_ms".into(), json!(age));
    if !fresh {
        result.insert("status".into(), json!("stale"));
    }
    Ok(Json(Value::Object(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixtures() -> (Value, Value, Value) {
        (
            json!({"mode":"live","account_id":"a","group_id":"g","run_id":"r","capital_usdt":"200"}),
            json!({"run_id":"r","group_id":"g","profileId":"a","observed_at":"2026-09-09T01:00:00Z","nav_usdt":"201.000000000000000001","pnl_usdt":"1.000000000000000001","max_drawdown":"-0.0032","tradingState":"ACTIVE"}),
            json!({"runId":"r","groupId":"g","observed_at":"2026-09-09T01:00:00Z","positions":[],"orders":[],"fills":[]}),
        )
    }
    #[test]
    fn decimals_and_percentage_units_are_explicit() {
        let (m, s, v) = fixtures();
        let result = project("g", "r", &m, &s, Some(&v), false).unwrap();
        assert_eq!(result["metrics"]["nav"], "201.000000000000000001");
        assert_eq!(result["metrics"]["returnPct"], "0.5");
        assert_eq!(result["metrics"]["maxDrawdownPct"], "-0.32");
        assert!(result["metrics"]["fees"].is_null());
    }
    #[test]
    fn missing_and_misaligned_positions_are_unknown_not_zero() {
        let (m, s, mut v) = fixtures();
        v["observed_at"] = json!("2026-09-09T00:00:00Z");
        let result = project("g", "r", &m, &s, Some(&v), false).unwrap();
        assert!(result["positions"].is_null());
        assert_eq!(result["sources"]["positions"], false);
    }
    #[test]
    fn context_mismatch_and_traversal_are_rejected() {
        let (m, mut s, v) = fixtures();
        s["profileId"] = json!("other");
        assert!(project("g", "r", &m, &s, Some(&v), false).is_err());
        assert!(!identifier("../r"));
        assert!(!identifier("r/x"));
    }
}
