#![forbid(unsafe_code)]

use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result, bail};
use montlok_contracts::v2;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub mod outbox;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Checkpoint {
    pub offset: u64,
    #[serde(default)]
    pub streams: HashMap<String, u64>,
}

impl Checkpoint {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).context("parse adapter checkpoint")?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("tmp");
        let mut bytes = serde_json::to_vec(self)?;
        bytes.push(b'\n');
        std::fs::write(&temporary, bytes)?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    pub fn next_sequence(&mut self, stream: &str, supplied: Option<u64>) -> Result<u64> {
        let current = self.streams.get(stream).copied().unwrap_or(0);
        let next = supplied.unwrap_or(current + 1);
        if next != current + 1 {
            bail!(
                "source sequence gap for {stream}: expected {}, received {next}",
                current + 1
            );
        }
        self.streams.insert(stream.to_string(), next);
        Ok(next)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PersistedEvent {
    #[serde(alias = "type", alias = "eventType")]
    pub event_type: String,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default, alias = "sequence", alias = "streamSeq")]
    pub stream_seq: Option<u64>,
    #[serde(default, alias = "eventId")]
    pub event_id: Option<String>,
    #[serde(default, alias = "occurredAtNs", alias = "ts_event", alias = "time")]
    pub occurred_at_ns: Option<i64>,
    #[serde(default, alias = "receivedAtNs", alias = "ts_init")]
    pub received_at_ns: Option<i64>,
    #[serde(default, alias = "accountId")]
    pub account_id: Option<String>,
    #[serde(default, alias = "groupId", alias = "strategyGroupId")]
    pub strategy_group_id: Option<String>,
    #[serde(default, alias = "runId")]
    pub run_id: Option<String>,
    #[serde(default, alias = "instrumentId", alias = "instrument")]
    pub instrument_id: Option<String>,
    #[serde(default, alias = "correlationId")]
    pub correlation_id: Option<String>,
    #[serde(default, alias = "causationId")]
    pub causation_id: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}

pub fn map_event(
    record: PersistedEvent,
    checkpoint: &mut Checkpoint,
    now_ns: i64,
) -> Result<v2::EventEnvelope> {
    let source_payload_json = serde_json::to_vec(&record)?;
    let occurred_at_ns = record
        .occurred_at_ns
        .ok_or_else(|| anyhow::anyhow!("persisted event has no occurrence timestamp"))?;
    if occurred_at_ns <= 0 {
        bail!("occurrence timestamp must be positive nanoseconds");
    }
    let event_type = event_type(&record.event_type)?;
    let stream = record.stream.unwrap_or_else(|| {
        if let Some(run) = &record.run_id {
            format!("run.{run}")
        } else if let Some(instrument) = &record.instrument_id {
            format!("market.{instrument}")
        } else {
            "system.events".into()
        }
    });
    let stream_seq = checkpoint.next_sequence(&stream, record.stream_seq)?;
    let event_id = record
        .event_id
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    let correlation_id = record.correlation_id.unwrap_or_default();
    let mut fields = record.fields;
    if let Value::Object(nested) = record.payload {
        fields.extend(nested);
    }
    for (old, new) in [
        ("orderId", "clientOrderId"),
        ("id", "tradeId"),
        ("reason", "rejectReason"),
    ] {
        if !fields.contains_key(new)
            && let Some(value) = fields.get(old).cloned()
        {
            fields.insert(new.into(), value);
        }
    }
    fields
        .entry("venueTimeNs")
        .or_insert(Value::from(occurred_at_ns));
    let payload = map_payload(event_type, Value::Object(fields))?;
    Ok(v2::EventEnvelope {
        schema_version: 2,
        stream,
        stream_seq,
        snapshot_seq: 0,
        event_id,
        event_type: event_type as i32,
        occurred_at_ns,
        received_at_ns: record.received_at_ns.unwrap_or(now_ns),
        account_id: record.account_id.unwrap_or_default(),
        strategy_group_id: record.strategy_group_id.unwrap_or_default(),
        run_id: record.run_id.unwrap_or_default(),
        instrument_id: record.instrument_id.unwrap_or_default(),
        correlation_id,
        causation_id: record.causation_id.unwrap_or_default(),
        source: record
            .source
            .unwrap_or_else(|| "nautilus.persisted_log".into()),
        payload,
        source_payload_json,
    })
}

fn event_type(name: &str) -> Result<v2::EventType> {
    let normalized = name.trim().to_ascii_uppercase();
    Ok(match normalized.as_str() {
        "RUN_STATE_CHANGED" | "RUN_STATE" => v2::EventType::RunStateChanged,
        "MARKET_QUOTE" | "QUOTE" => v2::EventType::MarketQuote,
        "ORDER_BOOK_UPDATED" | "ORDER_BOOK" | "BOOK" => v2::EventType::OrderBookUpdated,
        "MODEL_INFERENCE_COMPLETED" | "INFERENCE" => v2::EventType::ModelInferenceCompleted,
        "SIGNAL_GENERATED" | "SIGNAL" => v2::EventType::SignalGenerated,
        "TARGET_POSITION_CHANGED" | "TARGET" => v2::EventType::TargetPositionChanged,
        "RISK_DECISION" => v2::EventType::RiskDecision,
        "ORDER_SUBMITTED" | "SUBMITTED" => v2::EventType::OrderSubmitted,
        "ORDER_ACCEPTED" | "ACCEPTED" => v2::EventType::OrderAccepted,
        "ORDER_REJECTED" | "REJECTED" => v2::EventType::OrderRejected,
        "ORDER_CANCEL_REQUESTED" | "CANCEL_REQUESTED" => v2::EventType::OrderCancelRequested,
        "ORDER_CANCELED" | "ORDER_CANCELLED" => v2::EventType::OrderCanceled,
        "ROUTE_UPDATED" | "ROUTE" => v2::EventType::RouteUpdated,
        "FILL_RECEIVED" | "FILL" | "TRADE" => v2::EventType::FillReceived,
        "POSITION_UPDATED" | "POSITION" => v2::EventType::PositionUpdated,
        "PNL_UPDATED" | "PNL" => v2::EventType::PnlUpdated,
        "ALERT_RAISED" => v2::EventType::AlertRaised,
        "ALERT_CLEARED" => v2::EventType::AlertCleared,
        "OPERATION_RECEIPT_UPDATED" | "OPERATION_RECEIPT" => v2::EventType::OperationReceiptUpdated,
        _ => bail!("unsupported persisted event type {name}"),
    })
}

fn decimal(payload: &Value, key: &str) -> Option<v2::DecimalValue> {
    payload
        .get(key)
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
        .map(|value| v2::DecimalValue { value })
}

fn string(payload: &Value, key: &str) -> String {
    payload
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn integer(payload: &Value, key: &str) -> i64 {
    payload.get(key).and_then(Value::as_i64).unwrap_or_default()
}

fn map_payload(
    event_type: v2::EventType,
    payload: Value,
) -> Result<Option<v2::event_envelope::Payload>> {
    use v2::event_envelope::Payload;
    Ok(Some(match event_type {
        v2::EventType::RunStateChanged => Payload::RunStateChanged(v2::RunStateChanged {
            previous: run_state(&string(&payload, "previous")) as i32,
            current: run_state(&string(&payload, "current")) as i32,
            reason: string(&payload, "reason"),
            node_id: string(&payload, "nodeId"),
        }),
        v2::EventType::MarketQuote => Payload::MarketQuote(v2::MarketQuote {
            bid_price: decimal(&payload, "bidPrice"),
            bid_size: decimal(&payload, "bidSize"),
            ask_price: decimal(&payload, "askPrice"),
            ask_size: decimal(&payload, "askSize"),
            last_price: decimal(&payload, "lastPrice"),
            venue_time_ns: integer(&payload, "venueTimeNs"),
        }),
        v2::EventType::ModelInferenceCompleted => {
            Payload::ModelInferenceCompleted(v2::ModelInferenceCompleted {
                model_inference_id: string(&payload, "modelInferenceId"),
                model_release_id: string(&payload, "modelReleaseId"),
                model_hash: string(&payload, "modelHash"),
                feature_version: string(&payload, "featureVersion"),
                prediction: decimal(&payload, "prediction"),
                latency_ns: payload
                    .get("latencyNs")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                input_as_of_ns: integer(&payload, "inputAsOfNs"),
            })
        }
        v2::EventType::SignalGenerated => Payload::SignalGenerated(v2::SignalGenerated {
            signal_id: string(&payload, "signalId"),
            model_inference_id: string(&payload, "modelInferenceId"),
            signal_version: string(&payload, "signalVersion"),
            value: decimal(&payload, "value"),
            confidence: decimal(&payload, "confidence"),
        }),
        v2::EventType::TargetPositionChanged => {
            Payload::TargetPositionChanged(v2::TargetPositionChanged {
                decision_id: string(&payload, "decisionId"),
                signal_id: string(&payload, "signalId"),
                target_quantity: decimal(&payload, "targetQuantity"),
                target_weight: decimal(&payload, "targetWeight"),
                previous_quantity: decimal(&payload, "previousQuantity"),
            })
        }
        v2::EventType::RiskDecision => Payload::RiskDecision(v2::RiskDecision {
            risk_decision_id: string(&payload, "riskDecisionId"),
            decision_id: string(&payload, "decisionId"),
            outcome: risk_outcome(&string(&payload, "outcome")) as i32,
            rule_id: string(&payload, "ruleId"),
            message: string(&payload, "message"),
            requested_quantity: decimal(&payload, "requestedQuantity"),
            approved_quantity: decimal(&payload, "approvedQuantity"),
        }),
        v2::EventType::OrderSubmitted
        | v2::EventType::OrderAccepted
        | v2::EventType::OrderRejected
        | v2::EventType::OrderCancelRequested
        | v2::EventType::OrderCanceled => Payload::OrderEvent(v2::OrderEvent {
            operation_id: string(&payload, "operationId"),
            risk_decision_id: string(&payload, "riskDecisionId"),
            client_order_id: string(&payload, "clientOrderId"),
            venue_order_id: string(&payload, "venueOrderId"),
            side: side(&string(&payload, "side")) as i32,
            order_type: string(&payload, "orderType"),
            quantity: decimal(&payload, "quantity"),
            price: decimal(&payload, "price"),
            state: order_state(&string(&payload, "state")) as i32,
            reject_code: string(&payload, "rejectCode"),
            reject_reason: string(&payload, "rejectReason"),
        }),
        v2::EventType::RouteUpdated => Payload::RouteUpdated(v2::RouteUpdated {
            route_id: string(&payload, "routeId"),
            client_order_id: string(&payload, "clientOrderId"),
            venue_order_id: string(&payload, "venueOrderId"),
            venue: string(&payload, "venue"),
            state: order_state(&string(&payload, "state")) as i32,
            venue_sequence: payload
                .get("venueSequence")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        }),
        v2::EventType::FillReceived => Payload::FillReceived(v2::FillReceived {
            trade_id: string(&payload, "tradeId"),
            route_id: string(&payload, "routeId"),
            venue_order_id: string(&payload, "venueOrderId"),
            side: side(&string(&payload, "side")) as i32,
            quantity: decimal(&payload, "quantity"),
            price: decimal(&payload, "price"),
            fee: decimal(&payload, "fee"),
            fee_currency: string(&payload, "feeCurrency"),
            liquidity: string(&payload, "liquidity"),
            venue_time_ns: integer(&payload, "venueTimeNs"),
        }),
        v2::EventType::PositionUpdated => Payload::PositionUpdated(v2::PositionUpdated {
            position_event_id: string(&payload, "positionEventId"),
            trade_id: string(&payload, "tradeId"),
            quantity: decimal(&payload, "quantity"),
            average_price: decimal(&payload, "averagePrice"),
            market_value: decimal(&payload, "marketValue"),
            target_quantity: decimal(&payload, "targetQuantity"),
            target_weight: decimal(&payload, "targetWeight"),
            actual_weight: decimal(&payload, "actualWeight"),
        }),
        v2::EventType::PnlUpdated => Payload::PnlUpdated(v2::PnlUpdated {
            pnl_event_id: string(&payload, "pnlEventId"),
            position_event_id: string(&payload, "positionEventId"),
            nav: decimal(&payload, "nav"),
            realized_pnl: decimal(&payload, "realizedPnl"),
            unrealized_pnl: decimal(&payload, "unrealizedPnl"),
            fees: decimal(&payload, "fees"),
            drawdown_percent: decimal(&payload, "drawdownPercent"),
            valuation_at_ns: integer(&payload, "valuationAtNs"),
            market_source: string(&payload, "marketSource"),
            accounting_basis: string(&payload, "accountingBasis"),
            calculation_version: string(&payload, "calculationVersion"),
            source_event_seq: payload
                .get("sourceEventSeq")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        }),
        v2::EventType::AlertRaised | v2::EventType::AlertCleared => {
            Payload::AlertChanged(v2::AlertChanged {
                alert_id: string(&payload, "alertId"),
                severity: string(&payload, "severity"),
                code: string(&payload, "code"),
                summary: string(&payload, "summary"),
                detail: string(&payload, "detail"),
                active: event_type == v2::EventType::AlertRaised,
            })
        }
        v2::EventType::OperationReceiptUpdated => {
            Payload::OperationReceiptUpdated(v2::OperationReceiptUpdated {
                operation_id: string(&payload, "operationId"),
                status: string(&payload, "status"),
                action: string(&payload, "action"),
                reason: string(&payload, "reason"),
                expires_at_ns: integer(&payload, "expiresAtNs"),
            })
        }
        v2::EventType::OrderBookUpdated => Payload::OrderBookUpdated(v2::OrderBookUpdated {
            bids: book_levels(&payload, "bids")?,
            asks: book_levels(&payload, "asks")?,
            venue_sequence: payload
                .get("venueSequence")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            venue_time_ns: integer(&payload, "venueTimeNs"),
        }),
        v2::EventType::Unspecified => return Ok(None),
    }))
}

fn book_levels(payload: &Value, key: &str) -> Result<Vec<v2::BookLevel>> {
    let rows = payload
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("book has no {key}"))?;
    rows.iter()
        .map(|row| {
            let object = if let Some(array) = row.as_array() {
                if array.len() < 2 {
                    bail!("book level needs price and size");
                }
                serde_json::json!({"price":array[0],"size":array[1],"count":array.get(3)})
            } else {
                row.clone()
            };
            let price = decimal(&object, "price")
                .ok_or_else(|| anyhow::anyhow!("book price is missing"))?;
            let size =
                decimal(&object, "size").ok_or_else(|| anyhow::anyhow!("book size is missing"))?;
            Ok(v2::BookLevel {
                price: Some(price),
                size: Some(size),
                count: object.get("count").and_then(Value::as_u64).unwrap_or(0) as u32,
            })
        })
        .collect()
}

fn run_state(value: &str) -> v2::RunState {
    match value.to_ascii_uppercase().as_str() {
        "PREPARING" => v2::RunState::RunPreparing,
        "RUNNING" | "ACTIVE" => v2::RunState::RunRunning,
        "PAUSED" | "HALTED" => v2::RunState::RunPaused,
        "REDUCING" => v2::RunState::RunReducing,
        "STOPPING" => v2::RunState::RunStopping,
        "STOPPED" | "COMPLETED" => v2::RunState::RunStopped,
        "FAILED" | "ERROR" => v2::RunState::RunFailed,
        _ => v2::RunState::Unspecified,
    }
}

fn side(value: &str) -> v2::Side {
    if value.eq_ignore_ascii_case("BUY") {
        v2::Side::Buy
    } else if value.eq_ignore_ascii_case("SELL") {
        v2::Side::Sell
    } else {
        v2::Side::Unspecified
    }
}
fn risk_outcome(value: &str) -> v2::RiskOutcome {
    match value.to_ascii_uppercase().as_str() {
        "ACCEPTED" => v2::RiskOutcome::RiskAccepted,
        "CLIPPED" => v2::RiskOutcome::RiskClipped,
        "REJECTED" => v2::RiskOutcome::RiskRejected,
        _ => v2::RiskOutcome::Unspecified,
    }
}
fn order_state(value: &str) -> v2::OrderState {
    match value.to_ascii_uppercase().as_str() {
        "PENDING_SUBMIT" | "SUBMITTED" => v2::OrderState::OrderPendingSubmit,
        "ACCEPTED" => v2::OrderState::OrderAcceptedByVenue,
        "PARTIALLY_FILLED" => v2::OrderState::OrderPartiallyFilled,
        "FILLED" => v2::OrderState::OrderFilled,
        "PENDING_CANCEL" => v2::OrderState::OrderPendingCancel,
        "CANCELED" | "CANCELLED" => v2::OrderState::OrderCanceledByVenue,
        "REJECTED" => v2::OrderState::OrderRejectedByVenue,
        "EXPIRED" => v2::OrderState::OrderExpired,
        _ => v2::OrderState::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn checkpoint_is_atomic_and_sequences_are_per_stream() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("adapter.json");
        let mut checkpoint = Checkpoint::default();
        assert_eq!(checkpoint.next_sequence("orders", None).unwrap(), 1);
        assert_eq!(checkpoint.next_sequence("quotes", None).unwrap(), 1);
        assert_eq!(checkpoint.next_sequence("orders", Some(2)).unwrap(), 2);
        checkpoint.offset = 42;
        checkpoint.save(&path).unwrap();
        assert_eq!(Checkpoint::load(&path).unwrap().offset, 42);
    }

    #[test]
    fn order_identity_and_decimal_values_are_preserved() {
        let record: PersistedEvent = serde_json::from_value(serde_json::json!({
            "type": "ORDER_ACCEPTED", "runId": "run-a", "instrument": "BTC-USDT",
            "time": 1800000000000000000i64,
            "correlationId": "correlation-a", "payload": {
                "clientOrderId": "client-a", "venueOrderId": "venue-a", "side": "BUY",
                "quantity": "0.00001", "price": "80000.1", "state": "ACCEPTED"
            }
        }))
        .unwrap();
        let event = map_event(record, &mut Checkpoint::default(), 100).unwrap();
        assert_eq!(event.stream, "run.run-a");
        assert_eq!(event.stream_seq, 1);
        let Some(v2::event_envelope::Payload::OrderEvent(order)) = event.payload else {
            panic!("order payload");
        };
        assert_eq!(order.client_order_id, "client-a");
        assert_eq!(order.quantity.unwrap().value, "0.00001");
    }
}
