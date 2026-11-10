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

//! WebSocket message dispatch for the OKX execution client.
//!
//! Routes incoming [`OKXWsMessage`] variants to the appropriate parsing and
//! event emission paths. Tracked orders (submitted through this client) produce
//! proper order events; untracked orders fall back to execution reports for
//! downstream reconciliation.

use std::{collections::VecDeque, fmt::Display, hash::Hash, sync::Mutex};

use ahash::AHashMap;
use dashmap::DashMap;
use nautilus_core::{AtomicMap, MUTEX_POISONED, UUID4, UnixNanos, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType},
    events::{OrderAccepted, OrderEventAny, OrderFilled, OrderRejected},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::TRIGGERABLE_ORDER_TYPES,
    reports::FillReport,
    types::{Currency, Money, Quantity},
};
use ustr::Ustr;

use crate::{
    common::{
        consts::{
            OKX_FIELD_CLORDID, OKX_FIELD_SCODE, OKX_FIELD_SMSG, OKX_FIELD_SUBCODE,
            OKX_POST_ONLY_CANCEL_REASON, OKX_POST_ONLY_CANCEL_SOURCE, OKX_SUCCESS_CODE,
        },
        enums::{OKXOrderStatus, OKXOrderType},
        parse::{
            is_market_price, parse_client_order_id, parse_millisecond_timestamp, parse_price,
            parse_quantity,
        },
    },
    http::models::{OKXAccount, OKXCancelAlgoOrderResponse, OKXPosition, OKXSpreadOrder},
    websocket::{
        client::{
            OKXPendingResponseHandle, OKXWebSocketQueue, OKXWebSocketQueueFailureGuard,
            OKXWebSocketQueueFailureKind,
        },
        enums::OKXWsOperation,
        handler::{is_post_only_auto_cancel, is_unfilled_rpi_cancel},
        messages::{ExecutionReport, OKXOrderMsg, OKXWsMessage},
        parse::{
            OrderStateSnapshot, ParsedOrderEvent, parse_algo_order_msg,
            parse_fee_fill_cache_values, parse_order_event, parse_order_msg_with_previous,
            parse_spread_order_event, parse_spread_order_msg_with_previous,
        },
        replay_cache::{
            BoundedExecutionReplayCache, ExecutionReplayCache, LegacyExecutionReplayCache,
            ReplayCacheCapacityError,
        },
    },
};

/// Maximum entries held by the dedup sets before the oldest is evicted.
const DEDUP_CAPACITY: usize = 10_000;

/// Terminal execution-consumer failure which must stop the owning WebSocket connection.
#[derive(Debug)]
pub(crate) enum ExecutionDispatchError {
    ReplayCapacity(ReplayCacheCapacityError),
    Conversion(String),
    PendingAmbiguous(String),
    ReconnectGap,
}

impl ExecutionDispatchError {
    fn conversion(context: impl Into<String>) -> Self {
        Self::Conversion(context.into())
    }

    fn pending_ambiguous(context: impl Into<String>) -> Self {
        Self::PendingAmbiguous(context.into())
    }
}

impl From<ReplayCacheCapacityError> for ExecutionDispatchError {
    fn from(error: ReplayCacheCapacityError) -> Self {
        Self::ReplayCapacity(error)
    }
}

impl Display for ExecutionDispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplayCapacity(error) => Display::fmt(error, f),
            Self::Conversion(context) => write!(f, "OKX execution conversion failed: {context}"),
            Self::PendingAmbiguous(context) => {
                write!(f, "OKX pending operation became ambiguous: {context}")
            }
            Self::ReconnectGap => write!(
                f,
                "OKX execution WebSocket reconnected after an unobserved venue-event gap"
            ),
        }
    }
}

impl std::error::Error for ExecutionDispatchError {}

/// Latches the shared queue/runner fail-stop domain for a production dispatch failure.
pub(crate) fn halt_on_execution_dispatch_error(
    queue_failure: &OKXWebSocketQueueFailureGuard,
    error: &ExecutionDispatchError,
    consumer: &str,
) {
    let (queue, kind) = match error {
        ExecutionDispatchError::ReplayCapacity(_) => (
            OKXWebSocketQueue::Output,
            OKXWebSocketQueueFailureKind::Full,
        ),
        ExecutionDispatchError::Conversion(_) => (
            OKXWebSocketQueue::Output,
            OKXWebSocketQueueFailureKind::Conversion,
        ),
        ExecutionDispatchError::PendingAmbiguous(_) => (
            OKXWebSocketQueue::PendingOperations,
            OKXWebSocketQueueFailureKind::PendingAmbiguous,
        ),
        ExecutionDispatchError::ReconnectGap => (
            OKXWebSocketQueue::Output,
            OKXWebSocketQueueFailureKind::ReconnectGap,
        ),
    };
    log::error!("{error}; halting the OKX {consumer} execution consumer");
    queue_failure.fail(queue, kind);
}

/// Bounded deduplication set with FIFO eviction.
///
/// Insertions are tagged with a sequence number so a stale marker left
/// behind by `remove` or by re-insertion of the same key cannot evict the
/// live entry when it reaches the front of the queue.
#[derive(Debug)]
pub struct BoundedDedup<K> {
    inner: Mutex<BoundedDedupInner<K>>,
    capacity: usize,
}

#[derive(Debug)]
struct BoundedDedupInner<K> {
    set: AHashMap<K, u64>,
    queue: VecDeque<(K, u64)>,
    next_seq: u64,
}

impl<K> BoundedDedup<K>
where
    K: Eq + Hash + Clone,
{
    /// Creates a new dedup set with the given maximum capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(BoundedDedupInner {
                set: AHashMap::with_capacity(capacity),
                queue: VecDeque::with_capacity(capacity),
                next_seq: 0,
            }),
            capacity,
        }
    }

    /// Returns `true` if the key is currently present.
    #[allow(clippy::missing_panics_doc, reason = "mutex poisoning is not expected")]
    pub fn contains(&self, key: &K) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .set
            .contains_key(key)
    }

    /// Inserts the key. Returns `true` when the key was already present
    /// (duplicate) and `false` when this call was the first insertion.
    #[allow(clippy::missing_panics_doc, reason = "mutex poisoning is not expected")]
    pub fn check_and_insert(&self, key: K) -> bool {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED);
        if inner.set.contains_key(&key) {
            return true;
        }
        let seq = inner.next_seq;
        inner.next_seq = inner.next_seq.wrapping_add(1);
        inner.set.insert(key.clone(), seq);
        inner.queue.push_back((key, seq));
        while inner.queue.len() > self.capacity
            && let Some((old_key, old_seq)) = inner.queue.pop_front()
        {
            // Skip if a fresher insertion has superseded this marker.
            if inner.set.get(&old_key) == Some(&old_seq) {
                inner.set.remove(&old_key);
            }
        }
        false
    }

    /// Inserts the key without reporting whether it was already present.
    pub fn insert(&self, key: K) {
        let _ = self.check_and_insert(key);
    }

    /// Drops the key from the set if present. Returns `true` if it was found.
    #[allow(clippy::missing_panics_doc, reason = "mutex poisoning is not expected")]
    pub fn remove(&self, key: &K) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .set
            .remove(key)
            .is_some()
    }
}

/// Order identity context stored at submission time, used by the WS dispatch
/// task to produce proper order events without Cache access.
///
/// These fields are immutable for the lifetime of an order and are used to
/// construct proper order events (OrderAccepted, OrderFilled, etc.) instead
/// of execution reports.
#[derive(Debug, Clone)]
pub struct OrderIdentity {
    pub instrument_id: InstrumentId,
    pub strategy_id: StrategyId,
    pub order_side: OrderSide,
    pub order_type: OrderType,
}

/// Shared state for cross-stream event deduplication between the private
/// and business WebSocket dispatch loops.
#[derive(Debug)]
pub struct WsDispatchState {
    pub order_identities: DashMap<ClientOrderId, OrderIdentity>,
    pub emitted_accepted: BoundedDedup<ClientOrderId>,
    pub triggered_orders: BoundedDedup<ClientOrderId>,
    pub filled_orders: BoundedDedup<ClientOrderId>,
    pub terminal_orders: BoundedDedup<ClientOrderId>,
    pub emitted_trades: BoundedDedup<TradeId>,
    pre_accept_rejections: BoundedDedup<Ustr>,
    pending_responses: Option<OKXPendingResponseHandle>,
}

impl Default for WsDispatchState {
    fn default() -> Self {
        Self {
            order_identities: DashMap::new(),
            emitted_accepted: BoundedDedup::new(DEDUP_CAPACITY),
            triggered_orders: BoundedDedup::new(DEDUP_CAPACITY),
            filled_orders: BoundedDedup::new(DEDUP_CAPACITY),
            terminal_orders: BoundedDedup::new(DEDUP_CAPACITY),
            emitted_trades: BoundedDedup::new(DEDUP_CAPACITY),
            pre_accept_rejections: BoundedDedup::new(DEDUP_CAPACITY),
            pending_responses: None,
        }
    }
}

impl WsDispatchState {
    // Creates a production dispatch state sharing exact request correlation with the WebSocket
    // client that admits operations. `Default` remains useful for offline message parsing where
    // there is no live transport or pending request lifecycle.
    pub(crate) fn with_pending_responses(pending_responses: OKXPendingResponseHandle) -> Self {
        Self {
            pending_responses: Some(pending_responses),
            ..Default::default()
        }
    }
}

impl WsDispatchState {
    pub(crate) fn insert_accepted(&self, cid: ClientOrderId) {
        self.emitted_accepted.insert(cid);
    }

    pub(crate) fn insert_triggered(&self, cid: ClientOrderId) {
        self.triggered_orders.insert(cid);
    }

    pub(crate) fn insert_filled(&self, cid: ClientOrderId) {
        self.filled_orders.insert(cid);
    }

    pub(crate) fn insert_terminal(&self, cid: ClientOrderId) {
        self.terminal_orders.insert(cid);
    }

    /// Returns `true` if this trade was already emitted (duplicate).
    /// Uses atomic insert to avoid TOCTOU races between concurrent streams.
    pub fn check_and_insert_trade(&self, trade_id: TradeId) -> bool {
        self.emitted_trades.check_and_insert(trade_id)
    }
}

/// Dispatches a WebSocket message with cross-stream deduplication.
///
/// For orders with a tracked identity (submitted through this client), produces
/// proper order events (OrderAccepted, OrderCanceled, OrderFilled, etc.).
/// For untracked orders (external or pre-existing), falls back to execution
/// reports for downstream reconciliation.
///
/// # Panics
///
/// Panics only if the legacy unbounded replay-cache adapter violates its no-capacity-error
/// invariant.
#[expect(clippy::too_many_arguments)]
pub fn dispatch_ws_message(
    message: OKXWsMessage,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    account_id: AccountId,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    fee_cache: &mut AHashMap<Ustr, Money>,
    filled_qty_cache: &mut AHashMap<Ustr, Quantity>,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
    clock: &AtomicTime,
) {
    let mut replay_cache = LegacyExecutionReplayCache::new(fee_cache, filled_qty_cache);
    if let Err(error) = dispatch_ws_message_inner(
        message,
        emitter,
        state,
        account_id,
        instruments,
        &mut replay_cache,
        order_state_cache,
        clock,
    ) {
        // The public map-backed dispatcher has no live queue-failure domain. Production callers
        // use `dispatch_ws_message_bounded`, which propagates this error and halts the connection.
        log::error!("Offline OKX execution dispatch failed: {error}");
    }
}

/// Outcome of dispatching one WebSocket message on an execution consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchOutcome {
    /// The message was processed or intentionally ignored.
    Processed,
    /// The transport reconnected; venue events may have been missed while it was down.
    ///
    /// The consumer owns the response (reconcile or halt) according to the configured
    /// reconnect policy, so this is a signal rather than an error.
    Reconnected,
}

/// Dispatches a WebSocket message through the bounded production replay ledger.
///
/// # Errors
///
/// Returns a terminal capacity error before any event from the message is emitted.
#[expect(clippy::too_many_arguments)]
pub(crate) fn dispatch_ws_message_bounded(
    message: OKXWsMessage,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    account_id: AccountId,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    replay_cache: &mut BoundedExecutionReplayCache,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
    clock: &AtomicTime,
) -> Result<DispatchOutcome, ExecutionDispatchError> {
    dispatch_ws_message_inner(
        message,
        emitter,
        state,
        account_id,
        instruments,
        replay_cache,
        order_state_cache,
        clock,
    )
}

#[expect(clippy::too_many_arguments)]
fn dispatch_ws_message_inner(
    message: OKXWsMessage,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    account_id: AccountId,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    replay_cache: &mut dyn ExecutionReplayCache,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
    clock: &AtomicTime,
) -> Result<DispatchOutcome, ExecutionDispatchError> {
    replay_cache.preflight_message(&message)?;
    let guard = instruments.load();
    let instruments: &AHashMap<Ustr, InstrumentAny> = &guard;

    match message {
        OKXWsMessage::Orders(order_msgs) => {
            let ts_init = clock.get_time_ns();
            prevalidate_order_messages_atomic(
                &order_msgs,
                account_id,
                instruments,
                replay_cache,
                ts_init,
            )?;
            dispatch_order_messages(
                &order_msgs,
                emitter,
                state,
                account_id,
                instruments,
                replay_cache,
                order_state_cache,
                ts_init,
            )?;
            if let Some(pending_responses) = &state.pending_responses {
                for order in &order_msgs {
                    pending_responses.observe_order_update(&order.cl_ord_id, order.state);
                }
            }
        }
        OKXWsMessage::SpreadOrders(order_msgs) => {
            let ts_init = clock.get_time_ns();
            prevalidate_spread_order_messages_atomic(
                &order_msgs,
                account_id,
                instruments,
                replay_cache,
                ts_init,
            )?;
            dispatch_spread_order_messages(
                &order_msgs,
                emitter,
                state,
                account_id,
                instruments,
                replay_cache,
                order_state_cache,
                ts_init,
            )?;
        }
        OKXWsMessage::AlgoOrders(algo_msgs) => {
            let ts_init = clock.get_time_ns();
            let mut reports = Vec::new();

            for msg in algo_msgs {
                match parse_algo_order_msg(&msg, account_id, instruments, ts_init) {
                    Ok(Some(report)) => reports.push(report),
                    Ok(None) => {}
                    Err(error) => {
                        return Err(ExecutionDispatchError::conversion(format!(
                            "algo order id={} instrument={}: {error}",
                            msg.algo_id, msg.inst_id
                        )));
                    }
                }
            }
            dispatch_execution_reports(reports, emitter, state);
        }
        OKXWsMessage::Account(data) => {
            let ts_init = clock.get_time_ns();

            let accounts = serde_json::from_value::<Vec<OKXAccount>>(data).map_err(|error| {
                ExecutionDispatchError::conversion(format!(
                    "private account payload deserialization: {error}"
                ))
            })?;
            let mut account_states = Vec::with_capacity(accounts.len());
            for account in &accounts {
                account_states.push(
                    crate::common::parse::parse_account_state(account, account_id, ts_init)
                        .map_err(|error| {
                            ExecutionDispatchError::conversion(format!(
                                "private account state for account_id={account_id}: {error}"
                            ))
                        })?,
                );
            }
            for account_state in account_states {
                emitter.send_account_state(account_state);
            }
        }
        OKXWsMessage::Positions(data) => {
            let ts_init = clock.get_time_ns();
            let positions = serde_json::from_value::<Vec<OKXPosition>>(data).map_err(|error| {
                ExecutionDispatchError::conversion(format!(
                    "private positions payload deserialization: {error}"
                ))
            })?;
            let mut reports = Vec::with_capacity(positions.len());
            for position in positions {
                let instrument = instruments.get(&position.inst_id).ok_or_else(|| {
                    ExecutionDispatchError::conversion(format!(
                        "private position instrument missing: inst_id={}",
                        position.inst_id
                    ))
                })?;
                reports.push(
                    crate::common::parse::parse_position_status_report(
                        &position,
                        account_id,
                        instrument.id(),
                        instrument.size_precision(),
                        ts_init,
                    )
                    .map_err(|error| {
                        ExecutionDispatchError::conversion(format!(
                            "private position inst_id={}: {error}",
                            position.inst_id
                        ))
                    })?,
                );
            }
            for report in reports {
                emitter.send_position_report(report);
            }
        }
        OKXWsMessage::OrderResponse {
            id,
            op,
            code,
            msg,
            data,
        } => {
            let ts_init = clock.get_time_ns();
            let response_client_order_id = data
                .first()
                .and_then(|item| {
                    item.get(OKX_FIELD_CLORDID)
                        .or_else(|| item.get("algoClOrdId"))
                })
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty());
            // One venue request owns one pending entry even when its response contains multiple
            // batch items. Resolve the top-level request exactly once before mutating any
            // item-level lifecycle state.
            let pending_resolution = state
                .pending_responses
                .as_ref()
                .filter(|pending| pending.tracks_operation(&op))
                .map(|pending_responses| {
                    pending_responses.resolve(&op, id.as_deref(), response_client_order_id)
                });
            if matches!(pending_resolution, Some(None)) {
                log::error!(
                    "Ignoring unmatched OKX order response: \
                     id={id:?} op={op:?} cl_ord_id={response_client_order_id:?}"
                );
                return Ok(DispatchOutcome::Processed);
            }
            let pending_info = pending_resolution.flatten();

            if pending_info
                .as_ref()
                .is_some_and(|info| info.routing.is_none())
                && data.is_empty()
            {
                return Err(ExecutionDispatchError::pending_ambiguous(format!(
                    "aggregate response has no item acknowledgements: id={id:?} op={op:?} \
                     code={code} msg={msg:?}"
                )));
            }

            for (index, item) in data.iter().enumerate() {
                let parsed = parse_order_response_item(item, index)?;
                let s_code = parsed.s_code.as_str();
                let s_msg = parsed.s_msg.as_str();
                let reason = format_order_response_reason(s_code, s_msg, &parsed.sub_code);
                let cl_ord_id = parsed.client_order_id.as_str();

                if s_code == OKX_SUCCESS_CODE {
                    log::debug!("Order response ok: op={op:?} cl_ord_id={cl_ord_id}");
                    continue;
                }

                let client_order_id = parse_client_order_id(cl_ord_id)
                    .or_else(|| pending_info.as_ref().and_then(|info| info.client_order_id));
                let Some(client_order_id) = client_order_id else {
                    log::warn!(
                        "Order response error without client_order_id: \
                         op={op:?} s_code={s_code} s_msg={s_msg}"
                    );
                    continue;
                };

                let tracked_identity = state
                    .order_identities
                    .get(&client_order_id)
                    .map(|entry| entry.clone());
                let routing = tracked_identity
                    .as_ref()
                    .map(|ident| (ident.strategy_id, ident.instrument_id))
                    .or_else(|| {
                        pending_info
                            .as_ref()
                            .and_then(|info| info.routing)
                            .map(|routing| (routing.strategy_id, routing.instrument_id))
                    });
                let Some((strategy_id, instrument_id)) = routing else {
                    log::warn!(
                        "Order response error for untracked order: \
                         op={op:?} cl_ord_id={cl_ord_id} s_code={s_code} s_msg={s_msg}"
                    );
                    continue;
                };

                let venue_order_id = parsed
                    .venue_order_id
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(VenueOrderId::new);

                match op {
                    OKXWsOperation::Order
                    | OKXWsOperation::BatchOrders
                    | OKXWsOperation::OrderAlgo => {
                        state.order_identities.remove(&client_order_id);
                        emitter.emit_order_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            &reason,
                            ts_init,
                            false,
                        );
                    }
                    OKXWsOperation::CancelOrder
                    | OKXWsOperation::BatchCancelOrders
                    | OKXWsOperation::MassCancel
                    | OKXWsOperation::CancelAlgos => {
                        emitter.emit_order_cancel_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            venue_order_id,
                            &reason,
                            ts_init,
                        );
                    }
                    OKXWsOperation::AmendOrder | OKXWsOperation::BatchAmendOrders => {
                        emitter.emit_order_modify_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            venue_order_id,
                            &reason,
                            ts_init,
                        );
                    }
                    _ => {
                        log::warn!(
                            "Order response error for unhandled op: \
                             op={op:?} cl_ord_id={cl_ord_id} s_code={s_code} s_msg={s_msg}"
                        );
                    }
                }
            }

            if code != "0" && data.is_empty() {
                log::warn!(
                    "Order response error (no data): id={id:?} op={op:?} code={code} msg={msg}"
                );
                if let Some(info) = pending_info.as_ref() {
                    let Some(client_order_id) = info.client_order_id else {
                        log::warn!(
                            "Resolved data-less OKX response has no client_order_id: \
                             id={id:?} op={op:?}"
                        );
                        return Ok(DispatchOutcome::Processed);
                    };
                    let tracked_identity = state
                        .order_identities
                        .get(&client_order_id)
                        .map(|entry| entry.clone());
                    let routing = tracked_identity
                        .as_ref()
                        .map(|identity| (identity.strategy_id, identity.instrument_id))
                        .or_else(|| {
                            info.routing
                                .map(|routing| (routing.strategy_id, routing.instrument_id))
                        });
                    let Some((strategy_id, instrument_id)) = routing else {
                        log::warn!(
                            "Resolved data-less OKX response for untracked order: \
                             id={id:?} op={op:?} client_order_id={client_order_id}"
                        );
                        return Ok(DispatchOutcome::Processed);
                    };
                    let reason = if msg.is_empty() {
                        code.as_str()
                    } else {
                        msg.as_str()
                    };
                    match op {
                        OKXWsOperation::Order
                        | OKXWsOperation::BatchOrders
                        | OKXWsOperation::OrderAlgo => {
                            state.order_identities.remove(&client_order_id);
                            emitter.emit_order_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                reason,
                                ts_init,
                                false,
                            );
                        }
                        OKXWsOperation::CancelOrder
                        | OKXWsOperation::BatchCancelOrders
                        | OKXWsOperation::MassCancel
                        | OKXWsOperation::CancelAlgos => {
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                None,
                                reason,
                                ts_init,
                            );
                        }
                        OKXWsOperation::AmendOrder | OKXWsOperation::BatchAmendOrders => {
                            emitter.emit_order_modify_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                None,
                                reason,
                                ts_init,
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
        OKXWsMessage::SendFailed {
            request_id,
            client_order_id,
            op,
            error,
        } => {
            log::warn!(
                "WebSocket send failed without structured venue response: \
                 request_id={request_id}, client_order_id={client_order_id:?}, \
                 op={op:?}, awaiting reconciliation: {error}"
            );

            if let Some(client_order_id) = client_order_id {
                if let Some(pending_responses) = &state.pending_responses {
                    pending_responses.mark_send_failed(
                        op.as_ref(),
                        &request_id,
                        Some(client_order_id),
                    );
                }
            } else if let Some(pending_responses) = &state.pending_responses {
                pending_responses.mark_send_failed(op.as_ref(), &request_id, None);
            }
        }
        OKXWsMessage::ChannelData { channel, .. } => {
            if channel == crate::websocket::enums::OKXWsChannel::Fills {
                return Err(ExecutionDispatchError::conversion(
                    "received unsupported OKX fills channel message; fills are sourced from the \
                     orders channel",
                ));
            }
            log::debug!("Ignoring data channel message on execution client: {channel:?}");
        }
        OKXWsMessage::BookData { .. }
        | OKXWsMessage::RpiBookData { .. }
        | OKXWsMessage::Instruments(_) => {
            log::debug!("Ignoring data message on execution client");
        }
        OKXWsMessage::Error(e) => {
            log::warn!(
                "Websocket error: code={} message={} conn_id={:?}",
                e.code,
                e.message,
                e.conn_id
            );
        }
        OKXWsMessage::Reconnected => {
            return Ok(DispatchOutcome::Reconnected);
        }
        OKXWsMessage::Authenticated => {
            log::debug!("Websocket authenticated");
        }
    }
    Ok(DispatchOutcome::Processed)
}

fn prevalidate_order_messages_atomic(
    order_msgs: &[OKXOrderMsg],
    account_id: AccountId,
    instruments: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &dyn ExecutionReplayCache,
    ts_init: UnixNanos,
) -> Result<(), ExecutionDispatchError> {
    let mut batch_fees: AHashMap<Ustr, Money> = AHashMap::new();
    let mut batch_fills: AHashMap<Ustr, Quantity> = AHashMap::new();

    for msg in order_msgs {
        let instrument = instruments.get(&msg.inst_id).ok_or_else(|| {
            ExecutionDispatchError::conversion(format!(
                "orders channel instrument missing: inst_id={} ord_id={}",
                msg.inst_id, msg.ord_id
            ))
        })?;
        let previous_fee = batch_fees
            .get(&msg.ord_id)
            .copied()
            .or_else(|| replay_cache.previous_fee(&msg.ord_id));
        let previous_filled_qty = batch_fills
            .get(&msg.ord_id)
            .copied()
            .or_else(|| replay_cache.previous_filled_qty(&msg.ord_id));
        parse_order_msg_with_previous(
            msg,
            account_id,
            instruments,
            previous_fee,
            previous_filled_qty,
            ts_init,
        )
        .map_err(|error| {
            ExecutionDispatchError::conversion(format!(
                "atomic orders prevalidation inst_id={} ord_id={}: {error}",
                msg.inst_id, msg.ord_id
            ))
        })?;
        let (fee, filled_qty) = parse_fee_fill_cache_values(msg, instrument).map_err(|error| {
            ExecutionDispatchError::conversion(format!(
                "atomic order replay prevalidation ord_id={}: {error}",
                msg.ord_id
            ))
        })?;
        if let Some(fee) = fee {
            batch_fees.insert(msg.ord_id, fee);
        }
        if let Some(filled_qty) = filled_qty {
            batch_fills.insert(msg.ord_id, filled_qty);
        }
    }
    Ok(())
}

fn prevalidate_spread_order_messages_atomic(
    order_msgs: &[OKXSpreadOrder],
    account_id: AccountId,
    instruments: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &dyn ExecutionReplayCache,
    ts_init: UnixNanos,
) -> Result<(), ExecutionDispatchError> {
    let mut batch_fills: AHashMap<Ustr, Quantity> = AHashMap::new();

    for msg in order_msgs {
        let instrument = instruments.get(&msg.sprd_id).ok_or_else(|| {
            ExecutionDispatchError::conversion(format!(
                "spread orders channel instrument missing: sprd_id={} ord_id={}",
                msg.sprd_id, msg.ord_id
            ))
        })?;
        let previous_filled_qty = batch_fills
            .get(&msg.ord_id)
            .copied()
            .or_else(|| replay_cache.previous_filled_qty(&msg.ord_id));
        parse_spread_order_msg_with_previous(
            msg,
            account_id,
            instruments,
            previous_filled_qty,
            ts_init,
        )
        .map_err(|error| {
            ExecutionDispatchError::conversion(format!(
                "atomic spread prevalidation sprd_id={} ord_id={}: {error}",
                msg.sprd_id, msg.ord_id
            ))
        })?;
        if !msg.acc_fill_sz.is_empty() && msg.acc_fill_sz != "0" {
            let filled_qty = parse_quantity(&msg.acc_fill_sz, instrument.size_precision())
                .map_err(|error| {
                    ExecutionDispatchError::conversion(format!(
                        "atomic spread replay prevalidation ord_id={} value={:?}: {error}",
                        msg.ord_id, msg.acc_fill_sz
                    ))
                })?;
            batch_fills.insert(msg.ord_id, filled_qty);
        }
    }
    Ok(())
}

/// Dispatches order messages, producing proper order events for tracked orders
/// and falling back to execution reports for untracked/external orders.
#[expect(clippy::too_many_arguments)]
fn dispatch_order_messages(
    order_msgs: &[OKXOrderMsg],
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    account_id: AccountId,
    instruments: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &mut dyn ExecutionReplayCache,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
    ts_init: UnixNanos,
) -> Result<(), ExecutionDispatchError> {
    for msg in order_msgs {
        let instrument = instruments.get(&msg.inst_id).ok_or_else(|| {
            ExecutionDispatchError::conversion(format!(
                "orders channel instrument missing: inst_id={} ord_id={}",
                msg.inst_id, msg.ord_id
            ))
        })?;

        let Some(client_order_id) = parse_client_order_id(&msg.cl_ord_id) else {
            log::debug!(
                "Order without client_order_id (ord_id={}), sending as report",
                msg.ord_id
            );
            dispatch_order_msg_as_report(
                msg,
                account_id,
                instruments,
                replay_cache,
                emitter,
                state,
                ts_init,
            )?;
            continue;
        };

        // Resolve identity: check direct match first, then fall back to the
        // parent algo order ID for triggered child orders. OKX assigns a new
        // cl_ord_id to child orders when an algo/stop triggers, preserving the
        // parent's client order ID in algo_cl_ord_id.
        let (client_order_id, identity) = match state
            .order_identities
            .get(&client_order_id)
            .map(|r| r.clone())
        {
            Some(ident) => (client_order_id, Some(ident)),
            None => {
                if let Some(parent_id) = msg
                    .algo_cl_ord_id
                    .as_deref()
                    .and_then(parse_client_order_id)
                {
                    let parent_ident = state.order_identities.get(&parent_id).map(|r| r.clone());

                    if parent_ident.is_some() {
                        (parent_id, parent_ident)
                    } else {
                        (client_order_id, None)
                    }
                } else {
                    (client_order_id, None)
                }
            }
        };

        if let Some(ident) = identity {
            let is_post_only_cancel = is_post_only_auto_cancel(msg);

            if is_post_only_cancel
                || (!state.emitted_accepted.contains(&client_order_id)
                    && is_unfilled_rpi_cancel(msg))
            {
                // These venue-side cancels are confirmed zero-fill rejections before
                // acceptance. Keep their replay marker in the bounded dispatch window while
                // releasing the cumulative execution-cache slot below.
                if state.pre_accept_rejections.check_and_insert(msg.ord_id) {
                    replay_cache.remove_explicitly_safe_rejection(&msg.ord_id);
                    log::debug!(
                        "Skipping duplicate pre-accept rejection for {client_order_id}: ord_id={}",
                        msg.ord_id
                    );
                    continue;
                }

                let ts_event = parse_millisecond_timestamp(msg.u_time);
                let reason = if msg.ord_type == OKXOrderType::Rpi {
                    msg.cancel_source_reason
                        .as_deref()
                        .filter(|reason| !reason.is_empty())
                        .unwrap_or("RPI order canceled before acceptance")
                } else {
                    "Post-only order would have taken liquidity"
                };
                let rejected = OrderRejected::new(
                    emitter.trader_id(),
                    ident.strategy_id,
                    instrument.id(),
                    client_order_id,
                    account_id,
                    Ustr::from(reason),
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    true, // due_post_only
                );
                state.order_identities.remove(&client_order_id);
                order_state_cache.remove(&client_order_id);
                replay_cache.remove_explicitly_safe_rejection(&msg.ord_id);
                emitter.send_order_event(OrderEventAny::Rejected(rejected));
                continue;
            }

            let previous_fee = replay_cache.previous_fee(&msg.ord_id);
            let previous_filled_qty = replay_cache.previous_filled_qty(&msg.ord_id);
            let previous_state = order_state_cache.get(&client_order_id);

            match parse_order_event(
                msg,
                client_order_id,
                account_id,
                emitter.trader_id(),
                ident.strategy_id,
                instrument,
                previous_fee,
                previous_filled_qty,
                previous_state,
                ts_init,
            ) {
                Ok(event) => {
                    update_order_caches(
                        msg,
                        instrument,
                        client_order_id,
                        replay_cache,
                        order_state_cache,
                    )?;
                    dispatch_parsed_order_event(
                        event,
                        client_order_id,
                        account_id,
                        VenueOrderId::new(msg.ord_id),
                        &ident,
                        instrument,
                        msg.state,
                        emitter,
                        state,
                        order_state_cache,
                        ts_init,
                    );
                }
                Err(error) => {
                    return Err(ExecutionDispatchError::conversion(format!(
                        "orders channel event cl_ord_id={client_order_id} ord_id={}: {error}",
                        msg.ord_id
                    )));
                }
            }
        } else if (is_post_only_auto_cancel(msg)
            || (!state.emitted_accepted.contains(&client_order_id) && is_unfilled_rpi_cancel(msg)))
            && state.pre_accept_rejections.contains(&msg.ord_id)
        {
            // Bounded production preflight reserves the replayed ID again. It remains safe to
            // release because both predicates require a zero cumulative fill.
            replay_cache.remove_explicitly_safe_rejection(&msg.ord_id);
            log::debug!(
                "Skipping replayed pre-accept rejection for {client_order_id}: ord_id={}",
                msg.ord_id
            );
        } else {
            log::debug!(
                "Untracked order {client_order_id} (ord_id={}), sending as report for reconciliation",
                msg.ord_id
            );
            dispatch_order_msg_as_report(
                msg,
                account_id,
                instruments,
                replay_cache,
                emitter,
                state,
                ts_init,
            )?;
        }
    }
    Ok(())
}

#[expect(clippy::too_many_arguments)]
fn dispatch_spread_order_messages(
    order_msgs: &[OKXSpreadOrder],
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    account_id: AccountId,
    instruments: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &mut dyn ExecutionReplayCache,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
    ts_init: UnixNanos,
) -> Result<(), ExecutionDispatchError> {
    for msg in order_msgs {
        let instrument = instruments.get(&msg.sprd_id).ok_or_else(|| {
            ExecutionDispatchError::conversion(format!(
                "spread orders channel instrument missing: sprd_id={} ord_id={}",
                msg.sprd_id, msg.ord_id
            ))
        })?;

        let Some(client_order_id) = parse_client_order_id(msg.cl_ord_id.as_str()) else {
            log::debug!(
                "Spread order without client_order_id (ord_id={}), sending as report",
                msg.ord_id
            );
            dispatch_spread_order_msg_as_report(
                msg,
                account_id,
                instruments,
                replay_cache,
                emitter,
                state,
                ts_init,
            )?;
            continue;
        };

        let identity = state
            .order_identities
            .get(&client_order_id)
            .map(|r| r.clone());

        if let Some(ident) = identity {
            if is_spread_post_only_auto_cancel(msg) {
                if state.pre_accept_rejections.check_and_insert(msg.ord_id) {
                    replay_cache.remove_explicitly_safe_rejection(&msg.ord_id);
                    log::debug!(
                        "Skipping duplicate spread post-only rejection for {client_order_id}: ord_id={}",
                        msg.ord_id
                    );
                    continue;
                }
                let ts_event = msg
                    .u_time
                    .or(msg.c_time)
                    .map_or(ts_init, parse_millisecond_timestamp);
                let rejected = OrderRejected::new(
                    emitter.trader_id(),
                    ident.strategy_id,
                    instrument.id(),
                    client_order_id,
                    account_id,
                    Ustr::from(OKX_POST_ONLY_CANCEL_REASON),
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    true,
                );
                state.order_identities.remove(&client_order_id);
                order_state_cache.remove(&client_order_id);
                replay_cache.remove_explicitly_safe_rejection(&msg.ord_id);
                emitter.send_order_event(OrderEventAny::Rejected(rejected));
                continue;
            }

            let previous_filled_qty = replay_cache.previous_filled_qty(&msg.ord_id);
            let previous_state = order_state_cache.get(&client_order_id);

            match parse_spread_order_event(
                msg,
                client_order_id,
                account_id,
                emitter.trader_id(),
                ident.strategy_id,
                instrument,
                previous_filled_qty,
                previous_state,
                ts_init,
            ) {
                Ok(event) => {
                    update_spread_order_caches(
                        msg,
                        instrument,
                        client_order_id,
                        replay_cache,
                        order_state_cache,
                    )?;
                    dispatch_parsed_order_event(
                        event,
                        client_order_id,
                        account_id,
                        VenueOrderId::new(msg.ord_id.as_str()),
                        &ident,
                        instrument,
                        msg.state,
                        emitter,
                        state,
                        order_state_cache,
                        ts_init,
                    );
                }
                Err(error) => {
                    return Err(ExecutionDispatchError::conversion(format!(
                        "spread orders channel event cl_ord_id={client_order_id} ord_id={}: \
                         {error}",
                        msg.ord_id
                    )));
                }
            }
        } else if is_spread_post_only_auto_cancel(msg)
            && state.pre_accept_rejections.contains(&msg.ord_id)
        {
            // Bounded production preflight reserves the replayed ID again. A spread post-only
            // pre-accept rejection is only recognized when cumulative fill is exactly zero.
            replay_cache.remove_explicitly_safe_rejection(&msg.ord_id);
            log::debug!(
                "Skipping replayed spread post-only rejection for {client_order_id}: ord_id={}",
                msg.ord_id
            );
        } else {
            log::debug!(
                "Untracked spread order {client_order_id} (ord_id={}), sending as report for reconciliation",
                msg.ord_id
            );
            dispatch_spread_order_msg_as_report(
                msg,
                account_id,
                instruments,
                replay_cache,
                emitter,
                state,
                ts_init,
            )?;
        }
    }
    Ok(())
}

/// Dispatches a parsed order event as a proper `OrderEventAny`.
///
/// Guarantees the `Submitted -> Accepted -> ...` lifecycle by synthesizing
/// `OrderAccepted` before any other event when one has not yet been emitted.
/// Duplicate `Accepted` events (e.g. from reconnect replays) are suppressed.
#[expect(clippy::too_many_arguments)]
fn dispatch_parsed_order_event(
    event: ParsedOrderEvent,
    client_order_id: ClientOrderId,
    account_id: AccountId,
    venue_order_id: VenueOrderId,
    identity: &OrderIdentity,
    instrument: &InstrumentAny,
    venue_status: OKXOrderStatus,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
    ts_init: UnixNanos,
) {
    let is_terminal;

    match event {
        ParsedOrderEvent::Accepted(e) => {
            if state.emitted_accepted.contains(&client_order_id)
                || state.filled_orders.contains(&client_order_id)
                || state.triggered_orders.contains(&client_order_id)
                || state.terminal_orders.contains(&client_order_id)
            {
                log::debug!("Skipping duplicate Accepted for {client_order_id}");
                return;
            }
            state.insert_accepted(client_order_id);
            is_terminal = false;
            emitter.send_order_event(OrderEventAny::Accepted(e));
        }
        ParsedOrderEvent::Triggered(e) => {
            if state.filled_orders.contains(&client_order_id) {
                log::debug!("Skipping stale Triggered for {client_order_id} (already filled)");
                return;
            }

            if !TRIGGERABLE_ORDER_TYPES.contains(&identity.order_type) {
                log::debug!(
                    "Skipping OrderTriggered for {} order {client_order_id}: market-style stops have no TRIGGERED state",
                    identity.order_type,
                );
                state.insert_triggered(client_order_id);
                return;
            }

            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            state.insert_triggered(client_order_id);
            is_terminal = false;
            emitter.send_order_event(OrderEventAny::Triggered(e));
        }
        ParsedOrderEvent::Canceled(e) => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            state.triggered_orders.remove(&client_order_id);
            state.filled_orders.remove(&client_order_id);
            is_terminal = true;
            emitter.send_order_event(OrderEventAny::Canceled(e));
        }
        ParsedOrderEvent::Expired(e) => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            state.triggered_orders.remove(&client_order_id);
            state.filled_orders.remove(&client_order_id);
            is_terminal = true;
            emitter.send_order_event(OrderEventAny::Expired(e));
        }
        ParsedOrderEvent::Updated(e) => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            is_terminal = false;
            emitter.send_order_event(OrderEventAny::Updated(e));
        }
        ParsedOrderEvent::Fill(fill_report) => {
            let is_duplicate = state.check_and_insert_trade(fill_report.trade_id);
            is_terminal = venue_status == OKXOrderStatus::Filled;

            if is_duplicate {
                log::debug!(
                    "Skipping duplicate fill for {client_order_id}: trade_id={}",
                    fill_report.trade_id
                );
            } else {
                ensure_accepted_emitted(
                    client_order_id,
                    account_id,
                    venue_order_id,
                    identity,
                    emitter,
                    state,
                    ts_init,
                );
                state.insert_filled(client_order_id);
                state.triggered_orders.remove(&client_order_id);
                let filled = fill_report_to_order_filled(
                    &fill_report,
                    emitter.trader_id(),
                    identity,
                    instrument.quote_currency(),
                );
                emitter.send_order_event(OrderEventAny::Filled(filled));
            }
        }
        ParsedOrderEvent::StatusOnly(report) => {
            is_terminal = matches!(
                report.order_status,
                OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Expired
            );
            emitter.send_order_status_report(*report);
        }
        ParsedOrderEvent::Skipped => return,
    }

    if is_terminal {
        state.insert_terminal(client_order_id);
        state.order_identities.remove(&client_order_id);
        state.emitted_accepted.remove(&client_order_id);
        order_state_cache.remove(&client_order_id);
        // Keep the replay entry: terminal messages replayed after reconnect go through the
        // untracked report path and need prior cumulative state to avoid re-emitting a full fill.
    }
}

/// Synthesizes and emits `OrderAccepted` if one has not yet been emitted for
/// this order. Handles fast-filling orders that skip the `Live` state on OKX.
fn ensure_accepted_emitted(
    client_order_id: ClientOrderId,
    account_id: AccountId,
    venue_order_id: VenueOrderId,
    identity: &OrderIdentity,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    ts_init: UnixNanos,
) {
    if state.emitted_accepted.contains(&client_order_id) {
        return;
    }
    state.insert_accepted(client_order_id);
    let accepted = OrderAccepted::new(
        emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        client_order_id,
        venue_order_id,
        account_id,
        UUID4::new(),
        ts_init,
        ts_init,
        false,
    );
    emitter.send_order_event(OrderEventAny::Accepted(accepted));
}

/// Converts a [`FillReport`] into an [`OrderFilled`] event using tracked identity.
fn fill_report_to_order_filled(
    report: &FillReport,
    trader_id: TraderId,
    identity: &OrderIdentity,
    quote_currency: Currency,
) -> OrderFilled {
    OrderFilled::new(
        trader_id,
        identity.strategy_id,
        report.instrument_id,
        report
            .client_order_id
            .expect("tracked order has client_order_id"),
        report.venue_order_id,
        report.account_id,
        report.trade_id,
        identity.order_side,
        identity.order_type,
        report.last_qty,
        report.last_px,
        quote_currency,
        report.liquidity_side,
        UUID4::new(),
        report.ts_event,
        report.ts_init,
        false,
        report.venue_position_id,
        Some(report.commission),
        None,
    )
}

/// Falls back to the report path for a single order message.
fn dispatch_order_msg_as_report(
    msg: &OKXOrderMsg,
    account_id: AccountId,
    instruments: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &mut dyn ExecutionReplayCache,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    ts_init: UnixNanos,
) -> Result<(), ExecutionDispatchError> {
    match parse_order_msg_with_previous(
        msg,
        account_id,
        instruments,
        replay_cache.previous_fee(&msg.ord_id),
        replay_cache.previous_filled_qty(&msg.ord_id),
        ts_init,
    ) {
        Ok(report) => {
            let instrument = instruments.get(&msg.inst_id).ok_or_else(|| {
                ExecutionDispatchError::conversion(format!(
                    "order report instrument missing: inst_id={} ord_id={}",
                    msg.inst_id, msg.ord_id
                ))
            })?;
            update_order_replay_cache(msg, instrument, replay_cache)?;
            dispatch_execution_reports(vec![report], emitter, state);
        }
        Err(error) => {
            return Err(ExecutionDispatchError::conversion(format!(
                "order report inst_id={} ord_id={}: {error}",
                msg.inst_id, msg.ord_id
            )));
        }
    }
    Ok(())
}

fn dispatch_spread_order_msg_as_report(
    msg: &OKXSpreadOrder,
    account_id: AccountId,
    instruments: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &mut dyn ExecutionReplayCache,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    ts_init: UnixNanos,
) -> Result<(), ExecutionDispatchError> {
    match parse_spread_order_msg_with_previous(
        msg,
        account_id,
        instruments,
        replay_cache.previous_filled_qty(&msg.ord_id),
        ts_init,
    ) {
        Ok(report) => {
            let instrument = instruments.get(&msg.sprd_id).ok_or_else(|| {
                ExecutionDispatchError::conversion(format!(
                    "spread order report instrument missing: sprd_id={} ord_id={}",
                    msg.sprd_id, msg.ord_id
                ))
            })?;
            update_spread_replay_cache(msg, instrument, replay_cache)?;
            dispatch_execution_reports(vec![report], emitter, state);
        }
        Err(error) => {
            return Err(ExecutionDispatchError::conversion(format!(
                "spread order report sprd_id={} ord_id={}: {error}",
                msg.sprd_id, msg.ord_id
            )));
        }
    }
    Ok(())
}

/// Updates fee, fill, and order state caches from a raw OKX order message.
fn update_order_caches(
    msg: &OKXOrderMsg,
    instrument: &InstrumentAny,
    client_order_id: ClientOrderId,
    replay_cache: &mut dyn ExecutionReplayCache,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
) -> Result<(), ExecutionDispatchError> {
    let quantity = parse_quantity(&msg.sz, instrument.size_precision()).map_err(|error| {
        ExecutionDispatchError::conversion(format!(
            "order cache quantity ord_id={} value={:?}: {error}",
            msg.ord_id, msg.sz
        ))
    })?;
    let price = if is_market_price(&msg.px) {
        None
    } else {
        Some(
            parse_price(&msg.px, instrument.price_precision()).map_err(|error| {
                ExecutionDispatchError::conversion(format!(
                    "order cache price ord_id={} value={:?}: {error}",
                    msg.ord_id, msg.px
                ))
            })?,
        )
    };
    update_order_replay_cache(msg, instrument, replay_cache)?;

    let venue_order_id = VenueOrderId::new(msg.ord_id);
    order_state_cache.insert(
        client_order_id,
        OrderStateSnapshot {
            venue_order_id,
            quantity,
            price,
        },
    );
    Ok(())
}

fn update_spread_order_caches(
    msg: &OKXSpreadOrder,
    instrument: &InstrumentAny,
    client_order_id: ClientOrderId,
    replay_cache: &mut dyn ExecutionReplayCache,
    order_state_cache: &mut AHashMap<ClientOrderId, OrderStateSnapshot>,
) -> Result<(), ExecutionDispatchError> {
    let quantity = parse_quantity(&msg.sz, instrument.size_precision()).map_err(|error| {
        ExecutionDispatchError::conversion(format!(
            "spread order cache quantity ord_id={} value={:?}: {error}",
            msg.ord_id, msg.sz
        ))
    })?;
    let price = if is_market_price(&msg.px) {
        None
    } else {
        Some(
            parse_price(&msg.px, instrument.price_precision()).map_err(|error| {
                ExecutionDispatchError::conversion(format!(
                    "spread order cache price ord_id={} value={:?}: {error}",
                    msg.ord_id, msg.px
                ))
            })?,
        )
    };
    update_spread_replay_cache(msg, instrument, replay_cache)?;

    let venue_order_id = VenueOrderId::new(msg.ord_id.as_str());
    order_state_cache.insert(
        client_order_id,
        OrderStateSnapshot {
            venue_order_id,
            quantity,
            price,
        },
    );
    Ok(())
}

fn update_order_replay_cache(
    msg: &OKXOrderMsg,
    instrument: &InstrumentAny,
    replay_cache: &mut dyn ExecutionReplayCache,
) -> Result<(), ExecutionDispatchError> {
    let (fee, filled_qty) = parse_fee_fill_cache_values(msg, instrument).map_err(|error| {
        ExecutionDispatchError::conversion(format!(
            "order replay values ord_id={}: {error}",
            msg.ord_id
        ))
    })?;
    replay_cache.update(msg.ord_id, fee, filled_qty);
    Ok(())
}

fn update_spread_replay_cache(
    msg: &OKXSpreadOrder,
    instrument: &InstrumentAny,
    replay_cache: &mut dyn ExecutionReplayCache,
) -> Result<(), ExecutionDispatchError> {
    let filled_qty = if msg.acc_fill_sz.is_empty() || msg.acc_fill_sz == "0" {
        None
    } else {
        Some(
            parse_quantity(&msg.acc_fill_sz, instrument.size_precision()).map_err(|error| {
                ExecutionDispatchError::conversion(format!(
                    "spread replay cumulative fill ord_id={} value={:?}: {error}",
                    msg.ord_id, msg.acc_fill_sz
                ))
            })?,
        )
    };
    replay_cache.update(msg.ord_id, None, filled_qty);
    Ok(())
}

fn is_spread_post_only_auto_cancel(msg: &OKXSpreadOrder) -> bool {
    msg.state == OKXOrderStatus::Canceled
        && msg.cancel_source == OKX_POST_ONLY_CANCEL_SOURCE
        && (msg.acc_fill_sz.is_empty() || msg.acc_fill_sz == "0")
}

/// Dispatches execution reports with cross-stream deduplication.
pub fn dispatch_execution_reports(
    reports: Vec<ExecutionReport>,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
) {
    log::debug!("Processing {} execution report(s)", reports.len());

    for report in reports {
        match report {
            ExecutionReport::Order(order_report) => {
                if let Some(cid) = order_report.client_order_id {
                    match order_report.order_status {
                        // Guard form reformats awkwardly across multiple lines
                        #[allow(clippy::collapsible_match)]
                        OrderStatus::Accepted => {
                            if state.terminal_orders.contains(&cid)
                                || state.filled_orders.contains(&cid)
                                || state.triggered_orders.contains(&cid)
                            {
                                log::debug!(
                                    "Skipping stale OrderStatusReport(Accepted) \
                                     for {cid} (order already terminal)"
                                );
                                continue;
                            }
                        }
                        OrderStatus::Triggered => {
                            if state.filled_orders.contains(&cid) {
                                log::debug!(
                                    "Skipping stale OrderStatusReport(Triggered) \
                                     for {cid} (already filled)"
                                );
                                continue;
                            }
                            state.insert_triggered(cid);
                        }
                        OrderStatus::Filled => {
                            // A tracked order already driven to a terminal fill replays through
                            // this path after reconnect with unchanged cumulative state. Dropping
                            // it here keeps exactly-once delivery; a Filled report for an order we
                            // last saw canceled still passes through so reconciliation can act.
                            if state.terminal_orders.contains(&cid)
                                && state.filled_orders.contains(&cid)
                            {
                                log::debug!(
                                    "Skipping replayed OrderStatusReport(Filled) \
                                     for {cid} (terminal fill already emitted)"
                                );
                                continue;
                            }
                            state.insert_filled(cid);
                            state.insert_terminal(cid);
                            state.triggered_orders.remove(&cid);
                        }
                        OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected => {
                            if state.terminal_orders.contains(&cid)
                                && !state.filled_orders.contains(&cid)
                            {
                                log::debug!(
                                    "Skipping replayed OrderStatusReport({:?}) \
                                     for {cid} (terminal state already emitted)",
                                    order_report.order_status
                                );
                                continue;
                            }
                            state.insert_terminal(cid);
                            state.triggered_orders.remove(&cid);
                            state.filled_orders.remove(&cid);
                        }
                        _ => {}
                    }
                }
                emitter.send_order_status_report(order_report);
            }
            ExecutionReport::Fill(fill_report) => {
                if state.check_and_insert_trade(fill_report.trade_id) {
                    log::debug!(
                        "Skipping duplicate fill report: trade_id={}",
                        fill_report.trade_id
                    );
                    continue;
                }

                if let Some(cid) = fill_report.client_order_id {
                    state.insert_filled(cid);
                    state.triggered_orders.remove(&cid);
                }
                emitter.send_fill_report(fill_report);
            }
        }
    }
}

fn format_order_response_reason(s_code: &str, s_msg: &str, sub_code: &str) -> String {
    match (s_msg.is_empty(), sub_code.is_empty(), s_code.is_empty()) {
        (false, true, _) => s_msg.to_string(),
        (false, false, _) => format!("{s_msg} (subCode={sub_code})"),
        (true, false, false) => format!("sCode={s_code} subCode={sub_code}"),
        (true, false, true) => format!("subCode={sub_code}"),
        (true, true, false) => format!("sCode={s_code}"),
        (true, true, true) => String::new(),
    }
}

pub(crate) struct ParsedOrderResponseItem {
    pub(crate) s_code: String,
    pub(crate) s_msg: String,
    pub(crate) sub_code: String,
    pub(crate) client_order_id: String,
    pub(crate) venue_order_id: Option<String>,
}

pub(crate) fn parse_order_response_item(
    item: &serde_json::Value,
    index: usize,
) -> Result<ParsedOrderResponseItem, ExecutionDispatchError> {
    let object = item.as_object().ok_or_else(|| {
        ExecutionDispatchError::conversion(format!(
            "order response item at index {index} is not an object"
        ))
    })?;
    let required = |field: &str| {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                ExecutionDispatchError::conversion(format!(
                    "order response item at index {index} has missing/non-string {field}"
                ))
            })
    };
    let optional = |field: &str| {
        object
            .get(field)
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    ExecutionDispatchError::conversion(format!(
                        "order response item at index {index} has non-string {field}"
                    ))
                })
            })
            .transpose()
    };
    let s_code = required(OKX_FIELD_SCODE)?;
    if s_code.is_empty() {
        return Err(ExecutionDispatchError::conversion(format!(
            "order response item at index {index} has empty sCode"
        )));
    }

    let client_order_id = optional(OKX_FIELD_CLORDID)?
        .or(optional("algoClOrdId")?)
        .unwrap_or_default();
    let venue_order_id = optional("ordId")?.or(optional("algoId")?);

    Ok(ParsedOrderResponseItem {
        s_code,
        s_msg: required(OKX_FIELD_SMSG)?,
        sub_code: optional(OKX_FIELD_SUBCODE)?.unwrap_or_default(),
        client_order_id,
        venue_order_id,
    })
}

#[derive(Debug, Clone)]
pub struct AlgoCancelContext {
    pub client_order_id: ClientOrderId,
    pub instrument_id: InstrumentId,
    pub strategy_id: StrategyId,
    pub venue_order_id: Option<VenueOrderId>,
}

// Contexts must correspond 1:1 with the requests that produced
// the responses (OKX preserves request order in batch responses).
pub fn emit_algo_cancel_rejections(
    responses: &[OKXCancelAlgoOrderResponse],
    contexts: &[AlgoCancelContext],
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
) {
    for (i, item) in responses.iter().enumerate() {
        let code = item.s_code.as_deref().unwrap_or(OKX_SUCCESS_CODE);
        if code == OKX_SUCCESS_CODE {
            continue;
        }

        let msg = item.s_msg.as_deref().unwrap_or("");

        if let Some(ctx) = contexts.get(i) {
            let ts = clock.get_time_ns();
            emitter.emit_order_cancel_rejected_event(
                ctx.strategy_id,
                ctx.instrument_id,
                ctx.client_order_id,
                ctx.venue_order_id,
                msg,
                ts,
            );
        } else {
            log::warn!(
                "Algo cancel rejected but no context at index {i}: \
                 algo_id={} sCode={code} sMsg={msg}",
                item.algo_id
            );
        }
    }
}

pub fn emit_batch_cancel_failure(
    contexts: &[AlgoCancelContext],
    error: &str,
    _emitter: &ExecutionEventEmitter,
    _clock: &'static AtomicTime,
) {
    for ctx in contexts {
        log::warn!(
            "Ambiguous algo batch cancel failure for {}, awaiting reconciliation: {error}",
            ctx.client_order_id
        );
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::live::{LiveIngressControl, LiveIngressFailureKind, LiveIngressLane};
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_model::enums::AccountType;
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::websocket::{
        client::{OKXWebSocketClient, OKXWebSocketQueue, OKXWebSocketQueueFailureKind},
        live_ingress_failure_callback,
    };

    #[rstest]
    #[case("0")]
    #[case("51000")]
    fn two_item_batch_response_resolves_native_aggregate_once(#[case] item_code: &str) {
        let client = OKXWebSocketClient::default();
        let request_id = "native-batch-request";
        client
            .admit_aggregate_for_test(OKXWsOperation::BatchOrders, request_id)
            .unwrap();
        let state = WsDispatchState::with_pending_responses(client.pending_response_handle());
        let account_id = AccountId::from("OKX-001");
        let emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            TraderId::from("TRADER-001"),
            account_id,
            AccountType::Margin,
            None,
        );
        let data = ["ORDER-001", "ORDER-002"]
            .into_iter()
            .map(|client_order_id| {
                json!({
                    "sCode": item_code,
                    "sMsg": if item_code == OKX_SUCCESS_CODE { "" } else { "rejected" },
                    "clOrdId": client_order_id,
                    "ordId": "12345",
                })
            })
            .collect();
        let message = OKXWsMessage::OrderResponse {
            id: Some(request_id.to_string()),
            op: OKXWsOperation::BatchOrders,
            code: "0".to_string(),
            msg: String::new(),
            data,
        };
        let instruments = AtomicMap::new();
        let mut fee_cache = AHashMap::new();
        let mut filled_qty_cache = AHashMap::new();
        let mut order_state_cache = AHashMap::new();

        dispatch_ws_message(
            message,
            &emitter,
            &state,
            account_id,
            &instruments,
            &mut fee_cache,
            &mut filled_qty_cache,
            &mut order_state_cache,
            get_atomic_clock_realtime(),
        );

        assert_eq!(client.pending_operation_count_for_test(), 0);
        assert_eq!(client.queue_failure_guard().failure(), None);
    }

    #[rstest]
    #[case("0", "")]
    #[case("1", "batch rejected")]
    fn data_less_batch_response_fails_native_aggregate_ambiguous(
        #[case] code: &str,
        #[case] message: &str,
    ) {
        let client = OKXWebSocketClient::default();
        let request_id = "native-data-less-request";
        client
            .admit_aggregate_for_test(OKXWsOperation::BatchCancelOrders, request_id)
            .unwrap();
        let state = WsDispatchState::with_pending_responses(client.pending_response_handle());
        let account_id = AccountId::from("OKX-001");
        let emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            TraderId::from("TRADER-001"),
            account_id,
            AccountType::Margin,
            None,
        );
        let instruments = AtomicMap::new();
        let mut replay_cache = BoundedExecutionReplayCache::default();
        let mut order_state_cache = AHashMap::new();

        let error = dispatch_ws_message_bounded(
            OKXWsMessage::OrderResponse {
                id: Some(request_id.to_string()),
                op: OKXWsOperation::BatchCancelOrders,
                code: code.to_string(),
                msg: message.to_string(),
                data: Vec::new(),
            },
            &emitter,
            &state,
            account_id,
            &instruments,
            &mut replay_cache,
            &mut order_state_cache,
            get_atomic_clock_realtime(),
        )
        .unwrap_err();

        assert_eq!(client.pending_operation_count_for_test(), 0);
        assert!(matches!(error, ExecutionDispatchError::PendingAmbiguous(_)));
        halt_on_execution_dispatch_error(&client.queue_failure_guard(), &error, "native test");
        assert_eq!(
            client.queue_failure_guard().failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[rstest]
    fn native_execution_conversion_halts_shared_live_ingress() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("ws_orders.json");
        let frame: crate::websocket::messages::OKXWsFrame =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let crate::websocket::messages::OKXWsFrame::Data { data, .. } = frame else {
            panic!("expected orders fixture");
        };
        let orders: Vec<OKXOrderMsg> = serde_json::from_value(data).unwrap();
        let client = OKXWebSocketClient::default();
        let live = LiveIngressControl::detached();
        client.set_queue_failure_callback(live_ingress_failure_callback(
            live.clone(),
            LiveIngressLane::ExecEvents,
        ));
        let account_id = AccountId::from("OKX-001");
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TRADER-001"),
            account_id,
            AccountType::Margin,
            None,
        );
        let state = WsDispatchState::default();
        let instruments = AtomicMap::new();
        let mut replay_cache = BoundedExecutionReplayCache::default();
        let mut order_state_cache = AHashMap::new();

        let error = dispatch_ws_message_bounded(
            OKXWsMessage::Orders(orders),
            &emitter,
            &state,
            account_id,
            &instruments,
            &mut replay_cache,
            &mut order_state_cache,
            clock,
        )
        .unwrap_err();
        assert!(matches!(error, ExecutionDispatchError::Conversion(_)));

        halt_on_execution_dispatch_error(&client.queue_failure_guard(), &error, "native test");
        assert!(live.is_halted());
        assert_eq!(
            client.queue_failure_guard().failure(),
            Some((
                OKXWebSocketQueue::Output,
                OKXWebSocketQueueFailureKind::Conversion,
            ))
        );
    }

    #[rstest]
    fn native_execution_reconnect_is_signalled_and_halt_policy_maps_to_integrity_failure() {
        let client = OKXWebSocketClient::default();
        let live = LiveIngressControl::detached();
        client.set_queue_failure_callback(live_ingress_failure_callback(
            live.clone(),
            LiveIngressLane::ExecEvents,
        ));
        let account_id = AccountId::from("OKX-001");
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TRADER-001"),
            account_id,
            AccountType::Margin,
            None,
        );
        let mut replay_cache = BoundedExecutionReplayCache::default();
        let mut order_state_cache = AHashMap::new();

        // Dispatch reports the reconnect as a signal; the consumer applies the policy.
        let outcome = dispatch_ws_message_bounded(
            OKXWsMessage::Reconnected,
            &emitter,
            &WsDispatchState::default(),
            account_id,
            &AtomicMap::new(),
            &mut replay_cache,
            &mut order_state_cache,
            clock,
        )
        .unwrap();
        assert_eq!(outcome, DispatchOutcome::Reconnected);
        assert!(live.failure().is_none());

        // The `Halt` policy (or a failed reconciliation) maps to the integrity fail-stop domain.
        halt_on_execution_dispatch_error(
            &client.queue_failure_guard(),
            &ExecutionDispatchError::ReconnectGap,
            "native test",
        );
        assert_eq!(
            live.failure().map(|failure| failure.kind),
            Some(LiveIngressFailureKind::Integrity),
        );
        assert_eq!(
            client.queue_failure_guard().failure(),
            Some((
                OKXWebSocketQueue::Output,
                OKXWebSocketQueueFailureKind::ReconnectGap,
            ))
        );
    }

    #[rstest]
    #[case("51000", "Rejected", "", "Rejected")]
    #[case("51000", "Rejected", "51004", "Rejected (subCode=51004)")]
    #[case("51000", "", "51004", "sCode=51000 subCode=51004")]
    #[case("51000", "", "", "sCode=51000")]
    #[case("", "", "51004", "subCode=51004")]
    #[case("", "", "", "")]
    fn test_format_order_response_reason(
        #[case] s_code: &str,
        #[case] s_msg: &str,
        #[case] sub_code: &str,
        #[case] expected: &str,
    ) {
        assert_eq!(
            format_order_response_reason(s_code, s_msg, sub_code),
            expected
        );
    }

    #[rstest]
    fn test_bounded_dedup_check_and_insert_returns_false_on_first_insert() {
        let dedup = BoundedDedup::<u32>::new(4);
        assert!(!dedup.check_and_insert(1));
        assert!(dedup.contains(&1));
    }

    #[rstest]
    fn test_bounded_dedup_check_and_insert_returns_true_on_duplicate() {
        let dedup = BoundedDedup::<u32>::new(4);
        dedup.insert(1);
        assert!(dedup.check_and_insert(1));
    }

    #[rstest]
    fn test_bounded_dedup_evicts_oldest_on_overflow() {
        let dedup = BoundedDedup::<u32>::new(3);
        dedup.insert(1);
        dedup.insert(2);
        dedup.insert(3);
        dedup.insert(4);

        assert!(!dedup.contains(&1));
        assert!(dedup.contains(&2));
        assert!(dedup.contains(&3));
        assert!(dedup.contains(&4));
    }

    #[rstest]
    fn test_bounded_dedup_evicted_key_is_not_treated_as_duplicate() {
        let dedup = BoundedDedup::<u32>::new(2);
        dedup.insert(1);
        dedup.insert(2);
        dedup.insert(3);

        assert!(!dedup.check_and_insert(1));
        assert!(dedup.contains(&1));
    }

    #[rstest]
    fn test_bounded_dedup_remove_drops_entry() {
        let dedup = BoundedDedup::<u32>::new(4);
        dedup.insert(1);
        assert!(dedup.remove(&1));
        assert!(!dedup.contains(&1));
        assert!(!dedup.remove(&1));
    }

    #[rstest]
    fn test_bounded_dedup_remove_then_reinsert_does_not_double_count() {
        let dedup = BoundedDedup::<u32>::new(2);
        for k in 0u32..1000 {
            dedup.insert(k);
            dedup.remove(&k);
        }
        assert!(!dedup.contains(&0));
        assert!(!dedup.contains(&500));
    }

    #[rstest]
    fn test_bounded_dedup_reinsert_survives_stale_marker_eviction() {
        let dedup = BoundedDedup::<u32>::new(3);
        dedup.insert(1);
        dedup.remove(&1);

        dedup.insert(2);
        dedup.insert(3);
        dedup.insert(1);

        assert!(dedup.contains(&1));
        assert!(dedup.contains(&2));
        assert!(dedup.contains(&3));
    }
}
