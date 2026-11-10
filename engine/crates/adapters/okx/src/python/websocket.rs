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

//! Python bindings for the OKX WebSocket client.
//!
//! # Design Pattern: Clone and Share State
//!
//! The WebSocket client must be cloned for async operations because PyO3's `future_into_py`
//! requires `'static` futures (cannot borrow from `self`). To ensure clones share the same
//! connection state, key fields use `Arc<RwLock<T>>`:
//!
//! - `inner: Arc<RwLock<Option<WebSocketClient>>>` - The WebSocket connection.
//!
//! Without shared state, clones would be independent, causing:
//! - Lost WebSocket messages.
//! - Missing instrument data.
//! - Connection state desynchronization.
//!
//! ## Connection Flow
//!
//! 1. Clone the client for async operation.
//! 2. Connect and populate shared state on the clone.
//! 3. Spawn stream handler as background task.
//! 4. Return immediately (non-blocking).
//!
//! ## Important Notes
//!
//! - Never use `block_on()` - it blocks the runtime.
//! - Always clone before async blocks for lifetime requirements.
//! - RwLock is preferred over Mutex (many reads, few writes).

use std::{str::FromStr, time::Duration};

use ahash::{AHashMap, AHashSet};
use futures_util::StreamExt;
use nautilus_common::{
    cache::quote::QuoteCache,
    live::{
        LiveIngressControl, LiveIngressLane, get_runtime,
        runner::{try_get_data_event_sender, try_get_exec_event_sender},
    },
    python::data_ingress::{PyDataIngress, fail_python_data_ingress},
};
use nautilus_core::{
    UUID4, UnixNanos,
    python::{call_python_threadsafe, params::value_to_pyobject, to_pyruntime_err, to_pyvalue_err},
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{BarType, Data, InstrumentStatus, OrderBookDeltas_API},
    enums::{OrderSide, OrderType, PositionSide, TimeInForce},
    events::{OrderAccepted, OrderCancelRejected, OrderModifyRejected, OrderRejected},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    python::{
        data::data_to_pycapsule,
        instruments::{instrument_any_to_pyobject, pyobject_to_instrument_any},
    },
    types::{Money, Price, Quantity},
};
use nautilus_network::websocket::TransportBackend;
use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    types::{PyDict, PyTuple},
};
use ustr::Ustr;

use super::{extract_optional_string, extract_optional_trigger_type};
use crate::{
    book_sync::{BookSequenceOutcome, BookSyncTracker},
    common::{
        consts::{OKX_FIELD_CLORDID, OKX_SUCCESS_CODE},
        enums::{
            OKXBookAction, OKXGreeksType, OKXInstrumentStatus, OKXInstrumentType, OKXTradeMode,
            OKXVipLevel,
        },
        models::OKXInstrument,
        parse::{
            okx_status_to_market_action, parse_account_state, parse_instrument_any,
            parse_instrument_id, parse_millisecond_timestamp, parse_position_status_report,
            parse_quantity,
        },
    },
    http::models::{OKXAccount, OKXPosition, OKXSpreadOrder},
    websocket::{
        OKXWebSocketClient,
        client::OKXWebSocketRuntimeHandle,
        dispatch::{
            ExecutionDispatchError, halt_on_execution_dispatch_error, parse_order_response_item,
        },
        enums::{OKXWsChannel, OKXWsOperation},
        live_ingress_failure_callback,
        messages::{
            ExecutionReport, NautilusWsMessage, OKXAlgoOrderMsg, OKXBookMsg, OKXOptionSummaryMsg,
            OKXOrderMsg, OKXRpiBookMsg, OKXWebSocketError, OKXWsMessage, WsAttachAlgoOrdParams,
            WsAttachAlgoOrdParamsBuilder,
        },
        parse::{
            extract_fees_from_cached_instrument, parse_algo_order_msg, parse_book_msg_vec,
            parse_fee_fill_cache_values, parse_index_price_msg_vec, parse_option_summary_greeks,
            parse_order_msg_with_previous, parse_rpi_book_msg_vec,
            parse_spread_order_msg_with_previous, parse_ws_message_data,
        },
        replay_cache::BoundedExecutionReplayCache,
    },
};

type PyBatchSubmitOrder = (
    OKXInstrumentType,
    InstrumentId,
    OKXTradeMode,
    ClientOrderId,
    OrderSide,
    OrderType,
    Quantity,
    Option<PositionSide>,
    Option<Price>,
    Option<Price>,
    Option<bool>,
    Option<bool>,
    Option<String>,
    Option<String>,
);

type PyLegacyBatchSubmitOrder = (
    OKXInstrumentType,
    InstrumentId,
    OKXTradeMode,
    ClientOrderId,
    OrderSide,
    OrderType,
    Quantity,
    Option<PositionSide>,
    Option<Price>,
    Option<Price>,
    Option<bool>,
    Option<bool>,
);

type PyBatchModifyOrder = (
    String,
    InstrumentId,
    ClientOrderId,
    Option<String>,
    Option<Price>,
    Option<Quantity>,
    Option<String>,
);

type PyLegacyBatchModifyOrder = (
    String,
    InstrumentId,
    ClientOrderId,
    Option<String>,
    Option<Price>,
    Option<Quantity>,
);

fn resolve_python_live_ingress(
    live_ingress_lane: &str,
) -> Result<(LiveIngressControl, LiveIngressLane), String> {
    let (control, lane) = match live_ingress_lane {
        "data" => {
            let sender = try_get_data_event_sender().ok_or_else(|| {
                "OKX Python live data connect requires the current runner TLS data sender"
                    .to_string()
            })?;
            if sender.lane() != LiveIngressLane::DataEvents
                || sender
                    .control()
                    .lane_metrics(LiveIngressLane::DataEvents)
                    .capacity
                    == 0
            {
                return Err(
                    "OKX Python live data connect requires a bounded DataEvents TLS sender"
                        .to_string(),
                );
            }
            (sender.control().clone(), LiveIngressLane::DataEvents)
        }
        "execution" => {
            let sender = try_get_exec_event_sender().ok_or_else(|| {
                "OKX Python live execution connect requires the current runner TLS execution sender"
                    .to_string()
            })?;
            if sender.lane() != LiveIngressLane::ExecEvents
                || sender
                    .control()
                    .lane_metrics(LiveIngressLane::ExecEvents)
                    .capacity
                    == 0
            {
                return Err(
                    "OKX Python live execution connect requires a bounded ExecEvents TLS sender"
                        .to_string(),
                );
            }
            (sender.control().clone(), LiveIngressLane::ExecEvents)
        }
        other => {
            return Err(format!(
                "Invalid OKX live_ingress_lane '{other}'; expected 'data' or 'execution'"
            ));
        }
    };

    if control.is_halted() {
        return Err(format!(
            "OKX Python live connect rejected: runner ingress is already halted ({:?})",
            control.failure()
        ));
    }
    Ok((control, lane))
}

fn extract_batch_submit_order(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<PyBatchSubmitOrder> {
    if let Ok(tuple) = obj.bind(py).cast::<PyTuple>()
        && tuple.len() == 14
    {
        return Ok((
            tuple.get_item(0)?.extract()?,
            tuple.get_item(1)?.extract()?,
            tuple.get_item(2)?.extract()?,
            tuple.get_item(3)?.extract()?,
            tuple.get_item(4)?.extract()?,
            tuple.get_item(5)?.extract()?,
            tuple.get_item(6)?.extract()?,
            tuple.get_item(7)?.extract()?,
            tuple.get_item(8)?.extract()?,
            tuple.get_item(9)?.extract()?,
            tuple.get_item(10)?.extract()?,
            tuple.get_item(11)?.extract()?,
            tuple.get_item(12)?.extract()?,
            tuple.get_item(13)?.extract()?,
        ));
    }

    let (
        instrument_type,
        instrument_id,
        td_mode,
        client_order_id,
        order_side,
        order_type,
        quantity,
        position_side,
        price,
        trigger_price,
        post_only,
        reduce_only,
    ): PyLegacyBatchSubmitOrder = obj.extract(py).map_err(to_pyruntime_err)?;

    Ok((
        instrument_type,
        instrument_id,
        td_mode,
        client_order_id,
        order_side,
        order_type,
        quantity,
        position_side,
        price,
        trigger_price,
        post_only,
        reduce_only,
        None,
        None,
    ))
}

fn parse_attach_algo_ords(
    py: Python<'_>,
    attach_algo_ords: Option<Vec<Py<PyDict>>>,
) -> PyResult<Option<Vec<WsAttachAlgoOrdParams>>> {
    attach_algo_ords
        .map(|items| {
            items
                .into_iter()
                .map(|item| {
                    let dict = item.bind(py);
                    let mut builder = WsAttachAlgoOrdParamsBuilder::default();

                    if let Some(value) = extract_optional_string(dict, "attach_algo_cl_ord_id")? {
                        builder.attach_algo_cl_ord_id(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "sl_trigger_px")? {
                        builder.sl_trigger_px(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "sl_ord_px")? {
                        builder.sl_ord_px(value);
                    }

                    if let Some(value) = extract_optional_trigger_type(dict, "sl_trigger_px_type")?
                    {
                        builder.sl_trigger_px_type(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "tp_trigger_px")? {
                        builder.tp_trigger_px(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "tp_ord_px")? {
                        builder.tp_ord_px(value);
                    }

                    if let Some(value) = extract_optional_trigger_type(dict, "tp_trigger_px_type")?
                    {
                        builder.tp_trigger_px_type(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "callback_ratio")? {
                        builder.callback_ratio(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "callback_spread")? {
                        builder.callback_spread(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "active_px")? {
                        builder.active_px(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "new_callback_ratio")? {
                        builder.new_callback_ratio(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "new_callback_spread")? {
                        builder.new_callback_spread(value);
                    }

                    if let Some(value) = extract_optional_string(dict, "new_active_px")? {
                        builder.new_active_px(value);
                    }

                    builder.build().map_err(to_pyvalue_err)
                })
                .collect::<PyResult<Vec<_>>>()
        })
        .transpose()
}

#[pyo3::pymethods]
impl OKXWebSocketError {
    #[getter]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[getter]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[getter]
    pub fn conn_id(&self) -> Option<&str> {
        self.conn_id.as_deref()
    }

    #[getter]
    pub fn ts_event(&self) -> u64 {
        self.timestamp
    }

    fn __repr__(&self) -> String {
        format!(
            "OKXWebSocketError(code='{}', message='{}', conn_id={:?}, ts_event={})",
            self.code, self.message, self.conn_id, self.timestamp
        )
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OKXWebSocketClient {
    /// Provides a WebSocket client for connecting to [OKX](https://okx.com).
    #[new]
    #[pyo3(signature = (url=None, api_key=None, api_secret=None, api_passphrase=None, account_id=None, heartbeat=None, auth_timeout_secs=None, proxy_url=None))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        url: Option<String>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        account_id: Option<AccountId>,
        heartbeat: Option<u64>,
        auth_timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> PyResult<Self> {
        Self::new(
            url,
            api_key,
            api_secret,
            api_passphrase,
            account_id,
            heartbeat,
            auth_timeout_secs,
            TransportBackend::default(),
            proxy_url,
        )
        .map_err(to_pyvalue_err)
    }

    /// Creates a new `OKXWebSocketClient` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if credential values cannot be loaded or if the
    /// client fails to initialize.
    #[staticmethod]
    #[pyo3(name = "with_credentials")]
    #[pyo3(signature = (url=None, api_key=None, api_secret=None, api_passphrase=None, account_id=None, heartbeat=None, auth_timeout_secs=None, proxy_url=None))]
    #[expect(clippy::too_many_arguments)]
    fn py_with_credentials(
        url: Option<String>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        account_id: Option<AccountId>,
        heartbeat: Option<u64>,
        auth_timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> PyResult<Self> {
        Self::with_credentials(
            url,
            api_key,
            api_secret,
            api_passphrase,
            account_id,
            heartbeat,
            auth_timeout_secs,
            TransportBackend::default(),
            proxy_url,
        )
        .map_err(to_pyvalue_err)
    }

    /// Creates a new authenticated `OKXWebSocketClient` using environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if required environment variables are missing or if
    /// the client fails to initialize.
    #[staticmethod]
    #[pyo3(name = "from_env")]
    fn py_from_env() -> PyResult<Self> {
        Self::from_env().map_err(to_pyvalue_err)
    }

    /// Returns the websocket url being used by the client.
    #[getter]
    #[pyo3(name = "url")]
    #[must_use]
    pub fn py_url(&self) -> &str {
        self.url()
    }

    /// Returns the public API key being used by the client.
    #[getter]
    #[pyo3(name = "api_key")]
    #[must_use]
    pub fn py_api_key(&self) -> Option<&str> {
        self.api_key()
    }

    /// Returns a masked version of the API key for logging purposes.
    #[getter]
    #[pyo3(name = "api_key_masked")]
    #[must_use]
    pub fn py_api_key_masked(&self) -> Option<String> {
        self.api_key_masked()
    }

    /// Returns a value indicating whether the client is active.
    #[pyo3(name = "is_active")]
    fn py_is_active(&mut self) -> bool {
        self.is_active()
    }

    /// Returns a value indicating whether the client is closed.
    #[pyo3(name = "is_closed")]
    fn py_is_closed(&mut self) -> bool {
        self.is_closed()
    }

    /// Cancel all pending WebSocket requests.
    #[pyo3(name = "cancel_all_requests")]
    pub fn py_cancel_all_requests(&self) {
        self.cancel_all_requests();
    }

    /// Get active subscriptions for a specific instrument.
    #[pyo3(name = "get_subscriptions")]
    fn py_get_subscriptions(&self, instrument_id: InstrumentId) -> Vec<String> {
        let channels = self.get_subscriptions(instrument_id);

        // Convert to OKX channel names
        channels
            .iter()
            .map(|c| {
                serde_json::to_value(c)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_else(|| c.to_string())
            })
            .collect()
    }

    /// Sets the VIP level for this client.
    ///
    /// The VIP level determines which WebSocket channels are available.
    #[pyo3(name = "set_vip_level")]
    fn py_set_vip_level(&self, vip_level: OKXVipLevel) {
        self.set_vip_level(vip_level);
    }

    /// Gets the current VIP level.
    #[pyo3(name = "vip_level")]
    #[getter]
    fn py_vip_level(&self) -> OKXVipLevel {
        self.vip_level()
    }

    /// Connect to the OKX WebSocket server.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection process fails.
    #[pyo3(name = "connect")]
    #[pyo3(signature = (loop_, instruments, callback, live_ingress_lane, python_data_ingress=None))]
    #[expect(clippy::needless_pass_by_value)]
    fn py_connect<'py>(
        &mut self,
        py: Python<'py>,
        loop_: Py<PyAny>,
        instruments: Vec<Py<PyAny>>,
        callback: Py<PyAny>,
        live_ingress_lane: &str,
        python_data_ingress: Option<PyRef<'_, PyDataIngress>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let call_soon: Py<PyAny> = if let Some(bridge) = python_data_ingress {
            let scheduler = bridge.scheduler(py, &loop_, live_ingress_lane)?;
            let failed = bridge.failure_callback();
            self.queue_failure_guard()
                .set_subscription_failure_callback(std::sync::Arc::clone(&failed));
            self.set_queue_failure_callback(std::sync::Arc::new(move |queue, kind| {
                failed(format!("okx_data_ingress_{queue:?}_{kind:?}"));
            }));
            scheduler
        } else {
            let (control, lane) =
                resolve_python_live_ingress(live_ingress_lane).map_err(to_pyruntime_err)?;
            self.set_queue_failure_callback(live_ingress_failure_callback(control, lane));
            loop_.getattr(py, "call_soon_threadsafe")?
        };

        let mut instruments_any = Vec::new();

        for inst in instruments {
            let inst_any = pyobject_to_instrument_any(py, inst)?;
            instruments_any.push(inst_any);
        }

        self.cache_instruments(&instruments_any);

        let mut client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.connect().await.map_err(to_pyruntime_err)?;

            let runtime_handle = client.runtime_handle();
            let account_id = client.account_id;
            let instruments_snapshot = client.instruments_snapshot();
            let option_greeks_subs_arc = client.option_greeks_subs().clone();
            let stream = client.stream();
            let clock = get_atomic_clock_realtime();
            // `stream` and `runtime_handle` deliberately carry no strong TaskOwner reference.
            // The temporary connect clone must be gone before the detached consumer starts so
            // dropping the Python object is the last-external-owner RAII shutdown boundary.
            drop(client);

            get_runtime().spawn(async move {
                let mut instruments_by_symbol = instruments_snapshot;
                let mut quote_cache = QuoteCache::new();
                let mut funding_cache: AHashMap<Ustr, (Ustr, u64)> = AHashMap::new();
                let mut replay_cache = BoundedExecutionReplayCache::default();
                let mut book_sync_by_channel: AHashMap<OKXWsChannel, BookSyncTracker> =
                    AHashMap::new();
                tokio::pin!(stream);

                while let Some(msg) = stream.next().await {
                    match msg {
                        OKXWsMessage::BookData { arg, action, data } => {
                            let Some(inst_id) = arg.inst_id else {
                                continue;
                            };
                            let Some(instrument) = instruments_by_symbol.get(&inst_id) else {
                                log::warn!("No cached instrument for book data: {inst_id}");
                                continue;
                            };
                            let instrument_id = instrument.id();
                            let sequences = data
                                .iter()
                                .map(|message| (message.prev_seq_id, message.seq_id))
                                .collect::<Vec<_>>();
                            let Some(data_vec) = parse_book_data(instrument, action, data, clock)
                            else {
                                record_python_data_failure(
                                    &call_soon,
                                    "okx_book_parse_failed".to_string(),
                                );
                                continue;
                            };
                            let outcome = validate_python_book_sequence(
                                &mut book_sync_by_channel,
                                arg.channel.clone(),
                                instrument_id,
                                action,
                                &sequences,
                            );

                            if handle_python_book_sequence(
                                &runtime_handle,
                                instrument_id,
                                arg.channel,
                                outcome,
                            )
                            .await
                            {
                                emit_book_data(data_vec, &call_soon, &callback);
                            }
                        }
                        OKXWsMessage::RpiBookData { arg, action, data } => {
                            let Some(inst_id) = arg.inst_id else {
                                continue;
                            };
                            let Some(instrument) = instruments_by_symbol.get(&inst_id) else {
                                log::warn!("No cached instrument for RPI book data: {inst_id}");
                                continue;
                            };
                            let instrument_id = instrument.id();
                            let sequences = data
                                .iter()
                                .map(|message| (Some(message.prev_seq_id), message.seq_id))
                                .collect::<Vec<_>>();
                            let Some(data_vec) =
                                parse_rpi_book_data(instrument, action, data, clock)
                            else {
                                record_python_data_failure(
                                    &call_soon,
                                    "okx_rpi_book_parse_failed".to_string(),
                                );
                                continue;
                            };
                            let outcome = validate_python_book_sequence(
                                &mut book_sync_by_channel,
                                arg.channel.clone(),
                                instrument_id,
                                action,
                                &sequences,
                            );

                            if handle_python_book_sequence(
                                &runtime_handle,
                                instrument_id,
                                arg.channel,
                                outcome,
                            )
                            .await
                            {
                                emit_book_data(data_vec, &call_soon, &callback);
                            }
                        }
                        OKXWsMessage::ChannelData {
                            channel,
                            inst_id,
                            data,
                        } => {
                            if channel == OKXWsChannel::Fills {
                                let error = ExecutionDispatchError::Conversion(
                                    "received unsupported OKX fills channel message; fills are \
                                     sourced from the orders channel"
                                        .to_string(),
                                );
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                            let greeks_guard = option_greeks_subs_arc.load();
                            handle_channel_data(
                                &channel,
                                inst_id,
                                data,
                                &mut instruments_by_symbol,
                                &mut quote_cache,
                                &mut funding_cache,
                                &greeks_guard,
                                clock,
                                &call_soon,
                                &callback,
                            );
                        }
                        OKXWsMessage::Instruments(okx_instruments) => {
                            handle_instruments(
                                okx_instruments,
                                &mut instruments_by_symbol,
                                clock,
                                &call_soon,
                                &callback,
                            );
                        }
                        OKXWsMessage::Orders(order_msgs) => {
                            if let Err(error) = handle_orders(
                                &order_msgs,
                                account_id,
                                &instruments_by_symbol,
                                &mut replay_cache,
                                clock,
                                &call_soon,
                                &callback,
                            ) {
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                        }
                        OKXWsMessage::SpreadOrders(order_msgs) => {
                            if let Err(error) = handle_spread_orders(
                                &order_msgs,
                                account_id,
                                &instruments_by_symbol,
                                &mut replay_cache,
                                clock,
                                &call_soon,
                                &callback,
                            ) {
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                        }
                        OKXWsMessage::AlgoOrders(algo_msgs) => {
                            if let Err(error) = handle_algo_orders(
                                algo_msgs,
                                account_id,
                                &instruments_by_symbol,
                                clock,
                                &call_soon,
                                &callback,
                            ) {
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                        }
                        OKXWsMessage::Account(data) => {
                            if let Err(error) =
                                handle_account(data, account_id, clock, &call_soon, &callback)
                            {
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                        }
                        OKXWsMessage::Positions(data) => {
                            if let Err(error) = handle_positions(
                                data,
                                account_id,
                                &instruments_by_symbol,
                                clock,
                                &call_soon,
                                &callback,
                            ) {
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                        }
                        OKXWsMessage::OrderResponse {
                            id,
                            op,
                            code,
                            msg,
                            data,
                        } => {
                            if let Err(error) = handle_order_response(
                                id.as_deref(),
                                &op,
                                &code,
                                &msg,
                                &data,
                                &runtime_handle,
                                account_id,
                                clock,
                                &call_soon,
                                &callback,
                            ) {
                                halt_on_execution_dispatch_error(
                                    &runtime_handle.queue_failure_guard(),
                                    &error,
                                    "Python",
                                );
                                break;
                            }
                        }
                        OKXWsMessage::SendFailed {
                            request_id,
                            client_order_id,
                            op,
                            error,
                        } => {
                            handle_send_failed(
                                &request_id,
                                client_order_id,
                                op.as_ref(),
                                &error,
                                &runtime_handle,
                            );
                        }
                        OKXWsMessage::Error(msg) => {
                            call_python_with_data(&call_soon, &callback, |py| msg.into_py_any(py));
                        }
                        OKXWsMessage::Reconnected => {
                            quote_cache.clear();
                            book_sync_by_channel.clear();
                        }
                        OKXWsMessage::Authenticated => {}
                    }
                }
                // The explicit node closes its bridge before a normal client
                // disconnect. Otherwise a vanished consumer is terminal data
                // loss, not an idle/healthy connection. TLS callers unchanged.
                record_python_data_failure(
                    &call_soon,
                    "okx_python_data_consumer_stopped".to_string(),
                );
            });

            Ok(())
        })
    }

    /// Wait until the WebSocket connection is active.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection times out.
    #[pyo3(name = "wait_until_active")]
    fn py_wait_until_active<'py>(
        &self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .wait_until_active(timeout_secs)
                .await
                .map_err(to_pyruntime_err)?;
            Ok(())
        })
    }

    /// Closes the client.
    ///
    /// # Errors
    ///
    /// Returns an error if disconnecting the websocket or cleaning up the
    /// client fails.
    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.close().await {
                log::warn!("Error on close: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to instrument updates for a specific instrument type.
    ///
    /// Provides updates when instrument specifications change.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-instruments-channel>.
    #[pyo3(name = "subscribe_instruments")]
    fn py_subscribe_instruments<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_instruments(instrument_type).await {
                log::error!("Failed to subscribe to instruments '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to instrument updates for a specific instrument.
    ///
    /// Since OKX doesn't support subscribing to individual instruments via `instId`,
    /// this method subscribes to the entire instrument type. OKX handles duplicate
    /// subscriptions gracefully and pushes a fresh snapshot on each subscribe.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-instruments-channel>.
    #[pyo3(name = "subscribe_instrument")]
    fn py_subscribe_instrument<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_instrument(instrument_id).await {
                log::error!("Failed to subscribe to instrument {instrument_id}: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to order book data for an instrument.
    ///
    /// This is a convenience method that calls `Self.subscribe_book_with_depth` with depth 0,
    /// which automatically selects the appropriate channel based on VIP level.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_book")]
    fn py_subscribe_book<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .subscribe_book(instrument_id)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to 50-level tick-by-tick order book data for an instrument.
    ///
    /// Provides real-time updates whenever order book changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-50-depth-tbt-channel>.
    #[pyo3(name = "subscribe_book50_l2_tbt")]
    fn py_subscribe_book50_l2_tbt<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_book50_l2_tbt(instrument_id).await {
                log::error!("Failed to subscribe to book50_tbt: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to tick-by-tick full depth (400 levels) order book data for an instrument.
    ///
    /// Provides real-time updates with all depth levels whenever order book changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-400-depth-tbt-channel>.
    #[pyo3(name = "subscribe_book_l2_tbt")]
    fn py_subscribe_book_l2_tbt<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_book_l2_tbt(instrument_id).await {
                log::error!("Failed to subscribe to books_l2_tbt: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to order book data with automatic channel selection based on VIP level and depth.
    ///
    /// Selects the optimal channel based on user's VIP tier and requested depth:
    /// - depth 50: Requires VIP4+, subscribes to `books50-l2-tbt`
    /// - depth 0 or 400:
    ///   - VIP5+: subscribes to `books-l2-tbt` (400 depth, fastest)
    ///   - Below VIP5: subscribes to `books` (standard depth)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Subscription request fails
    /// - depth is 50 but VIP level is below 4
    #[pyo3(name = "subscribe_book_with_depth")]
    fn py_subscribe_book_with_depth<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        depth: u16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .subscribe_book_with_depth(instrument_id, depth)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to 5-level order book snapshot data for an instrument.
    ///
    /// Updates every 100ms when there are changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-5-depth-channel>.
    #[pyo3(name = "subscribe_book_depth5")]
    fn py_subscribe_book_depth5<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_book_depth5(instrument_id).await {
                log::error!("Failed to subscribe to books5: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to best bid/ask quote data for an instrument.
    ///
    /// Provides tick-by-tick updates of the best bid and ask prices using the bbo-tbt channel.
    /// Supports all instrument types: SPOT, MARGIN, SWAP, FUTURES, OPTION.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-best-bid-offer-channel>.
    #[pyo3(name = "subscribe_quotes")]
    fn py_subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_quotes(instrument_id).await {
                log::error!("Failed to subscribe to quotes: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to trade data for an instrument.
    ///
    /// When `aggregated` is `false`, subscribes to the `trades` channel (per-match updates).
    /// When `aggregated` is `true`, subscribes to the `trades-all` channel (aggregated updates).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-trades-channel>.
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-all-trades-channel>.
    #[pyo3(name = "subscribe_trades")]
    fn py_subscribe_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        aggregated: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_trades(instrument_id, aggregated).await {
                log::error!("Failed to subscribe to trades: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to candlestick/bar data for an instrument.
    ///
    /// Supports various time intervals from 1s to 3M.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-candlesticks-channel>.
    #[pyo3(name = "subscribe_bars")]
    fn py_subscribe_bars<'py>(
        &self,
        py: Python<'py>,
        bar_type: BarType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_bars(bar_type).await {
                log::error!("Failed to subscribe to bars: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from full order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_book")]
    fn py_unsubscribe_book<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_book(instrument_id).await {
                log::error!("Failed to unsubscribe from order book: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from 5-level order book snapshot data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_book_depth5")]
    fn py_unsubscribe_book_depth5<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_book_depth5(instrument_id).await {
                log::error!("Failed to unsubscribe from books5: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from 50-level tick-by-tick order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_book50_l2_tbt")]
    fn py_unsubscribe_book50_l2_tbt<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_book50_l2_tbt(instrument_id).await {
                log::error!("Failed to unsubscribe from books50_l2_tbt: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from tick-by-tick full depth order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_book_l2_tbt")]
    fn py_unsubscribe_book_l2_tbt<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_book_l2_tbt(instrument_id).await {
                log::error!("Failed to unsubscribe from books_l2_tbt: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from best bid/ask quote data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_quotes")]
    fn py_unsubscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_quotes(instrument_id).await {
                log::error!("Failed to unsubscribe from quotes: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from trade data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_trades")]
    fn py_unsubscribe_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        aggregated: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_trades(instrument_id, aggregated).await {
                log::error!("Failed to unsubscribe from trades: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from candlestick/bar data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_bars")]
    fn py_unsubscribe_bars<'py>(
        &self,
        py: Python<'py>,
        bar_type: BarType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_bars(bar_type).await {
                log::error!("Failed to unsubscribe from bars: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to 24hr rolling ticker data for an instrument.
    ///
    /// Updates every 100ms with trading statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-tickers-channel>.
    #[pyo3(name = "subscribe_ticker")]
    fn py_subscribe_ticker<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_ticker(instrument_id).await {
                log::error!("Failed to subscribe to ticker: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from 24hr rolling ticker data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_ticker")]
    fn py_unsubscribe_ticker<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_ticker(instrument_id).await {
                log::error!("Failed to unsubscribe from ticker: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to mark price data for derivatives instruments.
    ///
    /// Updates every 200ms for perpetual swaps, or at settlement for futures.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-mark-price-channel>.
    #[pyo3(name = "subscribe_mark_prices")]
    fn py_subscribe_mark_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_mark_prices(instrument_id).await {
                log::error!("Failed to subscribe to mark prices: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from mark price data for a derivatives instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_mark_prices")]
    fn py_unsubscribe_mark_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_mark_prices(instrument_id).await {
                log::error!("Failed to unsubscribe from mark prices: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to index price data for an instrument.
    ///
    /// Updates every second with the underlying index price.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-index-tickers-channel>.
    #[pyo3(name = "subscribe_index_prices")]
    fn py_subscribe_index_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_index_prices(instrument_id).await {
                log::error!("Failed to subscribe to index prices: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from index price data for the base pair derived from
    /// `instrument_id`.
    ///
    /// Refcounting is handled internally so any caller (Rust data client,
    /// Python wrapper, etc.) can pair every `subscribe_index_prices` with
    /// exactly one `unsubscribe_index_prices`. The OKX `index-tickers`
    /// channel is keyed by base pair (e.g. `BTC-USDT`), so the venue
    /// unsubscribe only fires when the last subscriber for that pair drops.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    #[pyo3(name = "unsubscribe_index_prices")]
    fn py_unsubscribe_index_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_index_prices(instrument_id).await {
                log::error!("Failed to unsubscribe from index prices: {e}");
            }
            Ok(())
        })
    }

    /// Adds an instrument to the option greeks subscription filter, emitting both
    /// Black-Scholes and price-adjusted greeks.
    #[pyo3(name = "add_option_greeks_sub")]
    fn py_add_option_greeks_sub(&self, instrument_id: InstrumentId) {
        self.add_option_greeks_sub(instrument_id);
    }

    /// Adds an instrument to the option greeks subscription filter with an explicit
    /// set of greeks conventions to emit. An empty set is treated as "emit both".
    #[pyo3(name = "add_option_greeks_sub_with_conventions")]
    fn py_add_option_greeks_sub_with_conventions(
        &self,
        instrument_id: InstrumentId,
        conventions: Vec<OKXGreeksType>,
    ) {
        self.add_option_greeks_sub_with_conventions(
            instrument_id,
            conventions.into_iter().collect(),
        );
    }

    /// Removes an instrument from the option greeks subscription filter.
    #[pyo3(name = "remove_option_greeks_sub")]
    fn py_remove_option_greeks_sub(&self, instrument_id: InstrumentId) {
        self.remove_option_greeks_sub(&instrument_id);
    }

    /// Subscribes to option summary data for an instrument family.
    ///
    /// Streams greeks (delta, gamma, vega, theta), implied volatility, and other
    /// option metrics for all instruments in the specified family.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-option-summary-channel>.
    #[pyo3(name = "subscribe_option_summary")]
    fn py_subscribe_option_summary<'py>(
        &self,
        py: Python<'py>,
        inst_family: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let family = Ustr::from(inst_family);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_option_summary(family).await {
                log::error!("Failed to subscribe to option summary: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from option summary data for an instrument family.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    #[pyo3(name = "unsubscribe_option_summary")]
    fn py_unsubscribe_option_summary<'py>(
        &self,
        py: Python<'py>,
        inst_family: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let family = Ustr::from(inst_family);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_option_summary(family).await {
                log::error!("Failed to unsubscribe from option summary: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to funding rate data for perpetual swap instruments.
    ///
    /// Updates when funding rate changes or at funding intervals.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-funding-rate-channel>.
    #[pyo3(name = "subscribe_funding_rates")]
    fn py_subscribe_funding_rates<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_funding_rates(instrument_id).await {
                log::error!("Failed to subscribe to funding rates: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribe from funding rate data for a perpetual swap instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_funding_rates")]
    fn py_unsubscribe_funding_rates<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_funding_rates(instrument_id).await {
                log::error!("Failed to unsubscribe from funding rates: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to event contract market updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-event-contract-markets-channel>.
    #[pyo3(name = "subscribe_event_contract_markets")]
    fn py_subscribe_event_contract_markets<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_event_contract_markets().await {
                log::error!("Failed to subscribe to event contract markets: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from event contract market updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    #[pyo3(name = "unsubscribe_event_contract_markets")]
    fn py_unsubscribe_event_contract_markets<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_event_contract_markets().await {
                log::error!("Failed to unsubscribe from event contract markets: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_orders")]
    fn py_subscribe_orders<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_orders(instrument_type).await {
                log::error!("Failed to subscribe to orders '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_orders")]
    fn py_unsubscribe_orders<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_orders(instrument_type).await {
                log::error!("Failed to unsubscribe from orders '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to Nitro spread order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_spread_orders")]
    fn py_subscribe_spread_orders<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_spread_orders().await {
                log::error!("Failed to subscribe to spread orders: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from Nitro spread order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_spread_orders")]
    fn py_unsubscribe_spread_orders<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_spread_orders().await {
                log::error!("Failed to unsubscribe from spread orders: {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to algo order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_orders_algo")]
    fn py_subscribe_orders_algo<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_orders_algo(instrument_type).await {
                log::error!("Failed to subscribe to algo orders '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from algo order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_orders_algo")]
    fn py_unsubscribe_orders_algo<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_orders_algo(instrument_type).await {
                log::error!("Failed to unsubscribe from algo orders '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to advance algo order updates (trailing stops, iceberg, twap).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_algo_advance")]
    fn py_subscribe_algo_advance<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_algo_advance(instrument_type).await {
                log::error!("Failed to subscribe to algo-advance '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from advance algo order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_algo_advance")]
    fn py_unsubscribe_algo_advance<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_algo_advance(instrument_type).await {
                log::error!("Failed to unsubscribe from algo-advance '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to fill updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_fills")]
    fn py_subscribe_fills<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_fills(instrument_type).await {
                log::error!("Failed to subscribe to fills '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from fill updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_fills")]
    fn py_unsubscribe_fills<'py>(
        &self,
        py: Python<'py>,
        instrument_type: OKXInstrumentType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_fills(instrument_type).await {
                log::error!("Failed to unsubscribe from fills '{instrument_type}': {e}");
            }
            Ok(())
        })
    }

    /// Subscribes to account balance updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "subscribe_account")]
    fn py_subscribe_account<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.subscribe_account().await {
                log::error!("Failed to subscribe to account: {e}");
            }
            Ok(())
        })
    }

    /// Unsubscribes from account balance updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    #[pyo3(name = "unsubscribe_account")]
    fn py_unsubscribe_account<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = client.unsubscribe_account().await {
                log::error!("Failed to unsubscribe from account: {e}");
            }
            Ok(())
        })
    }

    /// Submits an order, automatically routing conditional orders to the algo endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the order parameters are invalid or if the request
    /// cannot be sent to the websocket client.
    ///
    /// # References
    ///
    /// - Regular orders: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-place-order>
    /// - Algo orders: <https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-place-algo-order>
    #[pyo3(name = "submit_order")]
    #[pyo3(signature = (
        trader_id,
        strategy_id,
        instrument_id,
        td_mode,
        client_order_id,
        order_side,
        order_type,
        quantity,
        time_in_force=None,
        price=None,
        trigger_price=None,
        post_only=None,
        reduce_only=None,
        quote_quantity=None,
        position_side=None,
        attach_algo_ords=None,
        px_usd=None,
        px_vol=None,
        speed_bump=None,
        outcome=None,
        slippage_pct=None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_submit_order<'py>(
        &self,
        py: Python<'py>,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        td_mode: OKXTradeMode,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        time_in_force: Option<TimeInForce>,
        price: Option<Price>,
        trigger_price: Option<Price>,
        post_only: Option<bool>,
        reduce_only: Option<bool>,
        quote_quantity: Option<bool>,
        position_side: Option<PositionSide>,
        attach_algo_ords: Option<Vec<Py<PyDict>>>,
        px_usd: Option<String>,
        px_vol: Option<String>,
        speed_bump: Option<String>,
        outcome: Option<String>,
        slippage_pct: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let attach_algo_ords = parse_attach_algo_ords(py, attach_algo_ords)?;
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .submit_order(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    td_mode,
                    client_order_id,
                    order_side,
                    order_type,
                    quantity,
                    time_in_force,
                    price,
                    trigger_price,
                    post_only,
                    reduce_only,
                    quote_quantity,
                    position_side,
                    attach_algo_ords,
                    px_usd,
                    px_vol,
                    speed_bump,
                    outcome,
                    slippage_pct,
                    None,
                    None,
                    None,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Cancels an existing order.
    ///
    /// # Errors
    ///
    /// Returns an error if the cancel parameters are invalid or if the
    /// cancellation request fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-cancel-order>.
    #[pyo3(name = "cancel_order", signature = (
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id=None,
        venue_order_id=None,
    ))]
    fn py_cancel_order<'py>(
        &self,
        py: Python<'py>,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .cancel_order(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Place a new order via WebSocket.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-place-order>
    /// Modifies an existing order.
    ///
    /// # Errors
    ///
    /// Returns an error if the amend parameters are invalid or if the
    /// websocket request fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-amend-order>.
    #[pyo3(name = "modify_order")]
    #[pyo3(signature = (
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id=None,
        venue_order_id=None,
        price=None,
        quantity=None,
        new_px_usd=None,
        new_px_vol=None,
        speed_bump=None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_modify_order<'py>(
        &self,
        py: Python<'py>,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
        price: Option<Price>,
        quantity: Option<Quantity>,
        new_px_usd: Option<String>,
        new_px_vol: Option<String>,
        speed_bump: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .modify_order(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    price,
                    quantity,
                    venue_order_id,
                    new_px_usd,
                    new_px_vol,
                    speed_bump,
                    None,
                    None,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Submits multiple orders.
    ///
    /// # Errors
    ///
    /// Returns an error if any batch order parameters are invalid or if the
    /// batch request fails to send.
    #[pyo3(name = "batch_submit_orders")]
    fn py_batch_submit_orders<'py>(
        &self,
        py: Python<'py>,
        orders: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut domain_orders = Vec::with_capacity(orders.len());

        for obj in orders {
            let (
                instrument_type,
                instrument_id,
                td_mode,
                client_order_id,
                order_side,
                order_type,
                quantity,
                position_side,
                price,
                trigger_price,
                post_only,
                reduce_only,
                speed_bump,
                outcome,
            ) = extract_batch_submit_order(py, &obj)?;

            domain_orders.push((
                instrument_type,
                instrument_id,
                td_mode,
                client_order_id,
                order_side,
                position_side,
                order_type,
                quantity,
                price,
                trigger_price,
                post_only,
                reduce_only,
                speed_bump,
                outcome,
                None,
                None,
                None,
            ));
        }

        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .batch_submit_orders(domain_orders)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Cancels multiple orders.
    ///
    /// Supports up to 20 orders per batch.
    ///
    /// # Errors
    ///
    /// Returns an error if cancel parameters are invalid or if the batch
    /// request fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-batch-cancel-orders>
    #[pyo3(name = "batch_cancel_orders")]
    fn py_batch_cancel_orders<'py>(
        &self,
        py: Python<'py>,
        cancels: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut batched_cancels = Vec::with_capacity(cancels.len());

        for obj in cancels {
            let (instrument_id, client_order_id, order_id): (
                InstrumentId,
                Option<ClientOrderId>,
                Option<VenueOrderId>,
            ) = obj.extract(py).map_err(to_pyruntime_err)?;
            batched_cancels.push((instrument_id, client_order_id, order_id));
        }

        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .batch_cancel_orders(batched_cancels)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Modifies multiple orders.
    ///
    /// # Errors
    ///
    /// Returns an error if amend parameters are invalid or if the batch request
    /// fails to send.
    #[pyo3(name = "batch_modify_orders")]
    fn py_batch_modify_orders<'py>(
        &self,
        py: Python<'py>,
        orders: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut domain_orders = Vec::with_capacity(orders.len());

        for obj in orders {
            let extracted_with_event_params: PyResult<PyBatchModifyOrder> =
                obj.extract(py).map_err(to_pyruntime_err);

            let (
                instrument_type,
                instrument_id,
                client_order_id,
                request_id,
                price,
                quantity,
                speed_bump,
            ) = if let Ok(values) = extracted_with_event_params {
                values
            } else {
                let (
                    instrument_type,
                    instrument_id,
                    client_order_id,
                    request_id,
                    price,
                    quantity,
                ): PyLegacyBatchModifyOrder = obj.extract(py).map_err(to_pyruntime_err)?;

                (
                    instrument_type,
                    instrument_id,
                    client_order_id,
                    request_id,
                    price,
                    quantity,
                    None,
                )
            };
            let inst_type =
                OKXInstrumentType::from_str(&instrument_type).map_err(to_pyvalue_err)?;
            domain_orders.push((
                inst_type,
                instrument_id,
                client_order_id,
                request_id,
                price,
                quantity,
                speed_bump,
                None,
                None,
            ));
        }

        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .batch_modify_orders(domain_orders)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Mass cancels all orders for a given instrument via WebSocket.
    ///
    /// # Errors
    ///
    /// Returns an error if instrument metadata cannot be resolved or if the
    /// cancel request fails to send.
    ///
    /// # References
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-mass-cancel-order>
    #[pyo3(name = "mass_cancel_orders")]
    fn py_mass_cancel_orders<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .mass_cancel_orders(instrument_id)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Caches multiple instruments.
    ///
    /// Any existing instruments with the same symbols will be replaced.
    #[pyo3(name = "cache_instruments")]
    fn py_cache_instruments(&self, py: Python<'_>, instruments: Vec<Py<PyAny>>) -> PyResult<()> {
        let instruments: Result<Vec<_>, _> = instruments
            .into_iter()
            .map(|inst| pyobject_to_instrument_any(py, inst))
            .collect();
        self.cache_instruments(&instruments?);
        Ok(())
    }

    /// Caches a single instrument.
    ///
    /// Any existing instrument with the same symbol will be replaced.
    #[pyo3(name = "cache_instrument")]
    fn py_cache_instrument(&self, py: Python<'_>, instrument: Py<PyAny>) -> PyResult<()> {
        self.cache_instrument(pyobject_to_instrument_any(py, instrument)?);
        Ok(())
    }

    /// Caches multiple instIdCode mappings for instruments.
    ///
    /// This is typically called after loading instruments from the HTTP API.
    #[pyo3(name = "cache_inst_id_codes")]
    fn py_cache_inst_id_codes(&self, mappings: Vec<(String, u64)>) {
        let ustr_mappings = mappings
            .into_iter()
            .map(|(inst_id, code)| (Ustr::from(&inst_id), code));
        self.cache_inst_id_codes(ustr_mappings);
    }
}

fn validate_python_book_sequence(
    book_sync_by_channel: &mut AHashMap<OKXWsChannel, BookSyncTracker>,
    channel: OKXWsChannel,
    instrument_id: InstrumentId,
    action: OKXBookAction,
    sequences: &[(Option<i64>, u64)],
) -> BookSequenceOutcome {
    book_sync_by_channel
        .entry(channel)
        .or_default()
        .validate_sequence(
            instrument_id,
            action == OKXBookAction::Snapshot,
            sequences,
            Duration::ZERO,
            std::time::Instant::now(),
        )
}

async fn handle_python_book_sequence(
    runtime_handle: &OKXWebSocketRuntimeHandle,
    instrument_id: InstrumentId,
    channel: OKXWsChannel,
    outcome: BookSequenceOutcome,
) -> bool {
    match outcome {
        BookSequenceOutcome::Accept => true,
        BookSequenceOutcome::Suppress => false,
        BookSequenceOutcome::Recover {
            last_seq_id,
            prev_seq_id,
            seq_id,
        } => {
            log::warn!(
                "{channel} sequence gap for {instrument_id}: last_seq_id={last_seq_id:?}, \
                 prev_seq_id={prev_seq_id:?}, seq_id={seq_id}; requesting a fresh snapshot"
            );

            if let Err(e) = runtime_handle
                .resubscribe_ws_channel(instrument_id, channel.clone())
                .await
            {
                log::error!("Failed to recover {channel} sequence for {instrument_id}: {e}");
            }
            false
        }
    }
}

fn parse_book_data(
    instrument: &InstrumentAny,
    action: OKXBookAction,
    data: Vec<OKXBookMsg>,
    clock: &AtomicTime,
) -> Option<Vec<Data>> {
    let ts_init = clock.get_time_ns();

    match parse_book_msg_vec(
        data,
        &instrument.id(),
        instrument.price_precision(),
        instrument.size_precision(),
        action,
        ts_init,
    ) {
        Ok(data) => Some(data),
        Err(e) => {
            log::error!("Failed to parse book data: {e}");
            None
        }
    }
}

fn parse_rpi_book_data(
    instrument: &InstrumentAny,
    action: OKXBookAction,
    data: Vec<OKXRpiBookMsg>,
    clock: &AtomicTime,
) -> Option<Vec<Data>> {
    let ts_init = clock.get_time_ns();

    match parse_rpi_book_msg_vec(
        data,
        &instrument.id(),
        instrument.price_precision(),
        instrument.size_precision(),
        action,
        ts_init,
    ) {
        Ok(data) => Some(data),
        Err(e) => {
            log::error!("Failed to parse RPI book data: {e}");
            None
        }
    }
}

fn emit_book_data(data: Vec<Data>, call_soon: &Py<PyAny>, callback: &Py<PyAny>) {
    Python::attach(|py| {
        for data in data {
            let py_obj = data_to_pycapsule(py, data);
            call_python_threadsafe(py, call_soon, callback, py_obj);
        }
    });
}

#[expect(clippy::too_many_arguments)]
fn handle_channel_data(
    channel: &OKXWsChannel,
    inst_id: Option<Ustr>,
    data: serde_json::Value,
    instruments_by_symbol: &mut AHashMap<Ustr, InstrumentAny>,
    quote_cache: &mut QuoteCache,
    funding_cache: &mut AHashMap<Ustr, (Ustr, u64)>,
    option_greeks_subs: &AHashMap<InstrumentId, AHashSet<OKXGreeksType>>,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) {
    if matches!(channel, OKXWsChannel::OptionSummary) {
        let ts_init = clock.get_time_ns();

        match serde_json::from_value::<Vec<OKXOptionSummaryMsg>>(data) {
            Ok(msgs) => {
                for msg in &msgs {
                    let Some(instrument) = instruments_by_symbol.get(&msg.inst_id) else {
                        continue;
                    };
                    let instrument_id = instrument.id();
                    let Some(conventions) = option_greeks_subs.get(&instrument_id) else {
                        continue;
                    };

                    for greeks_type in conventions {
                        match parse_option_summary_greeks(
                            msg,
                            &instrument_id,
                            *greeks_type,
                            ts_init,
                        ) {
                            Ok(greeks) => {
                                Python::attach(|py| match greeks.into_py_any(py) {
                                    Ok(py_obj) => {
                                        call_python_threadsafe(py, call_soon, callback, py_obj);
                                    }
                                    Err(e) => {
                                        fail_python_data_ingress(
                                            py,
                                            call_soon,
                                            format!("option_greeks_conversion_failed: {e}"),
                                        );
                                        log::error!(
                                            "Failed to convert OptionGreeks to Python: {e}"
                                        );
                                    }
                                });
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to parse option summary for {} ({greeks_type:?}): {e}",
                                    msg.inst_id
                                );
                            }
                        }
                    }
                }
            }
            Err(e) => log::error!("Failed to deserialize option summary data: {e}"),
        }
        return;
    }

    if matches!(channel, OKXWsChannel::EventContractMarkets) {
        dispatch_json_value_to_python(&data, call_soon, callback);
        return;
    }

    let Some(inst_id) = inst_id else { return };

    if matches!(channel, OKXWsChannel::IndexTickers) {
        let ts_init = clock.get_time_ns();
        let prefix = format!("{inst_id}-");
        let matching: Vec<_> = instruments_by_symbol
            .values()
            .filter(|i| {
                let s = i.symbol().inner();
                s == inst_id || s.as_str().starts_with(&prefix)
            })
            .collect();

        for instrument in matching {
            if let Ok(data_vec) = parse_index_price_msg_vec(
                data.clone(),
                &instrument.id(),
                instrument.price_precision(),
                ts_init,
            ) {
                Python::attach(|py| {
                    for d in data_vec {
                        let py_obj = data_to_pycapsule(py, d);
                        call_python_threadsafe(py, call_soon, callback, py_obj);
                    }
                });
            }
        }
        return;
    }

    let Some(instrument) = instruments_by_symbol.get(&inst_id) else {
        log::warn!("No cached instrument for {channel:?}: {inst_id}");
        return;
    };
    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let ts_init = clock.get_time_ns();

    if matches!(channel, OKXWsChannel::BboTbt) {
        handle_bbo_tbt(
            data,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
            quote_cache,
            call_soon,
            callback,
        );
        return;
    }

    match parse_ws_message_data(
        channel,
        data,
        &instrument_id,
        price_precision,
        size_precision,
        ts_init,
        funding_cache,
        instruments_by_symbol,
    ) {
        Ok(Some(ws_msg)) => {
            dispatch_nautilus_ws_msg_to_python(ws_msg, call_soon, callback, instruments_by_symbol);
        }
        Ok(None) => {}
        Err(e) => {
            record_python_data_failure(call_soon, format!("okx_data_parse_failed: {e}"));
            log::error!("Failed to parse {channel:?} data: {e}");
        }
    }
}

#[expect(clippy::too_many_arguments)]
fn handle_bbo_tbt(
    data: serde_json::Value,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
    quote_cache: &mut QuoteCache,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) {
    let msgs: Vec<OKXBookMsg> = match serde_json::from_value(data) {
        Ok(msgs) => msgs,
        Err(e) => {
            record_python_data_failure(call_soon, format!("okx_bbo_deserialize_failed: {e}"));
            log::error!("Failed to deserialize BboTbt data: {e}");
            return;
        }
    };

    for msg in &msgs {
        let bid = msg.bids.first();
        let ask = msg.asks.first();

        let bid_price = bid.and_then(|e| Price::from_decimal_dp(e.price, price_precision).ok());
        let bid_size = bid.and_then(|e| Quantity::from_decimal_dp(e.size, size_precision).ok());
        let ask_price = ask.and_then(|e| Price::from_decimal_dp(e.price, price_precision).ok());
        let ask_size = ask.and_then(|e| Quantity::from_decimal_dp(e.size, size_precision).ok());
        let ts_event = parse_millisecond_timestamp(msg.ts);

        match quote_cache.process(
            instrument_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            ts_event,
            ts_init,
        ) {
            Ok(quote) => {
                Python::attach(|py| {
                    let py_obj = data_to_pycapsule(py, Data::Quote(quote));
                    call_python_threadsafe(py, call_soon, callback, py_obj);
                });
            }
            Err(e) => {
                log::debug!("Skipping partial BboTbt for {instrument_id}: {e}");
            }
        }
    }
}

fn handle_instruments(
    okx_instruments: Vec<OKXInstrument>,
    instruments_by_symbol: &mut AHashMap<Ustr, InstrumentAny>,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) {
    let ts_init = clock.get_time_ns();

    for okx_inst in okx_instruments {
        let inst_key = Ustr::from(&okx_inst.inst_id);
        let (margin_init, margin_maint, maker_fee, taker_fee) =
            instruments_by_symbol.get(&inst_key).map_or(
                (None, None, None, None),
                extract_fees_from_cached_instrument,
            );
        let status_action = okx_status_to_market_action(okx_inst.state);
        let is_live = matches!(okx_inst.state, OKXInstrumentStatus::Live);

        match parse_instrument_any(
            &okx_inst,
            margin_init,
            margin_maint,
            maker_fee,
            taker_fee,
            ts_init,
        ) {
            Ok(Some(inst_any)) => {
                let instrument_id = inst_any.id();
                instruments_by_symbol.insert(inst_any.symbol().inner(), inst_any.clone());
                call_python_with_data(call_soon, callback, |py| {
                    instrument_any_to_pyobject(py, inst_any)
                });
                let status = InstrumentStatus::new(
                    instrument_id,
                    status_action,
                    ts_init,
                    ts_init,
                    None,
                    None,
                    Some(is_live),
                    None,
                    None,
                );
                call_python_with_data(call_soon, callback, |py| status.into_py_any(py));
            }
            Ok(None) => {
                let instrument_id = instruments_by_symbol
                    .get(&inst_key)
                    .map_or_else(|| parse_instrument_id(inst_key), |i| i.id());
                let status = InstrumentStatus::new(
                    instrument_id,
                    status_action,
                    ts_init,
                    ts_init,
                    None,
                    None,
                    Some(is_live),
                    None,
                    None,
                );
                call_python_with_data(call_soon, callback, |py| status.into_py_any(py));
            }
            Err(e) => {
                log::warn!("Failed to parse instrument {}: {e}", okx_inst.inst_id);
                let instrument_id = instruments_by_symbol
                    .get(&inst_key)
                    .map_or_else(|| parse_instrument_id(inst_key), |i| i.id());
                let status = InstrumentStatus::new(
                    instrument_id,
                    status_action,
                    ts_init,
                    ts_init,
                    None,
                    None,
                    Some(is_live),
                    None,
                    None,
                );
                call_python_with_data(call_soon, callback, |py| status.into_py_any(py));
            }
        }
    }
}

fn handle_orders(
    order_msgs: &[OKXOrderMsg],
    account_id: AccountId,
    instruments_by_symbol: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &mut BoundedExecutionReplayCache,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) -> Result<(), ExecutionDispatchError> {
    replay_cache.preflight_orders(order_msgs)?;
    let ts_init = clock.get_time_ns();
    let mut parsed = Vec::with_capacity(order_msgs.len());
    let mut batch_fees: AHashMap<Ustr, Money> = AHashMap::new();
    let mut batch_fills: AHashMap<Ustr, Quantity> = AHashMap::new();

    for msg in order_msgs {
        let previous_fee = batch_fees
            .get(&msg.ord_id)
            .copied()
            .or_else(|| replay_cache.previous_fee(&msg.ord_id));
        let previous_filled_qty = batch_fills
            .get(&msg.ord_id)
            .copied()
            .or_else(|| replay_cache.previous_filled_qty(&msg.ord_id));
        let report = parse_order_msg_with_previous(
            msg,
            account_id,
            instruments_by_symbol,
            previous_fee,
            previous_filled_qty,
            ts_init,
        )
        .map_err(|error| {
            ExecutionDispatchError::Conversion(format!(
                "Python orders report inst_id={} ord_id={}: {error}",
                msg.inst_id, msg.ord_id
            ))
        })?;
        let instrument = instruments_by_symbol.get(&msg.inst_id).ok_or_else(|| {
            ExecutionDispatchError::Conversion(format!(
                "Python orders instrument missing: inst_id={} ord_id={}",
                msg.inst_id, msg.ord_id
            ))
        })?;
        let (fee, filled_qty) = parse_fee_fill_cache_values(msg, instrument).map_err(|error| {
            ExecutionDispatchError::Conversion(format!(
                "Python orders replay values ord_id={}: {error}",
                msg.ord_id
            ))
        })?;
        if let Some(fee) = fee {
            batch_fees.insert(msg.ord_id, fee);
        }
        if let Some(filled_qty) = filled_qty {
            batch_fills.insert(msg.ord_id, filled_qty);
        }
        parsed.push((report, msg.ord_id, fee, filled_qty));
    }

    let mut reports = Vec::with_capacity(parsed.len());
    for (report, venue_order_id, fee, filled_qty) in parsed {
        replay_cache.update(venue_order_id, fee, filled_qty);
        reports.push(report);
    }
    dispatch_execution_reports_to_python(reports, call_soon, callback);
    Ok(())
}

fn handle_spread_orders(
    order_msgs: &[OKXSpreadOrder],
    account_id: AccountId,
    instruments_by_symbol: &AHashMap<Ustr, InstrumentAny>,
    replay_cache: &mut BoundedExecutionReplayCache,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) -> Result<(), ExecutionDispatchError> {
    replay_cache.preflight_spread_orders(order_msgs)?;
    let ts_init = clock.get_time_ns();
    let mut parsed = Vec::with_capacity(order_msgs.len());
    let mut batch_fills: AHashMap<Ustr, Quantity> = AHashMap::new();

    for msg in order_msgs {
        let previous_filled_qty = batch_fills
            .get(&msg.ord_id)
            .copied()
            .or_else(|| replay_cache.previous_filled_qty(&msg.ord_id));
        let report = parse_spread_order_msg_with_previous(
            msg,
            account_id,
            instruments_by_symbol,
            previous_filled_qty,
            ts_init,
        )
        .map_err(|error| {
            ExecutionDispatchError::Conversion(format!(
                "Python spread report sprd_id={} ord_id={}: {error}",
                msg.sprd_id, msg.ord_id
            ))
        })?;
        let instrument = instruments_by_symbol.get(&msg.sprd_id).ok_or_else(|| {
            ExecutionDispatchError::Conversion(format!(
                "Python spread instrument missing: sprd_id={} ord_id={}",
                msg.sprd_id, msg.ord_id
            ))
        })?;
        let filled_qty = if msg.acc_fill_sz.is_empty() || msg.acc_fill_sz == "0" {
            None
        } else {
            Some(
                parse_quantity(&msg.acc_fill_sz, instrument.size_precision()).map_err(|error| {
                    ExecutionDispatchError::Conversion(format!(
                        "Python spread cumulative fill ord_id={} value={:?}: {error}",
                        msg.ord_id, msg.acc_fill_sz
                    ))
                })?,
            )
        };
        if let Some(filled_qty) = filled_qty {
            batch_fills.insert(msg.ord_id, filled_qty);
        }
        parsed.push((report, msg.ord_id, filled_qty));
    }

    let mut reports = Vec::with_capacity(parsed.len());
    for (report, venue_order_id, filled_qty) in parsed {
        replay_cache.update(venue_order_id, None, filled_qty);
        reports.push(report);
    }
    dispatch_execution_reports_to_python(reports, call_soon, callback);
    Ok(())
}

fn handle_algo_orders(
    algo_msgs: Vec<OKXAlgoOrderMsg>,
    account_id: AccountId,
    instruments_by_symbol: &AHashMap<Ustr, InstrumentAny>,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) -> Result<(), ExecutionDispatchError> {
    let ts_init = clock.get_time_ns();
    let mut reports = Vec::new();
    for algo_msg in algo_msgs {
        match parse_algo_order_msg(&algo_msg, account_id, instruments_by_symbol, ts_init) {
            Ok(Some(report)) => reports.push(report),
            Ok(None) => {}
            Err(error) => {
                return Err(ExecutionDispatchError::Conversion(format!(
                    "Python algo order id={} instrument={}: {error}",
                    algo_msg.algo_id, algo_msg.inst_id
                )));
            }
        }
    }
    dispatch_execution_reports_to_python(reports, call_soon, callback);
    Ok(())
}

fn handle_account(
    data: serde_json::Value,
    account_id: AccountId,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) -> Result<(), ExecutionDispatchError> {
    let accounts = serde_json::from_value::<Vec<OKXAccount>>(data).map_err(|error| {
        ExecutionDispatchError::Conversion(format!(
            "Python account payload deserialization: {error}"
        ))
    })?;
    let ts_init = clock.get_time_ns();
    let mut states = Vec::with_capacity(accounts.len());
    for account in &accounts {
        states.push(
            parse_account_state(account, account_id, ts_init).map_err(|error| {
                ExecutionDispatchError::Conversion(format!(
                    "Python account state account_id={account_id}: {error}"
                ))
            })?,
        );
    }
    for account_state in states {
        call_python_with_data(call_soon, callback, |py| account_state.into_py_any(py));
    }
    Ok(())
}

fn handle_positions(
    data: serde_json::Value,
    account_id: AccountId,
    instruments_by_symbol: &AHashMap<Ustr, InstrumentAny>,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) -> Result<(), ExecutionDispatchError> {
    let positions = serde_json::from_value::<Vec<OKXPosition>>(data).map_err(|error| {
        ExecutionDispatchError::Conversion(format!(
            "Python positions payload deserialization: {error}"
        ))
    })?;
    let ts_init = clock.get_time_ns();
    let mut reports = Vec::with_capacity(positions.len());
    for position in positions {
        let inst_key = Ustr::from(&position.inst_id);
        let instrument = instruments_by_symbol.get(&inst_key).ok_or_else(|| {
            ExecutionDispatchError::Conversion(format!(
                "Python position instrument missing: inst_id={}",
                position.inst_id
            ))
        })?;
        reports.push(
            parse_position_status_report(
                &position,
                account_id,
                instrument.id(),
                instrument.size_precision(),
                ts_init,
            )
            .map_err(|error| {
                ExecutionDispatchError::Conversion(format!(
                    "Python position inst_id={}: {error}",
                    position.inst_id
                ))
            })?,
        );
    }
    for report in reports {
        call_python_with_data(call_soon, callback, |py| report.into_py_any(py));
    }
    Ok(())
}

#[expect(clippy::too_many_arguments)]
fn handle_order_response(
    id: Option<&str>,
    op: &OKXWsOperation,
    code: &str,
    msg: &str,
    data: &[serde_json::Value],
    runtime_handle: &OKXWebSocketRuntimeHandle,
    account_id: AccountId,
    clock: &AtomicTime,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) -> Result<(), ExecutionDispatchError> {
    let response_client_order_id = data
        .first()
        .and_then(|item| {
            item.get(OKX_FIELD_CLORDID)
                .or_else(|| item.get("algoClOrdId"))
        })
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    // Resolve one top-level venue request exactly once. Batch responses can contain up to 20
    // items, but they consume one shared pending-capacity permit and must never attempt a second
    // lookup after the aggregate entry has been removed.
    let pending_resolution = runtime_handle
        .pending_responses()
        .tracks_operation(op)
        .then(|| {
            runtime_handle
                .pending_responses()
                .resolve(op, id, response_client_order_id)
        });
    if matches!(pending_resolution, Some(None)) {
        log::error!(
            "Ignoring unmatched OKX Python order response: \
             id={id:?} op={op:?} cl_ord_id={response_client_order_id:?}"
        );
        return Err(ExecutionDispatchError::PendingAmbiguous(format!(
            "unmatched Python order response id={id:?} op={op:?} \
             cl_ord_id={response_client_order_id:?}"
        )));
    }
    let pending_info = pending_resolution.flatten();

    if pending_info
        .as_ref()
        .is_some_and(|info| info.routing.is_none())
        && data.is_empty()
    {
        return Err(ExecutionDispatchError::PendingAmbiguous(format!(
            "aggregate Python response has no item acknowledgements: id={id:?} op={op:?} \
             code={code} msg={msg:?}"
        )));
    }

    for (index, item) in data.iter().enumerate() {
        let parsed = parse_order_response_item(item, index)?;
        let s_code = parsed.s_code.as_str();
        let s_msg = parsed.s_msg.as_str();
        let cl_ord_id = parsed.client_order_id.as_str();

        let resolved_client_order_id = pending_info
            .as_ref()
            .and_then(|info| info.client_order_id)
            .or_else(|| (!cl_ord_id.is_empty()).then(|| ClientOrderId::from(cl_ord_id)));

        if s_code == OKX_SUCCESS_CODE {
            log::debug!("Order response ok: op={op:?} cl_ord_id={cl_ord_id}");
            match op {
                OKXWsOperation::Order | OKXWsOperation::BatchOrders => {
                    if let Some(routing) = pending_info.as_ref().and_then(|info| info.routing) {
                        let venue_order_id = parsed
                            .venue_order_id
                            .as_deref()
                            .filter(|value| !value.is_empty());

                        if let Some(ord_id) = venue_order_id {
                            let ts_init = clock.get_time_ns();
                            let Some(client_order_id) = resolved_client_order_id else {
                                log::error!(
                                    "No client_order_id for accepted order: request_id={id:?}"
                                );
                                continue;
                            };
                            let accepted = OrderAccepted::new(
                                routing.trader_id,
                                routing.strategy_id,
                                routing.instrument_id,
                                client_order_id,
                                VenueOrderId::new(ord_id),
                                account_id,
                                UUID4::new(),
                                ts_init,
                                ts_init,
                                false,
                            );
                            call_python_with_data(call_soon, callback, |py| {
                                accepted.into_py_any(py)
                            });
                        } else {
                            log::error!(
                                "No venue_order_id for accepted order: cl_ord_id={cl_ord_id}"
                            );
                        }
                    }
                }
                OKXWsOperation::OrderAlgo => {
                    log::debug!("Algo order placement confirmed: cl_ord_id={cl_ord_id}");
                }
                OKXWsOperation::CancelOrder
                | OKXWsOperation::BatchCancelOrders
                | OKXWsOperation::MassCancel
                | OKXWsOperation::CancelAlgos => {}
                OKXWsOperation::AmendOrder | OKXWsOperation::BatchAmendOrders => {}
                _ => {}
            }
        } else {
            log::warn!(
                "Order response rejected: op={op:?} cl_ord_id={cl_ord_id} \
                 s_code={s_code} s_msg={s_msg}"
            );
            let ts_init = clock.get_time_ns();
            let Some(client_order_id) = resolved_client_order_id else {
                log::error!("No client_order_id for rejected order response: request_id={id:?}");
                continue;
            };
            let venue_order_id = parsed
                .venue_order_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(VenueOrderId::new);
            // Python batch APIs do not carry one Nautilus trader/strategy route per item. Never
            // fabricate a route from the aggregate pending entry; the orders channel and venue
            // reconciliation remain authoritative for those item-level failures.
            let routing = pending_info.as_ref().and_then(|info| info.routing);
            if routing.is_none() {
                log::warn!(
                    "No singular Python routing context for rejected aggregate item; \
                     preserving item for order-channel reconciliation: \
                     id={id:?} op={op:?} cl_ord_id={cl_ord_id}"
                );
            }

            match op {
                OKXWsOperation::Order | OKXWsOperation::BatchOrders | OKXWsOperation::OrderAlgo => {
                    if let Some(routing) = routing {
                        let rejected = OrderRejected::new(
                            routing.trader_id,
                            routing.strategy_id,
                            routing.instrument_id,
                            client_order_id,
                            account_id,
                            Ustr::from(s_msg),
                            UUID4::new(),
                            ts_init,
                            ts_init,
                            false,
                            false,
                        );
                        call_python_with_data(call_soon, callback, |py| rejected.into_py_any(py));
                    }
                }
                OKXWsOperation::CancelOrder
                | OKXWsOperation::BatchCancelOrders
                | OKXWsOperation::MassCancel
                | OKXWsOperation::CancelAlgos => {
                    if let Some(routing) = routing {
                        let rejected = OrderCancelRejected::new(
                            routing.trader_id,
                            routing.strategy_id,
                            routing.instrument_id,
                            client_order_id,
                            Ustr::from(s_msg),
                            UUID4::new(),
                            ts_init,
                            ts_init,
                            false,
                            venue_order_id,
                            Some(account_id),
                        );
                        call_python_with_data(call_soon, callback, |py| rejected.into_py_any(py));
                    }
                }
                OKXWsOperation::AmendOrder | OKXWsOperation::BatchAmendOrders => {
                    if let Some(routing) = routing {
                        let rejected = OrderModifyRejected::new(
                            routing.trader_id,
                            routing.strategy_id,
                            routing.instrument_id,
                            client_order_id,
                            Ustr::from(s_msg),
                            UUID4::new(),
                            ts_init,
                            ts_init,
                            false,
                            venue_order_id,
                            Some(account_id),
                        );
                        call_python_with_data(call_soon, callback, |py| rejected.into_py_any(py));
                    }
                }
                _ => {}
            }
        }
    }

    if code != "0" && data.is_empty() {
        log::warn!("Order response error (no data): id={id:?} op={op:?} code={code} msg={msg}");
        if let Some(info) = pending_info.as_ref() {
            let Some(client_order_id) = info.client_order_id else {
                log::warn!(
                    "Resolved data-less OKX Python response has no client_order_id: \
                     id={id:?} op={op:?}"
                );
                return Ok(());
            };
            let Some(routing) = info.routing else {
                log::warn!(
                    "Resolved data-less OKX Python response has no singular routing: \
                     id={id:?} op={op:?}"
                );
                return Ok(());
            };
            let ts_init = clock.get_time_ns();
            let reason = Ustr::from(if msg.is_empty() { code } else { msg });
            match op {
                OKXWsOperation::Order => {
                    let rejected = OrderRejected::new(
                        routing.trader_id,
                        routing.strategy_id,
                        routing.instrument_id,
                        client_order_id,
                        account_id,
                        reason,
                        UUID4::new(),
                        ts_init,
                        ts_init,
                        false,
                        false,
                    );
                    call_python_with_data(call_soon, callback, |py| rejected.into_py_any(py));
                }
                OKXWsOperation::CancelOrder => {
                    let rejected = OrderCancelRejected::new(
                        routing.trader_id,
                        routing.strategy_id,
                        routing.instrument_id,
                        client_order_id,
                        reason,
                        UUID4::new(),
                        ts_init,
                        ts_init,
                        false,
                        None,
                        Some(account_id),
                    );
                    call_python_with_data(call_soon, callback, |py| rejected.into_py_any(py));
                }
                OKXWsOperation::AmendOrder => {
                    let rejected = OrderModifyRejected::new(
                        routing.trader_id,
                        routing.strategy_id,
                        routing.instrument_id,
                        client_order_id,
                        reason,
                        UUID4::new(),
                        ts_init,
                        ts_init,
                        false,
                        None,
                        Some(account_id),
                    );
                    call_python_with_data(call_soon, callback, |py| rejected.into_py_any(py));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn handle_send_failed(
    request_id: &str,
    client_order_id: Option<ClientOrderId>,
    op: Option<&OKXWsOperation>,
    error: &str,
    runtime_handle: &OKXWebSocketRuntimeHandle,
) {
    log::error!("WebSocket send failed: request_id={request_id} error={error}");
    runtime_handle
        .pending_responses()
        .mark_send_failed(op, request_id, client_order_id);
}

fn record_python_data_failure(call_soon: &Py<PyAny>, reason: String) {
    Python::attach(|py| fail_python_data_ingress(py, call_soon, reason));
}

fn call_python_with_data<F>(call_soon: &Py<PyAny>, callback: &Py<PyAny>, data_converter: F)
where
    F: FnOnce(Python) -> PyResult<Py<PyAny>>,
{
    Python::attach(|py| match data_converter(py) {
        Ok(py_obj) => call_python_threadsafe(py, call_soon, callback, py_obj),
        Err(e) => {
            fail_python_data_ingress(py, call_soon, format!("data_conversion_failed: {e}"));
            log::error!("Failed to convert data to Python object: {e}");
        }
    });
}

fn dispatch_json_value_to_python(
    data: &serde_json::Value,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) {
    call_python_with_data(call_soon, callback, |py| value_to_pyobject(py, data));
}

fn dispatch_nautilus_ws_msg_to_python(
    msg: NautilusWsMessage,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
    instruments_by_symbol: &mut AHashMap<Ustr, InstrumentAny>,
) {
    match msg {
        NautilusWsMessage::Data(payloads) => Python::attach(|py| {
            for data in payloads {
                let py_obj = data_to_pycapsule(py, data);
                call_python_threadsafe(py, call_soon, callback, py_obj);
            }
        }),
        NautilusWsMessage::Deltas(deltas) => Python::attach(|py| {
            let py_obj = data_to_pycapsule(py, Data::Deltas(OrderBookDeltas_API::new(deltas)));
            call_python_threadsafe(py, call_soon, callback, py_obj);
        }),
        NautilusWsMessage::FundingRates(updates) => {
            for data in updates {
                call_python_with_data(call_soon, callback, |py| data.into_py_any(py));
            }
        }
        NautilusWsMessage::Instrument(instrument, status) => {
            instruments_by_symbol.insert(instrument.symbol().inner(), (*instrument).clone());
            call_python_with_data(call_soon, callback, |py| {
                instrument_any_to_pyobject(py, *instrument)
            });

            if let Some(status) = status {
                call_python_with_data(call_soon, callback, |py| status.into_py_any(py));
            }
        }
        NautilusWsMessage::InstrumentStatus(status) => {
            call_python_with_data(call_soon, callback, |py| status.into_py_any(py));
        }
        NautilusWsMessage::Raw(data) => {
            dispatch_json_value_to_python(&data, call_soon, callback);
        }
        _ => {}
    }
}

fn dispatch_execution_reports_to_python(
    reports: Vec<ExecutionReport>,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) {
    for report in reports {
        match report {
            ExecutionReport::Order(report) => {
                call_python_with_data(call_soon, callback, |py| report.into_py_any(py));
            }
            ExecutionReport::Fill(report) => {
                call_python_with_data(call_soon, callback, |py| report.into_py_any(py));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::live::runner::{
        bounded_data_event_channel, bounded_exec_event_channel, replace_data_event_sender,
        replace_exec_event_sender,
    };
    use nautilus_core::time::get_atomic_clock_realtime;
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::websocket::{
        client::{OKXWebSocketQueue, OKXWebSocketQueueFailureKind},
        messages::OKXWsFrame,
    };

    #[rstest]
    fn python_websocket_constructor_omitted_url_defaults_to_demo_public_endpoint() {
        let client =
            OKXWebSocketClient::py_new(None, None, None, None, None, None, None, None).unwrap();

        assert_eq!(client.url(), "wss://wspap.okx.com:8443/ws/v5/public");
    }

    #[rstest]
    fn python_websocket_constructor_preserves_explicit_live_url() {
        let live_url = "wss://ws.okx.com:8443/ws/v5/public";
        let client = OKXWebSocketClient::py_new(
            Some(live_url.to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(client.url(), live_url);
    }

    #[rstest]
    #[case("0")]
    #[case("51000")]
    fn two_item_batch_response_resolves_python_aggregate_once(#[case] item_code: &str) {
        Python::initialize();
        let client = OKXWebSocketClient::default();
        let request_id = "python-batch-request";
        client
            .admit_aggregate_for_test(OKXWsOperation::BatchOrders, request_id)
            .unwrap();
        let runtime_handle = client.runtime_handle();
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
            .collect::<Vec<_>>();
        let (call_soon, callback) = Python::attach(|py| (py.None(), py.None()));

        handle_order_response(
            Some(request_id),
            &OKXWsOperation::BatchOrders,
            "0",
            "",
            &data,
            &runtime_handle,
            AccountId::from("OKX-001"),
            get_atomic_clock_realtime(),
            &call_soon,
            &callback,
        )
        .unwrap();

        assert_eq!(client.pending_operation_count_for_test(), 0);
        assert_eq!(client.queue_failure_guard().failure(), None);
    }

    #[rstest]
    #[case("0", "")]
    #[case("1", "batch rejected")]
    fn data_less_batch_response_fails_python_aggregate_ambiguous(
        #[case] code: &str,
        #[case] message: &str,
    ) {
        Python::initialize();
        let client = OKXWebSocketClient::default();
        let request_id = "python-data-less-request";
        client
            .admit_aggregate_for_test(OKXWsOperation::BatchAmendOrders, request_id)
            .unwrap();
        let runtime_handle = client.runtime_handle();
        let (call_soon, callback) = Python::attach(|py| (py.None(), py.None()));

        let error = handle_order_response(
            Some(request_id),
            &OKXWsOperation::BatchAmendOrders,
            code,
            message,
            &[],
            &runtime_handle,
            AccountId::from("OKX-001"),
            get_atomic_clock_realtime(),
            &call_soon,
            &callback,
        )
        .unwrap_err();

        assert_eq!(client.pending_operation_count_for_test(), 0);
        assert!(matches!(error, ExecutionDispatchError::PendingAmbiguous(_)));
        halt_on_execution_dispatch_error(&client.queue_failure_guard(), &error, "Python test");
        assert_eq!(
            client.queue_failure_guard().failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[rstest]
    fn python_order_consumer_propagates_replay_cache_capacity_before_callbacks() {
        Python::initialize();
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("ws_orders_post_only_canceled_first.json");
        let content = std::fs::read_to_string(path).unwrap();
        let frame: OKXWsFrame = serde_json::from_str(&content).unwrap();
        let OKXWsFrame::Data { data, .. } = frame else {
            panic!("expected order data fixture");
        };
        let orders: Vec<OKXOrderMsg> = serde_json::from_value(data).unwrap();
        let (call_soon, callback) = Python::attach(|py| (py.None(), py.None()));
        let mut replay_cache = BoundedExecutionReplayCache::new(0);

        let error = handle_orders(
            &orders,
            AccountId::from("OKX-001"),
            &AHashMap::new(),
            &mut replay_cache,
            get_atomic_clock_realtime(),
            &call_soon,
            &callback,
        )
        .unwrap_err();

        let ExecutionDispatchError::ReplayCapacity(error) = error else {
            panic!("expected replay-capacity error");
        };
        assert_eq!(error.capacity(), 0);
        assert_eq!(error.len(), 0);
        assert_eq!(error.requested_new(), 1);
        assert_eq!(replay_cache.len(), 0);
    }

    #[rstest]
    fn python_execution_conversion_halts_shared_live_ingress_before_callbacks() {
        Python::initialize();
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("ws_orders.json");
        let frame: OKXWsFrame =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let OKXWsFrame::Data { data, .. } = frame else {
            panic!("expected orders fixture");
        };
        let orders: Vec<OKXOrderMsg> = serde_json::from_value(data).unwrap();
        let (call_soon, callback) = Python::attach(|py| (py.None(), py.None()));
        let mut replay_cache = BoundedExecutionReplayCache::default();
        let client = OKXWebSocketClient::default();
        let live = LiveIngressControl::detached();
        client.set_queue_failure_callback(live_ingress_failure_callback(
            live.clone(),
            LiveIngressLane::ExecEvents,
        ));

        let error = handle_orders(
            &orders,
            AccountId::from("OKX-001"),
            &AHashMap::new(),
            &mut replay_cache,
            get_atomic_clock_realtime(),
            &call_soon,
            &callback,
        )
        .unwrap_err();
        assert!(matches!(error, ExecutionDispatchError::Conversion(_)));

        halt_on_execution_dispatch_error(&client.queue_failure_guard(), &error, "Python test");
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
    #[case("data")]
    #[case("execution")]
    fn python_live_connect_fails_closed_without_current_tls_sender(#[case] lane: &str) {
        let lane = lane.to_string();
        let error = std::thread::spawn(move || resolve_python_live_ingress(&lane).unwrap_err())
            .join()
            .unwrap();
        assert!(error.contains("requires the current runner TLS"));
    }

    #[rstest]
    fn python_live_data_binding_uses_bounded_current_tls_control() {
        std::thread::spawn(|| {
            let (sender, _receiver) = bounded_data_event_channel(17);
            replace_data_event_sender(sender);

            let (control, lane) = resolve_python_live_ingress("data").unwrap();

            assert_eq!(lane, LiveIngressLane::DataEvents);
            assert_eq!(control.lane_metrics(lane).capacity, 17);
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn python_live_execution_binding_uses_bounded_current_tls_control() {
        std::thread::spawn(|| {
            let (sender, _receiver) = bounded_exec_event_channel(19);
            replace_exec_event_sender(sender);

            let (control, lane) = resolve_python_live_ingress("execution").unwrap();

            assert_eq!(lane, LiveIngressLane::ExecEvents);
            assert_eq!(control.lane_metrics(lane).capacity, 19);
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn python_live_binding_rejects_already_halted_tls_control() {
        std::thread::spawn(|| {
            let (sender, _receiver) = bounded_data_event_channel(11);
            sender.control().fail_full(LiveIngressLane::DataEvents);
            replace_data_event_sender(sender);

            let error = resolve_python_live_ingress("data").unwrap_err();

            assert!(error.contains("already halted"));
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn python_live_binding_rejects_unknown_lane() {
        let error = resolve_python_live_ingress("orders").unwrap_err();
        assert!(error.contains("expected 'data' or 'execution'"));
    }

    #[rstest]
    #[cfg(debug_assertions)]
    fn python_live_binding_rejects_debug_unbounded_tls_sender() {
        std::thread::spawn(|| {
            let (sender, _receiver) =
                tokio::sync::mpsc::unbounded_channel::<nautilus_common::messages::DataEvent>();
            replace_data_event_sender(sender);

            let error = resolve_python_live_ingress("data").unwrap_err();

            assert!(error.contains("requires a bounded DataEvents TLS sender"));
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn python_book_sequence_state_is_isolated_per_channel() {
        let mut trackers = AHashMap::new();
        let instrument_id = InstrumentId::from("BTC-USDT.OKX");

        let books_snapshot = validate_python_book_sequence(
            &mut trackers,
            OKXWsChannel::Books,
            instrument_id,
            OKXBookAction::Snapshot,
            &[(Some(-1), 100)],
        );
        let rpi_snapshot = validate_python_book_sequence(
            &mut trackers,
            OKXWsChannel::BooksRpi,
            instrument_id,
            OKXBookAction::Snapshot,
            &[(Some(-1), 200)],
        );
        let books_update = validate_python_book_sequence(
            &mut trackers,
            OKXWsChannel::Books,
            instrument_id,
            OKXBookAction::Update,
            &[(Some(100), 101)],
        );
        let rpi_update = validate_python_book_sequence(
            &mut trackers,
            OKXWsChannel::BooksRpi,
            instrument_id,
            OKXBookAction::Update,
            &[(Some(200), 201)],
        );

        assert_eq!(books_snapshot, BookSequenceOutcome::Accept);
        assert_eq!(rpi_snapshot, BookSequenceOutcome::Accept);
        assert_eq!(books_update, BookSequenceOutcome::Accept);
        assert_eq!(rpi_update, BookSequenceOutcome::Accept);
    }
}
