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

//! Live execution client implementation for the OKX adapter.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::AHashMap;
use anyhow::Context;
use async_trait::async_trait;
use futures_util::{Stream, StreamExt, pin_mut};
use nautilus_common::{
    clients::ExecutionClient,
    live::{
        LiveIngressLane, TaskExit, get_runtime, runner::get_exec_event_sender, spawn_supervised,
        task::TaskHandles,
    },
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    AtomicMap, UnixNanos,
    params::Params,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType, OrderSide, OrderType, TimeInForce, TrailingOffsetType},
    events::OrderDeniedReason,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, Venue, VenueOrderId,
    },
    instruments::InstrumentAny,
    orders::Order,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance},
};
use rust_decimal::Decimal;
use tokio::{sync::mpsc, task::JoinHandle};
use ustr::Ustr;

#[cfg(test)]
use nautilus_model::types::Quantity;

use crate::{
    common::{
        consts::{
            OKX_CONDITIONAL_ORDER_TYPES, OKX_SUCCESS_CODE, OKX_VENUE, OKX_WS_HEARTBEAT_SECS,
            resolve_instrument_families, should_retry_error_code, validate_okx_client_order_id,
        },
        enums::{
            OKXEnvironment, OKXInstrumentType, OKXMarginMode, OKXReconnectPolicy, OKXTradeMode,
            is_advance_algo_order,
        },
        parse::okx_instrument_type_from_symbol,
    },
    config::OKXExecClientConfig,
    http::{client::OKXHttpClient, error::OKXHttpError, models::OKXCancelAlgoOrderRequest},
    protection::{CancelAllAfterPolicy, RenewalDecision, RenewalHealth},
    reconciliation::{
        FillSnapshotQuery, OKXReconciliationSource, OrderSnapshotQuery, RECONNECT_NOTICE_CAPACITY,
        ReconnectNotice, ReconnectReconcilePolicy, effective_instrument_types,
        is_spread_instrument, require_complete_reconciliation_component, run_reconnect_reconciler,
        supports_algo_orders,
    },
    websocket::{
        client::{
            OKXWebSocketClient, OKXWebSocketQueue, OKXWebSocketQueueFailureGuard,
            OKXWebSocketQueueFailureKind,
        },
        dispatch::{
            AlgoCancelContext, DispatchOutcome, ExecutionDispatchError, OrderIdentity,
            WsDispatchState, dispatch_ws_message_bounded, emit_algo_cancel_rejections,
            emit_batch_cancel_failure, halt_on_execution_dispatch_error,
        },
        error::OKXWsError,
        live_ingress_failure_callback,
        messages::OKXWsMessage,
        parse::OrderStateSnapshot,
        replay_cache::BoundedExecutionReplayCache,
    },
};

#[derive(Debug)]
pub struct OKXExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: OKXExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: OKXHttpClient,
    ws_private: OKXWebSocketClient,
    ws_business: OKXWebSocketClient,
    trade_mode: OKXTradeMode,
    ws_stream_handle: Option<JoinHandle<()>>,
    ws_business_stream_handle: Option<JoinHandle<()>>,
    ws_dispatch_state: Arc<WsDispatchState>,
    pending_tasks: TaskHandles,
    /// Sender side of the reconnect-notice channel while a reconciler task is running.
    reconnect_tx: Option<mpsc::Sender<ReconnectNotice>>,
    /// Set for the duration of an intentional disconnect so supervised task exits are not
    /// mistaken for failures.
    shutting_down: Arc<AtomicBool>,
}

fn should_disable_cancel_all_after_on_disconnect(
    configured: bool,
    private_failed: bool,
    business_failed: bool,
) -> bool {
    configured && !private_failed && !business_failed
}

fn validate_venue_account_configuration(
    config: &OKXExecClientConfig,
    account: &crate::http::models::OKXAccountConfiguration,
) -> anyhow::Result<()> {
    if let Some(expected_uid) = config.expected_uid.as_deref() {
        anyhow::ensure!(
            account.uid == expected_uid,
            "OKX credential UID mismatch: expected {expected_uid:?}, received {:?}",
            account.uid
        );
    }
    if let Some(expected_main_uid) = config.expected_main_uid.as_deref() {
        anyhow::ensure!(
            account.main_uid == expected_main_uid,
            "OKX main UID mismatch: expected {expected_main_uid:?}, received {:?}",
            account.main_uid
        );
    }
    if let Some(expected_account_type) = config.expected_account_type {
        anyhow::ensure!(
            account.account_type == expected_account_type.to_string(),
            "OKX account type mismatch: expected {expected_account_type}, received {:?}",
            account.account_type
        );
    }
    if let Some(expected_level) = config.expected_account_level {
        anyhow::ensure!(
            account.acct_lv == expected_level.to_string(),
            "OKX account level mismatch: expected {expected_level}, received {:?}",
            account.acct_lv
        );
    }
    if config.environment == OKXEnvironment::Live {
        anyhow::ensure!(
            account.pos_mode == config.expected_position_mode,
            "OKX position mode mismatch: expected {:?}, received {:?}",
            config.expected_position_mode,
            account.pos_mode
        );
    }

    let permissions = account
        .perm
        .split(',')
        .map(str::trim)
        .filter(|permission| !permission.is_empty())
        .collect::<std::collections::HashSet<_>>();
    anyhow::ensure!(
        permissions.contains("read_only") && permissions.contains("trade"),
        "OKX execution API key must have both read_only and trade permissions"
    );
    anyhow::ensure!(
        !permissions.contains("withdraw"),
        "OKX execution API key must not have withdraw permission"
    );
    if config.environment == OKXEnvironment::Live && config.require_ip_binding {
        anyhow::ensure!(
            !account.ip.trim().is_empty(),
            "OKX live API key must be bound to at least one IP address"
        );
    }
    Ok(())
}

impl OKXExecutionClient {
    /// Creates a new [`OKXExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialize.
    pub fn new(core: ExecutionClientCore, config: OKXExecClientConfig) -> anyhow::Result<Self> {
        config.validate_execution_safety()?;
        if config.use_fills_channel {
            anyhow::bail!(
                "OKX fills WebSocket channel is unsupported; keep `use_fills_channel=false`. \
                 Fill lifecycle is sourced from the orders channel"
            );
        }
        let http_client = OKXHttpClient::with_credentials(
            config.api_key.clone(),
            config.api_secret.clone(),
            config.api_passphrase.clone(),
            Some(config.http_base_url()),
            config.http_timeout_secs,
            config.max_retries,
            config.retry_delay_initial_ms,
            config.retry_delay_max_ms,
            config.environment,
            config.proxy_url.clone(),
        )?;

        let account_id = core.account_id;

        let ws_private = OKXWebSocketClient::with_credentials(
            Some(config.ws_private_url()),
            config.api_key.clone(),
            config.api_secret.clone(),
            config.api_passphrase.clone(),
            Some(account_id),
            Some(OKX_WS_HEARTBEAT_SECS),
            config.auth_timeout_secs,
            config.transport_backend,
            config.proxy_url.clone(),
        )
        .context("failed to construct OKX private websocket client")?;

        let ws_business = OKXWebSocketClient::with_credentials(
            Some(config.ws_business_url()),
            config.api_key.clone(),
            config.api_secret.clone(),
            config.api_passphrase.clone(),
            Some(account_id),
            Some(OKX_WS_HEARTBEAT_SECS),
            config.auth_timeout_secs,
            config.transport_backend,
            config.proxy_url.clone(),
        )
        .context("failed to construct OKX business websocket client")?;

        let trade_mode = Self::derive_default_trade_mode(core.account_type, &config);
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            None,
        );

        let ws_dispatch_state = Arc::new(WsDispatchState::with_pending_responses(
            ws_private.pending_response_handle(),
        ));

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            ws_private,
            ws_business,
            trade_mode,
            ws_stream_handle: None,
            ws_business_stream_handle: None,
            ws_dispatch_state,
            pending_tasks: TaskHandles::default(),
            reconnect_tx: None,
            shutting_down: Arc::new(AtomicBool::new(false)),
        })
    }

    fn validate_account_configuration(
        &self,
        account: &crate::http::models::OKXAccountConfiguration,
    ) -> anyhow::Result<()> {
        validate_venue_account_configuration(&self.config, account)
    }

    async fn preflight_live_execution(&self) -> anyhow::Result<()> {
        let account = self
            .http_client
            .request_account_configuration()
            .await
            .context("failed to retrieve OKX venue account identity")?;
        self.validate_account_configuration(&account)?;
        log::info!(
            "Validated {:?} OKX credentials: uid={}, main_uid={}, account_type={}, account_level={}, position_mode={:?}, key_label={:?}, ip_bound={}",
            self.config.environment,
            account.uid,
            account.main_uid,
            account.account_type,
            account.acct_lv,
            account.pos_mode,
            account.label,
            !account.ip.trim().is_empty(),
        );
        Ok(())
    }

    /// Returns the Cancel All After timing policy, or `None` when protection is not configured.
    fn cancel_all_after_policy(&self) -> Option<CancelAllAfterPolicy> {
        self.config.cancel_all_after_secs.map(|countdown| {
            CancelAllAfterPolicy::from_secs(countdown, self.config.cancel_all_after_heartbeat_secs)
        })
    }

    async fn renew_cancel_all_after(&self) -> anyhow::Result<()> {
        let Some(policy) = self.cancel_all_after_policy() else {
            return Ok(());
        };
        arm_cancel_all_after(&self.http_client, &policy)
            .await
            .context("failed to arm OKX Cancel All After before execution startup")?;
        Ok(())
    }

    /// Arms Cancel All After and starts the supervised renewal heartbeat.
    ///
    /// The heartbeat tolerates transient renewal failures for as long as the venue-side
    /// countdown still leaves room for another attempt (see [`CancelAllAfterPolicy`]); only when
    /// protection can no longer be proven armed is the shared failure domain latched. A panic or
    /// unexpected exit of the heartbeat task is treated the same way.
    async fn start_cancel_all_after_heartbeat(&self) -> anyhow::Result<()> {
        let Some(policy) = self.cancel_all_after_policy() else {
            return Ok(());
        };
        let armed_at = Instant::now();
        self.renew_cancel_all_after().await?;

        let http_client = self.http_client.clone();
        let queue_failure = self.ws_private.queue_failure_guard();
        let exit_guard = queue_failure.clone();
        let shutting_down = Arc::clone(&self.shutting_down);
        let handle = spawn_supervised(
            "okx-cancel-all-after-heartbeat",
            run_cancel_all_after_heartbeat(policy, armed_at, http_client, queue_failure),
            move |exit| {
                if shutting_down.load(Ordering::Acquire) {
                    return;
                }
                if exit.is_panic() {
                    log::error!("OKX Cancel All After heartbeat task {exit}; protection lost");
                }
                exit_guard.fail(
                    OKXWebSocketQueue::VenueProtection,
                    OKXWebSocketQueueFailureKind::ProtectionLost,
                );
            },
        );
        self.pending_tasks.push(handle);
        Ok(())
    }

    /// Spawns a supervised consumer for one execution WebSocket stream.
    ///
    /// Any exit of the consumer while the client is not shutting down (a panic, or the stream
    /// ending because the transport gave up) means venue events are no longer observed, so it
    /// latches the shared failure domain instead of leaving a silently dead client.
    fn spawn_execution_consumer(
        &self,
        label: &'static str,
        ws: &OKXWebSocketClient,
        stream: impl Stream<Item = OKXWsMessage> + Send + 'static,
    ) -> JoinHandle<()> {
        let context = ExecutionConsumerContext {
            label,
            emitter: self.emitter.clone(),
            state: Arc::clone(&self.ws_dispatch_state),
            account_id: self.core.account_id,
            instruments: ws.instruments_cache_arc(),
            queue_failure: ws.queue_failure_guard(),
            clock: self.clock,
            reconnect: match self.config.reconnect_policy {
                OKXReconnectPolicy::Halt => ReconnectHandling::Halt,
                OKXReconnectPolicy::Reconcile => self
                    .reconnect_tx
                    .clone()
                    .map_or(ReconnectHandling::Halt, ReconnectHandling::Reconcile),
            },
        };
        let exit_guard = ws.queue_failure_guard();
        let shutting_down = Arc::clone(&self.shutting_down);

        spawn_supervised(
            label,
            run_execution_consumer(context, stream),
            move |exit| {
                if shutting_down.load(Ordering::Acquire) {
                    return;
                }
                match exit {
                    TaskExit::Panicked(_) => {
                        log::error!("OKX {label} execution consumer {exit}; halting");
                    }
                    TaskExit::Completed => log::error!(
                        "OKX {label} execution consumer stopped while the client is active; halting"
                    ),
                }
                exit_guard.fail(
                    OKXWebSocketQueue::Output,
                    OKXWebSocketQueueFailureKind::ConsumerStopped,
                );
            },
        )
    }

    /// Starts the reconnect reconciler when the policy is `Reconcile`, returning its notice sender.
    fn start_reconnect_reconciler(&mut self) {
        if self.config.reconnect_policy != OKXReconnectPolicy::Reconcile
            || self.reconnect_tx.is_some()
        {
            return;
        }
        let (tx, rx) = mpsc::channel(RECONNECT_NOTICE_CAPACITY);
        let policy = ReconnectReconcilePolicy {
            lookback_margin: Duration::from_secs(self.config.reconnect_reconcile_lookback_secs),
            ..ReconnectReconcilePolicy::default()
        };
        let queue_failure = self.ws_private.queue_failure_guard();
        let exit_guard = queue_failure.clone();
        let shutting_down = Arc::clone(&self.shutting_down);
        let handle = spawn_supervised(
            "okx-reconnect-reconciler",
            run_reconnect_reconciler(
                rx,
                self.reconciliation_source(),
                self.emitter.clone(),
                queue_failure,
                policy,
                self.clock,
            ),
            move |exit| {
                if shutting_down.load(Ordering::Acquire) {
                    return;
                }
                if exit.is_panic() {
                    log::error!("OKX reconnect reconciler {exit}; halting");
                    exit_guard.fail(
                        OKXWebSocketQueue::Output,
                        OKXWebSocketQueueFailureKind::ReconnectGap,
                    );
                }
            },
        );
        self.pending_tasks.push(handle);
        self.reconnect_tx = Some(tx);
    }

    fn reconciliation_source(&self) -> OKXReconciliationSource {
        OKXReconciliationSource::new(
            self.http_client.clone(),
            self.core.client_id,
            self.core.account_id,
            &self.config.instrument_types,
            self.config.load_spreads,
            self.clock,
        )
    }

    fn derive_default_trade_mode(
        account_type: AccountType,
        config: &OKXExecClientConfig,
    ) -> OKXTradeMode {
        let is_cross_margin = config.margin_mode == Some(OKXMarginMode::Cross);

        if account_type == AccountType::Cash {
            if !config.use_spot_margin {
                return OKXTradeMode::Cash;
            }
            return if is_cross_margin {
                OKXTradeMode::Cross
            } else {
                OKXTradeMode::Isolated
            };
        }

        if is_cross_margin {
            OKXTradeMode::Cross
        } else {
            OKXTradeMode::Isolated
        }
    }

    fn trade_mode_for_order(
        &self,
        instrument_id: InstrumentId,
        params: &Option<Params>,
    ) -> OKXTradeMode {
        if let Some(td_mode_str) = get_param_as_string(params, "td_mode") {
            match td_mode_str.parse::<OKXTradeMode>() {
                Ok(mode) => return mode,
                Err(_) => {
                    log::warn!("Invalid td_mode '{td_mode_str}', using derived trade mode");
                }
            }
        }

        derive_trade_mode_for_instrument(
            instrument_id,
            self.config.margin_mode,
            self.config.use_spot_margin,
        )
    }

    fn instrument_types(&self) -> Vec<OKXInstrumentType> {
        effective_instrument_types(&self.config.instrument_types)
    }

    fn update_account_state(&self) {
        let http_client = self.http_client.clone();
        let account_id = self.core.account_id;
        let emitter = self.emitter.clone();

        self.spawn_task("query_account", async move {
            let account_state = http_client
                .request_account_state(account_id)
                .await
                .context("failed to request OKX account state")?;
            emitter.send_account_state(account_state);
            Ok(())
        });
    }

    fn is_conditional_order(&self, order_type: OrderType) -> bool {
        OKX_CONDITIONAL_ORDER_TYPES.contains(&order_type)
    }

    fn submit_order_route(
        &self,
        instrument_id: InstrumentId,
        order_type: OrderType,
    ) -> anyhow::Result<OrderCommandRoute> {
        if self.is_conditional_order(order_type) {
            if is_spread_instrument(instrument_id) {
                anyhow::bail!(
                    "Trigger/conditional orders ({order_type:?}) are not supported for OKX spreads"
                );
            }

            let inst_type = okx_instrument_type_from_symbol(instrument_id.symbol.as_str());
            if inst_type == OKXInstrumentType::Option {
                anyhow::bail!(
                    "Trigger/conditional orders ({order_type:?}) are not supported for OKX options"
                );
            }

            return Ok(OrderCommandRoute::AlgoHttp);
        }

        if is_spread_instrument(instrument_id) {
            Ok(OrderCommandRoute::SpreadHttp)
        } else {
            Ok(OrderCommandRoute::RegularWs)
        }
    }

    fn cancel_order_route(
        &self,
        instrument_id: InstrumentId,
        order_state: Option<(OrderType, Option<bool>)>,
    ) -> OrderCommandRoute {
        if is_spread_instrument(instrument_id) {
            return OrderCommandRoute::SpreadHttp;
        }

        if order_state.is_some_and(|(order_type, is_triggered)| {
            self.is_conditional_order(order_type) && is_triggered != Some(true)
        }) {
            OrderCommandRoute::AlgoHttp
        } else {
            OrderCommandRoute::RegularWs
        }
    }

    fn cancel_all_orders_route(&self, instrument_id: InstrumentId) -> CancelAllOrdersRoute {
        if is_spread_instrument(instrument_id) {
            CancelAllOrdersRoute::SpreadHttp
        } else if self.config.use_mm_mass_cancel {
            CancelAllOrdersRoute::MassCancelHttp
        } else {
            CancelAllOrdersRoute::BatchWs
        }
    }

    fn submit_regular_order(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = {
            let cache = self.core.cache();
            cache.try_order_owned(&cmd.client_order_id)?
        };
        let ws_private = self.ws_private.clone();
        let trade_mode = self.trade_mode_for_order(cmd.instrument_id, &cmd.params);

        let emitter = self.emitter.clone();
        let clock = self.clock;
        let trader_id = self.core.trader_id;
        let client_order_id = order.client_order_id();
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();

        self.ws_dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id,
                order_side: order.order_side(),
                order_type: order.order_type(),
            },
        );
        let order_side = order.order_side();
        let order_type = order.order_type();
        let quantity = order.quantity();
        let time_in_force = order.time_in_force();
        let price = order.price();
        let trigger_price = order.trigger_price();
        let is_post_only = order.is_post_only();
        let is_reduce_only = order.is_reduce_only();
        let is_quote_quantity = order.is_quote_quantity();

        let px_usd = get_param_as_string(&cmd.params, "px_usd");
        let px_vol = get_param_as_string(&cmd.params, "px_vol");
        let speed_bump = get_param_as_string(&cmd.params, "speed_bump");
        let outcome = get_param_as_string(&cmd.params, "outcome");
        let slippage_pct = get_param_as_string(&cmd.params, "slippage_pct");
        let rpi = get_param_as_bool(&cmd.params, "rpi");
        let rpi_taker_access = get_param_as_bool(&cmd.params, "rpi_taker_access");
        let rpi_px_round = get_param_as_bool(&cmd.params, "rpi_px_round");

        self.spawn_task("submit_order", async move {
            let result = ws_private
                .submit_order(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    trade_mode,
                    client_order_id,
                    order_side,
                    order_type,
                    quantity,
                    Some(time_in_force),
                    price,
                    trigger_price,
                    Some(is_post_only),
                    Some(is_reduce_only),
                    Some(is_quote_quantity),
                    None,
                    None,
                    px_usd,
                    px_vol,
                    speed_bump,
                    outcome,
                    slippage_pct,
                    rpi,
                    rpi_taker_access,
                    rpi_px_round,
                )
                .await;

            if let Err(e) = result {
                if is_okx_ws_local_command_failure(&e) {
                    let ts_event = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("submit-order-error: {e}"),
                        ts_event,
                        false,
                    );
                } else {
                    log::warn!(
                        "Ambiguous submit failure for {client_order_id}, awaiting reconciliation: {e}"
                    );
                }
                return Err(anyhow::Error::new(e).context("submit order failed"));
            }

            Ok(())
        });

        Ok(())
    }

    fn submit_order_http(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = {
            let cache = self.core.cache();
            cache.try_order_owned(&cmd.client_order_id)?
        };
        let http_client = self.http_client.clone();
        let trade_mode = self.trade_mode_for_order(cmd.instrument_id, &cmd.params);

        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_id = order.client_order_id();
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();

        self.ws_dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id,
                order_side: order.order_side(),
                order_type: order.order_type(),
            },
        );
        let order_side = order.order_side();
        let order_type = order.order_type();
        let quantity = order.quantity();
        let time_in_force = order.time_in_force();
        let price = order.price();
        let is_post_only = order.is_post_only();
        let rpi = get_param_as_bool(&cmd.params, "rpi");
        let rpi_taker_access = get_param_as_bool(&cmd.params, "rpi_taker_access");
        let rpi_px_round = get_param_as_bool(&cmd.params, "rpi_px_round");

        self.spawn_task("submit_order_http", async move {
            let result = http_client
                .place_order_with_domain_types(
                    instrument_id,
                    trade_mode,
                    client_order_id,
                    order_side,
                    order_type,
                    quantity,
                    Some(time_in_force),
                    price,
                    Some(is_post_only),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    rpi,
                    rpi_taker_access,
                    rpi_px_round,
                )
                .await;

            if let Err(e) = result {
                if is_okx_http_submit_rejection(&e) || is_okx_http_local_command_failure(&e) {
                    let ts_event = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("submit-order-error: {e}"),
                        ts_event,
                        false,
                    );
                } else {
                    log::warn!(
                        "Ambiguous HTTP submit failure for {client_order_id}, awaiting reconciliation: {e}"
                    );
                }
                return Err(anyhow::Error::new(e).context("submit order failed"));
            }

            Ok(())
        });

        Ok(())
    }

    fn submit_conditional_order(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = {
            let cache = self.core.cache();
            cache.try_order_owned(&cmd.client_order_id)?
        };
        let http_client = self.http_client.clone();
        let trade_mode = self.trade_mode_for_order(cmd.instrument_id, &cmd.params);

        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_id = order.client_order_id();
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();
        let order_side = order.order_side();
        let order_type = order.order_type();

        self.ws_dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id,
                order_side,
                order_type,
            },
        );
        let quantity = order.quantity();
        let trigger_type = order.trigger_type();
        let trigger_price = order.trigger_price();
        let price = order.price();
        let is_reduce_only = order.is_reduce_only();

        let trailing_offset = order.trailing_offset();
        let trailing_offset_type = order.trailing_offset_type();
        let activation_price = order.activation_price();

        let close_fraction = get_param_as_string(&cmd.params, "close_fraction");
        let reduce_only = if close_fraction.is_some() {
            Some(true)
        } else {
            Some(is_reduce_only)
        };

        let (callback_ratio, callback_spread) = if order_type == OrderType::TrailingStopMarket {
            let offset = trailing_offset
                .ok_or_else(|| anyhow::anyhow!("TrailingStopMarket requires trailing_offset"))?;
            let offset_type = trailing_offset_type.ok_or_else(|| {
                anyhow::anyhow!("TrailingStopMarket requires trailing_offset_type")
            })?;

            match offset_type {
                TrailingOffsetType::BasisPoints => {
                    // Convert basis points to ratio (e.g., 100 bps = 0.01)
                    let ratio = offset / Decimal::from(10000);
                    (Some(ratio.to_string()), None)
                }
                TrailingOffsetType::Price => (None, Some(offset.to_string())),
                _ => {
                    anyhow::bail!("Unsupported trailing_offset_type for OKX: {offset_type:?}");
                }
            }
        } else {
            (None, None)
        };

        self.spawn_task("submit_algo_order", async move {
            let result = http_client
                .place_algo_order_with_domain_types(
                    instrument_id,
                    trade_mode,
                    client_order_id,
                    order_side,
                    order_type,
                    quantity,
                    trigger_price,
                    trigger_type,
                    price,
                    reduce_only,
                    close_fraction,
                    callback_ratio,
                    callback_spread,
                    activation_price,
                )
                .await;

            if let Err(e) = result {
                if is_okx_http_submit_rejection(&e) || is_okx_http_local_command_failure(&e) {
                    let ts_event = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("submit-order-error: {e}"),
                        ts_event,
                        false,
                    );
                } else {
                    log::warn!(
                        "Ambiguous algo submit failure for {client_order_id}, awaiting reconciliation: {e}"
                    );
                }
                return Err(anyhow::Error::new(e).context("submit algo order failed"));
            }

            Ok(())
        });

        Ok(())
    }

    fn cancel_ws_order(&self, cmd: &CancelOrder) {
        self.ensure_order_identity(cmd.client_order_id, cmd.strategy_id, cmd.instrument_id);

        let ws_private = self.ws_private.clone();
        let command = cmd.clone();

        self.spawn_task("cancel_order", async move {
            let result = ws_private
                .cancel_order(
                    command.trader_id,
                    command.strategy_id,
                    command.instrument_id,
                    Some(command.client_order_id),
                    command.venue_order_id,
                )
                .await;

            if let Err(e) = result {
                if is_okx_ws_local_command_failure(&e) {
                    log::warn!(
                        "Cancel command failed local validation for {}: {e}",
                        command.client_order_id
                    );
                } else {
                    log::warn!(
                        "Ambiguous cancel failure for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                }
                return Err(anyhow::Error::new(e).context("cancel order failed"));
            }

            Ok(())
        });
    }

    fn cancel_order_http(&self, cmd: &CancelOrder) {
        self.ensure_order_identity(cmd.client_order_id, cmd.strategy_id, cmd.instrument_id);

        let http_client = self.http_client.clone();
        let command = cmd.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("cancel_order_http", async move {
            let result = http_client
                .cancel_order(
                    command.instrument_id,
                    Some(command.client_order_id),
                    command.venue_order_id,
                )
                .await;

            if let Err(e) = result {
                if is_okx_http_structured_venue_rejection(&e) {
                    let ts_event = clock.get_time_ns();
                    emitter.emit_order_cancel_rejected_event(
                        command.strategy_id,
                        command.instrument_id,
                        command.client_order_id,
                        command.venue_order_id,
                        &format!("cancel-order-error: {e}"),
                        ts_event,
                    );
                } else if is_okx_http_local_command_failure(&e) {
                    log::warn!(
                        "HTTP cancel command failed local validation for {}: {e}",
                        command.client_order_id
                    );
                } else {
                    log::warn!(
                        "Ambiguous HTTP cancel failure for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                }
                return Err(anyhow::Error::new(e).context("cancel order failed"));
            }

            Ok(())
        });
    }

    fn cancel_algo_order(&self, cmd: &CancelOrder) {
        let http_client = self.http_client.clone();
        let command = cmd.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        let cache = self.core.cache();
        let is_advance = cache
            .order(&cmd.client_order_id)
            .is_some_and(|o| is_advance_algo_order(o.order_type()));
        drop(cache);

        let request = OKXCancelAlgoOrderRequest {
            inst_id: cmd.instrument_id.symbol.to_string(),
            inst_id_code: None,
            algo_id: cmd.venue_order_id.map(|id| id.to_string()),
            algo_cl_ord_id: if cmd.venue_order_id.is_none() {
                Some(cmd.client_order_id.to_string())
            } else {
                None
            },
        };

        self.spawn_task("cancel_algo_order", async move {
            let responses = if is_advance {
                http_client.cancel_advance_algo_orders(vec![request]).await
            } else {
                http_client.cancel_algo_orders(vec![request]).await
            };

            let reject_reason = match &responses {
                Err(e) if is_okx_http_structured_venue_rejection(e) => {
                    Some(format!("cancel-algo-order-error: {e}"))
                }
                Err(e) if is_okx_http_local_command_failure(e) => {
                    log::warn!(
                        "Algo cancel command failed local validation for {}: {e}",
                        command.client_order_id
                    );
                    None
                }
                Err(e) => {
                    log::warn!(
                        "Ambiguous algo cancel failure for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                    None
                }
                Ok(resps) => {
                    // Check per-order business status code
                    resps.first().and_then(|r| {
                        r.s_code.as_deref().and_then(|code| {
                            if code == OKX_SUCCESS_CODE {
                                None
                            } else {
                                let msg = r.s_msg.as_deref().unwrap_or("unknown");
                                Some(format!(
                                    "cancel-algo-order-rejected: s_code={code}, s_msg={msg}"
                                ))
                            }
                        })
                    })
                }
            };

            if let Some(reason) = reject_reason {
                let ts_event = clock.get_time_ns();
                emitter.emit_order_cancel_rejected_event(
                    command.strategy_id,
                    command.instrument_id,
                    command.client_order_id,
                    command.venue_order_id,
                    &reason,
                    ts_event,
                );
                anyhow::bail!("{reason}");
            }

            if let Err(e) = responses {
                return Err(anyhow::Error::new(e).context("cancel algo order failed"));
            }

            Ok(())
        });
    }

    fn mass_cancel_instrument(&self, instrument_id: InstrumentId) {
        if is_spread_instrument(instrument_id) {
            let http_client = self.http_client.clone();
            self.spawn_task("mass_cancel_orders_http", async move {
                http_client
                    .cancel_all_orders(instrument_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("Mass cancel orders failed: {e}"))?;
                Ok(())
            });
            return;
        }

        let ws_private = self.ws_private.clone();

        self.spawn_task("mass_cancel_orders", async move {
            ws_private.mass_cancel_orders(instrument_id).await?;
            Ok(())
        });
    }

    /// Populates `order_identities` for an order if not already present.
    ///
    /// Needed for cancel/modify commands on orders loaded via reconciliation
    /// (which bypass `submit_order` and therefore have no identity entry).
    /// Uses `DashMap::entry().or_insert_with` to keep the check-and-insert
    /// atomic; without it, two concurrent reconciliation tasks could race
    /// past a `contains_key` check and overwrite each other with stale
    /// cache state.
    fn ensure_order_identity(
        &self,
        client_order_id: ClientOrderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
    ) {
        self.ws_dispatch_state
            .order_identities
            .entry(client_order_id)
            .or_insert_with(|| {
                let cache = self.core.cache();
                let (order_side, order_type) = cache
                    .order(&client_order_id)
                    .map_or((OrderSide::NoOrderSide, OrderType::Market), |o| {
                        (o.order_side(), o.order_type())
                    });
                drop(cache);

                OrderIdentity {
                    instrument_id,
                    strategy_id,
                    order_side,
                    order_type,
                }
            });
    }

    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = get_runtime();
        let handle = runtime.spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("{description} failed: {e:?}");
            }
        });

        self.pending_tasks.push(handle);
    }

    // Partitions algo cancel orders into regular and advance, then spawns
    // HTTP tasks for each group with per-item and batch-level rejection handling.
    fn dispatch_algo_cancels(&self, items: Vec<(OKXCancelAlgoOrderRequest, AlgoCancelContext)>) {
        let mut regular_requests = Vec::new();
        let mut regular_contexts = Vec::new();
        let mut advance_requests = Vec::new();
        let mut advance_contexts = Vec::new();

        let cache = self.core.cache();

        for (request, ctx) in items {
            let is_advance = cache
                .order(&ctx.client_order_id)
                .is_some_and(|o| is_advance_algo_order(o.order_type()));

            if is_advance {
                advance_requests.push(request);
                advance_contexts.push(ctx);
            } else {
                regular_requests.push(request);
                regular_contexts.push(ctx);
            }
        }

        drop(cache);

        if !regular_requests.is_empty() {
            let client = self.http_client.clone();
            let emitter = self.emitter.clone();
            let clock = self.clock;

            self.spawn_task("cancel_algo_orders", async move {
                match client.cancel_algo_orders(regular_requests).await {
                    Ok(responses) => {
                        emit_algo_cancel_rejections(&responses, &regular_contexts, &emitter, clock);
                    }
                    Err(e) => {
                        if is_okx_http_local_command_failure(&e) {
                            for ctx in &regular_contexts {
                                log::warn!(
                                    "Algo batch cancel command failed local validation for {}: {e}",
                                    ctx.client_order_id
                                );
                            }
                        } else {
                            let msg = format!("{e}");
                            emit_batch_cancel_failure(&regular_contexts, &msg, &emitter, clock);
                        }
                        return Err(anyhow::Error::new(e).context("cancel algo orders failed"));
                    }
                }
                Ok(())
            });
        }

        if !advance_requests.is_empty() {
            let client = self.http_client.clone();
            let emitter = self.emitter.clone();
            let clock = self.clock;

            self.spawn_task("cancel_advance_algo_orders", async move {
                match client.cancel_advance_algo_orders(advance_requests).await {
                    Ok(responses) => {
                        emit_algo_cancel_rejections(&responses, &advance_contexts, &emitter, clock);
                    }
                    Err(e) => {
                        if is_okx_http_local_command_failure(&e) {
                            for ctx in &advance_contexts {
                                log::warn!(
                                    "Advance algo batch cancel command failed local validation for {}: {e}",
                                    ctx.client_order_id
                                );
                            }
                        } else {
                            let msg = format!("{e}");
                            emit_batch_cancel_failure(&advance_contexts, &msg, &emitter, clock);
                        }
                        return Err(
                            anyhow::Error::new(e).context("cancel advance algo orders failed")
                        );
                    }
                }
                Ok(())
            });
        }
    }

    fn abort_pending_tasks(&self) {
        self.pending_tasks.abort_all();
    }

    /// Polls the cache until the account is registered or timeout is reached.
    async fn await_account_registered(&self, timeout_secs: f64) -> anyhow::Result<()> {
        let account_id = self.core.account_id;

        if self.core.cache().account(&account_id).is_some() {
            log::info!("Account {account_id} registered");
            return Ok(());
        }

        let start = Instant::now();
        let timeout = Duration::from_secs_f64(timeout_secs);
        let interval = Duration::from_millis(10);

        loop {
            tokio::time::sleep(interval).await;

            if self.core.cache().account(&account_id).is_some() {
                log::info!("Account {account_id} registered");
                return Ok(());
            }

            if start.elapsed() >= timeout {
                anyhow::bail!(
                    "Timeout waiting for account {account_id} to be registered after {timeout_secs}s"
                );
            }
        }
    }
}

fn derive_trade_mode_for_instrument(
    instrument_id: InstrumentId,
    margin_mode: Option<OKXMarginMode>,
    use_spot_margin: bool,
) -> OKXTradeMode {
    let inst_type = okx_instrument_type_from_symbol(instrument_id.symbol.as_str());
    let is_cross_margin = margin_mode == Some(OKXMarginMode::Cross);

    match inst_type {
        OKXInstrumentType::Spot => {
            if use_spot_margin {
                if is_cross_margin {
                    OKXTradeMode::Cross
                } else {
                    OKXTradeMode::Isolated
                }
            } else {
                OKXTradeMode::Cash
            }
        }
        _ => {
            if is_cross_margin {
                OKXTradeMode::Cross
            } else {
                OKXTradeMode::Isolated
            }
        }
    }
}

/// Arms (or renews) Cancel All After within the policy's request timeout.
async fn arm_cancel_all_after(
    http_client: &OKXHttpClient,
    policy: &CancelAllAfterPolicy,
) -> anyhow::Result<()> {
    let tag = crate::common::consts::OKX_NAUTILUS_BROKER_ID;
    tokio::time::timeout(
        policy.request_timeout,
        http_client.arm_cancel_all_after(policy.countdown.as_secs(), Some(tag)),
    )
    .await
    .context("timed out arming OKX Cancel All After")??;
    Ok(())
}

/// Keeps Cancel All After armed until the shared failure domain latches or protection is lost.
///
/// Renewals are scheduled from the last successful arm time rather than a fixed interval so a
/// slow request cannot silently push the next renewal past the venue countdown. Failures are
/// retried while the countdown leaves enough headroom; once it does not, protection is declared
/// lost and the client halts. Stopping renewals on halt is intentional: the venue then cancels
/// every tag-scoped order when the countdown fires.
async fn run_cancel_all_after_heartbeat(
    policy: CancelAllAfterPolicy,
    mut armed_at: Instant,
    http_client: OKXHttpClient,
    queue_failure: OKXWebSocketQueueFailureGuard,
) {
    let mut health = RenewalHealth::default();
    let mut next_attempt = armed_at + policy.heartbeat;

    loop {
        tokio::time::sleep_until(next_attempt.into()).await;
        if queue_failure.is_failed() {
            log::debug!("OKX Cancel All After heartbeat stopping: failure domain latched");
            break;
        }

        let attempt_started = Instant::now();
        match arm_cancel_all_after(&http_client, &policy).await {
            Ok(()) => {
                // The venue starts its countdown no earlier than when the request left, so
                // measuring from the attempt start keeps the local deadline conservative.
                armed_at = attempt_started;
                let failures = health.record_success();
                if failures > 0 {
                    log::info!(
                        "OKX Cancel All After renewal recovered after {failures} consecutive failure(s)"
                    );
                }
                next_attempt = armed_at + policy.heartbeat;
            }
            Err(error) => {
                let failures = health.record_failure();
                match policy.after_failure(armed_at, Instant::now()) {
                    RenewalDecision::Retry { delay, remaining } => {
                        log::warn!(
                            "OKX Cancel All After renewal failed (consecutive={failures}, venue countdown remaining={remaining:?}); retrying in {delay:?}: {error:#}"
                        );
                        next_attempt = Instant::now() + delay;
                    }
                    RenewalDecision::Halt { remaining } => {
                        log::error!(
                            "OKX Cancel All After protection lost after {failures} consecutive failure(s) (venue countdown remaining={remaining:?}): {error:#}"
                        );
                        queue_failure.fail(
                            OKXWebSocketQueue::VenueProtection,
                            OKXWebSocketQueueFailureKind::ProtectionLost,
                        );
                        break;
                    }
                }
            }
        }
    }
}

/// How an execution consumer reacts to a transport reconnect.
#[derive(Debug, Clone)]
enum ReconnectHandling {
    /// Latch `ReconnectGap` and stop the consumer.
    Halt,
    /// Hand the gap to the reconnect reconciler and keep consuming.
    Reconcile(mpsc::Sender<ReconnectNotice>),
}

/// Everything an execution consumer task needs, captured once at spawn time.
#[derive(Debug, Clone)]
struct ExecutionConsumerContext {
    label: &'static str,
    emitter: ExecutionEventEmitter,
    state: Arc<WsDispatchState>,
    account_id: AccountId,
    instruments: Arc<AtomicMap<Ustr, InstrumentAny>>,
    queue_failure: OKXWebSocketQueueFailureGuard,
    clock: &'static AtomicTime,
    reconnect: ReconnectHandling,
}

/// Consumes one execution WebSocket stream until it ends or a terminal dispatch error occurs.
///
/// `last_message_at` tracks the newest point at which the connection was known to be alive; it
/// bounds the venue snapshot requested after a reconnect. Every message (including pings and
/// errors) is evidence of liveness, so all processed messages advance it.
async fn run_execution_consumer(
    ctx: ExecutionConsumerContext,
    stream: impl Stream<Item = OKXWsMessage> + Send + 'static,
) {
    let mut replay_cache = BoundedExecutionReplayCache::default();
    let mut order_state_cache: AHashMap<ClientOrderId, OrderStateSnapshot> = AHashMap::new();
    let mut last_message_at = ctx.clock.get_time_ns();

    pin_mut!(stream);

    while let Some(message) = stream.next().await {
        match dispatch_ws_message_bounded(
            message,
            &ctx.emitter,
            &ctx.state,
            ctx.account_id,
            &ctx.instruments,
            &mut replay_cache,
            &mut order_state_cache,
            ctx.clock,
        ) {
            Ok(DispatchOutcome::Processed) => {
                last_message_at = ctx.clock.get_time_ns();
            }
            Ok(DispatchOutcome::Reconnected) => {
                let now = ctx.clock.get_time_ns();
                match &ctx.reconnect {
                    ReconnectHandling::Halt => {
                        halt_on_execution_dispatch_error(
                            &ctx.queue_failure,
                            &ExecutionDispatchError::ReconnectGap,
                            ctx.label,
                        );
                        break;
                    }
                    ReconnectHandling::Reconcile(notices) => {
                        let notice = ReconnectNotice {
                            consumer: ctx.label,
                            last_message_at,
                            detected_at: now,
                        };
                        if let Err(error) = notices.try_send(notice) {
                            // A saturated or closed notice queue means the reconciler cannot
                            // keep up with a flapping transport; the gap is no longer bounded.
                            log::error!(
                                "OKX {} execution consumer could not queue reconnect reconciliation: {error}",
                                ctx.label
                            );
                            halt_on_execution_dispatch_error(
                                &ctx.queue_failure,
                                &ExecutionDispatchError::ReconnectGap,
                                ctx.label,
                            );
                            break;
                        }
                        last_message_at = now;
                    }
                }
            }
            Err(error) => {
                halt_on_execution_dispatch_error(&ctx.queue_failure, &error, ctx.label);
                break;
            }
        }
    }

    log::debug!("OKX {} execution consumer finished", ctx.label);
}

#[async_trait(?Send)]
impl ExecutionClient for OKXExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *OKX_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        self.preflight_live_execution().await?;
        // Protect any tag-scoped orders inherited from a prior process before performing the
        // longer startup sequence. If startup fails, this one-shot timer remains armed and expires.
        self.renew_cancel_all_after().await?;

        let instrument_types = self.instrument_types();

        if !self.core.instruments_initialized() {
            let mut all_instruments = Vec::new();
            let mut all_inst_id_codes = Vec::new();

            for instrument_type in &instrument_types {
                let Some(families) =
                    resolve_instrument_families(&self.config.instrument_families, *instrument_type)
                else {
                    continue;
                };

                if families.is_empty() {
                    let (instruments, inst_id_codes) = self
                        .http_client
                        .request_instruments(*instrument_type, None)
                        .await
                        .with_context(|| {
                            format!("failed to request OKX instruments for {instrument_type:?}")
                        })?;

                    if instruments.is_empty() {
                        log::warn!("No instruments returned for {instrument_type:?}");
                        continue;
                    }

                    log::debug!(
                        "Loaded {} {instrument_type:?} instruments",
                        instruments.len()
                    );

                    self.http_client.cache_instruments(&instruments);
                    all_instruments.extend(instruments);
                    all_inst_id_codes.extend(inst_id_codes);
                } else {
                    for family in &families {
                        let (instruments, inst_id_codes) = self
                            .http_client
                            .request_instruments(*instrument_type, Some(family.clone()))
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to request OKX instruments for {instrument_type:?} family {family}"
                                )
                            })?;

                        if instruments.is_empty() {
                            log::warn!(
                                "No instruments returned for {instrument_type:?} family {family}"
                            );
                            continue;
                        }

                        log::debug!(
                            "Loaded {} {instrument_type:?} instruments for family {family}",
                            instruments.len()
                        );

                        self.http_client.cache_instruments(&instruments);
                        all_instruments.extend(instruments);
                        all_inst_id_codes.extend(inst_id_codes);
                    }
                }
            }

            if all_instruments.is_empty() {
                anyhow::bail!(
                    "No instruments loaded for configured types {instrument_types:?}, \
                     cannot initialize execution client"
                );
            }

            self.ws_private.cache_instruments(&all_instruments);
            self.ws_private
                .cache_inst_id_codes(all_inst_id_codes.clone());
            self.ws_business.cache_instruments(&all_instruments);
            self.ws_business.cache_inst_id_codes(all_inst_id_codes);
            self.core.set_instruments_initialized();
        }

        self.shutting_down.store(false, Ordering::Release);
        self.start_reconnect_reconciler();

        self.ws_private.connect().await?;
        self.ws_private.wait_until_active(10.0).await?;
        log::info!("Connected to private WebSocket");

        if self.ws_stream_handle.is_none() {
            let stream = self.ws_private.stream();
            self.ws_stream_handle =
                Some(self.spawn_execution_consumer("private", &self.ws_private, stream));
        }

        self.ws_business.connect().await?;
        self.ws_business.wait_until_active(10.0).await?;
        log::info!("Connected to business WebSocket");

        if self.ws_business_stream_handle.is_none() {
            let stream = self.ws_business.stream();
            self.ws_business_stream_handle =
                Some(self.spawn_execution_consumer("business", &self.ws_business, stream));
        }

        for inst_type in &instrument_types {
            log::debug!("Subscribing to orders channel for {inst_type:?}");
            self.ws_private.subscribe_orders(*inst_type).await?;
        }

        self.ws_private.subscribe_account().await?;

        if self.config.load_spreads {
            log::debug!("Subscribing to Nitro spread orders channel");
            self.ws_business.subscribe_spread_orders().await?;
        }

        // Subscribe to algo orders on business WebSocket (OKX requires this endpoint)
        for inst_type in &instrument_types {
            if supports_algo_orders(*inst_type) {
                self.ws_business.subscribe_orders_algo(*inst_type).await?;
                self.ws_business.subscribe_algo_advance(*inst_type).await?;
            }
        }

        let account_state = self
            .http_client
            .request_account_state(self.core.account_id)
            .await
            .context("failed to request OKX account state")?;

        if !account_state.balances.is_empty() {
            log::debug!(
                "Received account state with {} balance(s)",
                account_state.balances.len()
            );
        }
        self.emitter.send_account_state(account_state);

        // Wait for account to be registered in cache before completing connect
        self.await_account_registered(30.0).await?;

        // Arm exchange-side order protection only after every startup dependency has succeeded,
        // but before the execution client advertises itself as connected or admits an order.
        self.start_cancel_all_after_heartbeat().await?;

        self.core.set_connected();
        log::info!("Connected: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        // Mark the shutdown first so supervised task exits caused by closing the transports are
        // not reported as failures. Consumers are aborted before the sockets close for the same
        // reason: a stream ending during an intentional disconnect is not a dead consumer.
        self.shutting_down.store(true, Ordering::Release);
        self.reconnect_tx = None;
        self.abort_pending_tasks();

        // A normal operator stop has already canceled strategy orders. Clear the venue timer so
        // it cannot later cancel unrelated manual orders opened after this process exits. When the
        // shared failure domain is latched the timer intentionally remains armed.
        let protection_result = if should_disable_cancel_all_after_on_disconnect(
            self.cancel_all_after_policy().is_some(),
            self.ws_private.queue_failure().is_some(),
            self.ws_business.queue_failure().is_some(),
        ) {
            let tag = crate::common::consts::OKX_NAUTILUS_BROKER_ID;
            Some(
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    self.http_client.disable_cancel_all_after(Some(tag)),
                )
                .await
                .context("timed out disabling OKX Cancel All After")?,
            )
        } else {
            None
        };
        // Do not latch HTTP cancellation until the normal-stop protection request above has
        // completed; otherwise the client cancels its own CAA disable call.
        self.http_client.cancel_all_requests();

        if let Some(handle) = self.ws_stream_handle.take() {
            handle.abort();
        }

        if let Some(handle) = self.ws_business_stream_handle.take() {
            handle.abort();
        }

        if let Err(e) = self.ws_private.close().await {
            log::warn!("Error closing private websocket: {e:?}");
        }

        if let Err(e) = self.ws_business.close().await {
            log::warn!("Error closing business websocket: {e:?}");
        }

        self.core.set_disconnected();
        log::info!("Disconnected: client_id={}", self.core.client_id);
        if let Some(result) = protection_result {
            result.context("failed to disable OKX Cancel All After during normal disconnect")?;
        }
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        self.update_account_state();
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let account_id = self.core.account_id;
        let emitter = self.emitter.clone();
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id;
        let is_spread = is_spread_instrument(instrument_id);
        let should_query_algo = !is_spread_instrument(instrument_id)
            && supports_algo_orders(okx_instrument_type_from_symbol(
                instrument_id.symbol.as_str(),
            ));

        self.spawn_task("query_order", async move {
            if is_spread {
                let report = http_client
                    .request_spread_order_status_report(
                        account_id,
                        instrument_id,
                        client_order_id,
                        venue_order_id,
                    )
                    .await
                    .context("OKX query_order failed to fetch targeted spread order")?;
                if let Some(report) = report {
                    emitter.send_order_status_report(report);
                } else {
                    log::warn!(
                        "OKX query_order received explicit venue not-found for spread order: \
                         client_order_id={client_order_id}, venue_order_id={venue_order_id:?}"
                    );
                }
                return Ok(());
            }

            let regular_report = http_client
                .request_order_status_report(
                    account_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                )
                .await
                .context("OKX query_order failed to fetch targeted regular order")?;
            if let Some(report) = regular_report {
                emitter.send_order_status_report(report);
                return Ok(());
            }

            if should_query_algo {
                let algo_report = http_client
                    .request_algo_order_status_report_by_ids(
                        account_id,
                        instrument_id,
                        client_order_id,
                        venue_order_id,
                    )
                    .await
                    .context("OKX query_order failed to fetch targeted algo order")?;
                if let Some(report) = algo_report {
                    emitter.send_order_status_report(report);
                    return Ok(());
                }
            }

            log::warn!(
                "OKX query_order received explicit venue not-found from every applicable exact-order endpoint: \
                 client_order_id={client_order_id}, venue_order_id={venue_order_id:?}"
            );
            Ok(())
        });
        Ok(())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        self.http_client.cache_instrument(instrument.clone());
        self.ws_private.cache_instrument(instrument.clone());
        self.ws_business.cache_instrument(instrument);
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        let sender = get_exec_event_sender();
        let ingress_control = sender.control().clone();
        self.ws_private
            .set_queue_failure_callback(live_ingress_failure_callback(
                ingress_control.clone(),
                LiveIngressLane::ExecEvents,
            ));
        self.ws_business
            .set_queue_failure_callback(live_ingress_failure_callback(
                ingress_control,
                LiveIngressLane::ExecEvents,
            ));
        self.emitter.set_sender(sender);
        self.core.set_started();

        let http_client = self.http_client.clone();
        let ws_private = self.ws_private.clone();
        let ws_business = self.ws_business.clone();
        let instrument_types = self.config.instrument_types.clone();
        let instrument_families = self.config.instrument_families.clone();

        get_runtime().spawn(async move {
            let mut all_instruments = Vec::new();
            let mut all_inst_id_codes = Vec::new();

            for instrument_type in instrument_types {
                let Some(families) =
                    resolve_instrument_families(&instrument_families, instrument_type)
                else {
                    continue;
                };

                if families.is_empty() {
                    match http_client.request_instruments(instrument_type, None).await {
                        Ok((instruments, inst_id_codes)) => {
                            if instruments.is_empty() {
                                log::warn!("No instruments returned for {instrument_type:?}");
                                continue;
                            }
                            http_client.cache_instruments(&instruments);
                            all_instruments.extend(instruments);
                            all_inst_id_codes.extend(inst_id_codes);
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to request instruments for {instrument_type:?}: {e}"
                            );
                        }
                    }
                } else {
                    for family in &families {
                        match http_client
                            .request_instruments(instrument_type, Some(family.clone()))
                            .await
                        {
                            Ok((instruments, inst_id_codes)) => {
                                if instruments.is_empty() {
                                    log::warn!(
                                        "No instruments returned for {instrument_type:?} family {family}"
                                    );
                                    continue;
                                }
                                http_client.cache_instruments(&instruments);
                                all_instruments.extend(instruments);
                                all_inst_id_codes.extend(inst_id_codes);
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to request instruments for {instrument_type:?} family {family}: {e}"
                                );
                            }
                        }
                    }
                }
            }

            if all_instruments.is_empty() {
                log::error!(
                    "Instrument bootstrap yielded no instruments, order submissions will fail"
                );
            } else {
                ws_private.cache_instruments(&all_instruments);
                ws_private.cache_inst_id_codes(all_inst_id_codes.clone());
                ws_business.cache_instruments(&all_instruments);
                ws_business.cache_inst_id_codes(all_inst_id_codes);
                log::debug!("Instruments initialized");
            }
        });

        log::info!(
            "Started: client_id={}, account_id={}, account_type={:?}, trade_mode={:?}, instrument_types={:?}, use_fills_channel={}, environment={}, proxy_url={:?}",
            self.core.client_id,
            self.core.account_id,
            self.core.account_type,
            self.trade_mode,
            self.config.instrument_types,
            self.config.use_fills_channel,
            self.config.environment,
            self.config.proxy_url,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        self.core.set_stopped();
        self.core.set_disconnected();

        if let Some(handle) = self.ws_stream_handle.take() {
            handle.abort();
        }

        if let Some(handle) = self.ws_business_stream_handle.take() {
            handle.abort();
        }
        self.abort_pending_tasks();
        log::info!("Stopped: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let Some(instrument_id) = cmd.instrument_id else {
            log::warn!("generate_order_status_report requires instrument_id: {cmd:?}");
            return Ok(None);
        };

        let mut reports = self
            .http_client
            .request_order_status_reports(
                self.core.account_id,
                None,
                Some(instrument_id),
                None,
                None,
                false,
                None,
            )
            .await?;

        if !is_spread_instrument(instrument_id)
            && supports_algo_orders(okx_instrument_type_from_symbol(
                instrument_id.symbol.as_str(),
            ))
        {
            // Merge algo orders (stop, OCO, TP/SL, trailing). They live on a
            // separate OKX endpoint and would otherwise be dropped from
            // reconciliation, leaving stop/conditional orders unrecovered after
            // a restart.
            let algo_reports = require_complete_reconciliation_component(
                self.http_client
                    .request_algo_order_status_reports(
                        self.core.account_id,
                        None,
                        Some(instrument_id),
                        None,
                        cmd.client_order_id,
                        None,
                        None,
                    )
                    .await,
                &format!("algo order status reports for {instrument_id}"),
            )?;
            reports.extend(algo_reports);
        }

        if let Some(client_order_id) = cmd.client_order_id {
            reports.retain(|report| report.client_order_id == Some(client_order_id));
        }

        if let Some(venue_order_id) = cmd.venue_order_id {
            reports.retain(|report| report.venue_order_id.as_str() == venue_order_id.as_str());
        }

        Ok(reports.into_iter().next())
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.reconciliation_source()
            .order_status_reports(OrderSnapshotQuery {
                instrument_id: cmd.instrument_id,
                open_only: cmd.open_only,
                start: cmd.start,
                end: cmd.end,
            })
            .await
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.reconciliation_source()
            .fill_reports(FillSnapshotQuery {
                instrument_id: cmd.instrument_id,
                venue_order_id: cmd.venue_order_id,
                start: cmd.start,
                end: cmd.end,
            })
            .await
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.reconciliation_source()
            .position_status_reports(cmd.instrument_id)
            .await
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        log::info!("Generating ExecutionMassStatus (lookback_mins={lookback_mins:?})");

        let ts_now = self.clock.get_time_ns();
        let start = lookback_mins.map(|mins| {
            let lookback_ns = mins * 60 * 1_000_000_000;
            UnixNanos::from(ts_now.as_u64().saturating_sub(lookback_ns))
        });

        let mass_status = Box::pin(self.reconciliation_source().mass_status(start)).await?;
        Ok(Some(mass_status))
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let route = {
            let cache = self.core.cache();
            let order = cache.try_order(&cmd.client_order_id)?;

            if order.is_closed() {
                log::warn!("Cannot submit closed order {}", order.client_order_id());
                return Ok(());
            }

            if let Err(reason) = validate_okx_client_order_id(cmd.client_order_id.as_str()) {
                let denied = OrderDeniedReason::InvalidClientOrderId { detail: reason };
                self.emitter.emit_order_denied(&order, &denied.to_string());
                return Ok(());
            }

            let order_type = order.order_type();
            let route = self.submit_order_route(cmd.instrument_id, order_type)?;

            log::debug!("OrderSubmitted client_order_id={}", order.client_order_id());
            self.emitter.emit_order_submitted(&order);

            route
        };

        match route {
            OrderCommandRoute::RegularWs => self.submit_regular_order(&cmd),
            OrderCommandRoute::AlgoHttp => self.submit_conditional_order(&cmd),
            OrderCommandRoute::SpreadHttp => self.submit_order_http(&cmd),
        }
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        if is_spread_instrument(cmd.instrument_id) {
            let cache = self.core.cache();
            let denied = OrderDeniedReason::UnsupportedOrderList {
                detail: "spread instruments are not supported in order lists".to_string(),
            }
            .to_string();

            for client_order_id in &cmd.order_list.client_order_ids {
                let order = cache.try_order(client_order_id)?;
                self.emitter.emit_order_denied(&order, &denied);
            }
            return Ok(());
        }

        let inst_type = okx_instrument_type_from_symbol(cmd.instrument_id.symbol.as_str());

        // Validate all orders before emitting any submitted events
        let cache = self.core.cache();

        // Pre-validate every clOrdId so an invalid leg denies the whole list atomically;
        // otherwise sibling legs would be left in the cache without a terminal event.
        let invalid: Vec<(ClientOrderId, String)> = cmd
            .order_list
            .client_order_ids
            .iter()
            .filter_map(|cid| {
                validate_okx_client_order_id(cid.as_str())
                    .err()
                    .map(|r| (*cid, r))
            })
            .collect();

        if !invalid.is_empty() {
            let order_list_id = cmd.order_list.id;
            for client_order_id in &cmd.order_list.client_order_ids {
                let order = cache.try_order(client_order_id)?;
                let denied = invalid
                    .iter()
                    .find(|(cid, _)| cid == client_order_id)
                    .map_or_else(
                        || OrderDeniedReason::OrderListDenied { order_list_id },
                        |(_, r)| OrderDeniedReason::InvalidClientOrderId { detail: r.clone() },
                    );
                self.emitter.emit_order_denied(&order, &denied.to_string());
            }
            return Ok(());
        }

        for client_order_id in &cmd.order_list.client_order_ids {
            let order = cache.try_order(client_order_id)?;

            if self.is_conditional_order(order.order_type()) {
                anyhow::bail!("Conditional orders not supported in order lists: {client_order_id}");
            }

            if order.time_in_force() != TimeInForce::Gtc {
                anyhow::bail!(
                    "Only GTC orders supported in order lists: {client_order_id} has {:?}",
                    order.time_in_force()
                );
            }
        }

        // Build batch payload and emit submitted events
        let mut batch_orders = Vec::new();
        let speed_bump = get_param_as_string(&cmd.params, "speed_bump");
        let outcome = get_param_as_string(&cmd.params, "outcome");
        let rpi = get_param_as_bool(&cmd.params, "rpi");
        let rpi_taker_access = get_param_as_bool(&cmd.params, "rpi_taker_access");
        let rpi_px_round = get_param_as_bool(&cmd.params, "rpi_px_round");

        for client_order_id in &cmd.order_list.client_order_ids {
            let order = cache.order(client_order_id).expect("validated above");

            batch_orders.push((
                inst_type,
                cmd.instrument_id,
                self.trade_mode_for_order(cmd.instrument_id, &cmd.params),
                order.client_order_id(),
                order.order_side(),
                None, // position_side: WS client defaults to Net for derivatives
                order.order_type(),
                order.quantity(),
                order.price(),
                order.trigger_price(),
                Some(order.is_post_only()),
                Some(order.is_reduce_only()),
                speed_bump.clone(),
                outcome.clone(),
                rpi,
                rpi_taker_access,
                rpi_px_round,
            ));

            self.ws_dispatch_state.order_identities.insert(
                order.client_order_id(),
                OrderIdentity {
                    instrument_id: cmd.instrument_id,
                    strategy_id: order.strategy_id(),
                    order_side: order.order_side(),
                    order_type: order.order_type(),
                },
            );

            log::debug!("OrderSubmitted client_order_id={}", order.client_order_id());
            self.emitter.emit_order_submitted(&order);
        }

        drop(cache);

        let ws_private = self.ws_private.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let instrument_id = cmd.instrument_id;
        let strategy_id = cmd.strategy_id;
        let client_order_ids: Vec<_> = cmd.order_list.client_order_ids;
        let dispatch_state = Arc::clone(&self.ws_dispatch_state);

        self.spawn_task("batch_submit_orders", async move {
            let result = ws_private
                .batch_submit_orders(batch_orders)
                .await;

            if let Err(e) = result {
                if is_okx_ws_local_command_failure(&e) {
                    let ts_event = clock.get_time_ns();

                    for cid in &client_order_ids {
                        dispatch_state.order_identities.remove(cid);
                        emitter.emit_order_rejected_event(
                            strategy_id,
                            instrument_id,
                            *cid,
                            &format!("batch-submit-error: {e}"),
                            ts_event,
                            false,
                        );
                    }
                } else {
                    log::warn!(
                        "Ambiguous batch submit failure for {} orders on {instrument_id}, awaiting reconciliation: {e}",
                        client_order_ids.len()
                    );
                }
                return Err(anyhow::Error::new(e).context("batch submit orders failed"));
            }

            Ok(())
        });

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        if is_spread_instrument(cmd.instrument_id) {
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                "OKX spread orders do not support modify requests",
                self.clock.get_time_ns(),
            );
            return Ok(());
        }

        self.ensure_order_identity(cmd.client_order_id, cmd.strategy_id, cmd.instrument_id);

        let ws_private = self.ws_private.clone();
        let command = cmd.clone();

        let new_px_usd = get_param_as_string(&cmd.params, "px_usd");
        let new_px_vol = get_param_as_string(&cmd.params, "px_vol");
        let speed_bump = get_param_as_string(&cmd.params, "speed_bump");
        let rpi_taker_access = get_param_as_bool(&cmd.params, "rpi_taker_access");
        let rpi_px_round = get_param_as_bool(&cmd.params, "rpi_px_round");

        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("modify_order", async move {
            let result = ws_private
                .modify_order(
                    command.trader_id,
                    command.strategy_id,
                    command.instrument_id,
                    Some(command.client_order_id),
                    command.price,
                    command.quantity,
                    command.venue_order_id,
                    new_px_usd,
                    new_px_vol,
                    speed_bump,
                    rpi_taker_access,
                    rpi_px_round,
                )
                .await;

            if let Err(e) = result {
                if is_okx_ws_local_command_failure(&e) {
                    let ts_event = clock.get_time_ns();
                    emitter.emit_order_modify_rejected_event(
                        command.strategy_id,
                        command.instrument_id,
                        command.client_order_id,
                        command.venue_order_id,
                        &format!("modify-order-error: {e}"),
                        ts_event,
                    );
                } else {
                    log::warn!(
                        "Ambiguous modify failure for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                }
                return Err(anyhow::Error::new(e).context("modify order failed"));
            }

            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let route = {
            let cache = self.core.cache();
            let order_state = cache
                .order(&cmd.client_order_id)
                .map(|order| (order.order_type(), order.is_triggered()));
            self.cancel_order_route(cmd.instrument_id, order_state)
        };

        match route {
            OrderCommandRoute::RegularWs => self.cancel_ws_order(&cmd),
            OrderCommandRoute::AlgoHttp => self.cancel_algo_order(&cmd),
            OrderCommandRoute::SpreadHttp => self.cancel_order_http(&cmd),
        }
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        match self.cancel_all_orders_route(cmd.instrument_id) {
            CancelAllOrdersRoute::SpreadHttp | CancelAllOrdersRoute::MassCancelHttp => {
                self.mass_cancel_instrument(cmd.instrument_id);
                Ok(())
            }
            CancelAllOrdersRoute::BatchWs => {
                let cache = self.core.cache();
                let open_orders =
                    cache.orders_open(None, Some(&cmd.instrument_id), None, None, None);

                if open_orders.is_empty() {
                    log::debug!("No open orders to cancel for {}", cmd.instrument_id);
                    return Ok(());
                }

                let mut regular_payload = Vec::new();
                let mut regular_cancel_contexts = Vec::new();
                let mut algo_orders: Vec<(
                    InstrumentId,
                    ClientOrderId,
                    Option<VenueOrderId>,
                    TraderId,
                    StrategyId,
                )> = Vec::new();

                for order in &open_orders {
                    let order_state = Some((order.order_type(), order.is_triggered()));
                    match self.cancel_order_route(order.instrument_id(), order_state) {
                        OrderCommandRoute::RegularWs => {
                            self.ensure_order_identity(
                                order.client_order_id(),
                                order.strategy_id(),
                                order.instrument_id(),
                            );
                            regular_payload.push((
                                order.instrument_id(),
                                Some(order.client_order_id()),
                                order.venue_order_id(),
                            ));
                            regular_cancel_contexts.push((
                                order.client_order_id(),
                                order.instrument_id(),
                                order.strategy_id(),
                            ));
                        }
                        OrderCommandRoute::AlgoHttp => {
                            algo_orders.push((
                                order.instrument_id(),
                                order.client_order_id(),
                                order.venue_order_id(),
                                order.trader_id(),
                                order.strategy_id(),
                            ));
                        }
                        OrderCommandRoute::SpreadHttp => {}
                    }
                }
                drop(open_orders);
                drop(cache);

                log::debug!(
                    "Canceling {} regular orders and {} algo orders for {}",
                    regular_payload.len(),
                    algo_orders.len(),
                    cmd.instrument_id
                );

                if !regular_payload.is_empty() {
                    let ws_private = self.ws_private.clone();

                    self.spawn_task("batch_cancel_orders", async move {
                        if let Err(e) = ws_private.batch_cancel_orders(regular_payload).await {
                            if is_okx_ws_local_command_failure(&e) {
                                log::warn!(
                                    "Batch cancel command failed local validation for {} orders: {e}",
                                    regular_cancel_contexts.len()
                                );
                            } else {
                                log::warn!(
                                    "Ambiguous batch cancel failure for {} orders, awaiting reconciliation: {e}",
                                    regular_cancel_contexts.len()
                                );
                            }
                            return Err(anyhow::Error::new(e).context("batch cancel orders failed"));
                        }
                        Ok(())
                    });
                }

                // OKX doesn't support algo cancel via private WebSocket, must use HTTP
                if !algo_orders.is_empty() {
                    let items: Vec<_> = algo_orders
                        .into_iter()
                        .map(
                            |(
                                instrument_id,
                                client_order_id,
                                venue_order_id,
                                _trader_id,
                                strategy_id,
                            )| {
                                let request = OKXCancelAlgoOrderRequest {
                                    inst_id: instrument_id.symbol.to_string(),
                                    inst_id_code: None,
                                    algo_id: venue_order_id.map(|id| id.to_string()),
                                    algo_cl_ord_id: if venue_order_id.is_none() {
                                        Some(client_order_id.to_string())
                                    } else {
                                        None
                                    },
                                };
                                let ctx = AlgoCancelContext {
                                    client_order_id,
                                    instrument_id,
                                    strategy_id,
                                    venue_order_id,
                                };
                                (request, ctx)
                            },
                        )
                        .collect();
                    self.dispatch_algo_cancels(items);
                }

                Ok(())
            }
        }
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let cache = self.core.cache();

        let mut regular_payload = Vec::new();
        let mut algo_orders = Vec::new();
        let mut http_orders = Vec::new();

        for cancel in &cmd.cancels {
            let order_state = cache
                .order(&cancel.client_order_id)
                .map(|order| (order.order_type(), order.is_triggered()));

            match self.cancel_order_route(cancel.instrument_id, order_state) {
                OrderCommandRoute::RegularWs => {
                    self.ensure_order_identity(
                        cancel.client_order_id,
                        cancel.strategy_id,
                        cancel.instrument_id,
                    );
                    regular_payload.push((
                        cancel.instrument_id,
                        Some(cancel.client_order_id),
                        cancel.venue_order_id,
                    ));
                }
                OrderCommandRoute::AlgoHttp => algo_orders.push(cancel.clone()),
                OrderCommandRoute::SpreadHttp => {
                    self.ensure_order_identity(
                        cancel.client_order_id,
                        cancel.strategy_id,
                        cancel.instrument_id,
                    );
                    http_orders.push((
                        cancel.client_order_id,
                        cancel.instrument_id,
                        cancel.strategy_id,
                        cancel.venue_order_id,
                    ));
                }
            }
        }
        drop(cache);

        if !regular_payload.is_empty() {
            let ws_private = self.ws_private.clone();
            let cancel_contexts: Vec<_> = cmd
                .cancels
                .iter()
                .filter(|c| {
                    regular_payload
                        .iter()
                        .any(|(_, cid, _)| *cid == Some(c.client_order_id))
                })
                .map(|c| (c.client_order_id, c.instrument_id, c.strategy_id))
                .collect();

            self.spawn_task("batch_cancel_orders", async move {
                if let Err(e) = ws_private.batch_cancel_orders(regular_payload).await {
                    if is_okx_ws_local_command_failure(&e) {
                        log::warn!(
                            "Batch cancel command failed local validation for {} orders: {e}",
                            cancel_contexts.len()
                        );
                    } else {
                        log::warn!(
                            "Ambiguous batch cancel failure for {} orders, awaiting reconciliation: {e}",
                            cancel_contexts.len()
                        );
                    }
                    return Err(anyhow::Error::new(e).context("batch cancel orders failed"));
                }
                Ok(())
            });
        }

        // OKX doesn't support algo cancel via private WebSocket, must use HTTP
        if !algo_orders.is_empty() {
            let items: Vec<_> = algo_orders
                .into_iter()
                .map(|cancel| {
                    let request = OKXCancelAlgoOrderRequest {
                        inst_id: cancel.instrument_id.symbol.to_string(),
                        inst_id_code: None,
                        algo_id: cancel.venue_order_id.map(|id| id.to_string()),
                        algo_cl_ord_id: if cancel.venue_order_id.is_none() {
                            Some(cancel.client_order_id.to_string())
                        } else {
                            None
                        },
                    };
                    let ctx = AlgoCancelContext {
                        client_order_id: cancel.client_order_id,
                        instrument_id: cancel.instrument_id,
                        strategy_id: cancel.strategy_id,
                        venue_order_id: cancel.venue_order_id,
                    };
                    (request, ctx)
                })
                .collect();
            self.dispatch_algo_cancels(items);
        }

        if !http_orders.is_empty() {
            let client = self.http_client.clone();
            let emitter = self.emitter.clone();
            let clock = self.clock;

            self.spawn_task("cancel_http_orders", async move {
                for (client_order_id, instrument_id, strategy_id, venue_order_id) in http_orders {
                    if let Err(e) = client
                        .cancel_order(instrument_id, Some(client_order_id), venue_order_id)
                        .await
                    {
                        if is_okx_http_structured_venue_rejection(&e) {
                            let ts_event = clock.get_time_ns();
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                venue_order_id,
                                &format!("cancel-http-order-error: {e}"),
                                ts_event,
                            );
                        } else if is_okx_http_local_command_failure(&e) {
                            log::warn!(
                                "HTTP cancel command failed local validation for {client_order_id}: {e}"
                            );
                        } else {
                            log::warn!(
                                "Ambiguous HTTP cancel failure for {client_order_id}, awaiting reconciliation: {e}"
                            );
                        }
                    }
                }
                Ok(())
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderCommandRoute {
    RegularWs,
    AlgoHttp,
    SpreadHttp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelAllOrdersRoute {
    BatchWs,
    MassCancelHttp,
    SpreadHttp,
}

fn is_okx_http_structured_venue_rejection(error: &OKXHttpError) -> bool {
    matches!(error, OKXHttpError::OkxError { .. })
}

fn is_okx_http_submit_rejection(error: &OKXHttpError) -> bool {
    match error {
        OKXHttpError::OkxError {
            error_code,
            message,
        } => {
            !matches!(error_code.as_str(), "50004" | "51149")
                && (should_retry_error_code(error_code) || !is_ambiguous_okx_http_failure(message))
        }
        _ => false,
    }
}

fn is_okx_http_local_command_failure(error: &OKXHttpError) -> bool {
    match error {
        OKXHttpError::MissingCredentials => true,
        OKXHttpError::ValidationError(message) => !is_ambiguous_okx_http_failure(message),
        _ => false,
    }
}

fn is_okx_ws_local_command_failure(error: &OKXWsError) -> bool {
    match error {
        OKXWsError::ClientError(message) => !is_ambiguous_okx_ws_client_failure(message),
        OKXWsError::JsonError(_) => true,
        _ => false,
    }
}

fn is_ambiguous_okx_http_failure(message: &str) -> bool {
    contains_any_ignore_ascii_case(
        message,
        &[
            "empty response",
            "timeout",
            "timed out",
            "retry",
            "request canceled",
            "network",
            "unexpected http status",
            "status code",
            "failed to deserialize",
            "failed to parse",
        ],
    )
}

fn is_ambiguous_okx_ws_client_failure(message: &str) -> bool {
    contains_any_ignore_ascii_case(
        message,
        &[
            "handler not available",
            "no active websocket client",
            "send failed",
            "timeout",
            "timed out",
            "retry",
            "connection",
            "network",
        ],
    )
}

fn contains_any_ignore_ascii_case(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| {
        value
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
    })
}

fn get_param_as_string(params: &Option<Params>, key: &str) -> Option<String> {
    params.as_ref().and_then(|p| {
        p.get(key).and_then(|v| {
            v.as_str()
                .map(ToString::to_string)
                .or_else(|| v.as_f64().map(|n| n.to_string()))
        })
    })
}

fn get_param_as_bool(params: &Option<Params>, key: &str) -> Option<bool> {
    params.as_ref().and_then(|params| params.get_bool(key))
}

// Picks the report that best answers the query. Tiered so a strong signal
// wins over a weak one regardless of ordering in the merged result set:
//   1. Exact `client_order_id` match.
//   2. Exact `venue_order_id` match (rare: only when the cached vid is
//      still valid; OKX rotates venue_order_id once an algo order triggers).
//
// Triggered-algo recovery is handled by the algo endpoint in the caller,
// which queries by algo_cl_ord_id and returns the parent's algo record
// directly. `linked_order_ids` is deliberately not consulted here because
// it is also populated with attached TP/SL child ids on the parent order,
// which would otherwise let a query for a child match the parent's report.
#[cfg(test)]
fn select_query_order_report(
    reports: Vec<OrderStatusReport>,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
) -> Option<OrderStatusReport> {
    let mut by_vid: Option<OrderStatusReport> = None;

    for report in reports {
        if report.client_order_id == Some(client_order_id) {
            return Some(report);
        }

        if by_vid.is_none()
            && venue_order_id
                .as_ref()
                .is_some_and(|vid| report.venue_order_id.as_str() == vid.as_str())
        {
            by_vid = Some(report);
        }
    }

    by_vid
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::cache::Cache;
    use nautilus_model::{enums::OrderStatus, instruments::Instrument};
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::websocket::{
        dispatch::ExecutionDispatchError,
        messages::{OKXOrderMsg, OKXWsFrame, OKXWsMessage},
    };

    fn account_configuration() -> crate::http::models::OKXAccountConfiguration {
        crate::http::models::OKXAccountConfiguration {
            uid: "123".to_string(),
            main_uid: "123".to_string(),
            account_type: "0".to_string(),
            acct_lv: "1".to_string(),
            pos_mode: crate::common::enums::OKXPositionMode::NetMode,
            label: "test".to_string(),
            ip: "203.0.113.10".to_string(),
            perm: "read_only,trade".to_string(),
        }
    }

    #[rstest]
    #[case(true, false, false, true)]
    #[case(false, false, false, false)]
    #[case(true, true, false, false)]
    #[case(true, false, true, false)]
    fn test_cancel_all_after_disable_only_on_normal_configured_disconnect(
        #[case] configured: bool,
        #[case] private_failed: bool,
        #[case] business_failed: bool,
        #[case] expected: bool,
    ) {
        assert_eq!(
            should_disable_cancel_all_after_on_disconnect(
                configured,
                private_failed,
                business_failed,
            ),
            expected,
        );
    }

    #[rstest]
    fn test_demo_account_configuration_accepts_unbound_key_but_checks_permissions() {
        let config = OKXExecClientConfig::default();
        let mut account = account_configuration();
        account.ip.clear();
        validate_venue_account_configuration(&config, &account).unwrap();

        account.perm = "read_only,trade,withdraw".to_string();
        let error = validate_venue_account_configuration(&config, &account).unwrap_err();
        assert!(error.to_string().contains("withdraw"));
    }

    #[rstest]
    fn test_live_account_configuration_rejects_identity_or_unbound_key() {
        let config = OKXExecClientConfig::builder()
            .environment(OKXEnvironment::Live)
            .expected_uid("123".to_string())
            .expected_main_uid("123".to_string())
            .expected_account_type(0)
            .expected_account_level(1)
            .cancel_all_after_secs(30)
            .build();
        let mut account = account_configuration();
        account.uid = "wrong".to_string();
        let error = validate_venue_account_configuration(&config, &account).unwrap_err();
        assert!(error.to_string().contains("UID mismatch"));

        account.uid = "123".to_string();
        account.ip.clear();
        let error = validate_venue_account_configuration(&config, &account).unwrap_err();
        assert!(error.to_string().contains("IP address"));
    }

    fn build_config(
        margin_mode: Option<OKXMarginMode>,
        use_spot_margin: bool,
    ) -> OKXExecClientConfig {
        OKXExecClientConfig {
            margin_mode,
            use_spot_margin,
            ..OKXExecClientConfig::default()
        }
    }

    #[rstest]
    #[case::spot(OKXInstrumentType::Spot, true)]
    #[case::margin(OKXInstrumentType::Margin, true)]
    #[case::swap(OKXInstrumentType::Swap, true)]
    #[case::futures(OKXInstrumentType::Futures, true)]
    #[case::option(OKXInstrumentType::Option, false)]
    #[case::events(OKXInstrumentType::Events, false)]
    fn test_supports_algo_orders(
        #[case] instrument_type: OKXInstrumentType,
        #[case] expected: bool,
    ) {
        assert_eq!(supports_algo_orders(instrument_type), expected);
    }

    #[rstest]
    fn required_reconciliation_component_propagates_endpoint_failure() {
        let result: anyhow::Result<Vec<u8>> = Err(anyhow::anyhow!("endpoint offline"));

        let error = require_complete_reconciliation_component(result, "algo order status reports")
            .unwrap_err();
        let rendered = format!("{error:#}");

        assert!(rendered.contains("required reconciliation component unavailable"));
        assert!(rendered.contains("algo order status reports"));
        assert!(rendered.contains("endpoint offline"));
    }

    #[rstest]
    fn required_reconciliation_component_preserves_complete_results() {
        let reports =
            require_complete_reconciliation_component(Ok(vec![1_u8, 2_u8]), "spread fill reports")
                .unwrap();

        assert_eq!(reports, vec![1, 2]);
    }

    #[rstest]
    fn execution_client_rejects_unsupported_fills_channel_before_transport_construction() {
        let config = OKXExecClientConfig {
            use_fills_channel: true,
            ..OKXExecClientConfig::default()
        };
        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::from("OKX-FILLS-UNSUPPORTED"),
            *OKX_VENUE,
            OmsType::Hedging,
            config.account_id,
            AccountType::Cash,
            None,
            Rc::new(RefCell::new(Cache::default())),
        );

        let error = match OKXExecutionClient::new(core, config) {
            Ok(_) => panic!("fills channel must fail before client construction"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("fills WebSocket channel is unsupported")
        );
    }

    #[rstest]
    fn native_dispatch_rejects_over_capacity_before_processing_the_message() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("ws_orders_post_only_canceled_first.json");
        let content = std::fs::read_to_string(path).unwrap();
        let frame: OKXWsFrame = serde_json::from_str(&content).unwrap();
        let OKXWsFrame::Data { data, .. } = frame else {
            panic!("expected order data fixture");
        };
        let orders: Vec<OKXOrderMsg> = serde_json::from_value(data).unwrap();
        let clock = get_atomic_clock_realtime();
        let account_id = AccountId::from("OKX-001");
        let emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TRADER-001"),
            account_id,
            AccountType::Margin,
            None,
        );
        let state = WsDispatchState::default();
        let instruments = nautilus_core::AtomicMap::new();
        let mut replay_cache = BoundedExecutionReplayCache::new(0);
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

        let ExecutionDispatchError::ReplayCapacity(error) = error else {
            panic!("expected replay-capacity error");
        };
        assert_eq!(error.capacity(), 0);
        assert_eq!(error.len(), 0);
        assert_eq!(error.requested_new(), 1);
        assert_eq!(replay_cache.len(), 0);
        assert!(order_state_cache.is_empty());
    }

    #[rstest]
    #[case::accepted_despite_timeout("51149", "Order timed out. Please try again.", false)]
    #[case::documented_unknown_outcome(
        "50004",
        "API endpoint request timeout; please check the request result",
        false
    )]
    #[case::unknown_timeout_code("59999", "Order request timed out", false)]
    #[case::temporary_system_rejection("50013", "System busy, please retry later", true)]
    #[case::parameter_rejection("51000", "Parameter state error", true)]
    fn test_is_okx_http_submit_rejection(
        #[case] error_code: &str,
        #[case] message: &str,
        #[case] expected: bool,
    ) {
        let error = OKXHttpError::OkxError {
            error_code: error_code.to_string(),
            message: message.to_string(),
        };

        assert_eq!(is_okx_http_submit_rejection(&error), expected);
    }

    #[rstest]
    fn test_is_okx_http_submit_rejection_ignores_local_error() {
        let error = OKXHttpError::ValidationError("invalid quantity".to_string());

        assert!(!is_okx_http_submit_rejection(&error));
    }

    #[rstest]
    #[case::cash_no_spot_margin(AccountType::Cash, None, false, OKXTradeMode::Cash)]
    #[case::cash_spot_margin_cross(
        AccountType::Cash,
        Some(OKXMarginMode::Cross),
        true,
        OKXTradeMode::Cross
    )]
    #[case::cash_spot_margin_isolated(
        AccountType::Cash,
        Some(OKXMarginMode::Isolated),
        true,
        OKXTradeMode::Isolated
    )]
    #[case::cash_spot_margin_none(AccountType::Cash, None, true, OKXTradeMode::Isolated)]
    #[case::margin_cross(
        AccountType::Margin,
        Some(OKXMarginMode::Cross),
        false,
        OKXTradeMode::Cross
    )]
    #[case::margin_isolated(
        AccountType::Margin,
        Some(OKXMarginMode::Isolated),
        false,
        OKXTradeMode::Isolated
    )]
    #[case::margin_none(AccountType::Margin, None, false, OKXTradeMode::Isolated)]
    fn test_derive_default_trade_mode(
        #[case] account_type: AccountType,
        #[case] margin_mode: Option<OKXMarginMode>,
        #[case] use_spot_margin: bool,
        #[case] expected: OKXTradeMode,
    ) {
        let config = build_config(margin_mode, use_spot_margin);

        let result = OKXExecutionClient::derive_default_trade_mode(account_type, &config);

        assert_eq!(result, expected);
    }

    #[rstest]
    #[case::spot_no_margin("BTC-USDT", None, false, OKXTradeMode::Cash)]
    #[case::spot_cross_margin("BTC-USDT", Some(OKXMarginMode::Cross), true, OKXTradeMode::Cross)]
    #[case::spot_isolated_margin(
        "ETH-USDT",
        Some(OKXMarginMode::Isolated),
        true,
        OKXTradeMode::Isolated
    )]
    #[case::spot_margin_no_mode("BTC-USDT", None, true, OKXTradeMode::Isolated)]
    #[case::swap_cross(
        "BTC-USDT-SWAP",
        Some(OKXMarginMode::Cross),
        false,
        OKXTradeMode::Cross
    )]
    #[case::swap_isolated(
        "BTC-USDT-SWAP",
        Some(OKXMarginMode::Isolated),
        false,
        OKXTradeMode::Isolated
    )]
    #[case::swap_no_mode("ETH-USDT-SWAP", None, false, OKXTradeMode::Isolated)]
    #[case::futures_cross(
        "BTC-USDT-250328",
        Some(OKXMarginMode::Cross),
        false,
        OKXTradeMode::Cross
    )]
    #[case::futures_isolated("BTC-USDT-250328", None, false, OKXTradeMode::Isolated)]
    #[case::option_cross(
        "BTC-USD-250328-50000-C",
        Some(OKXMarginMode::Cross),
        false,
        OKXTradeMode::Cross
    )]
    #[case::option_isolated("BTC-USD-250328-50000-C", None, false, OKXTradeMode::Isolated)]
    fn test_derive_trade_mode_for_instrument(
        #[case] symbol: &str,
        #[case] margin_mode: Option<OKXMarginMode>,
        #[case] use_spot_margin: bool,
        #[case] expected: OKXTradeMode,
    ) {
        let instrument_id = InstrumentId::from(format!("{symbol}.OKX").as_str());

        let result = derive_trade_mode_for_instrument(instrument_id, margin_mode, use_spot_margin);

        assert_eq!(result, expected);
    }

    #[rstest]
    #[case::override_to_cross("cross", OKXTradeMode::Cross)]
    #[case::override_to_cash("cash", OKXTradeMode::Cash)]
    #[case::override_to_isolated("isolated", OKXTradeMode::Isolated)]
    #[case::override_to_spot_isolated("spot_isolated", OKXTradeMode::SpotIsolated)]
    #[case::case_insensitive("CROSS", OKXTradeMode::Cross)]
    fn test_td_mode_param_override(#[case] td_mode_value: &str, #[case] expected: OKXTradeMode) {
        let mut params = Params::new();
        params.insert(
            "td_mode".to_string(),
            Value::String(td_mode_value.to_string()),
        );

        let result = get_param_as_string(&Some(params), "td_mode")
            .and_then(|s| s.parse::<OKXTradeMode>().ok());

        assert_eq!(result, Some(expected));
    }

    #[rstest]
    fn test_td_mode_param_invalid_falls_through() {
        let mut params = Params::new();
        params.insert("td_mode".to_string(), Value::String("invalid".to_string()));

        let result = get_param_as_string(&Some(params), "td_mode")
            .and_then(|s| s.parse::<OKXTradeMode>().ok());

        assert_eq!(result, None);
    }

    #[rstest]
    fn test_td_mode_param_absent_falls_through() {
        let result = get_param_as_string(&None, "td_mode");

        assert_eq!(result, None);
    }

    #[rstest]
    fn test_close_fraction_present_sets_reduce_only_true() {
        let mut params = Params::new();
        params.insert("close_fraction".to_string(), Value::String("1".to_string()));
        let params = Some(params);

        let close_fraction = get_param_as_string(&params, "close_fraction");
        let is_reduce_only = false;
        let reduce_only = if close_fraction.is_some() {
            Some(true)
        } else {
            Some(is_reduce_only)
        };

        assert_eq!(close_fraction, Some("1".to_string()));
        assert_eq!(reduce_only, Some(true));
    }

    #[rstest]
    fn test_close_fraction_absent_preserves_reduce_only() {
        let params: Option<Params> = None;

        let close_fraction = get_param_as_string(&params, "close_fraction");
        let is_reduce_only = false;
        let reduce_only = if close_fraction.is_some() {
            Some(true)
        } else {
            Some(is_reduce_only)
        };

        assert_eq!(close_fraction, None);
        assert_eq!(reduce_only, Some(false));
    }

    #[rstest]
    fn test_close_fraction_absent_with_reduce_only_true() {
        let params: Option<Params> = None;

        let close_fraction = get_param_as_string(&params, "close_fraction");
        let is_reduce_only = true;
        let reduce_only = if close_fraction.is_some() {
            Some(true)
        } else {
            Some(is_reduce_only)
        };

        assert_eq!(close_fraction, None);
        assert_eq!(reduce_only, Some(true));
    }

    fn make_query_order_report(cid: Option<&str>, vid: &str) -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("OKX-001"),
            InstrumentId::from("BTC-USDT.OKX"),
            cid.map(ClientOrderId::from),
            VenueOrderId::from(vid),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::new(1.0, 0),
            Quantity::zero(0),
            UnixNanos::default(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        )
    }

    fn with_linked(mut report: OrderStatusReport, linked: &[&str]) -> OrderStatusReport {
        report.linked_order_ids = Some(linked.iter().map(|s| ClientOrderId::from(*s)).collect());
        report
    }

    #[rstest]
    fn test_select_query_order_report_matches_client_order_id() {
        let reports = vec![make_query_order_report(Some("O-001"), "V-1")];
        let selected = select_query_order_report(reports, ClientOrderId::from("O-001"), None);
        assert_eq!(
            selected.and_then(|r| r.client_order_id),
            Some(ClientOrderId::from("O-001"))
        );
    }

    #[rstest]
    fn test_select_query_order_report_client_wins_over_venue_mismatch() {
        let reports = vec![make_query_order_report(Some("O-001"), "V-1")];
        let selected = select_query_order_report(
            reports,
            ClientOrderId::from("O-001"),
            Some(VenueOrderId::from("V-OTHER")),
        );
        assert_eq!(
            selected.and_then(|r| r.client_order_id),
            Some(ClientOrderId::from("O-001"))
        );
    }

    #[rstest]
    fn test_select_query_order_report_falls_back_to_venue_order_id() {
        // Algo child trigger: report's client_order_id is the child, the
        // command still carries the pre-trigger venue_order_id.
        let reports = vec![make_query_order_report(Some("O-CHILD"), "V-1")];
        let selected = select_query_order_report(
            reports,
            ClientOrderId::from("O-PARENT"),
            Some(VenueOrderId::from("V-1")),
        );
        assert_eq!(
            selected.map(|r| r.venue_order_id.as_str().to_string()),
            Some("V-1".to_string()),
        );
    }

    #[rstest]
    fn test_select_query_order_report_rejects_when_nothing_matches() {
        let reports = vec![make_query_order_report(Some("O-OTHER"), "V-OTHER")];
        let selected = select_query_order_report(
            reports,
            ClientOrderId::from("O-001"),
            Some(VenueOrderId::from("V-1")),
        );
        assert!(selected.is_none());
    }

    #[rstest]
    fn test_select_query_order_report_rejects_when_client_differs_and_no_vid_provided() {
        let reports = vec![make_query_order_report(Some("O-OTHER"), "V-1")];
        let selected = select_query_order_report(reports, ClientOrderId::from("O-001"), None);
        assert!(selected.is_none());
    }

    #[rstest]
    fn test_select_query_order_report_ignores_linked_order_ids_for_parent_with_attached_tp() {
        // Parent order has attached TP/SL children listed in its
        // linked_order_ids. A query for one of those children must NOT
        // resolve to the parent's report via the linked_order_ids.
        let child_cid = "O-CHILD-TP";
        let reports = vec![with_linked(
            make_query_order_report(Some("O-PARENT"), "V-PARENT"),
            &[child_cid, "O-CHILD-SL"],
        )];
        let selected = select_query_order_report(reports, ClientOrderId::from(child_cid), None);
        assert!(selected.is_none());
    }

    #[rstest]
    fn test_select_query_order_report_client_match_wins_over_vid_match_elsewhere() {
        // Ordering invariant: the client_order_id match beats a vid match on
        // a different report regardless of which appears first in the list.
        let reports = vec![
            make_query_order_report(Some("O-OTHER"), "V-1"),
            make_query_order_report(Some("O-001"), "V-2"),
        ];
        let selected = select_query_order_report(
            reports,
            ClientOrderId::from("O-001"),
            Some(VenueOrderId::from("V-1")),
        );
        assert_eq!(
            selected.and_then(|r| r.client_order_id),
            Some(ClientOrderId::from("O-001")),
        );
    }

    fn build_test_exec_client() -> OKXExecutionClient {
        let config = OKXExecClientConfig {
            api_key: Some("test_key".to_string()),
            api_secret: Some("test_secret".to_string()),
            api_passphrase: Some("test_pass".to_string()),
            ..OKXExecClientConfig::default()
        };

        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::from("OKX-TEST"),
            *OKX_VENUE,
            OmsType::Hedging,
            config.account_id,
            AccountType::Cash,
            None,
            cache,
        );

        OKXExecutionClient::new(core, config).expect("failed to build test client")
    }

    #[rstest]
    fn test_on_instrument_writes_through_to_client_caches() {
        // Bus-delivered instrument updates must land in both the private and
        // business WebSocket caches, and in the HTTP cache used by reconciliation.
        use nautilus_model::instruments::stubs::crypto_perpetual_ethusdt;

        let mut client = build_test_exec_client();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let symbol = instrument.symbol().inner();
        let raw_symbol = instrument.raw_symbol().inner();

        client.on_instrument(instrument.clone());

        let private_cache = client.ws_private.instruments_cache_arc();
        let business_cache = client.ws_business.instruments_cache_arc();
        assert_eq!(
            client
                .http_client
                .get_instrument(&raw_symbol)
                .map(|i| i.id()),
            Some(instrument.id()),
        );
        assert_eq!(
            private_cache.load().get(&symbol).map(|i| i.id()),
            Some(instrument.id()),
        );
        assert_eq!(
            business_cache.load().get(&symbol).map(|i| i.id()),
            Some(instrument.id()),
        );
    }
}
