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

//! Risk management engine implementation.

pub mod config;

use std::{cell::RefCell, fmt::Debug, rc::Rc, str::FromStr};

use ahash::AHashMap;
use config::RiskEngineConfig;
use indexmap::IndexMap;
use nautilus_common::{
    cache::Cache,
    clock::Clock,
    logging::{CMD, EVT, RECV},
    messages::{
        execution::{BatchModifyOrders, ModifyOrder, SubmitOrder, SubmitOrderList, TradingCommand},
        system::{
            RiskMaxNotional, RiskStateChangeReason, RiskStateChanged, trading::TradingStateChanged,
        },
    },
    msgbus,
    msgbus::{MessagingSwitchboard, TypedHandler, TypedIntoHandler, get_message_bus},
    runner::{TradingCommandMessage, try_get_trading_cmd_sender},
    throttler::{RateLimit, Throttler},
};
use nautilus_core::{UUID4, UnixNanos, WeakCell};
use nautilus_execution::trailing::{
    trailing_stop_calculate_with_bid_ask, trailing_stop_calculate_with_last,
};
use nautilus_model::{
    accounts::{Account, AccountAny},
    data::{QuoteTick, TradeTick},
    enums::{
        OrderSide, OrderStatus, PositionSide, TimeInForce, TradingState, TrailingOffsetType,
        TriggerType,
    },
    events::{OrderDenied, OrderDeniedReason, OrderEventAny, OrderModifyRejected, PositionEvent},
    identifiers::{AccountId, ClientOrderId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{
        Currency, Money, Price, Quantity, money::MoneyRaw, price::PriceRaw, quantity::QuantityRaw,
    },
};
use nautilus_portfolio::Portfolio;
use rust_decimal::{Decimal, RoundingStrategy};
use thiserror::Error;
use ustr::Ustr;

fn format_rate_limit(rate_limit: &RateLimit) -> String {
    let interval_ns = rate_limit.interval_ns();
    let limit = rate_limit.limit();
    let total_secs = interval_ns / 1_000_000_000;
    let remainder_ns = interval_ns % 1_000_000_000;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if remainder_ns == 0 {
        format!("{limit}/{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        let micros = remainder_ns / 1_000;
        format!("{limit}/{hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}

type SubmitCommandFn = Box<dyn Fn(TradingCommand)>;
type ModifyOrderFn = Box<dyn Fn(ModifyOrder)>;

#[derive(Clone, Copy, Debug)]
struct RiskPriceBounds {
    adverse: Price,
    lower: Price,
    upper: Price,
}

impl RiskPriceBounds {
    const fn fixed(price: Price) -> Self {
        Self {
            adverse: price,
            lower: price,
            upper: price,
        }
    }
}

/// An operator risk-policy change authorized by [`RiskEngine`].
///
/// The wrapped wire snapshot is intentionally private and this type has no public constructor,
/// `Default`, or deserialization implementation. External safe Rust can inspect an authorized
/// change received from the message bus, but cannot manufacture one for durable admission.
#[derive(Debug, Eq, PartialEq)]
pub struct AuthorizedRiskStateChange {
    snapshot: RiskStateChanged,
}

impl AuthorizedRiskStateChange {
    pub(crate) const fn new(snapshot: RiskStateChanged) -> Self {
        Self { snapshot }
    }

    /// Returns the canonical wire snapshot carried by this authorized change.
    #[must_use]
    pub const fn snapshot(&self) -> &RiskStateChanged {
        &self.snapshot
    }

    /// Returns the unique identity of the authorized change.
    #[must_use]
    pub const fn event_id(&self) -> UUID4 {
        self.snapshot.event_id
    }
}

/// Result of an admitted operator-configurable risk-policy operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskMutationOutcome {
    /// The operation was admitted and any requested in-memory change was applied.
    Applied,
    /// The requested state already matched the in-memory risk projection.
    Unchanged,
}

/// Error returned when a risk-policy mutation or restore is rejected.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RiskMutationError {
    /// The event-store admission barrier rejected the authoritative state snapshot.
    #[error("durability admission rejected risk-policy operation on topic '{topic}'")]
    DurabilityRejected {
        /// The topic whose durable admission failed.
        topic: String,
    },
    /// Restore was attempted after authoritative event capture became active.
    #[error("risk-policy restore is forbidden during authoritative event capture")]
    RestoreDuringAuthoritativeCapture,
    /// A restored risk snapshot contains an invalid instrument identifier.
    #[error("invalid risk-state instrument ID '{value}': {reason}")]
    InvalidInstrumentId {
        /// The invalid identifier text.
        value: String,
        /// The parser failure.
        reason: String,
    },
    /// A dynamic or restored max-notional value is invalid.
    #[error("invalid max notional for '{instrument_id}': '{value}' ({reason})")]
    InvalidMaxNotional {
        /// The affected instrument identifier.
        instrument_id: String,
        /// The rejected decimal text.
        value: String,
        /// The validation failure.
        reason: String,
    },
    /// A restored risk snapshot repeats an instrument identifier.
    #[error("duplicate max-notional instrument ID '{instrument_id}' in risk-state snapshot")]
    DuplicateInstrumentId {
        /// The repeated instrument identifier.
        instrument_id: String,
    },
    /// A restored snapshot belongs to a different trader.
    #[error("risk-state snapshot trader mismatch: expected '{expected}', found '{actual}'")]
    TraderMismatch {
        /// The active message-bus trader.
        expected: String,
        /// The snapshot trader.
        actual: String,
    },
    /// A terminal process failure has permanently disabled ordinary re-arming.
    #[error(
        "terminal failure latch is set; only Halted is permitted for this risk-engine instance"
    )]
    TerminalFailureLatched,
    /// Crash recovery was incomplete and no authenticated operator re-arm protocol exists.
    #[error(
        "incomplete-recovery interlock is set; only Halted is permitted because authenticated operator re-arm is not implemented"
    )]
    IncompleteRecoveryInterlock,
}

/// Central risk management engine that validates and controls trading operations.
///
/// The `RiskEngine` provides pre-trade risk checks including order validation,
/// balance verification, position sizing limits, and trading state management. It acts as
/// a gateway between strategy orders and execution.
///
/// These checks are local admission controls, not a guarantee of venue execution price or final
/// account exposure. Market-style orders are denied unless freshness and adverse-price stress are
/// explicitly configured; even then, real-money operation requires adapter or exchange-side price
/// protection for a venue-enforced execution bound.
#[allow(dead_code)]
pub struct RiskEngine {
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    portfolio: Portfolio,
    pub throttled_submit: Throttler<TradingCommand, SubmitCommandFn>,
    pub throttled_modify_order: Throttler<ModifyOrder, ModifyOrderFn>,
    max_notional_per_order: AHashMap<InstrumentId, Decimal>,
    trading_state: TradingState,
    terminal_failure_latched: bool,
    incomplete_recovery_interlock: bool,
    config: RiskEngineConfig,
    command_count: u64,
    event_count: u64,
}

impl Debug for RiskEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RiskEngine)).finish()
    }
}

impl RiskEngine {
    /// Creates a new [`RiskEngine`] instance.
    pub fn new(
        config: RiskEngineConfig,
        portfolio: Portfolio,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> Self {
        let throttled_submit = Self::create_submit_throttler(&config, clock.clone(), cache.clone());

        let throttled_modify_order =
            Self::create_modify_order_throttler(&config, clock.clone(), cache.clone());

        Self {
            clock,
            cache,
            portfolio,
            throttled_submit,
            throttled_modify_order,
            max_notional_per_order: config.max_notional_per_order.clone(),
            trading_state: TradingState::Active,
            terminal_failure_latched: false,
            incomplete_recovery_interlock: false,
            config,
            command_count: 0,
            event_count: 0,
        }
    }

    /// Registers all message bus handlers for the risk engine.
    pub fn register_msgbus_handlers(engine: &Rc<RefCell<Self>>) {
        let weak = WeakCell::from(Rc::downgrade(engine));

        let weak_execute = weak.clone();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::risk_engine_execute(),
            TypedIntoHandler::from(move |cmd: TradingCommand| {
                if let Some(rc) = weak_execute.upgrade() {
                    rc.borrow_mut().execute(cmd);
                }
            }),
        );

        // Queued endpoint for deferred command execution (re-entrancy safe).
        // When a strategy calls `submit_order()` from within an event handler
        // (e.g., `on_order_filled`), the command is routed through this endpoint.
        // In live mode the `TradingCommandSender` queues the command for the next
        // event-loop iteration, preventing a synchronous `deny_order()` from
        // dispatching an `OrderDenied` back into a strategy that still holds a
        // mutable borrow - which would otherwise panic on `RefCell` re-entrancy.
        // If no sender is installed, the queued endpoint falls back to direct dispatch.
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::risk_engine_queue_execute(),
            TypedIntoHandler::from(move |cmd: TradingCommand| {
                if let Some(sender) = try_get_trading_cmd_sender() {
                    sender.execute(TradingCommandMessage::new(
                        MessagingSwitchboard::risk_engine_execute(),
                        cmd,
                    ));
                } else {
                    let endpoint = MessagingSwitchboard::risk_engine_execute();
                    msgbus::send_trading_command(endpoint, cmd);
                }
            }),
        );

        let weak_process = weak.clone();
        msgbus::register_order_event_endpoint(
            MessagingSwitchboard::risk_engine_process(),
            TypedIntoHandler::from(move |event: OrderEventAny| {
                if let Some(rc) = weak_process.upgrade() {
                    rc.borrow_mut().process(event);
                }
            }),
        );

        let weak_order_events = weak.clone();
        msgbus::subscribe_order_events(
            "events.order.*".into(),
            TypedHandler::from(move |event: &OrderEventAny| {
                // Risk-generated events can publish while `execute` still owns the engine,
                // and processing is observational, so skipping reentrant events is safe.
                // TODO: Revisit this if order-event processing gains stateful behavior
                if let Some(rc) = weak_order_events.upgrade()
                    && let Ok(mut engine) = rc.try_borrow_mut()
                {
                    engine.process(event.clone());
                }
            }),
            Some(10),
        );

        let weak_position_events = weak;
        msgbus::subscribe_position_events(
            "events.position.*".into(),
            TypedHandler::from(move |event: &PositionEvent| {
                if let Some(rc) = weak_position_events.upgrade() {
                    rc.borrow_mut().process_position_event(event);
                }
            }),
            Some(10),
        );
    }

    fn create_submit_throttler(
        config: &RiskEngineConfig,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> Throttler<TradingCommand, SubmitCommandFn> {
        let success_handler = {
            Box::new(move |command: TradingCommand| {
                let endpoint = MessagingSwitchboard::exec_engine_queue_execute();
                msgbus::send_trading_command(endpoint, command);
            }) as Box<dyn Fn(TradingCommand)>
        };

        let failure_handler = {
            let cache = cache;
            let clock = clock.clone();
            Box::new(move |command: TradingCommand| {
                let reason = OrderDeniedReason::RateLimitExceeded.to_string();

                match command {
                    TradingCommand::SubmitOrder(submit_order) => {
                        log::warn!(
                            "SubmitOrder for {} DENIED: {reason}",
                            submit_order.client_order_id,
                        );

                        Self::handle_submit_order_cache(&cache, &submit_order);

                        let denied = Self::create_order_denied(&submit_order, &reason, &clock);

                        let endpoint = MessagingSwitchboard::exec_engine_process();
                        msgbus::send_order_event(endpoint, denied);
                    }
                    TradingCommand::SubmitOrderList(submit_order_list) => {
                        log::warn!(
                            "SubmitOrderList for {} DENIED: {reason}",
                            submit_order_list.order_list.id,
                        );

                        let orders: Vec<OrderAny> = cache.borrow().orders_for_ids(
                            &submit_order_list.order_list.client_order_ids,
                            &submit_order_list,
                        );

                        let timestamp = clock.borrow().timestamp_ns();

                        for order in &orders {
                            if order.status() == OrderStatus::Initialized {
                                let denied = OrderEventAny::Denied(OrderDenied::new(
                                    order.trader_id(),
                                    order.strategy_id(),
                                    order.instrument_id(),
                                    order.client_order_id(),
                                    reason.as_str().into(),
                                    UUID4::new(),
                                    timestamp,
                                    timestamp,
                                ));
                                let endpoint = MessagingSwitchboard::exec_engine_process();
                                msgbus::send_order_event(endpoint, denied);
                            }
                        }
                    }
                    _ => {
                        log::error!("Unexpected command type in submit throttler: {command}");
                    }
                }
            }) as Box<dyn Fn(TradingCommand)>
        };

        Throttler::new(
            config.max_order_submit,
            clock,
            "ORDER_SUBMIT_THROTTLER",
            success_handler,
            Some(failure_handler),
            Ustr::from(UUID4::new().as_str()),
        )
    }

    fn create_modify_order_throttler(
        config: &RiskEngineConfig,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> Throttler<ModifyOrder, ModifyOrderFn> {
        let success_handler = {
            Box::new(move |order: ModifyOrder| {
                let endpoint = MessagingSwitchboard::exec_engine_queue_execute();
                msgbus::send_trading_command(endpoint, TradingCommand::ModifyOrder(order));
            }) as Box<dyn Fn(ModifyOrder)>
        };

        let failure_handler = {
            let cache = cache;
            let clock = clock.clone();
            Box::new(move |order: ModifyOrder| {
                let reason = "Exceeded MAX_ORDER_MODIFY_RATE";
                log::warn!(
                    "SubmitOrder for {} DENIED: {}",
                    order.client_order_id,
                    reason
                );

                let Some(order) = Self::get_existing_order(&cache, &order) else {
                    return;
                };

                let rejected = Self::create_modify_rejected(&order, reason, &clock);

                let endpoint = MessagingSwitchboard::exec_engine_process();
                msgbus::send_order_event(endpoint, rejected);
            }) as Box<dyn Fn(ModifyOrder)>
        };

        Throttler::new(
            config.max_order_modify,
            clock,
            "ORDER_MODIFY_THROTTLER",
            success_handler,
            Some(failure_handler),
            Ustr::from(UUID4::new().as_str()),
        )
    }

    fn handle_submit_order_cache(cache: &Rc<RefCell<Cache>>, submit_order: &SubmitOrder) {
        let cache = cache.borrow();
        if !cache.order_exists(&submit_order.client_order_id) {
            log::error!(
                "Order not found in cache for client_order_id: {}",
                submit_order.client_order_id
            );
        }
    }

    fn get_existing_order(cache: &Rc<RefCell<Cache>>, order: &ModifyOrder) -> Option<OrderAny> {
        let cache = cache.borrow();
        if let Some(order) = cache.order(&order.client_order_id) {
            Some(order.clone())
        } else {
            log::error!(
                "Order with command.client_order_id: {} not found",
                order.client_order_id
            );
            None
        }
    }

    fn create_order_denied(
        submit_order: &SubmitOrder,
        reason: &str,
        clock: &Rc<RefCell<dyn Clock>>,
    ) -> OrderEventAny {
        let timestamp = clock.borrow().timestamp_ns();
        OrderEventAny::Denied(OrderDenied::new(
            submit_order.trader_id,
            submit_order.strategy_id,
            submit_order.instrument_id,
            submit_order.client_order_id,
            reason.into(),
            UUID4::new(),
            timestamp,
            timestamp,
        ))
    }

    fn create_modify_rejected(
        order: &OrderAny,
        reason: &str,
        clock: &Rc<RefCell<dyn Clock>>,
    ) -> OrderEventAny {
        let timestamp = clock.borrow().timestamp_ns();
        OrderEventAny::ModifyRejected(OrderModifyRejected::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            reason.into(),
            UUID4::new(),
            timestamp,
            timestamp,
            false,
            order.venue_order_id(),
            order.account_id(),
        ))
    }

    /// Executes a trading command through the risk management pipeline.
    // Required by message bus dispatch
    pub fn execute(&mut self, command: TradingCommand) {
        self.command_count += 1;

        // This will extend to other commands such as `RiskCommand`
        self.handle_command(command);
    }

    /// Processes an order event for risk monitoring and state updates.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "message bus dispatch passes owned order events"
    )]
    pub fn process(&mut self, event: OrderEventAny) {
        self.event_count += 1;

        // This will extend to other events such as `RiskEvent`
        self.handle_event(&event);
    }

    fn process_position_event(&mut self, event: &PositionEvent) {
        self.event_count += 1;

        self.handle_position_event(event);
    }

    /// Sets the trading state for risk control enforcement.
    pub fn set_trading_state(&mut self, state: TradingState) {
        if let Err(error) = self.try_set_trading_state(state) {
            log::error!("Failed to set trading state: {error}");
        }
    }

    /// Irreversibly latches and halts in-memory order admission for a terminal process failure.
    ///
    /// Normal policy changes must use [`Self::try_set_trading_state`] so the new policy is
    /// durably admitted before it takes effect. A terminal infrastructure failure is the one
    /// exception: the durability path itself may be the failed dependency, and allowing trading
    /// to remain active would be less safe than losing the final notification. The caller must
    /// request immediate system shutdown after invoking this method. Neither an ordinary state
    /// mutation, [`Self::try_reset`], nor a restored historical checkpoint can clear this latch;
    /// constructing a fresh engine is the only reinitialization boundary currently implemented.
    pub fn force_halt_for_terminal_failure(&mut self, reason: &str) {
        self.terminal_failure_latched = true;
        self.trading_state = TradingState::Halted;
        log::error!("Risk engine terminally halted and latched: {reason}");
    }

    /// Halts admission and latches an incomplete crash-recovery interlock.
    ///
    /// A recovery source with an unknown or failed terminal outcome is not operator authorization
    /// to resume trading, even when it contains a complete historical risk checkpoint. This method
    /// leaves configured and dynamically restored limits intact, but ordinary state changes,
    /// [`Self::try_reset`], and later checkpoint restores cannot transition this engine away from
    /// `Halted`. No authenticated re-arm protocol is currently implemented; a production recovery
    /// therefore remains safe-disabled for the lifetime of this engine instance.
    pub fn force_halt_for_incomplete_recovery(&mut self, reason: &str) {
        self.incomplete_recovery_interlock = true;
        self.trading_state = TradingState::Halted;
        log::error!("Risk engine halted by incomplete-recovery interlock: {reason}");
    }

    /// Durably admits and applies a trading-state change.
    ///
    /// The authoritative full risk-policy snapshot is handed to the message-bus admission
    /// barrier before the in-memory state changes. A rejected admission leaves the engine
    /// unchanged. The legacy [`TradingStateChanged`] notification is published only after the
    /// admitted snapshot has been applied.
    ///
    /// # Errors
    ///
    /// Returns [`RiskMutationError::TerminalFailureLatched`] or
    /// [`RiskMutationError::IncompleteRecoveryInterlock`] when a caller requests any state other
    /// than `Halted` after the corresponding safety interlock was set. Returns
    /// [`RiskMutationError::DurabilityRejected`] when the bus tap rejects the snapshot.
    pub fn try_set_trading_state(
        &mut self,
        state: TradingState,
    ) -> Result<RiskMutationOutcome, RiskMutationError> {
        self.ensure_state_allowed_by_interlocks(state)?;

        if state == self.trading_state {
            log::warn!("No change to trading state: already set to {state:?}");
            return Ok(RiskMutationOutcome::Unchanged);
        }

        let snapshot = self.risk_state_snapshot(
            state,
            &self.max_notional_per_order,
            RiskStateChangeReason::TradingStateChanged,
        );
        self.admit_risk_state(&snapshot, |engine| engine.trading_state = state)?;

        let config = self.config_as_map();
        let event = TradingStateChanged::new(
            snapshot.trader_id,
            state,
            config,
            UUID4::new(),
            snapshot.ts_event,
            snapshot.ts_init,
        );

        msgbus::publish_any("events.risk".into(), &event);

        log::info!("Trading state set to {state:?}");
        Ok(RiskMutationOutcome::Applied)
    }

    /// Sets the maximum notional value per order for the specified instrument.
    pub fn set_max_notional_per_order(&mut self, instrument_id: InstrumentId, new_value: Decimal) {
        if let Err(error) = self.try_set_max_notional_per_order(instrument_id, new_value) {
            log::error!("Failed to set max notional per order: {error}");
        }
    }

    /// Durably admits and applies a max-notional-per-order change.
    ///
    /// # Errors
    ///
    /// Returns [`RiskMutationError::InvalidMaxNotional`] when `new_value` is not positive, or
    /// [`RiskMutationError::DurabilityRejected`] when the bus tap rejects the full post-policy
    /// snapshot. Either failure leaves the configured limits unchanged.
    pub fn try_set_max_notional_per_order(
        &mut self,
        instrument_id: InstrumentId,
        new_value: Decimal,
    ) -> Result<RiskMutationOutcome, RiskMutationError> {
        Self::validate_max_notional(instrument_id, new_value)?;

        if self.max_notional_per_order.get(&instrument_id) == Some(&new_value) {
            return Ok(RiskMutationOutcome::Unchanged);
        }

        let mut post_state = self.max_notional_per_order.clone();
        post_state.insert(instrument_id, new_value);
        let snapshot = self.risk_state_snapshot(
            self.trading_state,
            &post_state,
            RiskStateChangeReason::MaxNotionalChanged,
        );
        self.admit_risk_state(&snapshot, |engine| {
            engine
                .max_notional_per_order
                .insert(instrument_id, new_value);
        })?;

        let new_value_str = new_value.to_string();
        log::info!("Set MAX_NOTIONAL_PER_ORDER: {instrument_id} {new_value_str}");
        Ok(RiskMutationOutcome::Applied)
    }

    /// Starts the risk engine.
    pub fn start(&mut self) {
        log::info!("Started");
    }

    /// Stops the risk engine.
    pub fn stop(&mut self) {
        log::info!("Stopped");
    }

    /// Resets the risk engine to its initial state.
    pub fn reset(&mut self) {
        if let Err(error) = self.try_reset() {
            log::error!("Failed to reset risk engine: {error}");
        }
    }

    /// Durably admits and applies a reset to the configured initial risk state.
    ///
    /// # Errors
    ///
    /// Returns [`RiskMutationError::TerminalFailureLatched`] or
    /// [`RiskMutationError::IncompleteRecoveryInterlock`] after a terminal failure or incomplete
    /// recovery because reset would implicitly re-arm `Active`. Returns
    /// [`RiskMutationError::DurabilityRejected`] when the bus tap rejects the reset policy
    /// snapshot. A rejected reset leaves all risk fields, counters, and throttlers unchanged.
    pub fn try_reset(&mut self) -> Result<RiskMutationOutcome, RiskMutationError> {
        self.ensure_state_allowed_by_interlocks(TradingState::Active)?;

        let post_state = self.config.max_notional_per_order.clone();
        let snapshot = self.risk_state_snapshot(
            TradingState::Active,
            &post_state,
            RiskStateChangeReason::Reset,
        );
        self.admit_risk_state(&snapshot, move |engine| {
            engine.throttled_submit.reset();
            engine.throttled_modify_order.reset();
            engine.max_notional_per_order = post_state;
            engine.trading_state = TradingState::Active;
            engine.command_count = 0;
            engine.event_count = 0;
        })?;

        log::info!("Reset");
        Ok(RiskMutationOutcome::Applied)
    }

    /// Durably checkpoints the operator-configurable risk policy.
    ///
    /// This checkpoint covers the trading state and per-instrument maximum order notionals. It
    /// deliberately excludes throttler rolling windows, buffers, timers, and diagnostic counters.
    /// The checkpoint is admitted as an [`AuthorizedRiskStateChange`] and does not mutate policy.
    ///
    /// # Errors
    ///
    /// Returns [`RiskMutationError::DurabilityRejected`] when the bus tap rejects the checkpoint.
    pub fn try_checkpoint_risk_state(&mut self) -> Result<RiskMutationOutcome, RiskMutationError> {
        let snapshot = self.risk_state_snapshot(
            self.trading_state,
            &self.max_notional_per_order,
            RiskStateChangeReason::Initialized,
        );
        self.admit_risk_state(&snapshot, |_| {})?;
        Ok(RiskMutationOutcome::Applied)
    }

    /// Restores a previously durable operator-configurable risk-policy checkpoint.
    ///
    /// Restore is an applicator for already-admitted replay input and therefore does not publish
    /// another event. The complete policy snapshot is validated before either covered field
    /// changes. A successful restore conservatively replaces unavailable throttler rolling-window
    /// state with a full configured-interval recovery cooldown.
    ///
    /// This reducer does not treat a historical checkpoint as operator authorization. If either a
    /// terminal-failure latch or incomplete-recovery interlock is already set, notionals are still
    /// restored from the validated snapshot but its trading state is forced to remain `Halted`.
    /// Ordinary state/reset APIs cannot re-arm such an engine; no authenticated re-arm protocol is
    /// currently implemented.
    ///
    /// Restore is permitted only before an exclusive message-bus tap establishes authoritative
    /// event capture. Once capture is active, policy changes must use the admitted mutation APIs.
    ///
    /// # Errors
    ///
    /// Returns an error when authoritative capture is active, the checkpoint belongs to another
    /// trader, repeats an instrument, contains an invalid instrument ID or decimal, or contains a
    /// non-positive max notional.
    pub fn restore_risk_state(
        &mut self,
        snapshot: &RiskStateChanged,
    ) -> Result<RiskMutationOutcome, RiskMutationError> {
        if msgbus::is_exclusive_bus_tap_installed() {
            return Err(RiskMutationError::RestoreDuringAuthoritativeCapture);
        }

        let expected_trader = get_message_bus().borrow().trader_id;
        if snapshot.trader_id != expected_trader {
            return Err(RiskMutationError::TraderMismatch {
                expected: expected_trader.to_string(),
                actual: snapshot.trader_id.to_string(),
            });
        }

        let restored = Self::parse_max_notionals(&snapshot.max_notional_per_order)?;
        let restored_state = if self.has_safety_interlock() {
            TradingState::Halted
        } else {
            snapshot.state
        };
        if restored_state == self.trading_state && restored == self.max_notional_per_order {
            self.throttled_submit.enter_recovery_cooldown();
            self.throttled_modify_order.enter_recovery_cooldown();
            return Ok(RiskMutationOutcome::Unchanged);
        }

        self.trading_state = restored_state;
        self.max_notional_per_order = restored;
        self.throttled_submit.enter_recovery_cooldown();
        self.throttled_modify_order.enter_recovery_cooldown();
        Ok(RiskMutationOutcome::Applied)
    }

    /// Disposes of the risk engine, releasing resources.
    pub fn dispose(&mut self) {
        log::info!("Disposed");
    }

    /// Returns a reference to the clock.
    #[must_use]
    pub fn clock(&self) -> &Rc<RefCell<dyn Clock>> {
        &self.clock
    }

    /// Returns a reference to the cache.
    #[must_use]
    pub fn cache(&self) -> &Rc<RefCell<Cache>> {
        &self.cache
    }

    /// Returns a mutable reference to the portfolio.
    pub fn portfolio_mut(&mut self) -> &mut Portfolio {
        &mut self.portfolio
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &RiskEngineConfig {
        &self.config
    }

    /// Returns the total count of trading commands received by the engine.
    #[must_use]
    pub const fn command_count(&self) -> u64 {
        self.command_count
    }

    /// Returns the total count of order events received by the engine.
    #[must_use]
    pub const fn event_count(&self) -> u64 {
        self.event_count
    }

    /// Returns the current trading state.
    #[must_use]
    pub const fn trading_state(&self) -> TradingState {
        self.trading_state
    }

    /// Returns whether a terminal process failure permanently latched this engine instance.
    #[must_use]
    pub const fn is_terminal_failure_latched(&self) -> bool {
        self.terminal_failure_latched
    }

    /// Returns whether incomplete recovery has safe-disabled this engine instance.
    #[must_use]
    pub const fn is_incomplete_recovery_interlocked(&self) -> bool {
        self.incomplete_recovery_interlock
    }

    /// Returns a reference to the max notional per order settings.
    #[must_use]
    pub const fn max_notional_per_order(&self) -> &AHashMap<InstrumentId, Decimal> {
        &self.max_notional_per_order
    }

    const fn has_safety_interlock(&self) -> bool {
        self.terminal_failure_latched || self.incomplete_recovery_interlock
    }

    fn ensure_state_allowed_by_interlocks(
        &self,
        state: TradingState,
    ) -> Result<(), RiskMutationError> {
        if state == TradingState::Halted {
            return Ok(());
        }
        if self.terminal_failure_latched {
            return Err(RiskMutationError::TerminalFailureLatched);
        }
        if self.incomplete_recovery_interlock {
            return Err(RiskMutationError::IncompleteRecoveryInterlock);
        }
        Ok(())
    }

    fn risk_state_snapshot(
        &self,
        state: TradingState,
        max_notional_per_order: &AHashMap<InstrumentId, Decimal>,
        reason: RiskStateChangeReason,
    ) -> RiskStateChanged {
        let ts_now = self.clock.borrow().timestamp_ns();
        let trader_id = get_message_bus().borrow().trader_id;
        RiskStateChanged::new(
            trader_id,
            state,
            max_notional_per_order
                .iter()
                .map(|(instrument_id, value)| (*instrument_id, *value)),
            reason,
            UUID4::new(),
            ts_now,
            ts_now,
        )
    }

    fn admit_risk_state(
        &mut self,
        snapshot: &RiskStateChanged,
        apply: impl FnOnce(&mut Self),
    ) -> Result<(), RiskMutationError> {
        let topic = MessagingSwitchboard::risk_state_topic();
        let authorized = AuthorizedRiskStateChange::new(snapshot.clone());
        if !msgbus::publish_any_with_admission(topic, &authorized, || apply(self)) {
            return Err(RiskMutationError::DurabilityRejected {
                topic: topic.to_string(),
            });
        }
        Ok(())
    }

    fn validate_max_notional(
        instrument_id: InstrumentId,
        value: Decimal,
    ) -> Result<(), RiskMutationError> {
        if value <= Decimal::ZERO {
            return Err(RiskMutationError::InvalidMaxNotional {
                instrument_id: instrument_id.to_string(),
                value: value.to_string(),
                reason: "value must be positive".to_string(),
            });
        }
        Ok(())
    }

    fn parse_max_notionals(
        entries: &[RiskMaxNotional],
    ) -> Result<AHashMap<InstrumentId, Decimal>, RiskMutationError> {
        let mut restored = AHashMap::with_capacity(entries.len());
        for entry in entries {
            let instrument_id = InstrumentId::from_str(&entry.instrument_id).map_err(|error| {
                RiskMutationError::InvalidInstrumentId {
                    value: entry.instrument_id.clone(),
                    reason: error.to_string(),
                }
            })?;

            if restored.contains_key(&instrument_id) {
                return Err(RiskMutationError::DuplicateInstrumentId {
                    instrument_id: entry.instrument_id.clone(),
                });
            }

            let value = Decimal::from_str(&entry.value).map_err(|error| {
                RiskMutationError::InvalidMaxNotional {
                    instrument_id: entry.instrument_id.clone(),
                    value: entry.value.clone(),
                    reason: error.to_string(),
                }
            })?;
            Self::validate_max_notional(instrument_id, value)?;
            restored.insert(instrument_id, value);
        }
        Ok(restored)
    }

    fn config_as_map(&self) -> IndexMap<String, String> {
        let mut map = IndexMap::new();
        map.insert("bypass".to_string(), self.config.bypass.to_string());
        map.insert(
            "max_order_submit_rate".to_string(),
            format_rate_limit(&self.config.max_order_submit),
        );
        map.insert(
            "max_order_modify_rate".to_string(),
            format_rate_limit(&self.config.max_order_modify),
        );

        let mut max_notionals = self.max_notional_per_order.iter().collect::<Vec<_>>();
        max_notionals.sort_unstable_by_key(|(instrument_id, _)| instrument_id.to_string());
        for (instrument_id, value) in max_notionals {
            map.insert(
                format!("max_notional_per_order.{instrument_id}"),
                value.normalize().to_string(),
            );
        }

        map.insert(
            "max_market_data_age_ns".to_string(),
            self.config
                .max_market_data_age_ns
                .map_or_else(|| "None".to_string(), |value| value.to_string()),
        );
        map.insert(
            "market_order_price_bound_bps".to_string(),
            self.config
                .market_order_price_bound_bps
                .map_or_else(|| "None".to_string(), |value| value.to_string()),
        );

        map.insert("debug".to_string(), self.config.debug.to_string());
        map
    }

    fn handle_command(&mut self, command: TradingCommand) {
        if self.config.debug {
            log::debug!("{CMD}{RECV} {command:?}");
        }

        match command {
            TradingCommand::SubmitOrder(submit_order) => self.handle_submit_order(submit_order),
            TradingCommand::SubmitOrderList(submit_order_list) => {
                self.handle_submit_order_list(submit_order_list);
            }
            TradingCommand::ModifyOrder(modify_order) => self.handle_modify_order(modify_order),
            TradingCommand::ModifyOrders(modify_orders) => {
                self.handle_batch_modify_orders(modify_orders);
            }
            TradingCommand::QueryAccount(query_account) => {
                Self::send_to_execution(TradingCommand::QueryAccount(query_account));
            }
            _ => {
                log::error!("Cannot handle command: {command}");
            }
        }
    }

    fn handle_submit_order(&mut self, command: SubmitOrder) {
        if self.trading_state == TradingState::Halted {
            self.deny_command(
                TradingCommand::SubmitOrder(command),
                &OrderDeniedReason::TradingHalted.to_string(),
            );
            return;
        }

        if self.config.bypass && self.trading_state != TradingState::Reducing {
            Self::send_to_execution(TradingCommand::SubmitOrder(command));
            return;
        }

        let order = {
            let cache = self.cache.borrow();
            let Some(order) = cache.order(&command.client_order_id) else {
                log::error!(
                    "Cannot handle submit order: order not found in cache for {}",
                    command.client_order_id
                );
                return;
            };
            order.clone()
        };

        if let Some(position_id) = command.position_id
            && order.is_reduce_only()
        {
            let position_exists = {
                let cache = self.cache.borrow();
                cache
                    .position(&position_id)
                    .map(|pos| (pos.side, pos.quantity))
            };

            if let Some((pos_side, pos_quantity)) = position_exists {
                if !order.would_reduce_only(pos_side, pos_quantity) {
                    self.deny_command(
                        TradingCommand::SubmitOrder(command),
                        &OrderDeniedReason::ReduceOnlyWouldIncreasePosition { position_id }
                            .to_string(),
                    );
                    return; // Denied
                }
            } else {
                self.deny_command(
                    TradingCommand::SubmitOrder(command),
                    &OrderDeniedReason::PositionNotFound { position_id }.to_string(),
                );
                return;
            }
        }

        let instrument_exists = {
            let cache = self.cache.borrow();
            cache.instrument(&command.instrument_id).cloned()
        };

        let Some(instrument) = instrument_exists else {
            self.deny_command(
                TradingCommand::SubmitOrder(command.clone()),
                &OrderDeniedReason::InstrumentNotFound {
                    instrument_id: command.instrument_id,
                }
                .to_string(),
            );
            return; // Denied
        };

        if self
            .first_non_reducing_order(std::slice::from_ref(&order))
            .is_some()
        {
            self.deny_command(
                TradingCommand::SubmitOrder(command),
                &OrderDeniedReason::TradingStateReducing {
                    order_side: order.order_side(),
                    instrument_id: instrument.id(),
                }
                .to_string(),
            );
            return;
        }

        if self.config.bypass {
            Self::send_to_execution(TradingCommand::SubmitOrder(command));
            return;
        }

        if !self.check_order(&instrument, &order) {
            return; // Denied
        }

        if !self.check_orders_risk(&[order]) {
            return; // Denied
        }

        // Route through execution gateway for TradingState checks & throttling
        self.execution_gateway(TradingCommand::SubmitOrder(command));
    }

    fn handle_submit_order_list(&mut self, command: SubmitOrderList) {
        if self.trading_state == TradingState::Halted {
            self.deny_command(
                TradingCommand::SubmitOrderList(command),
                &OrderDeniedReason::TradingHalted.to_string(),
            );
            return;
        }

        if self.config.bypass && self.trading_state != TradingState::Reducing {
            Self::send_to_execution(TradingCommand::SubmitOrderList(command));
            return;
        }

        let orders: Vec<OrderAny> = self
            .cache
            .borrow()
            .orders_for_ids(&command.order_list.client_order_ids, &command);

        if orders.len() != command.order_list.client_order_ids.len() {
            self.deny_order_list(
                &orders,
                &OrderDeniedReason::OrderListIncomplete {
                    order_list_id: command.order_list.id,
                }
                .to_string(),
            );
            return; // Denied
        }

        // Per-order checks use each order's own instrument; the cumulative
        // risk check uses the representative. See docs/concepts/orders.md
        // (Order lists -> Caveats for mixed-instrument lists).
        let mut instruments: AHashMap<InstrumentId, InstrumentAny> = AHashMap::new();

        for order in &orders {
            let instrument_id = order.instrument_id();
            if instruments.contains_key(&instrument_id) {
                continue;
            }
            let resolved = self.cache.borrow().instrument(&instrument_id).cloned();
            let Some(instrument) = resolved else {
                self.deny_command(
                    TradingCommand::SubmitOrderList(command),
                    &OrderDeniedReason::InstrumentNotFound { instrument_id }.to_string(),
                );
                return; // Denied
            };
            instruments.insert(instrument_id, instrument);
        }

        if let Some(order) = self.first_non_reducing_order(&orders) {
            self.deny_order_list(
                &orders,
                &OrderDeniedReason::TradingStateReducing {
                    order_side: order.order_side(),
                    instrument_id: order.instrument_id(),
                }
                .to_string(),
            );
            return;
        }

        if self.config.bypass {
            Self::send_to_execution(TradingCommand::SubmitOrderList(command));
            return;
        }

        for order in &orders {
            let Some(instrument) = instruments.get(&order.instrument_id()) else {
                self.deny_order(
                    order,
                    &OrderDeniedReason::InstrumentNotFound {
                        instrument_id: order.instrument_id(),
                    }
                    .to_string(),
                );
                return; // Denied
            };

            if !self.check_order(instrument, order) {
                return; // Denied
            }
        }

        if !instruments.contains_key(&command.instrument_id) {
            self.deny_order_list(
                &orders,
                &OrderDeniedReason::InstrumentNotFound {
                    instrument_id: command.instrument_id,
                }
                .to_string(),
            );
            return; // Denied
        }

        if !self.check_orders_risk(&orders) {
            self.deny_order_list(
                &orders,
                &OrderDeniedReason::OrderListDenied {
                    order_list_id: command.order_list.id,
                }
                .to_string(),
            );
            return; // Denied
        }

        self.execution_gateway(TradingCommand::SubmitOrderList(command));
    }

    fn handle_modify_order(&mut self, command: ModifyOrder) {
        if self.trading_state == TradingState::Halted {
            if let Some(order) = Self::get_existing_order(&self.cache, &command) {
                self.reject_modify_order(&order, "TradingState is HALTED: Cannot modify order");
            }
            return;
        }

        if self.config.bypass && self.trading_state != TradingState::Reducing {
            Self::send_to_execution(TradingCommand::ModifyOrder(command));
            return;
        }

        if !self.validate_modify_order(&command, true) {
            return;
        }

        if self.config.bypass {
            Self::send_to_execution(TradingCommand::ModifyOrder(command));
        } else {
            self.throttled_modify_order.send(command);
        }
    }

    fn handle_batch_modify_orders(&mut self, command: BatchModifyOrders) {
        if self.trading_state == TradingState::Halted {
            for modify in &command.modifies {
                if let Some(order) = Self::get_existing_order(&self.cache, modify) {
                    self.reject_modify_order(&order, "TradingState is HALTED: Cannot modify order");
                }
            }
            return;
        }

        if self.config.bypass && self.trading_state != TradingState::Reducing {
            Self::send_to_execution(TradingCommand::ModifyOrders(command));
            return;
        }

        if command.modifies.is_empty() {
            log::warn!("Cannot handle BatchModifyOrders: no modify commands");
            return;
        }

        let mut rejected_client_order_ids = Vec::new();
        let mut valid = true;

        for modify in &command.modifies {
            if modify.instrument_id != command.instrument_id {
                if let Some(order) = self
                    .cache
                    .borrow()
                    .order(&modify.client_order_id)
                    .map(|o| o.clone())
                {
                    self.reject_modify_order(
                        &order,
                        &format!(
                            "BatchModifyOrders instrument {} does not match child instrument {}",
                            command.instrument_id, modify.instrument_id
                        ),
                    );
                }
                rejected_client_order_ids.push(modify.client_order_id);
                valid = false;
                continue;
            }

            if !self.validate_modify_order(modify, false) {
                rejected_client_order_ids.push(modify.client_order_id);
                valid = false;
            }
        }

        if valid && let Some(modify) = self.first_non_reducing_modify(&command.modifies) {
            if let Some(order) = Self::get_existing_order(&self.cache, modify) {
                self.reject_modify_order(
                    &order,
                    &format!(
                        "TradingState is REDUCING and update cannot be proven to reduce position {} without crossing flat",
                        order.instrument_id()
                    ),
                );
            }
            rejected_client_order_ids.push(modify.client_order_id);
            valid = false;
        }

        if !valid {
            let reason = "BatchModifyOrders rejected because one or more child modifications failed validation";

            for modify in &command.modifies {
                if rejected_client_order_ids.contains(&modify.client_order_id) {
                    continue;
                }

                let Some(order) = Self::get_existing_order(&self.cache, modify) else {
                    continue;
                };

                self.reject_modify_order(&order, reason);
            }
            return;
        }

        if self.config.bypass {
            Self::send_to_execution(TradingCommand::ModifyOrders(command));
            return;
        }

        if !self
            .throttled_modify_order
            .try_reserve(command.modifies.len())
        {
            let reason = "Exceeded MAX_ORDER_MODIFY_RATE";

            for modify in &command.modifies {
                let Some(order) = Self::get_existing_order(&self.cache, modify) else {
                    continue;
                };
                self.reject_modify_order(&order, reason);
            }
            return;
        }

        Self::send_to_execution(TradingCommand::ModifyOrders(command));
    }

    fn validate_modify_order(&self, command: &ModifyOrder, check_reducing: bool) -> bool {
        let order_exists = {
            let cache = self.cache.borrow();
            cache.order(&command.client_order_id).map(|o| o.clone())
        };

        let Some(order) = order_exists else {
            log::error!(
                "ModifyOrder DENIED: Order with command.client_order_id: {} not found",
                command.client_order_id
            );
            return false;
        };

        if order.is_closed() {
            self.reject_modify_order(
                &order,
                &format!(
                    "Order with command.client_order_id: {} already closed",
                    command.client_order_id
                ),
            );
            return false;
        } else if order.status() == OrderStatus::PendingCancel {
            self.reject_modify_order(
                &order,
                &format!(
                    "Order with command.client_order_id: {} is already pending cancel",
                    command.client_order_id
                ),
            );
            return false;
        }

        let maybe_instrument = {
            let cache = self.cache.borrow();
            cache.instrument(&command.instrument_id).cloned()
        };

        let Some(instrument) = maybe_instrument else {
            self.reject_modify_order(
                &order,
                &format!("no instrument found for {:?}", command.instrument_id),
            );
            return false;
        };

        // Check Price
        let mut risk_msg = Self::check_price(&instrument, command.price);
        if let Some(risk_msg) = risk_msg {
            self.reject_modify_order(&order, &risk_msg);
            return false;
        }

        // Check Trigger
        risk_msg = Self::check_price(&instrument, command.trigger_price);
        if let Some(risk_msg) = risk_msg {
            self.reject_modify_order(&order, &risk_msg);
            return false;
        }

        // Check Quantity
        risk_msg = Self::check_quantity(&instrument, command.quantity, order.is_quote_quantity());
        if let Some(risk_msg) = risk_msg {
            self.reject_modify_order(&order, &risk_msg);
            return false;
        }

        // Check TradingState
        match self.trading_state {
            TradingState::Halted => {
                self.reject_modify_order(&order, "TradingState is HALTED: Cannot modify order");
                return false;
            }
            TradingState::Reducing => {
                if check_reducing
                    && self
                        .first_non_reducing_modify(std::slice::from_ref(command))
                        .is_some()
                {
                    self.reject_modify_order(
                        &order,
                        &format!(
                            "TradingState is REDUCING and update cannot be proven to reduce position {} without crossing flat",
                            instrument.id()
                        ),
                    );
                    return false;
                }
            }
            TradingState::Active => {}
        }

        true
    }

    fn check_order(&self, instrument: &InstrumentAny, order: &OrderAny) -> bool {
        if order.order_side() == OrderSide::NoOrderSide {
            self.deny_order(
                order,
                &OrderDeniedReason::InvalidOrderSide {
                    order_side: order.order_side(),
                }
                .to_string(),
            );
            return false;
        }

        if !self.check_order_price(instrument, order)
            || !self.check_order_quantity(instrument, order)
        {
            return false; // Denied
        }

        if order.time_in_force() == TimeInForce::Gtd {
            let Some(expire_time) = order.expire_time() else {
                self.deny_order(order, &OrderDeniedReason::MissingExpireTime.to_string());
                return false; // Denied
            };

            if expire_time <= self.clock.borrow().timestamp_ns() {
                self.deny_order(
                    order,
                    &OrderDeniedReason::ExpireTimeInPast {
                        expire_time: expire_time.to_rfc3339(),
                    }
                    .to_string(),
                );
                return false; // Denied
            }
        }

        true
    }

    fn first_non_reducing_order<'a>(&self, orders: &'a [OrderAny]) -> Option<&'a OrderAny> {
        if self.trading_state != TradingState::Reducing {
            return None;
        }

        let cache = self.cache.borrow();
        let proposed_ids: Vec<_> = orders.iter().map(Order::client_order_id).collect();
        let mut remaining_by_instrument_side: AHashMap<
            (InstrumentId, AccountId, OrderSide),
            QuantityRaw,
        > = AHashMap::new();

        for order in orders {
            // Quote-denominated quantities cannot be compared directly with cached base-position
            // quantities. REDUCING is a fail-closed state, so an order without a directly
            // comparable quantity is not admitted here.
            if order.is_quote_quantity() {
                return Some(order);
            }

            let side = order.order_side();
            let position_side = match side {
                OrderSide::Buy => PositionSide::Short,
                OrderSide::Sell => PositionSide::Long,
                OrderSide::NoOrderSide => return Some(order),
            };
            let cash_inventory = Self::reducing_cash_inventory(&cache, order, &proposed_ids);
            let Some(account_id) = cash_inventory
                .map(|(id, _)| id)
                .or_else(|| Self::reducing_account_id(&cache, order, position_side))
            else {
                return Some(order);
            };
            let key = (order.instrument_id(), account_id, side);
            let remaining = remaining_by_instrument_side.entry(key).or_insert_with(|| {
                let position_quantity =
                    cash_inventory.map(|(_, quantity)| quantity).or_else(|| {
                        cache
                            .positions_open(
                                None,
                                Some(&order.instrument_id()),
                                None,
                                Some(&account_id),
                                Some(position_side),
                            )
                            .iter()
                            .try_fold(0 as QuantityRaw, |total, position| {
                                total.checked_add(position.quantity.raw)
                            })
                    });

                let pending_quantity = cache
                    .orders_open(None, Some(&order.instrument_id()), None, None, Some(side))
                    .iter()
                    .filter(|pending| {
                        !orders.iter().any(|candidate| {
                            candidate.client_order_id() == pending.client_order_id()
                        })
                    })
                    .try_fold(0 as QuantityRaw, |total, pending| {
                        match pending.account_id() {
                            Some(pending_account_id) if pending_account_id != account_id => {
                                Some(total)
                            }
                            Some(_) if !pending.is_quote_quantity() => {
                                total.checked_add(pending.leaves_qty().raw)
                            }
                            Some(_) | None => None,
                        }
                    });

                match (position_quantity, pending_quantity) {
                    (Some(position), Some(pending)) => position.saturating_sub(pending),
                    _ => 0,
                }
            });

            if order.quantity().raw > *remaining {
                return Some(order);
            }
            *remaining -= order.quantity().raw;
        }

        None
    }

    fn reducing_account_id(
        cache: &Cache,
        order: &OrderAny,
        position_side: PositionSide,
    ) -> Option<AccountId> {
        if let Some(account_id) = order.account_id() {
            return Some(account_id);
        }

        let mut account_id = None;
        for position in cache.positions_open(
            None,
            Some(&order.instrument_id()),
            None,
            None,
            Some(position_side),
        ) {
            if account_id.is_some_and(|existing| existing != position.account_id) {
                return None;
            }
            account_id = Some(position.account_id);
        }
        account_id
    }

    /// Cash inventory can predate this node and need not have a derivative-style
    /// Position object. Its balance still permits a strictly inventory-reducing
    /// sell. Unknown locks remain unavailable; pending native sells are counted
    /// once by the caller's batch reservation logic.
    fn reducing_cash_inventory(
        cache: &Cache,
        order: &OrderAny,
        proposed_ids: &[ClientOrderId],
    ) -> Option<(AccountId, QuantityRaw)> {
        if order.order_side() != OrderSide::Sell {
            return None;
        }
        let instrument_id = order.instrument_id();
        let InstrumentAny::CurrencyPair(instrument) = cache.instrument(&instrument_id)? else {
            return None;
        };
        let account = match order.account_id() {
            Some(id) => cache.account(&id)?,
            None => cache.account_for_venue(&instrument_id.venue)?,
        };
        let account_id = account.id();
        let AccountAny::Cash(cash) = &*account else {
            return None;
        };
        if cash.allow_borrowing {
            return None;
        }
        let free = cash
            .balance_free(Some(instrument.base_currency))?
            .as_decimal();
        let total = cash
            .balance_total(Some(instrument.base_currency))?
            .as_decimal();
        if free.is_sign_negative() || total.is_sign_negative() {
            return None;
        }
        let pending = cache
            .orders_open(
                None,
                Some(&instrument_id),
                None,
                None,
                Some(OrderSide::Sell),
            )
            .iter()
            .filter(|pending| !proposed_ids.contains(&pending.client_order_id()))
            .try_fold(Decimal::ZERO, |sum, pending| match pending.account_id() {
                Some(id) if id != account_id => Some(sum),
                Some(_) if !pending.is_quote_quantity() => {
                    sum.checked_add(pending.leaves_qty().as_decimal())
                }
                _ => None,
            })?;
        let capacity = total
            .min(free.checked_add(pending)?)
            .round_dp_with_strategy(instrument.size_precision.into(), RoundingStrategy::ToZero);
        let quantity = Quantity::from_str(&capacity.to_string()).ok()?;
        Some((account_id, quantity.raw))
    }

    fn first_non_reducing_modify<'a>(
        &self,
        modifies: &'a [ModifyOrder],
    ) -> Option<&'a ModifyOrder> {
        if self.trading_state != TradingState::Reducing {
            return None;
        }

        let cache = self.cache.borrow();
        let mut remaining_by_instrument_account_side: AHashMap<
            (InstrumentId, AccountId, OrderSide),
            QuantityRaw,
        > = AHashMap::new();
        let modified_ids: Vec<_> = modifies
            .iter()
            .map(|candidate| candidate.client_order_id)
            .collect();

        for modify in modifies {
            let Some(order) = cache.order(&modify.client_order_id) else {
                return Some(modify);
            };
            if order.is_quote_quantity() {
                return Some(modify);
            }

            let side = order.order_side();
            let position_side = match side {
                OrderSide::Buy => PositionSide::Short,
                OrderSide::Sell => PositionSide::Long,
                OrderSide::NoOrderSide => return Some(modify),
            };
            let cash_inventory = Self::reducing_cash_inventory(&cache, &order, &[]);
            let Some(account_id) = cash_inventory
                .map(|(id, _)| id)
                .or_else(|| Self::reducing_account_id(&cache, &order, position_side))
            else {
                return Some(modify);
            };
            let key = (order.instrument_id(), account_id, side);
            let remaining = remaining_by_instrument_account_side
                .entry(key)
                .or_insert_with(|| {
                    let position_quantity =
                        cash_inventory.map(|(_, quantity)| quantity).or_else(|| {
                            cache
                                .positions_open(
                                    None,
                                    Some(&order.instrument_id()),
                                    None,
                                    Some(&account_id),
                                    Some(position_side),
                                )
                                .iter()
                                .try_fold(0 as QuantityRaw, |total, position| {
                                    total.checked_add(position.quantity.raw)
                                })
                        });

                    let other_pending_quantity = cache
                        .orders_open(None, Some(&order.instrument_id()), None, None, Some(side))
                        .iter()
                        .filter(|pending| !modified_ids.contains(&pending.client_order_id()))
                        .try_fold(0 as QuantityRaw, |total, pending| {
                            match pending.account_id() {
                                Some(pending_account_id) if pending_account_id != account_id => {
                                    Some(total)
                                }
                                Some(_) if !pending.is_quote_quantity() => {
                                    total.checked_add(pending.leaves_qty().raw)
                                }
                                Some(_) | None => None,
                            }
                        });

                    match (position_quantity, other_pending_quantity) {
                        (Some(position), Some(pending)) => position.saturating_sub(pending),
                        _ => 0,
                    }
                });

            let requested_total = modify.quantity.unwrap_or_else(|| order.quantity());
            let Some(requested_leaves_raw) =
                requested_total.raw.checked_sub(order.filled_qty().raw)
            else {
                return Some(modify);
            };
            if requested_leaves_raw > *remaining {
                return Some(modify);
            }
            *remaining -= requested_leaves_raw;
        }

        None
    }

    fn check_order_price(&self, instrument: &InstrumentAny, order: &OrderAny) -> bool {
        if order.price().is_some() {
            let risk_msg = Self::check_price(instrument, order.price());
            if let Some(risk_msg) = risk_msg {
                self.deny_order(order, &risk_msg);
                return false; // Denied
            }
        }

        if order.trigger_price().is_some() {
            let risk_msg = Self::check_price(instrument, order.trigger_price());
            if let Some(risk_msg) = risk_msg {
                self.deny_order(order, &format!("trigger {risk_msg}"));
                return false; // Denied
            }
        }

        true
    }

    fn check_order_quantity(&self, instrument: &InstrumentAny, order: &OrderAny) -> bool {
        let risk_msg = Self::check_quantity(
            instrument,
            Some(order.quantity()),
            order.is_quote_quantity(),
        );

        if let Some(risk_msg) = risk_msg {
            self.deny_order(order, &risk_msg);
            return false; // Denied
        }

        true
    }

    fn market_valuation_config(&self) -> Result<(u64, u32), String> {
        match (
            self.config.max_market_data_age_ns,
            self.config.market_order_price_bound_bps,
        ) {
            (Some(max_age_ns), Some(bound_bps)) => Ok((max_age_ns, bound_bps)),
            _ => Err(
                "market-style valuation is disabled: max_market_data_age_ns and market_order_price_bound_bps must both be explicitly configured; local valuation is not a venue-enforced price collar"
                    .to_string(),
            ),
        }
    }

    fn ensure_market_data_fresh(
        &self,
        kind: &str,
        instrument_id: InstrumentId,
        ts_init: UnixNanos,
    ) -> Result<(), String> {
        let (max_age_ns, _) = self.market_valuation_config()?;
        let now = self.clock.borrow().timestamp_ns();
        let Some(age_ns) = now.duration_since(&ts_init) else {
            return Err(format!(
                "cannot value {instrument_id}: {kind} local-receipt timestamp {ts_init} is in the future relative to risk clock {now}"
            ));
        };
        if age_ns > max_age_ns {
            return Err(format!(
                "cannot value {instrument_id}: stale {kind} evidence age {age_ns}ns exceeds configured maximum {max_age_ns}ns"
            ));
        }
        Ok(())
    }

    fn fresh_quote(&self, instrument_id: InstrumentId) -> Result<QuoteTick, String> {
        let quote = *self
            .cache
            .borrow()
            .quote(&instrument_id)
            .ok_or_else(|| format!("cannot value {instrument_id}: no quote evidence"))?;
        self.ensure_market_data_fresh("quote", instrument_id, quote.ts_init)?;
        Ok(quote)
    }

    fn fresh_trade(&self, instrument_id: InstrumentId) -> Result<TradeTick, String> {
        let trade = *self
            .cache
            .borrow()
            .trade(&instrument_id)
            .ok_or_else(|| format!("cannot value {instrument_id}: no trade evidence"))?;
        self.ensure_market_data_fresh("trade", instrument_id, trade.ts_init)?;
        Ok(trade)
    }

    fn market_reference_price(
        &self,
        instrument_id: InstrumentId,
        side: OrderSide,
    ) -> Result<Price, String> {
        self.market_valuation_config()?;

        let quote_error = match self
            .fresh_quote(instrument_id)
            .and_then(|quote| match side {
                OrderSide::Buy => Ok(quote.ask_price),
                OrderSide::Sell => Ok(quote.bid_price),
                OrderSide::NoOrderSide => {
                    Err(OrderDeniedReason::InvalidOrderSide { order_side: side }.to_string())
                }
            }) {
            Ok(price) => return Ok(price),
            Err(error) => error,
        };

        let trade_result = self.fresh_trade(instrument_id).map(|trade| trade.price);
        match trade_result {
            Ok(price) if side != OrderSide::NoOrderSide => Ok(price),
            Ok(_) => Err(OrderDeniedReason::InvalidOrderSide { order_side: side }.to_string()),
            Err(trade_error) => Err(format!(
                "cannot value market-style order for {instrument_id}; quote evidence failed ({quote_error}) and trade evidence failed ({trade_error})"
            )),
        }
    }

    fn trailing_reference_price(
        &self,
        instrument: &InstrumentAny,
        order: &OrderAny,
    ) -> Result<Price, String> {
        let offset_type = order
            .trailing_offset_type()
            .ok_or_else(|| OrderDeniedReason::MissingTrailingOffsetType.to_string())?;
        if !matches!(
            offset_type,
            TrailingOffsetType::Price | TrailingOffsetType::BasisPoints | TrailingOffsetType::Ticks
        ) {
            return Err(
                OrderDeniedReason::UnsupportedTrailingOffsetType { offset_type }.to_string(),
            );
        }
        let trigger_type = order
            .trigger_type()
            .ok_or_else(|| OrderDeniedReason::MissingTriggerType.to_string())?;
        let trailing_offset = order
            .trailing_offset()
            .ok_or_else(|| OrderDeniedReason::MissingTrailingOffset.to_string())?;

        let calculate_with_quote = |quote: QuoteTick| {
            trailing_stop_calculate_with_bid_ask(
                instrument.price_increment(),
                offset_type,
                order.order_side_specified(),
                trailing_offset,
                quote.bid_price,
                quote.ask_price,
            )
            .map_err(|error| {
                OrderDeniedReason::TrailingStopCalcFailed {
                    detail: error.to_string(),
                }
                .to_string()
            })
        };
        let calculate_with_trade = |trade: TradeTick| {
            trailing_stop_calculate_with_last(
                instrument.price_increment(),
                offset_type,
                order.order_side_specified(),
                trailing_offset,
                trade.price,
            )
            .map_err(|error| {
                OrderDeniedReason::TrailingStopCalcFailed {
                    detail: error.to_string(),
                }
                .to_string()
            })
        };

        match trigger_type {
            TriggerType::BidAsk | TriggerType::DoubleBidAsk => {
                calculate_with_quote(self.fresh_quote(instrument.id())?)
            }
            TriggerType::LastPrice | TriggerType::DoubleLast => {
                calculate_with_trade(self.fresh_trade(instrument.id())?)
            }
            TriggerType::LastOrBidAsk => match self.fresh_trade(instrument.id()) {
                Ok(trade) => calculate_with_trade(trade),
                Err(trade_error) => match self.fresh_quote(instrument.id()) {
                    Ok(quote) => calculate_with_quote(quote),
                    Err(quote_error) => Err(format!(
                        "cannot calculate trailing trigger for {}: trade evidence failed ({trade_error}) and quote evidence failed ({quote_error})",
                        instrument.id()
                    )),
                },
            },
            TriggerType::NoTrigger
            | TriggerType::Default
            | TriggerType::MarkPrice
            | TriggerType::IndexPrice
            | TriggerType::MidPoint => Err(format!(
                "cannot safely calculate trailing trigger type {trigger_type}: matching fresh market-data evidence is unavailable"
            )),
        }
    }

    #[allow(
        clippy::useless_conversion,
        reason = "PriceRaw is i64 normally and i128 when the workspace unifies high-precision"
    )]
    fn stressed_price_bounds(
        &self,
        instrument: &InstrumentAny,
        reference: Price,
        side: OrderSide,
    ) -> Result<RiskPriceBounds, String> {
        if let Some(reason) = Self::check_price(instrument, Some(reference)) {
            return Err(format!("invalid valuation reference: {reason}"));
        }
        let (_, bound_bps) = self.market_valuation_config()?;
        let raw = i128::from(reference.raw);
        let magnitude = raw.checked_abs().ok_or_else(|| {
            "cannot stress valuation price: reference magnitude overflow".to_string()
        })?;
        let scaled = magnitude
            .checked_mul(i128::from(bound_bps))
            .ok_or_else(|| {
                "cannot stress valuation price: basis-point multiplication overflow".to_string()
            })?;
        let percentage_delta = scaled.checked_add(9_999).ok_or_else(|| {
            "cannot stress valuation price: basis-point rounding overflow".to_string()
        })? / 10_000;
        let tick_delta = i128::from(instrument.price_increment().raw)
            .checked_abs()
            .ok_or_else(|| "cannot stress valuation price: price increment overflow".to_string())?;
        let delta = percentage_delta.max(tick_delta).max(1);
        let lower_raw = raw
            .checked_sub(delta)
            .ok_or_else(|| "cannot stress valuation price: lower bound overflow".to_string())?;
        let upper_raw = raw
            .checked_add(delta)
            .ok_or_else(|| "cannot stress valuation price: upper bound overflow".to_string())?;
        let lower = Price::from_raw_checked(
            PriceRaw::try_from(lower_raw)
                .map_err(|error| format!("cannot represent stressed lower price: {error}"))?,
            reference.precision,
        )
        .map_err(|error| format!("cannot represent stressed lower price: {error}"))?;
        let upper = Price::from_raw_checked(
            PriceRaw::try_from(upper_raw)
                .map_err(|error| format!("cannot represent stressed upper price: {error}"))?,
            reference.precision,
        )
        .map_err(|error| format!("cannot represent stressed upper price: {error}"))?;
        for bound in [lower, upper] {
            if let Some(reason) = Self::check_price(instrument, Some(bound)) {
                return Err(format!("invalid stressed valuation bound: {reason}"));
            }
        }
        let adverse = match side {
            OrderSide::Buy => upper,
            OrderSide::Sell => lower,
            OrderSide::NoOrderSide => {
                return Err(OrderDeniedReason::InvalidOrderSide { order_side: side }.to_string());
            }
        };
        Ok(RiskPriceBounds {
            adverse,
            lower,
            upper,
        })
    }

    fn price_bounds_for_order(
        &self,
        instrument: &InstrumentAny,
        order: &OrderAny,
    ) -> Result<RiskPriceBounds, String> {
        match order {
            OrderAny::Limit(_) | OrderAny::LimitIfTouched(_) | OrderAny::StopLimit(_) => order
                .price()
                .map(RiskPriceBounds::fixed)
                .ok_or_else(|| format!("{} order has no explicit limit price", order.order_type())),
            OrderAny::TrailingStopLimit(_) if order.price().is_some() => {
                Ok(RiskPriceBounds::fixed(order.price().expect("checked Some")))
            }
            OrderAny::Market(_) | OrderAny::MarketToLimit(_) => {
                let reference = self.market_reference_price(instrument.id(), order.order_side())?;
                self.stressed_price_bounds(instrument, reference, order.order_side())
            }
            OrderAny::StopMarket(_) | OrderAny::MarketIfTouched(_) => {
                self.market_valuation_config()?;
                let trigger_reference = order
                    .trigger_price()
                    .ok_or_else(|| format!("{} order has no trigger price", order.order_type()))?;
                let current_reference =
                    self.market_reference_price(instrument.id(), order.order_side())?;
                let trigger_bounds =
                    self.stressed_price_bounds(instrument, trigger_reference, order.order_side())?;
                let current_bounds =
                    self.stressed_price_bounds(instrument, current_reference, order.order_side())?;
                Self::merge_price_bounds(trigger_bounds, current_bounds, order.order_side())
            }
            OrderAny::TrailingStopMarket(_) => {
                self.market_valuation_config()?;
                let trigger_reference = match order.trigger_price() {
                    Some(trigger_price) => trigger_price,
                    None => self.trailing_reference_price(instrument, order)?,
                };
                let current_reference =
                    self.market_reference_price(instrument.id(), order.order_side())?;
                let trigger_bounds =
                    self.stressed_price_bounds(instrument, trigger_reference, order.order_side())?;
                let current_bounds =
                    self.stressed_price_bounds(instrument, current_reference, order.order_side())?;
                Self::merge_price_bounds(trigger_bounds, current_bounds, order.order_side())
            }
            OrderAny::TrailingStopLimit(_) => {
                Err("trailing stop limit has no explicit execution limit price".to_string())
            }
        }
    }

    fn merge_price_bounds(
        first: RiskPriceBounds,
        second: RiskPriceBounds,
        side: OrderSide,
    ) -> Result<RiskPriceBounds, String> {
        let lower = first.lower.min(second.lower);
        let upper = first.upper.max(second.upper);
        let adverse = match side {
            OrderSide::Buy => upper,
            OrderSide::Sell => lower,
            OrderSide::NoOrderSide => {
                return Err(OrderDeniedReason::InvalidOrderSide { order_side: side }.to_string());
            }
        };
        Ok(RiskPriceBounds {
            adverse,
            lower,
            upper,
        })
    }

    fn notional_bounds(
        instrument: &InstrumentAny,
        lower_quantity: Quantity,
        upper_quantity: Quantity,
        bounds: RiskPriceBounds,
        use_quote_for_inverse: Option<bool>,
    ) -> Result<(Money, Money), String> {
        let lower = instrument
            .try_calculate_notional_value(lower_quantity, bounds.lower, use_quote_for_inverse)
            .map_err(|error| format!("cannot calculate lower-bound notional value: {error}"))?;
        let upper = instrument
            .try_calculate_notional_value(upper_quantity, bounds.upper, use_quote_for_inverse)
            .map_err(|error| format!("cannot calculate upper-bound notional value: {error}"))?;
        if lower.currency != upper.currency {
            return Err(format!(
                "bounded notional currency mismatch: {} != {}",
                lower.currency, upper.currency
            ));
        }
        Ok(if lower <= upper {
            (lower, upper)
        } else {
            (upper, lower)
        })
    }

    fn checked_quantity_sum(
        current: QuantityRaw,
        additional: QuantityRaw,
        context: &str,
    ) -> Result<QuantityRaw, String> {
        current
            .checked_add(additional)
            .ok_or_else(|| format!("cannot calculate {context}: quantity overflow"))
    }

    fn checked_negate_money(value: Money, context: &str) -> Result<Money, String> {
        let raw = value
            .raw
            .checked_neg()
            .ok_or_else(|| format!("cannot calculate {context}: negation overflow"))?;
        Money::from_raw_checked(raw, value.currency)
            .map_err(|error| format!("cannot calculate {context}: {error}"))
    }

    fn check_orders_risk(&self, orders: &[OrderAny]) -> bool {
        let mut orders_by_instrument_account: AHashMap<
            (InstrumentId, Option<AccountId>),
            Vec<&OrderAny>,
        > = AHashMap::new();
        for order in orders {
            orders_by_instrument_account
                .entry((order.instrument_id(), order.account_id()))
                .or_default()
                .push(order);
        }

        for ((instrument_id, account_id), account_orders) in &orders_by_instrument_account {
            let instrument = self.cache.borrow().instrument(instrument_id).cloned();
            let Some(instrument) = instrument else {
                for order in account_orders {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::InstrumentNotFound {
                            instrument_id: *instrument_id,
                        }
                        .to_string(),
                    );
                }
                return false;
            };
            if !self.check_orders_risk_for_account(&instrument, account_orders, *account_id) {
                return false;
            }
        }

        true
    }

    #[allow(
        clippy::too_many_lines,
        reason = "risk checks keep related denial branches together for auditability"
    )]
    fn check_orders_risk_for_account(
        &self,
        instrument: &InstrumentAny,
        orders: &[&OrderAny],
        account_id: Option<AccountId>,
    ) -> bool {
        let mut max_notional: Option<Money> = None;

        // Determine max notional
        let max_notional_setting = self.max_notional_per_order.get(&instrument.id());
        if let Some(max_notional_setting_val) = max_notional_setting.copied() {
            let Ok(max_notional_value) =
                Money::from_decimal(max_notional_setting_val, instrument.quote_currency())
            else {
                for order in orders {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::InvalidMaxNotionalPerOrder {
                            instrument_id: instrument.id(),
                            value: max_notional_setting_val,
                        }
                        .to_string(),
                    );
                }
                return false; // Denied
            };
            max_notional = Some(max_notional_value);
        }

        // Get account for risk checks: use explicit account_id if provided, otherwise venue lookup
        let resolved_account = {
            let cache = self.cache.borrow();

            if let Some(account_id) = account_id {
                cache.account_owned(&account_id)
            } else {
                cache.account_for_venue_owned(&instrument.id().venue)
            }
        };

        let Some(mut account) = resolved_account else {
            let reason = format!(
                "cannot check order risk: account not found for venue {} (account_id={account_id:?})",
                instrument.id().venue
            );
            for order in orders {
                self.deny_order(order, &reason);
            }
            return false;
        };
        let risk_account_id = account.id();

        let is_margin = matches!(account, AccountAny::Margin(_));
        let is_betting = matches!(account, AccountAny::Betting(_));
        let allow_borrowing = match &account {
            AccountAny::Cash(cash) => cash.allow_borrowing,
            AccountAny::Margin(_) | AccountAny::Betting(_) => false,
        };

        // Get net LONG position quantity for this instrument (for position-reducing sell checks),
        // accounting for already submitted (but unfilled) SELL orders to prevent overselling.
        let (net_long_qty_raw, pending_sell_qty_raw) = {
            let cache = self.cache.borrow();
            let long_qty: QuantityRaw = cache
                .positions_open(
                    None,
                    Some(&instrument.id()),
                    None,
                    Some(&risk_account_id),
                    Some(PositionSide::Long),
                )
                .iter()
                .map(|pos| pos.quantity.raw)
                .sum();
            let pending_sells: QuantityRaw = cache
                .orders_open(
                    None,
                    Some(&instrument.id()),
                    None,
                    None,
                    Some(OrderSide::Sell),
                )
                .iter()
                .filter(|pending| {
                    !orders
                        .iter()
                        .any(|candidate| candidate.client_order_id() == pending.client_order_id())
                })
                .try_fold(0 as QuantityRaw, |total, order| match order.account_id() {
                    Some(account_id) if account_id != risk_account_id => Some(total),
                    Some(_) => total.checked_add(order.leaves_qty().raw),
                    None => None,
                })
                .unwrap_or(QuantityRaw::MAX);
            (long_qty, pending_sells)
        };

        // Available quantity is long position minus pending sells
        let available_long_qty_raw = net_long_qty_raw.saturating_sub(pending_sell_qty_raw);

        if self.config.debug && net_long_qty_raw > 0 {
            log::debug!(
                "Net LONG qty (raw): {net_long_qty_raw}, pending sells: {pending_sell_qty_raw}, available: {available_long_qty_raw}"
            );
        }

        // For margin and betting accounts, also track SHORT positions for buy-side reduction
        let available_short_qty_raw = if is_margin || is_betting {
            let cache = self.cache.borrow();
            let short_qty: QuantityRaw = cache
                .positions_open(
                    None,
                    Some(&instrument.id()),
                    None,
                    Some(&risk_account_id),
                    Some(PositionSide::Short),
                )
                .iter()
                .map(|pos| pos.quantity.raw)
                .sum();
            let pending_buys: QuantityRaw = cache
                .orders_open(
                    None,
                    Some(&instrument.id()),
                    None,
                    None,
                    Some(OrderSide::Buy),
                )
                .iter()
                .filter(|pending| {
                    !orders
                        .iter()
                        .any(|candidate| candidate.client_order_id() == pending.client_order_id())
                })
                .try_fold(0 as QuantityRaw, |total, order| match order.account_id() {
                    Some(account_id) if account_id != risk_account_id => Some(total),
                    Some(_) => total.checked_add(order.leaves_qty().raw),
                    None => None,
                })
                .unwrap_or(QuantityRaw::MAX);

            if self.config.debug && short_qty > 0 {
                log::debug!(
                    "Net SHORT qty (raw): {short_qty}, pending buys: {pending_buys}, available: {}",
                    short_qty.saturating_sub(pending_buys)
                );
            }

            short_qty.saturating_sub(pending_buys)
        } else {
            0
        };

        // Track cumulative quantities to determine position-reducing vs position-opening orders
        let mut cum_sell_qty_raw: QuantityRaw = 0;
        let mut cum_buy_qty_raw: QuantityRaw = 0;

        let mut cum_notional_buy: Option<Money> = None;
        let mut cum_notional_sell: Option<Money> = None;
        let mut cum_margin_required: Option<Money> = None;
        let mut base_currency: Option<Currency> = None;

        for order in orders {
            // Derive independent bounds per order. Reusing a cached BUY ask for a later SELL (or
            // vice versa) would understate one side of a mixed order list.
            let price_bounds = match self.price_bounds_for_order(instrument, order) {
                Ok(bounds) => bounds,
                Err(reason) => {
                    self.deny_order(order, &format!("cannot prove order price safety: {reason}"));
                    return false;
                }
            };
            if self.config.debug {
                log::debug!("Adverse side price estimate: {}", price_bounds.adverse);
            }

            let (lower_quantity, upper_quantity) =
                if order.is_quote_quantity() && !instrument.is_inverse() {
                    let lower = match instrument
                        .try_calculate_base_quantity(order.quantity(), price_bounds.lower)
                    {
                        Ok(quantity) => quantity,
                        Err(error) => {
                            self.deny_order(
                                order,
                                &format!("cannot calculate lower-bound quote quantity: {error}"),
                            );
                            return false;
                        }
                    };
                    let upper = match instrument
                        .try_calculate_base_quantity(order.quantity(), price_bounds.upper)
                    {
                        Ok(quantity) => quantity,
                        Err(error) => {
                            self.deny_order(
                                order,
                                &format!("cannot calculate upper-bound quote quantity: {error}"),
                            );
                            return false;
                        }
                    };
                    (lower, upper)
                } else {
                    (order.quantity(), order.quantity())
                };
            let effective_quantity = if lower_quantity >= upper_quantity {
                lower_quantity
            } else {
                upper_quantity
            };

            // Base-quantity bounds (`min_quantity`/`max_quantity`) do not apply to
            // quote-denominated orders: the client-side conversion uses an estimated
            // price and may differ from the venue fill, and some venues enforce
            // distinct per-order-type minimums. The venue is authoritative for
            // quote-denominated sizing; rely on `min_notional`/`max_notional` below.
            if !order.is_quote_quantity() {
                if let Some(max_quantity) = instrument.max_quantity()
                    && effective_quantity > max_quantity
                {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::QuantityExceedsMaximum {
                            effective_quantity,
                            max_quantity,
                        }
                        .to_string(),
                    );
                    return false; // Denied
                }

                if let Some(min_quantity) = instrument.min_quantity()
                    && effective_quantity < min_quantity
                {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::QuantityBelowMinimum {
                            effective_quantity,
                            min_quantity,
                        }
                        .to_string(),
                    );
                    return false; // Denied
                }
            }

            let (min_risk_notional, notional) = match Self::notional_bounds(
                instrument,
                lower_quantity,
                upper_quantity,
                price_bounds,
                Some(true),
            ) {
                Ok(bounds) => bounds,
                Err(error) => {
                    self.deny_order(order, &format!("Cannot calculate notional bounds: {error}"));
                    return false;
                }
            };

            if self.config.debug {
                log::debug!(
                    "Conservative notional bounds: min={min_risk_notional:?}, max={notional:?}"
                );
            }

            // Check MAX notional per order limit
            if let Some(max_notional_value) = max_notional {
                if notional.currency != max_notional_value.currency {
                    self.deny_order(
                        order,
                        &format!(
                            "cannot compare configured max-notional currencies: {} != {}",
                            notional.currency, max_notional_value.currency
                        ),
                    );
                    return false;
                }
                if notional > max_notional_value {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::NotionalExceedsMaxPerOrder {
                            max_notional: max_notional_value,
                            notional,
                        }
                        .to_string(),
                    );
                    return false; // Denied
                }
            }

            // Check MIN notional instrument limit
            if let Some(min_notional) = instrument.min_notional() {
                if min_risk_notional.currency != min_notional.currency {
                    self.deny_order(
                        order,
                        &format!(
                            "cannot compare minimum notional currencies: {} != {}",
                            min_risk_notional.currency, min_notional.currency
                        ),
                    );
                    return false;
                }
                if min_risk_notional < min_notional {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::NotionalBelowMinimum {
                            min_notional,
                            notional: min_risk_notional,
                        }
                        .to_string(),
                    );
                    return false; // Denied
                }
            }

            // Check MAX notional instrument limit
            if let Some(max_notional) = instrument.max_notional() {
                if notional.currency != max_notional.currency {
                    self.deny_order(
                        order,
                        &format!(
                            "cannot compare maximum notional currencies: {} != {}",
                            notional.currency, max_notional.currency
                        ),
                    );
                    return false;
                }
                if notional > max_notional {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::NotionalExceedsMaximum {
                            max_notional,
                            notional,
                        }
                        .to_string(),
                    );
                    return false; // Denied
                }
            }

            if is_margin {
                // Calculate both ends because inverse/non-linear products can have their maximum
                // requirement on either side of the stressed price interval.
                let margin_req = match &mut account {
                    AccountAny::Margin(margin) => {
                        let lower = margin.calculate_initial_margin(
                            instrument,
                            lower_quantity,
                            price_bounds.lower,
                            None,
                        );
                        let upper = margin.calculate_initial_margin(
                            instrument,
                            upper_quantity,
                            price_bounds.upper,
                            None,
                        );
                        match (lower, upper) {
                            (Ok(lower), Ok(upper)) if lower.currency == upper.currency => {
                                if lower >= upper { lower } else { upper }
                            }
                            (Ok(lower), Ok(upper)) => {
                                self.deny_order(
                                    order,
                                    &format!(
                                        "Cannot compare bounded margin currencies: {} != {}",
                                        lower.currency, upper.currency
                                    ),
                                );
                                return false;
                            }
                            (Err(error), _) | (_, Err(error)) => {
                                self.deny_order(
                                    order,
                                    &format!("Cannot calculate bounded initial margin: {error}"),
                                );
                                return false;
                            }
                        }
                    }
                    _ => unreachable!(),
                };

                if self.config.debug {
                    log::debug!("Initial margin required: {margin_req}");
                }

                // Determine if order is position-reducing
                let next_sell_qty_raw = if order.is_sell() {
                    match Self::checked_quantity_sum(
                        cum_sell_qty_raw,
                        effective_quantity.raw,
                        "cumulative sell quantity",
                    ) {
                        Ok(value) => Some(value),
                        Err(reason) => {
                            self.deny_order(order, &reason);
                            return false;
                        }
                    }
                } else {
                    None
                };
                let next_buy_qty_raw = if order.is_buy() {
                    match Self::checked_quantity_sum(
                        cum_buy_qty_raw,
                        effective_quantity.raw,
                        "cumulative buy quantity",
                    ) {
                        Ok(value) => Some(value),
                        Err(reason) => {
                            self.deny_order(order, &reason);
                            return false;
                        }
                    }
                } else {
                    None
                };
                let is_reducing = next_sell_qty_raw
                    .is_some_and(|quantity| quantity <= available_long_qty_raw)
                    || next_buy_qty_raw.is_some_and(|quantity| quantity <= available_short_qty_raw);

                if let Some(quantity) = next_sell_qty_raw {
                    cum_sell_qty_raw = quantity;
                } else if let Some(quantity) = next_buy_qty_raw {
                    cum_buy_qty_raw = quantity;
                }

                if is_reducing {
                    if self.config.debug {
                        log::debug!("Position-reducing order skips margin check");
                    }
                    continue;
                }

                // Missing collateral evidence is unsafe for any position-opening order. A
                // reducing classification above is derived from authoritative cached positions
                // and quantities, so it does not require funding evidence.
                let margin_free = match &account {
                    AccountAny::Margin(margin) => margin.balance_free(Some(margin_req.currency)),
                    _ => unreachable!(),
                };
                let Some(margin_free_val) = margin_free else {
                    self.deny_order(
                        order,
                        &format!(
                            "cannot check order risk: no free margin collateral balance for {}",
                            margin_req.currency
                        ),
                    );
                    return false;
                };

                // Per-order margin check
                if margin_req > margin_free_val {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::MarginExceedsFreeBalance {
                            free: margin_free_val,
                            margin_required: margin_req,
                        }
                        .to_string(),
                    );
                    return false;
                }

                // Cumulative margin check
                match cum_margin_required.as_mut() {
                    Some(cum) => {
                        let Some(total) = cum.checked_add(margin_req) else {
                            self.deny_order(
                                order,
                                "Cannot calculate cumulative margin: total exceeds Money bounds",
                            );
                            return false;
                        };
                        *cum = total;
                    }
                    None => cum_margin_required = Some(margin_req),
                }

                if self.config.debug {
                    log::debug!("Cumulative margin required: {cum_margin_required:?}");
                }

                if let Some(cum_margin) = cum_margin_required
                    && cum_margin > margin_free_val
                {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::CumMarginExceedsFreeBalance {
                            free: margin_free_val,
                            cum_margin,
                        }
                        .to_string(),
                    );
                    return false;
                }
            } else {
                // Cash account: check the maximum full notional across the stressed interval.
                let (_, notional) = match Self::notional_bounds(
                    instrument,
                    lower_quantity,
                    upper_quantity,
                    price_bounds,
                    None,
                ) {
                    Ok(bounds) => bounds,
                    Err(error) => {
                        self.deny_order(
                            order,
                            &format!("Cannot calculate cash notional bounds: {error}"),
                        );
                        return false;
                    }
                };
                let order_balance_impact = if is_betting {
                    match &mut account {
                        AccountAny::Betting(betting) => {
                            let lower = betting.calculate_balance_locked(
                                instrument,
                                order.order_side(),
                                lower_quantity,
                                price_bounds.lower,
                                None,
                            );
                            let upper = betting.calculate_balance_locked(
                                instrument,
                                order.order_side(),
                                upper_quantity,
                                price_bounds.upper,
                                None,
                            );
                            match (lower, upper) {
                                (Ok(lower), Ok(upper)) if lower.currency == upper.currency => {
                                    let locked = if lower >= upper { lower } else { upper };
                                    match Self::checked_negate_money(
                                        locked,
                                        "betting balance impact",
                                    ) {
                                        Ok(impact) => impact,
                                        Err(reason) => {
                                            self.deny_order(order, &reason);
                                            return false;
                                        }
                                    }
                                }
                                (Ok(lower), Ok(upper)) => {
                                    self.deny_order(
                                        order,
                                        &format!(
                                            "Cannot compare bounded betting balance currencies: {} != {}",
                                            lower.currency, upper.currency
                                        ),
                                    );
                                    return false;
                                }
                                (Err(error), _) | (_, Err(error)) => {
                                    self.deny_order(
                                        order,
                                        &format!(
                                            "Cannot calculate bounded betting balance locked: {error}"
                                        ),
                                    );
                                    return false;
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    match order.order_side() {
                        OrderSide::Buy => {
                            match Self::checked_negate_money(notional, "cash buy balance impact") {
                                Ok(impact) => impact,
                                Err(reason) => {
                                    self.deny_order(order, &reason);
                                    return false;
                                }
                            }
                        }
                        OrderSide::Sell => Money::from_raw(notional.raw, notional.currency),
                        OrderSide::NoOrderSide => {
                            self.deny_order(
                                order,
                                &OrderDeniedReason::InvalidOrderSide {
                                    order_side: order.order_side(),
                                }
                                .to_string(),
                            );
                            return false; // Denied
                        }
                    }
                };

                if self.config.debug {
                    log::debug!("Balance impact: {order_balance_impact}");
                }

                // Check if order reduces an existing position
                let is_position_reducing = if order.is_buy() {
                    let next = match Self::checked_quantity_sum(
                        cum_buy_qty_raw,
                        effective_quantity.raw,
                        "cumulative buy quantity",
                    ) {
                        Ok(value) => value,
                        Err(reason) => {
                            self.deny_order(order, &reason);
                            return false;
                        }
                    };
                    let reducing = next <= available_short_qty_raw;
                    cum_buy_qty_raw = next;
                    reducing
                } else if order.is_sell() {
                    let next = match Self::checked_quantity_sum(
                        cum_sell_qty_raw,
                        effective_quantity.raw,
                        "cumulative sell quantity",
                    ) {
                        Ok(value) => value,
                        Err(reason) => {
                            self.deny_order(order, &reason);
                            return false;
                        }
                    };
                    let reducing = next <= available_long_qty_raw;
                    cum_sell_qty_raw = next;
                    reducing
                } else {
                    false
                };

                if is_position_reducing {
                    if self.config.debug {
                        log::debug!("Position-reducing order skips balance check");
                    }
                    continue;
                }

                // Resolve funding only after authoritative cached positions prove this order is
                // not reducing. The balance currency follows the calculated impact rather than
                // assuming every instrument is funded in its quote currency.
                let free = match &account {
                    AccountAny::Cash(cash) => {
                        cash.balance_free(Some(order_balance_impact.currency))
                    }
                    AccountAny::Betting(betting) => {
                        betting.balance_free(Some(order_balance_impact.currency))
                    }
                    AccountAny::Margin(_) => unreachable!(),
                };
                let Some(free_val) = free else {
                    self.deny_order(
                        order,
                        &format!(
                            "cannot check order risk: no free {} balance is available for account {}",
                            order_balance_impact.currency,
                            account.id()
                        ),
                    );
                    return false;
                };

                if self.config.debug {
                    log::debug!("Free balance: {free_val}");
                }

                // Deny when order exceeds free balance (unless borrowing is enabled)
                if !allow_borrowing
                    && (free_val.as_decimal() + order_balance_impact.as_decimal()) < Decimal::ZERO
                {
                    self.deny_order(
                        order,
                        &OrderDeniedReason::NotionalExceedsFreeBalance {
                            free: free_val,
                            notional,
                        }
                        .to_string(),
                    );
                    return false;
                }

                if base_currency.is_none() {
                    base_currency = instrument.base_currency();
                }

                if order.is_buy() {
                    match cum_notional_buy.as_mut() {
                        Some(cum_notional_buy_val) => {
                            let increment = match Self::checked_negate_money(
                                order_balance_impact,
                                "buy notional increment",
                            ) {
                                Ok(value) => value,
                                Err(reason) => {
                                    self.deny_order(order, &reason);
                                    return false;
                                }
                            };
                            let Some(total) = cum_notional_buy_val.checked_add(increment) else {
                                self.deny_order(
                                    order,
                                    "cannot calculate cumulative buy notional: Money overflow or currency mismatch",
                                );
                                return false;
                            };
                            *cum_notional_buy_val = total;
                        }
                        None => {
                            cum_notional_buy = match Self::checked_negate_money(
                                order_balance_impact,
                                "buy notional",
                            ) {
                                Ok(value) => Some(value),
                                Err(reason) => {
                                    self.deny_order(order, &reason);
                                    return false;
                                }
                            };
                        }
                    }

                    if self.config.debug {
                        log::debug!("Cumulative notional BUY: {cum_notional_buy:?}");
                    }

                    if !allow_borrowing
                        && let (Some(free), Some(cum_notional_buy)) = (free, cum_notional_buy)
                        && cum_notional_buy > free
                    {
                        self.deny_order(
                            order,
                            &OrderDeniedReason::CumNotionalExceedsFreeBalance {
                                free,
                                cum_notional: cum_notional_buy,
                            }
                            .to_string(),
                        );
                        return false; // Denied
                    }
                } else if order.is_sell() {
                    if is_betting {
                        match cum_notional_sell.as_mut() {
                            Some(cum_notional_sell_val) => {
                                let increment = match Self::checked_negate_money(
                                    order_balance_impact,
                                    "betting sell liability increment",
                                ) {
                                    Ok(value) => value,
                                    Err(reason) => {
                                        self.deny_order(order, &reason);
                                        return false;
                                    }
                                };
                                let Some(total) = cum_notional_sell_val.checked_add(increment)
                                else {
                                    self.deny_order(
                                        order,
                                        "cannot calculate cumulative betting sell liability: Money overflow or currency mismatch",
                                    );
                                    return false;
                                };
                                *cum_notional_sell_val = total;
                            }
                            None => {
                                cum_notional_sell = match Self::checked_negate_money(
                                    order_balance_impact,
                                    "betting sell liability",
                                ) {
                                    Ok(value) => Some(value),
                                    Err(reason) => {
                                        self.deny_order(order, &reason);
                                        return false;
                                    }
                                };
                            }
                        }

                        if self.config.debug {
                            log::debug!("Cumulative betting SELL liability: {cum_notional_sell:?}");
                        }

                        if !allow_borrowing
                            && let (Some(free), Some(cum_notional_sell)) = (free, cum_notional_sell)
                            && cum_notional_sell > free
                        {
                            self.deny_order(
                                order,
                                &OrderDeniedReason::CumNotionalExceedsFreeBalance {
                                    free,
                                    cum_notional: cum_notional_sell,
                                }
                                .to_string(),
                            );
                            return false;
                        }

                        continue;
                    }

                    let has_base_currency = match &account {
                        AccountAny::Margin(_) => false,
                        AccountAny::Cash(cash) => cash.base_currency.is_some(),
                        AccountAny::Betting(betting) => betting.base_currency.is_some(),
                    };

                    if has_base_currency {
                        match cum_notional_sell.as_mut() {
                            Some(cum_notional_sell_val) => {
                                let Some(total) =
                                    cum_notional_sell_val.checked_add(order_balance_impact)
                                else {
                                    self.deny_order(
                                        order,
                                        "cannot calculate cumulative sell notional: Money overflow or currency mismatch",
                                    );
                                    return false;
                                };
                                *cum_notional_sell_val = total;
                            }
                            None => {
                                cum_notional_sell = Some(order_balance_impact);
                            }
                        }

                        if self.config.debug {
                            log::debug!("Cumulative notional SELL: {cum_notional_sell:?}");
                        }

                        if !allow_borrowing
                            && let (Some(free), Some(cum_notional_sell)) = (free, cum_notional_sell)
                            && cum_notional_sell > free
                        {
                            self.deny_order(
                                order,
                                &OrderDeniedReason::CumNotionalExceedsFreeBalance {
                                    free,
                                    cum_notional: cum_notional_sell,
                                }
                                .to_string(),
                            );
                            return false; // Denied
                        }
                    } else {
                        let Some(base_currency) = base_currency else {
                            self.deny_order(
                                order,
                                "cannot check sell risk: instrument has no base currency",
                            );
                            return false;
                        };
                        let cash_value_raw: MoneyRaw = match effective_quantity.raw.try_into() {
                            Ok(value) => value,
                            Err(e) => {
                                self.deny_order(
                                    order,
                                    &OrderDeniedReason::QuantityConversionFailed {
                                        detail: e.to_string(),
                                    }
                                    .to_string(),
                                );
                                return false; // Denied
                            }
                        };
                        let cash_value = Money::from_raw(cash_value_raw, base_currency);

                        // Use base-currency free balance for sell checks
                        let base_free = match &account {
                            AccountAny::Margin(_) => None,
                            AccountAny::Cash(cash) => cash.balance_free(Some(base_currency)),
                            AccountAny::Betting(betting) => {
                                betting.balance_free(Some(base_currency))
                            }
                        };
                        let Some(base_free) = base_free else {
                            self.deny_order(
                                order,
                                &format!(
                                    "cannot check sell risk: no free base-currency balance for {base_currency}"
                                ),
                            );
                            return false;
                        };

                        if self.config.debug
                            && let AccountAny::Cash(cash) = &account
                        {
                            log::debug!("Cash value: {cash_value:?}");
                            log::debug!("Total: {:?}", cash.balance_total(Some(base_currency)));
                            log::debug!("Locked: {:?}", cash.balance_locked(Some(base_currency)));
                            log::debug!("Free: {base_free}");
                        }

                        if self.config.debug
                            && let AccountAny::Betting(betting) = &account
                        {
                            log::debug!("Cash value: {cash_value:?}");
                            log::debug!("Total: {:?}", betting.balance_total(Some(base_currency)));
                            log::debug!(
                                "Locked: {:?}",
                                betting.balance_locked(Some(base_currency))
                            );
                            log::debug!("Free: {base_free}");
                        }

                        match cum_notional_sell {
                            Some(value) => {
                                let Some(total) = value.checked_add(cash_value) else {
                                    self.deny_order(
                                        order,
                                        "cannot calculate cumulative base sell quantity: Money overflow or currency mismatch",
                                    );
                                    return false;
                                };
                                cum_notional_sell = Some(total);
                            }
                            None => cum_notional_sell = Some(cash_value),
                        }

                        if self.config.debug {
                            log::debug!("Cumulative notional SELL: {cum_notional_sell:?}");
                        }

                        if !allow_borrowing
                            && let Some(cum_notional_sell) = cum_notional_sell
                            && cum_notional_sell.raw > base_free.raw
                        {
                            self.deny_order(
                                order,
                                &OrderDeniedReason::CumNotionalExceedsFreeBalance {
                                    free: base_free,
                                    cum_notional: cum_notional_sell,
                                }
                                .to_string(),
                            );
                            return false; // Denied
                        }
                    }
                }
            }
        }

        // Finally
        true // Passed
    }

    fn check_price(instrument: &InstrumentAny, price: Option<Price>) -> Option<String> {
        let price_val = price?;

        if price_val.precision > instrument.price_precision() {
            return Some(format!(
                "price {} invalid (precision {} > {})",
                price_val,
                price_val.precision,
                instrument.price_precision()
            ));
        }

        if !instrument.allows_negative_price() && price_val.raw <= 0 {
            return Some(format!("price {price_val} invalid (<= 0)"));
        }

        None
    }

    fn check_quantity(
        instrument: &InstrumentAny,
        quantity: Option<Quantity>,
        is_quote_quantity: bool,
    ) -> Option<String> {
        let quantity_val = quantity?;

        // Check precision
        if quantity_val.precision > instrument.size_precision() {
            return Some(format!(
                "quantity {} invalid (precision {} > {})",
                quantity_val,
                quantity_val.precision,
                instrument.size_precision()
            ));
        }

        // Skip min/max checks for quote quantities (they will be checked in check_orders_risk using effective_quantity)
        if is_quote_quantity {
            return None;
        }

        // Check maximum quantity
        if let Some(max_quantity) = instrument.max_quantity()
            && quantity_val > max_quantity
        {
            return Some(format!(
                "quantity {quantity_val} invalid (> maximum trade size of {max_quantity})"
            ));
        }

        // Check minimum quantity
        if let Some(min_quantity) = instrument.min_quantity()
            && quantity_val < min_quantity
        {
            return Some(format!(
                "quantity {quantity_val} invalid (< minimum trade size of {min_quantity})"
            ));
        }

        None
    }

    fn deny_command(&self, command: TradingCommand, reason: &str) {
        match command {
            TradingCommand::SubmitOrder(command) => {
                let order = {
                    let cache = self.cache.borrow();
                    cache.order(&command.client_order_id).map(|o| o.clone())
                };

                if let Some(ref order) = order {
                    self.deny_order(order, reason);
                } else {
                    log::error!(
                        "Cannot deny order: not found in cache for {}",
                        command.client_order_id
                    );
                }
            }
            TradingCommand::SubmitOrderList(command) => {
                let orders: Vec<OrderAny> = self
                    .cache
                    .borrow()
                    .orders_for_ids(&command.order_list.client_order_ids, &command);
                self.deny_order_list(&orders, reason);
            }
            _ => {
                log::error!("Cannot deny command {command}");
            }
        }
    }

    fn deny_order(&self, order: &OrderAny, reason: &str) {
        log::warn!(
            "SubmitOrder for {} DENIED: {}",
            order.client_order_id(),
            reason
        );

        if order.status() != OrderStatus::Initialized {
            return;
        }

        // Scope the cache borrow to avoid RefCell conflict when sending to ExecEngine
        {
            let mut cache = self.cache.borrow_mut();
            if !cache.order_exists(&order.client_order_id())
                && let Err(e) = cache.add_order(order.clone(), None, None, false)
            {
                log::error!("Cannot add order to cache: {e}");
                return;
            }
        }

        let denied = OrderEventAny::Denied(OrderDenied::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            reason.into(),
            UUID4::new(),
            self.clock.borrow().timestamp_ns(),
            self.clock.borrow().timestamp_ns(),
        ));

        let endpoint = MessagingSwitchboard::exec_engine_process();
        msgbus::send_order_event(endpoint, denied);
    }

    fn deny_order_list(&self, orders: &[OrderAny], reason: &str) {
        for order in orders {
            if !order.is_closed() {
                self.deny_order(order, reason);
            }
        }
    }

    fn reject_modify_order(&self, order: &OrderAny, reason: &str) {
        let ts_event = self.clock.borrow().timestamp_ns();
        let denied = OrderEventAny::ModifyRejected(OrderModifyRejected::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            reason.into(),
            UUID4::new(),
            ts_event,
            ts_event,
            false,
            order.venue_order_id(),
            order.account_id(),
        ));

        let endpoint = MessagingSwitchboard::exec_engine_process();
        msgbus::send_order_event(endpoint, denied);
    }

    fn execution_gateway(&mut self, command: TradingCommand) {
        match self.trading_state {
            TradingState::Halted => match command {
                TradingCommand::SubmitOrder(submit_order) => {
                    let order = {
                        let cache = self.cache.borrow();
                        cache
                            .order(&submit_order.client_order_id)
                            .map(|o| o.clone())
                    };

                    if let Some(ref order) = order {
                        self.deny_order(order, &OrderDeniedReason::TradingHalted.to_string());
                    }
                }
                TradingCommand::SubmitOrderList(submit_order_list) => {
                    let orders: Vec<OrderAny> = self.cache.borrow().orders_for_ids(
                        &submit_order_list.order_list.client_order_ids,
                        &submit_order_list,
                    );
                    self.deny_order_list(&orders, &OrderDeniedReason::TradingHalted.to_string());
                }
                _ => {}
            },
            TradingState::Reducing => {
                match &command {
                    TradingCommand::SubmitOrder(submit_order) => {
                        let order = {
                            let cache = self.cache.borrow();
                            cache
                                .order(&submit_order.client_order_id)
                                .map(|o| o.clone())
                        };

                        if let Some(ref order) = order
                            && self
                                .first_non_reducing_order(std::slice::from_ref(order))
                                .is_some()
                        {
                            self.deny_order(
                                order,
                                &OrderDeniedReason::TradingStateReducing {
                                    order_side: order.order_side(),
                                    instrument_id: order.instrument_id(),
                                }
                                .to_string(),
                            );
                            return;
                        }
                    }
                    TradingCommand::SubmitOrderList(submit_order_list) => {
                        let orders: Vec<OrderAny> = self.cache.borrow().orders_for_ids(
                            &submit_order_list.order_list.client_order_ids,
                            &submit_order_list,
                        );

                        if let Some(order) = self.first_non_reducing_order(&orders) {
                            self.deny_order_list(
                                &orders,
                                &OrderDeniedReason::TradingStateReducing {
                                    order_side: order.order_side(),
                                    instrument_id: order.instrument_id(),
                                }
                                .to_string(),
                            );
                            return;
                        }
                    }
                    _ => {}
                }
                // Not denied: forward to throttler
                self.throttled_submit.send(command);
            }
            TradingState::Active => match command {
                TradingCommand::SubmitOrder(_) | TradingCommand::SubmitOrderList(_) => {
                    self.throttled_submit.send(command);
                }
                _ => {}
            },
        }
    }

    fn send_to_execution(command: TradingCommand) {
        let endpoint = MessagingSwitchboard::exec_engine_queue_execute();
        msgbus::send_trading_command(endpoint, command);
    }

    fn handle_event(&self, event: &OrderEventAny) {
        // We intend to extend the risk engine to be able to handle additional events.
        // For now we just log.
        if self.config.debug {
            log::debug!("{RECV}{EVT} {event:?}");
        }
    }

    fn handle_position_event(&self, event: &PositionEvent) {
        if self.config.debug {
            log::debug!("{RECV}{EVT} {event:?}");
        }
    }
}
