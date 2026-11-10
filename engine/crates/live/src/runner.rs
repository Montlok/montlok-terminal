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

//! Async event loop runner for live and sandbox trading nodes.
//!
//! `AsyncRunner` owns five tokio mpsc channel pairs plus a shutdown
//! signal channel. Construction creates the channels without side
//! effects. The sender halves are placed into thread-local storage
//! via [`AsyncRunner::bind_senders`] so that adapters and engine
//! components can resolve them through the `get_*_sender()` accessors
//! in `nautilus_common::runner` and `nautilus_common::live::runner`.
//!
//! Channel pairs:
//!
//! - **Time events**: timer callbacks dispatched by the clock.
//! - **Execution events**: fills, order updates, and account state from
//!   execution clients to the execution engine.
//! - **Trading commands**: deferred order actions routed to their direct endpoint.
//! - **Data events**: market data from adapters to the data engine.
//! - **Data commands**: subscribe/unsubscribe requests to data clients.
//!
//! Both `AsyncRunner::run` and `LiveNode::run` use a `biased;` select with
//! exec branches polled ahead of data branches, so a strategy action
//! (cancel, submit) is not delayed behind a market-data backlog when the
//! select polls receivers each iteration. The two loops use slightly
//! different cmd/evt sub-orders because `LiveNode::run` also folds in the
//! maintenance timer and signal handling that `AsyncRunner::run` does not
//! see; check each `select!` block for the exact order at that site.
//!
//! The runner can drive the event loop in two ways:
//!
//! - **Standalone**: call [`AsyncRunner::run`], which binds senders and
//!   enters a `tokio::select!` loop internally.
//! - **Integrated**: call [`AsyncRunner::take_channels`] to extract the
//!   receivers and run the `select!` loop directly inside `LiveNode::run`,
//!   where it is interleaved with startup, reconciliation, and shutdown
//!   phases.
//!
//! # Invariants
//!
//! - `bind_senders` must be called before any code that reads from TLS.
//!   This includes adapter constructors, clock initialization, and
//!   execution client start methods. Every path from construction to
//!   the event loop must bind before the first TLS read.
//! - The event loop and all TLS consumers must execute on the same
//!   thread. Senders are cloneable and `Send`, but the `RefCell`-backed
//!   TLS slots are not accessible from other threads.
//! - Only one runner at a time can own the TLS slots on a given thread. Rebinding the same bundle
//!   is idempotent; a different bundle is rejected before any TLS slot changes.
//! - A bound runner must be consumed and dropped on the same OS thread. TLS cannot be cleared from
//!   another thread, so cross-thread rebinding or unbinding fails closed.

use std::{fmt::Debug, sync::Arc};

use nautilus_common::{
    live::runner::{
        LiveEventSender, LiveIngressBundle, LiveIngressControl, LiveIngressFailure, LiveIngressLane,
    },
    messages::{
        DataEvent, ExecutionEvent, ExecutionReport, data::DataCommand, execution::TradingCommand,
    },
    msgbus::{self, MessagingSwitchboard},
    runner::{
        DataCommandSender, TimeEventMessage, TimeEventSender, TradingCommandMessage,
        TradingCommandSender, replace_data_cmd_sender, replace_exec_cmd_sender,
        replace_time_event_sender,
    },
};
use nautilus_model::events::OrderEventAny;

/// Asynchronous implementation of `DataCommandSender` for live environments.
#[derive(Debug)]
pub struct AsyncDataCommandSender {
    cmd_tx: LiveEventSender<DataCommand>,
}

impl AsyncDataCommandSender {
    #[must_use]
    pub(crate) fn new(cmd_tx: LiveEventSender<DataCommand>) -> Self {
        assert_eq!(cmd_tx.lane(), LiveIngressLane::DataCommands);
        Self { cmd_tx }
    }
}

impl DataCommandSender for AsyncDataCommandSender {
    fn execute(&self, command: DataCommand) {
        if let Err(e) = self.cmd_tx.send(command)
            && !e.is_fail_stopped()
        {
            log::error!("Failed to send data command: {e}");
        }
    }
}

/// Asynchronous implementation of `TimeEventSender` for live environments.
#[derive(Debug, Clone)]
pub struct AsyncTimeEventSender {
    time_tx: LiveEventSender<TimeEventMessage>,
}

impl AsyncTimeEventSender {
    #[must_use]
    pub(crate) fn new(time_tx: LiveEventSender<TimeEventMessage>) -> Self {
        assert_eq!(time_tx.lane(), LiveIngressLane::TimeEvents);
        Self { time_tx }
    }
}

impl TimeEventSender for AsyncTimeEventSender {
    fn send(&self, message: TimeEventMessage) {
        if let Err(e) = self.time_tx.send(message)
            && !e.is_fail_stopped()
        {
            log::error!("Failed to send time event message: {e}");
        }
    }
}

/// Asynchronous implementation of `TradingCommandSender` for live environments.
#[derive(Debug)]
pub struct AsyncTradingCommandSender {
    cmd_tx: LiveEventSender<TradingCommandMessage>,
}

impl AsyncTradingCommandSender {
    #[must_use]
    pub(crate) fn new(cmd_tx: LiveEventSender<TradingCommandMessage>) -> Self {
        assert_eq!(cmd_tx.lane(), LiveIngressLane::ExecCommands);
        Self { cmd_tx }
    }
}

impl TradingCommandSender for AsyncTradingCommandSender {
    fn execute(&self, message: TradingCommandMessage) {
        if let Err(e) = self.cmd_tx.send(message)
            && !e.is_fail_stopped()
        {
            log::error!("Failed to send trading command: {e}");
        }
    }
}

pub trait Runner {
    fn run(&mut self);
}

/// Channel receivers for the async event loop.
///
/// These can be extracted from `AsyncRunner` via `take_channels()` to drive
/// the event loop directly on the same thread as the msgbus endpoints.
#[derive(Debug)]
pub struct AsyncRunnerChannels {
    pub time_evt_rx: tokio::sync::mpsc::Receiver<TimeEventMessage>,
    pub exec_evt_rx: tokio::sync::mpsc::Receiver<ExecutionEvent>,
    pub exec_cmd_rx: tokio::sync::mpsc::Receiver<TradingCommandMessage>,
    pub data_evt_rx: tokio::sync::mpsc::Receiver<DataEvent>,
    pub data_cmd_rx: tokio::sync::mpsc::Receiver<DataCommand>,
    pub ingress_failure_rx: tokio::sync::watch::Receiver<Option<LiveIngressFailure>>,
    pub ingress_control: LiveIngressControl,
    pub(crate) ingress_bundle: LiveIngressBundle,
}

#[cfg(feature = "node")]
#[allow(
    clippy::large_enum_variant,
    reason = "runner channels already carry these domain values by ownership; boxing would alter the hot-path allocation contract"
)]
pub(crate) enum PendingRunnerEvent {
    Time(TimeEventMessage),
    ExecEvent(ExecutionEvent),
    ExecCommand(TradingCommandMessage),
    DataEvent(DataEvent),
    DataCommand(DataCommand),
    IngressFailure(LiveIngressFailure),
}

pub struct AsyncRunner {
    channels: AsyncRunnerChannels,
    signal_rx: tokio::sync::mpsc::Receiver<()>,
    signal_tx: tokio::sync::mpsc::Sender<()>,
}

/// Handle for stopping the `AsyncRunner` from another context.
#[derive(Clone, Debug)]
pub struct AsyncRunnerHandle {
    signal_tx: tokio::sync::mpsc::Sender<()>,
}

impl AsyncRunnerHandle {
    /// Signals the runner to stop.
    pub fn stop(&self) {
        if let Err(error) = self.signal_tx.try_send(())
            && !matches!(error, tokio::sync::mpsc::error::TrySendError::Full(()))
        {
            log::error!("Failed to send shutdown signal: {error}");
        }
    }
}

impl Default for AsyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for AsyncRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AsyncRunner)).finish()
    }
}

impl AsyncRunner {
    /// Default per-lane capacity used by a standalone runner.
    pub const DEFAULT_QUEUE_CAPACITY: usize = 100_000;

    /// Creates a new [`AsyncRunner`] instance.
    ///
    /// Creates channels but does not bind senders to thread-local storage.
    /// Call [`bind_senders`](Self::bind_senders) before creating clients that
    /// read from TLS, and again before entering the event loop.
    #[must_use]
    pub fn new() -> Self {
        Self::with_validated_queue_capacities(
            Self::DEFAULT_QUEUE_CAPACITY,
            Self::DEFAULT_QUEUE_CAPACITY,
            Self::DEFAULT_QUEUE_CAPACITY,
        )
    }

    /// Creates a runner whose data, risk-command, and execution lanes use the supplied hard
    /// capacities.
    ///
    /// # Errors
    ///
    /// Returns an error when any capacity is zero.
    pub fn with_queue_capacities(
        data_capacity: usize,
        risk_capacity: usize,
        exec_capacity: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(data_capacity > 0, "data ingress capacity must be non-zero");
        anyhow::ensure!(risk_capacity > 0, "risk ingress capacity must be non-zero");
        anyhow::ensure!(
            exec_capacity > 0,
            "execution ingress capacity must be non-zero"
        );

        Ok(Self::with_validated_queue_capacities(
            data_capacity,
            risk_capacity,
            exec_capacity,
        ))
    }

    fn with_validated_queue_capacities(
        data_capacity: usize,
        risk_capacity: usize,
        exec_capacity: usize,
    ) -> Self {
        debug_assert!(data_capacity > 0);
        debug_assert!(risk_capacity > 0);
        debug_assert!(exec_capacity > 0);

        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel(1);
        let (ingress_bundle, receivers) =
            LiveIngressBundle::bounded(data_capacity, risk_capacity, exec_capacity)
                .expect("validated live ingress capacities must construct a complete bundle");
        let ingress_control = ingress_bundle.control().clone();

        Self {
            channels: AsyncRunnerChannels {
                time_evt_rx: receivers.time_events,
                exec_evt_rx: receivers.exec_events,
                exec_cmd_rx: receivers.exec_commands,
                data_evt_rx: receivers.data_events,
                data_cmd_rx: receivers.data_commands,
                ingress_failure_rx: ingress_control.subscribe(),
                ingress_control,
                ingress_bundle,
            },
            signal_rx,
            signal_tx,
        }
    }

    /// Binds this runner's channel senders to thread-local storage.
    ///
    /// Call before creating clients that read from TLS (e.g., in the builder),
    /// and again before entering the event loop to reassert this runner's binding.
    /// A different live runner cannot replace an active bundle on the same thread.
    ///
    /// # Panics
    ///
    /// Panics if another live ingress bundle owns the current thread or this runner was already
    /// bound on a different thread. `AsyncRunner` is statically thread-affine, so the latter cannot
    /// occur through safe Rust after construction.
    pub fn bind_senders(&self) {
        self.channels
            .ingress_bundle
            .bind_current_thread()
            .expect("live ingress TLS must be owned by this complete runner bundle");
        replace_time_event_sender(Arc::new(AsyncTimeEventSender::new(
            self.channels.ingress_bundle.time_event_sender(),
        )));
        replace_exec_cmd_sender(Arc::new(AsyncTradingCommandSender::new(
            self.channels.ingress_bundle.exec_command_sender(),
        )));
        replace_data_cmd_sender(Arc::new(AsyncDataCommandSender::new(
            self.channels.ingress_bundle.data_command_sender(),
        )));
    }

    /// Stops the runner with an internal shutdown signal.
    pub fn stop(&self) {
        if let Err(error) = self.signal_tx.try_send(())
            && !matches!(error, tokio::sync::mpsc::error::TrySendError::Full(()))
        {
            log::error!("Failed to send shutdown signal: {error}");
        }
    }

    /// Returns a handle that can be used to stop the runner from another context.
    #[must_use]
    pub fn handle(&self) -> AsyncRunnerHandle {
        AsyncRunnerHandle {
            signal_tx: self.signal_tx.clone(),
        }
    }

    /// Returns the runner-wide bounded-ingress fail-stop control.
    #[must_use]
    pub fn ingress_control(&self) -> LiveIngressControl {
        self.channels.ingress_control.clone()
    }

    /// Returns the first ingress failure, if one has already occurred.
    #[must_use]
    pub fn ingress_failure(&self) -> Option<LiveIngressFailure> {
        self.channels.ingress_control.failure()
    }

    /// Consumes the runner and returns the channel receivers for direct event loop driving.
    ///
    /// This is used when the event loop needs to run on the same thread as the msgbus
    /// endpoints (which use thread-local storage).
    #[must_use]
    pub fn take_channels(self) -> AsyncRunnerChannels {
        self.channels
    }

    /// Flushes all pending data events and commands from the channels.
    ///
    /// Loops until both data channels are empty, processing each item
    /// into the cache immediately. Used in `start()` where channels are
    /// not extracted.
    pub fn flush_pending_data(&mut self) {
        let mut total = 0;

        loop {
            let mut progressed = false;

            // Events drain before commands here even though the runtime select
            // prefers the opposite for everything-else: `LiveNode::start()`
            // calls this after `connect_data_clients()` to push queued
            // `DataEvent::Instrument` items into the cache. A pending
            // subscription command (e.g. `SubscribeBars`) processed before the
            // matching instrument lands would be rejected by the data engine.
            while let Ok(evt) = self.channels.data_evt_rx.try_recv() {
                Self::handle_data_event(evt);
                progressed = true;
                total += 1;
            }

            while let Ok(cmd) = self.channels.data_cmd_rx.try_recv() {
                Self::handle_data_command(cmd);
                progressed = true;
                total += 1;
            }

            if !progressed {
                break;
            }
        }

        if total > 0 {
            log::debug!("Flushed {total} pending data events/commands");
        }
    }

    /// Runs the async runner event loop.
    ///
    /// This method processes data events, time events, execution events, and signal events in an async loop.
    /// It will run until a signal is received or the event streams are closed.
    pub async fn run(&mut self) {
        self.bind_senders();

        log::info!("AsyncRunner starting");

        loop {
            tokio::select! {
                biased;

                Some(()) = self.signal_rx.recv() => {
                    log::info!("AsyncRunner received signal, shutting down");
                    return;
                },
                result = self.channels.ingress_failure_rx.changed() => {
                    let failure = self.channels.ingress_control.failure();
                    if result.is_err() || failure.is_some() {
                        log::error!("Live ingress fail-stopped: {failure:?}");
                        return;
                    }
                },
                Some(handler) = self.channels.time_evt_rx.recv() => {
                    let _ = Self::handle_time_event(handler);
                },
                Some(cmd) = self.channels.exec_cmd_rx.recv() => {
                    Self::handle_trading_command(cmd);
                },
                Some(evt) = self.channels.exec_evt_rx.recv() => {
                    Self::handle_exec_event(evt);
                },
                Some(cmd) = self.channels.data_cmd_rx.recv() => {
                    Self::handle_data_command(cmd);
                },
                Some(evt) = self.channels.data_evt_rx.recv() => {
                    Self::handle_data_event(evt);
                },
                else => {
                    log::debug!("AsyncRunner all channels closed, exiting");
                    return;
                }
            };
        }
    }

    /// Handles a time event by running its callback.
    #[inline]
    #[must_use]
    pub fn handle_time_event(message: TimeEventMessage) -> bool {
        message.dispatch()
    }

    /// Handles a data command by sending to the `DataEngine`.
    #[inline]
    pub fn handle_data_command(cmd: DataCommand) {
        msgbus::send_data_command(MessagingSwitchboard::data_engine_execute(), cmd);
    }

    /// Handles a data event by sending to the appropriate `DataEngine` endpoint.
    #[inline]
    pub fn handle_data_event(event: DataEvent) {
        match event {
            DataEvent::Data(data) => {
                msgbus::send_data(MessagingSwitchboard::data_engine_process_data(), data);
            }
            DataEvent::Instrument(data) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &data);
            }
            DataEvent::Response(resp) => {
                msgbus::send_data_response(MessagingSwitchboard::data_engine_response(), resp);
            }
            DataEvent::FundingRate(funding_rate) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &funding_rate);
            }
            DataEvent::InstrumentStatus(status) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &status);
            }
            DataEvent::OptionGreeks(greeks) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &greeks);
            }
            #[cfg(feature = "defi")]
            DataEvent::DeFi(data) => {
                msgbus::send_defi_data(MessagingSwitchboard::data_engine_process_defi_data(), data);
            }
        }
    }

    /// Dispatches an internal execution command directly to the execution engine.
    #[inline]
    pub fn handle_exec_command(cmd: TradingCommand) {
        msgbus::send_trading_command(MessagingSwitchboard::exec_engine_execute(), cmd);
    }

    /// Dispatches a deferred trading command to its direct endpoint.
    #[inline]
    pub fn handle_trading_command(message: TradingCommandMessage) {
        let mut messages = vec![message];
        while let Some(message) = messages.pop() {
            messages.extend(message.dispatch().into_iter().rev());
        }
    }

    /// Handles an execution event by sending to the appropriate engine endpoint.
    #[inline]
    pub fn handle_exec_event(event: ExecutionEvent) {
        match event {
            ExecutionEvent::Order(order_event) => {
                msgbus::send_order_event(MessagingSwitchboard::exec_engine_process(), order_event);
            }
            ExecutionEvent::OrderSubmittedBatch(batch) => {
                for submitted in batch {
                    msgbus::send_order_event(
                        MessagingSwitchboard::exec_engine_process(),
                        OrderEventAny::Submitted(submitted),
                    );
                }
            }
            ExecutionEvent::OrderAcceptedBatch(batch) => {
                for accepted in batch {
                    msgbus::send_order_event(
                        MessagingSwitchboard::exec_engine_process(),
                        OrderEventAny::Accepted(accepted),
                    );
                }
            }
            ExecutionEvent::OrderCanceledBatch(batch) => {
                for canceled in batch {
                    msgbus::send_order_event(
                        MessagingSwitchboard::exec_engine_process(),
                        OrderEventAny::Canceled(canceled),
                    );
                }
            }
            ExecutionEvent::Report(report) => {
                Self::handle_exec_report(report);
            }
            ExecutionEvent::Account(ref account) => {
                msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    account,
                );
            }
        }
    }

    #[inline]
    pub fn handle_exec_report(report: ExecutionReport) {
        let endpoint = MessagingSwitchboard::exec_engine_reconcile_execution_report();
        msgbus::send_execution_report(endpoint, report);
    }
}

#[cfg(feature = "node")]
impl AsyncRunner {
    pub(crate) fn poll_pending(&mut self, mut process: impl FnMut(PendingRunnerEvent)) -> usize {
        self.bind_senders();

        if let Some(failure) = self.ingress_failure() {
            process(PendingRunnerEvent::IngressFailure(failure));
            return 1;
        }

        let pending = (
            self.channels.time_evt_rx.len(),
            self.channels.exec_evt_rx.len(),
            self.channels.exec_cmd_rx.len(),
            self.channels.data_evt_rx.len(),
            self.channels.data_cmd_rx.len(),
        );
        let mut processed = 0;
        processed += poll_channel(
            &mut self.channels.time_evt_rx,
            pending.0,
            PendingRunnerEvent::Time,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.exec_evt_rx,
            pending.1,
            PendingRunnerEvent::ExecEvent,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.exec_cmd_rx,
            pending.2,
            PendingRunnerEvent::ExecCommand,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.data_evt_rx,
            pending.3,
            PendingRunnerEvent::DataEvent,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.data_cmd_rx,
            pending.4,
            PendingRunnerEvent::DataCommand,
            &mut process,
        );
        processed
    }

    pub(crate) async fn recv(&mut self) -> Option<PendingRunnerEvent> {
        tokio::select! {
            biased;

            result = self.channels.ingress_failure_rx.changed() => {
                if result.is_err() {
                    None
                } else {
                    self.channels
                        .ingress_control
                        .failure()
                        .map(PendingRunnerEvent::IngressFailure)
                }
            }
            Some(message) = self.channels.time_evt_rx.recv() => {
                Some(PendingRunnerEvent::Time(message))
            }
            Some(event) = self.channels.exec_evt_rx.recv() => {
                Some(PendingRunnerEvent::ExecEvent(event))
            }
            Some(command) = self.channels.exec_cmd_rx.recv() => {
                Some(PendingRunnerEvent::ExecCommand(command))
            }
            Some(event) = self.channels.data_evt_rx.recv() => {
                Some(PendingRunnerEvent::DataEvent(event))
            }
            Some(command) = self.channels.data_cmd_rx.recv() => {
                Some(PendingRunnerEvent::DataCommand(command))
            }
            else => None,
        }
    }
}

#[cfg(feature = "node")]
fn poll_channel<T>(
    receiver: &mut tokio::sync::mpsc::Receiver<T>,
    pending: usize,
    event: impl Fn(T) -> PendingRunnerEvent,
    process: &mut impl FnMut(PendingRunnerEvent),
) -> usize {
    let mut processed = 0;

    for _ in 0..pending {
        let Ok(message) = receiver.try_recv() else {
            break;
        };

        process(event(message));
        processed += 1;
    }

    processed
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        live::runner::{get_data_event_sender, get_exec_event_sender},
        messages::{
            ExecutionEvent, ExecutionReport,
            data::{SubscribeCommand, SubscribeCustomData},
            execution::{CancelAllOrders, TradingCommand},
        },
        msgbus::{TypedIntoHandler, stubs::get_typed_into_message_saving_handler},
        runner::{
            TimeEventMessage, get_data_cmd_sender, get_time_event_sender, get_trading_cmd_sender,
            replace_exec_cmd_sender, try_get_time_event_sender, try_get_trading_cmd_sender,
        },
        timer::{TimeEvent, TimeEventCallback},
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_execution::engine::ExecutionEngine;
    use nautilus_model::{
        data::{Data, DataType, quote::QuoteTick},
        enums::{
            AccountType, LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSideSpecified,
            TimeInForce,
        },
        events::{
            OrderAcceptedBatch, OrderCanceledBatch, OrderEvent, OrderEventAny, OrderSubmittedBatch,
            account::state::AccountState,
            order::spec::{OrderAcceptedSpec, OrderCanceledSpec, OrderSubmittedSpec},
        },
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
            TraderId, VenueOrderId,
        },
        reports::{FillReport, OrderStatusReport, PositionStatusReport},
        types::{Money, Price, Quantity},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;

    // Test fixture for creating test quotes
    fn test_quote() -> QuoteTick {
        QuoteTick {
            instrument_id: InstrumentId::from("EUR/USD.SIM"),
            bid_price: Price::from("1.10000"),
            ask_price: Price::from("1.10001"),
            bid_size: Quantity::from(1_000_000),
            ask_size: Quantity::from(1_000_000),
            ts_event: UnixNanos::default(),
            ts_init: UnixNanos::default(),
        }
    }

    fn test_data_command_channel() -> (
        LiveEventSender<DataCommand>,
        tokio::sync::mpsc::Receiver<DataCommand>,
    ) {
        let (bundle, receivers) = LiveIngressBundle::bounded(4096, 4096, 4096).unwrap();
        (bundle.data_command_sender(), receivers.data_commands)
    }

    fn test_time_event_channel() -> (
        LiveEventSender<TimeEventMessage>,
        tokio::sync::mpsc::Receiver<TimeEventMessage>,
    ) {
        let (bundle, receivers) = LiveIngressBundle::bounded(4096, 4096, 4096).unwrap();
        (bundle.time_event_sender(), receivers.time_events)
    }

    fn test_exec_command_channel() -> (
        LiveEventSender<TradingCommandMessage>,
        tokio::sync::mpsc::Receiver<TradingCommandMessage>,
    ) {
        let (bundle, receivers) = LiveIngressBundle::bounded(4096, 4096, 4096).unwrap();
        (bundle.exec_command_sender(), receivers.exec_commands)
    }

    // Test fixture to create AsyncRunner with manual channels.
    // Sender halves are dummies (not connected to the test receivers) since
    // these tests exercise the event loop, not TLS binding.
    fn create_test_runner(
        time_evt_rx: tokio::sync::mpsc::Receiver<TimeEventMessage>,
        data_evt_rx: tokio::sync::mpsc::Receiver<DataEvent>,
        data_cmd_rx: tokio::sync::mpsc::Receiver<DataCommand>,
        exec_evt_rx: tokio::sync::mpsc::Receiver<ExecutionEvent>,
        exec_cmd_rx: tokio::sync::mpsc::Receiver<TradingCommandMessage>,
        signal_rx: tokio::sync::mpsc::Receiver<()>,
        signal_tx: tokio::sync::mpsc::Sender<()>,
    ) -> AsyncRunner {
        let (ingress_bundle, _receivers) = LiveIngressBundle::bounded(4096, 4096, 4096).unwrap();
        let ingress_control = ingress_bundle.control().clone();

        AsyncRunner {
            channels: AsyncRunnerChannels {
                time_evt_rx,
                exec_evt_rx,
                exec_cmd_rx,
                data_evt_rx,
                data_cmd_rx,
                ingress_failure_rx: ingress_control.subscribe(),
                ingress_control,
                ingress_bundle,
            },
            signal_rx,
            signal_tx,
        }
    }

    #[cfg(feature = "node")]
    #[rstest]
    fn test_poll_pending_processes_entry_snapshot_across_channels() {
        let (time_evt_tx, time_evt_rx) = tokio::sync::mpsc::channel(4096);
        let (data_evt_tx, data_evt_rx) = tokio::sync::mpsc::channel(4096);
        let (data_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel(4096);
        let (exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel(4096);
        let (exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel(4096);

        let time_event = TimeEvent::new(
            Ustr::from("test"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        time_evt_tx
            .try_send(TimeEventMessage::new(
                time_event,
                TimeEventCallback::from(|_: TimeEvent| {}),
            ))
            .unwrap();
        exec_evt_tx
            .try_send(ExecutionEvent::Order(OrderEventAny::Submitted(
                OrderSubmittedSpec::builder()
                    .client_order_id(ClientOrderId::from("O-POLL-001"))
                    .build(),
            )))
            .unwrap();
        exec_cmd_tx
            .try_send(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("S-POLL-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    OrderSide::Buy,
                    UUID4::new(),
                    UnixNanos::from(3),
                    None,
                    None,
                )),
            ))
            .unwrap();
        data_evt_tx
            .try_send(DataEvent::Data(Data::Quote(test_quote())))
            .unwrap();
        data_cmd_tx
            .try_send(DataCommand::Subscribe(SubscribeCommand::Data(
                SubscribeCustomData {
                    client_id: Some(ClientId::from("POLL")),
                    venue: None,
                    data_type: DataType::new("QuoteTick", None, None),
                    command_id: UUID4::new(),
                    ts_init: UnixNanos::from(4),
                    correlation_id: None,
                    params: None,
                },
            )))
            .unwrap();

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx,
        );
        let mut processed_by_channel = [0; 5];

        let first = runner.poll_pending(|event| match event {
            PendingRunnerEvent::Time(_) => processed_by_channel[0] += 1,
            PendingRunnerEvent::ExecEvent(_) => processed_by_channel[1] += 1,
            PendingRunnerEvent::ExecCommand(_) => processed_by_channel[2] += 1,
            PendingRunnerEvent::DataEvent(_) => {
                processed_by_channel[3] += 1;
                data_evt_tx
                    .try_send(DataEvent::Data(Data::Quote(test_quote())))
                    .unwrap();
            }
            PendingRunnerEvent::DataCommand(_) => processed_by_channel[4] += 1,
            PendingRunnerEvent::IngressFailure(failure) => {
                panic!("Unexpected ingress failure: {failure}")
            }
        });
        let second = runner.poll_pending(|event| match event {
            PendingRunnerEvent::DataEvent(_) => processed_by_channel[3] += 1,
            _ => panic!("Unexpected runner event"),
        });

        assert_eq!(first, 5);
        assert_eq!(second, 1);
        assert_eq!(processed_by_channel, [1, 1, 1, 2, 1]);
    }

    #[rstest]
    fn test_async_data_command_sender_creation() {
        let (tx, _rx) = test_data_command_channel();
        let sender = AsyncDataCommandSender::new(tx);
        assert!(format!("{sender:?}").contains("AsyncDataCommandSender"));
    }

    #[rstest]
    fn test_async_time_event_sender_creation() {
        let (tx, _rx) = test_time_event_channel();
        let sender = AsyncTimeEventSender::new(tx);
        assert!(format!("{sender:?}").contains("AsyncTimeEventSender"));
    }

    #[tokio::test]
    async fn test_async_data_command_sender_execute() {
        let (tx, mut rx) = test_data_command_channel();
        let sender = AsyncDataCommandSender::new(tx);

        let command = DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
            client_id: Some(ClientId::from("TEST")),
            venue: None,
            data_type: DataType::new("QuoteTick", None, None),
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            params: None,
        }));

        sender.execute(command.clone());

        let received = rx.recv().await.unwrap();
        match (received, command) {
            (
                DataCommand::Subscribe(SubscribeCommand::Data(r)),
                DataCommand::Subscribe(SubscribeCommand::Data(c)),
            ) => {
                assert_eq!(r.client_id, c.client_id);
                assert_eq!(r.data_type, c.data_type);
            }
            _ => panic!("Command mismatch"),
        }
    }

    #[tokio::test]
    async fn test_async_time_event_sender_send() {
        let (tx, mut rx) = test_time_event_channel();
        let sender = AsyncTimeEventSender::new(tx);

        let event = TimeEvent::new(
            Ustr::from("test"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let callback = TimeEventCallback::from(|_: TimeEvent| {});
        let message = TimeEventMessage::new(event, callback);

        sender.send(message);

        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_runner_shutdown_signal() {
        // Create runner with manual channels to avoid global state
        let (_data_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        // Start runner

        // Send shutdown signal
        signal_tx.try_send(()).unwrap();

        // Runner should stop quickly
        let result = tokio::time::timeout(Duration::from_millis(100), runner.run()).await;
        assert!(result.is_ok(), "Runner should stop on signal");
    }

    #[tokio::test]
    async fn test_runner_closes_on_channel_drop() {
        let (data_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        // Start runner

        drop(data_tx);

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        signal_tx.try_send(()).ok();

        // Runner should stop when channels close or on signal
        let result = tokio::time::timeout(Duration::from_millis(200), runner.run()).await;
        assert!(
            result.is_ok(),
            "Runner should stop when channels close or on signal"
        );
    }

    #[tokio::test]
    async fn test_concurrent_event_sending() {
        let (data_evt_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_data_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_evt_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        // Setup runner
        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        // Spawn multiple concurrent senders
        let mut handles = vec![];

        for _ in 0..5 {
            let tx_clone = data_evt_tx.clone();

            let handle = tokio::spawn(async move {
                for _ in 0..20 {
                    let quote = test_quote();
                    tx_clone
                        .try_send(DataEvent::Data(Data::Quote(quote)))
                        .unwrap();
                    tokio::task::yield_now().await;
                }
            });
            handles.push(handle);
        }

        // Start runner in background

        // Wait for all senders
        for handle in handles {
            handle.await.unwrap();
        }

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        signal_tx.try_send(()).unwrap();

        let _ = tokio::time::timeout(Duration::from_millis(200), runner.run()).await;
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_channel_send_performance(#[case] count: usize) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let quote = test_quote();

        // Send events
        for _ in 0..count {
            tx.try_send(DataEvent::Data(Data::Quote(quote))).unwrap();
        }

        // Verify all received
        let mut received = 0;
        while rx.try_recv().is_ok() {
            received += 1;
        }

        assert_eq!(received, count);
    }

    #[rstest]
    fn test_async_trading_command_sender_creation() {
        let (tx, _rx) = test_exec_command_channel();
        let sender = AsyncTradingCommandSender::new(tx);
        assert!(format!("{sender:?}").contains("AsyncTradingCommandSender"));
    }

    #[rstest]
    fn test_async_trading_command_sender_preserves_target_endpoints() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let (risk_handler, risk_saving_handler) =
                get_typed_into_message_saving_handler::<TradingCommand>(Some(Ustr::from(
                    "RiskEngine.execute",
                )));
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                risk_handler,
            );
            let (exec_handler, exec_saving_handler) =
                get_typed_into_message_saving_handler::<TradingCommand>(Some(Ustr::from(
                    "ExecEngine.execute",
                )));
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::exec_engine_execute(),
                exec_handler,
            );

            let (tx, mut rx) = test_exec_command_channel();
            let sender = AsyncTradingCommandSender::new(tx);
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("RISK-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    OrderSide::Buy,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("EXEC-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    OrderSide::Sell,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));

            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());
            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());

            let risk_commands = risk_saving_handler.get_messages();
            let exec_commands = exec_saving_handler.get_messages();
            assert!(rx.try_recv().is_err());
            assert_eq!(risk_commands.len(), 1);
            assert_eq!(
                risk_commands[0].strategy_id(),
                Some(StrategyId::from("RISK-001"))
            );
            assert_eq!(exec_commands.len(), 1);
            assert_eq!(
                exec_commands[0].strategy_id(),
                Some(StrategyId::from("EXEC-001"))
            );
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_async_runner_preserves_deferred_follow_up_order() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let clock = Rc::new(RefCell::new(TestClock::new()));
            let cache = Rc::new(RefCell::new(Cache::default()));
            let exec_engine = Rc::new(RefCell::new(ExecutionEngine::new(clock, cache, None)));
            ExecutionEngine::register_msgbus_handlers(&exec_engine);
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(|command: TradingCommand| {
                    msgbus::send_trading_command(
                        MessagingSwitchboard::exec_engine_queue_execute(),
                        command,
                    );
                }),
            );
            let (exec_handler, exec_saving_handler) =
                get_typed_into_message_saving_handler::<TradingCommand>(Some(Ustr::from(
                    "ExecEngine.execute",
                )));
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::exec_engine_execute(),
                exec_handler,
            );

            let (tx, mut rx) = test_exec_command_channel();
            let sender = Arc::new(AsyncTradingCommandSender::new(tx));
            replace_exec_cmd_sender(sender.clone());
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("FIRST-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    OrderSide::Buy,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("SECOND-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    OrderSide::Sell,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));

            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());
            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());

            let commands = exec_saving_handler.get_messages();
            let strategy_ids = commands
                .iter()
                .map(TradingCommand::strategy_id)
                .collect::<Vec<_>>();
            assert!(rx.try_recv().is_err());
            assert_eq!(commands.len(), 2);
            assert_eq!(
                strategy_ids,
                vec![
                    Some(StrategyId::from("FIRST-001")),
                    Some(StrategyId::from("SECOND-001"))
                ]
            );
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_async_runner_dispatches_deferred_exec_command_once() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let clock = Rc::new(RefCell::new(TestClock::new()));
            let cache = Rc::new(RefCell::new(Cache::default()));
            let exec_engine = Rc::new(RefCell::new(ExecutionEngine::new(clock, cache, None)));
            ExecutionEngine::register_msgbus_handlers(&exec_engine);

            let (tx, mut rx) = test_exec_command_channel();
            replace_exec_cmd_sender(Arc::new(AsyncTradingCommandSender::new(tx)));
            let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
                TraderId::from("TRADER-001"),
                None,
                StrategyId::from("EXEC-001"),
                InstrumentId::from("EUR/USD.SIM"),
                OrderSide::Buy,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ));

            msgbus::send_trading_command(
                MessagingSwitchboard::exec_engine_queue_execute(),
                command,
            );
            assert_eq!(exec_engine.borrow().command_count(), 0);

            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());

            assert!(rx.try_recv().is_err());
            assert_eq!(exec_engine.borrow().command_count(), 1);
        })
        .join()
        .unwrap();
    }

    #[tokio::test]
    async fn test_runner_processes_trading_commands() {
        let (_data_evt_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_data_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_evt_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            InstrumentId::from("EUR/USD.SIM"),
            OrderSide::Buy,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None, // correlation_id
        ));
        exec_cmd_tx
            .try_send(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                command,
            ))
            .unwrap();

        tokio::task::yield_now().await;
        signal_tx.try_send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), runner.run()).await;
        assert!(result.is_ok(), "Runner should process command and stop");
    }

    #[tokio::test]
    async fn test_runner_processes_multiple_trading_commands() {
        let (_data_evt_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_data_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_evt_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        for i in 0..10 {
            let strategy_id = format!("S-{i:03}");
            let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
                TraderId::from("TRADER-001"),
                None,
                StrategyId::from(strategy_id.as_str()),
                InstrumentId::from("EUR/USD.SIM"),
                OrderSide::Buy,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None, // correlation_id
            ));
            exec_cmd_tx
                .try_send(TradingCommandMessage::new(
                    MessagingSwitchboard::exec_engine_execute(),
                    command,
                ))
                .unwrap();
        }

        tokio::task::yield_now().await;
        signal_tx.try_send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), runner.run()).await;
        assert!(
            result.is_ok(),
            "Runner should process all commands and stop"
        );
    }

    #[tokio::test]
    async fn test_execution_event_order_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let event = OrderSubmittedSpec::builder()
            .client_order_id(ClientOrderId::from("O-001"))
            .build();

        tx.try_send(ExecutionEvent::Order(OrderEventAny::Submitted(event)))
            .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Order(OrderEventAny::Submitted(e)) => {
                assert_eq!(e.client_order_id(), ClientOrderId::from("O-001"));
            }
            _ => panic!("Expected OrderSubmitted event"),
        }
    }

    #[tokio::test]
    async fn test_execution_report_order_status_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let report = OrderStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            Some(ClientOrderId::from("O-001")),
            VenueOrderId::from("V-001"),
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::from(100_000),
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            UnixNanos::from(3),
            None,
        );

        tx.try_send(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
            report,
        ))))
        .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Report(ExecutionReport::Order(r)) => {
                assert_eq!(r.venue_order_id.as_str(), "V-001");
                assert_eq!(r.order_status, OrderStatus::Accepted);
            }
            _ => panic!("Expected OrderStatusReport"),
        }
    }

    #[tokio::test]
    async fn test_execution_report_fill() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let report = FillReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            VenueOrderId::from("V-001"),
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from(100_000),
            Price::from("1.10000"),
            Money::from("10 USD"),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-001")),
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );

        tx.try_send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
            report,
        ))))
        .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Report(ExecutionReport::Fill(r)) => {
                assert_eq!(r.venue_order_id.as_str(), "V-001");
                assert_eq!(r.trade_id.to_string(), "T-001");
            }
            _ => panic!("Expected FillReport"),
        }
    }

    #[tokio::test]
    async fn test_execution_report_position() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let report = PositionStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            PositionSideSpecified::Long,
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
            Some(PositionId::from("P-001")),
            None,
        );

        tx.try_send(ExecutionEvent::Report(ExecutionReport::Position(Box::new(
            report,
        ))))
        .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Report(ExecutionReport::Position(r)) => {
                assert_eq!(r.venue_position_id.unwrap().as_str(), "P-001");
            }
            _ => panic!("Expected PositionStatusReport"),
        }
    }

    #[tokio::test]
    async fn test_execution_event_account() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let account_state = AccountState::new(
            AccountId::from("SIM-001"),
            AccountType::Cash,
            vec![],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );

        tx.try_send(ExecutionEvent::Account(account_state)).unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Account(r) => {
                assert_eq!(r.account_id.as_str(), "SIM-001");
            }
            _ => panic!("Expected AccountState"),
        }
    }

    #[tokio::test]
    async fn test_runner_stop_method() {
        let (_data_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        // Use stop via signal_tx directly
        signal_tx.try_send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), runner.run()).await;
        assert!(result.is_ok(), "Runner should stop when stop() is called");
    }

    #[tokio::test]
    async fn test_all_event_types_integration() {
        let (data_evt_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (data_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (time_evt_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        // Send data event
        let quote = test_quote();
        data_evt_tx
            .try_send(DataEvent::Data(Data::Quote(quote)))
            .unwrap();

        // Send data command
        let command = DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
            client_id: Some(ClientId::from("TEST")),
            venue: None,
            data_type: DataType::new("QuoteTick", None, None),
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            params: None,
        }));
        data_cmd_tx.try_send(command).unwrap();

        // Send time event
        let event = TimeEvent::new(
            Ustr::from("test"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let callback = TimeEventCallback::from(|_: TimeEvent| {});
        let message = TimeEventMessage::new(event, callback);
        time_evt_tx.try_send(message).unwrap();

        // Send execution order event
        let order_event = OrderSubmittedSpec::builder()
            .client_order_id(ClientOrderId::from("O-001"))
            .build();
        exec_evt_tx
            .try_send(ExecutionEvent::Order(OrderEventAny::Submitted(order_event)))
            .unwrap();

        // Send execution report (OrderStatus)
        let order_status = OrderStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            Some(ClientOrderId::from("O-001")),
            VenueOrderId::from("V-001"),
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::from(100_000),
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            UnixNanos::from(3),
            None,
        );
        exec_evt_tx
            .try_send(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
                order_status,
            ))))
            .unwrap();

        // Send execution report (Fill)
        let fill = FillReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            VenueOrderId::from("V-001"),
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from(100_000),
            Price::from("1.10000"),
            Money::from("10 USD"),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-001")),
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );
        exec_evt_tx
            .try_send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                fill,
            ))))
            .unwrap();

        // Send execution report (Position)
        let position = PositionStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            PositionSideSpecified::Long,
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
            Some(PositionId::from("P-001")),
            None,
        );
        exec_evt_tx
            .try_send(ExecutionEvent::Report(ExecutionReport::Position(Box::new(
                position,
            ))))
            .unwrap();

        // Send account event
        let account_state = AccountState::new(
            AccountId::from("SIM-001"),
            AccountType::Cash,
            vec![],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );
        exec_evt_tx
            .try_send(ExecutionEvent::Account(account_state))
            .unwrap();

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        signal_tx.try_send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(200), runner.run()).await;
        assert!(
            result.is_ok(),
            "Runner should process all event types and stop cleanly"
        );
    }

    #[tokio::test]
    async fn test_runner_handle_stops_runner() {
        let (_data_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        // Get handle before moving runner
        let handle = runner.handle();

        // Use handle to stop
        handle.stop();

        let result = tokio::time::timeout(Duration::from_millis(100), runner.run()).await;
        assert!(result.is_ok(), "Runner should stop via handle");
    }

    #[tokio::test]
    async fn test_runner_handle_is_cloneable() {
        let (signal_tx, _signal_rx) = tokio::sync::mpsc::channel::<()>(4096);
        let handle = AsyncRunnerHandle { signal_tx };

        let handle2 = handle.clone();

        // Both handles should be able to send stop signals
        assert!(handle.signal_tx.try_send(()).is_ok());
        assert!(handle2.signal_tx.try_send(()).is_ok());
    }

    #[tokio::test]
    async fn test_runner_processes_events_before_stop() {
        let (data_evt_tx, data_evt_rx) = tokio::sync::mpsc::channel::<DataEvent>(4096);
        let (_cmd_tx, data_cmd_rx) = tokio::sync::mpsc::channel::<DataCommand>(4096);
        let (_time_tx, time_evt_rx) = tokio::sync::mpsc::channel::<TimeEventMessage>(4096);
        let (_exec_evt_tx, exec_evt_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);
        let (_exec_cmd_tx, exec_cmd_rx) = tokio::sync::mpsc::channel::<TradingCommandMessage>(4096);
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(4096);

        let mut runner = create_test_runner(
            time_evt_rx,
            data_evt_rx,
            data_cmd_rx,
            exec_evt_rx,
            exec_cmd_rx,
            signal_rx,
            signal_tx.clone(),
        );

        let handle = runner.handle();

        // Send events before starting runner
        for _ in 0..10 {
            let quote = test_quote();
            data_evt_tx
                .try_send(DataEvent::Data(Data::Quote(quote)))
                .unwrap();
        }

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        handle.stop();

        let result = tokio::time::timeout(Duration::from_millis(200), runner.run()).await;
        assert!(result.is_ok(), "Runner should process events and stop");
    }

    #[rstest]
    fn test_new_does_not_bind_tls() {
        std::thread::spawn(|| {
            let _runner = AsyncRunner::new();
            assert!(try_get_time_event_sender().is_none());
            assert!(try_get_trading_cmd_sender().is_none());
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_bind_senders_routes_to_runner_channels() {
        std::thread::spawn(|| {
            let mut runner = AsyncRunner::new();
            runner.bind_senders();

            get_data_cmd_sender().execute(DataCommand::Subscribe(SubscribeCommand::Data(
                SubscribeCustomData {
                    client_id: Some(ClientId::from("TEST")),
                    venue: None,
                    data_type: DataType::new("test", None, None),
                    command_id: UUID4::new(),
                    ts_init: UnixNanos::default(),
                    correlation_id: None,
                    params: None,
                },
            )));
            assert!(runner.channels.data_cmd_rx.try_recv().is_ok());

            get_trading_cmd_sender().execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("S-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    OrderSide::Buy,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None, // correlation_id
                )),
            ));
            assert!(runner.channels.exec_cmd_rx.try_recv().is_ok());

            let event = TimeEvent::new(
                Ustr::from("test"),
                UUID4::new(),
                UnixNanos::from(1),
                UnixNanos::from(2),
            );
            let callback = TimeEventCallback::from(|_: TimeEvent| {});
            get_time_event_sender().send(TimeEventMessage::new(event, callback));
            assert!(runner.channels.time_evt_rx.try_recv().is_ok());

            get_data_event_sender()
                .try_send(DataEvent::Data(Data::Quote(test_quote())))
                .unwrap();
            assert!(runner.channels.data_evt_rx.try_recv().is_ok());

            let account = AccountState::new(
                AccountId::from("SIM-001"),
                AccountType::Cash,
                vec![],
                vec![],
                true,
                UUID4::new(),
                UnixNanos::from(1),
                UnixNanos::from(2),
                None,
            );
            get_exec_event_sender()
                .try_send(ExecutionEvent::Account(account))
                .unwrap();
            assert!(runner.channels.exec_evt_rx.try_recv().is_ok());
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_bind_senders_rejects_different_active_runner_without_mutation() {
        std::thread::spawn(|| {
            let mut runner1 = AsyncRunner::new();
            runner1.bind_senders();

            let mut runner2 = AsyncRunner::new();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runner2.bind_senders();
            }));
            assert!(result.is_err());

            get_data_cmd_sender().execute(DataCommand::Subscribe(SubscribeCommand::Data(
                SubscribeCustomData {
                    client_id: Some(ClientId::from("TEST")),
                    venue: None,
                    data_type: DataType::new("test", None, None),
                    command_id: UUID4::new(),
                    ts_init: UnixNanos::default(),
                    correlation_id: None,
                    params: None,
                },
            )));

            assert!(runner1.channels.data_cmd_rx.try_recv().is_ok());
            assert!(runner2.channels.data_cmd_rx.try_recv().is_err());
        })
        .join()
        .unwrap();
    }

    #[tokio::test]
    async fn test_execution_event_order_submitted_batch_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let events = vec![
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        let batch = OrderSubmittedBatch::new(events);
        tx.try_send(ExecutionEvent::OrderSubmittedBatch(batch))
            .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::OrderSubmittedBatch(b) => {
                assert_eq!(b.len(), 2);
                assert_eq!(b.events[0].client_order_id, ClientOrderId::from("O-001"));
                assert_eq!(b.events[1].client_order_id, ClientOrderId::from("O-002"));
            }
            _ => panic!("Expected OrderSubmittedBatch event"),
        }
    }

    #[tokio::test]
    async fn test_execution_event_order_accepted_batch_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let events = vec![
            OrderAcceptedSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderAcceptedSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        let batch = OrderAcceptedBatch::new(events);
        tx.try_send(ExecutionEvent::OrderAcceptedBatch(batch))
            .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::OrderAcceptedBatch(b) => {
                assert_eq!(b.len(), 2);
                assert_eq!(b.events[0].client_order_id, ClientOrderId::from("O-001"));
                assert_eq!(b.events[1].client_order_id, ClientOrderId::from("O-002"));
            }
            _ => panic!("Expected OrderAcceptedBatch event"),
        }
    }

    #[tokio::test]
    async fn test_execution_event_order_canceled_batch_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(4096);

        let events = vec![
            OrderCanceledSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderCanceledSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        let batch = OrderCanceledBatch::new(events);
        tx.try_send(ExecutionEvent::OrderCanceledBatch(batch))
            .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::OrderCanceledBatch(b) => {
                assert_eq!(b.len(), 2);
                assert_eq!(b.events[0].client_order_id, ClientOrderId::from("O-001"));
                assert_eq!(b.events[1].client_order_id, ClientOrderId::from("O-002"));
            }
            _ => panic!("Expected OrderCanceledBatch event"),
        }
    }
}
