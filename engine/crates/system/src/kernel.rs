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

use std::{
    cell::{Cell, Ref, RefCell},
    fmt::Debug,
    rc::Rc,
    time::Duration,
};

use nautilus_common::{
    cache::{Cache, CacheConfig, database::CacheDatabaseAdapter},
    clock::Clock,
    component::Component,
    enums::{ComponentState, Environment},
    logging::{
        arm_shutdown_on_error, disarm_shutdown_on_error, headers, init_logging,
        logger::{LogGuard, LoggerConfig},
        try_drain_shutdown_on_error_trigger,
    },
    messages::system::ShutdownSystem,
    msgbus::{
        self, MessageBus, MessagingSwitchboard, ShareableMessageHandler, get_message_bus,
        set_message_bus,
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_data::engine::DataEngine;
use nautilus_execution::{
    engine::ExecutionEngine,
    order_emulator::{adapter::OrderEmulatorAdapter, emulator::OrderEmulator},
};
use nautilus_model::identifiers::{ClientId, TraderId};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_risk::engine::RiskEngine;
use ustr::Ustr;

use crate::{
    builder::NautilusKernelBuilder,
    clock_factory::ClockFactory,
    config::NautilusKernelConfig,
    event_store::{
        EventStoreFactory, EventStoreSealReceipt, KernelEventStore, RegisteredComponents,
    },
    trader::Trader,
};

/// Core Nautilus system kernel.
///
/// Orchestrates data and execution engines, cache, clock, and messaging across environments.
#[derive(Debug)]
pub struct NautilusKernel {
    /// The kernel name (for logging and identification).
    pub name: String,
    /// The unique instance identifier for this kernel.
    pub instance_id: UUID4,
    /// The machine identifier (hostname or similar).
    pub machine_id: String,
    /// The kernel configuration.
    pub config: Box<dyn NautilusKernelConfig>,
    /// The shared in-memory cache.
    pub cache: Rc<RefCell<Cache>>,
    /// The clock driving the kernel.
    pub clock: Rc<RefCell<dyn Clock>>,
    /// The portfolio manager.
    pub portfolio: Rc<RefCell<Portfolio>>,
    /// Guard for the logging subsystem (keeps logger thread alive).
    pub log_guard: LogGuard,
    /// The data engine instance.
    pub data_engine: Rc<RefCell<DataEngine>>,
    /// The risk engine instance.
    pub risk_engine: Rc<RefCell<RiskEngine>>,
    /// The execution engine instance.
    pub exec_engine: Rc<RefCell<ExecutionEngine>>,
    /// The order emulator for handling emulated orders.
    pub order_emulator: OrderEmulatorAdapter,
    /// The trader component (shared for [`Controller`](crate::controller::Controller) access).
    pub trader: Rc<RefCell<Trader>>,
    /// The UNIX timestamp (nanoseconds) when the kernel was created.
    pub ts_created: UnixNanos,
    /// The UNIX timestamp (nanoseconds) when the kernel was last started.
    pub ts_started: Option<UnixNanos>,
    /// The UNIX timestamp (nanoseconds) when the kernel was last shutdown.
    pub ts_shutdown: Option<UnixNanos>,
    shutdown_requested: Rc<Cell<bool>>,
    event_store: Option<Box<dyn KernelEventStore>>,
    event_store_replay: bool,
    state_save_armed: bool,
}

/// Optional construction-time dependencies for [`NautilusKernel`].
#[derive(Default)]
pub struct NautilusKernelDependencies {
    clock_factory: Option<ClockFactory>,
    cache_database: Option<Box<dyn CacheDatabaseAdapter>>,
    event_store_factory: Option<EventStoreFactory>,
}

impl Debug for NautilusKernelDependencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(NautilusKernelDependencies))
            .field("clock_factory", &self.clock_factory.is_some())
            .field("cache_database", &self.cache_database.is_some())
            .field("event_store_factory", &self.event_store_factory.is_some())
            .finish()
    }
}

impl NautilusKernelDependencies {
    /// Add a clock factory.
    #[must_use]
    pub fn with_clock_factory(mut self, clock_factory: Option<ClockFactory>) -> Self {
        self.clock_factory = clock_factory;
        self
    }

    /// Add a cache database adapter.
    #[must_use]
    pub fn with_cache_database(
        mut self,
        cache_database: Option<Box<dyn CacheDatabaseAdapter>>,
    ) -> Self {
        self.cache_database = cache_database;
        self
    }

    /// Add an event-store factory.
    #[must_use]
    pub fn with_event_store_factory(
        mut self,
        event_store_factory: Option<EventStoreFactory>,
    ) -> Self {
        self.event_store_factory = event_store_factory;
        self
    }
}

impl NautilusKernel {
    /// Create a new [`NautilusKernelBuilder`] for fluent configuration.
    #[must_use]
    pub const fn builder(
        name: String,
        trader_id: TraderId,
        environment: Environment,
    ) -> NautilusKernelBuilder {
        NautilusKernelBuilder::new(name, trader_id, environment)
    }

    /// Create a new [`NautilusKernel`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel fails to initialize.
    pub fn new<T: NautilusKernelConfig + 'static>(name: String, config: T) -> anyhow::Result<Self> {
        Self::new_with(name, config, None, None)
    }

    /// Create a new [`NautilusKernel`] instance with an injected cache database adapter.
    ///
    /// The adapter is passed straight to [`Cache::new`] so the kernel can restore
    /// generic cache state (including snapshot blobs anchored by the event store) from
    /// the durable backing store on startup, without an external caller pre-seeding the
    /// in-memory cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel fails to initialize.
    pub fn new_with_cache_database<T: NautilusKernelConfig + 'static>(
        name: String,
        config: T,
        cache_database: Option<Box<dyn CacheDatabaseAdapter>>,
    ) -> anyhow::Result<Self> {
        Self::new_with(name, config, cache_database, None)
    }

    /// Create a new [`NautilusKernel`] instance with optional cache database and event store
    /// injections.
    ///
    /// The cache adapter is passed to [`Cache::new`]; the event-store factory is invoked
    /// with the kernel's clock so the resulting [`KernelEventStore`] implementation shares
    /// the same time source the kernel uses to stamp `RunStarted`/`RunEnded` and any
    /// drop-seal fallback timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel fails to initialize or the event-store factory fails.
    pub fn new_with<T: NautilusKernelConfig + 'static>(
        name: String,
        config: T,
        cache_database: Option<Box<dyn CacheDatabaseAdapter>>,
        event_store_factory: Option<EventStoreFactory>,
    ) -> anyhow::Result<Self> {
        Self::new_with_dependencies(
            name,
            config,
            NautilusKernelDependencies::default()
                .with_cache_database(cache_database)
                .with_event_store_factory(event_store_factory),
        )
    }

    /// Create a new [`NautilusKernel`] instance with construction-time dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel fails to initialize or an injected factory fails.
    pub fn new_with_dependencies<T: NautilusKernelConfig + 'static>(
        name: String,
        config: T,
        dependencies: NautilusKernelDependencies,
    ) -> anyhow::Result<Self> {
        let NautilusKernelDependencies {
            clock_factory,
            cache_database,
            event_store_factory,
        } = dependencies;
        let instance_id = config.instance_id().unwrap_or_default();
        let machine_id = Self::determine_machine_id()?;

        let logger_config = config.logging();
        let log_guard = Self::initialize_logging(config.trader_id(), instance_id, logger_config)?;
        headers::log_header(
            config.trader_id(),
            &machine_id,
            instance_id,
            Ustr::from(&name),
        );

        log::info!("Building system kernel");

        let clock_factory =
            clock_factory.unwrap_or_else(|| ClockFactory::for_environment(config.environment()));
        let clock = clock_factory.clock();
        let event_store = match event_store_factory {
            Some(factory) => Some(factory(instance_id, clock.clone())?),
            None => None,
        };
        let cache = Self::initialize_cache(config.cache(), cache_database);

        let msgbus = Rc::new(RefCell::new(MessageBus::new(
            config.trader_id(),
            instance_id,
            Some(name.clone()),
            None,
        )));
        set_message_bus(msgbus);

        if let Some(config) = config.msgbus()
            && let Some(filter) = config.types_filter
        {
            get_message_bus().borrow_mut().set_types_filter(filter);
        }

        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            config.portfolio(),
        )));

        let risk_engine = RiskEngine::new(
            config.risk_engine().unwrap_or_default(),
            portfolio.borrow().clone_shallow(),
            clock.clone(),
            cache.clone(),
        );
        let risk_engine = Rc::new(RefCell::new(risk_engine));

        let exec_engine = ExecutionEngine::new(clock.clone(), cache.clone(), config.exec_engine());
        let exec_engine = Rc::new(RefCell::new(exec_engine));

        let order_emulator = OrderEmulatorAdapter::new(clock.clone(), cache.clone());

        let data_engine = DataEngine::new(clock.clone(), cache.clone(), config.data_engine());
        let data_engine = Rc::new(RefCell::new(data_engine));

        DataEngine::register_msgbus_handlers(&data_engine);
        RiskEngine::register_msgbus_handlers(&risk_engine);
        ExecutionEngine::register_msgbus_handlers(&exec_engine);
        OrderEmulator::register_msgbus_handlers(&order_emulator.emulator());

        let shutdown_requested = Rc::new(Cell::new(false));
        Self::register_shutdown_handler(config.trader_id(), shutdown_requested.clone());

        let trader = Rc::new(RefCell::new(Trader::new(
            config.trader_id(),
            instance_id,
            config.environment(),
            clock_factory,
            cache.clone(),
            portfolio.clone(),
        )));

        let ts_created = clock.borrow().timestamp_ns();

        Ok(Self {
            name,
            instance_id,
            machine_id,
            event_store,
            config: Box::new(config),
            cache,
            clock,
            portfolio,
            log_guard,
            data_engine,
            risk_engine,
            exec_engine,
            order_emulator,
            trader,
            ts_created,
            ts_started: None,
            ts_shutdown: None,
            shutdown_requested,
            event_store_replay: false,
            state_save_armed: false,
        })
    }

    fn register_shutdown_handler(trader_id: TraderId, shutdown_requested: Rc<Cell<bool>>) {
        let handler = ShareableMessageHandler::from_typed(move |cmd: &ShutdownSystem| {
            if cmd.trader_id != trader_id {
                log::warn!("Received {cmd} not for this trader {trader_id}, ignoring");
                return;
            }

            if shutdown_requested.get() {
                log::debug!("Shutdown already requested, ignoring {cmd}");
                return;
            }

            log::info!("Received {cmd}, requesting shutdown");
            shutdown_requested.set(true);
        });
        let topic = MessagingSwitchboard::shutdown_system_topic();
        msgbus::subscribe_any(topic.into(), handler, None);
    }

    fn determine_machine_id() -> anyhow::Result<String> {
        sysinfo::System::host_name().ok_or_else(|| anyhow::anyhow!("Failed to determine hostname"))
    }

    fn initialize_logging(
        trader_id: TraderId,
        instance_id: UUID4,
        config: LoggerConfig,
    ) -> anyhow::Result<LogGuard> {
        #[cfg(feature = "tracing-bridge")]
        let use_tracing = config.use_tracing;

        let file_config = config.file_config.clone().unwrap_or_default();
        let log_guard = match init_logging(trader_id, instance_id, config, file_config) {
            Ok(guard) => guard,
            Err(e) => {
                // Only recover from SetLoggerError (logger already registered).
                // This is common in tests where multiple kernels are created and
                // the log crate's global logger persists after LogGuard teardown.
                // Any other error (e.g. thread spawn failure) is propagated.
                if e.downcast_ref::<log::SetLoggerError>().is_some() {
                    if let Some(guard) = LogGuard::new() {
                        guard
                    } else {
                        return Err(e.context(
                            "A non-Nautilus logger is already registered; \
                             cannot initialize Nautilus logging",
                        ));
                    }
                } else {
                    return Err(e);
                }
            }
        };

        // Initialize tracing subscriber if enabled (idempotent)
        #[cfg(feature = "tracing-bridge")]
        if use_tracing && !nautilus_common::logging::bridge::tracing_is_initialized() {
            nautilus_common::logging::bridge::init_tracing()?;
        }

        Ok(log_guard)
    }

    fn initialize_cache(
        cache_config: Option<CacheConfig>,
        cache_database: Option<Box<dyn CacheDatabaseAdapter>>,
    ) -> Rc<RefCell<Cache>> {
        let cache_config = cache_config.unwrap_or_default();
        let cache = Cache::new(Some(cache_config), cache_database);

        Rc::new(RefCell::new(cache))
    }

    fn cancel_timers(&self) {
        self.clock.borrow_mut().cancel_timers();
    }

    #[must_use]
    pub fn generate_timestamp_ns(&self) -> UnixNanos {
        self.clock.borrow().timestamp_ns()
    }

    /// Returns the kernel's environment context (Backtest, Sandbox, Live).
    #[must_use]
    pub fn environment(&self) -> Environment {
        self.config.environment()
    }

    /// Returns the kernel's name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the kernel's trader ID.
    #[must_use]
    pub fn trader_id(&self) -> TraderId {
        self.config.trader_id()
    }

    /// Returns the kernel's machine ID.
    #[must_use]
    pub fn machine_id(&self) -> &str {
        &self.machine_id
    }

    /// Returns the kernel's instance ID.
    #[must_use]
    pub const fn instance_id(&self) -> UUID4 {
        self.instance_id
    }

    /// Returns the delay after stopping the node to await residual events before final shutdown.
    #[must_use]
    pub fn delay_post_stop(&self) -> Duration {
        self.config.delay_post_stop()
    }

    /// Returns the UNIX timestamp (ns) when the kernel was created.
    #[must_use]
    pub const fn ts_created(&self) -> UnixNanos {
        self.ts_created
    }

    /// Returns the UNIX timestamp (ns) when the kernel was last started.
    #[must_use]
    pub const fn ts_started(&self) -> Option<UnixNanos> {
        self.ts_started
    }

    /// Returns the UNIX timestamp (ns) when the kernel was last shutdown.
    #[must_use]
    pub const fn ts_shutdown(&self) -> Option<UnixNanos> {
        self.ts_shutdown
    }

    /// Returns `true` if shutdown has been requested.
    ///
    /// Drains pending shutdown-on-error logs before checking the kernel flag.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.drain_shutdown_on_error_trigger();

        if self
            .event_store
            .as_deref()
            .is_some_and(KernelEventStore::is_halted)
            && !self.shutdown_requested.get()
        {
            log::error!("Event store fail-stopped; requesting terminal kernel shutdown");
            self.shutdown_requested.set(true);
        }

        self.shutdown_requested.get()
    }

    /// Returns whether the kernel's event store has entered its terminal fail-stop state.
    #[must_use]
    pub fn is_event_store_halted(&self) -> bool {
        self.event_store
            .as_deref()
            .is_some_and(KernelEventStore::is_halted)
    }

    /// Clears the shutdown flag.
    ///
    /// Call this before starting a fresh run so a prior `ShutdownSystem`
    /// command does not abort it.
    pub fn reset_shutdown_flag(&self) {
        self.shutdown_requested.set(false);
    }

    /// Returns a shared handle to the shutdown flag for async runtimes
    /// that need to poll it outside the kernel's direct borrow.
    #[must_use]
    pub fn shutdown_flag(&self) -> Rc<Cell<bool>> {
        self.shutdown_requested.clone()
    }

    /// Halts risk admission and requests terminal shutdown after an infrastructure fail-stop.
    ///
    /// This path deliberately does not depend on the event-store admission barrier: queue
    /// overflow can be caused by that barrier being unavailable or too slow. The live runtime
    /// follows the request by disconnecting every client before disposal. Without an event store
    /// this only guarantees a process-local Halted state; strict live configurations must reject
    /// construction unless a durable event store is installed.
    pub fn fail_stop_live_ingress(&mut self, reason: &str) {
        {
            let mut risk_engine = self.risk_engine.borrow_mut();
            risk_engine.force_halt_for_terminal_failure(reason);
        }

        if self.event_store.is_some() {
            let checkpoint_error = self
                .risk_engine
                .borrow_mut()
                .try_checkpoint_risk_state()
                .err();
            if let Some(checkpoint_error) = checkpoint_error.as_ref() {
                log::error!(
                    "Failed to checkpoint terminal Halted risk policy: {checkpoint_error}; poisoning current event-store run"
                );
            }

            // Poison unconditionally. A successful message-bus admission does not prove an
            // authoritative event-store tap was attached, and a clean Ended seal could otherwise
            // preserve an older Active checkpoint across restart.
            let poison_reason = checkpoint_error.map_or_else(
                || reason.to_string(),
                |error| format!("{reason}; Halted checkpoint failed: {error}"),
            );
            if let Some(event_store) = self.event_store.as_deref_mut()
                && let Err(error) = event_store.fail_stop(&poison_reason)
            {
                log::error!("Failed to latch terminal event-store failure: {error}");
            }
        } else {
            log::warn!(
                "Live ingress fail-stop has no configured event store; Halted is process-local and cannot be recovered after restart"
            );
        }
        self.shutdown_requested.set(true);
    }

    fn drain_shutdown_on_error_trigger(&self) {
        try_drain_shutdown_on_error_trigger(|trigger| {
            let command = ShutdownSystem::new(
                self.config.trader_id(),
                trigger.component,
                Some(format!(
                    "Error log received from {}: {}",
                    trigger.component, trigger.message
                )),
                UUID4::new(),
                trigger.timestamp,
                None,
            );

            msgbus::try_publish_any(
                MessagingSwitchboard::shutdown_system_topic(),
                command.as_any(),
            )
        });
    }

    /// Returns whether the kernel has been configured to load state.
    #[must_use]
    pub fn load_state(&self) -> bool {
        self.config.load_state()
    }

    /// Returns whether the kernel has been configured to save state.
    #[must_use]
    pub fn save_state(&self) -> bool {
        self.config.save_state()
    }

    /// Returns the kernel's clock.
    #[must_use]
    pub fn clock(&self) -> Rc<RefCell<dyn Clock>> {
        self.clock.clone()
    }

    /// Returns the kernel's cache.
    #[must_use]
    pub fn cache(&self) -> Rc<RefCell<Cache>> {
        self.cache.clone()
    }

    /// Returns the kernel's portfolio.
    #[must_use]
    pub fn portfolio(&self) -> Ref<'_, Portfolio> {
        self.portfolio.borrow()
    }

    /// Returns the kernel's data engine.
    #[must_use]
    pub fn data_engine(&self) -> Ref<'_, DataEngine> {
        self.data_engine.borrow()
    }

    /// Returns the kernel's risk engine.
    #[must_use]
    pub const fn risk_engine(&self) -> &Rc<RefCell<RiskEngine>> {
        &self.risk_engine
    }

    /// Returns the kernel's execution engine.
    #[must_use]
    pub const fn exec_engine(&self) -> &Rc<RefCell<ExecutionEngine>> {
        &self.exec_engine
    }

    /// Returns the kernel's trader (shared reference).
    #[must_use]
    pub fn trader(&self) -> &Rc<RefCell<Trader>> {
        &self.trader
    }

    fn verify_strategy_configurations_for_start(&self) -> bool {
        if let Err(e) = self
            .trader
            .borrow()
            .ensure_artifact_configurations_current()
        {
            log::error!("Strategy configuration failed kernel start-boundary verification: {e:#}");
            self.shutdown_requested.set(true);
            return false;
        }
        true
    }

    /// Starts the Nautilus system kernel synchronously (for backtest use).
    pub fn start(&mut self) {
        arm_shutdown_on_error(self.config.shutdown_on_error());
        log::info!("Starting");

        self.event_store_replay = false;

        if !self.verify_strategy_configurations_for_start() {
            return;
        }

        if let Some(event_store) = self.event_store.as_deref_mut() {
            self.exec_engine.borrow_mut().set_snapshot_anchorer(None);

            let components = Self::collect_registered_components(&self.trader);
            let environment = self.config.environment();
            let event_store_replay_configured = event_store.is_event_store_replay_configured();

            if event_store_replay_configured && !self.config.load_state() {
                log::error!("Event-store replay requires load_state=true");
                self.shutdown_requested.set(true);
                return;
            }

            let risk_recovery = match event_store.restore_parent_risk_state(self.instance_id) {
                Ok(risk_recovery) => risk_recovery,
                Err(e) => {
                    log::error!("Failed to restore risk policy from event-store source: {e}");
                    self.shutdown_requested.set(true);
                    return;
                }
            };
            if let Some(recovery) = risk_recovery {
                if recovery.force_halted {
                    self.risk_engine
                        .borrow_mut()
                        .force_halt_for_incomplete_recovery(
                            "event-store source did not have a verified clean terminal outcome",
                        );
                }
                if let Some(checkpoint) = recovery.checkpoint.as_ref()
                    && let Err(e) = self.risk_engine.borrow_mut().restore_risk_state(checkpoint)
                {
                    log::error!("Failed to apply restored event-store risk policy: {e}");
                    self.shutdown_requested.set(true);
                    return;
                }
            }

            if self.config.load_state()
                && let Err(e) =
                    event_store.restore_parent_cache(self.instance_id, &mut self.cache.borrow_mut())
            {
                log::error!("Failed to restore cache from event-store replay source: {e}");
                self.shutdown_requested.set(true);
                return;
            }

            if let Err(e) = event_store.open(self.instance_id, &components, environment) {
                log::error!("Failed to open event-store run: {e}");
                self.shutdown_requested.set(true);
                return;
            }

            if event_store.is_halted() {
                log::error!("Event store halted during startup");
                self.shutdown_requested.set(true);
                return;
            }

            if let Err(e) = self.risk_engine.borrow_mut().try_checkpoint_risk_state() {
                log::error!("Failed to durably checkpoint startup risk policy: {e}");
                self.shutdown_requested.set(true);
                return;
            }

            if event_store.is_halted() {
                log::error!("Event store halted while checkpointing startup risk policy");
                self.shutdown_requested.set(true);
                return;
            }

            let anchorer = event_store.snapshot_anchorer();
            self.exec_engine
                .borrow_mut()
                .set_snapshot_anchorer(anchorer);
            self.event_store_replay = event_store_replay_configured;
        }

        if self.event_store_replay {
            log::info!(
                "Event-store replay loaded; skipping engines, clients, trader startup, and live reconciliation",
            );
            self.ts_started = Some(self.clock.borrow().timestamp_ns());
            log::info!("Started");
            return;
        }

        self.start_engines();

        log::info!("Initializing trader");
        if let Err(e) = self.trader.borrow_mut().initialize() {
            log::error!("Error initializing trader: {e:?}");
            return;
        }

        // Execution and data clients are started by their engines via `start_engines` above

        self.ts_started = Some(self.clock.borrow().timestamp_ns());
        log::info!("Started");
    }

    fn collect_registered_components(trader: &Rc<RefCell<Trader>>) -> RegisteredComponents {
        let trader = trader.borrow();
        let mut actors = trader
            .actor_ids()
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        let mut strategies = trader
            .strategy_ids()
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        let mut algorithms = trader
            .exec_algorithm_ids()
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        actors.sort_unstable();
        strategies.sort_unstable();
        algorithms.sort_unstable();
        actors.dedup();
        strategies.dedup();
        algorithms.dedup();
        RegisteredComponents {
            actors,
            strategies,
            algorithms,
        }
    }

    /// Starts the Nautilus system kernel asynchronously.
    #[expect(
        clippy::unused_async,
        reason = "keeps the public async kernel API shape stable"
    )]
    pub async fn start_async(&mut self) {
        self.start();
    }

    /// Starts the trader (strategies and actors).
    ///
    /// This should be called after clients are connected and instruments are cached.
    ///
    /// # Errors
    ///
    /// Returns an error if the trader or a registered component fails to start. A failed partial
    /// start is stopped immediately before the error is returned.
    pub fn start_trader(&mut self) -> anyhow::Result<()> {
        log::info!("Starting trader...");

        if self.config.load_state() {
            Trader::load_state(&self.trader)
                .map_err(|e| anyhow::anyhow!("Failed to load actor and strategy state: {e:#}"))?;
        }

        self.state_save_armed = self.config.save_state();
        self.order_emulator.start();

        if let Err(start_err) = Trader::start_with_component_callbacks(&self.trader) {
            let stop_result = self.stop_trader_after_start_failure();
            self.order_emulator.stop();
            let save_result = self.save_trader_state();

            let mut errors = vec![format!("Failed to start trader: {start_err}")];
            if let Err(e) = stop_result {
                errors.push(format!("failed to stop partial trader start: {e}"));
            }

            if let Err(e) = save_result {
                errors.push(format!("failed to save partial trader state: {e}"));
            }
            anyhow::bail!("{}", errors.join("; "));
        }

        log::info!("Trader started");
        Ok(())
    }

    /// Stops the trader and its registered components.
    ///
    /// This method initiates a graceful shutdown of trading components (strategies, actors)
    /// which may trigger residual events such as order cancellations. The caller should
    /// continue processing events after calling this method to handle these residual events.
    pub fn stop_trader(&mut self) {
        disarm_shutdown_on_error();

        if !self.trader.borrow().is_running() {
            return;
        }

        log::info!("Stopping trader...");

        if let Err(e) = self.trader.borrow_mut().stop() {
            log::error!("Error stopping trader: {e}");
        }
    }

    /// Stops a partially started trader without deferring managed strategy shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if any active trader component cannot be stopped.
    pub fn stop_trader_after_start_failure(&mut self) -> anyhow::Result<()> {
        disarm_shutdown_on_error();

        if !matches!(
            self.trader.borrow().state(),
            ComponentState::Starting | ComponentState::Running
        ) {
            return Ok(());
        }

        log::info!("Stopping trader immediately...");
        self.trader.borrow_mut().stop_after_start_failure()
    }

    /// Finalizes the kernel shutdown after the grace period.
    ///
    /// This method should be called after the residual events grace period has elapsed
    /// and all remaining events have been processed. It disconnects clients and stops engines.
    ///
    /// # Errors
    ///
    /// Returns an error if actor or strategy state cannot be saved.
    #[allow(unknown_lints)]
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "keeps the public async kernel API shape stable"
    )]
    pub async fn finalize_stop(&mut self) -> anyhow::Result<()> {
        disarm_shutdown_on_error();

        // Execution and data clients are stopped by their engines via `stop_engines` below

        let save_result = self.save_trader_state();
        self.portfolio.borrow_mut().finalize_equity_curve();
        self.stop_engines();
        self.cancel_timers();
        let artifact_config_result = self
            .trader
            .borrow()
            .ensure_artifact_configurations_current()
            .map_err(|error| {
                anyhow::anyhow!(
                    "Strategy configuration failed kernel shutdown-boundary verification: {error:#}"
                )
            });

        let ts_shutdown = self.clock.borrow().timestamp_ns();

        let seal_result = self.seal_event_store(ts_shutdown);
        self.ts_shutdown = Some(ts_shutdown);
        log::info!("Stopped");
        let shutdown_result = match (save_result, artifact_config_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(save_error), Ok(())) => Err(save_error),
            (Ok(()), Err(config_error)) => Err(config_error),
            (Err(save_error), Err(config_error)) => Err(anyhow::anyhow!(
                "trader state save failed: {save_error:#}; strategy configuration verification failed: {config_error:#}"
            )),
        };
        match (shutdown_result, seal_result) {
            (Ok(()), Ok(_)) => Ok(()),
            (Err(shutdown_error), Ok(_)) => Err(shutdown_error),
            (Ok(()), Err(seal_error)) => Err(seal_error),
            (Err(shutdown_error), Err(seal_error)) => Err(anyhow::anyhow!(
                "kernel shutdown failed: {shutdown_error:#}; event-store seal failed: {seal_error:#}"
            )),
        }
    }

    /// Seals and verifies the current event-store run before result evidence is issued.
    ///
    /// Backtests call this after all state-affecting stop/save work and before building their
    /// promotion-capable result. Live shutdown reaches the same seam through
    /// [`Self::finalize_stop`].
    ///
    /// # Errors
    ///
    /// Returns an error when the event store cannot cleanly close or verify its sealed run.
    pub fn seal_event_store(
        &mut self,
        ts_shutdown: UnixNanos,
    ) -> anyhow::Result<Option<EventStoreSealReceipt>> {
        let Some(event_store) = self.event_store.as_deref_mut() else {
            return Ok(None);
        };
        self.exec_engine.borrow_mut().set_snapshot_anchorer(None);
        event_store.seal(ts_shutdown)
    }

    /// Saves actor and strategy state at most once for the current trader run.
    ///
    /// # Errors
    ///
    /// Returns an error if a component callback or cache persistence operation fails.
    pub fn save_trader_state(&mut self) -> anyhow::Result<()> {
        if !std::mem::take(&mut self.state_save_armed) {
            return Ok(());
        }

        Trader::save_state(&self.trader)
    }

    /// Returns the kernel-managed event-store integration, when one was injected.
    ///
    /// Callers wire an implementation through
    /// [`NautilusKernelBuilder::with_event_store`](crate::builder::NautilusKernelBuilder::with_event_store);
    /// without an injected adapter this returns `None`.
    #[must_use]
    pub fn event_store(&self) -> Option<&dyn KernelEventStore> {
        self.event_store.as_deref()
    }

    /// Replaces the provenance committed by the next event-store `RunStarted` header.
    ///
    /// # Errors
    ///
    /// Returns an error when the injected event store is already open or terminally halted.
    pub fn set_event_store_run_provenance(
        &mut self,
        provenance: nautilus_provenance::RunProvenance,
    ) -> anyhow::Result<()> {
        if let Some(event_store) = self.event_store.as_deref_mut() {
            event_store.set_run_provenance(provenance)?;
        }
        Ok(())
    }

    /// Returns whether the event-store integration is running an event-store replay start.
    #[must_use]
    pub fn is_event_store_replay(&self) -> bool {
        self.event_store_replay
    }

    /// Returns whether the event-store integration is configured for an event-store replay start.
    #[must_use]
    pub fn is_event_store_replay_configured(&self) -> bool {
        self.event_store
            .as_deref()
            .is_some_and(KernelEventStore::is_event_store_replay_configured)
    }

    /// Resets the Nautilus system kernel to its initial state.
    pub fn reset(&mut self) {
        disarm_shutdown_on_error();
        log::info!("Resetting");

        if let Err(e) = self.trader.borrow_mut().reset() {
            log::error!("Error resetting trader: {e:?}");
        }

        self.data_engine.borrow_mut().reset();
        self.exec_engine.borrow_mut().reset();
        self.risk_engine.borrow_mut().reset();
        self.order_emulator.reset();
        self.portfolio.borrow_mut().reset();

        self.ts_started = None;
        self.ts_shutdown = None;
        self.state_save_armed = false;

        log::info!("Reset");
    }

    /// Disposes of the Nautilus system kernel, releasing resources.
    pub fn dispose(&mut self) {
        disarm_shutdown_on_error();
        log::info!("Disposing");

        let trader_state = self.trader.borrow().state();
        match trader_state {
            ComponentState::Running => self.stop_trader(),
            ComponentState::Starting => {
                if let Err(e) = self.stop_trader_after_start_failure() {
                    log::error!("Error stopping partial trader start during disposal: {e:?}");
                }
            }
            _ => {}
        }

        if let Err(e) = self.save_trader_state() {
            log::error!("Error saving trader state during disposal: {e:?}");
        }

        {
            let mut trader = self.trader.borrow_mut();
            if trader.state() == ComponentState::PreInitialized
                && let Err(e) = trader.initialize()
            {
                log::error!("Error initializing trader for disposal: {e:?}");
            }

            if !trader.is_disposed()
                && let Err(e) = trader.dispose()
            {
                log::error!("Error disposing trader: {e:?}");
            }
        }

        self.stop_engines();
        self.portfolio.borrow_mut().reset();
        self.cancel_timers();

        // Normal backtest/live finalization consumes the session first; this is a defensive
        // fallback for callers which dispose a partially initialized kernel.
        if let Some(event_store) = self.event_store.as_deref_mut() {
            self.exec_engine.borrow_mut().set_snapshot_anchorer(None);
            let ts_dispose = self.clock.borrow().timestamp_ns();
            if let Err(error) = event_store.seal(ts_dispose) {
                log::error!("Error sealing event store during kernel disposal: {error:#}");
            }
        }

        self.data_engine.borrow_mut().dispose();
        self.exec_engine.borrow_mut().dispose();
        self.risk_engine.borrow_mut().dispose();
        self.order_emulator.dispose();
        self.cache.borrow_mut().dispose();
        get_message_bus().borrow_mut().dispose();

        log::info!("Disposed");
    }

    /// Starts all engine components.
    fn start_engines(&self) {
        self.data_engine.borrow_mut().start();
        self.exec_engine.borrow_mut().start();
        self.risk_engine.borrow_mut().start();
    }

    /// Stops all engine components.
    fn stop_engines(&self) {
        self.data_engine.borrow_mut().stop();
        self.exec_engine.borrow_mut().stop();
        self.risk_engine.borrow_mut().stop();
        self.order_emulator.stop();
    }

    /// Connects data engine clients.
    ///
    /// Data clients are connected first so that instruments are published
    /// and can be drained into the cache before execution clients connect.
    #[expect(clippy::await_holding_refcell_ref)] // Single-threaded runtime, intentional design
    pub async fn connect_data_clients(&mut self) {
        log::info!("Connecting data clients...");
        self.data_engine.borrow_mut().connect().await;
    }

    /// Connects execution engine clients.
    ///
    /// Must be called after data clients are connected and instrument events
    /// have been drained into the cache, so execution clients can load instruments.
    #[expect(clippy::await_holding_refcell_ref)] // Single-threaded runtime, intentional design
    pub async fn connect_exec_clients(&mut self) {
        log::info!("Connecting execution clients...");
        self.exec_engine.borrow_mut().connect().await;
    }

    /// Disconnects all engine clients.
    ///
    /// # Errors
    ///
    /// Returns an error if any client fails to disconnect.
    #[expect(clippy::await_holding_refcell_ref)] // Single-threaded runtime, intentional design
    pub async fn disconnect_clients(&mut self) -> anyhow::Result<()> {
        log::info!("Disconnecting clients...");
        let mut data_engine = self.data_engine.borrow_mut();
        let mut exec_engine = self.exec_engine.borrow_mut();
        let (data_result, exec_result) =
            futures::join!(data_engine.disconnect(), exec_engine.disconnect());

        match (data_result, exec_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(data_err), Ok(())) => Err(data_err),
            (Ok(()), Err(exec_err)) => Err(exec_err),
            (Err(data_err), Err(exec_err)) => anyhow::bail!(
                "Failed to disconnect data clients: {data_err}; failed to disconnect execution \
                 clients: {exec_err}"
            ),
        }
    }

    /// Returns `true` if all engine clients are connected.
    #[must_use]
    pub fn check_engines_connected(&self) -> bool {
        self.data_engine.borrow().check_connected() && self.exec_engine.borrow().check_connected()
    }

    /// Returns `true` if all engine clients are disconnected.
    #[must_use]
    pub fn check_engines_disconnected(&self) -> bool {
        self.data_engine.borrow().check_disconnected()
            && self.exec_engine.borrow().check_disconnected()
    }

    /// Returns connection status for all data clients.
    #[must_use]
    pub fn data_client_connection_status(&self) -> Vec<(ClientId, bool)> {
        self.data_engine.borrow().client_connection_status()
    }

    /// Returns connection status for all execution clients.
    #[must_use]
    pub fn exec_client_connection_status(&self) -> Vec<(ClientId, bool)> {
        self.exec_engine.borrow().client_connection_status()
    }
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use nautilus_common::messages::system::ShutdownSystem;
    use nautilus_core::UUID4;
    use rstest::*;
    use ustr::Ustr;

    use super::*;
    use crate::builder::NautilusKernelBuilder;

    #[rstest]
    fn test_shutdown_system_sets_kernel_flag() {
        let kernel = NautilusKernelBuilder::default().build().unwrap();
        assert!(!kernel.is_shutdown_requested());

        let command = ShutdownSystem::new(
            kernel.trader_id(),
            Ustr::from("TestComponent"),
            Some("unit test".to_string()),
            UUID4::new(),
            kernel.generate_timestamp_ns(),
            None, // correlation_id
        );

        msgbus::publish_any(
            MessagingSwitchboard::shutdown_system_topic(),
            command.as_any(),
        );
        assert!(kernel.is_shutdown_requested());

        kernel.reset_shutdown_flag();
        assert!(!kernel.is_shutdown_requested());
    }

    #[rstest]
    fn test_shutdown_system_idempotent() {
        let kernel = NautilusKernelBuilder::default().build().unwrap();

        let make_cmd = || {
            ShutdownSystem::new(
                kernel.trader_id(),
                Ustr::from("TestComponent"),
                None,
                UUID4::new(),
                kernel.generate_timestamp_ns(),
                None, // correlation_id
            )
        };

        let topic = MessagingSwitchboard::shutdown_system_topic();
        msgbus::publish_any(topic, make_cmd().as_any());
        assert!(kernel.is_shutdown_requested());

        msgbus::publish_any(topic, make_cmd().as_any());
        assert!(kernel.is_shutdown_requested());

        kernel.reset_shutdown_flag();
        assert!(!kernel.is_shutdown_requested());

        msgbus::publish_any(topic, make_cmd().as_any());
        assert!(kernel.is_shutdown_requested());
    }

    #[rstest]
    fn test_shutdown_system_ignores_other_trader() {
        let kernel = NautilusKernelBuilder::default().build().unwrap();

        let command = ShutdownSystem::new(
            TraderId::from("OTHER-TRADER"),
            Ustr::from("TestComponent"),
            None,
            UUID4::new(),
            kernel.generate_timestamp_ns(),
            None, // correlation_id
        );

        msgbus::publish_any(
            MessagingSwitchboard::shutdown_system_topic(),
            command.as_any(),
        );
        assert!(!kernel.is_shutdown_requested());
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use futures::FutureExt;
    use indexmap::IndexMap;
    use nautilus_common::{
        actor::{
            DataActor,
            registry::{get_actor_unchecked, try_get_actor_unchecked},
        },
        cache::Cache,
        messages::{
            data::{DataCommand, SubscribeCommand, UnsubscribeCommand},
            execution::{SubmitOrder, TradingCommand},
            system::{RiskMaxNotional, RiskStateChangeReason, RiskStateChanged},
        },
        msgbus::stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
    };
    use nautilus_execution::engine::SnapshotAnchorer;
    use nautilus_model::{
        enums::{OrderSide, OrderStatus, OrderType, TradingState, TriggerType},
        events::OrderEventAny,
        identifiers::{ActorId, ClientOrderId, ComponentId, InstrumentId, StrategyId, TraderId},
        instruments::{
            CryptoPerpetual, Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt,
        },
        orders::{Order, OrderAny, OrderTestBuilder},
        types::{Price, Quantity},
    };
    use nautilus_risk::{RiskMutationError, engine::config::RiskEngineConfig};
    use nautilus_testkit::{
        cache::TestCacheDatabaseControl,
        components::{StateActor, StateStrategy},
    };
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{config::StrategyConfig, core::StrategyCore},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::{
        builder::NautilusKernelBuilder,
        event_store::{KernelEventStore, RegisteredComponents, RiskStateRecovery},
        trader::{ArtifactConfiguredStrategy, ArtifactConfiguredStrategyInstance},
    };

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    struct KernelMutableArtifactConfig {
        strategy_id: StrategyId,
        max_order_quantity: Cell<u64>,
        mutate_on_stop: bool,
    }

    #[derive(Debug)]
    struct KernelMutableArtifactStrategy {
        core: StrategyCore,
        artifact_config: KernelMutableArtifactConfig,
    }

    impl DataActor for KernelMutableArtifactStrategy {
        fn on_stop(&mut self) -> anyhow::Result<()> {
            if self.artifact_config.mutate_on_stop {
                self.artifact_config.max_order_quantity.set(u64::MAX);
            }
            Ok(())
        }
    }

    nautilus_strategy!(KernelMutableArtifactStrategy);

    impl ArtifactConfiguredStrategy for KernelMutableArtifactStrategy {
        type ArtifactConfig = KernelMutableArtifactConfig;

        fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            Ok(Self {
                core: StrategyCore::new(StrategyConfig {
                    strategy_id: Some(config.strategy_id),
                    ..Default::default()
                }),
                artifact_config: config,
            })
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            &self.artifact_config
        }
    }

    #[derive(Debug)]
    struct RecordingEventStore {
        control: TestCacheDatabaseControl,
        opened: bool,
    }

    impl KernelEventStore for RecordingEventStore {
        fn set_run_provenance(
            &mut self,
            _provenance: nautilus_provenance::RunProvenance,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn restore_parent_cache(
            &mut self,
            _instance_id: UUID4,
            _cache: &mut Cache,
        ) -> anyhow::Result<()> {
            self.control.record("event_store.restore");
            Ok(())
        }

        fn open(
            &mut self,
            _instance_id: UUID4,
            _components: &RegisteredComponents,
            _environment: Environment,
        ) -> anyhow::Result<()> {
            self.control.record("event_store.open");
            self.opened = true;
            Ok(())
        }

        fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer> {
            None
        }

        fn fail_stop(&mut self, _reason: &str) -> anyhow::Result<()> {
            self.control.record("event_store.fail_stop");
            self.opened = false;
            Ok(())
        }

        fn seal(&mut self, _ts_init: UnixNanos) -> anyhow::Result<Option<EventStoreSealReceipt>> {
            if self.opened {
                self.control.record("event_store.seal");
                self.opened = false;
            }
            Ok(None)
        }

        fn run_id(&self) -> Option<&str> {
            None
        }

        fn parent_run_id(&self) -> Option<&str> {
            None
        }

        fn is_halted(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct ExternallyHaltedEventStore {
        halted: Rc<Cell<bool>>,
    }

    #[derive(Debug)]
    struct RestartAwareEventStore {
        predecessor_poisoned: Rc<Cell<bool>>,
        recover_predecessor: bool,
        recovery_checkpoint: Option<RiskStateChanged>,
        opened: bool,
    }

    impl KernelEventStore for RestartAwareEventStore {
        fn set_run_provenance(
            &mut self,
            _provenance: nautilus_provenance::RunProvenance,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn restore_parent_cache(
            &mut self,
            _instance_id: UUID4,
            _cache: &mut Cache,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn restore_parent_risk_state(
            &mut self,
            _instance_id: UUID4,
        ) -> anyhow::Result<Option<RiskStateRecovery>> {
            Ok(
                (self.recover_predecessor && self.predecessor_poisoned.get()).then_some(
                    RiskStateRecovery {
                        checkpoint: self.recovery_checkpoint.clone(),
                        force_halted: true,
                    },
                ),
            )
        }

        fn open(
            &mut self,
            _instance_id: UUID4,
            _components: &RegisteredComponents,
            _environment: Environment,
        ) -> anyhow::Result<()> {
            self.opened = true;
            Ok(())
        }

        fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer> {
            None
        }

        fn fail_stop(&mut self, _reason: &str) -> anyhow::Result<()> {
            self.predecessor_poisoned.set(true);
            self.opened = false;
            Ok(())
        }

        fn seal(&mut self, _ts_init: UnixNanos) -> anyhow::Result<Option<EventStoreSealReceipt>> {
            self.opened = false;
            Ok(None)
        }

        fn run_id(&self) -> Option<&str> {
            None
        }

        fn parent_run_id(&self) -> Option<&str> {
            None
        }

        fn is_halted(&self) -> bool {
            false
        }
    }

    impl KernelEventStore for ExternallyHaltedEventStore {
        fn set_run_provenance(
            &mut self,
            _provenance: nautilus_provenance::RunProvenance,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn restore_parent_cache(
            &mut self,
            _instance_id: UUID4,
            _cache: &mut Cache,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn open(
            &mut self,
            _instance_id: UUID4,
            _components: &RegisteredComponents,
            _environment: Environment,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer> {
            None
        }

        fn seal(&mut self, _ts_init: UnixNanos) -> anyhow::Result<Option<EventStoreSealReceipt>> {
            Ok(None)
        }

        fn run_id(&self) -> Option<&str> {
            None
        }

        fn parent_run_id(&self) -> Option<&str> {
            None
        }

        fn is_halted(&self) -> bool {
            self.halted.get()
        }
    }

    #[rstest]
    fn test_kernel_start_rejects_changed_artifact_configuration_before_initialization() {
        let mut kernel = NautilusKernelBuilder::default().build().unwrap();
        let strategy_id = StrategyId::from("KernelMutableArtifact-001");
        let content = serde_json::to_vec(&KernelMutableArtifactConfig {
            strategy_id,
            max_order_quantity: Cell::new(10),
            mutate_on_stop: false,
        })
        .unwrap();
        let instance =
            ArtifactConfiguredStrategyInstance::<KernelMutableArtifactStrategy>::decode(&content)
                .unwrap();
        kernel
            .trader
            .borrow_mut()
            .add_artifact_configured_strategy(instance)
            .unwrap();
        let registered =
            try_get_actor_unchecked::<KernelMutableArtifactStrategy>(&strategy_id.inner()).unwrap();
        registered.artifact_config().max_order_quantity.set(11);
        drop(registered);

        kernel.start();

        assert!(kernel.is_shutdown_requested());
        assert_eq!(
            kernel.trader.borrow().state(),
            ComponentState::PreInitialized
        );
        kernel.trader.borrow_mut().clear_strategies().unwrap();
    }

    #[rstest]
    fn test_kernel_shutdown_rejects_artifact_configuration_mutated_during_on_stop() {
        let mut kernel = NautilusKernelBuilder::default().build().unwrap();
        let strategy_id = StrategyId::from("KernelMutableOnStop-001");
        let content = serde_json::to_vec(&KernelMutableArtifactConfig {
            strategy_id,
            max_order_quantity: Cell::new(10),
            mutate_on_stop: true,
        })
        .unwrap();
        let instance =
            ArtifactConfiguredStrategyInstance::<KernelMutableArtifactStrategy>::decode(&content)
                .unwrap();
        kernel
            .trader
            .borrow_mut()
            .add_artifact_configured_strategy(instance)
            .unwrap();

        kernel.start();
        assert!(!kernel.is_shutdown_requested());
        kernel.start_trader().unwrap();
        kernel.stop_trader();
        let error = finalize(&mut kernel).unwrap_err();

        assert!(
            error.to_string().contains("shutdown-boundary verification"),
            "shutdown must reject the mutated retained configuration: {error:#}"
        );
        kernel.trader.borrow_mut().clear_strategies().unwrap();
    }

    #[rstest]
    fn test_event_store_halt_promotes_terminal_kernel_shutdown() {
        let halted = Rc::new(Cell::new(false));
        let event_store_halted = Rc::clone(&halted);
        let kernel = NautilusKernelBuilder::default()
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(ExternallyHaltedEventStore {
                    halted: event_store_halted,
                }))
            })
            .build()
            .unwrap();

        assert!(!kernel.is_event_store_halted());
        assert!(!kernel.is_shutdown_requested());

        halted.set(true);

        assert!(kernel.is_event_store_halted());
        assert!(kernel.is_shutdown_requested());
        assert!(kernel.shutdown_flag().get());

        kernel.reset_shutdown_flag();
        assert!(!kernel.shutdown_flag().get());
        assert!(kernel.is_shutdown_requested());
        assert!(kernel.shutdown_flag().get());
    }

    fn state(key: &str, value: &[u8]) -> IndexMap<String, Vec<u8>> {
        IndexMap::from([(key.to_string(), value.to_vec())])
    }

    fn finalize(kernel: &mut NautilusKernel) -> anyhow::Result<()> {
        kernel
            .finalize_stop()
            .now_or_never()
            .expect("kernel finalization must not yield")
    }

    fn add_state_components(
        kernel: &NautilusKernel,
        control: &TestCacheDatabaseControl,
        actor: StateActor,
        strategy: StateStrategy,
    ) {
        kernel.trader.borrow_mut().add_actor(actor).unwrap();
        kernel.trader.borrow_mut().add_strategy(strategy).unwrap();
        control.record("components.registered");
    }

    fn create_stop_market_order(instrument: &CryptoPerpetual, client_order_id: &str) -> OrderAny {
        OrderTestBuilder::new(OrderType::StopMarket)
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::from(client_order_id))
            .side(OrderSide::Buy)
            .trigger_price(Price::from("5100.00"))
            .quantity(Quantity::from(1))
            .emulation_trigger(TriggerType::BidAsk)
            .build()
    }

    fn register_data_command_handler(id: &str) -> TypedIntoMessageSavingHandler<DataCommand> {
        let (handler, saving_handler) =
            get_typed_into_message_saving_handler::<DataCommand>(Some(Ustr::from(id)));
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            handler,
        );
        saving_handler
    }

    #[rstest]
    fn test_state_persistence_orders_restore_load_start_stop_save_seal_and_dispose() {
        let actor_id = ActorId::from("STATE-ACTOR");
        let strategy_id = StrategyId::from("STATE-STRATEGY-001");
        let actor_load = state("actor-loaded", b"actor-load-value");
        let strategy_load = state("strategy-loaded", b"strategy-load-value");
        let actor_save = state("actor-saved", b"actor-save-value");
        let strategy_save = state("strategy-saved", b"strategy-save-value");
        let (database, control) = TestCacheDatabaseControl::create();
        control.set_actor_state(ComponentId::from(actor_id.as_str()), &actor_load);
        control.set_strategy_state(strategy_id, &strategy_load);

        let event_store_control = control.clone();
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(RecordingEventStore {
                    control: event_store_control,
                    opened: false,
                }))
            })
            .build()
            .unwrap();

        let actor = StateActor::new(actor_id, control.clone(), actor_save.clone());
        let strategy = StateStrategy::new(strategy_id, control.clone(), strategy_save.clone());
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        kernel.start_trader().unwrap();

        let actor_state = get_actor_unchecked::<StateActor>(&actor_id.inner())
            .state_load()
            .cloned();
        let strategy_state = get_actor_unchecked::<StateStrategy>(&strategy_id.inner())
            .state_load()
            .cloned();
        assert_eq!(actor_state, Some(actor_load));
        assert_eq!(strategy_state, Some(strategy_load));

        kernel.stop_trader();
        kernel.stop_trader();
        finalize(&mut kernel).unwrap();
        finalize(&mut kernel).unwrap();
        kernel.dispose();

        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "event_store.restore",
                "event_store.open",
                "actor.load:STATE-ACTOR",
                "actor.on_load",
                "strategy.load:STATE-STRATEGY-001",
                "strategy.on_load",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:STATE-ACTOR",
                "strategy.on_save",
                "strategy.update:STATE-STRATEGY-001",
                "event_store.seal",
                "database.close",
            ]
        );
        assert_eq!(
            control.actor_state(&ComponentId::from(actor_id.as_str())),
            Some(actor_save)
        );
        assert_eq!(control.strategy_state(&strategy_id), Some(strategy_save));
    }

    #[rstest]
    fn test_live_ingress_fail_stop_poisons_store_even_when_bus_admission_returns_ok() {
        let control = TestCacheDatabaseControl::default();
        let event_store_control = control.clone();
        let mut kernel = NautilusKernelBuilder::default()
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(RecordingEventStore {
                    control: event_store_control,
                    opened: false,
                }))
            })
            .build()
            .expect("kernel");
        kernel.start();

        kernel.fail_stop_live_ingress("data queue full");

        assert_eq!(
            kernel.risk_engine.borrow().trading_state(),
            TradingState::Halted
        );
        assert!(kernel.is_shutdown_requested());
        assert!(
            control
                .events()
                .contains(&"event_store.fail_stop".to_string())
        );
    }

    #[rstest]
    fn test_live_ingress_overflow_restart_forces_halted_and_bypass_still_denies() {
        let predecessor_poisoned = Rc::new(Cell::new(false));
        let first_state = Rc::clone(&predecessor_poisoned);
        let mut first_kernel = NautilusKernelBuilder::default()
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(RestartAwareEventStore {
                    predecessor_poisoned: first_state,
                    recover_predecessor: false,
                    recovery_checkpoint: None,
                    opened: false,
                }))
            })
            .build()
            .expect("first kernel");
        first_kernel.start();

        first_kernel.fail_stop_live_ingress("data queue full");

        assert!(predecessor_poisoned.get());
        assert_eq!(
            first_kernel.risk_engine.borrow().trading_state(),
            TradingState::Halted
        );
        assert!(
            first_kernel
                .risk_engine
                .borrow()
                .is_terminal_failure_latched()
        );
        assert_eq!(
            first_kernel
                .risk_engine
                .borrow_mut()
                .try_set_trading_state(TradingState::Active)
                .unwrap_err(),
            RiskMutationError::TerminalFailureLatched
        );
        assert_eq!(
            first_kernel
                .risk_engine
                .borrow_mut()
                .try_reset()
                .unwrap_err(),
            RiskMutationError::TerminalFailureLatched
        );
        drop(first_kernel);

        let second_state = Rc::clone(&predecessor_poisoned);
        let mut restarted = NautilusKernelBuilder::default()
            .with_risk_engine_config(RiskEngineConfig {
                bypass: true,
                ..RiskEngineConfig::default()
            })
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(RestartAwareEventStore {
                    predecessor_poisoned: second_state,
                    recover_predecessor: true,
                    recovery_checkpoint: None,
                    opened: false,
                }))
            })
            .build()
            .expect("restarted kernel");
        restarted.start();

        assert_eq!(
            restarted.risk_engine.borrow().trading_state(),
            TradingState::Halted
        );
        assert!(
            restarted
                .risk_engine
                .borrow()
                .is_incomplete_recovery_interlocked()
        );
        assert_eq!(
            restarted
                .risk_engine
                .borrow_mut()
                .try_set_trading_state(TradingState::Active)
                .unwrap_err(),
            RiskMutationError::IncompleteRecoveryInterlock
        );
        assert_eq!(
            restarted.risk_engine.borrow_mut().try_reset().unwrap_err(),
            RiskMutationError::IncompleteRecoveryInterlock
        );

        let (handler, denied) = get_typed_into_message_saving_handler::<OrderEventAny>(Some(
            Ustr::from("ExecEngine.process.restart_halt"),
        ));
        msgbus::register_order_event_endpoint(MessagingSwitchboard::exec_engine_process(), handler);
        let instrument = crypto_perpetual_ethusdt();
        let client_id = ClientId::from("OKX");
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::from("O-RESTART-HALTED"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .build();
        {
            let mut cache = restarted.cache.borrow_mut();
            cache
                .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
                .expect("cache instrument");
            cache
                .add_order(order.clone(), None, Some(client_id), false)
                .expect("cache order");
        }
        let submit = SubmitOrder::new(
            restarted.trader_id(),
            Some(client_id),
            StrategyId::from("RESTART-HALT-001"),
            order.instrument_id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            restarted.generate_timestamp_ns(),
            None,
        );

        restarted
            .risk_engine
            .borrow_mut()
            .execute(TradingCommand::SubmitOrder(submit));

        let denied = denied.get_messages();
        assert_eq!(denied.len(), 1);
        assert_eq!(denied[0].message(), Some(Ustr::from("TRADING_HALTED")));
    }

    #[rstest]
    fn test_recovered_checkpoint_restores_limits_but_cannot_rearm_active() {
        let trader_id = TraderId::from("RECOVERY-001");
        let instrument_id = InstrumentId::from("BTC/USDT.OKX");
        let checkpoint = RiskStateChanged {
            trader_id,
            state: TradingState::Active,
            max_notional_per_order: vec![RiskMaxNotional {
                instrument_id: instrument_id.to_string(),
                value: "75000".to_string(),
            }],
            reason: RiskStateChangeReason::MaxNotionalChanged,
            event_id: UUID4::new(),
            ts_event: UnixNanos::from(1),
            ts_init: UnixNanos::from(1),
        };
        let predecessor_poisoned = Rc::new(Cell::new(true));
        let predecessor_state = Rc::clone(&predecessor_poisoned);
        let mut restarted = NautilusKernelBuilder::new(
            "RecoveryInterlockKernel".to_string(),
            trader_id,
            Environment::Backtest,
        )
        .with_event_store(move |_instance_id, _clock| {
            Ok(Box::new(RestartAwareEventStore {
                predecessor_poisoned: predecessor_state,
                recover_predecessor: true,
                recovery_checkpoint: Some(checkpoint),
                opened: false,
            }))
        })
        .build()
        .expect("restarted kernel with checkpoint");

        restarted.start();

        let mut risk_engine = restarted.risk_engine.borrow_mut();
        assert_eq!(risk_engine.trading_state(), TradingState::Halted);
        assert!(risk_engine.is_incomplete_recovery_interlocked());
        assert_eq!(
            risk_engine
                .max_notional_per_order()
                .get(&instrument_id)
                .map(ToString::to_string),
            Some("75000".to_string())
        );
        assert_eq!(
            risk_engine
                .try_set_trading_state(TradingState::Active)
                .unwrap_err(),
            RiskMutationError::IncompleteRecoveryInterlock
        );
        assert_eq!(
            risk_engine.try_reset().unwrap_err(),
            RiskMutationError::IncompleteRecoveryInterlock
        );
    }

    #[rstest]
    fn test_state_persistence_skips_callbacks_without_cache_backing() {
        let actor_id = ActorId::from("NO-BACKING-ACTOR");
        let strategy_id = StrategyId::from("NO-BACKING-STRATEGY-001");
        let control = TestCacheDatabaseControl::default();
        let mut kernel = NautilusKernelBuilder::default().build().unwrap();
        let actor = StateActor::new(actor_id, control.clone(), state("actor", b"save"));
        let strategy = StateStrategy::new(strategy_id, control.clone(), state("strategy", b"save"));
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        kernel.start_trader().unwrap();
        kernel.stop_trader();
        finalize(&mut kernel).unwrap();
        kernel.dispose();

        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
            ]
        );
    }

    #[rstest]
    fn test_state_persistence_skips_empty_load_and_persists_empty_save() {
        let actor_id = ActorId::from("EMPTY-STATE-ACTOR");
        let strategy_id = StrategyId::from("EMPTY-STATE-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .build()
            .unwrap();
        let actor = StateActor::new(actor_id, control.clone(), IndexMap::new());
        let strategy = StateStrategy::new(strategy_id, control.clone(), IndexMap::new());
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        kernel.start_trader().unwrap();
        kernel.stop_trader();
        finalize(&mut kernel).unwrap();

        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "actor.load:EMPTY-STATE-ACTOR",
                "strategy.load:EMPTY-STATE-STRATEGY-001",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:EMPTY-STATE-ACTOR",
                "strategy.on_save",
                "strategy.update:EMPTY-STATE-STRATEGY-001",
            ]
        );
        assert_eq!(
            control.actor_state(&ComponentId::from(actor_id.as_str())),
            Some(IndexMap::new())
        );
        assert_eq!(control.strategy_state(&strategy_id), Some(IndexMap::new()));
        kernel.dispose();
    }

    #[rstest]
    fn test_state_save_reports_all_callback_errors_and_continues_shutdown() {
        let actor_id = ActorId::from("FAIL-SAVE-ACTOR");
        let strategy_id = StrategyId::from("FAIL-SAVE-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        let event_store_control = control.clone();
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(RecordingEventStore {
                    control: event_store_control,
                    opened: false,
                }))
            })
            .build()
            .unwrap();
        let actor = StateActor::new(actor_id, control.clone(), IndexMap::new()).with_fail_save();
        let strategy =
            StateStrategy::new(strategy_id, control.clone(), IndexMap::new()).with_fail_save();
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        kernel.start_trader().unwrap();
        kernel.stop_trader();
        let expected_shutdown = kernel.clock.borrow().timestamp_ns();
        let error = finalize(&mut kernel).unwrap_err();
        kernel.dispose();

        assert_eq!(
            error.to_string(),
            "Failed to save component state: actor FAIL-SAVE-ACTOR callback: test actor on_save \
             failure; strategy FAIL-SAVE-STRATEGY-001 callback: test strategy on_save failure"
        );
        assert_eq!(kernel.ts_shutdown, Some(expected_shutdown));
        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "event_store.restore",
                "event_store.open",
                "actor.load:FAIL-SAVE-ACTOR",
                "strategy.load:FAIL-SAVE-STRATEGY-001",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "strategy.on_save",
                "event_store.seal",
                "database.close",
            ]
        );
    }

    #[rstest]
    fn test_state_load_callback_failure_prevents_start_and_save() {
        let actor_id = ActorId::from("FAIL-LOAD-ACTOR");
        let strategy_id = StrategyId::from("FAIL-LOAD-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        control.set_actor_state(
            ComponentId::from(actor_id.as_str()),
            &state("actor", b"load"),
        );
        control.set_strategy_state(strategy_id, &state("strategy", b"load"));
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .build()
            .unwrap();
        let actor = StateActor::new(actor_id, control.clone(), IndexMap::new()).with_fail_load();
        let strategy = StateStrategy::new(strategy_id, control.clone(), IndexMap::new());
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        let error = kernel.start_trader().unwrap_err();
        kernel.dispose();

        assert_eq!(
            error.to_string(),
            "Failed to load actor and strategy state: Failed to restore actor FAIL-LOAD-ACTOR \
             state: test actor on_load failure"
        );
        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "actor.load:FAIL-LOAD-ACTOR",
                "actor.on_load",
                "database.close",
            ]
        );
    }

    #[rstest]
    fn test_state_save_reports_all_persistence_errors() {
        let actor_id = ActorId::from("FAIL-UPDATE-ACTOR");
        let strategy_id = StrategyId::from("FAIL-UPDATE-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        control.set_fail_update_actor(true);
        control.set_fail_update_strategy(true);
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .build()
            .unwrap();
        let actor = StateActor::new(actor_id, control.clone(), state("actor", b"save"));
        let strategy = StateStrategy::new(strategy_id, control.clone(), state("strategy", b"save"));
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        kernel.start_trader().unwrap();
        kernel.stop_trader();
        let error = finalize(&mut kernel).unwrap_err();
        kernel.dispose();

        assert_eq!(
            error.to_string(),
            "Failed to save component state: actor FAIL-UPDATE-ACTOR persistence: test actor \
             update failure; strategy FAIL-UPDATE-STRATEGY-001 persistence: test strategy update \
             failure"
        );
        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "actor.load:FAIL-UPDATE-ACTOR",
                "strategy.load:FAIL-UPDATE-STRATEGY-001",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:FAIL-UPDATE-ACTOR",
                "strategy.on_save",
                "strategy.update:FAIL-UPDATE-STRATEGY-001",
                "database.close",
            ]
        );
    }

    #[rstest]
    fn test_partial_startup_stops_and_saves_once() {
        let actor_id = ActorId::from("PARTIAL-ACTOR");
        let strategy_id = StrategyId::from("PARTIAL-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .build()
            .unwrap();
        let actor = StateActor::new(actor_id, control.clone(), state("actor", b"partial"));
        let strategy =
            StateStrategy::new(strategy_id, control.clone(), state("strategy", b"partial"))
                .with_fail_start();
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        let error = kernel.start_trader().unwrap_err();
        kernel.dispose();

        assert_eq!(
            error.to_string(),
            "Failed to start trader: test strategy on_start failure"
        );
        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "actor.load:PARTIAL-ACTOR",
                "strategy.load:PARTIAL-STRATEGY-001",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:PARTIAL-ACTOR",
                "strategy.on_save",
                "strategy.update:PARTIAL-STRATEGY-001",
                "database.close",
            ]
        );
        assert_eq!(
            control.actor_state(&ComponentId::from(actor_id.as_str())),
            Some(state("actor", b"partial"))
        );
        assert_eq!(
            control.strategy_state(&strategy_id),
            Some(state("strategy", b"partial"))
        );
    }

    #[rstest]
    fn test_forced_dispose_stops_and_saves_once() {
        let actor_id = ActorId::from("FORCED-ACTOR");
        let strategy_id = StrategyId::from("FORCED-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        let mut kernel = NautilusKernelBuilder::default()
            .with_cache_database(Box::new(database))
            .build()
            .unwrap();
        let actor = StateActor::new(actor_id, control.clone(), state("actor", b"forced"));
        let strategy =
            StateStrategy::new(strategy_id, control.clone(), state("strategy", b"forced"));
        add_state_components(&kernel, &control, actor, strategy);

        kernel.start();
        kernel.start_trader().unwrap();
        kernel.dispose();

        assert_eq!(
            control.events(),
            vec![
                "components.registered",
                "actor.load:FORCED-ACTOR",
                "strategy.load:FORCED-STRATEGY-001",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:FORCED-ACTOR",
                "strategy.on_save",
                "strategy.update:FORCED-STRATEGY-001",
                "database.close",
            ]
        );
    }

    #[rstest]
    fn test_start_trader_starts_order_emulator_for_cached_emulated_orders() {
        let mut kernel = NautilusKernelBuilder::default().build().unwrap();
        let data_commands = register_data_command_handler("DataEngine.queue_execute.kernel_start");
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let first_order = create_stop_market_order(&instrument, "O-KERNEL-001");
        let second_order = create_stop_market_order(&instrument, "O-KERNEL-002");
        let first_client_order_id = first_order.client_order_id();
        let second_client_order_id = second_order.client_order_id();
        kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
        kernel
            .cache
            .borrow_mut()
            .add_order(first_order, None, None, false)
            .unwrap();
        kernel
            .cache
            .borrow_mut()
            .add_order(second_order, None, None, false)
            .unwrap();

        kernel.start();
        assert!(
            kernel
                .order_emulator
                .get_emulator()
                .get_matching_core(&instrument_id)
                .is_none()
        );
        kernel.start_trader().unwrap();

        let commands = data_commands.get_messages();
        let cache = kernel.cache.borrow();
        let first_status = cache.order(&first_client_order_id).unwrap().status();
        let second_status = cache.order(&second_client_order_id).unwrap().status();
        drop(cache);
        let emulator = kernel.order_emulator.get_emulator();
        assert!(emulator.get_matching_core(&instrument_id).is_some());
        assert_eq!(emulator.subscribed_quotes(), vec![instrument_id]);
        assert_eq!(first_status, OrderStatus::Emulated);
        assert_eq!(second_status, OrderStatus::Emulated);
        assert!(commands.iter().any(|command| matches!(
            command,
            DataCommand::Subscribe(SubscribeCommand::Quotes(command))
                if command.instrument_id == instrument_id
        )));

        data_commands.clear();
        drop(emulator);
        kernel.stop_trader();
        kernel.dispose();

        let commands = data_commands.get_messages();
        let emulator = kernel.order_emulator.get_emulator();
        assert!(emulator.subscribed_quotes().is_empty());
        assert!(emulator.get_matching_core(&instrument_id).is_none());
        assert!(commands.iter().any(|command| matches!(
            command,
            DataCommand::Unsubscribe(UnsubscribeCommand::Quotes(command))
                if command.instrument_id == instrument_id
        )));
    }

    #[rstest]
    fn test_reset_resets_order_emulator_state() {
        let mut kernel = NautilusKernelBuilder::default().build().unwrap();
        let data_commands = register_data_command_handler("DataEngine.queue_execute.kernel_reset");
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let order = create_stop_market_order(&instrument, "O-KERNEL-RESET-001");
        kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
        kernel
            .cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();

        kernel.start();
        kernel.start_trader().unwrap();
        assert!(
            kernel
                .order_emulator
                .get_emulator()
                .get_matching_core(&instrument_id)
                .is_some()
        );
        kernel.stop_trader();
        data_commands.clear();

        kernel.reset();

        let commands = data_commands.get_messages();
        let emulator = kernel.order_emulator.get_emulator();
        assert!(emulator.subscribed_quotes().is_empty());
        assert!(emulator.get_matching_core(&instrument_id).is_none());
        assert!(commands.iter().any(|command| matches!(
            command,
            DataCommand::Unsubscribe(UnsubscribeCommand::Quotes(command))
                if command.instrument_id == instrument_id
        )));

        drop(emulator);
        kernel.dispose();
    }
}
