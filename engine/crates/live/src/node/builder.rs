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

//! Builder for constructing [`LiveNode`] instances.

use std::{cell::RefCell, collections::HashMap, fmt::Debug, rc::Rc, time::Duration};

use nautilus_common::{
    cache::CacheConfig,
    clients::ExecutionClient,
    clock::Clock,
    enums::Environment,
    factories::{
        ClientConfig, DataClientFactory, ExecutionClientFactory, SimulatedExecutionClientFactory,
    },
    logging::{install_panic_logging_hook, logger::LoggerConfig},
    msgbus::{
        BusMessage, MessageBusBackingFactory, MessageBusConfig, MessageBusExternalEgress,
        MessageBusExternalIngress, external_egress_from_backing, external_io_from_backing,
    },
};
use nautilus_core::UUID4;
use nautilus_data::client::DataClientAdapter;
use nautilus_event_store::{EventStoreLifecycle, EventStoreLifecycleOptions, IsolatedVerifier};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_model::identifiers::{TraderId, Venue};
#[cfg(feature = "artifact-trust")]
use nautilus_plugin::{ArtifactDigest, TrustPolicy, VerifiedStrategyArtifact};
use nautilus_portfolio::config::PortfolioConfig;
#[cfg(feature = "python")]
use nautilus_system::trader::Trader;
use nautilus_system::{
    clock_factory::ClockFactory,
    config::StreamingConfig,
    event_store::{EventStoreConfig, EventStoreFactory, KernelEventStore},
    kernel::{NautilusKernel, NautilusKernelDependencies},
};
use nautilus_trading::ImportableControllerConfig;

#[cfg(not(feature = "artifact-trust"))]
use super::validate_strategy_artifact_feature_support;
use super::{
    LiveNode,
    config::{
        LiveDataEngineConfig, LiveExecEngineConfig, LiveNodeConfig, LiveRiskEngineConfig,
        RoutingConfig, validate_live_environment,
    },
};
use crate::{
    execution::{
        client::LiveExecutionClient,
        manager::{ExecutionManager, ExecutionManagerConfig},
    },
    runner::AsyncRunner,
};

#[derive(Debug)]
enum ExecutionClientFactoryEntry {
    Adapter(Box<dyn ExecutionClientFactory>),
    Simulated(Box<dyn SimulatedExecutionClientFactory>),
}

pub(crate) struct ExternalMessageBusIngress(Box<dyn MessageBusExternalIngress>);

impl Debug for ExternalMessageBusIngress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ExternalMessageBusIngress))
            .finish_non_exhaustive()
    }
}

/// Builder for constructing a [`LiveNode`] with a fluent API.
///
/// Provides configuration options specific to live nodes, including client factory
/// registration, timeout settings, and optional event-store injection for run-lifecycle
/// audit and replay (see [`Self::with_event_store`]).
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.live", unsendable)
)]
pub struct LiveNodeBuilder {
    name: String,
    config: LiveNodeConfig,
    data_client_factories: HashMap<String, Box<dyn DataClientFactory>>,
    exec_client_factories: HashMap<String, ExecutionClientFactoryEntry>,
    data_client_configs: HashMap<String, Box<dyn ClientConfig>>,
    exec_client_configs: HashMap<String, Box<dyn ClientConfig>>,
    data_client_routing: HashMap<String, RoutingConfig>,
    exec_client_routing: HashMap<String, RoutingConfig>,
    event_store_factory: Option<EventStoreFactory>,
    clock_factory: Option<ClockFactory>,
    external_msgbus_factory: Option<Box<dyn MessageBusBackingFactory>>,
    external_msgbus_egress: Option<Box<dyn MessageBusExternalEgress>>,
    external_msgbus_ingress: Option<ExternalMessageBusIngress>,
    strategy_artifact_admission_required: bool,
    #[cfg(feature = "artifact-trust")]
    admitted_strategy_artifacts: Vec<VerifiedStrategyArtifact>,
    #[cfg(feature = "artifact-trust")]
    strategy_artifact_trust_policy: Option<TrustPolicy>,
    #[cfg(feature = "artifact-trust")]
    strategy_artifact_expected_policy_commitment: Option<ArtifactDigest>,
}

impl Debug for LiveNodeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(LiveNodeBuilder))
            .field("name", &self.name)
            .field("config", &self.config)
            .field("data_client_factories", &self.data_client_factories.keys())
            .field("exec_client_factories", &self.exec_client_factories.keys())
            .field("data_client_configs", &self.data_client_configs.keys())
            .field("exec_client_configs", &self.exec_client_configs.keys())
            .field("event_store_factory", &self.event_store_factory.is_some())
            .field("clock_factory", &self.clock_factory.is_some())
            .field(
                "external_msgbus_factory",
                &self.external_msgbus_factory.is_some(),
            )
            .field(
                "external_msgbus_egress",
                &self.external_msgbus_egress.is_some(),
            )
            .field(
                "external_msgbus_ingress",
                &self.external_msgbus_ingress.is_some(),
            )
            .field(
                "strategy_artifact_admission_required",
                &self.strategy_artifact_admission_required,
            )
            .finish_non_exhaustive()
    }
}

impl LiveNodeBuilder {
    /// Creates a new [`LiveNodeBuilder`] with required parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if `environment` is invalid (BACKTEST).
    pub fn new(trader_id: TraderId, environment: Environment) -> anyhow::Result<Self> {
        validate_live_environment(environment)?;

        let config = LiveNodeConfig {
            environment,
            trader_id,
            ..Default::default()
        };
        let strategy_artifact_admission_required = config.strategy_artifact_admission_required;

        Ok(Self {
            name: "LiveNode".to_string(),
            config,
            data_client_factories: HashMap::new(),
            exec_client_factories: HashMap::new(),
            data_client_configs: HashMap::new(),
            exec_client_configs: HashMap::new(),
            data_client_routing: HashMap::new(),
            exec_client_routing: HashMap::new(),
            event_store_factory: None,
            clock_factory: None,
            external_msgbus_factory: None,
            external_msgbus_egress: None,
            external_msgbus_ingress: None,
            strategy_artifact_admission_required,
            #[cfg(feature = "artifact-trust")]
            admitted_strategy_artifacts: Vec::new(),
            #[cfg(feature = "artifact-trust")]
            strategy_artifact_trust_policy: None,
            #[cfg(feature = "artifact-trust")]
            strategy_artifact_expected_policy_commitment: None,
        })
    }

    /// Creates a new [`LiveNodeBuilder`] from an existing [`LiveNodeConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error if the config's environment is invalid (BACKTEST).
    pub fn from_config(config: LiveNodeConfig) -> anyhow::Result<Self> {
        validate_live_environment(config.environment)?;
        let strategy_artifact_admission_required = config.strategy_artifact_admission_required;

        Ok(Self {
            name: "LiveNode".to_string(),
            config,
            data_client_factories: HashMap::new(),
            exec_client_factories: HashMap::new(),
            data_client_configs: HashMap::new(),
            exec_client_configs: HashMap::new(),
            data_client_routing: HashMap::new(),
            exec_client_routing: HashMap::new(),
            event_store_factory: None,
            clock_factory: None,
            external_msgbus_factory: None,
            external_msgbus_egress: None,
            external_msgbus_ingress: None,
            strategy_artifact_admission_required,
            #[cfg(feature = "artifact-trust")]
            admitted_strategy_artifacts: Vec::new(),
            #[cfg(feature = "artifact-trust")]
            strategy_artifact_trust_policy: None,
            #[cfg(feature = "artifact-trust")]
            strategy_artifact_expected_policy_commitment: None,
        })
    }

    /// Returns the name for the node.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set the name for the node.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the instance ID for the node.
    #[must_use]
    pub const fn with_instance_id(mut self, instance_id: UUID4) -> Self {
        self.config.instance_id = Some(instance_id);
        self
    }

    /// Configure whether to load state on startup.
    #[must_use]
    pub const fn with_load_state(mut self, load_state: bool) -> Self {
        self.config.load_state = load_state;
        self
    }

    /// Configure whether to save state on shutdown.
    #[must_use]
    pub const fn with_save_state(mut self, save_state: bool) -> Self {
        self.config.save_state = save_state;
        self
    }

    /// Set the importable controller configuration for the node.
    ///
    /// The controller is instantiated and registered with the trader during
    /// [`LiveNodeBuilder::build`], enabling runtime strategy/actor management
    /// (create, start, stop, remove) without restarting the node. This mirrors
    /// the `controller` field on [`LiveNodeConfig`] used by the config-based
    /// [`LiveNode::build`] path, so a builder that also registers client
    /// factories can host a controller in a single node.
    #[must_use]
    pub fn with_controller(mut self, controller: ImportableControllerConfig) -> Self {
        self.config.controller = Some(controller);
        self
    }

    /// Set the connection timeout in seconds.
    #[must_use]
    pub const fn with_timeout_connection(mut self, timeout_secs: u64) -> Self {
        self.config.timeout_connection = Duration::from_secs(timeout_secs);
        self
    }

    /// Set the reconciliation timeout in seconds.
    #[must_use]
    pub const fn with_timeout_reconciliation(mut self, timeout_secs: u64) -> Self {
        self.config.timeout_reconciliation = Duration::from_secs(timeout_secs);
        self
    }

    /// Configure whether to run startup reconciliation.
    #[must_use]
    pub fn with_reconciliation(mut self, reconciliation: bool) -> Self {
        self.config.exec_engine.reconciliation = reconciliation;
        self
    }

    /// Set the reconciliation lookback in minutes.
    #[must_use]
    pub fn with_reconciliation_lookback_mins(mut self, mins: u32) -> Self {
        self.config.exec_engine.reconciliation_lookback_mins = Some(mins);
        self
    }

    /// Set the portfolio initialization timeout in seconds.
    #[must_use]
    pub const fn with_timeout_portfolio(mut self, timeout_secs: u64) -> Self {
        self.config.timeout_portfolio = Duration::from_secs(timeout_secs);
        self
    }

    /// Set the disconnection timeout in seconds.
    #[must_use]
    pub const fn with_timeout_disconnection_secs(mut self, timeout_secs: u64) -> Self {
        self.config.timeout_disconnection = Duration::from_secs(timeout_secs);
        self
    }

    /// Set the post-stop delay in seconds.
    #[must_use]
    pub const fn with_delay_post_stop_secs(mut self, delay_secs: u64) -> Self {
        self.config.delay_post_stop = Duration::from_secs(delay_secs);
        self
    }

    /// Set the shutdown timeout in seconds.
    #[must_use]
    pub const fn with_delay_shutdown_secs(mut self, delay_secs: u64) -> Self {
        self.config.timeout_shutdown = Duration::from_secs(delay_secs);
        self
    }

    /// Inject a caller-supplied clock factory for the kernel and component clocks.
    #[must_use]
    pub fn with_clock_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn() -> Rc<RefCell<dyn Clock>> + 'static,
    {
        self.clock_factory = Some(ClockFactory::new(factory));
        self
    }

    /// Set the cache configuration.
    #[must_use]
    pub fn with_cache_config(mut self, config: CacheConfig) -> Self {
        self.config.cache = Some(config);
        self
    }

    /// Set the message bus configuration.
    ///
    /// External streams are consumed when an ingress implementation is injected with
    /// [`Self::with_external_ingress`] or built from [`Self::with_external_msgbus_factory`].
    #[must_use]
    pub fn with_msgbus_config(mut self, config: MessageBusConfig) -> Self {
        self.config.msgbus = Some(config);
        self
    }

    /// Set the portfolio configuration.
    #[must_use]
    pub fn with_portfolio_config(mut self, config: PortfolioConfig) -> Self {
        self.config.portfolio = Some(config);
        self
    }

    /// Set the streaming configuration.
    ///
    /// The Rust live runtime does not support this setting yet.
    /// `build()` returns an error when it is set.
    #[must_use]
    pub fn with_streaming_config(mut self, config: StreamingConfig) -> Self {
        self.config.streaming = Some(config);
        self
    }

    /// Set the data engine configuration.
    ///
    /// `qsize` is the hard capacity of the data-event, data-command, and timer lanes.
    #[must_use]
    pub fn with_data_engine_config(mut self, config: LiveDataEngineConfig) -> Self {
        self.config.data_engine = config;
        self
    }

    /// Set the risk engine configuration.
    ///
    /// `qsize` is the hard capacity of the strategy/risk command lane.
    #[must_use]
    pub fn with_risk_engine_config(mut self, config: LiveRiskEngineConfig) -> Self {
        self.config.risk_engine = config;
        self
    }

    /// Set the execution engine configuration.
    ///
    /// `qsize` is the hard capacity of the venue execution-event lane.
    #[must_use]
    pub fn with_exec_engine_config(mut self, config: LiveExecEngineConfig) -> Self {
        self.config.exec_engine = config;
        self
    }

    /// Inject an event-store implementation to drive run-lifecycle capture.
    ///
    /// The factory receives the kernel's instance id and clock so the returned
    /// `KernelEventStore` shares the same time source the kernel uses to stamp run
    /// lifecycle entries. The concrete implementation lives outside this crate;
    /// callers typically build it from
    /// [`LiveNodeConfig::event_store`](crate::config::LiveNodeConfig::event_store)
    /// inside the closure.
    #[must_use]
    pub fn with_event_store<F>(mut self, factory: F) -> Self
    where
        F: FnOnce(UUID4, Rc<RefCell<dyn Clock>>) -> anyhow::Result<Box<dyn KernelEventStore>>
            + 'static,
    {
        self.event_store_factory = Some(Box::new(factory));
        self
    }

    /// Installs the canonical Redb lifecycle with an explicitly measured isolated verifier and
    /// enables strict durable ingress fail-stop.
    ///
    /// Unlike [`Self::with_event_store`], this is the first-class production path: lifecycle
    /// configuration and process-local verifier policy are wired as one unit. The helper is never
    /// discovered from `PATH` or environment variables.
    #[must_use]
    pub fn with_canonical_event_store(
        mut self,
        config: EventStoreConfig,
        verifier: IsolatedVerifier,
    ) -> Self {
        let lifecycle_config = config.clone();
        let options = EventStoreLifecycleOptions::new().with_isolated_verifier(verifier);
        self.config.event_store = Some(config);
        self.config.require_durable_ingress_fail_stop = true;
        self.event_store_factory = Some(Box::new(move |instance_id, clock| {
            let lifecycle = EventStoreLifecycle::boot_with_options(
                Some(lifecycle_config),
                instance_id,
                clock,
                options,
            )?;
            Ok(Box::new(lifecycle))
        }));
        self
    }

    /// Requires every strategy registered with this node to have verified release evidence.
    ///
    /// The requirement is installed before controller construction, so native, Python, and
    /// runtime-created strategies all pass through the same fail-closed trader gate.
    /// `minimum_policy_epoch` must come from externally persisted, monotonic trusted state; the
    /// live node enforces this floor but does not persist it.
    #[must_use]
    pub fn require_strategy_artifact_admission(mut self, minimum_policy_epoch: u64) -> Self {
        self.strategy_artifact_admission_required = true;
        self.config.strategy_artifact_admission_required = true;
        self.set_strategy_artifact_minimum_policy_epoch(minimum_policy_epoch);
        self
    }

    /// Pins the exact host trust policy used for strategy artifact admission.
    ///
    /// The floor and expected commitment must be loaded independently from externally persisted,
    /// authenticated trusted state; callers must not derive either from the supplied `policy`.
    #[cfg(feature = "artifact-trust")]
    #[must_use]
    pub fn with_strategy_artifact_trust_policy(
        mut self,
        policy: TrustPolicy,
        minimum_policy_epoch: u64,
        expected_policy_commitment: ArtifactDigest,
    ) -> Self {
        self.strategy_artifact_admission_required = true;
        self.config.strategy_artifact_admission_required = true;
        self.set_strategy_artifact_minimum_policy_epoch(minimum_policy_epoch);
        self.strategy_artifact_trust_policy = Some(policy);
        self.strategy_artifact_expected_policy_commitment = Some(expected_policy_commitment);
        self
    }

    /// Adds a cryptographically verified strategy release to the node admission set.
    ///
    /// Supplying an artifact also enables mandatory strategy artifact admission. The floor must
    /// come from externally persisted, monotonic trusted state.
    #[cfg(feature = "artifact-trust")]
    #[must_use]
    pub fn with_admitted_strategy_artifact(
        mut self,
        artifact: VerifiedStrategyArtifact,
        minimum_policy_epoch: u64,
    ) -> Self {
        self.strategy_artifact_admission_required = true;
        self.config.strategy_artifact_admission_required = true;
        self.set_strategy_artifact_minimum_policy_epoch(minimum_policy_epoch);
        self.admitted_strategy_artifacts.push(artifact);
        self
    }

    fn set_strategy_artifact_minimum_policy_epoch(&mut self, minimum_policy_epoch: u64) {
        self.config.strategy_artifact_minimum_policy_epoch = Some(
            self.config
                .strategy_artifact_minimum_policy_epoch
                .map_or(minimum_policy_epoch, |existing| {
                    existing.max(minimum_policy_epoch)
                }),
        );
    }

    #[cfg(feature = "artifact-trust")]
    fn validate_strategy_artifact_trust_inputs(&self) -> anyhow::Result<()> {
        if !self.strategy_artifact_admission_required {
            return Ok(());
        }
        let minimum_policy_epoch = self
            .config
            .strategy_artifact_minimum_policy_epoch
            .ok_or_else(|| {
                anyhow::anyhow!("Strategy artifact minimum policy epoch must be provided")
            })?;
        anyhow::ensure!(
            minimum_policy_epoch > 0,
            "Strategy artifact minimum policy epoch must be greater than zero"
        );
        if let Some(policy) = self.strategy_artifact_trust_policy.as_ref() {
            anyhow::ensure!(
                policy.policy_epoch >= minimum_policy_epoch,
                "Strategy artifact trust policy epoch {} is below the host minimum {minimum_policy_epoch}",
                policy.policy_epoch
            );
            let commitment = policy.validated_commitment()?;
            let expected_commitment = self
                .strategy_artifact_expected_policy_commitment
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Strategy artifact expected policy commitment must be loaded from authenticated deployment state"
                    )
                })?;
            anyhow::ensure!(
                commitment == expected_commitment,
                "Strategy artifact trust policy does not match the externally authorized commitment"
            );
            for artifact in &self.admitted_strategy_artifacts {
                anyhow::ensure!(
                    artifact.trust_policy_epoch() >= minimum_policy_epoch,
                    "Verified strategy artifact policy epoch {} is below the host minimum {minimum_policy_epoch}",
                    artifact.trust_policy_epoch()
                );
                anyhow::ensure!(
                    artifact.trust_policy_commitment() == commitment,
                    "Verified strategy artifact was minted under a different trust policy"
                );
            }
        } else {
            anyhow::ensure!(
                self.admitted_strategy_artifacts.is_empty(),
                "Admitted strategy artifacts require an explicitly pinned trust policy"
            );
        }
        Ok(())
    }

    /// Inject external message bus egress for serialized message bus publications.
    #[must_use]
    pub fn with_external_msgbus_egress(
        mut self,
        external_egress: Box<dyn MessageBusExternalEgress>,
    ) -> Self {
        self.external_msgbus_egress = Some(external_egress);
        self
    }

    /// Build and inject external message bus egress and configured ingress from a factory.
    #[must_use]
    pub fn with_external_msgbus_factory(
        mut self,
        factory: Box<dyn MessageBusBackingFactory>,
    ) -> Self {
        self.external_msgbus_factory = Some(factory);
        self
    }

    /// Inject external message bus ingress for serialized inbound publications.
    #[must_use]
    pub fn with_external_ingress(
        mut self,
        external_ingress: Box<dyn MessageBusExternalIngress>,
    ) -> Self {
        self.external_msgbus_ingress = Some(ExternalMessageBusIngress(external_ingress));
        self
    }

    /// Set the logging configuration.
    #[must_use]
    pub fn with_logging(mut self, logging: LoggerConfig) -> Self {
        self.config.logging = logging;
        self
    }

    /// Adds a data client factory with configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if a client with the same name is already registered.
    pub fn add_data_client(
        self,
        name: Option<String>,
        factory: Box<dyn DataClientFactory>,
        config: Box<dyn ClientConfig>,
    ) -> anyhow::Result<Self> {
        self.add_data_client_with_routing(name, factory, config, RoutingConfig::default())
    }

    /// Adds a data client factory with configuration and explicit routing.
    ///
    /// # Errors
    ///
    /// Returns an error if a client with the same name is already registered.
    pub fn add_data_client_with_routing(
        mut self,
        name: Option<String>,
        factory: Box<dyn DataClientFactory>,
        config: Box<dyn ClientConfig>,
        routing: RoutingConfig,
    ) -> anyhow::Result<Self> {
        let name = name.unwrap_or_else(|| factory.name().to_string());

        if self.data_client_factories.contains_key(&name) {
            anyhow::bail!("Data client '{name}' is already registered");
        }

        self.data_client_factories.insert(name.clone(), factory);
        self.data_client_configs.insert(name.clone(), config);
        self.data_client_routing.insert(name, routing);
        Ok(self)
    }

    /// Adds an execution client factory with configuration.
    ///
    /// Equivalent to [`Self::add_exec_client_with_routing`] with default (empty)
    /// routing.
    ///
    /// # Errors
    ///
    /// Returns an error if a client with the same name is already registered.
    pub fn add_exec_client(
        self,
        name: Option<String>,
        factory: Box<dyn ExecutionClientFactory>,
        config: Box<dyn ClientConfig>,
    ) -> anyhow::Result<Self> {
        self.add_exec_client_with_routing(name, factory, config, RoutingConfig::default())
    }

    /// Adds an execution client factory with configuration and explicit routing.
    ///
    /// # Errors
    ///
    /// Returns an error if a client with the same name is already registered.
    pub fn add_exec_client_with_routing(
        mut self,
        name: Option<String>,
        factory: Box<dyn ExecutionClientFactory>,
        config: Box<dyn ClientConfig>,
        routing: RoutingConfig,
    ) -> anyhow::Result<Self> {
        let name = name.unwrap_or_else(|| factory.name().to_string());

        if self.exec_client_factories.contains_key(&name) {
            anyhow::bail!("Execution client '{name}' is already registered");
        }

        self.exec_client_factories
            .insert(name.clone(), ExecutionClientFactoryEntry::Adapter(factory));
        self.exec_client_configs.insert(name.clone(), config);
        self.exec_client_routing.insert(name, routing);
        Ok(self)
    }

    /// Add a simulated execution client factory.
    ///
    /// This path is for sync-core clients such as the sandbox matching engine, which owns cache
    /// mutation. Live venue adapters should use [`Self::add_exec_client`].
    ///
    /// # Errors
    ///
    /// Returns an error if a client with the same name is already registered.
    pub fn add_simulated_exec_client(
        mut self,
        name: Option<String>,
        factory: Box<dyn SimulatedExecutionClientFactory>,
        config: Box<dyn ClientConfig>,
    ) -> anyhow::Result<Self> {
        let name = name.unwrap_or_else(|| factory.name().to_string());

        if self.exec_client_factories.contains_key(&name) {
            anyhow::bail!("Execution client '{name}' is already registered");
        }

        self.exec_client_factories.insert(
            name.clone(),
            ExecutionClientFactoryEntry::Simulated(factory),
        );
        self.exec_client_configs.insert(name, config);
        Ok(self)
    }

    /// Build the [`LiveNode`] with the configured settings.
    ///
    /// This will:
    /// 1. Build the underlying kernel.
    /// 2. Create clients using factories.
    /// 3. Register clients with engines.
    ///
    /// # Errors
    ///
    /// Returns an error if node construction fails.
    pub fn build(mut self) -> anyhow::Result<LiveNode> {
        #[cfg(not(feature = "artifact-trust"))]
        validate_strategy_artifact_feature_support(self.strategy_artifact_admission_required)?;
        self.config.validate_runtime_support()?;
        #[cfg(feature = "artifact-trust")]
        self.validate_strategy_artifact_trust_inputs()?;

        log::info!(
            "Building LiveNode with {} data clients and {} execution clients",
            self.data_client_factories.len(),
            self.exec_client_factories.len()
        );

        if self.config.event_store.is_some() && self.event_store_factory.is_none() {
            anyhow::bail!(
                "LiveNodeConfig.event_store is set but no factory was registered; \
                 call LiveNodeBuilder::with_event_store(...) to install one"
            );
        }

        if self.external_msgbus_factory.is_some()
            && (self.external_msgbus_egress.is_some() || self.external_msgbus_ingress.is_some())
        {
            anyhow::bail!(
                "external message bus factory cannot be combined with injected egress or ingress"
            );
        }

        install_panic_logging_hook();

        let runner = AsyncRunner::with_queue_capacities(
            usize::try_from(self.config.data_engine.qsize)?,
            usize::try_from(self.config.risk_engine.qsize)?,
            usize::try_from(self.config.exec_engine.qsize)?,
        )?;
        runner.bind_senders();

        let kernel = NautilusKernel::new_with_dependencies(
            self.name.clone(),
            self.config.clone(),
            NautilusKernelDependencies::default()
                .with_clock_factory(self.clock_factory.take())
                .with_event_store_factory(self.event_store_factory.take()),
        )?;
        if self.strategy_artifact_admission_required {
            #[cfg(feature = "artifact-trust")]
            {
                let minimum_policy_epoch = self
                    .config
                    .strategy_artifact_minimum_policy_epoch
                    .ok_or_else(|| {
                        anyhow::anyhow!("Strategy artifact minimum policy epoch must be provided")
                    })?;
                let mut trader = kernel.trader.borrow_mut();
                if let Some(policy) = self.strategy_artifact_trust_policy.as_ref() {
                    let expected_policy_commitment = self
                        .strategy_artifact_expected_policy_commitment
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Strategy artifact expected policy commitment must be provided"
                            )
                        })?;
                    trader.require_strategy_artifact_admission_with_policy(
                        policy,
                        minimum_policy_epoch,
                        expected_policy_commitment,
                    )?;
                } else {
                    trader.require_strategy_artifact_admission(minimum_policy_epoch)?;
                }
                for artifact in self.admitted_strategy_artifacts.drain(..) {
                    trader.admit_strategy_artifact(artifact)?;
                }
            }
            #[cfg(not(feature = "artifact-trust"))]
            anyhow::bail!("Strategy artifact admission requires the 'artifact-trust' feature");
        }
        #[cfg(feature = "python")]
        if let Some(controller) = self.config.controller.as_ref() {
            Trader::add_controller_from_importable_config(&kernel.trader, controller)?;
        }
        #[cfg(not(feature = "python"))]
        if let Some(controller) = self.config.controller.as_ref() {
            anyhow::bail!(
                "LiveNodeConfig.controller for importable controller '{}' requires the python feature",
                controller.controller_path
            );
        }

        self.install_external_msgbus_factory(&kernel)?;

        if let Some(external_egress) = self.external_msgbus_egress.take() {
            let config = self.config.msgbus.clone().unwrap_or_default();
            nautilus_common::msgbus::get_message_bus()
                .borrow_mut()
                .set_external_egress_config(external_egress, &config)?;
        }

        for (name, factory) in self.data_client_factories {
            if let Some(config) = self.data_client_configs.remove(&name) {
                log::debug!("Creating data client {name}");

                let client = factory.create(
                    &name,
                    config.as_ref(),
                    kernel.cache().into(),
                    kernel.clock(),
                )?;
                let client_id = client.client_id();
                let venue = client.venue();

                let adapter = DataClientAdapter::new(
                    client_id, venue, true, // handles_order_book_deltas
                    true, // handles_order_book_snapshots
                    client,
                );

                let routing = self.data_client_routing.remove(&name).unwrap_or_default();

                {
                    let mut data_engine = kernel.data_engine.borrow_mut();
                    data_engine.register_client(adapter, venue);

                    if routing.default {
                        data_engine.set_default_client(client_id)?;
                    }

                    if let Some(venues) = &routing.venues {
                        for venue_str in venues {
                            data_engine.register_venue_routing(
                                client_id,
                                Venue::new(venue_str.as_str()),
                            )?;
                        }
                    }
                }

                log::info!("Registered DataClient-{client_id}");
            } else {
                log::warn!("No config found for data client factory {name}");
            }
        }

        let mut exec_clients = Vec::new();

        for (name, factory) in self.exec_client_factories {
            if let Some(config) = self.exec_client_configs.remove(&name) {
                log::debug!("Creating execution client {name}");

                let client = match factory {
                    ExecutionClientFactoryEntry::Adapter(factory) => {
                        factory.create(&name, config.as_ref(), kernel.cache().into())?
                    }
                    ExecutionClientFactoryEntry::Simulated(factory) => {
                        factory.create(&name, config.as_ref(), kernel.cache())?
                    }
                };
                let client = LiveExecutionClient::new(client);
                let client_id = client.client_id();
                let venue = client.venue();

                let routing = self.exec_client_routing.remove(&name).unwrap_or_default();

                {
                    let mut exec_engine = kernel.exec_engine.borrow_mut();
                    exec_engine.register_client(Box::new(client.clone()))?;

                    if routing.default {
                        exec_engine.set_default_client(client_id)?;
                    }

                    if let Some(venues) = &routing.venues {
                        for venue_str in venues {
                            exec_engine.register_venue_routing(
                                client_id,
                                Venue::new(venue_str.as_str()),
                            )?;
                        }
                    }
                }
                ExecutionEngine::subscribe_venue_instruments(&kernel.exec_engine, venue);
                exec_clients.push(client);

                log::info!("Registered ExecutionClient-{client_id}");
            } else {
                log::warn!("No config found for execution client factory {name}");
            }
        }

        let exec_manager_config = ExecutionManagerConfig::from(&self.config.exec_engine)
            .with_trader_id(self.config.trader_id);
        let mut exec_manager = ExecutionManager::new(
            kernel.clock.clone(),
            kernel.cache.clone(),
            exec_manager_config,
        );

        for client in &exec_clients {
            exec_manager.set_position_reconciliation_tolerance(
                client.account_id(),
                client.position_reconciliation_tolerance(),
            );
        }

        let node = LiveNode::new_from_builder(
            kernel,
            runner,
            self.config,
            exec_manager,
            exec_clients,
            self.external_msgbus_ingress,
        );
        node.load_configured_plugins()?;

        log::info!("Built successfully");

        Ok(node)
    }

    fn install_external_msgbus_factory(&mut self, kernel: &NautilusKernel) -> anyhow::Result<()> {
        let Some(factory) = self.external_msgbus_factory.take() else {
            return Ok(());
        };

        let config = self.config.msgbus.clone().unwrap_or_default();
        let has_external_streams = config
            .external_streams
            .as_ref()
            .is_some_and(|streams| !streams.is_empty());
        config.validate()?;
        let backing = factory.create(self.config.trader_id, kernel.instance_id, config)?;

        if has_external_streams {
            let (external_egress, external_ingress) = external_io_from_backing(backing);
            self.external_msgbus_egress = Some(external_egress);
            self.external_msgbus_ingress = Some(ExternalMessageBusIngress(external_ingress));
        } else {
            self.external_msgbus_egress = Some(external_egress_from_backing(backing));
        }

        Ok(())
    }
}

impl ExternalMessageBusIngress {
    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub(crate) fn take_receiver(
        &mut self,
    ) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.0.take_receiver()
    }

    pub(crate) fn close(&mut self) {
        self.0.close();
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use nautilus_common::enums::Environment;
    use nautilus_event_store::IsolatedVerifier;
    use nautilus_model::identifiers::TraderId;
    use nautilus_provenance::ContentDigest;
    use nautilus_system::event_store::{EventStoreConfig, RunIdentity};
    use nautilus_trading::ImportableControllerConfig;
    use rstest::rstest;

    use super::LiveNodeBuilder;

    #[rstest]
    fn test_with_controller_sets_config_controller() {
        let controller = ImportableControllerConfig {
            controller_path: "module:Controller".to_string(),
            config_path: "module:ControllerConfig".to_string(),
            config: HashMap::new(),
        };

        let builder = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .with_controller(controller);

        assert!(builder.config.controller.is_some());
    }

    #[rstest]
    fn test_with_canonical_event_store_wires_strict_lifecycle_as_one_unit() {
        let helper_path = std::env::current_exe().expect("test executable path");
        let helper_digest = ContentDigest::digest_bytes(
            &fs::read(&helper_path).expect("read test executable for measurement"),
        );
        let verifier =
            IsolatedVerifier::new(&helper_path, helper_digest).expect("measure helper executable");
        let event_store = EventStoreConfig {
            base_dir: std::path::PathBuf::from("/event-store"),
            identity: RunIdentity {
                binary_hash: "11".repeat(32),
                crate_versions: "22".repeat(32),
                config_hash: "33".repeat(32),
                ..Default::default()
            },
            ..Default::default()
        };

        let builder = LiveNodeBuilder::new(TraderId::from("TRADER-001"), Environment::Live)
            .expect("builder")
            .with_canonical_event_store(event_store.clone(), verifier);

        assert!(builder.config.require_durable_ingress_fail_stop);
        let installed = builder
            .config
            .event_store
            .as_ref()
            .expect("event store config");
        assert_eq!(installed.base_dir, event_store.base_dir);
        assert_eq!(installed.identity, event_store.identity);
        assert!(builder.event_store_factory.is_some());
    }
}
