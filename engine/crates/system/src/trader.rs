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

//! Central orchestrator for managing actors, strategies, and execution algorithms.
//!
//! The `Trader` component serves as the primary coordination layer between the kernel
//! and individual trading components. It manages component lifecycles, provides
//! unique identification, and coordinates with system engines.

#[cfg(feature = "strategy-artifact-trust")]
use std::sync::OnceLock;
use std::{
    cell::{RefCell, UnsafeCell},
    fmt::Debug,
    rc::Rc,
};

use ahash::AHashMap;
use indexmap::IndexMap;
#[cfg(feature = "python")]
use nautilus_common::{
    actor::data_actor::ImportableActorConfig,
    python::actor::{PyDataActor, PyDataActorInner},
};
use nautilus_common::{
    actor::{
        Actor, DataActor, DataActorNative,
        registry::{get_actor, try_get_actor_unchecked},
    },
    cache::Cache,
    clock::Clock,
    component::{
        Component, component_state, dispose_component, register_component_actor, reset_component,
        resume_component, start_component, stop_component, with_component_registry,
    },
    enums::{ComponentState, ComponentTrigger, Environment},
    messages::execution::TradingCommand,
    msgbus,
    msgbus::{
        ShareableMessageHandler, TypedHandler, get_message_bus,
        switchboard::{get_event_order_topic, get_event_position_topic},
    },
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    events::{OrderEventAny, PositionEvent},
    identifiers::{
        ActorId, ComponentId, ExecAlgorithmId, StrategyId, TraderId, normalize_order_id_tag,
    },
    orders::Order,
};
#[cfg(feature = "strategy-artifact-trust")]
use nautilus_plugin::{
    ArtifactDigest, StrategyRuntimeArtifact, TrustPolicy, VerifiedStrategyArtifact,
    artifact_content_digest,
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_provenance::ContentDigest;
use nautilus_trading::{
    ExecutionAlgorithm, ExecutionAlgorithmNative,
    strategy::{Strategy, StrategyNative},
};
#[cfg(feature = "python")]
use nautilus_trading::{
    ImportableControllerConfig, ImportableStrategyConfig,
    python::strategy::{PyStrategy, PyStrategyInner},
};
#[cfg(feature = "python")]
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use serde::{Serialize, de::DeserializeOwned};
use ustr::Ustr;

use crate::{
    clock_factory::ClockFactory,
    registration::{
        base_strategy_id, ensure_unique_order_id_tag, strategy_control_endpoint,
        strategy_registration_id,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrategyCommand {
    ExitMarket,
}

type ExecAlgorithmSubscriptionFn = Box<dyn FnMut() -> anyhow::Result<()>>;
type PersistedComponentState = IndexMap<String, Vec<u8>>;
type ComponentStateLoadFn = fn(Ustr, PersistedComponentState) -> anyhow::Result<()>;
type ComponentStateSaveFn = fn(Ustr) -> anyhow::Result<PersistedComponentState>;
type StrategyConfigVerifyFn = Box<dyn Fn() -> anyhow::Result<()>>;
type StrategyConfigEncodeFn<T> = fn(&T) -> anyhow::Result<Vec<u8>>;

#[cfg(feature = "strategy-artifact-trust")]
fn measure_runtime_executable() -> Result<ArtifactDigest, String> {
    static STARTUP_EXECUTABLE_DIGEST: OnceLock<Result<ArtifactDigest, String>> = OnceLock::new();

    STARTUP_EXECUTABLE_DIGEST
        .get_or_init(|| {
            let executable_path = std::env::current_exe()
                .map_err(|error| format!("Cannot resolve current executable: {error}"))?;
            let executable = std::fs::read(&executable_path).map_err(|error| {
                format!(
                    "Cannot measure current executable '{}': {error}",
                    executable_path.display()
                )
            })?;
            Ok(artifact_content_digest(&executable))
        })
        .clone()
}

/// A native strategy whose retained runtime configuration can be reconstructed from an
/// authenticated JSON artifact without a caller-provided factory.
///
/// The constructor and retained-config accessor are part of the measured host executable. This
/// keeps the configuration decoder inside the admitted runtime boundary and lets the host verify
/// that construction did not substitute or discard the decoded configuration. Registration,
/// startup, and evidence-producing completion boundaries re-serialize the same registered object
/// to catch in-process configuration drift. These checks do not continuously enforce immutability
/// and do not make hostile native strategy code hermetic; production evaluation still requires an
/// isolated runner and independently authenticated evidence.
pub trait ArtifactConfiguredStrategy: Sized {
    /// Strictly decoded strategy configuration retained by the constructed strategy.
    type ArtifactConfig: DeserializeOwned + Serialize;

    /// Constructs the strategy from the authenticated configuration value.
    ///
    /// # Errors
    ///
    /// Returns an error when the strategy cannot be constructed from the validated value.
    fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self>;

    /// Returns the current configuration retained by the runtime strategy.
    ///
    /// Evidence-producing callers compare its canonical JSON with the admitted artifact at their
    /// explicit trust boundaries and fail closed if the bytes differ.
    fn artifact_config(&self) -> &Self::ArtifactConfig;
}

/// A strategy constructed by the host-owned authenticated configuration decoder.
///
/// The fields are intentionally private: callers can inspect the strategy for routing metadata,
/// while trusted registration paths consume it only after performing their own admission checks.
#[derive(Debug)]
pub struct ArtifactConfiguredStrategyInstance<T> {
    strategy: T,
    config_artifact_content: Box<[u8]>,
    config_artifact_digest: ContentDigest,
}

impl<T> ArtifactConfiguredStrategyInstance<T>
where
    T: ArtifactConfiguredStrategy,
{
    /// Strictly decodes canonical JSON, constructs `T`, and verifies the retained configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid/non-canonical JSON or when construction substitutes the
    /// decoded configuration.
    pub fn decode(config_artifact_content: &[u8]) -> anyhow::Result<Self> {
        let config: T::ArtifactConfig = serde_json::from_slice(config_artifact_content)
            .map_err(|error| anyhow::anyhow!("Invalid strategy configuration JSON: {error}"))?;
        let canonical = serde_json::to_vec(&config).map_err(|error| {
            anyhow::anyhow!("Cannot canonicalize decoded strategy configuration: {error}")
        })?;
        anyhow::ensure!(
            canonical == config_artifact_content,
            "Strategy configuration artifact is not canonical JSON"
        );

        let strategy = T::from_artifact_config(config)?;
        let retained = serde_json::to_vec(strategy.artifact_config()).map_err(|error| {
            anyhow::anyhow!("Cannot encode retained strategy configuration: {error}")
        })?;
        anyhow::ensure!(
            retained == config_artifact_content,
            "Constructed strategy did not retain the authenticated configuration"
        );

        Ok(Self {
            strategy,
            config_artifact_content: config_artifact_content.into(),
            config_artifact_digest: ContentDigest::digest_bytes(config_artifact_content),
        })
    }

    fn verify_retained_config(&self) -> anyhow::Result<()> {
        let retained = serde_json::to_vec(self.strategy.artifact_config()).map_err(|error| {
            anyhow::anyhow!("Cannot encode retained strategy configuration: {error}")
        })?;
        let retained_digest = ContentDigest::digest_bytes(&retained);
        anyhow::ensure!(
            retained.as_slice() == self.config_artifact_content.as_ref(),
            "Retained strategy configuration changed after artifact verification"
        );
        anyhow::ensure!(
            retained_digest == self.config_artifact_digest,
            "Retained strategy configuration digest changed after artifact verification"
        );
        Ok(())
    }

    /// Returns the constructed strategy for read-only routing inspection.
    #[must_use]
    pub const fn strategy(&self) -> &T {
        &self.strategy
    }

    /// Returns the canonical authenticated bytes which produced this instance.
    #[must_use]
    pub fn config_artifact_content(&self) -> &[u8] {
        &self.config_artifact_content
    }

    fn into_parts(self) -> (T, ArtifactConfigCommitment<T>) {
        (
            self.strategy,
            ArtifactConfigCommitment {
                content: self.config_artifact_content,
                digest: self.config_artifact_digest,
                encode: encode_artifact_config::<T>,
            },
        )
    }
}

fn encode_artifact_config<T>(strategy: &T) -> anyhow::Result<Vec<u8>>
where
    T: ArtifactConfiguredStrategy,
{
    serde_json::to_vec(strategy.artifact_config())
        .map_err(|error| anyhow::anyhow!("Cannot encode retained strategy configuration: {error}"))
}

struct ArtifactConfigCommitment<T> {
    content: Box<[u8]>,
    digest: ContentDigest,
    encode: StrategyConfigEncodeFn<T>,
}

impl<T> ArtifactConfigCommitment<T> {
    fn verify(&self, strategy: &T) -> anyhow::Result<()> {
        let retained = (self.encode)(strategy)?;
        let retained_digest = ContentDigest::digest_bytes(&retained);
        anyhow::ensure!(
            retained.as_slice() == self.content.as_ref(),
            "Retained strategy configuration changed after artifact verification"
        );
        anyhow::ensure!(
            retained_digest == self.digest,
            "Retained strategy configuration digest changed after artifact verification"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct RegisteredComponentIdentity {
    token: RegisteredComponentToken,
    actor: Rc<UnsafeCell<dyn Actor>>,
    component: Rc<UnsafeCell<dyn Component>>,
}

#[derive(Clone, Copy)]
struct RegisteredComponentToken {
    actor_id: Ustr,
    component_id: Ustr,
    actor_address: usize,
    component_address: usize,
}

impl RegisteredComponentIdentity {
    fn from_registered<T>(
        actor_id: Ustr,
        component_id: Ustr,
        registered: &Rc<UnsafeCell<T>>,
    ) -> Self
    where
        T: Actor + Component + 'static,
    {
        let actor: Rc<UnsafeCell<dyn Actor>> = registered.clone();
        let component: Rc<UnsafeCell<dyn Component>> = registered.clone();
        Self {
            token: RegisteredComponentToken {
                actor_id,
                component_id,
                actor_address: Rc::as_ptr(&actor).addr(),
                component_address: Rc::as_ptr(&component).addr(),
            },
            actor,
            component,
        }
    }

    fn capture(actor_id: Ustr, component_id: Ustr) -> anyhow::Result<Self> {
        let actor = get_actor(&actor_id)
            .ok_or_else(|| anyhow::anyhow!("Actor '{actor_id}' not found in global registry"))?;
        let component = with_component_registry(|registry| registry.get(&component_id))
            .ok_or_else(|| {
                anyhow::anyhow!("Component '{component_id}' not found in global registry")
            })?;
        anyhow::ensure!(
            std::ptr::addr_eq(Rc::as_ptr(&actor), Rc::as_ptr(&component)),
            "Actor '{actor_id}' and component '{component_id}' do not refer to the same registered instance"
        );
        Ok(Self {
            token: RegisteredComponentToken {
                actor_id,
                component_id,
                actor_address: Rc::as_ptr(&actor).addr(),
                component_address: Rc::as_ptr(&component).addr(),
            },
            actor,
            component,
        })
    }

    fn ensure_current(&self) -> anyhow::Result<()> {
        self.token.ensure_current()?;
        let actor = get_actor(&self.token.actor_id).ok_or_else(|| {
            anyhow::anyhow!(
                "Registered actor '{}' disappeared from the global registry",
                self.token.actor_id
            )
        })?;
        anyhow::ensure!(
            Rc::ptr_eq(&self.actor, &actor),
            "Registered actor '{}' identity changed after admission",
            self.token.actor_id
        );

        let component = with_component_registry(|registry| registry.get(&self.token.component_id))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Registered component '{}' disappeared from the global registry",
                    self.token.component_id
                )
            })?;
        anyhow::ensure!(
            Rc::ptr_eq(&self.component, &component),
            "Registered component '{}' identity changed after admission",
            self.token.component_id
        );
        Ok(())
    }

    const fn token(&self) -> RegisteredComponentToken {
        self.token
    }
}

impl RegisteredComponentToken {
    fn ensure_current(&self) -> anyhow::Result<()> {
        let actor = get_actor(&self.actor_id).ok_or_else(|| {
            anyhow::anyhow!(
                "Registered actor '{}' disappeared from the global registry",
                self.actor_id
            )
        })?;
        anyhow::ensure!(
            Rc::as_ptr(&actor).addr() == self.actor_address,
            "Registered actor '{}' identity changed after admission",
            self.actor_id
        );

        let component = with_component_registry(|registry| registry.get(&self.component_id))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Registered component '{}' disappeared from the global registry",
                    self.component_id
                )
            })?;
        anyhow::ensure!(
            Rc::as_ptr(&component).addr() == self.component_address,
            "Registered component '{}' identity changed after admission",
            self.component_id
        );
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ComponentStateCallbacks {
    identity: RegisteredComponentToken,
    load: ComponentStateLoadFn,
    save: ComponentStateSaveFn,
}

impl ComponentStateCallbacks {
    fn load(&self, component_id: Ustr, state: PersistedComponentState) -> anyhow::Result<()> {
        self.identity.ensure_current()?;
        (self.load)(component_id, state)
    }

    fn save(&self, component_id: Ustr) -> anyhow::Result<PersistedComponentState> {
        self.identity.ensure_current()?;
        (self.save)(component_id)
    }
}

/// Central orchestrator for managing trading components.
///
/// The `Trader` manages the lifecycle and coordination of actors, strategies,
/// and execution algorithms within the trading system. It provides component
/// registration, state management, and integration with system engines.
///
/// # Notes
///
/// Strategies implement `Strategy::stop() -> bool` which returns whether to proceed
/// with the component stop. This enables `manage_stop` behavior where the strategy
/// can defer stopping until a market exit completes.
///
/// We store type-erased closures because the component registry stores trait objects
/// and we need to call `Strategy::stop()` which requires the concrete type. The
/// closure is created during `add_strategy` when the concrete type `T` is known.
pub struct Trader {
    /// The unique trader identifier.
    pub trader_id: TraderId,
    /// The unique instance identifier.
    pub instance_id: UUID4,
    /// The trading environment context.
    pub environment: Environment,
    /// Component state for lifecycle management.
    state: ComponentState,
    /// Clock source for trader timestamps and component clocks.
    clock_factory: ClockFactory,
    /// System cache for data storage.
    cache: Rc<RefCell<Cache>>,
    /// Portfolio reference for strategy registration.
    portfolio: Rc<RefCell<Portfolio>>,
    /// Registered actor IDs (actors stored in global registry).
    actor_ids: Vec<ActorId>,
    /// Registration-time object identities keyed by their global component IDs.
    component_identities: AHashMap<Ustr, RegisteredComponentIdentity>,
    /// Type-erased state callbacks for registered actors.
    actor_state_callbacks: AHashMap<ActorId, ComponentStateCallbacks>,
    /// Registered strategy IDs (strategies stored in global registry).
    strategy_ids: Vec<StrategyId>,
    /// Whether every strategy registration and start requires a verified release artifact.
    #[cfg(feature = "strategy-artifact-trust")]
    strategy_artifact_admission_required: bool,
    /// Verified release artifacts keyed by their exact runtime strategy identity.
    #[cfg(feature = "strategy-artifact-trust")]
    admitted_strategy_artifacts: AHashMap<String, VerifiedStrategyArtifact>,
    /// Immutable commitment to the host policy authorized to mint admission capabilities.
    #[cfg(feature = "strategy-artifact-trust")]
    strategy_artifact_trust_policy_commitment: Option<ArtifactDigest>,
    /// Minimum policy epoch accepted by this host, sourced from monotonic deployment state.
    #[cfg(feature = "strategy-artifact-trust")]
    strategy_artifact_minimum_policy_epoch: Option<u64>,
    /// Startup filesystem snapshot of the host executable, measured once when the process creates
    /// its first trader and reused without path re-reads. This does not attest mapped pages,
    /// dynamic libraries, JIT code, or the process environment.
    #[cfg(feature = "strategy-artifact-trust")]
    runtime_executable_digest: Result<ArtifactDigest, String>,
    /// Type-erased state callbacks for registered strategies.
    strategy_state_callbacks: AHashMap<StrategyId, ComponentStateCallbacks>,
    /// Start-boundary revalidation of artifact-configured strategies against their exact bytes.
    strategy_config_verifiers: AHashMap<StrategyId, StrategyConfigVerifyFn>,
    /// Strategy stop functions for managed stop behavior.
    strategy_stop_fns: AHashMap<StrategyId, Box<dyn FnMut() -> bool>>,
    /// Msgbus handler IDs for strategy event subscriptions (order, position).
    strategy_handler_ids: AHashMap<StrategyId, (Ustr, Ustr)>,
    /// Registered exec algorithm IDs (algorithms stored in global registry).
    exec_algorithm_ids: Vec<ExecAlgorithmId>,
    /// Restores strategy event subscriptions for concrete execution algorithms.
    exec_algorithm_restore_fns: AHashMap<ExecAlgorithmId, ExecAlgorithmSubscriptionFn>,
    /// Removes strategy event subscriptions for concrete execution algorithms.
    exec_algorithm_cleanup_fns: AHashMap<ExecAlgorithmId, ExecAlgorithmSubscriptionFn>,
    /// Component clocks for individual components.
    clocks: IndexMap<ComponentId, Rc<RefCell<dyn Clock>>>,
    /// Timestamp when the trader was created.
    ts_created: UnixNanos,
    /// Timestamp when the trader was last started.
    ts_started: Option<UnixNanos>,
    /// Timestamp when the trader was last stopped.
    ts_stopped: Option<UnixNanos>,
}

impl Debug for Trader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", stringify!(TraderId)) // TODO
    }
}

impl Trader {
    /// Creates a new [`Trader`] instance.
    #[must_use]
    pub fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        environment: Environment,
        clock_factory: ClockFactory,
        cache: Rc<RefCell<Cache>>,
        portfolio: Rc<RefCell<Portfolio>>,
    ) -> Self {
        let clock = clock_factory.clock();
        let ts_created = clock.borrow().timestamp_ns();

        Self {
            trader_id,
            instance_id,
            environment,
            state: ComponentState::PreInitialized,
            clock_factory,
            cache,
            portfolio,
            actor_ids: Vec::new(),
            component_identities: AHashMap::new(),
            actor_state_callbacks: AHashMap::new(),
            strategy_ids: Vec::new(),
            #[cfg(feature = "strategy-artifact-trust")]
            strategy_artifact_admission_required: false,
            #[cfg(feature = "strategy-artifact-trust")]
            admitted_strategy_artifacts: AHashMap::new(),
            #[cfg(feature = "strategy-artifact-trust")]
            strategy_artifact_trust_policy_commitment: None,
            #[cfg(feature = "strategy-artifact-trust")]
            strategy_artifact_minimum_policy_epoch: None,
            #[cfg(feature = "strategy-artifact-trust")]
            runtime_executable_digest: measure_runtime_executable(),
            strategy_state_callbacks: AHashMap::new(),
            strategy_config_verifiers: AHashMap::new(),
            strategy_stop_fns: AHashMap::new(),
            strategy_handler_ids: AHashMap::new(),
            exec_algorithm_ids: Vec::new(),
            exec_algorithm_restore_fns: AHashMap::new(),
            exec_algorithm_cleanup_fns: AHashMap::new(),
            clocks: IndexMap::new(),
            ts_created,
            ts_started: None,
            ts_stopped: None,
        }
    }

    /// Returns the trader ID.
    #[must_use]
    pub const fn trader_id(&self) -> TraderId {
        self.trader_id
    }

    /// Returns the instance ID.
    #[must_use]
    pub const fn instance_id(&self) -> UUID4 {
        self.instance_id
    }

    /// Returns the trading environment.
    #[must_use]
    pub const fn environment(&self) -> Environment {
        self.environment
    }

    /// Returns the current component state.
    #[must_use]
    pub const fn state(&self) -> ComponentState {
        self.state
    }

    /// Returns the timestamp when the trader was created (UNIX nanoseconds).
    #[must_use]
    pub const fn ts_created(&self) -> UnixNanos {
        self.ts_created
    }

    /// Returns the timestamp when the trader was last started (UNIX nanoseconds).
    #[must_use]
    pub const fn ts_started(&self) -> Option<UnixNanos> {
        self.ts_started
    }

    /// Returns the timestamp when the trader was last stopped (UNIX nanoseconds).
    #[must_use]
    pub const fn ts_stopped(&self) -> Option<UnixNanos> {
        self.ts_stopped
    }

    /// Permanently enables fail-closed strategy artifact admission for this trader.
    ///
    /// Admission must be enabled before any strategy is registered. Once enabled, every native
    /// and Python strategy must have an exact-ID [`VerifiedStrategyArtifact`] before registration
    /// and again before startup. `minimum_policy_epoch` must come from externally persisted,
    /// monotonic trusted state. This library enforces the floor but does not persist it.
    ///
    /// # Errors
    ///
    /// Returns an error if strategies have already been registered.
    #[cfg(feature = "strategy-artifact-trust")]
    pub fn require_strategy_artifact_admission(
        &mut self,
        minimum_policy_epoch: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            minimum_policy_epoch > 0,
            "Strategy artifact minimum policy epoch must be greater than zero"
        );
        if !self.strategy_ids.is_empty() {
            anyhow::bail!(
                "Cannot enable strategy artifact admission after strategies are registered"
            );
        }
        if let Some(existing) = self.strategy_artifact_minimum_policy_epoch {
            anyhow::ensure!(
                minimum_policy_epoch >= existing,
                "Cannot lower the strategy artifact minimum policy epoch from {existing} to {minimum_policy_epoch}"
            );
            anyhow::ensure!(
                minimum_policy_epoch == existing || self.admitted_strategy_artifacts.is_empty(),
                "Cannot raise the strategy artifact minimum policy epoch after artifacts are admitted"
            );
        }
        if let Err(error) = &self.runtime_executable_digest {
            anyhow::bail!(
                "Strategy artifact admission cannot use the startup executable snapshot: {error}"
            );
        }
        self.strategy_artifact_minimum_policy_epoch = Some(minimum_policy_epoch);
        self.strategy_artifact_admission_required = true;
        Ok(())
    }

    /// Permanently pins the host trust policy and enables strategy artifact admission.
    ///
    /// The policy commitment cannot change for the lifetime of this trader. A verified artifact
    /// capability minted under any other caller-supplied policy is rejected at admission.
    /// `minimum_policy_epoch` and `expected_policy_commitment` must come from externally persisted,
    /// authenticated trusted state; this method deliberately derives neither value from `policy`.
    ///
    /// # Errors
    ///
    /// Returns an error after strategy registration, after artifact admission, when the policy is
    /// malformed or unauthorized, or when a different policy was already pinned.
    #[cfg(feature = "strategy-artifact-trust")]
    pub fn require_strategy_artifact_admission_with_policy(
        &mut self,
        policy: &TrustPolicy,
        minimum_policy_epoch: u64,
        expected_policy_commitment: ArtifactDigest,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            minimum_policy_epoch > 0,
            "Strategy artifact minimum policy epoch must be greater than zero"
        );
        anyhow::ensure!(
            policy.policy_epoch >= minimum_policy_epoch,
            "Strategy artifact trust policy epoch {} is below the host minimum {minimum_policy_epoch}",
            policy.policy_epoch
        );
        anyhow::ensure!(
            self.admitted_strategy_artifacts.is_empty(),
            "Cannot pin strategy artifact trust policy after artifacts are admitted"
        );
        let commitment = policy.validated_commitment()?;
        anyhow::ensure!(
            commitment == expected_policy_commitment,
            "Strategy artifact trust policy does not match the externally authorized commitment"
        );
        if let Some(existing) = self.strategy_artifact_trust_policy_commitment {
            anyhow::ensure!(
                existing == commitment,
                "Cannot replace the pinned strategy artifact trust policy"
            );
        }
        self.require_strategy_artifact_admission(minimum_policy_epoch)?;
        self.strategy_artifact_trust_policy_commitment = Some(commitment);
        Ok(())
    }

    /// Returns whether verified strategy artifacts are mandatory for this trader.
    #[must_use]
    #[cfg(feature = "strategy-artifact-trust")]
    pub const fn is_strategy_artifact_admission_required(&self) -> bool {
        self.strategy_artifact_admission_required
    }

    /// Admits a cryptographically verified strategy release artifact.
    ///
    /// Re-admitting the identical artifact is idempotent. Replacing an admission for the same
    /// strategy identity with different signed evidence is rejected to prevent runtime policy
    /// changes after verification.
    ///
    /// # Errors
    ///
    /// Returns an error if the verified artifact has an empty strategy identity or conflicts with
    /// an existing admission.
    #[cfg(feature = "strategy-artifact-trust")]
    pub fn admit_strategy_artifact(
        &mut self,
        artifact: VerifiedStrategyArtifact,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.strategy_artifact_admission_required,
            "Cannot admit a strategy artifact before admission is required"
        );
        let expected_policy_commitment = self
            .strategy_artifact_trust_policy_commitment
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot admit a strategy artifact before the host trust policy is pinned"
                )
            })?;
        let minimum_policy_epoch = self
            .strategy_artifact_minimum_policy_epoch
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot admit a strategy artifact before the host minimum policy epoch is pinned"
                )
            })?;
        anyhow::ensure!(
            artifact.trust_policy_epoch() >= minimum_policy_epoch,
            "Verified strategy artifact policy epoch {} is below the host minimum {minimum_policy_epoch}",
            artifact.trust_policy_epoch()
        );
        anyhow::ensure!(
            artifact.trust_policy_commitment() == expected_policy_commitment,
            "Verified strategy artifact was minted under a different trust policy"
        );
        let strategy_id = artifact.manifest().strategy_id.clone();
        if strategy_id.is_empty() {
            anyhow::bail!("Verified strategy artifact has an empty strategy identity");
        }

        if let Some(existing) = self.admitted_strategy_artifacts.get(&strategy_id) {
            anyhow::ensure!(
                existing == &artifact,
                "A different verified artifact is already admitted for strategy '{strategy_id}'"
            );
            return Ok(());
        }

        self.admitted_strategy_artifacts
            .insert(strategy_id, artifact);
        Ok(())
    }

    /// Returns the verified artifact admitted for `strategy_id`, if present.
    #[must_use]
    #[cfg(feature = "strategy-artifact-trust")]
    pub fn admitted_strategy_artifact(
        &self,
        strategy_id: &StrategyId,
    ) -> Option<&VerifiedStrategyArtifact> {
        self.admitted_strategy_artifacts
            .get(&strategy_id.to_string())
    }

    #[cfg(feature = "strategy-artifact-trust")]
    fn ensure_strategy_artifact_admitted(&self, strategy_id: StrategyId) -> anyhow::Result<()> {
        if !self.strategy_artifact_admission_required {
            return Ok(());
        }

        let runtime_id = strategy_id.to_string();
        let artifact = self
            .admitted_strategy_artifacts
            .get(&runtime_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Strategy '{runtime_id}' rejected: no verified release artifact was admitted"
                )
            })?;
        anyhow::ensure!(
            artifact.manifest().strategy_id == runtime_id,
            "Strategy '{runtime_id}' rejected: admitted artifact identity does not match"
        );
        Ok(())
    }

    #[cfg(feature = "strategy-artifact-trust")]
    fn ensure_native_strategy_artifact_admitted<T: 'static>(
        &self,
        strategy_id: StrategyId,
    ) -> anyhow::Result<()> {
        self.ensure_strategy_artifact_admitted(strategy_id)?;
        if !self.strategy_artifact_admission_required {
            return Ok(());
        }

        #[cfg(feature = "python")]
        anyhow::ensure!(
            std::any::TypeId::of::<T>() != std::any::TypeId::of::<PyStrategyInner>(),
            "Python strategy '{strategy_id}' rejected: secure admission requires an artifact-aware Python source loader"
        );

        let runtime_id = strategy_id.to_string();
        let artifact = self
            .admitted_strategy_artifacts
            .get(&runtime_id)
            .ok_or_else(|| {
                anyhow::anyhow!("Strategy '{runtime_id}' rejected: verified artifact disappeared")
            })?;
        anyhow::ensure!(
            artifact.manifest().runtime_artifact == StrategyRuntimeArtifact::HostExecutable,
            "Strategy '{runtime_id}' rejected: native registration requires a host_executable artifact"
        );
        let runtime_type = std::any::type_name::<T>();
        anyhow::ensure!(
            artifact.manifest().entrypoint == runtime_type,
            "Strategy '{runtime_id}' rejected: signed entrypoint '{}' does not match runtime type '{runtime_type}'",
            artifact.manifest().entrypoint
        );
        let runtime_digest = self.runtime_executable_digest.as_ref().map_err(|error| {
            anyhow::anyhow!(
                "Strategy '{runtime_id}' rejected: startup executable snapshot is unavailable: {error}"
            )
        })?;
        anyhow::ensure!(
            artifact.content_digest() == *runtime_digest,
            "Strategy '{runtime_id}' rejected: signed artifact is not the executing Nautilus binary"
        );
        Ok(())
    }

    #[cfg(feature = "strategy-artifact-trust")]
    fn ensure_strategy_config_artifact_admitted(
        &self,
        strategy_id: StrategyId,
        config_artifact_content: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        if !self.strategy_artifact_admission_required {
            return Ok(());
        }
        let runtime_id = strategy_id.to_string();
        let content = config_artifact_content.ok_or_else(|| {
            anyhow::anyhow!(
                "Strategy '{runtime_id}' rejected: secure admission requires exact strategy configuration bytes"
            )
        })?;
        let artifact = self
            .admitted_strategy_artifacts
            .get(&runtime_id)
            .ok_or_else(|| {
                anyhow::anyhow!("Strategy '{runtime_id}' rejected: verified artifact disappeared")
            })?;
        let actual_digest = artifact_content_digest(content);
        anyhow::ensure!(
            actual_digest == artifact.manifest().strategy_config_digest,
            "Strategy '{runtime_id}' rejected: runtime configuration does not match the evaluated configuration artifact"
        );
        Ok(())
    }

    #[cfg(not(feature = "strategy-artifact-trust"))]
    #[inline]
    #[expect(
        clippy::extra_unused_type_parameters,
        clippy::unnecessary_wraps,
        clippy::unused_self,
        reason = "feature-disabled shim preserves generic registration call sites"
    )]
    fn ensure_native_strategy_artifact_admitted<T: 'static>(
        &self,
        _strategy_id: StrategyId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[cfg(feature = "python")]
    fn ensure_python_strategy_artifact_admitted(
        &self,
        strategy_id: StrategyId,
    ) -> anyhow::Result<()> {
        self.ensure_strategy_artifact_admitted(strategy_id)?;
        #[cfg(feature = "strategy-artifact-trust")]
        anyhow::ensure!(
            !self.strategy_artifact_admission_required,
            "Python strategy '{strategy_id}' rejected: secure admission requires an artifact-aware Python source loader"
        );
        Ok(())
    }

    /// Refuses every Python strategy entry point before importing or introspecting Python when
    /// secure native-artifact admission is active.
    ///
    /// Python strategies need a separate loader which authenticates source bytes before import.
    /// Until that loader exists, touching a module, constructor, descriptor, or retained instance
    /// would execute code outside the admitted runtime boundary.
    #[cfg(feature = "python")]
    #[cfg_attr(
        not(feature = "strategy-artifact-trust"),
        expect(
            clippy::unnecessary_wraps,
            clippy::unused_self,
            reason = "feature-disabled Python shim preserves one fail-closed loader call site"
        )
    )]
    fn ensure_python_strategy_loader_available(&self) -> anyhow::Result<()> {
        #[cfg(feature = "strategy-artifact-trust")]
        anyhow::ensure!(
            !self.strategy_artifact_admission_required,
            "Python strategy rejected before import or introspection: secure admission requires an artifact-aware Python source loader"
        );
        Ok(())
    }

    #[cfg(not(feature = "strategy-artifact-trust"))]
    #[inline]
    #[expect(
        clippy::unnecessary_wraps,
        clippy::unused_self,
        reason = "feature-disabled shim preserves registration call-site symmetry"
    )]
    fn ensure_strategy_artifact_admitted(&self, _strategy_id: StrategyId) -> anyhow::Result<()> {
        Ok(())
    }

    #[cfg(feature = "strategy-artifact-trust")]
    fn ensure_all_strategy_artifacts_admitted(&self) -> anyhow::Result<()> {
        for strategy_id in &self.strategy_ids {
            self.ensure_strategy_artifact_admitted(*strategy_id)?;
        }
        self.ensure_artifact_configurations_current()
    }

    #[cfg(not(feature = "strategy-artifact-trust"))]
    #[inline]
    fn ensure_all_strategy_artifacts_admitted(&self) -> anyhow::Result<()> {
        self.ensure_artifact_configurations_current()
    }

    /// Re-serializes every artifact-configured strategy and compares it with its registration
    /// commitment at an explicit execution trust boundary.
    ///
    /// Callers use this before startup and again after stop callbacks but before issuing evidence.
    /// It detects retained configuration mutation at those boundaries; it is not a continuous
    /// runtime immutability guard.
    ///
    /// # Errors
    ///
    /// Returns an error if a registered strategy is missing or its retained configuration no
    /// longer serializes to the artifact bytes committed during registration.
    pub fn ensure_artifact_configurations_current(&self) -> anyhow::Result<()> {
        for strategy_id in self.strategy_config_verifiers.keys() {
            self.ensure_artifact_configuration_current(*strategy_id)?;
        }
        Ok(())
    }

    fn ensure_artifact_configuration_current(&self, strategy_id: StrategyId) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.strategy_ids.contains(&strategy_id),
            "Artifact configuration verifier exists for unregistered strategy '{strategy_id}'"
        );
        let Some(verify) = self.strategy_config_verifiers.get(&strategy_id) else {
            return Ok(());
        };
        verify().map_err(|error| {
            anyhow::anyhow!(
                "Strategy '{strategy_id}' retained configuration failed trust-boundary verification: {error:#}"
            )
        })
    }

    /// Returns the number of registered actors.
    #[must_use]
    pub const fn actor_count(&self) -> usize {
        self.actor_ids.len()
    }

    /// Returns the number of registered strategies.
    #[must_use]
    pub const fn strategy_count(&self) -> usize {
        self.strategy_ids.len()
    }

    /// Returns the number of registered execution algorithms.
    #[must_use]
    pub const fn exec_algorithm_count(&self) -> usize {
        self.exec_algorithm_ids.len()
    }

    /// Returns references to all component clocks for backtest time advancement.
    #[must_use]
    pub fn get_component_clocks(&self) -> Vec<Rc<RefCell<dyn Clock>>> {
        self.clocks.values().cloned().collect()
    }

    /// Returns the total number of registered components.
    #[must_use]
    pub const fn component_count(&self) -> usize {
        self.actor_ids.len() + self.strategy_ids.len() + self.exec_algorithm_ids.len()
    }

    /// Returns a list of all registered actor IDs.
    #[must_use]
    pub fn actor_ids(&self) -> Vec<ActorId> {
        self.actor_ids.clone()
    }

    /// Returns a list of all registered strategy IDs.
    #[must_use]
    pub fn strategy_ids(&self) -> Vec<StrategyId> {
        self.strategy_ids.clone()
    }

    /// Returns a list of all registered execution algorithm IDs.
    #[must_use]
    pub fn exec_algorithm_ids(&self) -> Vec<ExecAlgorithmId> {
        self.exec_algorithm_ids.clone()
    }

    fn ensure_registration_slot_available(&self, component_id: Ustr) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.component_identities.contains_key(&component_id),
            "Component '{component_id}' is already tracked by this trader"
        );
        Ok(())
    }

    fn insert_component_identity(
        &mut self,
        identity: RegisteredComponentIdentity,
    ) -> anyhow::Result<()> {
        let component_id = identity.token.component_id;
        anyhow::ensure!(
            !self.component_identities.contains_key(&component_id),
            "Component '{component_id}' is already tracked by this trader"
        );
        identity.ensure_current()?;
        self.component_identities.insert(component_id, identity);
        Ok(())
    }

    fn component_identity(
        &self,
        component_id: Ustr,
    ) -> anyhow::Result<&RegisteredComponentIdentity> {
        self.component_identities
            .get(&component_id)
            .ok_or_else(|| anyhow::anyhow!("Component '{component_id}' has no pinned identity"))
    }

    fn ensure_component_identity(&self, component_id: Ustr) -> anyhow::Result<()> {
        self.component_identity(component_id)?.ensure_current()
    }

    fn ensure_all_component_identities(&self) -> anyhow::Result<()> {
        for identity in self.component_identities.values() {
            identity.ensure_current()?;
        }
        Ok(())
    }

    /// Creates a clock for a component and registers it for time advancement.
    ///
    /// Each component gets its own clock instance so that the default time event
    /// callback registered on each clock is independent. In backtest mode, the
    /// clocks are also used for deterministic time advancement by the engine.
    pub fn create_component_clock(&mut self, component_id: ComponentId) -> Rc<RefCell<dyn Clock>> {
        let clock = self.clock_factory.create_component_clock();
        self.clocks.insert(component_id, clock.clone());
        clock
    }

    /// Adds an actor to the trader.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The trader is not in a valid state for adding components.
    /// - An actor with the same ID is already registered.
    pub fn add_actor<T>(&mut self, actor: T) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        self.validate_actor_or_strategy_registration()?;

        let actor_id = actor.actor_id();

        // Check for duplicate registration
        if self.actor_ids.contains(&actor_id) {
            anyhow::bail!("Actor {actor_id} is already registered");
        }

        let component_id = ComponentId::new(actor_id.inner().as_str());
        self.ensure_registration_slot_available(component_id.inner())?;
        let clock = self.create_component_clock(component_id);

        let mut actor_mut = actor;
        actor_mut.register(self.trader_id, clock, self.cache.clone())?;

        self.add_registered_actor(actor_mut)
    }

    /// Adds an actor to the trader using a factory function.
    ///
    /// The factory function is called at registration time to create the actor,
    /// avoiding cloning issues with non-cloneable actor types.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The factory function fails to create the actor.
    /// - The trader is not in a valid state for adding components.
    /// - An actor with the same ID is already registered.
    pub fn add_actor_from_factory<F, T>(&mut self, factory: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> anyhow::Result<T>,
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        let actor = factory()?;

        self.add_actor(actor)
    }

    /// Adds an importable Python actor to the trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor cannot be imported, configured, registered, or tracked.
    #[cfg(feature = "python")]
    pub fn add_actor_from_importable_config(
        &mut self,
        config: &ImportableActorConfig,
    ) -> anyhow::Result<ActorId> {
        self.validate_actor_or_strategy_registration()?;

        let (python_actor, actor_id) = create_python_actor(config)?;
        if self.actor_ids.contains(&actor_id) {
            anyhow::bail!("Actor {actor_id} is already registered");
        }

        self.register_python_actor_instance(&python_actor, actor_id)?;

        log::info!(
            "Registered Python actor {actor_id} with trader {}",
            self.trader_id
        );
        Ok(actor_id)
    }

    #[cfg(feature = "python")]
    fn register_python_actor_instance(
        &mut self,
        python_actor: &Py<PyAny>,
        actor_id: ActorId,
    ) -> anyhow::Result<()> {
        let component_id = ComponentId::new(actor_id.inner().as_str());
        let clock = self.create_component_clock(component_id);
        let trader_id = self.trader_id;
        let cache = self.cache.clone();

        Python::attach(|py| -> anyhow::Result<()> {
            let py_actor = python_actor.bind(py);
            let mut py_data_actor_ref = py_actor
                .extract::<PyRefMut<PyDataActor>>()
                .map_err(Into::<PyErr>::into)
                .map_err(|e| anyhow::anyhow!("Failed to extract PyDataActor: {e}"))?;

            py_data_actor_ref
                .register(trader_id, clock, cache)
                .map_err(|e| anyhow::anyhow!("Failed to register PyDataActor: {e}"))?;

            Ok(())
        })?;

        Python::attach(|py| -> anyhow::Result<()> {
            let py_actor = python_actor.bind(py);
            let py_data_actor_ref = py_actor
                .cast::<PyDataActor>()
                .map_err(|e| anyhow::anyhow!("Failed to downcast to PyDataActor: {e}"))?;
            py_data_actor_ref.borrow().register_in_global_registries();
            Ok(())
        })?;

        self.add_actor_id_for_lifecycle::<PyDataActorInner>(actor_id)?;

        Ok(())
    }

    /// Adds an importable Python controller to the trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the controller cannot be imported, configured, registered, or tracked.
    #[cfg(feature = "python")]
    pub fn add_controller_from_importable_config(
        trader: &Rc<RefCell<Self>>,
        config: &ImportableControllerConfig,
    ) -> anyhow::Result<ActorId> {
        trader.borrow().validate_actor_or_strategy_registration()?;

        let actor_config = ImportableActorConfig {
            actor_path: config.controller_path.clone(),
            config_path: config.config_path.clone(),
            config: config.config.clone(),
        };
        let (python_controller, actor_id) = create_python_actor(&actor_config)?;
        if trader.borrow().actor_ids.contains(&actor_id) {
            anyhow::bail!("Actor {actor_id} is already registered");
        }

        crate::python::controller::bind_controller_trader(&python_controller, trader)?;

        trader
            .borrow_mut()
            .register_python_actor_instance(&python_controller, actor_id)?;

        log::info!(
            "Registered Python controller {actor_id} with trader {}",
            trader.borrow().trader_id
        );
        Ok(actor_id)
    }

    /// Adds an already registered actor to the trader's component registry.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor cannot be registered in the component registry.
    pub fn add_registered_actor<T>(&mut self, actor: T) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        let actor_id = actor.actor_id();
        let component_id = actor.component_id().inner();
        self.ensure_registration_slot_available(component_id)?;

        // Register in both component and actor registries (this consumes the actor)
        let registered = register_component_actor(actor);
        let identity = RegisteredComponentIdentity::from_registered(
            actor_id.inner(),
            component_id,
            &registered,
        );
        self.insert_component_identity(identity.clone())?;

        // Store actor ID for lifecycle management
        self.actor_ids.push(actor_id);
        self.actor_state_callbacks.insert(
            actor_id,
            ComponentStateCallbacks {
                identity: identity.token(),
                load: Self::load_component_state::<T>,
                save: Self::save_component_state::<T>,
            },
        );

        log::info!("Registered actor {actor_id} with trader {}", self.trader_id);

        Ok(())
    }

    /// Adds an actor ID to the trader's lifecycle management without consuming the actor.
    ///
    /// This is useful when the actor is already registered in the global component registry
    /// but the trader needs to track it for lifecycle management. The caller is responsible
    /// for ensuring the actor is properly registered in the global registries.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor ID is already tracked by this trader.
    pub fn add_actor_id_for_lifecycle<T>(&mut self, actor_id: ActorId) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Debug + 'static,
    {
        // Check for duplicate registration
        if self.actor_ids.contains(&actor_id) {
            anyhow::bail!("Actor '{actor_id}' is already tracked by trader");
        }
        anyhow::ensure!(
            try_get_actor_unchecked::<T>(&actor_id.inner()).is_some(),
            "Actor '{actor_id}' is absent or has a different concrete type"
        );
        let component_id = actor_id.inner();
        let identity = RegisteredComponentIdentity::capture(actor_id.inner(), component_id)?;
        self.insert_component_identity(identity.clone())?;

        // Store actor ID for lifecycle management
        self.actor_ids.push(actor_id);
        self.actor_state_callbacks.insert(
            actor_id,
            ComponentStateCallbacks {
                identity: identity.token(),
                load: Self::load_component_state::<T>,
                save: Self::save_component_state::<T>,
            },
        );

        log::debug!(
            "Added actor ID '{actor_id}' to trader {} for lifecycle management",
            self.trader_id
        );

        Ok(())
    }

    /// Adds an externally-registered execution algorithm ID to the trader for lifecycle management.
    ///
    /// The execution algorithm must already be registered in the global component and actor
    /// registries. This method only tracks the ID so the trader can manage the algorithm's
    /// lifecycle (start/stop/dispose).
    ///
    /// # Errors
    ///
    /// Returns an error if an execution algorithm with the same ID is already tracked.
    pub fn add_exec_algorithm_id_for_lifecycle(
        &mut self,
        exec_algorithm_id: ExecAlgorithmId,
    ) -> anyhow::Result<()> {
        if self.exec_algorithm_ids.contains(&exec_algorithm_id) {
            anyhow::bail!("Execution algorithm '{exec_algorithm_id}' is already tracked by trader");
        }
        let identity = RegisteredComponentIdentity::capture(
            exec_algorithm_id.inner(),
            exec_algorithm_id.inner(),
        )?;
        self.insert_component_identity(identity)?;

        self.exec_algorithm_ids.push(exec_algorithm_id);

        log::debug!(
            "Added exec algorithm ID '{exec_algorithm_id}' to trader {} for lifecycle management",
            self.trader_id
        );

        Ok(())
    }

    /// Adds an externally-registered strategy to the trader for lifecycle management
    /// and installs its order/position event subscriptions, stop hook, and control endpoint.
    ///
    /// The strategy must already be registered in the global component and actor
    /// registries. The generic parameter `T` must match the concrete type stored
    /// in those registries so that the typed event handlers can retrieve it.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy ID is already tracked by this trader.
    pub fn add_strategy_id_with_subscriptions<T>(
        &mut self,
        strategy_id: StrategyId,
    ) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        if self.strategy_ids.contains(&strategy_id) {
            anyhow::bail!("Strategy '{strategy_id}' is already tracked by trader");
        }
        #[cfg(feature = "strategy-artifact-trust")]
        anyhow::ensure!(
            !self.strategy_artifact_admission_required,
            "Strategy '{strategy_id}' rejected: externally registered lifecycle strategies do not provide measured configuration bytes"
        );
        self.ensure_native_strategy_artifact_admitted::<T>(strategy_id)?;

        let existing_order_id_tags: Vec<&str> =
            self.strategy_ids.iter().map(StrategyId::get_tag).collect();
        ensure_unique_order_id_tag(&existing_order_id_tags, strategy_id.get_tag())?;

        let actor_id = Ustr::from(strategy_id.inner().as_str());
        anyhow::ensure!(
            try_get_actor_unchecked::<T>(&actor_id).is_some(),
            "Strategy '{strategy_id}' is absent or has a different concrete type"
        );
        let identity = RegisteredComponentIdentity::capture(actor_id, strategy_id.inner())?;
        self.insert_component_identity(identity.clone())?;

        // Subscribe to order events for this strategy
        let order_topic = get_event_order_topic(strategy_id);
        let order_actor_id = actor_id;
        let order_identity = identity.token();
        let order_handler = TypedHandler::from(move |event: &OrderEventAny| {
            if let Err(error) = order_identity.ensure_current() {
                log::error!("Refusing strategy {order_actor_id} order callback: {error:#}");
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&order_actor_id) {
                strategy.handle_order_event(event.clone());
            } else {
                log::error!("Strategy {order_actor_id} not found for order event handling");
            }
        });
        let order_handler_id = order_handler.id();
        msgbus::subscribe_order_events(order_topic.into(), order_handler, None);

        // Subscribe to position events for this strategy
        let position_topic = get_event_position_topic(strategy_id);
        let position_identity = identity.token();
        let position_handler = TypedHandler::from(move |event: &PositionEvent| {
            if let Err(error) = position_identity.ensure_current() {
                log::error!("Refusing strategy {actor_id} position callback: {error:#}");
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&actor_id) {
                strategy.handle_position_event(event.clone());
            } else {
                log::error!("Strategy {actor_id} not found for position event handling");
            }
        });
        let position_handler_id = position_handler.id();
        msgbus::subscribe_position_events(position_topic.into(), position_handler, None);

        let control_actor_id = actor_id;
        let control_identity = identity.token();
        let control_handler = TypedHandler::from(move |command: &StrategyCommand| {
            if let Err(error) = control_identity.ensure_current() {
                log::error!("Refusing strategy {control_actor_id} control callback: {error:#}");
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&control_actor_id) {
                match command {
                    StrategyCommand::ExitMarket => {
                        if let Err(e) = strategy.market_exit() {
                            log::error!(
                                "Error handling strategy command for {control_actor_id}: {e}"
                            );
                        }
                    }
                }
            } else {
                log::error!("Strategy {control_actor_id} not found for control handling");
            }
        });
        get_message_bus()
            .borrow_mut()
            .endpoint_map::<StrategyCommand>()
            .register(strategy_control_endpoint(strategy_id), control_handler);

        self.strategy_ids.push(strategy_id);
        self.strategy_state_callbacks.insert(
            strategy_id,
            ComponentStateCallbacks {
                identity: identity.token(),
                load: Self::load_component_state::<T>,
                save: Self::save_component_state::<T>,
            },
        );
        self.strategy_handler_ids
            .insert(strategy_id, (order_handler_id, position_handler_id));

        // Register stop hook
        let stop_actor_id = actor_id;
        let stop_identity = identity.token();
        let stop_fn = Box::new(move || -> bool {
            if let Err(error) = stop_identity.ensure_current() {
                log::error!("Refusing strategy {stop_actor_id} stop callback: {error:#}");
                true
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&stop_actor_id) {
                Strategy::stop(&mut *strategy)
            } else {
                log::error!("Strategy {stop_actor_id} not found for stop");
                true
            }
        });
        self.strategy_stop_fns.insert(strategy_id, stop_fn);

        log::debug!(
            "Added strategy '{strategy_id}' to trader {} with event subscriptions",
            self.trader_id
        );

        Ok(())
    }

    /// Prepares a strategy ID and order ID tag before registration.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy ID or order ID tag is already registered.
    pub fn prepare_strategy_for_registration<T>(
        &self,
        strategy: &mut T,
    ) -> anyhow::Result<StrategyId>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let existing_order_id_tags: Vec<&str> =
            self.strategy_ids.iter().map(StrategyId::get_tag).collect();

        let configured_strategy_id = StrategyNative::strategy_core(strategy).strategy_id();
        let runtime_order_id_tag =
            normalize_order_id_tag(StrategyNative::strategy_core(strategy).order_id_tag());

        let strategy_id = if let Some(strategy_id) = configured_strategy_id {
            ensure_unique_order_id_tag(&existing_order_id_tags, strategy_id.get_tag())?;
            StrategyNative::strategy_core_mut(strategy).change_id(strategy_id);
            strategy_id
        } else {
            let order_id_tag = runtime_order_id_tag.map_or_else(
                || format!("{:03}", existing_order_id_tags.len()),
                str::to_string,
            );
            ensure_unique_order_id_tag(&existing_order_id_tags, &order_id_tag)?;

            let base_id = strategy_registration_id::<T>(strategy);
            let strategy_id =
                StrategyId::from(format!("{}-{order_id_tag}", base_strategy_id(&base_id)));
            StrategyNative::strategy_core_mut(strategy).change_id(strategy_id);
            strategy_id
        };

        if self.strategy_ids.contains(&strategy_id) {
            anyhow::bail!("Strategy {strategy_id} is already registered");
        }

        self.ensure_native_strategy_artifact_admitted::<T>(strategy_id)?;

        Ok(strategy_id)
    }

    /// Adds a strategy to the trader.
    ///
    /// Strategies are registered in both the component registry (for lifecycle management)
    /// and the actor registry (for data callbacks via msgbus). The strategy's `StrategyCore`
    /// is also registered with the portfolio for order management.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The trader is not in a valid state for adding components.
    /// - A strategy with the same ID is already registered.
    pub fn add_strategy<T>(&mut self, strategy: T) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        self.add_strategy_inner(strategy, None)
    }

    /// Decodes and adds a strategy from its exact authenticated configuration artifact.
    ///
    /// This is the required native registration path when strategy artifact admission is enabled.
    /// Configuration construction is implemented by the measured strategy type rather than a
    /// caller-provided closure, and the retained runtime configuration is checked before admission.
    ///
    /// # Errors
    ///
    /// Returns an error for normal registration failures or when `config_artifact_content` does
    /// not match the configuration evaluated by the signed release attestation.
    #[cfg(feature = "strategy-artifact-trust")]
    pub fn add_strategy_from_artifact_config<T>(
        &mut self,
        strategy_id: StrategyId,
        config_artifact_content: &[u8],
    ) -> anyhow::Result<()>
    where
        T: ArtifactConfiguredStrategy
            + Strategy
            + StrategyNative
            + DataActorNative
            + Component
            + Debug
            + 'static,
    {
        self.preflight_artifact_configured_strategy::<T>(strategy_id, config_artifact_content)?;
        let mut instance =
            ArtifactConfiguredStrategyInstance::<T>::decode(config_artifact_content)?;
        let decoded_strategy_id = self.prepare_artifact_configured_strategy(&mut instance)?;
        anyhow::ensure!(
            decoded_strategy_id == strategy_id,
            "Decoded strategy identity '{decoded_strategy_id}' does not match pre-admitted target '{strategy_id}'"
        );
        self.add_artifact_configured_strategy(instance)
    }

    /// Verifies that an exact native strategy type and raw configuration artifact are admitted
    /// before their custom deserializer or constructor is allowed to execute.
    ///
    /// `strategy_id` is an external routing identity, not a value extracted from the untrusted
    /// configuration. The decoded strategy is checked again during registration, so it cannot
    /// substitute a different runtime identity after this side-effect-free preflight.
    ///
    /// # Errors
    ///
    /// Returns an error when the trader cannot accept a strategy, or the target identity, runtime
    /// type, startup executable snapshot, or raw configuration digest is not admitted.
    #[cfg(feature = "strategy-artifact-trust")]
    pub fn preflight_artifact_configured_strategy<T>(
        &self,
        strategy_id: StrategyId,
        config_artifact_content: &[u8],
    ) -> anyhow::Result<()>
    where
        T: ArtifactConfiguredStrategy
            + Strategy
            + StrategyNative
            + DataActorNative
            + Component
            + Debug
            + 'static,
    {
        self.validate_actor_or_strategy_registration()?;
        anyhow::ensure!(
            !self.strategy_ids.contains(&strategy_id),
            "Strategy {strategy_id} is already registered"
        );
        self.ensure_native_strategy_artifact_admitted::<T>(strategy_id)?;
        self.ensure_strategy_config_artifact_admitted(strategy_id, Some(config_artifact_content))
    }

    /// Performs admission checks and prepares a decoded strategy before external routing changes.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime type, identity, binary, or configuration digest is not
    /// admitted, or normal strategy ID preparation fails.
    pub fn prepare_artifact_configured_strategy<T>(
        &self,
        instance: &mut ArtifactConfiguredStrategyInstance<T>,
    ) -> anyhow::Result<StrategyId>
    where
        T: ArtifactConfiguredStrategy
            + Strategy
            + StrategyNative
            + DataActorNative
            + Component
            + Debug
            + 'static,
    {
        instance.verify_retained_config()?;
        let strategy_id = self.prepare_strategy_for_registration(&mut instance.strategy)?;
        instance.verify_retained_config()?;
        #[cfg(feature = "strategy-artifact-trust")]
        self.ensure_strategy_config_artifact_admitted(
            strategy_id,
            Some(&instance.config_artifact_content),
        )?;
        Ok(strategy_id)
    }

    /// Registers a strategy produced by [`ArtifactConfiguredStrategyInstance::decode`].
    ///
    /// # Errors
    ///
    /// Returns an error for normal registration or artifact-admission failures.
    pub fn add_artifact_configured_strategy<T>(
        &mut self,
        instance: ArtifactConfiguredStrategyInstance<T>,
    ) -> anyhow::Result<()>
    where
        T: ArtifactConfiguredStrategy
            + Strategy
            + StrategyNative
            + DataActorNative
            + Component
            + Debug
            + 'static,
    {
        instance.verify_retained_config()?;
        let (strategy, commitment) = instance.into_parts();
        self.add_strategy_inner(strategy, Some(commitment))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "registration atomically wires the pinned strategy identity into every lifecycle and message callback"
    )]
    fn add_strategy_inner<T>(
        &mut self,
        mut strategy: T,
        config_commitment: Option<ArtifactConfigCommitment<T>>,
    ) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        self.validate_actor_or_strategy_registration()?;

        if let Some(commitment) = &config_commitment {
            commitment.verify(&strategy)?;
        }

        let strategy_id = self.prepare_strategy_for_registration(&mut strategy)?;
        if let Some(commitment) = &config_commitment {
            commitment.verify(&strategy)?;
        }
        #[cfg(feature = "strategy-artifact-trust")]
        self.ensure_native_strategy_artifact_admitted::<T>(strategy_id)?;
        #[cfg(feature = "strategy-artifact-trust")]
        self.ensure_strategy_config_artifact_admitted(
            strategy_id,
            config_commitment
                .as_ref()
                .map(|commitment| commitment.content.as_ref()),
        )?;

        let component_id = strategy.component_id();
        let actor_id = strategy.actor_id().inner();
        self.ensure_registration_slot_available(component_id.inner())?;
        let clock = self.create_component_clock(component_id);

        // Register strategy core with portfolio for order management
        StrategyNative::strategy_core_mut(&mut strategy).register(
            self.trader_id,
            clock.clone(),
            self.cache.clone(),
            self.portfolio.clone(),
        )?;

        // Transition to Ready state
        strategy.initialize()?;
        if let Some(commitment) = &config_commitment {
            commitment.verify(&strategy)?;
        }

        // Register in both component and actor registries
        let registered = register_component_actor(strategy);
        let identity = RegisteredComponentIdentity::from_registered(
            actor_id,
            component_id.inner(),
            &registered,
        );
        self.insert_component_identity(identity.clone())?;

        let config_verifier = config_commitment.map(|commitment| {
            let identity = identity.clone();
            Box::new(move || {
                identity.ensure_current()?;
                let strategy = try_get_actor_unchecked::<T>(&actor_id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Strategy '{strategy_id}' disappeared or changed type after artifact admission"
                    )
                })?;
                commitment.verify(&strategy)
            }) as StrategyConfigVerifyFn
        });

        // Register default time event handler for this exact strategy instance.
        let time_identity = identity.token();
        let callback = TimeEventCallback::from(move |event: TimeEvent| {
            if let Err(error) = time_identity.ensure_current() {
                log::error!("Refusing strategy {actor_id} time callback: {error:#}");
            } else if let Some(mut actor) = try_get_actor_unchecked::<T>(&actor_id) {
                actor.handle_time_event(&event);
            } else {
                log::error!("Strategy {actor_id} not found for time event handling");
            }
        });
        clock.borrow_mut().register_default_handler(callback);

        let order_topic = get_event_order_topic(strategy_id);
        let order_actor_id = actor_id;
        let order_identity = identity.token();
        let order_handler = TypedHandler::from(move |event: &OrderEventAny| {
            if let Err(error) = order_identity.ensure_current() {
                log::error!("Refusing strategy {order_actor_id} order callback: {error:#}");
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&order_actor_id) {
                strategy.handle_order_event(event.clone());
            } else {
                log::error!("Strategy {order_actor_id} not found for order event handling");
            }
        });
        let order_handler_id = order_handler.id();
        msgbus::subscribe_order_events(order_topic.into(), order_handler, None);

        let position_topic = get_event_position_topic(strategy_id);
        let position_identity = identity.token();
        let position_handler = TypedHandler::from(move |event: &PositionEvent| {
            if let Err(error) = position_identity.ensure_current() {
                log::error!("Refusing strategy {actor_id} position callback: {error:#}");
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&actor_id) {
                strategy.handle_position_event(event.clone());
            } else {
                log::error!("Strategy {actor_id} not found for position event handling");
            }
        });
        let position_handler_id = position_handler.id();
        msgbus::subscribe_position_events(position_topic.into(), position_handler, None);

        let control_actor_id = actor_id;
        let control_identity = identity.token();
        let control_handler = TypedHandler::from(move |command: &StrategyCommand| {
            if let Err(error) = control_identity.ensure_current() {
                log::error!("Refusing strategy {control_actor_id} control callback: {error:#}");
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&control_actor_id) {
                match command {
                    StrategyCommand::ExitMarket => {
                        if let Err(e) = strategy.market_exit() {
                            log::error!(
                                "Error handling strategy command for {control_actor_id}: {e}"
                            );
                        }
                    }
                }
            } else {
                log::error!("Strategy {control_actor_id} not found for control handling");
            }
        });
        get_message_bus()
            .borrow_mut()
            .endpoint_map::<StrategyCommand>()
            .register(strategy_control_endpoint(strategy_id), control_handler);

        self.strategy_ids.push(strategy_id);
        self.strategy_state_callbacks.insert(
            strategy_id,
            ComponentStateCallbacks {
                identity: identity.token(),
                load: Self::load_component_state::<T>,
                save: Self::save_component_state::<T>,
            },
        );
        self.strategy_handler_ids
            .insert(strategy_id, (order_handler_id, position_handler_id));
        if let Some(config_verifier) = config_verifier {
            self.strategy_config_verifiers
                .insert(strategy_id, config_verifier);
        }

        let stop_actor_id = actor_id;
        let stop_identity = identity.token();
        let stop_fn = Box::new(move || -> bool {
            if let Err(error) = stop_identity.ensure_current() {
                log::error!("Refusing strategy {stop_actor_id} stop callback: {error:#}");
                true
            } else if let Some(mut strategy) = try_get_actor_unchecked::<T>(&stop_actor_id) {
                Strategy::stop(&mut *strategy)
            } else {
                log::error!("Strategy {stop_actor_id} not found for stop");
                true // Proceed with component stop anyway
            }
        });
        self.strategy_stop_fns.insert(strategy_id, stop_fn);

        log::info!(
            "Registered strategy {strategy_id} with trader {}",
            self.trader_id
        );

        Ok(())
    }

    /// Adds an importable Python strategy to the trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot be imported, configured, registered, or tracked.
    #[cfg(feature = "python")]
    pub fn add_strategy_from_importable_config(
        &mut self,
        config: &ImportableStrategyConfig,
    ) -> anyhow::Result<StrategyId> {
        self.validate_actor_or_strategy_registration()?;
        self.ensure_python_strategy_loader_available()?;

        let (python_strategy, strategy_id) = create_python_strategy(config)?;
        self.ensure_python_strategy_artifact_admitted(strategy_id)?;
        if self.strategy_ids.contains(&strategy_id) {
            anyhow::bail!("Strategy {strategy_id} is already registered");
        }
        let existing_order_id_tags: Vec<&str> =
            self.strategy_ids.iter().map(StrategyId::get_tag).collect();
        ensure_unique_order_id_tag(&existing_order_id_tags, strategy_id.get_tag())?;

        let component_id = ComponentId::new(strategy_id.inner().as_str());
        let clock = self.create_component_clock(component_id);
        let trader_id = self.trader_id;
        let cache = self.cache.clone();
        let portfolio = self.portfolio.clone();

        Python::attach(|py| -> anyhow::Result<()> {
            let py_strategy = python_strategy.bind(py);
            let mut py_strategy_ref = py_strategy
                .extract::<PyRefMut<PyStrategy>>()
                .map_err(Into::<PyErr>::into)
                .map_err(|e| anyhow::anyhow!("Failed to extract PyStrategy: {e}"))?;

            py_strategy_ref
                .register(trader_id, clock, cache, portfolio)
                .map_err(|e| anyhow::anyhow!("Failed to register PyStrategy: {e}"))?;

            Ok(())
        })?;

        Python::attach(|py| -> anyhow::Result<()> {
            let py_strategy = python_strategy.bind(py);
            let py_strategy_ref = py_strategy
                .cast::<PyStrategy>()
                .map_err(|e| anyhow::anyhow!("Failed to downcast to PyStrategy: {e}"))?;
            py_strategy_ref.borrow().register_in_global_registries();
            Ok(())
        })?;

        self.add_strategy_id_with_subscriptions::<PyStrategyInner>(strategy_id)?;

        log::info!(
            "Registered Python strategy {strategy_id} with trader {}",
            self.trader_id
        );
        Ok(strategy_id)
    }

    /// Adds a constructed Python strategy instance to the trader.
    ///
    /// This is the instance-based counterpart to [`Self::add_strategy_from_importable_config`]:
    /// the strategy is already constructed in Python, avoiding the `dict`-to-JSON round trip of
    /// the importable-config path. The strategy ID, order ID tag, and logging flags are sourced
    /// from the instance's retained `.config`.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot be configured, registered, or tracked.
    #[cfg(feature = "python")]
    pub fn add_python_strategy_instance(
        &mut self,
        strategy: &Py<PyAny>,
    ) -> anyhow::Result<StrategyId> {
        self.ensure_python_strategy_loader_available()?;
        self.prepare_python_strategy_instance(strategy)?;
        self.commit_python_strategy_instance(strategy)
    }

    /// Prepares a constructed Python strategy instance for registration without committing it.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot be configured, or its ID or order ID tag is
    /// already registered.
    #[cfg(feature = "python")]
    pub fn prepare_python_strategy_instance(
        &mut self,
        strategy: &Py<PyAny>,
    ) -> anyhow::Result<StrategyId> {
        self.validate_actor_or_strategy_registration()?;
        self.ensure_python_strategy_loader_available()?;

        let strategy_id = Python::attach(|py| -> anyhow::Result<StrategyId> {
            let bound = strategy.bind(py);

            let config_instance = bound
                .getattr("config")
                .ok()
                .filter(|config| !config.is_none());

            let mut py_strategy_ref = bound
                .extract::<PyRefMut<PyStrategy>>()
                .map_err(Into::<PyErr>::into)
                .map_err(|e| anyhow::anyhow!("Failed to extract PyStrategy: {e}"))?;

            if let Some(config_obj) = config_instance.as_ref() {
                configure_py_strategy(&mut py_strategy_ref, config_obj)?;
            }

            py_strategy_ref.set_python_instance(strategy.clone_ref(py));
            Ok(py_strategy_ref.strategy_id())
        })?;

        if self.strategy_ids.contains(&strategy_id) {
            anyhow::bail!("Strategy {strategy_id} is already registered");
        }

        let existing_order_id_tags: Vec<&str> =
            self.strategy_ids.iter().map(StrategyId::get_tag).collect();
        ensure_unique_order_id_tag(&existing_order_id_tags, strategy_id.get_tag())?;
        self.ensure_python_strategy_artifact_admitted(strategy_id)?;

        Ok(strategy_id)
    }

    /// Commits a previously prepared Python strategy instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot be registered or its subscriptions cannot be
    /// installed.
    #[cfg(feature = "python")]
    pub fn commit_python_strategy_instance(
        &mut self,
        strategy: &Py<PyAny>,
    ) -> anyhow::Result<StrategyId> {
        self.ensure_python_strategy_loader_available()?;
        let strategy_id = Python::attach(|py| -> anyhow::Result<StrategyId> {
            Ok(strategy
                .bind(py)
                .extract::<PyRef<PyStrategy>>()
                .map_err(Into::<PyErr>::into)
                .map_err(|e| anyhow::anyhow!("Failed to extract PyStrategy: {e}"))?
                .strategy_id())
        })?;
        self.ensure_python_strategy_artifact_admitted(strategy_id)?;

        let component_id = ComponentId::new(strategy_id.inner().as_str());
        let clock = self.create_component_clock(component_id);
        let trader_id = self.trader_id;
        let cache = self.cache.clone();
        let portfolio = self.portfolio.clone();

        Python::attach(|py| -> anyhow::Result<()> {
            let py_strategy = strategy.bind(py);
            let mut py_strategy_ref = py_strategy
                .extract::<PyRefMut<PyStrategy>>()
                .map_err(Into::<PyErr>::into)
                .map_err(|e| anyhow::anyhow!("Failed to extract PyStrategy: {e}"))?;

            py_strategy_ref
                .register(trader_id, clock, cache, portfolio)
                .map_err(|e| anyhow::anyhow!("Failed to register PyStrategy: {e}"))?;

            Ok(())
        })?;

        Python::attach(|py| -> anyhow::Result<()> {
            let py_strategy = strategy.bind(py);
            let py_strategy_ref = py_strategy
                .cast::<PyStrategy>()
                .map_err(|e| anyhow::anyhow!("Failed to downcast to PyStrategy: {e}"))?;
            py_strategy_ref.borrow().register_in_global_registries();
            Ok(())
        })?;

        self.add_strategy_id_with_subscriptions::<PyStrategyInner>(strategy_id)?;

        log::info!(
            "Registered Python strategy {strategy_id} with trader {}",
            self.trader_id
        );
        Ok(strategy_id)
    }

    /// Adds an execution algorithm to the trader.
    ///
    /// Execution algorithms are registered in both the component registry (for lifecycle
    /// management) and the actor registry (for data callbacks via msgbus).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The trader is not in a valid state for adding components.
    /// - An execution algorithm with the same ID is already registered.
    pub fn add_exec_algorithm<T>(&mut self, mut exec_algorithm: T) -> anyhow::Result<()>
    where
        T: ExecutionAlgorithm + ExecutionAlgorithmNative + Component + Debug + 'static,
    {
        self.validate_exec_algorithm_registration()?;

        let exec_algorithm_id =
            ExecAlgorithmId::from(exec_algorithm.component_id().inner().as_str());

        if self.exec_algorithm_ids.contains(&exec_algorithm_id) {
            anyhow::bail!("Execution algorithm '{exec_algorithm_id}' is already registered");
        }

        let component_id = exec_algorithm.component_id();
        let actor_id = component_id.inner();
        self.ensure_registration_slot_available(component_id.inner())?;
        let clock = self.create_component_clock(component_id);

        exec_algorithm.register(self.trader_id, clock, self.cache.clone())?;
        exec_algorithm
            .exec_algorithm_core_mut()
            .set_portfolio(self.portfolio.clone());

        let registered = register_component_actor(exec_algorithm);
        let identity = RegisteredComponentIdentity::from_registered(
            actor_id,
            component_id.inner(),
            &registered,
        );
        self.insert_component_identity(identity.clone())?;

        // Register the {id}.execute endpoint so the order manager can
        // route TradingCommands to this algorithm via msgbus::send_any
        let actor_id = Ustr::from(exec_algorithm_id.inner().as_str());
        let restore_actor_id = actor_id;
        let restore_identity = identity.token();
        let restore_fn: ExecAlgorithmSubscriptionFn = Box::new(move || {
            restore_identity.ensure_current()?;
            let Some(mut algo) = try_get_actor_unchecked::<T>(&restore_actor_id) else {
                anyhow::bail!(
                    "Execution algorithm {restore_actor_id} not found while restoring subscriptions"
                );
            };

            let mut strategy_ids = {
                let cache = algo.exec_algorithm_core_mut().cache_ref();
                cache
                    .orders_for_exec_algorithm(&exec_algorithm_id, None, None, None, None, None)
                    .into_iter()
                    .filter(|order| {
                        !order.is_closed() && order.exec_algorithm_id() == Some(exec_algorithm_id)
                    })
                    .map(|order| order.strategy_id())
                    .collect::<Vec<_>>()
            };
            strategy_ids.sort_unstable();
            strategy_ids.dedup();

            for strategy_id in strategy_ids {
                algo.subscribe_to_strategy_events(strategy_id);
            }

            Ok(())
        });
        let cleanup_actor_id = actor_id;
        let cleanup_identity = identity.token();
        let cleanup_fn: ExecAlgorithmSubscriptionFn = Box::new(move || {
            cleanup_identity.ensure_current()?;
            let Some(mut algo) = try_get_actor_unchecked::<T>(&cleanup_actor_id) else {
                anyhow::bail!(
                    "Execution algorithm {cleanup_actor_id} not found while cleaning subscriptions"
                );
            };
            algo.unsubscribe_all_strategy_events();
            Ok(())
        });
        let endpoint: Ustr = format!("{exec_algorithm_id}.execute").into();
        let execute_identity = identity.token();
        let handler = ShareableMessageHandler::from_typed(move |command: &TradingCommand| {
            if let Err(error) = execute_identity.ensure_current() {
                log::error!("Refusing execution algorithm {actor_id} callback: {error:#}");
            } else if let Some(mut algo) = try_get_actor_unchecked::<T>(&actor_id) {
                if let Err(e) = algo.execute(command.clone()) {
                    log::error!("Error executing command on algorithm {actor_id}: {e}");
                }
            } else {
                log::error!("Execution algorithm {actor_id} not found in registry");
            }
        });
        msgbus::register_any(endpoint.into(), handler);

        self.exec_algorithm_ids.push(exec_algorithm_id);
        self.exec_algorithm_restore_fns
            .insert(exec_algorithm_id, restore_fn);
        self.exec_algorithm_cleanup_fns
            .insert(exec_algorithm_id, cleanup_fn);

        log::info!(
            "Registered execution algorithm {exec_algorithm_id} with trader {}",
            self.trader_id
        );

        Ok(())
    }

    /// Validates that the trader is in a valid state for actor and strategy registration.
    ///
    /// Actors and strategies can be added while the trader is `PreInitialized`, `Ready`,
    /// `Stopped`, or `Running`. This enables the [`Controller`](crate::controller::Controller)
    /// to add them at runtime.
    fn validate_actor_or_strategy_registration(&self) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("register actor or strategy")?;
        match self.state {
            ComponentState::PreInitialized
            | ComponentState::Ready
            | ComponentState::Starting
            | ComponentState::Stopped
            | ComponentState::Running => Ok(()),
            ComponentState::Disposed => {
                anyhow::bail!("Cannot add components to disposed trader")
            }
            _ => anyhow::bail!("Cannot add components in current state: {}", self.state),
        }
    }

    /// Validates that the trader is in a valid state for execution algorithm registration.
    fn validate_exec_algorithm_registration(&self) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("register execution algorithm")?;
        match self.state {
            ComponentState::PreInitialized | ComponentState::Ready | ComponentState::Stopped => {
                Ok(())
            }
            ComponentState::Running => {
                anyhow::bail!("Cannot add execution algorithms to running trader")
            }
            ComponentState::Disposed => {
                anyhow::bail!("Cannot add components to disposed trader")
            }
            _ => anyhow::bail!(
                "Cannot add execution algorithms in current state: {}",
                self.state
            ),
        }
    }

    /// Starts all registered components.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to start.
    pub fn start_components(&mut self) -> anyhow::Result<()> {
        self.ensure_all_strategy_artifacts_admitted()?;
        self.ensure_all_component_identities()?;
        let actor_ids = self.actor_ids.clone();
        let strategy_ids = self.strategy_ids.clone();
        let exec_algorithm_ids = self.exec_algorithm_ids.clone();

        for actor_id in actor_ids {
            log::debug!("Starting actor {actor_id}");
            let identity = self.component_identity(actor_id.inner())?.clone();
            Self::start_component_if_not_running(actor_id.inner(), &identity)?;
        }

        for strategy_id in strategy_ids {
            log::debug!("Starting strategy {strategy_id}");
            let identity = self.component_identity(strategy_id.inner())?.clone();
            Self::start_component_if_not_running(strategy_id.inner(), &identity)?;
        }

        let mut restored_exec_algorithm_ids = Vec::new();

        for exec_algorithm_id in exec_algorithm_ids {
            log::debug!("Starting execution algorithm {exec_algorithm_id}");
            match self.start_exec_algorithm_if_not_running(exec_algorithm_id) {
                Ok(true) => restored_exec_algorithm_ids.push(exec_algorithm_id),
                Ok(false) => {}
                Err(start_err) => {
                    return Err(self.exec_algorithm_start_error_with_rollback(
                        exec_algorithm_id,
                        &restored_exec_algorithm_ids,
                        start_err,
                    ));
                }
            }
        }

        Ok(())
    }

    /// Starts the trader while releasing the trader borrow before component callbacks run.
    ///
    /// # Errors
    ///
    /// Returns an error if the trader state transition or any component startup fails.
    pub fn start_with_component_callbacks(trader: &Rc<RefCell<Self>>) -> anyhow::Result<()> {
        trader.borrow().ensure_all_strategy_artifacts_admitted()?;
        trader.borrow().ensure_all_component_identities()?;
        trader
            .borrow_mut()
            .transition_state(ComponentTrigger::Start)?;

        let (actor_ids, strategy_ids, exec_algorithm_ids) = {
            let trader_ref = trader.borrow();
            (
                trader_ref.actor_ids.clone(),
                trader_ref.strategy_ids.clone(),
                trader_ref.exec_algorithm_ids.clone(),
            )
        };

        for actor_id in actor_ids {
            log::debug!("Starting actor {actor_id}");
            let identity = trader
                .borrow()
                .component_identity(actor_id.inner())?
                .clone();
            Self::start_component_if_not_running(actor_id.inner(), &identity)?;
        }

        for strategy_id in strategy_ids {
            log::debug!("Starting strategy {strategy_id}");
            let identity = trader
                .borrow()
                .component_identity(strategy_id.inner())?
                .clone();
            Self::start_component_if_not_running(strategy_id.inner(), &identity)?;
        }

        let mut restored_exec_algorithm_ids = Vec::new();

        for exec_algorithm_id in exec_algorithm_ids {
            log::debug!("Starting execution algorithm {exec_algorithm_id}");
            let identity = trader
                .borrow()
                .component_identity(exec_algorithm_id.inner())?
                .clone();
            identity.ensure_current()?;
            let component_state = match component_state(&exec_algorithm_id.inner()) {
                Ok(state) => state,
                Err(start_err) => {
                    let e = trader
                        .borrow_mut()
                        .exec_algorithm_start_error_with_rollback(
                            exec_algorithm_id,
                            &restored_exec_algorithm_ids,
                            start_err,
                        );
                    return Err(e);
                }
            };

            if component_state == ComponentState::Running {
                continue;
            }

            if let Err(start_err) = trader
                .borrow_mut()
                .restore_exec_algorithm_subscriptions(exec_algorithm_id)
            {
                let e = trader
                    .borrow_mut()
                    .exec_algorithm_start_error_with_rollback(
                        exec_algorithm_id,
                        &restored_exec_algorithm_ids,
                        start_err,
                    );
                return Err(e);
            }
            restored_exec_algorithm_ids.push(exec_algorithm_id);

            identity.ensure_current()?;
            if let Err(start_err) = start_component(&exec_algorithm_id.inner()) {
                let e = trader
                    .borrow_mut()
                    .exec_algorithm_start_error_with_rollback(
                        exec_algorithm_id,
                        &restored_exec_algorithm_ids,
                        start_err,
                    );
                return Err(e);
            }
        }

        let mut trader_ref = trader.borrow_mut();
        let clock = trader_ref.clock_factory.clock();
        trader_ref.ts_started = Some(clock.borrow().timestamp_ns());
        trader_ref.transition_state(ComponentTrigger::StartCompleted)?;

        Ok(())
    }

    fn start_component_if_not_running(
        component_id: Ustr,
        identity: &RegisteredComponentIdentity,
    ) -> anyhow::Result<()> {
        identity.ensure_current()?;
        if component_state(&component_id)? == ComponentState::Running {
            return Ok(());
        }

        start_component(&component_id)
    }

    fn start_exec_algorithm_if_not_running(
        &mut self,
        exec_algorithm_id: ExecAlgorithmId,
    ) -> anyhow::Result<bool> {
        self.ensure_component_identity(exec_algorithm_id.inner())?;
        if component_state(&exec_algorithm_id.inner())? == ComponentState::Running {
            return Ok(false);
        }

        self.restore_exec_algorithm_subscriptions(exec_algorithm_id)?;
        self.ensure_component_identity(exec_algorithm_id.inner())?;
        if let Err(start_err) = start_component(&exec_algorithm_id.inner()) {
            return match self.cleanup_exec_algorithm_subscriptions(exec_algorithm_id) {
                Ok(()) => Err(start_err),
                Err(cleanup_err) => anyhow::bail!(
                    "Failed to start execution algorithm {exec_algorithm_id}: {start_err:#}; \
                     failed to roll back subscriptions: {cleanup_err:#}"
                ),
            };
        }

        Ok(true)
    }

    fn restore_exec_algorithm_subscriptions(
        &mut self,
        exec_algorithm_id: ExecAlgorithmId,
    ) -> anyhow::Result<()> {
        if let Some(restore_fn) = self.exec_algorithm_restore_fns.get_mut(&exec_algorithm_id) {
            restore_fn()?;
        }
        Ok(())
    }

    fn cleanup_exec_algorithm_subscriptions(
        &mut self,
        exec_algorithm_id: ExecAlgorithmId,
    ) -> anyhow::Result<()> {
        if let Some(cleanup_fn) = self.exec_algorithm_cleanup_fns.get_mut(&exec_algorithm_id) {
            cleanup_fn()?;
        }
        Ok(())
    }

    fn cleanup_exec_algorithm_subscriptions_for(
        &mut self,
        exec_algorithm_ids: &[ExecAlgorithmId],
    ) -> anyhow::Result<()> {
        let mut errors = Vec::new();

        for exec_algorithm_id in exec_algorithm_ids {
            if let Err(e) = self.cleanup_exec_algorithm_subscriptions(*exec_algorithm_id) {
                errors.push(format!("{exec_algorithm_id}: {e:#}"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("{}", errors.join("; "))
        }
    }

    fn exec_algorithm_start_error_with_rollback(
        &mut self,
        exec_algorithm_id: ExecAlgorithmId,
        restored_exec_algorithm_ids: &[ExecAlgorithmId],
        start_err: anyhow::Error,
    ) -> anyhow::Error {
        match self.cleanup_exec_algorithm_subscriptions_for(restored_exec_algorithm_ids) {
            Ok(()) => start_err,
            Err(cleanup_err) => anyhow::anyhow!(
                "Failed while starting execution algorithm {exec_algorithm_id}: {start_err:#}; \
                 failed to roll back restored subscriptions: {cleanup_err:#}"
            ),
        }
    }

    /// Stops all registered components.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to stop.
    pub fn stop_components(&mut self) -> anyhow::Result<()> {
        for actor_id in &self.actor_ids {
            log::debug!("Stopping actor {actor_id}");
            let identity = self.component_identity(actor_id.inner())?;
            Self::stop_component_if_active(actor_id.inner(), identity)?;
        }

        for exec_algorithm_id in &self.exec_algorithm_ids {
            log::debug!("Stopping execution algorithm {exec_algorithm_id}");
            let identity = self.component_identity(exec_algorithm_id.inner())?;
            Self::stop_component_if_active(exec_algorithm_id.inner(), identity)?;
        }

        for strategy_id in self.strategy_ids.clone() {
            log::debug!("Stopping strategy {strategy_id}");
            let identity = self.component_identity(strategy_id.inner())?.clone();
            identity.ensure_current()?;
            let should_proceed = self
                .strategy_stop_fns
                .get_mut(&strategy_id)
                .is_none_or(|stop_fn| stop_fn());

            if should_proceed {
                Self::stop_component_if_active(strategy_id.inner(), &identity)?;
            }
        }

        Ok(())
    }

    /// Stops a partially started trader without deferring managed strategy shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the trader transition or any component stop fails. All registered
    /// components still receive a stop attempt before the error is returned.
    pub fn stop_after_start_failure(&mut self) -> anyhow::Result<()> {
        self.transition_state(ComponentTrigger::Stop)?;

        let stop_result = self.stop_components_after_start_failure();
        let clock = self.clock_factory.clock();
        self.ts_stopped = Some(clock.borrow().timestamp_ns());
        let transition_result = self.transition_state(ComponentTrigger::StopCompleted);

        match (stop_result, transition_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(stop_err), Ok(())) => Err(stop_err),
            (Ok(()), Err(transition_err)) => Err(transition_err),
            (Err(stop_err), Err(transition_err)) => anyhow::bail!(
                "Failed to stop trader components: {stop_err}; failed to complete trader stop: \
                 {transition_err}"
            ),
        }
    }

    fn stop_components_after_start_failure(&mut self) -> anyhow::Result<()> {
        let mut errors = Vec::new();

        for actor_id in &self.actor_ids {
            log::debug!("Stopping actor {actor_id} after startup failure");
            let result = self
                .component_identity(actor_id.inner())
                .and_then(|identity| Self::stop_component_if_active(actor_id.inner(), identity));
            if let Err(e) = result {
                errors.push(format!("actor {actor_id}: {e:#}"));
            }
        }

        for exec_algorithm_id in self.exec_algorithm_ids.clone() {
            log::debug!("Stopping execution algorithm {exec_algorithm_id} after startup failure");
            let result = self
                .component_identity(exec_algorithm_id.inner())
                .and_then(|identity| {
                    Self::stop_component_if_active(exec_algorithm_id.inner(), identity)
                });
            if let Err(e) = result {
                errors.push(format!("execution algorithm {exec_algorithm_id}: {e:#}"));
            }

            if let Err(e) = self.cleanup_exec_algorithm_subscriptions(exec_algorithm_id) {
                errors.push(format!(
                    "execution algorithm {exec_algorithm_id} subscription cleanup: {e:#}"
                ));
            }
        }

        for strategy_id in &self.strategy_ids {
            log::debug!("Stopping strategy {strategy_id} after startup failure");
            let result = self
                .component_identity(strategy_id.inner())
                .and_then(|identity| Self::stop_component_if_active(strategy_id.inner(), identity));
            if let Err(e) = result {
                errors.push(format!("strategy {strategy_id}: {e:#}"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "Failed to stop one or more trader components after startup failure: {}",
                errors.join("; ")
            )
        }
    }

    fn stop_component_if_active(
        component_id: Ustr,
        identity: &RegisteredComponentIdentity,
    ) -> anyhow::Result<()> {
        identity.ensure_current()?;
        if !matches!(
            component_state(&component_id)?,
            ComponentState::Starting | ComponentState::Running
        ) {
            return Ok(());
        }

        stop_component(&component_id)
    }

    /// Resets all registered components.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to reset.
    pub fn reset_components(&mut self) -> anyhow::Result<()> {
        for actor_id in &self.actor_ids {
            log::debug!("Resetting actor {actor_id}");
            self.ensure_component_identity(actor_id.inner())?;
            reset_component(&actor_id.inner())?;
        }

        for strategy_id in &self.strategy_ids {
            log::debug!("Resetting strategy {strategy_id}");
            self.ensure_component_identity(strategy_id.inner())?;
            reset_component(&strategy_id.inner())?;
        }

        for exec_algorithm_id in self.exec_algorithm_ids.clone() {
            log::debug!("Resetting execution algorithm {exec_algorithm_id}");
            self.ensure_component_identity(exec_algorithm_id.inner())?;
            self.cleanup_exec_algorithm_subscriptions(exec_algorithm_id)?;
            self.ensure_component_identity(exec_algorithm_id.inner())?;
            reset_component(&exec_algorithm_id.inner())?;
        }

        Ok(())
    }

    /// Disposes of all registered components.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to dispose.
    pub fn dispose_components(&mut self) -> anyhow::Result<()> {
        for actor_id in &self.actor_ids {
            log::debug!("Disposing actor {actor_id}");
            self.ensure_component_identity(actor_id.inner())?;
            dispose_component(&actor_id.inner())?;
        }

        for strategy_id in &self.strategy_ids {
            log::debug!("Disposing strategy {strategy_id}");
            self.ensure_component_identity(strategy_id.inner())?;
            dispose_component(&strategy_id.inner())?;
            get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .deregister(strategy_control_endpoint(*strategy_id));
        }

        for exec_algorithm_id in self.exec_algorithm_ids.clone() {
            log::debug!("Disposing execution algorithm {exec_algorithm_id}");
            self.ensure_component_identity(exec_algorithm_id.inner())?;
            self.cleanup_exec_algorithm_subscriptions(exec_algorithm_id)?;
            self.ensure_component_identity(exec_algorithm_id.inner())?;
            dispose_component(&exec_algorithm_id.inner())?;
            let endpoint: Ustr = format!("{exec_algorithm_id}.execute").into();
            msgbus::deregister_any(endpoint.into());
        }

        for clock in self.clocks.values() {
            clock.borrow_mut().cancel_timers();
        }

        self.actor_ids.clear();
        self.component_identities.clear();
        self.actor_state_callbacks.clear();
        self.strategy_ids.clear();
        self.strategy_state_callbacks.clear();
        self.strategy_config_verifiers.clear();
        self.strategy_stop_fns.clear();
        self.strategy_handler_ids.clear();
        #[cfg(feature = "strategy-artifact-trust")]
        self.admitted_strategy_artifacts.clear();
        self.exec_algorithm_ids.clear();
        self.exec_algorithm_restore_fns.clear();
        self.exec_algorithm_cleanup_fns.clear();
        self.clocks.clear();

        Ok(())
    }

    /// Clears all registered strategies, disposing each and removing their clocks.
    ///
    /// # Errors
    ///
    /// Returns an error if any strategy fails to dispose.
    pub fn clear_strategies(&mut self) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("clear strategies")?;
        for strategy_id in &self.strategy_ids {
            log::debug!("Disposing strategy {strategy_id}");
            self.ensure_component_identity(strategy_id.inner())?;
            dispose_component(&strategy_id.inner())?;
            let component_id = ComponentId::new(strategy_id.inner().as_str());
            if let Some(clock) = self.clocks.get(&component_id) {
                clock.borrow_mut().cancel_timers();
            }
            self.clocks.shift_remove(&component_id);

            // Remove only this strategy's own msgbus handlers
            if let Some((order_hid, position_hid)) = self.strategy_handler_ids.get(strategy_id) {
                let order_topic = get_event_order_topic(*strategy_id);
                let position_topic = get_event_position_topic(*strategy_id);
                msgbus::remove_order_event_handler(order_topic.into(), *order_hid);
                msgbus::remove_position_event_handler(position_topic.into(), *position_hid);
            }

            get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .deregister(strategy_control_endpoint(*strategy_id));
            self.component_identities.remove(&strategy_id.inner());
        }

        self.strategy_ids.clear();
        self.strategy_state_callbacks.clear();
        self.strategy_config_verifiers.clear();
        self.strategy_stop_fns.clear();
        self.strategy_handler_ids.clear();
        #[cfg(feature = "strategy-artifact-trust")]
        self.admitted_strategy_artifacts.clear();

        Ok(())
    }

    /// Clears all registered actors, disposing each and removing their clocks.
    ///
    /// # Errors
    ///
    /// Returns an error if any actor fails to dispose.
    pub fn clear_actors(&mut self) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("clear actors")?;
        for actor_id in &self.actor_ids {
            log::debug!("Disposing actor {actor_id}");
            self.ensure_component_identity(actor_id.inner())?;
            // Stop if running before disposal; ignore stop failures so a single
            // misbehaving actor does not leave the rest in a half-cleared state.
            let _ = stop_component(&actor_id.inner());
            self.ensure_component_identity(actor_id.inner())?;
            dispose_component(&actor_id.inner())?;
            let component_id = ComponentId::new(actor_id.inner().as_str());
            if let Some(clock) = self.clocks.get(&component_id) {
                clock.borrow_mut().cancel_timers();
            }
            self.clocks.shift_remove(&component_id);
            self.component_identities.remove(&actor_id.inner());
        }

        self.actor_ids.clear();
        self.actor_state_callbacks.clear();

        Ok(())
    }

    /// Clears all registered execution algorithms, disposing each and removing their clocks.
    ///
    /// # Errors
    ///
    /// Returns an error if any execution algorithm fails to dispose.
    pub fn clear_exec_algorithms(&mut self) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("clear execution algorithms")?;
        for exec_algorithm_id in self.exec_algorithm_ids.clone() {
            log::debug!("Disposing execution algorithm {exec_algorithm_id}");
            self.ensure_component_identity(exec_algorithm_id.inner())?;
            self.cleanup_exec_algorithm_subscriptions(exec_algorithm_id)?;
            self.ensure_component_identity(exec_algorithm_id.inner())?;
            dispose_component(&exec_algorithm_id.inner())?;
            let endpoint: Ustr = format!("{exec_algorithm_id}.execute").into();
            msgbus::deregister_any(endpoint.into());
            let component_id = ComponentId::new(exec_algorithm_id.inner().as_str());
            if let Some(clock) = self.clocks.get(&component_id) {
                clock.borrow_mut().cancel_timers();
            }
            self.clocks.shift_remove(&component_id);
            self.exec_algorithm_restore_fns.remove(&exec_algorithm_id);
            self.exec_algorithm_cleanup_fns.remove(&exec_algorithm_id);
            self.component_identities.remove(&exec_algorithm_id.inner());
        }

        self.exec_algorithm_ids.clear();
        self.exec_algorithm_restore_fns.clear();
        self.exec_algorithm_cleanup_fns.clear();

        Ok(())
    }

    // -- Individual component management ----------------------------------------

    /// Starts the actor with the given `actor_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not registered or cannot be started.
    pub fn start_actor(&self, actor_id: &ActorId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("start actor")?;
        if !self.actor_ids.contains(actor_id) {
            anyhow::bail!("Cannot start actor, {actor_id} not found");
        }
        self.ensure_component_identity(actor_id.inner())?;
        start_component(&actor_id.inner())
    }

    /// Stops the actor with the given `actor_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not registered or cannot be stopped.
    pub fn stop_actor(&self, actor_id: &ActorId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("stop actor")?;
        if !self.actor_ids.contains(actor_id) {
            anyhow::bail!("Cannot stop actor, {actor_id} not found");
        }
        self.ensure_component_identity(actor_id.inner())?;
        stop_component(&actor_id.inner())
    }

    /// Removes the actor with the given `actor_id`.
    ///
    /// Will stop the actor first if it is currently running. Disposes the actor
    /// and removes it from the trader's tracking.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not registered.
    pub fn remove_actor(&mut self, actor_id: &ActorId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("remove actor")?;
        let pos = self
            .actor_ids
            .iter()
            .position(|id| id == actor_id)
            .ok_or_else(|| anyhow::anyhow!("Cannot remove actor, {actor_id} not found"))?;

        self.ensure_component_identity(actor_id.inner())?;
        // Stop if running, then dispose
        let _ = stop_component(&actor_id.inner());
        self.ensure_component_identity(actor_id.inner())?;
        dispose_component(&actor_id.inner())?;

        self.actor_ids.swap_remove(pos);
        self.actor_state_callbacks.remove(actor_id);
        self.component_identities.remove(&actor_id.inner());
        let component_id = ComponentId::new(actor_id.inner().as_str());
        if let Some(clock) = self.clocks.get(&component_id) {
            clock.borrow_mut().cancel_timers();
        }
        self.clocks.shift_remove(&component_id);

        log::info!("Removed actor {actor_id} from trader {}", self.trader_id);
        Ok(())
    }

    /// Starts the strategy with the given `strategy_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered or cannot be started.
    pub fn start_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("start strategy")?;
        if !self.strategy_ids.contains(strategy_id) {
            anyhow::bail!("Cannot start strategy, {strategy_id} not found");
        }
        self.ensure_strategy_artifact_admitted(*strategy_id)?;
        self.ensure_artifact_configuration_current(*strategy_id)?;
        self.ensure_component_identity(strategy_id.inner())?;
        start_component(&strategy_id.inner())
    }

    /// Resumes a stopped strategy without resetting its inventory or signals.
    ///
    /// # Errors
    /// Returns an error if identity, artifact admission or lifecycle checks fail.
    pub fn resume_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("resume strategy")?;
        if !self.strategy_ids.contains(strategy_id) {
            anyhow::bail!("Cannot resume strategy, {strategy_id} not found");
        }
        self.ensure_strategy_artifact_admitted(*strategy_id)?;
        self.ensure_artifact_configuration_current(*strategy_id)?;
        self.ensure_component_identity(strategy_id.inner())?;
        resume_component(&strategy_id.inner())
    }

    /// Stops the strategy with the given `strategy_id`.
    ///
    /// Respects the `manage_stop` behavior - if the strategy's stop function
    /// returns `false`, the component stop is deferred until market exit completes.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered or cannot be stopped.
    pub fn stop_strategy(&mut self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("stop strategy")?;
        if !self.strategy_ids.contains(strategy_id) {
            anyhow::bail!("Cannot stop strategy, {strategy_id} not found");
        }

        let identity = self.component_identity(strategy_id.inner())?.clone();
        identity.ensure_current()?;

        let should_proceed = self
            .strategy_stop_fns
            .get_mut(strategy_id)
            .is_none_or(|stop_fn| stop_fn());

        if should_proceed {
            identity.ensure_current()?;
            stop_component(&strategy_id.inner())?;
        }

        Ok(())
    }

    /// Exits the market for the strategy with the given `strategy_id`.
    ///
    /// Sends a strategy command to the strategy's control endpoint. The strategy
    /// then performs its own managed market exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered or its control endpoint is missing.
    pub fn market_exit_strategy(
        trader: &Rc<RefCell<Self>>,
        strategy_id: &StrategyId,
    ) -> anyhow::Result<()> {
        trader
            .borrow()
            .ensure_runtime_topology_mutation_allowed("market-exit strategy")?;
        let handler = trader.borrow().strategy_command_handler(*strategy_id)?;
        handler.handle(&StrategyCommand::ExitMarket);
        Ok(())
    }

    fn strategy_command_handler(
        &self,
        strategy_id: StrategyId,
    ) -> anyhow::Result<TypedHandler<StrategyCommand>> {
        if !self.strategy_ids.contains(&strategy_id) {
            anyhow::bail!("Cannot market exit strategy, {strategy_id} not found");
        }

        let endpoint = strategy_control_endpoint(strategy_id);
        let handler = {
            let msgbus = get_message_bus();
            msgbus
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .get(endpoint)
                .cloned()
        };

        let Some(handler) = handler else {
            anyhow::bail!(
                "Cannot exit market for strategy {strategy_id}: control endpoint '{}' not registered",
                endpoint.as_str()
            );
        };

        Ok(handler)
    }

    /// Removes the strategy with the given `strategy_id`.
    ///
    /// Will stop the strategy first if it is currently running. Disposes the strategy
    /// and removes it from the trader's tracking along with its event subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered.
    pub fn remove_strategy(&mut self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        self.ensure_runtime_topology_mutation_allowed("remove strategy")?;
        let pos = self
            .strategy_ids
            .iter()
            .position(|id| id == strategy_id)
            .ok_or_else(|| anyhow::anyhow!("Cannot remove strategy, {strategy_id} not found"))?;

        self.ensure_component_identity(strategy_id.inner())?;
        // Stop if running, then dispose
        let _ = stop_component(&strategy_id.inner());
        self.ensure_component_identity(strategy_id.inner())?;
        dispose_component(&strategy_id.inner())?;

        // Clean up event subscriptions
        if let Some((order_hid, position_hid)) = self.strategy_handler_ids.remove(strategy_id) {
            let order_topic = get_event_order_topic(*strategy_id);
            let position_topic = get_event_position_topic(*strategy_id);
            msgbus::remove_order_event_handler(order_topic.into(), order_hid);
            msgbus::remove_position_event_handler(position_topic.into(), position_hid);
        }

        get_message_bus()
            .borrow_mut()
            .endpoint_map::<StrategyCommand>()
            .deregister(strategy_control_endpoint(*strategy_id));

        self.strategy_ids.swap_remove(pos);
        self.strategy_state_callbacks.remove(strategy_id);
        self.strategy_config_verifiers.remove(strategy_id);
        self.strategy_stop_fns.remove(strategy_id);
        self.component_identities.remove(&strategy_id.inner());
        #[cfg(feature = "strategy-artifact-trust")]
        self.admitted_strategy_artifacts
            .remove(&strategy_id.to_string());
        let component_id = ComponentId::new(strategy_id.inner().as_str());
        if let Some(clock) = self.clocks.get(&component_id) {
            clock.borrow_mut().cancel_timers();
        }
        self.clocks.shift_remove(&component_id);

        log::info!(
            "Removed strategy {strategy_id} from trader {}",
            self.trader_id
        );
        Ok(())
    }

    #[expect(
        clippy::unused_self,
        reason = "method form keeps the authoritative-capture topology policy attached to Trader"
    )]
    fn ensure_runtime_topology_mutation_allowed(&self, operation: &str) -> anyhow::Result<()> {
        if msgbus::is_exclusive_bus_tap_installed() {
            anyhow::bail!(
                "Cannot {operation} while authoritative event-store capture is active: runtime component topology replay is not implemented"
            );
        }
        Ok(())
    }

    // -- Lifecycle management ---------------------------------------------------

    /// Loads persisted actor and strategy state in registration order.
    ///
    /// Empty state and a cache without database backing do not invoke component callbacks.
    ///
    /// # Errors
    ///
    /// Returns an error if state cannot be loaded or a component callback fails.
    pub(crate) fn load_state(trader: &Rc<RefCell<Self>>) -> anyhow::Result<()> {
        let (cache, actor_callbacks, strategy_callbacks) = {
            let trader = trader.borrow();
            let actor_callbacks = trader.actor_state_callbacks()?;
            let strategy_callbacks = trader.strategy_state_callbacks()?;

            (trader.cache.clone(), actor_callbacks, strategy_callbacks)
        };

        if !cache.borrow().has_backing() {
            return Ok(());
        }

        for (actor_id, callbacks) in actor_callbacks {
            let component_id = ComponentId::new(actor_id.inner().as_str());
            let state = cache
                .borrow()
                .load_actor_state(&component_id)
                .map_err(|e| anyhow::anyhow!("Failed to load actor {actor_id} state: {e:#}"))?;
            let Some(state) = state.filter(|state| !state.is_empty()) else {
                continue;
            };

            callbacks
                .load(actor_id.inner(), state)
                .map_err(|e| anyhow::anyhow!("Failed to restore actor {actor_id} state: {e:#}"))?;
        }

        for (strategy_id, callbacks) in strategy_callbacks {
            let state = cache
                .borrow()
                .load_strategy_state(&strategy_id)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to load strategy {strategy_id} state: {e:#}")
                })?;
            let Some(state) = state.filter(|state| !state.is_empty()) else {
                continue;
            };

            callbacks.load(strategy_id.inner(), state).map_err(|e| {
                anyhow::anyhow!("Failed to restore strategy {strategy_id} state: {e:#}")
            })?;
        }

        Ok(())
    }

    /// Saves actor and strategy state in registration order.
    ///
    /// Empty state is persisted, while a cache without database backing does not invoke
    /// component callbacks. All callbacks and updates receive an attempt before errors return.
    ///
    /// # Errors
    ///
    /// Returns an error containing every component callback or persistence failure.
    pub(crate) fn save_state(trader: &Rc<RefCell<Self>>) -> anyhow::Result<()> {
        let (cache, actor_callbacks, strategy_callbacks) = {
            let trader = trader.borrow();
            let actor_callbacks = trader.actor_state_callbacks()?;
            let strategy_callbacks = trader.strategy_state_callbacks()?;

            (trader.cache.clone(), actor_callbacks, strategy_callbacks)
        };

        if !cache.borrow().has_backing() {
            return Ok(());
        }

        let mut errors = Vec::new();

        for (actor_id, callbacks) in actor_callbacks {
            match callbacks.save(actor_id.inner()) {
                Ok(state) => {
                    let component_id = ComponentId::new(actor_id.inner().as_str());
                    if let Err(e) = cache.borrow().update_actor_state(&component_id, &state) {
                        errors.push(format!("actor {actor_id} persistence: {e:#}"));
                    }
                }
                Err(e) => errors.push(format!("actor {actor_id} callback: {e:#}")),
            }
        }

        for (strategy_id, callbacks) in strategy_callbacks {
            match callbacks.save(strategy_id.inner()) {
                Ok(state) => {
                    if let Err(e) = cache.borrow().update_strategy_state(&strategy_id, &state) {
                        errors.push(format!("strategy {strategy_id} persistence: {e:#}"));
                    }
                }
                Err(e) => errors.push(format!("strategy {strategy_id} callback: {e:#}")),
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("Failed to save component state: {}", errors.join("; "))
        }
    }

    fn actor_state_callbacks(&self) -> anyhow::Result<Vec<(ActorId, ComponentStateCallbacks)>> {
        self.actor_ids
            .iter()
            .map(|actor_id| {
                self.actor_state_callbacks
                    .get(actor_id)
                    .copied()
                    .map(|callbacks| (*actor_id, callbacks))
                    .ok_or_else(|| anyhow::anyhow!("Actor {actor_id} state callback not found"))
            })
            .collect()
    }

    fn strategy_state_callbacks(
        &self,
    ) -> anyhow::Result<Vec<(StrategyId, ComponentStateCallbacks)>> {
        self.strategy_ids
            .iter()
            .map(|strategy_id| {
                self.strategy_state_callbacks
                    .get(strategy_id)
                    .copied()
                    .map(|callbacks| (*strategy_id, callbacks))
                    .ok_or_else(|| {
                        anyhow::anyhow!("Strategy {strategy_id} state callback not found")
                    })
            })
            .collect()
    }

    fn load_component_state<T>(
        component_id: Ustr,
        state: PersistedComponentState,
    ) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Debug + 'static,
    {
        let mut component = try_get_actor_unchecked::<T>(&component_id).ok_or_else(|| {
            anyhow::anyhow!("Component {component_id} not found in actor registry")
        })?;
        component.on_load(state)
    }

    fn save_component_state<T>(component_id: Ustr) -> anyhow::Result<PersistedComponentState>
    where
        T: DataActor + DataActorNative + Debug + 'static,
    {
        let component = try_get_actor_unchecked::<T>(&component_id).ok_or_else(|| {
            anyhow::anyhow!("Component {component_id} not found in actor registry")
        })?;
        component.on_save()
    }

    /// Initializes the trader, transitioning from `PreInitialized` to `Ready` state.
    ///
    /// This method must be called before starting the trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the trader cannot be initialized from its current state.
    pub fn initialize(&mut self) -> anyhow::Result<()> {
        let new_state = self.state.transition(&ComponentTrigger::Initialize)?;
        self.state = new_state;

        Ok(())
    }

    fn on_start(&mut self) -> anyhow::Result<()> {
        self.start_components()?;

        // Transition to running state
        let clock = self.clock_factory.clock();
        self.ts_started = Some(clock.borrow().timestamp_ns());

        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.stop_components()?;

        let clock = self.clock_factory.clock();
        self.ts_stopped = Some(clock.borrow().timestamp_ns());

        Ok(())
    }

    fn on_reset(&mut self) -> anyhow::Result<()> {
        self.reset_components()?;

        self.ts_started = None;
        self.ts_stopped = None;

        Ok(())
    }

    fn on_dispose(&mut self) -> anyhow::Result<()> {
        if self.is_running() {
            self.stop()?;
        }

        self.dispose_components()?;

        Ok(())
    }
}

impl Component for Trader {
    fn component_id(&self) -> ComponentId {
        ComponentId::new(format!("Trader-{}", self.trader_id))
    }

    fn state(&self) -> ComponentState {
        self.state
    }

    fn transition_state(&mut self, trigger: ComponentTrigger) -> anyhow::Result<()> {
        self.state = self.state.transition(&trigger)?;
        log::info!("{}", self.state.variant_name());
        Ok(())
    }

    fn register(
        &mut self,
        _trader_id: TraderId,
        _clock: Rc<RefCell<dyn Clock>>,
        _cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Trader cannot register with itself")
    }

    fn on_start(&mut self) -> anyhow::Result<()> {
        Self::on_start(self)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        Self::on_stop(self)
    }

    fn on_reset(&mut self) -> anyhow::Result<()> {
        Self::on_reset(self)
    }

    fn on_dispose(&mut self) -> anyhow::Result<()> {
        Self::on_dispose(self)
    }
}

#[cfg(feature = "python")]
fn create_python_actor(config: &ImportableActorConfig) -> anyhow::Result<(Py<PyAny>, ActorId)> {
    let (module_name, class_name) = split_import_path(&config.actor_path, "actor_path")?;

    log::info!("Importing actor from module: {module_name} class: {class_name}");

    Python::attach(|py| -> anyhow::Result<(Py<PyAny>, ActorId)> {
        let actor_class = import_python_class(py, module_name, class_name)?;
        let config_instance = create_config_instance(py, &config.config_path, &config.config)?;

        let python_actor = if let Some(config_obj) = config_instance.as_ref() {
            actor_class.call1((config_obj,))?
        } else {
            actor_class.call0()?
        };

        let mut py_data_actor_ref = python_actor
            .extract::<PyRefMut<PyDataActor>>()
            .map_err(Into::<PyErr>::into)
            .map_err(|e| anyhow::anyhow!("Failed to extract PyDataActor: {e}"))?;

        if let Some(config_obj) = config_instance.as_ref() {
            configure_py_data_actor(&mut py_data_actor_ref, config_obj)?;
        }

        py_data_actor_ref.set_python_instance(python_actor.clone().unbind());
        let actor_id = py_data_actor_ref.actor_id();

        Ok((python_actor.unbind(), actor_id))
    })
}

#[cfg(feature = "python")]
fn create_python_strategy(
    config: &ImportableStrategyConfig,
) -> anyhow::Result<(Py<PyAny>, StrategyId)> {
    let (module_name, class_name) = split_import_path(&config.strategy_path, "strategy_path")?;

    log::info!("Importing strategy from module: {module_name} class: {class_name}");

    Python::attach(|py| -> anyhow::Result<(Py<PyAny>, StrategyId)> {
        let strategy_class = import_python_class(py, module_name, class_name)?;
        let config_instance = create_config_instance(py, &config.config_path, &config.config)?;

        let python_strategy = if let Some(config_obj) = config_instance.as_ref() {
            strategy_class.call1((config_obj,))?
        } else {
            strategy_class.call0()?
        };

        let mut py_strategy_ref = python_strategy
            .extract::<PyRefMut<PyStrategy>>()
            .map_err(Into::<PyErr>::into)
            .map_err(|e| anyhow::anyhow!("Failed to extract PyStrategy: {e}"))?;

        if let Some(config_obj) = config_instance.as_ref() {
            configure_py_strategy(&mut py_strategy_ref, config_obj)?;
        }

        py_strategy_ref.set_python_instance(python_strategy.clone().unbind());
        let strategy_id = py_strategy_ref.strategy_id();

        Ok((python_strategy.unbind(), strategy_id))
    })
}

#[cfg(feature = "python")]
fn split_import_path<'a>(path: &'a str, field: &str) -> anyhow::Result<(&'a str, &'a str)> {
    let Some((module_name, class_name)) = path.split_once(':') else {
        anyhow::bail!("{field} must be in format 'module.path:ClassName'");
    };

    if module_name.is_empty() || class_name.is_empty() || class_name.contains(':') {
        anyhow::bail!("{field} must be in format 'module.path:ClassName'");
    }

    Ok((module_name, class_name))
}

#[cfg(feature = "python")]
fn import_python_class<'py>(
    py: Python<'py>,
    module_name: &str,
    class_name: &str,
) -> anyhow::Result<Bound<'py, PyAny>> {
    let module = py
        .import(module_name)
        .map_err(|e| anyhow::anyhow!("Failed to import module {module_name}: {e}"))?;

    module
        .getattr(class_name)
        .map_err(|e| anyhow::anyhow!("Failed to get class {class_name}: {e}"))
}

#[cfg(feature = "python")]
fn create_config_instance<'py>(
    py: Python<'py>,
    config_path: &str,
    config: &std::collections::HashMap<String, serde_json::Value>,
) -> anyhow::Result<Option<Bound<'py, PyAny>>> {
    if config_path.is_empty() && config.is_empty() {
        log::debug!("No config_path or empty config, using None");
        return Ok(None);
    }

    let Some((config_module_name, config_class_name)) = config_path.split_once(':') else {
        anyhow::bail!("config_path must be in format 'module.path:ClassName', was {config_path}");
    };

    if config_module_name.is_empty()
        || config_class_name.is_empty()
        || config_class_name.contains(':')
    {
        anyhow::bail!("config_path must be in format 'module.path:ClassName', was {config_path}");
    }

    log::debug!(
        "Importing config class from module: {config_module_name} class: {config_class_name}"
    );

    let config_module = py
        .import(config_module_name)
        .map_err(|e| anyhow::anyhow!("Failed to import config module {config_module_name}: {e}"))?;
    let config_class = config_module
        .getattr(config_class_name)
        .map_err(|e| anyhow::anyhow!("Failed to get config class {config_class_name}: {e}"))?;
    let py_dict = PyDict::new(py);

    for (key, value) in config {
        let py_value = config_value_to_py(py, key, value)?;
        py_dict.set_item(key, py_value)?;
    }

    let config_instance = match config_class.call((), Some(&py_dict)) {
        Ok(instance) => instance,
        Err(kwargs_err) => match config_class.call0() {
            Ok(instance) => {
                for (key, value) in config {
                    let py_value = config_value_to_py(py, key, value)?;

                    if let Err(setattr_err) = instance.setattr(key, py_value) {
                        log::warn!("Failed to set attribute {key}: {setattr_err}");
                    }
                }

                if instance.hasattr("__post_init__")? {
                    instance.call_method0("__post_init__")?;
                }

                instance
            }
            Err(default_err) => {
                anyhow::bail!(
                    "Failed to create config instance. Tried kwargs: {kwargs_err}, default: {default_err}"
                );
            }
        },
    };

    Ok(Some(config_instance))
}

#[cfg(feature = "python")]
fn config_value_to_py<'py>(
    py: Python<'py>,
    key: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Bound<'py, PyAny>> {
    if key == "actor_id"
        && let Some(actor_id) = value.as_str()
    {
        return Ok(ActorId::new_checked(actor_id)?
            .into_pyobject(py)?
            .into_any());
    }

    let json_str = serde_json::to_string(value)
        .map_err(|e| anyhow::anyhow!("Failed to serialize config value: {e}"))?;

    Ok(PyModule::import(py, "json")?
        .call_method("loads", (json_str,), None)?
        .into_any())
}

#[cfg(feature = "python")]
fn configure_py_data_actor(
    actor: &mut PyRefMut<'_, PyDataActor>,
    config_obj: &Bound<'_, PyAny>,
) -> anyhow::Result<()> {
    if let Some(actor_id) = config_obj
        .getattr("actor_id")
        .ok()
        .filter(|value| !value.is_none())
    {
        let actor_id = if let Ok(actor_id) = actor_id.extract::<ActorId>() {
            actor_id
        } else if let Ok(actor_id_str) = actor_id.extract::<String>() {
            ActorId::new_checked(&actor_id_str)?
        } else {
            anyhow::bail!("Invalid `actor_id` type");
        };
        actor.set_actor_id(actor_id);
    }

    if let Some(log_events) = extract_bool_config_attr(config_obj, "log_events") {
        actor.set_log_events(log_events);
    }

    if let Some(log_commands) = extract_bool_config_attr(config_obj, "log_commands") {
        actor.set_log_commands(log_commands);
    }

    Ok(())
}

#[cfg(feature = "python")]
fn configure_py_strategy(
    strategy: &mut PyRefMut<'_, PyStrategy>,
    config_obj: &Bound<'_, PyAny>,
) -> anyhow::Result<()> {
    if let Some(strategy_id) = config_obj
        .getattr("strategy_id")
        .ok()
        .filter(|value| !value.is_none())
    {
        let strategy_id = if let Ok(strategy_id) = strategy_id.extract::<StrategyId>() {
            strategy_id
        } else if let Ok(strategy_id_str) = strategy_id.extract::<String>() {
            StrategyId::new_checked(&strategy_id_str)?
        } else {
            anyhow::bail!("Invalid `strategy_id` type");
        };
        strategy.set_strategy_id(strategy_id)?;
    }

    if let Some(order_id_tag) = config_obj
        .getattr("order_id_tag")
        .ok()
        .filter(|value| !value.is_none())
    {
        let order_id_tag = order_id_tag
            .extract::<String>()
            .map_err(|e| anyhow::anyhow!("Invalid `order_id_tag` type: {e}"))?;
        strategy.set_order_id_tag(&order_id_tag)?;
    }

    if let Some(log_events) = extract_bool_config_attr(config_obj, "log_events") {
        strategy.set_log_events(log_events);
    }

    if let Some(log_commands) = extract_bool_config_attr(config_obj, "log_commands") {
        strategy.set_log_commands(log_commands);
    }

    Ok(())
}

#[cfg(feature = "python")]
fn extract_bool_config_attr(config_obj: &Bound<'_, PyAny>, attr: &str) -> Option<bool> {
    config_obj
        .getattr(attr)
        .ok()
        .and_then(|value| value.extract::<bool>().ok())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "strategy-artifact-trust")]
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    #[cfg(feature = "strategy-artifact-trust")]
    use ed25519_dalek::{Signer, SigningKey};
    #[cfg(feature = "strategy-artifact-trust")]
    use nautilus_analysis::{
        ArtifactReference, EvaluationGate, EvaluationSuite, Observations, PromotionAttestation,
    };
    use nautilus_common::{
        actor::{
            DataActorCore,
            data_actor::DataActorConfig,
            registry::{get_actor_unchecked, try_get_actor_unchecked},
        },
        cache::Cache,
        clock::TestClock,
        enums::{ComponentState, Environment},
        msgbus,
        msgbus::{MessageBus, TypedHandler, switchboard::get_event_order_topic},
        nautilus_actor,
    };
    use nautilus_core::UUID4;
    #[cfg(feature = "strategy-artifact-trust")]
    use nautilus_core::UnixNanos;
    use nautilus_data::engine::{DataEngine, config::DataEngineConfig};
    use nautilus_execution::engine::{ExecutionEngine, config::ExecutionEngineConfig};
    use nautilus_model::{
        enums::{OrderType, PositionAdjustmentType},
        events::{
            OrderAccepted, OrderFilled, OrderRejected, OrderUpdated, PositionAdjusted,
            order::spec::{OrderFilledSpec, OrderRejectedSpec, OrderUpdatedSpec},
        },
        identifiers::{
            AccountId, ActorId, ClientOrderId, ComponentId, InstrumentId, PositionId, TraderId,
        },
        orders::{OrderAny, OrderTestBuilder},
        stubs::TestDefault,
        types::Quantity,
    };
    #[cfg(feature = "strategy-artifact-trust")]
    use nautilus_plugin::{
        DetachedSignature, StrategyArtifactManifest, StrategyArtifactReference, TrustPolicy,
        artifact_content_digest,
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_risk::engine::{RiskEngine, config::RiskEngineConfig};
    #[cfg(feature = "python")]
    use nautilus_testkit::cache::TestCacheDatabaseControl;
    use nautilus_trading::{
        ExecutionAlgorithmConfig, ExecutionAlgorithmCore, StrategyNative,
        nautilus_execution_algorithm, nautilus_strategy,
        strategy::{config::StrategyConfig, core::StrategyCore},
    };
    #[cfg(feature = "python")]
    use pyo3::ffi::c_str;
    use rstest::rstest;
    #[cfg(feature = "strategy-artifact-trust")]
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::clock_factory::ClockFactory;

    // Simple DataActor wrapper for testing
    #[derive(Debug)]
    struct TestDataActor {
        core: DataActorCore,
    }

    impl TestDataActor {
        fn new(config: DataActorConfig) -> Self {
            Self {
                core: DataActorCore::new(config),
            }
        }
    }

    impl DataActor for TestDataActor {}

    nautilus_actor!(TestDataActor);

    // Simple ExecutionAlgorithm wrapper for testing
    #[derive(Debug)]
    struct TestExecAlgorithm {
        core: ExecutionAlgorithmCore,
        fail_start: bool,
        rejected_events: usize,
        updated_events: usize,
        filled_events: usize,
        position_events: usize,
    }

    impl TestExecAlgorithm {
        fn new(config: ExecutionAlgorithmConfig) -> Self {
            Self {
                core: ExecutionAlgorithmCore::new(config),
                fail_start: false,
                rejected_events: 0,
                updated_events: 0,
                filled_events: 0,
                position_events: 0,
            }
        }
    }

    impl DataActor for TestExecAlgorithm {
        fn on_start(&mut self) -> anyhow::Result<()> {
            if self.fail_start {
                anyhow::bail!("test execution algorithm start failure");
            }
            Ok(())
        }
    }

    nautilus_execution_algorithm!(TestExecAlgorithm, {
        fn on_order(&mut self, _order: OrderAny) -> anyhow::Result<()> {
            Ok(())
        }

        fn on_order_rejected(&mut self, _event: OrderRejected) {
            self.rejected_events += 1;
        }

        fn on_order_updated(&mut self, _event: OrderUpdated) {
            self.updated_events += 1;
        }

        fn on_algo_order_filled(&mut self, _event: OrderFilled) {
            self.filled_events += 1;
        }

        fn on_position_event(&mut self, _event: PositionEvent) {
            self.position_events += 1;
        }
    });

    fn add_cached_exec_order(
        cache: &Rc<RefCell<Cache>>,
        client_order_id: ClientOrderId,
        strategy_id: StrategyId,
        exec_algorithm_id: Option<ExecAlgorithmId>,
        is_terminal: bool,
    ) -> OrderAny {
        let mut builder = OrderTestBuilder::new(OrderType::Market);
        builder
            .client_order_id(client_order_id)
            .strategy_id(strategy_id)
            .instrument_id(InstrumentId::test_default())
            .quantity(Quantity::from(1));

        if let Some(exec_algorithm_id) = exec_algorithm_id {
            builder
                .exec_algorithm_id(exec_algorithm_id)
                .exec_spawn_id(client_order_id);
        }

        let order = builder.build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();

        if is_terminal {
            let event = OrderEventAny::Rejected(
                OrderRejectedSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(order.client_order_id())
                    .account_id(AccountId::test_default())
                    .reason("TEST_TERMINAL".into())
                    .build(),
            );
            cache.borrow_mut().update_order(&event).unwrap();
        }

        order
    }

    // Simple Strategy wrapper for testing
    #[derive(Debug)]
    struct TestStrategy {
        core: StrategyCore,
    }

    impl TestStrategy {
        fn new(config: StrategyConfig) -> Self {
            Self {
                core: StrategyCore::new(config),
            }
        }
    }

    impl DataActor for TestStrategy {}

    nautilus_strategy!(TestStrategy);

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    struct MutableArtifactConfig {
        strategy_id: StrategyId,
        max_order_quantity: Cell<u64>,
    }

    #[derive(Debug)]
    struct MutableArtifactConfigStrategy {
        core: StrategyCore,
        artifact_config: MutableArtifactConfig,
        start_calls: usize,
    }

    impl DataActor for MutableArtifactConfigStrategy {
        fn on_start(&mut self) -> anyhow::Result<()> {
            self.start_calls += 1;
            Ok(())
        }
    }

    nautilus_strategy!(MutableArtifactConfigStrategy);

    impl ArtifactConfiguredStrategy for MutableArtifactConfigStrategy {
        type ArtifactConfig = MutableArtifactConfig;

        fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            Ok(Self {
                core: StrategyCore::new(StrategyConfig {
                    strategy_id: Some(config.strategy_id),
                    ..Default::default()
                }),
                artifact_config: config,
                start_calls: 0,
            })
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            &self.artifact_config
        }
    }

    #[cfg(feature = "strategy-artifact-trust")]
    impl ArtifactConfiguredStrategy for TestStrategy {
        type ArtifactConfig = StrategyConfig;

        fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            Ok(Self::new(config))
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            self.core.config()
        }
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[derive(Debug)]
    struct IgnoringArtifactConfigStrategy {
        core: StrategyCore,
    }

    #[cfg(feature = "strategy-artifact-trust")]
    impl DataActor for IgnoringArtifactConfigStrategy {}

    #[cfg(feature = "strategy-artifact-trust")]
    nautilus_strategy!(IgnoringArtifactConfigStrategy);

    #[cfg(feature = "strategy-artifact-trust")]
    impl ArtifactConfiguredStrategy for IgnoringArtifactConfigStrategy {
        type ArtifactConfig = StrategyConfig;

        fn from_artifact_config(_config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            Ok(Self {
                core: StrategyCore::new(StrategyConfig::default()),
            })
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            self.core.config()
        }
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[derive(Debug)]
    struct PreflightSideEffectStrategy {
        core: StrategyCore,
    }

    #[cfg(feature = "strategy-artifact-trust")]
    static PREFLIGHT_CONSTRUCTOR_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cfg(feature = "strategy-artifact-trust")]
    impl DataActor for PreflightSideEffectStrategy {}

    #[cfg(feature = "strategy-artifact-trust")]
    nautilus_strategy!(PreflightSideEffectStrategy);

    #[cfg(feature = "strategy-artifact-trust")]
    impl ArtifactConfiguredStrategy for PreflightSideEffectStrategy {
        type ArtifactConfig = StrategyConfig;

        fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            PREFLIGHT_CONSTRUCTOR_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(Self {
                core: StrategyCore::new(config),
            })
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            self.core.config()
        }
    }

    #[cfg(feature = "strategy-artifact-trust")]
    fn strategy_config_artifact(strategy_id: StrategyId) -> Vec<u8> {
        serde_json::to_vec(&StrategyConfig {
            strategy_id: Some(strategy_id),
            ..Default::default()
        })
        .unwrap()
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[expect(
        clippy::too_many_lines,
        reason = "the end-to-end trust fixture intentionally assembles both signatures and every v5 policy claim"
    )]
    fn verified_strategy_artifact_for(
        strategy_id: &str,
        artifact: &[u8],
        entrypoint: &str,
        config_artifact: &[u8],
        policy_epoch: u64,
    ) -> (VerifiedStrategyArtifact, TrustPolicy) {
        const KEY_BYTES: [u8; 32] = [19; 32];
        const EVALUATOR_KEY_BYTES: [u8; 32] = [23; 32];
        let signing_key = SigningKey::from_bytes(&KEY_BYTES);
        let evaluator_signing_key = SigningKey::from_bytes(&EVALUATOR_KEY_BYTES);
        let policy_digest = artifact_content_digest(b"admission-policy");
        let promotion_digest = artifact_content_digest(b"promotion-attestation");
        let market_scope_commitment = artifact_content_digest(b"test-market-scope");
        let mut manifest = StrategyArtifactManifest {
            schema_version: nautilus_plugin::STRATEGY_ARTIFACT_MANIFEST_VERSION,
            artifact_content_digest: artifact_content_digest(artifact),
            artifact_id: "strategy:test-admission:1.0.0".to_string(),
            runtime_artifact: nautilus_plugin::StrategyRuntimeArtifact::HostExecutable,
            entrypoint: entrypoint.to_string(),
            strategy_id: strategy_id.to_string(),
            strategy_version: "1.0.0".to_string(),
            target_triple: "test-target".to_string(),
            nautilus_abi_version: nautilus_plugin::NAUTILUS_PLUGIN_ABI_VERSION,
            nautilus_version: env!("CARGO_PKG_VERSION").to_string(),
            precision_mode: "test-precision".to_string(),
            policy_digest,
            promotion_evaluator_key_id: "test-evaluator-key".to_string(),
            promotion_suite_definition_digest: artifact_content_digest(b"evaluation-suite"),
            promotion_attestation_digest: promotion_digest,
            evaluated_run_provenance_digest: artifact_content_digest(b"test-run-provenance"),
            evaluated_market_scope_commitment: market_scope_commitment,
            strategy_config_artifact_id: "config:test-admission:1.0.0".to_string(),
            strategy_config_digest: artifact_content_digest(config_artifact),
            deployment_scope: "test-live-node".to_string(),
            signer_key_id: "test-release-key".to_string(),
        };
        let suite = EvaluationSuite::new(
            "release-promotion",
            "1",
            vec![EvaluationGate::Minimum {
                metric: "score".to_string(),
                minimum: 1.0,
            }],
        )
        .unwrap();
        let outcome = suite.evaluate(&Observations::from([("score".to_string(), 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new(
                manifest.artifact_id.clone(),
                manifest.artifact_content_digest.to_string(),
            )
            .unwrap(),
            ArtifactReference::new("backtest-result:test-run", promotion_digest.to_string())
                .unwrap(),
            manifest.evaluated_run_provenance_digest.to_string(),
            manifest.evaluated_market_scope_commitment.to_string(),
            ArtifactReference::new(
                manifest.strategy_config_artifact_id.clone(),
                manifest.strategy_config_digest.to_string(),
            )
            .unwrap(),
            manifest.entrypoint.clone(),
            manifest.strategy_id.clone(),
            None,
            manifest.deployment_scope.clone(),
            UnixNanos::new(42),
        )
        .unwrap();
        let evaluator_signature = evaluator_signing_key
            .sign(
                &attestation
                    .evaluator_signing_payload(&manifest.promotion_evaluator_key_id)
                    .unwrap(),
            )
            .to_bytes();
        let signed_attestation = attestation
            .into_signed(
                manifest.promotion_evaluator_key_id.clone(),
                evaluator_signature,
            )
            .unwrap();
        manifest.promotion_suite_definition_digest = nautilus_plugin::ArtifactDigest(
            *signed_attestation
                .attestation()
                .suite_definition_digest()
                .as_bytes(),
        );
        manifest.promotion_attestation_digest =
            nautilus_plugin::ArtifactDigest(*signed_attestation.envelope_digest().as_bytes());
        let approved_promotion_digest = manifest.promotion_attestation_digest;
        let approved_suite_definition_digest = manifest.promotion_suite_definition_digest;
        let signature = signing_key.sign(&manifest.signing_payload()).to_bytes();
        let reference = StrategyArtifactReference {
            manifest,
            signature: DetachedSignature::new(signature),
        };
        let policy = TrustPolicy {
            policy_epoch,
            trusted_signing_keys: BTreeMap::from([(
                "test-release-key".to_string(),
                signing_key.verifying_key().to_bytes().to_vec(),
            )]),
            revoked_signing_key_ids: BTreeSet::new(),
            trusted_evaluator_keys: BTreeMap::from([(
                "test-evaluator-key".to_string(),
                evaluator_signing_key.verifying_key().to_bytes().to_vec(),
            )]),
            revoked_evaluator_key_ids: BTreeSet::new(),
            expected_target_triple: "test-target".to_string(),
            expected_nautilus_abi_version: nautilus_plugin::NAUTILUS_PLUGIN_ABI_VERSION,
            expected_nautilus_version: env!("CARGO_PKG_VERSION").to_string(),
            expected_precision_mode: "test-precision".to_string(),
            expected_policy_digest: policy_digest,
            expected_promotion_attestation_digest: approved_promotion_digest,
            expected_market_scope_commitment: market_scope_commitment,
            expected_deployment_scope: "test-live-node".to_string(),
            expected_promotion_suite_id: "release-promotion".to_string(),
            expected_promotion_suite_version: "1".to_string(),
            expected_promotion_suite_definition_digest: approved_suite_definition_digest,
        };
        let verified = reference
            .verify(artifact, &signed_attestation, &policy)
            .unwrap();
        (verified, policy)
    }

    #[cfg(feature = "strategy-artifact-trust")]
    fn verified_strategy_artifact(strategy_id: &str) -> (VerifiedStrategyArtifact, TrustPolicy) {
        let executable = std::env::current_exe().unwrap();
        let artifact = std::fs::read(executable).unwrap();
        verified_strategy_artifact_for(
            strategy_id,
            &artifact,
            std::any::type_name::<TestStrategy>(),
            &strategy_config_artifact(StrategyId::from(strategy_id)),
            7,
        )
    }

    #[expect(clippy::type_complexity)]
    fn create_trader_components() -> (
        Rc<RefCell<MessageBus>>,
        Rc<RefCell<Cache>>,
        Rc<RefCell<Portfolio>>,
        Rc<RefCell<DataEngine>>,
        Rc<RefCell<RiskEngine>>,
        Rc<RefCell<ExecutionEngine>>,
        ClockFactory,
    ) {
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();
        let clock_factory = ClockFactory::test_default();
        let clock = clock_factory.clock();
        let mut clock_ref = clock.borrow_mut();
        let test_clock = clock_ref
            .as_any_mut()
            .downcast_mut::<TestClock>()
            .expect("test default clock must be TestClock");
        test_clock.set_time(1_000_000_000u64.into());
        drop(clock_ref);
        let msgbus = Rc::new(RefCell::new(MessageBus::new(
            trader_id,
            instance_id,
            Some("test".to_string()),
            None,
        )));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        let data_engine = Rc::new(RefCell::new(DataEngine::new(
            clock.clone(),
            cache.clone(),
            Some(DataEngineConfig::default()),
        )));

        // Create separate cache and clock instances for RiskEngine to avoid borrowing conflicts
        let risk_cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let risk_clock = Rc::new(RefCell::new(TestClock::new()));
        let risk_portfolio = Portfolio::new(
            risk_clock.clone() as Rc<RefCell<dyn Clock>>,
            risk_cache.clone(),
            None,
        );
        let risk_engine = Rc::new(RefCell::new(RiskEngine::new(
            RiskEngineConfig::default(),
            risk_portfolio,
            risk_clock as Rc<RefCell<dyn Clock>>,
            risk_cache,
        )));
        let exec_engine = Rc::new(RefCell::new(ExecutionEngine::new(
            clock.clone(),
            cache.clone(),
            Some(ExecutionEngineConfig::default()),
        )));

        (
            msgbus,
            cache,
            portfolio,
            data_engine,
            risk_engine,
            exec_engine,
            clock_factory,
        )
    }

    #[rstest]
    fn test_trader_creation() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        assert_eq!(trader.trader_id(), trader_id);
        assert_eq!(trader.instance_id(), instance_id);
        assert_eq!(trader.environment(), Environment::Backtest);
        assert_eq!(trader.state(), ComponentState::PreInitialized);
        assert_eq!(trader.actor_count(), 0);
        assert_eq!(trader.strategy_count(), 0);
        assert_eq!(trader.exec_algorithm_count(), 0);
        assert_eq!(trader.component_count(), 0);
        assert!(!trader.is_running());
        assert!(!trader.is_stopped());
        assert!(!trader.is_disposed());
        assert!(trader.ts_created() > 0);
        assert!(trader.ts_started().is_none());
        assert!(trader.ts_stopped().is_none());
    }

    #[rstest]
    fn test_trader_component_id() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::from("TRADER-001");
        let instance_id = UUID4::new();

        let trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        assert_eq!(
            trader.component_id(),
            ComponentId::from("Trader-TRADER-001")
        );
    }

    #[rstest]
    fn test_add_actor_success() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let actor = TestDataActor::new(DataActorConfig::default());
        let actor_id = actor.actor_id();

        let result = trader.add_actor(actor);
        assert!(result.is_ok());
        assert_eq!(trader.actor_count(), 1);
        assert_eq!(trader.component_count(), 1);
        assert!(trader.actor_ids().contains(&actor_id));
    }

    #[rstest]
    fn test_add_duplicate_actor_fails() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = DataActorConfig {
            actor_id: Some(ActorId::from("TestActor")),
            ..Default::default()
        };
        let actor1 = TestDataActor::new(config.clone());
        let actor2 = TestDataActor::new(config);

        // First addition should succeed
        assert!(trader.add_actor(actor1).is_ok());
        assert_eq!(trader.actor_count(), 1);

        // Second addition should fail
        let result = trader.add_actor(actor2);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already registered")
        );
        assert_eq!(trader.actor_count(), 1);
    }

    #[rstest]
    fn test_start_rejects_same_id_global_component_replacement() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let actor_id = ActorId::from("PinnedStartActor-001");
        let config = DataActorConfig {
            actor_id: Some(actor_id),
            ..Default::default()
        };
        trader
            .add_actor(TestDataActor::new(config.clone()))
            .unwrap();

        register_component_actor(TestDataActor::new(config));
        let error = trader.start_components().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("identity changed after admission")
        );
        assert_eq!(
            component_state(&actor_id.inner()).unwrap(),
            ComponentState::PreInitialized
        );
    }

    #[rstest]
    fn test_state_callback_rejects_same_id_global_actor_replacement() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let actor_id = ActorId::from("PinnedCallbackActor-001");
        let config = DataActorConfig {
            actor_id: Some(actor_id),
            ..Default::default()
        };
        trader
            .add_actor(TestDataActor::new(config.clone()))
            .unwrap();
        let callbacks = trader
            .actor_state_callbacks
            .get(&actor_id)
            .copied()
            .unwrap();

        register_component_actor(TestDataActor::new(config));
        let error = callbacks.save(actor_id.inner()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("identity changed after admission")
        );
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_admission_rejects_unverified_registration() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        trader.require_strategy_artifact_admission(7).unwrap();

        let error = trader
            .add_strategy(TestStrategy::new(StrategyConfig::default()))
            .unwrap_err();

        assert!(error.to_string().contains("no verified release artifact"));
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_policy_below_floor_is_rejected_without_side_effects() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let (_artifact, policy) = verified_strategy_artifact("AdmissionRollback-001");

        let error = trader
            .require_strategy_artifact_admission_with_policy(
                &policy,
                policy.policy_epoch + 1,
                policy.validated_commitment().unwrap(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("below the host minimum"));
        assert!(!trader.strategy_artifact_admission_required);
        assert!(trader.strategy_artifact_trust_policy_commitment.is_none());
        assert!(trader.strategy_artifact_minimum_policy_epoch.is_none());
        assert!(trader.runtime_executable_digest.is_ok());
        assert!(trader.admitted_strategy_artifacts.is_empty());
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_unauthorized_policy_commitment_is_rejected_without_side_effects() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let (_artifact, policy) = verified_strategy_artifact("PolicySubstitution-001");
        let unauthorized_commitment = artifact_content_digest(b"attacker-policy");

        let error = trader
            .require_strategy_artifact_admission_with_policy(&policy, 7, unauthorized_commitment)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("externally authorized commitment")
        );
        assert!(!trader.strategy_artifact_admission_required);
        assert!(trader.strategy_artifact_trust_policy_commitment.is_none());
        assert!(trader.strategy_artifact_minimum_policy_epoch.is_none());
        assert!(trader.admitted_strategy_artifacts.is_empty());
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_capability_from_old_epoch_cannot_enter_newer_floor() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let (old_artifact, mut current_policy) =
            verified_strategy_artifact("AdmissionRollback-002");
        current_policy.policy_epoch += 1;
        trader
            .require_strategy_artifact_admission_with_policy(
                &current_policy,
                current_policy.policy_epoch,
                current_policy.validated_commitment().unwrap(),
            )
            .unwrap();

        let error = trader.admit_strategy_artifact(old_artifact).unwrap_err();

        assert!(error.to_string().contains("below the host minimum"));
        assert!(trader.admitted_strategy_artifacts.is_empty());
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_admission_accepts_exact_verified_identity() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let strategy_id = StrategyId::from("AdmissionTest-777");
        let strategy_id_text = strategy_id.to_string();
        let (artifact, policy) = verified_strategy_artifact(&strategy_id_text);
        trader
            .require_strategy_artifact_admission_with_policy(
                &policy,
                7,
                policy.validated_commitment().unwrap(),
            )
            .unwrap();
        trader.admit_strategy_artifact(artifact).unwrap();
        trader
            .add_strategy_from_artifact_config::<TestStrategy>(
                strategy_id,
                &strategy_config_artifact(strategy_id),
            )
            .unwrap();

        assert_eq!(trader.strategy_ids(), vec![strategy_id]);
        assert!(trader.admitted_strategy_artifact(&strategy_id).is_some());
        trader.clear_strategies().unwrap();
        assert!(trader.admitted_strategy_artifact(&strategy_id).is_none());
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_admission_rejects_signed_unrelated_bytes() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let strategy_id = StrategyId::from("AdmissionTest-778");
        let config_artifact = strategy_config_artifact(strategy_id);
        let (artifact, policy) = verified_strategy_artifact_for(
            strategy_id.as_ref(),
            b"signed but unrelated strategy bytes",
            std::any::type_name::<TestStrategy>(),
            &config_artifact,
            7,
        );
        trader
            .require_strategy_artifact_admission_with_policy(
                &policy,
                7,
                policy.validated_commitment().unwrap(),
            )
            .unwrap();
        trader.admit_strategy_artifact(artifact).unwrap();

        let error = trader
            .add_strategy_from_artifact_config::<TestStrategy>(strategy_id, &config_artifact)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("not the executing Nautilus binary")
        );
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_split_artifact_registration_rechecks_native_runtime_identity() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let strategy_id = StrategyId::from("SplitAdmission-001");
        let config_artifact = strategy_config_artifact(strategy_id);
        let (artifact, policy) = verified_strategy_artifact_for(
            strategy_id.as_ref(),
            b"signed runtime from another process",
            std::any::type_name::<TestStrategy>(),
            &config_artifact,
            7,
        );
        trader
            .require_strategy_artifact_admission_with_policy(
                &policy,
                7,
                policy.validated_commitment().unwrap(),
            )
            .unwrap();
        trader.admit_strategy_artifact(artifact).unwrap();
        let instance =
            ArtifactConfiguredStrategyInstance::<TestStrategy>::decode(&config_artifact).unwrap();

        let error = trader
            .add_artifact_configured_strategy(instance)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("not the executing Nautilus binary")
        );
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(all(feature = "strategy-artifact-trust", feature = "python"))]
    #[rstest]
    fn test_secure_python_import_is_rejected_before_path_parsing_or_import() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        trader.require_strategy_artifact_admission(7).unwrap();
        let config = ImportableStrategyConfig {
            strategy_path: "this path must never be parsed or imported".to_string(),
            config_path: "nor.this:Config".to_string(),
            config: std::collections::HashMap::new(),
        };

        let error = trader
            .add_strategy_from_importable_config(&config)
            .unwrap_err();

        assert!(error.to_string().contains("before import or introspection"));
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_admission_rejects_runtime_config_substitution() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let strategy_id = StrategyId::from("AdmissionConfigMismatch-001");
        let (artifact, policy) = verified_strategy_artifact(strategy_id.as_ref());
        trader
            .require_strategy_artifact_admission_with_policy(
                &policy,
                7,
                policy.validated_commitment().unwrap(),
            )
            .unwrap();
        trader.admit_strategy_artifact(artifact).unwrap();

        let substituted = serde_json::to_vec(&StrategyConfig {
            strategy_id: Some(strategy_id),
            log_events: false,
            ..Default::default()
        })
        .unwrap();
        let error = trader
            .add_strategy_from_artifact_config::<TestStrategy>(strategy_id, &substituted)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not match the evaluated configuration")
        );
        assert_eq!(trader.strategy_count(), 0);
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_artifact_preflight_rejects_config_before_strategy_constructor_runs() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let strategy_id = StrategyId::from("AdmissionPreflight-001");
        let approved_config = strategy_config_artifact(strategy_id);
        let executable_path = std::env::current_exe().unwrap();
        let executable = std::fs::read(executable_path).unwrap();
        let (artifact, policy) = verified_strategy_artifact_for(
            strategy_id.as_ref(),
            &executable,
            std::any::type_name::<PreflightSideEffectStrategy>(),
            &approved_config,
            7,
        );
        trader
            .require_strategy_artifact_admission_with_policy(
                &policy,
                7,
                policy.validated_commitment().unwrap(),
            )
            .unwrap();
        trader.admit_strategy_artifact(artifact).unwrap();
        let substituted = strategy_config_artifact(StrategyId::from("SUBSTITUTED-001"));
        PREFLIGHT_CONSTRUCTOR_CALLS.store(0, Ordering::SeqCst);

        let error = trader
            .add_strategy_from_artifact_config::<PreflightSideEffectStrategy>(
                strategy_id,
                &substituted,
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not match the evaluated configuration")
        );
        assert_eq!(PREFLIGHT_CONSTRUCTOR_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(trader.strategy_count(), 0);
    }

    #[rstest]
    fn test_artifact_configured_registration_rechecks_interior_mutability() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let config = MutableArtifactConfig {
            strategy_id: StrategyId::from("MutableArtifactAdd-001"),
            max_order_quantity: Cell::new(10),
        };
        let content = serde_json::to_vec(&config).unwrap();
        let instance =
            ArtifactConfiguredStrategyInstance::<MutableArtifactConfigStrategy>::decode(&content)
                .unwrap();
        instance
            .strategy()
            .artifact_config()
            .max_order_quantity
            .set(11);

        let error = trader
            .add_artifact_configured_strategy(instance)
            .unwrap_err();

        assert!(error.to_string().contains("configuration changed"));
        assert_eq!(trader.strategy_count(), 0);
    }

    #[rstest]
    fn test_artifact_configured_start_rechecks_registered_instance() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let strategy_id = StrategyId::from("MutableArtifactStart-001");
        let config = MutableArtifactConfig {
            strategy_id,
            max_order_quantity: Cell::new(10),
        };
        let content = serde_json::to_vec(&config).unwrap();
        let instance =
            ArtifactConfiguredStrategyInstance::<MutableArtifactConfigStrategy>::decode(&content)
                .unwrap();
        trader.add_artifact_configured_strategy(instance).unwrap();

        let registered =
            try_get_actor_unchecked::<MutableArtifactConfigStrategy>(&strategy_id.inner()).unwrap();
        registered.artifact_config().max_order_quantity.set(11);
        drop(registered);

        let error = trader.start_components().unwrap_err();

        assert!(error.to_string().contains("trust-boundary verification"));
        let registered =
            try_get_actor_unchecked::<MutableArtifactConfigStrategy>(&strategy_id.inner()).unwrap();
        assert_eq!(registered.start_calls, 0);
        drop(registered);
        trader.clear_strategies().unwrap();
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_artifact_config_decoder_rejects_constructor_which_ignores_config() {
        let content = strategy_config_artifact(StrategyId::from("IgnoredConfig-001"));

        let error =
            ArtifactConfiguredStrategyInstance::<IgnoringArtifactConfigStrategy>::decode(&content)
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("did not retain the authenticated configuration")
        );
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_artifact_config_decoder_rejects_non_canonical_json_alias() {
        let canonical = strategy_config_artifact(StrategyId::from("NonCanonicalConfig-001"));
        let mut non_canonical = Vec::with_capacity(canonical.len() + 1);
        non_canonical.push(b' ');
        non_canonical.extend_from_slice(&canonical);

        let error =
            ArtifactConfiguredStrategyInstance::<TestStrategy>::decode(&non_canonical).unwrap_err();

        assert!(error.to_string().contains("not canonical JSON"));
    }

    #[cfg(feature = "strategy-artifact-trust")]
    #[rstest]
    fn test_strategy_artifact_admission_rejects_capability_from_different_policy() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Live,
            clock_factory,
            cache,
            portfolio,
        );
        let (artifact, mut pinned_policy) =
            verified_strategy_artifact("AdmissionPolicyMismatch-001");
        pinned_policy
            .trusted_signing_keys
            .insert("unrelated-release-key".to_string(), vec![3; 32]);
        trader
            .require_strategy_artifact_admission_with_policy(
                &pinned_policy,
                7,
                pinned_policy.validated_commitment().unwrap(),
            )
            .unwrap();

        let error = trader.admit_strategy_artifact(artifact).unwrap_err();

        assert!(error.to_string().contains("different trust policy"));
        assert_eq!(trader.strategy_count(), 0);
    }

    #[rstest]
    fn test_add_strategy_success() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("Test-Strategy")),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);

        let result = trader.add_strategy(strategy);
        assert!(result.is_ok());
        assert_eq!(trader.strategy_count(), 1);
        assert_eq!(trader.component_count(), 1);
        assert!(
            trader
                .strategy_ids()
                .contains(&StrategyId::from("Test-Strategy"))
        );
    }

    #[rstest]
    fn test_add_strategy_preserves_explicit_instrument_strategy_id() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let strategy_id = StrategyId::from("ExampleStrategy-XNAS");
        let config = StrategyConfig {
            strategy_id: Some(strategy_id),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);

        trader.add_strategy(strategy).unwrap();

        let mut registered = get_actor_unchecked::<TestStrategy>(&strategy_id.inner());
        let (client_order_id, order_list_id) = {
            let mut order_factory = registered.order_factory();
            (
                order_factory.generate_client_order_id(),
                order_factory.generate_order_list_id(),
            )
        };

        assert_eq!(trader.strategy_ids(), vec![strategy_id]);
        assert_eq!(registered.strategy_id(), Some(strategy_id));
        assert!(client_order_id.as_str().ends_with("-001-XNAS-1"));
        assert!(order_list_id.as_str().ends_with("-001-XNAS-1"));
    }

    #[rstest]
    fn test_add_strategy_appends_configured_order_id_tag_to_explicit_strategy_id() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let strategy_id = StrategyId::from("ExampleStrategy-XNAS");
        let runtime_strategy_id = StrategyId::from("ExampleStrategy-XNAS-T01");
        let config = StrategyConfig {
            strategy_id: Some(strategy_id),
            order_id_tag: Some("T01".to_string()),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);

        trader.add_strategy(strategy).unwrap();

        assert!(try_get_actor_unchecked::<TestStrategy>(&strategy_id.inner()).is_none());

        let mut registered = get_actor_unchecked::<TestStrategy>(&runtime_strategy_id.inner());
        let (client_order_id, order_list_id) = {
            let mut order_factory = registered.order_factory();
            (
                order_factory.generate_client_order_id(),
                order_factory.generate_order_list_id(),
            )
        };

        assert_eq!(trader.strategy_ids(), vec![runtime_strategy_id]);
        assert_eq!(registered.strategy_id(), Some(runtime_strategy_id));
        assert!(client_order_id.as_str().ends_with("-001-T01-1"));
        assert!(order_list_id.as_str().ends_with("-001-T01-1"));
    }

    #[rstest]
    fn test_add_strategies_with_no_order_id_tags_assigns_unique_tags() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let strategy1 = TestStrategy::new(StrategyConfig::default());
        let strategy2 = TestStrategy::new(StrategyConfig::default());

        assert!(trader.add_strategy(strategy1).is_ok());
        assert!(trader.add_strategy(strategy2).is_ok());
        assert_eq!(
            trader.strategy_ids(),
            vec![
                StrategyId::from("TestStrategy-000"),
                StrategyId::from("TestStrategy-001")
            ]
        );
    }

    #[rstest]
    fn test_prepare_strategy_for_registration_is_idempotent() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let mut strategy = TestStrategy::new(StrategyConfig::default());

        let prepared_id = trader
            .prepare_strategy_for_registration(&mut strategy)
            .unwrap();
        assert_eq!(prepared_id, StrategyId::from("TestStrategy-000"));
        let core = StrategyNative::strategy_core(&strategy);
        assert_eq!(core.config.strategy_id, None);
        assert_eq!(core.config.order_id_tag, None);
        assert_eq!(core.strategy_id(), Some(prepared_id));
        assert_eq!(core.order_id_tag(), Some("000"));

        assert!(trader.add_strategy(strategy).is_ok());
        assert_eq!(trader.strategy_ids(), vec![prepared_id]);
    }

    #[rstest]
    fn test_add_strategy_with_duplicate_order_id_tag_fails() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = StrategyConfig {
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        };
        let strategy1 = TestStrategy::new(config.clone());
        let strategy2 = TestStrategy::new(config);

        assert!(trader.add_strategy(strategy1).is_ok());
        assert_eq!(
            trader.strategy_ids(),
            vec![StrategyId::from("TestStrategy-001")]
        );

        let result = trader.add_strategy(strategy2);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("order_id_tag conflict")
        );
    }

    #[rstest]
    fn test_add_strategy_id_with_subscriptions_duplicate_order_id_tag_fails() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let foo_id = StrategyId::from("Foo-001");
        register_component_actor(TestStrategy::new(StrategyConfig {
            strategy_id: Some(foo_id),
            ..Default::default()
        }));
        assert!(
            trader
                .add_strategy_id_with_subscriptions::<TestStrategy>(foo_id)
                .is_ok()
        );

        let bar_id = StrategyId::from("Bar-001");
        register_component_actor(TestStrategy::new(StrategyConfig {
            strategy_id: Some(bar_id),
            ..Default::default()
        }));
        let result = trader.add_strategy_id_with_subscriptions::<TestStrategy>(bar_id);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("order_id_tag conflict")
        );
        assert_eq!(trader.strategy_ids(), vec![foo_id]);
    }

    #[rstest]
    fn test_add_strategy_with_mismatched_strategy_id_and_order_id_tag_appends_tag() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("TestStrategy-001")),
            order_id_tag: Some("002".to_string()),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);

        assert!(trader.add_strategy(strategy).is_ok());
        assert_eq!(
            trader.strategy_ids(),
            vec![StrategyId::from("TestStrategy-001-002")]
        );
    }

    #[rstest]
    fn test_add_exec_algorithm_success() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(ExecAlgorithmId::from("TestExecAlgorithm")),
            ..Default::default()
        };
        let exec_algorithm = TestExecAlgorithm::new(config);
        let exec_algorithm_id = ExecutionAlgorithm::id(&exec_algorithm);

        let result = trader.add_exec_algorithm(exec_algorithm);
        assert!(result.is_ok());
        assert_eq!(trader.exec_algorithm_count(), 1);
        assert_eq!(trader.component_count(), 1);
        assert!(trader.exec_algorithm_ids().contains(&exec_algorithm_id));
    }

    #[rstest]
    fn test_exec_algorithm_restores_cached_strategy_subscriptions_on_start_and_restart() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();
        let unique = UUID4::new();
        let exec_algorithm_id = ExecAlgorithmId::from(format!("RECOVERY-{unique}"));
        let other_algorithm_id = ExecAlgorithmId::from(format!("OTHER-{unique}"));
        let strategy_a = StrategyId::from(format!("RecoveryA-{unique}"));
        let strategy_b = StrategyId::from(format!("RecoveryB-{unique}"));
        let terminal_strategy = StrategyId::from(format!("Terminal-{unique}"));
        let external_strategy = StrategyId::external();

        let order_a = add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-A1-{unique}")),
            strategy_a,
            Some(exec_algorithm_id),
            false,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-A2-{unique}")),
            strategy_a,
            Some(exec_algorithm_id),
            false,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-B-{unique}")),
            strategy_b,
            Some(exec_algorithm_id),
            false,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-TERMINAL-{unique}")),
            terminal_strategy,
            Some(exec_algorithm_id),
            true,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-OTHER-{unique}")),
            StrategyId::from(format!("Other-{unique}")),
            Some(other_algorithm_id),
            false,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-EXTERNAL-{unique}")),
            external_strategy,
            None,
            false,
        );

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let config = ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(exec_algorithm_id),
            ..Default::default()
        };
        trader
            .add_exec_algorithm(TestExecAlgorithm::new(config))
            .unwrap();

        trader.start_components().unwrap();

        assert_eq!(order_a.exec_spawn_id(), Some(order_a.client_order_id()));
        {
            let registered = get_actor_unchecked::<TestExecAlgorithm>(&exec_algorithm_id.inner());
            assert!(registered.core.is_strategy_subscribed(&strategy_a));
            assert!(registered.core.is_strategy_subscribed(&strategy_b));
            assert!(!registered.core.is_strategy_subscribed(&terminal_strategy));
            assert!(!registered.core.is_strategy_subscribed(&external_strategy));
        }

        let rejected = OrderEventAny::Rejected(
            OrderRejectedSpec::builder()
                .trader_id(order_a.trader_id())
                .strategy_id(strategy_a)
                .instrument_id(order_a.instrument_id())
                .client_order_id(order_a.client_order_id())
                .account_id(AccountId::test_default())
                .reason("TEST_REJECTED".into())
                .build(),
        );
        let updated = OrderEventAny::Updated(
            OrderUpdatedSpec::builder()
                .trader_id(order_a.trader_id())
                .strategy_id(strategy_a)
                .instrument_id(order_a.instrument_id())
                .client_order_id(order_a.client_order_id())
                .build(),
        );
        let filled = OrderEventAny::Filled(
            OrderFilledSpec::builder()
                .trader_id(order_a.trader_id())
                .strategy_id(strategy_a)
                .instrument_id(order_a.instrument_id())
                .client_order_id(order_a.client_order_id())
                .build(),
        );
        let position = PositionEvent::PositionAdjusted(PositionAdjusted::new(
            trader_id,
            strategy_a,
            InstrumentId::test_default(),
            PositionId::from(format!("P-{unique}")),
            AccountId::test_default(),
            PositionAdjustmentType::Funding,
            None,
            None,
            None,
            UUID4::new(),
            0.into(),
            0.into(),
        ));

        let order_topic = format!("events.order.{strategy_a}");
        msgbus::publish_order_event(order_topic.clone().into(), &rejected);
        msgbus::publish_order_event(order_topic.clone().into(), &updated);
        msgbus::publish_order_event(order_topic.into(), &filled);
        msgbus::publish_position_event(format!("events.position.{strategy_a}").into(), &position);

        {
            let registered = get_actor_unchecked::<TestExecAlgorithm>(&exec_algorithm_id.inner());
            assert_eq!(registered.rejected_events, 1);
            assert_eq!(registered.updated_events, 1);
            assert_eq!(registered.filled_events, 1);
            assert_eq!(registered.position_events, 1);
        }

        trader.stop_components().unwrap();
        trader.reset_components().unwrap();
        {
            let registered = get_actor_unchecked::<TestExecAlgorithm>(&exec_algorithm_id.inner());
            assert!(!registered.core.is_strategy_subscribed(&strategy_a));
            assert!(!registered.core.is_strategy_subscribed(&strategy_b));
        }

        trader.start_components().unwrap();
        {
            let registered = get_actor_unchecked::<TestExecAlgorithm>(&exec_algorithm_id.inner());
            assert!(registered.core.is_strategy_subscribed(&strategy_a));
            assert!(registered.core.is_strategy_subscribed(&strategy_b));
        }

        trader.stop_components().unwrap();
        trader.clear_exec_algorithms().unwrap();
        assert!(trader.exec_algorithm_restore_fns.is_empty());
        assert!(trader.exec_algorithm_cleanup_fns.is_empty());
    }

    #[rstest]
    fn test_exec_algorithm_start_failure_cleans_all_restored_subscriptions() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();
        let unique = UUID4::new();
        let running_algorithm_id = ExecAlgorithmId::from(format!("RUNNING-{unique}"));
        let failing_algorithm_id = ExecAlgorithmId::from(format!("FAIL-{unique}"));
        let running_strategy_id = StrategyId::from(format!("Running-{unique}"));
        let failing_strategy_id = StrategyId::from(format!("Failing-{unique}"));
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-RUNNING-{unique}")),
            running_strategy_id,
            Some(running_algorithm_id),
            false,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-FAILING-{unique}")),
            failing_strategy_id,
            Some(failing_algorithm_id),
            false,
        );

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let running_config = ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(running_algorithm_id),
            ..Default::default()
        };
        trader
            .add_exec_algorithm(TestExecAlgorithm::new(running_config))
            .unwrap();
        let failing_config = ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(failing_algorithm_id),
            ..Default::default()
        };
        let mut failing_algorithm = TestExecAlgorithm::new(failing_config);
        failing_algorithm.fail_start = true;
        trader.add_exec_algorithm(failing_algorithm).unwrap();
        trader.initialize().unwrap();
        let trader = Rc::new(RefCell::new(trader));

        let error = Trader::start_with_component_callbacks(&trader).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("test execution algorithm start failure")
        );
        {
            let running = get_actor_unchecked::<TestExecAlgorithm>(&running_algorithm_id.inner());
            let failing = get_actor_unchecked::<TestExecAlgorithm>(&failing_algorithm_id.inner());
            assert!(!running.core.is_strategy_subscribed(&running_strategy_id));
            assert!(!failing.core.is_strategy_subscribed(&failing_strategy_id));
        }

        trader.borrow_mut().stop_after_start_failure().unwrap();

        let running = get_actor_unchecked::<TestExecAlgorithm>(&running_algorithm_id.inner());
        assert!(!running.core.is_strategy_subscribed(&running_strategy_id));
    }

    #[rstest]
    fn test_start_components_failure_cleans_previously_restored_subscriptions() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();
        let unique = UUID4::new();
        let running_algorithm_id = ExecAlgorithmId::from(format!("DIRECT-RUNNING-{unique}"));
        let failing_algorithm_id = ExecAlgorithmId::from(format!("DIRECT-FAIL-{unique}"));
        let running_strategy_id = StrategyId::from(format!("DirectRunning-{unique}"));
        let failing_strategy_id = StrategyId::from(format!("DirectFailing-{unique}"));
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-DIRECT-RUNNING-{unique}")),
            running_strategy_id,
            Some(running_algorithm_id),
            false,
        );
        add_cached_exec_order(
            &cache,
            ClientOrderId::from(format!("O-DIRECT-FAILING-{unique}")),
            failing_strategy_id,
            Some(failing_algorithm_id),
            false,
        );

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        trader
            .add_exec_algorithm(TestExecAlgorithm::new(ExecutionAlgorithmConfig {
                exec_algorithm_id: Some(running_algorithm_id),
                ..Default::default()
            }))
            .unwrap();
        let mut failing_algorithm = TestExecAlgorithm::new(ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(failing_algorithm_id),
            ..Default::default()
        });
        failing_algorithm.fail_start = true;
        trader.add_exec_algorithm(failing_algorithm).unwrap();

        let error = trader.start_components().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("test execution algorithm start failure")
        );
        let running = get_actor_unchecked::<TestExecAlgorithm>(&running_algorithm_id.inner());
        let failing = get_actor_unchecked::<TestExecAlgorithm>(&failing_algorithm_id.inner());
        assert!(!running.core.is_strategy_subscribed(&running_strategy_id));
        assert!(!failing.core.is_strategy_subscribed(&failing_strategy_id));
    }

    #[rstest]
    fn test_cannot_add_exec_algorithm_while_running() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        trader.state = ComponentState::Running;

        let config = ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(ExecAlgorithmId::from("TestExecAlgorithm")),
            ..Default::default()
        };
        let exec_algorithm = TestExecAlgorithm::new(config);

        let result = trader.add_exec_algorithm(exec_algorithm);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot add execution algorithms to running trader"
        );
        assert_eq!(trader.exec_algorithm_count(), 0);
    }

    #[rstest]
    fn test_component_lifecycle() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        // Add components
        let actor = TestDataActor::new(DataActorConfig::default());

        let strategy_config = StrategyConfig {
            strategy_id: Some(StrategyId::from("Test-Strategy")),
            ..Default::default()
        };
        let strategy = TestStrategy::new(strategy_config);

        let exec_algorithm_config = ExecutionAlgorithmConfig {
            exec_algorithm_id: Some(ExecAlgorithmId::from("TestExecAlgorithm")),
            ..Default::default()
        };
        let exec_algorithm = TestExecAlgorithm::new(exec_algorithm_config);

        assert!(trader.add_actor(actor).is_ok());
        assert!(trader.add_strategy(strategy).is_ok());
        assert!(trader.add_exec_algorithm(exec_algorithm).is_ok());
        assert_eq!(trader.component_count(), 3);

        // Test start components
        let start_result = trader.start_components();
        assert!(start_result.is_ok(), "{:?}", start_result.unwrap_err());

        // Test stop components
        assert!(trader.stop_components().is_ok());

        // Test reset components
        assert!(trader.reset_components().is_ok());

        // Test dispose components
        assert!(trader.dispose_components().is_ok());
        assert_eq!(trader.component_count(), 0);
    }

    #[rstest]
    fn test_trader_component_lifecycle() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        // Initially pre-initialized
        assert_eq!(trader.state(), ComponentState::PreInitialized);
        assert!(!trader.is_running());
        assert!(!trader.is_stopped());
        assert!(!trader.is_disposed());

        // Cannot start from pre-initialized state
        assert!(trader.start().is_err());

        // Simulate initialization (normally done by kernel)
        trader.initialize().unwrap();

        // Test start
        assert!(trader.start().is_ok());
        assert_eq!(trader.state(), ComponentState::Running);
        assert!(trader.is_running());
        assert!(trader.ts_started().is_some());

        // Test stop
        assert!(trader.stop().is_ok());
        assert_eq!(trader.state(), ComponentState::Stopped);
        assert!(trader.is_stopped());
        assert!(trader.ts_stopped().is_some());

        // Test reset
        assert!(trader.reset().is_ok());
        assert_eq!(trader.state(), ComponentState::Ready);
        assert!(trader.ts_started().is_none());
        assert!(trader.ts_stopped().is_none());

        // Test dispose
        assert!(trader.dispose().is_ok());
        assert_eq!(trader.state(), ComponentState::Disposed);
        assert!(trader.is_disposed());
    }

    #[rstest]
    fn test_market_exit_strategy_fails_when_control_endpoint_missing() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("Test-Strategy")),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);
        trader.add_strategy(strategy).unwrap();

        let strategy_id = StrategyId::from("Test-Strategy");
        let endpoint = strategy_control_endpoint(strategy_id);
        assert!(
            get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .is_registered(endpoint)
        );
        get_message_bus()
            .borrow_mut()
            .endpoint_map::<StrategyCommand>()
            .deregister(endpoint);

        let trader = Rc::new(RefCell::new(trader));
        let result = Trader::market_exit_strategy(&trader, &strategy_id);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Cannot exit market for strategy {strategy_id}: control endpoint '{}' not registered",
                endpoint.as_str()
            )
        );
    }

    #[rstest]
    fn test_remove_strategy_deregisters_strategy_endpoint() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("Test-Strategy")),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);
        trader.add_strategy(strategy).unwrap();

        let strategy_id = StrategyId::from("Test-Strategy");
        let endpoint = strategy_control_endpoint(strategy_id);
        assert!(
            get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .is_registered(endpoint)
        );

        trader.remove_strategy(&strategy_id).unwrap();

        assert!(
            !get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .is_registered(endpoint)
        );
    }

    #[rstest]
    fn test_can_add_components_while_running() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        // Simulate running state
        trader.state = ComponentState::Running;

        let actor = TestDataActor::new(DataActorConfig::default());
        let result = trader.add_actor(actor);
        assert!(result.is_ok());
        assert_eq!(trader.actor_count(), 1);
    }

    #[rstest]
    fn test_cannot_add_components_while_disposed() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        // Simulate disposed state
        trader.state = ComponentState::Disposed;

        let actor = TestDataActor::new(DataActorConfig::default());
        let result = trader.add_actor(actor);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("disposed trader"));
    }

    #[rstest]
    fn test_create_component_clock_backtest_creates_individual_clocks() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory.clone(),
            cache,
            portfolio,
        );

        let component_a = ComponentId::new("ACTOR-A");
        let component_b = ComponentId::new("ACTOR-B");
        let clock_a = trader.create_component_clock(component_a);
        let clock_b = trader.create_component_clock(component_b);
        let primary_clock = clock_factory.clock();

        // Each component gets its own clock instance
        assert_ne!(
            clock_a.as_ptr() as *const _,
            primary_clock.as_ptr() as *const _
        );
        assert_ne!(clock_a.as_ptr() as *const _, clock_b.as_ptr() as *const _);
    }

    #[rstest]
    fn test_get_component_clocks_returns_registration_order() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );
        let mut registered = Vec::new();

        for index in 0..32 {
            let component_id = ComponentId::new(format!("ACTOR-{index:02}").as_str());
            registered.push(trader.create_component_clock(component_id));
        }

        let returned = trader.get_component_clocks();
        assert_eq!(returned.len(), registered.len());
        for (actual, expected) in returned.iter().zip(&registered) {
            assert!(Rc::ptr_eq(actual, expected));
        }
    }

    #[rstest]
    fn test_create_component_clock_live_uses_factory_with_distinct_instances() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, _clock_factory) =
            create_trader_components();
        let calls = Rc::new(Cell::new(0usize));
        let calls_in_closure = calls.clone();
        let clock_factory = ClockFactory::new(move || {
            calls_in_closure.set(calls_in_closure.get() + 1);
            Rc::new(RefCell::new(TestClock::new())) as Rc<RefCell<dyn Clock>>
        });

        let mut trader = Trader::new(
            TraderId::test_default(),
            UUID4::new(),
            Environment::Sandbox,
            clock_factory,
            cache,
            portfolio,
        );

        let a = trader.create_component_clock(ComponentId::new("ACTOR-A"));
        let b = trader.create_component_clock(ComponentId::new("ACTOR-B"));

        assert_eq!(
            calls.get(),
            3,
            "factory invoked for primary clock and each component",
        );
        assert!(
            !Rc::ptr_eq(&a, &b),
            "each component must get its own clock instance"
        );
    }

    #[rstest]
    fn test_clear_strategies_preserves_other_handlers() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("Test-Strategy")),
            ..Default::default()
        };
        let strategy = TestStrategy::new(config);
        trader.add_strategy(strategy).unwrap();

        let strategy_id = StrategyId::from("Test-Strategy");
        let endpoint = strategy_control_endpoint(strategy_id);
        assert!(
            get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .is_registered(endpoint)
        );

        // Simulate an exec algorithm subscribing to the same strategy topic
        let ext_received = Rc::new(RefCell::new(0));
        let ext_clone = ext_received.clone();
        let ext_handler =
            TypedHandler::from_with_id("exec-algo-handler", move |_: &OrderEventAny| {
                *ext_clone.borrow_mut() += 1;
            });
        let order_topic = get_event_order_topic(strategy_id);
        msgbus::subscribe_order_events(order_topic.into(), ext_handler, None);

        trader.clear_strategies().unwrap();
        assert_eq!(trader.strategy_count(), 0);
        assert!(
            !get_message_bus()
                .borrow_mut()
                .endpoint_map::<StrategyCommand>()
                .is_registered(endpoint)
        );

        let event = OrderEventAny::Accepted(OrderAccepted::test_default());
        msgbus::publish_order_event(order_topic, &event);
        assert_eq!(*ext_received.borrow(), 1);
    }

    #[cfg(feature = "python")]
    #[rstest]
    fn test_python_actor_and_strategy_state_callbacks_use_registered_types() {
        pyo3::Python::initialize();

        Python::attach(|py| {
            py.run(
                c_str!(
                    r#"
class StateComponent:
    def __init__(self, state):
        self.state = state
        self.loaded = None
        self.calls = []

    def on_load(self, state):
        self.calls.append("on_load")
        self.loaded = dict(state)

    def on_save(self):
        self.calls.append("on_save")
        return self.state
"#
                ),
                None,
                None,
            )
            .unwrap();

            let component_class = py.eval(c_str!("StateComponent"), None, None).unwrap();
            let actor_save =
                IndexMap::from([("actor-save".to_string(), b"python-actor-saved".to_vec())]);
            let strategy_save = IndexMap::from([(
                "strategy-save".to_string(),
                b"python-strategy-saved".to_vec(),
            )]);
            let py_actor_state = PyDict::new(py);
            py_actor_state
                .set_item("actor-save", b"python-actor-saved")
                .unwrap();
            let py_strategy_state = PyDict::new(py);
            py_strategy_state
                .set_item("strategy-save", b"python-strategy-saved")
                .unwrap();
            let py_actor = component_class.call1((py_actor_state,)).unwrap().unbind();
            let py_strategy = component_class
                .call1((py_strategy_state,))
                .unwrap()
                .unbind();

            let actor_id = ActorId::from("PYTHON-STATE-ACTOR");
            let strategy_id = StrategyId::from("PYTHON-STATE-STRATEGY-001");
            let actor_load =
                IndexMap::from([("actor-load".to_string(), b"python-actor-loaded".to_vec())]);
            let strategy_load = IndexMap::from([(
                "strategy-load".to_string(),
                b"python-strategy-loaded".to_vec(),
            )]);
            let (database, control) = TestCacheDatabaseControl::create();
            control.set_actor_state(ComponentId::from(actor_id.as_str()), &actor_load);
            control.set_strategy_state(strategy_id, &strategy_load);

            let (
                _msgbus,
                cache,
                portfolio,
                _data_engine,
                _risk_engine,
                _exec_engine,
                clock_factory,
            ) = create_trader_components();
            cache.borrow_mut().set_database(Box::new(database));
            let trader_id = TraderId::test_default();
            let mut trader = Trader::new(
                trader_id,
                UUID4::new(),
                Environment::Backtest,
                clock_factory,
                cache.clone(),
                portfolio.clone(),
            );

            let mut actor = PyDataActor::new(Some(DataActorConfig {
                actor_id: Some(actor_id),
                ..Default::default()
            }));
            actor.set_python_instance(py_actor.clone_ref(py));
            let actor_clock = trader.create_component_clock(ComponentId::from(actor_id.as_str()));
            actor
                .register(trader_id, actor_clock, cache.clone())
                .unwrap();
            actor.register_in_global_registries();
            trader
                .add_actor_id_for_lifecycle::<PyDataActorInner>(actor_id)
                .unwrap();

            let mut strategy = PyStrategy::new(Some(StrategyConfig {
                strategy_id: Some(strategy_id),
                ..Default::default()
            }));
            strategy.set_python_instance(py_strategy.clone_ref(py));
            let strategy_clock =
                trader.create_component_clock(ComponentId::from(strategy_id.as_str()));
            strategy
                .register(trader_id, strategy_clock, cache, portfolio)
                .unwrap();
            strategy.register_in_global_registries();
            trader
                .add_strategy_id_with_subscriptions::<PyStrategyInner>(strategy_id)
                .unwrap();

            let trader = Rc::new(RefCell::new(trader));
            Trader::load_state(&trader).unwrap();
            Trader::save_state(&trader).unwrap();

            let actor_loaded = py_actor
                .getattr(py, "loaded")
                .unwrap()
                .extract::<std::collections::HashMap<String, Vec<u8>>>(py)
                .unwrap();
            let strategy_loaded = py_strategy
                .getattr(py, "loaded")
                .unwrap()
                .extract::<std::collections::HashMap<String, Vec<u8>>>(py)
                .unwrap();
            let actor_calls = py_actor
                .getattr(py, "calls")
                .unwrap()
                .extract::<Vec<String>>(py)
                .unwrap();
            let strategy_calls = py_strategy
                .getattr(py, "calls")
                .unwrap()
                .extract::<Vec<String>>(py)
                .unwrap();

            assert_eq!(
                actor_loaded,
                std::collections::HashMap::from([(
                    "actor-load".to_string(),
                    b"python-actor-loaded".to_vec(),
                )])
            );
            assert_eq!(
                strategy_loaded,
                std::collections::HashMap::from([(
                    "strategy-load".to_string(),
                    b"python-strategy-loaded".to_vec(),
                )])
            );
            assert_eq!(actor_calls, vec!["on_load", "on_save"]);
            assert_eq!(strategy_calls, vec!["on_load", "on_save"]);
            assert_eq!(
                control.actor_state(&ComponentId::from(actor_id.as_str())),
                Some(actor_save)
            );
            assert_eq!(control.strategy_state(&strategy_id), Some(strategy_save));
        });
    }

    #[rstest]
    fn test_clear_actors_disposes_and_clears_state() {
        let (_msgbus, cache, portfolio, _data_engine, _risk_engine, _exec_engine, clock_factory) =
            create_trader_components();
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();

        let mut trader = Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        );

        let actor_a = TestDataActor::new(DataActorConfig {
            actor_id: Some(ActorId::from("Actor-A")),
            ..Default::default()
        });
        let actor_b = TestDataActor::new(DataActorConfig {
            actor_id: Some(ActorId::from("Actor-B")),
            ..Default::default()
        });
        trader.add_actor(actor_a).unwrap();
        trader.add_actor(actor_b).unwrap();
        assert_eq!(trader.actor_count(), 2);
        assert_eq!(
            trader.get_component_clocks().len(),
            2,
            "each registered actor must have a component clock",
        );

        trader.clear_actors().unwrap();

        assert_eq!(trader.actor_count(), 0);
        assert!(trader.actor_ids().is_empty());
        assert_eq!(
            trader.get_component_clocks().len(),
            0,
            "actor clocks must be dropped after clear_actors",
        );
    }
}
