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

//! Kernel-facing seam for run-lifecycle event-sourcing.
//!
//! The [`KernelEventStore`] trait is the surface [`crate::kernel::NautilusKernel`] uses to wire
//! a durable event-sourcing session into its boot, snapshot, and seal flow. The concrete
//! implementation lives in `nautilus-event-store` so that crate can be developed and versioned
//! independently of `nautilus-system`; callers inject an implementation through the builder
//! (see [`crate::builder::NautilusKernelBuilder::with_event_store`]).

use std::{cell::RefCell, fmt::Debug, path::PathBuf, rc::Rc, time::Duration};

use indexmap::IndexMap;
use nautilus_common::{
    cache::Cache, clock::Clock, enums::Environment, messages::system::RiskStateChanged,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::SnapshotAnchorer;
pub use nautilus_provenance::{ContentDigest, RunProvenance};
use serde::{Deserialize, Serialize};

/// Factory closure invoked by the kernel to construct an injected event-store implementation.
///
/// Receives the kernel's instance id and clock so the resulting [`KernelEventStore`]
/// implementation scans the same on-disk run directory the kernel later passes to
/// `restore_parent_cache`/`open`, and stamps lifecycle timestamps against the same time
/// source the kernel uses.
pub type EventStoreFactory = Box<
    dyn FnOnce(UUID4, Rc<RefCell<dyn Clock>>) -> anyhow::Result<Box<dyn KernelEventStore>>
        + 'static,
>;

/// The component inventory captured into the event-store `RunStarted` entry.
///
/// This is an audit commitment to the sorted component identity set, not a reconstruction recipe.
/// Arbitrary native and Python component configuration and message-bus topology do not yet have a
/// library-owned canonical encoder, so this type deliberately does not claim to commit them.
/// Replay therefore does not instantiate actors, strategies, algorithms, subscriptions, or
/// command endpoints from this value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredComponents {
    /// Registered actor IDs, sorted lexicographically and unique.
    pub actors: Vec<String>,
    /// Registered strategy IDs, sorted lexicographically and unique.
    pub strategies: Vec<String>,
    /// Registered algorithm IDs, sorted lexicographically and unique.
    pub algorithms: Vec<String>,
}

impl RegisteredComponents {
    /// Returns whether every present identity is non-empty and each set is unique and sorted.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        [&self.actors, &self.strategies, &self.algorithms]
            .into_iter()
            .all(|identities| {
                identities.iter().all(|identity| !identity.is_empty())
                    && identities
                        .windows(2)
                        .all(|pair| pair[0].as_str() < pair[1].as_str())
            })
    }
}

/// Risk-policy bootstrap material loaded from one sealed event-store source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RiskStateRecovery {
    /// Latest authorized policy checkpoint, when the source generation emitted one.
    pub checkpoint: Option<RiskStateChanged>,
    /// Whether crash/unknown-outcome semantics require a process-lifetime safe-disabled interlock.
    ///
    /// The kernel applies this independently of checkpoint availability. A checkpoint can restore
    /// limits but is not authenticated operator authorization to return to `Active`; ordinary
    /// risk state and reset APIs cannot clear the resulting interlock.
    pub force_halted: bool,
}

/// Terminal event-store status bound into authenticated backtest evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventStoreTerminalStatus {
    /// The run committed `RunEnded`, sealed its manifest, and passed full verification.
    Ended,
    /// A prior process disappeared before a clean close; never valid for release promotion.
    CrashedRecovered,
    /// Integrity verification rejected the run; never valid for replay or promotion.
    Quarantined,
}

/// Immutable receipt for a sealed and verified event-store run.
///
/// The digest commits the terminal manifest, every ordered entry hash, and the cache snapshot
/// anchor. It does not currently carry the verifier-helper digest, source-file fingerprint, or
/// helper protocol version, so this value alone is not sufficient production promotion evidence.
/// Backtest evidence may bind it only as one input to a separately authenticated evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventStoreSealReceipt {
    run_id: String,
    high_watermark: u64,
    terminal_status: EventStoreTerminalStatus,
    store_digest: ContentDigest,
}

impl EventStoreSealReceipt {
    /// Constructs a store-verification receipt.
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        high_watermark: u64,
        terminal_status: EventStoreTerminalStatus,
        store_digest: ContentDigest,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            high_watermark,
            terminal_status,
            store_digest,
        }
    }

    /// Returns the event-store run identity.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Returns the final durable sequence acknowledged by the backend.
    #[must_use]
    pub const fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    /// Returns the verified terminal status.
    #[must_use]
    pub const fn terminal_status(&self) -> EventStoreTerminalStatus {
        self.terminal_status
    }

    /// Returns the canonical verified-store commitment.
    #[must_use]
    pub const fn store_digest(&self) -> ContentDigest {
        self.store_digest
    }
}

/// Kernel-facing seam for event-sourcing lifecycle integration.
///
/// `NautilusKernel` drives the open/restore/seal sequence through this trait so the concrete
/// event-store machinery (writers, readers, bus tap, redb backend) lives outside
/// `nautilus-system`. Implementations are typically built by the caller and injected via
/// [`crate::builder::NautilusKernelBuilder::with_event_store`].
pub trait KernelEventStore: Debug {
    /// Replaces the immutable provenance that the next `RunStarted` header will commit.
    ///
    /// The kernel calls this before [`Self::open`]. Implementations must reject changes after a
    /// run has opened or after a terminal event-store halt.
    ///
    /// # Errors
    ///
    /// Returns an error when provenance can no longer be changed safely.
    fn set_run_provenance(&mut self, provenance: RunProvenance) -> anyhow::Result<()>;

    /// Restores cache state from a configured replay source or recovered parent run.
    ///
    /// Implementations may open a sealed replay source, validate its snapshot anchor, and
    /// replay the tail directly into `cache`. The kernel calls this once before [`Self::open`].
    ///
    /// # Errors
    ///
    /// Returns an error when the source reader, snapshot restore, decode, or cache apply
    /// step fails.
    fn restore_parent_cache(&mut self, instance_id: UUID4, cache: &mut Cache)
    -> anyhow::Result<()>;

    /// Loads the latest authorized operator risk-policy checkpoint from the configured replay
    /// source or recovered parent run.
    ///
    /// The kernel validates and applies the returned DTO through the risk engine before opening a
    /// new run. A default implementation keeps non-event-store test doubles source-compatible.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be validated, scanned, or decoded.
    fn restore_parent_risk_state(
        &mut self,
        _instance_id: UUID4,
    ) -> anyhow::Result<Option<RiskStateRecovery>> {
        Ok(None)
    }

    /// Opens a fresh run for the current kernel session.
    ///
    /// `components` carries the registered manifest written to the run's `RunStarted` entry.
    /// `environment` selects the clock source the implementation uses to stamp publish
    /// timestamps. Idempotency across reset/rerun is the implementation's responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error when opening the new run, spawning the writer, or blocking on the
    /// initial entry ack fails.
    fn open(
        &mut self,
        instance_id: UUID4,
        components: &RegisteredComponents,
        environment: Environment,
    ) -> anyhow::Result<()>;

    /// Returns a snapshot anchorer for the currently open run, when capture is active.
    ///
    /// The execution engine installs the returned callback so position snapshots commit a
    /// matching anchor entry against the durable high-watermark.
    fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer>;

    /// Poisons the current run so it cannot be sealed as a clean `Ended` generation.
    ///
    /// Terminal infrastructure failures call this even when a final conservative risk checkpoint
    /// appeared to be admitted. Admission alone does not prove an authoritative bus tap persisted
    /// it, so implementations must leave the run unsealed (or otherwise non-clean) and make the
    /// next boot apply the crash-recovery safe-disabled interlock represented by `force_halted`.
    ///
    /// # Errors
    ///
    /// Returns an error when the implementation cannot latch the terminal failure. Callers still
    /// terminate the process and must not treat a later seal receipt as authoritative.
    fn fail_stop(&mut self, reason: &str) -> anyhow::Result<()> {
        anyhow::bail!("event-store implementation cannot latch terminal failure: {reason}")
    }

    /// Seals the open run by writing the terminal entry and updating the manifest.
    ///
    /// A configured implementation returns a receipt only after the terminal manifest and full
    /// durable entry set pass verification. An absent session returns `Ok(None)`. Halted or
    /// unverifiable sessions return an error and defer crash recovery to the next boot.
    ///
    /// # Errors
    ///
    /// Returns an error when close, manifest seal, or post-seal verification fails.
    fn seal(&mut self, ts_init: UnixNanos) -> anyhow::Result<Option<EventStoreSealReceipt>>;

    /// Returns the run id of the currently open run, when capture is active.
    fn run_id(&self) -> Option<&str>;

    /// Returns the configured replay source or recovered parent run id, when present.
    fn parent_run_id(&self) -> Option<&str>;

    /// Returns whether the current config enables event-store replay.
    ///
    /// Event-store replay restores cache state and opens a child run for inspection. The kernel
    /// promotes this config state to runtime state only after restore and open both succeed.
    fn is_event_store_replay_configured(&self) -> bool {
        false
    }

    /// Returns whether the implementation has signaled a fail-stop condition.
    fn is_halted(&self) -> bool;
}

/// How the supervisor prunes sealed run files.
///
/// The kernel records the choice in the manifest's `feature_flags`; actual retention
/// enforcement is performed by a separate supervisor process and is out of scope for
/// the kernel boot path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RetentionMode {
    /// Keep every sealed run; never reclaim.
    #[default]
    Full,
    /// Keep at most `keep_last` sealed runs; the supervisor reclaims older files.
    Bounded {
        /// The number of sealed runs to retain.
        keep_last: usize,
    },
    /// Keep the manifest plus a snapshot anchor and the tail since the anchor; older
    /// entries reclaim once a newer anchor is durable.
    SnapshotAnchored,
}

/// Host-supplied wire commitments recorded for one run.
///
/// Canonical digest syntax alone does not prove that a value was produced by trusted measurement.
/// Live deployments are responsible for deriving these commitments inside their deployment trust
/// boundary. The authenticated backtest constructor is a concrete exception: it replaces caller
/// values with library-measured executable, dependency-lock, environment, and configuration
/// commitments. Defaults remain empty placeholders for non-strict or test configurations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunIdentity {
    /// A canonical content-digest commitment supplied for the host executable.
    pub binary_hash: String,
    /// The entry payload schema version.
    pub schema_version: u32,
    /// A canonical content-digest commitment supplied for `Cargo.lock` or an equivalent manifest.
    pub crate_versions: String,
    /// The active Cargo features for the trader binary.
    pub feature_flags: Vec<String>,
    /// Per-adapter version stamp keyed by adapter name.
    pub adapter_versions: IndexMap<String, String>,
    /// A canonical content-digest commitment supplied for the kernel configuration.
    pub config_hash: String,
    /// Immutable artifacts and promotion evidence bound to this run.
    #[serde(default)]
    pub provenance: RunProvenance,
    /// The deterministic seed, populated when the run executes under a seeded mode.
    pub seed: Option<u64>,
}

/// Exact event payload schema emitted and understood by this runtime generation.
pub const EVENT_STORE_ENTRY_SCHEMA_VERSION: u32 = 1;

impl Default for RunIdentity {
    fn default() -> Self {
        Self {
            binary_hash: String::new(),
            schema_version: EVENT_STORE_ENTRY_SCHEMA_VERSION,
            crate_versions: String::new(),
            feature_flags: Vec::new(),
            adapter_versions: IndexMap::new(),
            config_hash: String::new(),
            provenance: RunProvenance::default(),
            seed: None,
        }
    }
}

/// The id of a captured run: `<start_ts_init>-<short_uuid>`, sortable by start time.
///
/// The runtime constructs this from the kernel's start timestamp plus a fresh `UUID4` so
/// the representation stays stable across processes and platforms.
pub type RunId = String;

/// Default maximum interval between data-marker cursor snapshots when no entry boundary occurs.
pub const DEFAULT_DATA_MARKER_SAFETY_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Default capacity of the data-marker writer's bounded submit channel.
pub const DEFAULT_DATA_MARKER_CHANNEL_CAPACITY: usize = 10_000;

/// Market-data class enabled for data-marker capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataMarkerClass {
    /// Order-book delta stream.
    BookDeltas,
    /// Level-10 order-book snapshot stream.
    BookDepth10,
    /// Quote (level-1 bid/ask) stream.
    Quote,
    /// Trade (last sale) stream.
    Trade,
    /// Bar (OHLCV aggregate) stream.
    Bar,
}

impl DataMarkerClass {
    /// All builtin data-marker classes in canonical order.
    pub const ALL: [Self; 5] = [
        Self::BookDeltas,
        Self::BookDepth10,
        Self::Quote,
        Self::Trade,
        Self::Bar,
    ];
}

/// Opt-in data-marker sidecar settings for an event-store run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataMarkerConfig {
    /// Market-data classes captured into marker cursors.
    #[serde(default = "default_data_marker_classes")]
    pub classes: Vec<DataMarkerClass>,
    /// Maximum interval between cursor snapshots when data advances without entry submissions.
    #[serde(default = "default_data_marker_safety_flush_interval")]
    pub safety_flush_interval: Duration,
    /// Capacity of the marker writer's bounded submit channel.
    #[serde(default = "default_data_marker_channel_capacity")]
    pub channel_capacity: usize,
    /// Instrument identifiers that emit one high-fidelity marker per observed data message.
    #[serde(default)]
    pub high_fidelity: Vec<String>,
}

impl Default for DataMarkerConfig {
    fn default() -> Self {
        Self {
            classes: default_data_marker_classes(),
            safety_flush_interval: DEFAULT_DATA_MARKER_SAFETY_FLUSH_INTERVAL,
            channel_capacity: DEFAULT_DATA_MARKER_CHANNEL_CAPACITY,
            high_fidelity: Vec::new(),
        }
    }
}

fn default_data_marker_classes() -> Vec<DataMarkerClass> {
    DataMarkerClass::ALL.to_vec()
}

const fn default_data_marker_safety_flush_interval() -> Duration {
    DEFAULT_DATA_MARKER_SAFETY_FLUSH_INTERVAL
}

const fn default_data_marker_channel_capacity() -> usize {
    DEFAULT_DATA_MARKER_CHANNEL_CAPACITY
}

/// Configuration for the kernel-managed event store run lifecycle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventStoreConfig {
    /// Root directory; the backend creates `<base_dir>/<instance_id>/<run_id>.redb`.
    pub base_dir: PathBuf,
    /// Stable identification for this trader instance and binary.
    pub identity: RunIdentity,
    /// How the supervisor reclaims sealed run files.
    pub retention: RetentionMode,
    /// Sealed run to restore cache state from before opening a fresh run.
    ///
    /// When set, this enables event-store replay: the kernel restores cache state from this run,
    /// records it as the parent link for the fresh child run, and then skips engines, clients,
    /// trader startup, and live reconciliation. Quarantined runs are rejected.
    pub replay_from_run_id: Option<RunId>,
    /// Data-marker sidecar settings. `None` disables marker capture for the run.
    #[serde(default)]
    pub data_markers: Option<DataMarkerConfig>,
    /// Capacity of the writer's bounded submit channel.
    pub channel_capacity: usize,
    /// Submit-side stall ceiling that triggers writer fail-stop.
    pub halt_threshold: Duration,
    /// Maximum time to wait for the `RunStarted` entry to durably commit before the
    /// kernel surfaces an event-store boot error.
    pub run_started_timeout: Duration,
}

impl Default for EventStoreConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::new(),
            identity: RunIdentity::default(),
            retention: RetentionMode::default(),
            replay_from_run_id: None,
            data_markers: None,
            channel_capacity: 10_000,
            halt_threshold: Duration::from_millis(250),
            run_started_timeout: Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn event_store_config_serde_roundtrip() {
        let config = EventStoreConfig::default();
        let json = serde_json::to_string(&config).expect("serialize");
        let restored: EventStoreConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(restored.channel_capacity, config.channel_capacity);
        assert_eq!(restored.halt_threshold, config.halt_threshold);
        assert_eq!(restored.run_started_timeout, config.run_started_timeout);
        assert_eq!(restored.base_dir, config.base_dir);
        assert_eq!(restored.retention, config.retention);
        assert_eq!(restored.replay_from_run_id, config.replay_from_run_id);
        assert_eq!(restored.identity, config.identity);
        assert_eq!(restored.data_markers, config.data_markers);
    }

    #[rstest]
    fn data_marker_config_serde_roundtrip() {
        let config = EventStoreConfig {
            data_markers: Some(DataMarkerConfig {
                classes: vec![DataMarkerClass::Quote, DataMarkerClass::BookDeltas],
                safety_flush_interval: Duration::from_millis(250),
                channel_capacity: 512,
                high_fidelity: vec!["ETHUSDT-PERP.BINANCE".to_string()],
            }),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).expect("serialize");
        let restored: EventStoreConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(restored.data_markers, config.data_markers);
    }

    #[rstest]
    fn retention_mode_serde_roundtrip() {
        for mode in [
            RetentionMode::Full,
            RetentionMode::Bounded { keep_last: 5 },
            RetentionMode::SnapshotAnchored,
        ] {
            let json = serde_json::to_string(&mode).expect("serialize");
            let restored: RetentionMode = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(restored, mode);
        }
    }

    #[rstest]
    fn event_store_config_default_values() {
        let config = EventStoreConfig::default();

        assert_eq!(config.channel_capacity, 10_000);
        assert_eq!(config.halt_threshold, Duration::from_millis(250));
        assert_eq!(config.run_started_timeout, Duration::from_secs(5));
        assert_eq!(config.base_dir, PathBuf::new());
        assert_eq!(config.retention, RetentionMode::Full);
        assert!(config.replay_from_run_id.is_none());
        assert_eq!(config.identity, RunIdentity::default());
        assert_eq!(
            config.identity.schema_version,
            EVENT_STORE_ENTRY_SCHEMA_VERSION
        );
        assert!(config.data_markers.is_none());
    }

    #[rstest]
    fn data_markers_default_is_none() {
        let config = EventStoreConfig::default();

        assert!(config.data_markers.is_none());
    }
}
