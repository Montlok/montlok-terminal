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

//! The core `BacktestEngine` for backtesting on historical data.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    fmt::Debug,
    mem::size_of,
    rc::{Rc, Weak},
    sync::Arc,
};

use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use nautilus_analysis::analyzer::PortfolioAnalyzer;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::Cache,
    clock::{Clock, TestClock},
    component::Component,
    enums::LogColor,
    log_info,
    logging::{
        logging_clock_set_realtime_mode, logging_clock_set_static_mode,
        logging_clock_set_static_time,
    },
    runner::{
        SyncDataCommandSender, SyncTradingCommandSender, data_cmd_queue_is_empty,
        drain_data_cmd_queue, drain_trading_cmd_queue, replace_data_cmd_sender,
        replace_exec_cmd_sender, trading_cmd_queue_is_empty,
    },
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{
    UUID4, UnixNanos, datetime::unix_nanos_to_iso8601, string::formatting::Separable,
    time::nanos_since_unix_epoch,
};
use nautilus_data::client::DataClientAdapter;
use nautilus_event_store::{EventStoreLifecycle, EventStoreLifecycleOptions};
use nautilus_execution::models::fill::FillModelHandle;
use nautilus_model::{
    accounts::{Account, AccountAny},
    data::{Data, HasTsInit},
    enums::{AccountType, AggregationSource, BookType},
    identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny},
    position::Position,
    types::Price,
};
use nautilus_provenance::{
    ArtifactId, ArtifactRef, ContentDigest, DatasetBinding, ExecutionEnvironmentBinding,
    ExecutionEnvironmentScope, ExecutionMarketScope, ProcessIdentity, RunProvenance,
    StrategyBinding,
};
use nautilus_serialization::sbe::{DataAny, FromSbe, ToSbe};
#[cfg(feature = "python")]
use nautilus_system::trader::Trader;
use nautilus_system::{
    EventStoreFactory, EventStoreSealReceipt, KernelEventStore,
    config::NautilusKernelConfig,
    event_store::{EVENT_STORE_ENTRY_SCHEMA_VERSION, EventStoreConfig, RunIdentity},
    kernel::NautilusKernel,
    trader::{ArtifactConfiguredStrategy, ArtifactConfiguredStrategyInstance},
};
use nautilus_trading::{
    ExecutionAlgorithm, ExecutionAlgorithmNative,
    strategy::{Strategy, StrategyNative},
};

use crate::{
    accumulator::TimeEventAccumulator,
    config::{BacktestEngineConfig, SimulatedVenueConfig},
    data_client::BacktestDataClient,
    data_iterator::{BacktestDataIterator, replay_key},
    environment::{BACKTEST_ENVIRONMENT_LOADER_ID, BacktestEnvironmentSpec},
    exchange::SimulatedExchange,
    execution_client::BacktestExecutionClient,
    result::BacktestResult,
};

/// Core backtesting engine for running event-driven strategy backtests on historical data.
///
/// The `BacktestEngine` provides a configurable simulation environment that processes historical
/// market data chronologically through an event-driven architecture. Fidelity is limited to the
/// market rules, models, and adapters explicitly installed for a run; generic simulation must not
/// be treated as proof that behavior exactly matches a live venue.
///
/// - Event-driven data replay with configurable latency models.
/// - Multi-venue and multi-asset support.
/// - Built-in order matching and execution simulation.
/// - Strategy and portfolio performance analysis.
/// - Strategy abstractions shared with live orchestration.
#[expect(
    clippy::struct_excessive_bools,
    reason = "sorting, shutdown, completion, and provenance flags are orthogonal engine lifecycle facts"
)]
pub struct BacktestEngine {
    kernel: NautilusKernel,
    instance_id: UUID4,
    config: BacktestEngineConfig,
    /// Digest of the host executable bytes measured once during engine startup.
    ///
    /// This is a startup file snapshot, not a measurement of dynamic dependencies or the process
    /// memory image.
    host_executable_startup_digest: ContentDigest,
    accumulator: TimeEventAccumulator,
    run_config_id: Option<String>,
    run_id: Option<UUID4>,
    run_provenance: RunProvenance,
    run_provenance_required: bool,
    /// Whether the authenticated constructor replaced caller placeholders with a library-owned
    /// event-store run identity derived from concrete executable, dependency, environment, and
    /// secure-default configuration commitments.
    library_owned_run_identity: bool,
    measured_run_inputs: MeasuredRunInputs,
    run_inputs_verified: bool,
    event_store_receipt: Option<EventStoreSealReceipt>,
    unmeasured_data_loaded: bool,
    unmeasured_strategy_loaded: bool,
    strategy_artifact_attempt_tainted: bool,
    unmeasured_actor_loaded: bool,
    unmeasured_exec_algorithm_loaded: bool,
    unmeasured_environment_mutation: bool,
    secure_boundary_escaped: Cell<bool>,
    streamed_data_evicted: bool,
    incomplete_artifact_data_evicted: bool,
    venues: IndexMap<Venue, Rc<RefCell<SimulatedExchange>>>,
    exec_clients: Vec<BacktestExecutionClient>,
    has_data: AHashSet<InstrumentId>,
    has_book_data: AHashSet<InstrumentId>,
    has_book_processed: AHashSet<InstrumentId>,
    data_iterator: BacktestDataIterator,
    data_len: usize,
    data_stream_counter: usize,
    ts_first: Option<UnixNanos>,
    ts_last_data: Option<UnixNanos>,
    sorted: bool,
    iteration: usize,
    force_stop: bool,
    last_ns: UnixNanos,
    last_module_ns: Option<UnixNanos>,
    last_liquidation_ns: Option<UnixNanos>,
    end_ns: UnixNanos,
    run_started: Option<UnixNanos>,
    run_finished: Option<UnixNanos>,
    backtest_start: Option<UnixNanos>,
    backtest_end: Option<UnixNanos>,
    funding_error: Option<String>,
    run_failure: Option<String>,
    run_succeeded: bool,
    completed_result: Option<BacktestResult>,
}

#[derive(Debug, Default)]
struct MeasuredRunInputs {
    data: Vec<MeasuredDataset>,
    environment: Option<ExecutionEnvironmentBinding>,
    strategy: Option<StrategyBinding>,
    config: Option<ArtifactRef>,
}

#[derive(Debug)]
struct MeasuredDataset {
    binding: DatasetBinding,
    stream_origin: Option<i32>,
    consumed_records: u64,
}

/// Controls market-data routing for an artifact-backed data batch.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ArtifactDataLoadOptions {
    /// Optional market-data client identity used for routing.
    pub client_id: Option<ClientId>,
}

/// Current canonical backtest data artifact schema version.
pub const BACKTEST_DATA_ARTIFACT_SCHEMA_VERSION: u16 = 1;

/// Stable identity of the library-owned canonical data artifact decoder.
pub const BACKTEST_DATA_ARTIFACT_DECODER_ID: &str =
    "nautilus-backtest/data-artifact-length-prefixed-sbe-v1";

/// Maximum accepted encoded data artifact size (512 MiB).
pub const MAX_BACKTEST_DATA_ARTIFACT_BYTES: usize = 512 * 1024 * 1024;

/// Maximum number of records accepted in one data artifact.
pub const MAX_BACKTEST_DATA_ARTIFACT_RECORDS: usize = 1_000_000;

/// Maximum accepted size of one framed SBE record (16 MiB).
pub const MAX_BACKTEST_DATA_ARTIFACT_FRAME_BYTES: usize = 16 * 1024 * 1024;

const BACKTEST_DATA_ARTIFACT_MAGIC: &[u8; 8] = b"NTBTSBE\0";
const BACKTEST_DATA_ARTIFACT_HEADER_BYTES: usize = 16;
const BACKTEST_DATA_ARTIFACT_RESERVED_FLAGS: u8 = 0;
const MATERIALIZED_DATA_DOMAIN: &[u8] = b"nautilus-backtest/materialized-data/sbe-v1";

/// Encodes built-in market data into the canonical artifact representation.
///
/// The format is a fixed 16-byte header followed by length-prefixed `DataAny` SBE frames. Custom
/// and DeFi data are rejected because they do not have a library-owned canonical SBE codec.
/// Record order is preserved and therefore remains part of the authenticated artifact bytes.
///
/// # Errors
///
/// Returns an error for empty or oversized batches, unsupported data variants, or SBE encoding
/// failures.
pub fn encode_backtest_data_artifact(data: &[Data]) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(!data.is_empty(), "Backtest data artifact must not be empty");
    anyhow::ensure!(
        data.len() <= MAX_BACKTEST_DATA_ARTIFACT_RECORDS,
        "Backtest data artifact record count {} exceeds limit {}",
        data.len(),
        MAX_BACKTEST_DATA_ARTIFACT_RECORDS
    );
    let record_count = u32::try_from(data.len()).map_err(|error| {
        anyhow::anyhow!("Backtest data artifact record count overflow: {error}")
    })?;
    let mut frames = Vec::with_capacity(data.len());
    let mut encoded_size = BACKTEST_DATA_ARTIFACT_HEADER_BYTES;
    for (index, item) in data.iter().enumerate() {
        let frame = data_to_sbe(item)?.to_sbe().map_err(|error| {
            anyhow::anyhow!("Cannot encode backtest data artifact record {index}: {error}")
        })?;
        anyhow::ensure!(
            !frame.is_empty() && frame.len() <= MAX_BACKTEST_DATA_ARTIFACT_FRAME_BYTES,
            "Backtest data artifact record {index} frame size {} is outside 1..={}",
            frame.len(),
            MAX_BACKTEST_DATA_ARTIFACT_FRAME_BYTES
        );
        let _ = u32::try_from(frame.len()).map_err(|error| {
            anyhow::anyhow!("Backtest data artifact record {index} frame size overflow: {error}")
        })?;
        encoded_size = encoded_size
            .checked_add(size_of::<u32>())
            .and_then(|size| size.checked_add(frame.len()))
            .ok_or_else(|| anyhow::anyhow!("Backtest data artifact size overflow"))?;
        anyhow::ensure!(
            encoded_size <= MAX_BACKTEST_DATA_ARTIFACT_BYTES,
            "Backtest data artifact size exceeds limit {MAX_BACKTEST_DATA_ARTIFACT_BYTES}"
        );
        frames.push(frame);
    }

    let mut encoded = Vec::with_capacity(encoded_size);
    encoded.extend_from_slice(BACKTEST_DATA_ARTIFACT_MAGIC);
    encoded.extend_from_slice(&BACKTEST_DATA_ARTIFACT_SCHEMA_VERSION.to_be_bytes());
    encoded.push(compiled_precision_flag());
    encoded.push(BACKTEST_DATA_ARTIFACT_RESERVED_FLAGS);
    encoded.extend_from_slice(&record_count.to_be_bytes());
    for frame in frames {
        encoded.extend_from_slice(
            &u32::try_from(frame.len())
                .map_err(|error| {
                    anyhow::anyhow!("Backtest data artifact frame size overflow: {error}")
                })?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&frame);
    }
    debug_assert_eq!(encoded.len(), encoded_size);
    Ok(encoded)
}

fn decode_backtest_data_artifact(artifact_content: &[u8]) -> anyhow::Result<Vec<Data>> {
    anyhow::ensure!(
        artifact_content.len() <= MAX_BACKTEST_DATA_ARTIFACT_BYTES,
        "Backtest data artifact size {} exceeds limit {}",
        artifact_content.len(),
        MAX_BACKTEST_DATA_ARTIFACT_BYTES
    );
    anyhow::ensure!(
        artifact_content.len() >= BACKTEST_DATA_ARTIFACT_HEADER_BYTES,
        "Backtest data artifact is shorter than its fixed header"
    );
    let mut offset = 0;
    let magic = take_artifact_bytes(
        artifact_content,
        &mut offset,
        BACKTEST_DATA_ARTIFACT_MAGIC.len(),
        "magic",
    )?;
    anyhow::ensure!(
        magic == BACKTEST_DATA_ARTIFACT_MAGIC,
        "Backtest data artifact magic mismatch"
    );
    let schema_version = read_artifact_u16(artifact_content, &mut offset, "schema version")?;
    anyhow::ensure!(
        schema_version == BACKTEST_DATA_ARTIFACT_SCHEMA_VERSION,
        "Unsupported backtest data artifact schema {schema_version}; expected {BACKTEST_DATA_ARTIFACT_SCHEMA_VERSION}"
    );
    let precision = take_artifact_bytes(artifact_content, &mut offset, 1, "precision flag")?[0];
    anyhow::ensure!(
        matches!(precision, 0 | 1),
        "Backtest data artifact precision flag {precision} is invalid"
    );
    anyhow::ensure!(
        precision == compiled_precision_flag(),
        "Backtest data artifact precision flag {precision} does not match compiled precision {}",
        compiled_precision_flag()
    );
    let reserved_flags =
        take_artifact_bytes(artifact_content, &mut offset, 1, "reserved flags")?[0];
    anyhow::ensure!(
        reserved_flags == BACKTEST_DATA_ARTIFACT_RESERVED_FLAGS,
        "Backtest data artifact reserved flags must be zero"
    );
    let record_count = usize::try_from(read_artifact_u32(
        artifact_content,
        &mut offset,
        "record count",
    )?)
    .map_err(|error| anyhow::anyhow!("Backtest data artifact record count overflow: {error}"))?;
    anyhow::ensure!(record_count > 0, "Backtest data artifact must not be empty");
    anyhow::ensure!(
        record_count <= MAX_BACKTEST_DATA_ARTIFACT_RECORDS,
        "Backtest data artifact record count {record_count} exceeds limit {MAX_BACKTEST_DATA_ARTIFACT_RECORDS}"
    );

    let mut data = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let frame_len = usize::try_from(read_artifact_u32(
            artifact_content,
            &mut offset,
            "record frame length",
        )?)
        .map_err(|error| {
            anyhow::anyhow!("Backtest data artifact record {index} frame size overflow: {error}")
        })?;
        anyhow::ensure!(
            frame_len > 0 && frame_len <= MAX_BACKTEST_DATA_ARTIFACT_FRAME_BYTES,
            "Backtest data artifact record {index} frame size {frame_len} is outside 1..={MAX_BACKTEST_DATA_ARTIFACT_FRAME_BYTES}"
        );
        let frame = take_artifact_bytes(artifact_content, &mut offset, frame_len, "record frame")?;
        let decoded = DataAny::from_sbe(frame).map_err(|error| {
            anyhow::anyhow!("Cannot decode backtest data artifact record {index}: {error}")
        })?;
        let canonical = decoded.to_sbe().map_err(|error| {
            anyhow::anyhow!("Cannot canonicalize backtest data artifact record {index}: {error}")
        })?;
        anyhow::ensure!(
            canonical == frame,
            "Backtest data artifact record {index} is not an exact canonical SBE frame"
        );
        data.push(data_from_sbe(decoded));
    }
    anyhow::ensure!(
        offset == artifact_content.len(),
        "Backtest data artifact has {} trailing bytes",
        artifact_content.len() - offset
    );
    Ok(data)
}

const fn compiled_precision_flag() -> u8 {
    if cfg!(feature = "high-precision") {
        1
    } else {
        0
    }
}

fn take_artifact_bytes<'a>(
    artifact_content: &'a [u8],
    offset: &mut usize,
    len: usize,
    field: &str,
) -> anyhow::Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("Backtest data artifact {field} offset overflow"))?;
    let value = artifact_content.get(*offset..end).ok_or_else(|| {
        anyhow::anyhow!(
            "Backtest data artifact {field} is truncated at byte {}",
            *offset
        )
    })?;
    *offset = end;
    Ok(value)
}

fn read_artifact_u16(
    artifact_content: &[u8],
    offset: &mut usize,
    field: &str,
) -> anyhow::Result<u16> {
    let bytes: [u8; 2] = take_artifact_bytes(artifact_content, offset, 2, field)?
        .try_into()
        .map_err(|error| anyhow::anyhow!("Invalid backtest data artifact {field}: {error}"))?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_artifact_u32(
    artifact_content: &[u8],
    offset: &mut usize,
    field: &str,
) -> anyhow::Result<u32> {
    let bytes: [u8; 4] = take_artifact_bytes(artifact_content, offset, 4, field)?
        .try_into()
        .map_err(|error| anyhow::anyhow!("Invalid backtest data artifact {field}: {error}"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn data_from_sbe(data: DataAny) -> Data {
    match data {
        DataAny::OrderBookDelta(value) => Data::Delta(value),
        DataAny::OrderBookDeltas(value) => {
            Data::Deltas(nautilus_model::data::OrderBookDeltas_API::new(value))
        }
        DataAny::OrderBookDepth10(value) => Data::Depth10(Box::new(value)),
        DataAny::Quote(value) => Data::Quote(value),
        DataAny::Trade(value) => Data::Trade(value),
        DataAny::Bar(value) => Data::Bar(value),
        DataAny::MarkPrice(value) => Data::MarkPriceUpdate(value),
        DataAny::IndexPrice(value) => Data::IndexPriceUpdate(value),
        DataAny::FundingRate(value) => Data::FundingRateUpdate(value),
        DataAny::OptionGreeks(value) => Data::OptionGreeks(value),
        DataAny::InstrumentStatus(value) => Data::InstrumentStatus(value),
        DataAny::InstrumentClose(value) => Data::InstrumentClose(value),
    }
}

fn canonical_materialized_data_digest(data: &[Data]) -> anyhow::Result<ContentDigest> {
    anyhow::ensure!(!data.is_empty(), "Artifact decoder returned no data");
    let mut ordered: Vec<_> = data.iter().collect();
    ordered.sort_by_key(|item| replay_key(item));

    let mut hasher = blake3::Hasher::new();
    hash_length_prefixed(&mut hasher, MATERIALIZED_DATA_DOMAIN);
    hasher.update(&[u8::from(cfg!(feature = "high-precision"))]);
    hasher.update(
        &u64::try_from(ordered.len())
            .map_err(|error| anyhow::anyhow!("Dataset record count overflow: {error}"))?
            .to_be_bytes(),
    );
    for item in ordered {
        let encoded = data_to_sbe(item)?.to_sbe()?;
        hash_length_prefixed(&mut hasher, &encoded);
    }
    Ok(ContentDigest::from_array(*hasher.finalize().as_bytes()))
}

#[allow(
    unreachable_patterns,
    clippy::match_wildcard_for_single_variants,
    reason = "nautilus-model feature unification can add current or future Data variants without enabling this crate's matching feature; preserve fail-closed handling"
)]
fn data_to_sbe(data: &Data) -> anyhow::Result<DataAny> {
    match data {
        Data::Delta(value) => Ok(DataAny::OrderBookDelta(*value)),
        Data::Deltas(value) => Ok(DataAny::OrderBookDeltas((**value).clone())),
        Data::Depth10(value) => Ok(DataAny::OrderBookDepth10(**value)),
        Data::Quote(value) => Ok(DataAny::Quote(*value)),
        Data::Trade(value) => Ok(DataAny::Trade(*value)),
        Data::Bar(value) => Ok(DataAny::Bar(*value)),
        Data::MarkPriceUpdate(value) => Ok(DataAny::MarkPrice(*value)),
        Data::IndexPriceUpdate(value) => Ok(DataAny::IndexPrice(*value)),
        Data::FundingRateUpdate(value) => Ok(DataAny::FundingRate(*value)),
        Data::OptionGreeks(value) => Ok(DataAny::OptionGreeks(*value)),
        Data::InstrumentStatus(value) => Ok(DataAny::InstrumentStatus(*value)),
        Data::InstrumentClose(value) => Ok(DataAny::InstrumentClose(*value)),
        Data::Custom(_) => anyhow::bail!(
            "Artifact-aware replay does not accept custom data without a canonical codec"
        ),
        // `nautilus-model/defi` can be feature-unified by another workspace member even when
        // this crate's own `defi` feature is disabled. Preserve the explicit Custom rejection
        // above and reject every non-built-in variant rather than relying on that local feature
        // flag for exhaustiveness.
        _ => anyhow::bail!("Artifact-aware replay does not accept data without a canonical codec"),
    }
}

fn hash_length_prefixed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn measure_host_executable_startup() -> anyhow::Result<ContentDigest> {
    let executable_path = std::env::current_exe()
        .map_err(|error| anyhow::anyhow!("Cannot resolve current executable: {error}"))?;
    let executable = std::fs::read(&executable_path).map_err(|error| {
        anyhow::anyhow!(
            "Cannot measure current executable startup snapshot '{}': {error}",
            executable_path.display()
        )
    })?;
    Ok(ContentDigest::digest_bytes(&executable))
}

const SECURE_BACKTEST_CONFIG_COMMITMENT_DOMAIN: &[u8] =
    b"nautilus-backtest/secure-default-v1/event-store-config";
const WORKSPACE_CARGO_LOCK: &[u8] = include_bytes!("../../../Cargo.lock");

fn install_library_owned_run_identity(
    event_store: &mut EventStoreConfig,
    environment_digest: ContentDigest,
    host_executable_startup_digest: ContentDigest,
) -> anyhow::Result<()> {
    let crate_versions_digest = ContentDigest::digest_bytes(WORKSPACE_CARGO_LOCK);

    // Commit the host-owned sink policy without recursively trusting or hashing caller-supplied
    // identity strings. The authenticated environment digest is paired explicitly with the
    // library's secure-default version so an equal filesystem config under another environment
    // cannot reuse the same commitment.
    let mut committed_config = event_store.clone();
    committed_config.identity = RunIdentity::default();
    let committed_config = serde_json::to_vec(&committed_config)
        .map_err(|error| anyhow::anyhow!("Cannot commit secure event-store config: {error}"))?;
    let mut hasher = blake3::Hasher::new();
    hash_length_prefixed(&mut hasher, SECURE_BACKTEST_CONFIG_COMMITMENT_DOMAIN);
    hash_length_prefixed(&mut hasher, environment_digest.as_bytes());
    hash_length_prefixed(&mut hasher, &committed_config);
    let config_digest = ContentDigest::from_array(*hasher.finalize().as_bytes());

    event_store.identity = RunIdentity {
        binary_hash: host_executable_startup_digest.to_string(),
        schema_version: EVENT_STORE_ENTRY_SCHEMA_VERSION,
        crate_versions: crate_versions_digest.to_string(),
        feature_flags: compiled_backtest_feature_flags(),
        adapter_versions: IndexMap::new(),
        config_hash: config_digest.to_string(),
        provenance: RunProvenance::default(),
        seed: None,
    };
    Ok(())
}

fn compiled_backtest_feature_flags() -> Vec<String> {
    let mut flags = Vec::new();
    for (enabled, name) in [
        (cfg!(feature = "defi"), "nautilus-backtest/defi"),
        (cfg!(feature = "examples"), "nautilus-backtest/examples"),
        (
            cfg!(feature = "extension-module"),
            "nautilus-backtest/extension-module",
        ),
        (cfg!(feature = "ffi"), "nautilus-backtest/ffi"),
        (
            cfg!(feature = "high-precision"),
            "nautilus-backtest/high-precision",
        ),
        (cfg!(feature = "plugin"), "nautilus-backtest/plugin"),
        (cfg!(feature = "python"), "nautilus-backtest/python"),
        (cfg!(feature = "streaming"), "nautilus-backtest/streaming"),
    ] {
        if enabled {
            flags.push(name.to_string());
        }
    }
    flags
}

fn redb_event_store_factory(
    config: EventStoreConfig,
    options: EventStoreLifecycleOptions,
) -> EventStoreFactory {
    Box::new(move |instance_id, clock| {
        let lifecycle =
            EventStoreLifecycle::boot_with_options(Some(config), instance_id, clock, options)?;
        Ok::<Box<dyn KernelEventStore>, anyhow::Error>(Box::new(lifecycle))
    })
}

impl Debug for BacktestEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BacktestEngine))
            .field("instance_id", &self.instance_id)
            .field("run_config_id", &self.run_config_id)
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

impl BacktestEngine {
    /// Create a new [`BacktestEngine`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the host executable startup snapshot cannot be measured or the core
    /// `NautilusKernel` fails to initialize.
    pub fn new(config: BacktestEngineConfig) -> anyhow::Result<Self> {
        let host_executable_startup_digest = measure_host_executable_startup()?;
        Self::new_with_host_executable_startup_digest(
            config,
            host_executable_startup_digest,
            EventStoreLifecycleOptions::new(),
        )
    }

    /// Creates a backtest engine with explicit process-local event-store lifecycle policy.
    ///
    /// This is the production constructor for configurations containing an authoritative Redb
    /// event store: callers inject a measured process-isolated verifier through `options`. The
    /// helper path and expected digest remain outside serialized strategy/run configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the host executable cannot be measured, event-store recovery fails
    /// closed, or the kernel cannot be initialized.
    pub fn new_with_event_store_lifecycle_options(
        config: BacktestEngineConfig,
        options: EventStoreLifecycleOptions,
    ) -> anyhow::Result<Self> {
        let host_executable_startup_digest = measure_host_executable_startup()?;
        Self::new_with_host_executable_startup_digest(
            config,
            host_executable_startup_digest,
            options,
        )
    }

    fn new_with_host_executable_startup_digest(
        mut config: BacktestEngineConfig,
        host_executable_startup_digest: ContentDigest,
        event_store_options: EventStoreLifecycleOptions,
    ) -> anyhow::Result<Self> {
        // The engine does not replay `add_instrument` on reset, so reruns rely
        // on the cache retaining instruments regardless of the caller's config.
        let mut cache_config = config.cache.unwrap_or_default();
        cache_config.drop_instruments_on_reset = false;
        config.cache = Some(cache_config);
        let event_store_factory = config
            .event_store
            .clone()
            .map(|config| redb_event_store_factory(config, event_store_options));
        let kernel = NautilusKernel::new_with(
            "BacktestEngine".to_string(),
            config.clone(),
            None,
            event_store_factory,
        )?;
        let instance_id = kernel.instance_id;
        #[cfg(feature = "python")]
        if let Some(controller) = config.controller.as_ref() {
            Trader::add_controller_from_importable_config(&kernel.trader, controller)?;
        }
        #[cfg(not(feature = "python"))]
        if let Some(controller) = config.controller.as_ref() {
            anyhow::bail!(
                "BacktestEngineConfig.controller for importable controller '{}' requires the python feature",
                controller.controller_path
            );
        }

        Ok(Self {
            kernel,
            instance_id,
            config,
            host_executable_startup_digest,
            accumulator: TimeEventAccumulator::new(),
            run_config_id: None,
            run_id: None,
            run_provenance: RunProvenance::default(),
            run_provenance_required: false,
            library_owned_run_identity: false,
            measured_run_inputs: MeasuredRunInputs::default(),
            run_inputs_verified: false,
            event_store_receipt: None,
            unmeasured_data_loaded: false,
            unmeasured_strategy_loaded: false,
            strategy_artifact_attempt_tainted: false,
            unmeasured_actor_loaded: false,
            unmeasured_exec_algorithm_loaded: false,
            unmeasured_environment_mutation: false,
            secure_boundary_escaped: Cell::new(false),
            streamed_data_evicted: false,
            incomplete_artifact_data_evicted: false,
            venues: IndexMap::new(),
            exec_clients: Vec::new(),
            has_data: AHashSet::new(),
            has_book_data: AHashSet::new(),
            has_book_processed: AHashSet::new(),
            data_iterator: BacktestDataIterator::new(),
            data_len: 0,
            data_stream_counter: 0,
            ts_first: None,
            ts_last_data: None,
            sorted: true,
            iteration: 0,
            force_stop: false,
            last_ns: UnixNanos::default(),
            last_module_ns: None,
            last_liquidation_ns: None,
            end_ns: UnixNanos::default(),
            run_started: None,
            run_finished: None,
            backtest_start: None,
            backtest_end: None,
            funding_error: None,
            run_failure: None,
            run_succeeded: false,
            completed_result: None,
        })
    }

    /// Creates an engine, venues, and instrument universe from one immutable environment artifact.
    ///
    /// This is the only constructor which can satisfy the execution-environment requirement of a
    /// provenance-enforced run. It ensures the authenticated bytes drive construction before the
    /// environment binding is minted. Parsing, schema validation, and runtime construction are
    /// library-owned: callers cannot supply a decoder or factory which ignores the authenticated
    /// bytes. The artifact must use [`BacktestEnvironmentSpec`]'s unique canonical JSON form.
    ///
    /// # Errors
    ///
    /// Returns an error when canonical decoding or engine construction fails, a required market
    /// context is absent, or a venue or instrument cannot be installed.
    pub fn from_environment_artifact(
        artifact_id: ArtifactId,
        artifact_content: &[u8],
    ) -> anyhow::Result<Self> {
        Self::from_environment_artifact_inner(
            artifact_id,
            artifact_content,
            None,
            EventStoreLifecycleOptions::new(),
        )
    }

    /// Creates an authenticated environment engine with a concrete authoritative event store.
    ///
    /// This compatibility constructor preserves the canonical environment artifact format: the
    /// artifact still exclusively describes the measured market environment, while
    /// `event_store` selects the host-owned Redb evidence sink. The event-store run commits the
    /// authenticated [`RunProvenance`] at startup and must seal and verify before release
    /// promotion can consume the result.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Self::from_environment_artifact`], or when
    /// the concrete event-store lifecycle cannot recover its configured instance directory.
    pub fn from_environment_artifact_with_event_store(
        artifact_id: ArtifactId,
        artifact_content: &[u8],
        event_store: EventStoreConfig,
    ) -> anyhow::Result<Self> {
        Self::from_environment_artifact_inner(
            artifact_id,
            artifact_content,
            Some(event_store),
            EventStoreLifecycleOptions::new(),
        )
    }

    /// Creates an authenticated environment engine with an authoritative event store and
    /// explicit process-local lifecycle policy.
    ///
    /// This is the secure production entry point. `options` must carry an explicitly measured
    /// process-isolated verifier for canonical Redb recovery and sealing; it is intentionally not
    /// serializable and cannot be supplied by the authenticated environment artifact.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Self::from_environment_artifact`], when
    /// the isolated helper policy is missing or invalid, or when event-store recovery fails.
    pub fn from_environment_artifact_with_event_store_options(
        artifact_id: ArtifactId,
        artifact_content: &[u8],
        event_store: EventStoreConfig,
        options: EventStoreLifecycleOptions,
    ) -> anyhow::Result<Self> {
        Self::from_environment_artifact_inner(
            artifact_id,
            artifact_content,
            Some(event_store),
            options,
        )
    }

    fn from_environment_artifact_inner(
        artifact_id: ArtifactId,
        artifact_content: &[u8],
        mut event_store: Option<EventStoreConfig>,
        event_store_options: EventStoreLifecycleOptions,
    ) -> anyhow::Result<Self> {
        let environment = BacktestEnvironmentSpec::from_canonical_json(artifact_content)?;
        let environment_digest = ContentDigest::digest_bytes(artifact_content);
        let host_executable_startup_digest = measure_host_executable_startup()?;
        if let Some(event_store) = event_store.as_mut() {
            install_library_owned_run_identity(
                event_store,
                environment_digest,
                host_executable_startup_digest,
            )?;
        }
        let expected_identity = event_store.as_ref().map(|config| config.identity.clone());
        let market_scopes = environment
            .venues
            .iter()
            .map(|venue| {
                ExecutionMarketScope::new(
                    venue.venue.to_string(),
                    venue.profile.stable_id(),
                    venue.listing_venue.stable_id(),
                    venue.execution_route.stable_id(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let scope = ExecutionEnvironmentScope::new(
            environment.replay_start_ns,
            environment.replay_end_ns,
            environment.knowledge_cutoff_ns,
            market_scopes,
        )?;
        let (mut engine_config, venues, instruments, market_contexts) =
            environment.into_runtime()?;
        engine_config.event_store = event_store;
        let mut engine = Self::new_with_host_executable_startup_digest(
            engine_config,
            host_executable_startup_digest,
            event_store_options,
        )?;
        engine.library_owned_run_identity = engine
            .config
            .event_store
            .as_ref()
            .zip(expected_identity.as_ref())
            .is_some_and(|(installed, expected)| installed.identity == *expected);
        for venue in venues {
            engine.add_venue(venue)?;
        }
        for instrument in instruments {
            engine.add_instrument(&instrument)?;
        }

        let venues = engine
            .venues
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let instruments = engine
            .venues
            .values()
            .flat_map(|exchange| {
                exchange
                    .borrow()
                    .instrument_ids()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let binding = ExecutionEnvironmentBinding::new_scoped(
            ArtifactRef::new(artifact_id, environment_digest),
            ProcessIdentity::new(BACKTEST_ENVIRONMENT_LOADER_ID)?,
            market_contexts,
            venues,
            instruments,
            scope,
        )?;
        engine.run_provenance.environment = Some(binding.clone());
        engine.measured_run_inputs.environment = Some(binding);
        engine.unmeasured_environment_mutation = false;
        Ok(engine)
    }

    /// Returns a reference to the underlying kernel.
    #[must_use]
    pub fn kernel(&self) -> &NautilusKernel {
        self.secure_boundary_escaped.set(true);
        &self.kernel
    }

    /// Returns a mutable reference to the underlying kernel.
    pub fn kernel_mut(&mut self) -> &mut NautilusKernel {
        self.secure_boundary_escaped.set(true);
        &mut self.kernel
    }

    /// Returns the kernel to trusted orchestration inside this crate without tainting evidence.
    #[cfg(feature = "streaming")]
    pub(crate) const fn kernel_internal(&self) -> &NautilusKernel {
        &self.kernel
    }

    /// Returns the trader ID for this engine.
    #[must_use]
    pub fn trader_id(&self) -> TraderId {
        self.kernel.trader_id()
    }

    /// Returns the machine ID for this engine.
    #[must_use]
    pub fn machine_id(&self) -> &str {
        self.kernel.machine_id()
    }

    /// Returns the unique instance ID for this engine.
    #[must_use]
    pub fn instance_id(&self) -> UUID4 {
        self.instance_id
    }

    /// Returns the current iteration count.
    #[must_use]
    pub fn iteration(&self) -> usize {
        self.iteration
    }

    /// Returns the last run config ID, if any.
    #[must_use]
    pub fn run_config_id(&self) -> Option<&str> {
        self.run_config_id.as_deref()
    }

    /// Returns the last run ID, if any.
    #[must_use]
    pub const fn run_id(&self) -> Option<UUID4> {
        self.run_id
    }

    /// Returns the immutable artifact bindings configured for the next or current run.
    #[must_use]
    pub const fn run_provenance(&self) -> &RunProvenance {
        &self.run_provenance
    }

    /// Binds immutable data, strategy, model, config, and policy artifacts to the next run.
    ///
    /// # Errors
    ///
    /// Returns an error after the run has started. Call [`Self::reset`] before binding evidence
    /// for another run.
    pub fn set_run_provenance(&mut self, provenance: RunProvenance) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.iteration == 0 && self.run_id.is_none(),
            "Cannot change run provenance after a backtest has started"
        );
        self.ensure_measured_inputs_match(&provenance, false)?;
        self.run_provenance = provenance;
        Ok(())
    }

    /// Decodes and loads one immutable data artifact while measuring its exact source bytes.
    ///
    /// The bytes must use [`encode_backtest_data_artifact`]'s versioned canonical framing. Decoding
    /// and decoder identity are library-owned, so callers cannot substitute data unrelated to the
    /// authenticated source. Every decoded built-in record is validated against the installed
    /// instrument universe, and every bar must be externally aggregated.
    ///
    /// # Errors
    ///
    /// Returns an error for an artifact identity conflict, canonical decoding failure, or normal
    /// data registration failure.
    pub fn add_data_from_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_content: &[u8],
        options: ArtifactDataLoadOptions,
    ) -> anyhow::Result<DatasetBinding> {
        let source = ArtifactRef::new(artifact_id, ContentDigest::digest_bytes(artifact_content));
        anyhow::ensure!(
            !self
                .measured_run_inputs
                .data
                .iter()
                .any(|existing| existing.binding.source().id() == source.id()),
            "Dataset artifact '{}' was already measured for this run",
            source.id()
        );

        let data = decode_backtest_data_artifact(artifact_content)?;
        self.validate_artifact_replay_timestamps(&data)?;
        let materialized_digest = canonical_materialized_data_digest(&data)?;
        let record_count = u64::try_from(data.len())
            .map_err(|error| anyhow::anyhow!("Dataset record count overflow: {error}"))?;
        let replay_ordinal = u64::try_from(self.data_stream_counter)
            .map_err(|error| anyhow::anyhow!("Dataset stream ordinal overflow: {error}"))?;
        let binding = DatasetBinding::new(
            source.clone(),
            materialized_digest,
            ProcessIdentity::new(BACKTEST_DATA_ARTIFACT_DECODER_ID)?,
            replay_ordinal,
            record_count,
        )?;
        if let Some(declared) = self
            .run_provenance
            .data
            .iter()
            .find(|existing| existing.source().id() == source.id())
        {
            anyhow::ensure!(
                declared == &binding,
                "Measured dataset artifact '{}' differs from the declared provenance",
                source.id()
            );
        }
        anyhow::ensure!(
            self.run_id.is_none() || self.run_provenance.data.contains(&binding),
            "Streaming dataset artifact '{}' must be declared before the run starts",
            source.id()
        );

        let stream_origin = self.add_data_inner(data, options.client_id, true, true, true)?;
        self.measured_run_inputs.data.push(MeasuredDataset {
            binding: binding.clone(),
            stream_origin: Some(stream_origin),
            consumed_records: 0,
        });
        if !self.run_provenance.data.contains(&binding) {
            self.run_provenance.data.push(binding.clone());
        }
        Ok(binding)
    }

    /// Adds a native strategy while binding the executable snapshot pinned at engine startup and
    /// applying exact canonical configuration bytes through the host-owned decoder contract.
    ///
    /// # Errors
    ///
    /// Returns an error when artifact identities conflict, configuration decoding fails, or normal
    /// strategy registration fails.
    pub fn add_strategy_from_artifact<T>(
        &mut self,
        strategy_id: StrategyId,
        strategy_artifact_id: ArtifactId,
        config_artifact_id: ArtifactId,
        config_artifact_content: &[u8],
    ) -> anyhow::Result<StrategyBinding>
    where
        T: ArtifactConfiguredStrategy
            + Strategy
            + StrategyNative
            + DataActorNative
            + Component
            + Debug
            + 'static,
    {
        anyhow::ensure!(
            self.measured_run_inputs.strategy.is_none(),
            "A strategy artifact was already measured for this run"
        );
        anyhow::ensure!(
            self.measured_run_inputs.config.is_none(),
            "A configuration artifact was already measured for this run"
        );
        let strategy_artifact =
            ArtifactRef::new(strategy_artifact_id, self.host_executable_startup_digest);
        let config_artifact = ArtifactRef::new(
            config_artifact_id,
            ContentDigest::digest_bytes(config_artifact_content),
        );
        let strategy_binding = StrategyBinding::new(
            strategy_artifact,
            std::any::type_name::<T>(),
            strategy_id.to_string(),
        )?;
        if let Some(declared) = &self.run_provenance.strategy {
            anyhow::ensure!(
                declared == &strategy_binding,
                "Measured strategy artifact differs from the declared provenance"
            );
        }
        if let Some(declared) = &self.run_provenance.config {
            anyhow::ensure!(
                declared == &config_artifact,
                "Measured configuration artifact differs from the declared provenance"
            );
        }

        let registration = (|| -> anyhow::Result<()> {
            let mut instance =
                ArtifactConfiguredStrategyInstance::<T>::decode(config_artifact_content)?;
            let decoded_strategy_id = self
                .kernel
                .trader
                .borrow()
                .prepare_artifact_configured_strategy(&mut instance)?;
            anyhow::ensure!(
                decoded_strategy_id == strategy_id,
                "Decoded strategy identity '{decoded_strategy_id}' does not match preflight target '{strategy_id}'"
            );
            let oms_type = StrategyNative::strategy_core(instance.strategy())
                .config
                .oms_type;
            self.kernel
                .trader
                .borrow_mut()
                .add_artifact_configured_strategy(instance)?;
            if let Some(oms_type) = oms_type {
                self.kernel
                    .exec_engine
                    .borrow_mut()
                    .register_oms_type(strategy_id, oms_type);
            }
            Ok(())
        })();
        if let Err(error) = registration {
            // Custom deserialization, constructors, and registration hooks execute in-process. A
            // failed attempt may already have changed process state, so this engine can no longer
            // issue verified evidence even if a later attempt succeeds.
            self.strategy_artifact_attempt_tainted = true;
            return Err(error);
        }
        self.run_provenance.strategy = Some(strategy_binding.clone());
        self.run_provenance.config = Some(config_artifact.clone());
        self.measured_run_inputs.strategy = Some(strategy_binding.clone());
        self.measured_run_inputs.config = Some(config_artifact);
        Ok(strategy_binding)
    }

    /// Enables fail-closed provenance validation for this engine and all subsequent runs.
    ///
    /// A secured run requires at least one dataset plus exact strategy and configuration
    /// artifacts. Evaluation and promotion artifacts cannot be claimed before the run executes.
    ///
    /// # Errors
    ///
    /// Returns an error once backtest or strategy execution has started. Provenance enforcement
    /// is a pre-execution trust decision and cannot be retrofitted to an observed run.
    pub fn require_run_provenance(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.iteration == 0
                && self.run_id.is_none()
                && self.run_started.is_none()
                && self.run_finished.is_none()
                && !self.kernel.trader.borrow().is_running(),
            "Cannot require run provenance after backtest or strategy execution has started"
        );
        self.run_provenance_required = true;
        Ok(())
    }

    fn validate_run_provenance(&self) -> anyhow::Result<()> {
        if !self.run_provenance_required {
            return Ok(());
        }

        anyhow::ensure!(
            !self.run_provenance.data.is_empty(),
            "Backtest provenance requires at least one dataset artifact"
        );
        anyhow::ensure!(
            self.run_provenance.environment.is_some(),
            "Backtest provenance requires an execution-environment artifact"
        );
        anyhow::ensure!(
            self.run_provenance.strategy.is_some(),
            "Backtest provenance requires a strategy artifact"
        );
        anyhow::ensure!(
            self.run_provenance.config.is_some(),
            "Backtest provenance requires a configuration artifact"
        );
        anyhow::ensure!(
            self.run_provenance.evaluation.is_none(),
            "Backtest provenance cannot claim an evaluation artifact before execution"
        );
        anyhow::ensure!(
            self.run_provenance.promotion.is_none(),
            "Backtest provenance cannot claim a promotion artifact before execution"
        );
        anyhow::ensure!(
            self.run_provenance.model.is_none(),
            "Backtest provenance cannot claim an unmeasured model artifact"
        );
        anyhow::ensure!(
            self.run_provenance.policy.is_none(),
            "Backtest provenance cannot claim an unmeasured policy artifact"
        );
        self.ensure_measured_inputs_match(&self.run_provenance, false)?;
        anyhow::ensure!(
            !self.measured_run_inputs.data.is_empty(),
            "Backtest provenance requires artifact-aware dataset loading"
        );
        anyhow::ensure!(
            self.measured_run_inputs.environment.is_some(),
            "Backtest provenance requires artifact-aware execution-environment construction"
        );
        anyhow::ensure!(
            self.measured_run_inputs.strategy.is_some()
                && self.measured_run_inputs.config.is_some(),
            "Backtest provenance requires artifact-aware strategy and configuration loading"
        );
        self.ensure_no_unmeasured_run_inputs()?;
        Ok(())
    }

    fn authenticated_replay_scope(&self) -> anyhow::Result<Option<&ExecutionEnvironmentScope>> {
        if !self.run_provenance_required {
            return Ok(None);
        }
        let environment = self.run_provenance.environment.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Authenticated replay requires an execution-environment artifact")
        })?;
        let scope = environment.scope().ok_or_else(|| {
            anyhow::anyhow!("Authenticated replay requires an exact scoped execution environment")
        })?;
        Ok(Some(scope))
    }

    fn resolve_run_bounds(
        &self,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
    ) -> anyhow::Result<(UnixNanos, UnixNanos)> {
        if let Some(scope) = self.authenticated_replay_scope()? {
            let scoped_start = UnixNanos::from(scope.replay_start_ns());
            let scoped_end = UnixNanos::from(scope.replay_end_ns());
            if let Some(start) = start {
                anyhow::ensure!(
                    start == scoped_start,
                    "Authenticated replay start {start} does not match environment scope start {scoped_start}"
                );
            }
            if let Some(end) = end {
                anyhow::ensure!(
                    end == scoped_end,
                    "Authenticated replay end {end} does not match environment scope end {scoped_end}"
                );
            }
            return Ok((scoped_start, scoped_end));
        }

        let start = start.unwrap_or_else(|| self.ts_first.unwrap_or_default());
        let end = end.unwrap_or_else(|| {
            self.ts_last_data
                .unwrap_or(UnixNanos::from(4_102_444_800_000_000u64))
        });
        anyhow::ensure!(start <= end, "start was > end");
        Ok((start, end))
    }

    fn validate_artifact_replay_timestamps(&self, data: &[Data]) -> anyhow::Result<()> {
        let Some(scope) = self
            .measured_run_inputs
            .environment
            .as_ref()
            .and_then(ExecutionEnvironmentBinding::scope)
        else {
            return Ok(());
        };
        for (index, item) in data.iter().enumerate() {
            let ts_init = item.ts_init().as_u64();
            anyhow::ensure!(
                ts_init >= scope.replay_start_ns() && ts_init < scope.replay_end_ns(),
                "Backtest data artifact record {index} timestamp {ts_init} is outside environment replay scope [{}, {})",
                scope.replay_start_ns(),
                scope.replay_end_ns()
            );
        }
        Ok(())
    }

    fn timer_flush_boundary(&self, end_ns: UnixNanos) -> UnixNanos {
        if self.run_provenance_required {
            UnixNanos::from(end_ns.as_u64() - 1)
        } else {
            end_ns
        }
    }

    fn ensure_measured_inputs_match(
        &self,
        provenance: &RunProvenance,
        require_all_data: bool,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.measured_run_inputs
                .data
                .iter()
                .all(|dataset| provenance.data.contains(&dataset.binding)),
            "Measured dataset artifacts differ from the declared run provenance"
        );
        if require_all_data {
            let mut measured: Vec<_> = self
                .measured_run_inputs
                .data
                .iter()
                .map(|dataset| dataset.binding.clone())
                .collect();
            measured.sort_unstable();
            measured.dedup();
            let mut declared = provenance.data.clone();
            declared.sort_unstable();
            declared.dedup();
            anyhow::ensure!(
                measured == declared,
                "Not every declared dataset artifact was measured by the backtest engine"
            );
        }
        if let Some(measured) = &self.measured_run_inputs.strategy {
            anyhow::ensure!(
                provenance.strategy.as_ref() == Some(measured),
                "Measured strategy artifact differs from the declared run provenance"
            );
        }
        if let Some(measured) = &self.measured_run_inputs.config {
            anyhow::ensure!(
                provenance.config.as_ref() == Some(measured),
                "Measured configuration artifact differs from the declared run provenance"
            );
        }
        if let Some(measured) = &self.measured_run_inputs.environment {
            anyhow::ensure!(
                provenance.environment.as_ref() == Some(measured),
                "Measured execution environment differs from the declared run provenance"
            );
        }
        if require_all_data {
            anyhow::ensure!(
                self.measured_run_inputs.environment.as_ref() == provenance.environment.as_ref(),
                "Execution-environment artifact was not measured by the backtest engine"
            );
            anyhow::ensure!(
                self.measured_run_inputs.strategy.as_ref() == provenance.strategy.as_ref(),
                "Strategy artifact was not measured by the backtest engine"
            );
            anyhow::ensure!(
                self.measured_run_inputs.config.as_ref() == provenance.config.as_ref(),
                "Configuration artifact was not measured by the backtest engine"
            );
        }
        Ok(())
    }

    fn verify_measured_run_inputs(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.run_provenance_required,
            "Verified run inputs require provenance enforcement to be locked before execution"
        );
        let scope = self.authenticated_replay_scope()?.ok_or_else(|| {
            anyhow::anyhow!("Verified run inputs require an authenticated replay scope")
        })?;
        anyhow::ensure!(
            self.backtest_start == Some(UnixNanos::from(scope.replay_start_ns())),
            "Verified run start {:?} does not exactly match authenticated environment scope start {}",
            self.backtest_start,
            scope.replay_start_ns()
        );
        anyhow::ensure!(
            self.backtest_end == Some(UnixNanos::from(scope.replay_end_ns())),
            "Verified run end {:?} does not exactly match authenticated environment scope end {}",
            self.backtest_end,
            scope.replay_end_ns()
        );
        anyhow::ensure!(
            !self.run_provenance.data.is_empty()
                && self.run_provenance.environment.is_some()
                && self.run_provenance.strategy.is_some()
                && self.run_provenance.config.is_some(),
            "Verified run inputs require data, execution environment, strategy, and configuration artifacts"
        );
        anyhow::ensure!(
            self.run_provenance.evaluation.is_none() && self.run_provenance.promotion.is_none(),
            "Verified run inputs cannot contain preclaimed evaluation or promotion artifacts"
        );
        anyhow::ensure!(
            self.run_provenance.model.is_none() && self.run_provenance.policy.is_none(),
            "Verified run inputs cannot contain unmeasured model or policy artifacts"
        );
        self.ensure_no_unmeasured_run_inputs()?;
        anyhow::ensure!(
            !self.incomplete_artifact_data_evicted,
            "Verified run inputs cannot include an artifact batch evicted before full consumption"
        );
        anyhow::ensure!(
            self.measured_run_inputs
                .data
                .iter()
                .all(|dataset| { dataset.consumed_records == dataset.binding.record_count() }),
            "Every measured dataset record must be processed before the run can be verified"
        );
        self.ensure_measured_inputs_match(&self.run_provenance, true)
    }

    fn ensure_no_unmeasured_run_inputs(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.unmeasured_data_loaded,
            "Verified run inputs cannot include data loaded outside the artifact-aware path"
        );
        anyhow::ensure!(
            !self.unmeasured_strategy_loaded,
            "Verified run inputs cannot include strategies loaded outside the artifact-aware path"
        );
        anyhow::ensure!(
            !self.strategy_artifact_attempt_tainted,
            "Verified run inputs cannot follow a failed in-process strategy artifact attempt"
        );
        anyhow::ensure!(
            !self.unmeasured_actor_loaded,
            "Verified run inputs cannot include actors without measured artifacts"
        );
        anyhow::ensure!(
            !self.unmeasured_exec_algorithm_loaded,
            "Verified run inputs cannot include execution algorithms without measured artifacts"
        );
        anyhow::ensure!(
            !self.unmeasured_environment_mutation,
            "Verified run inputs cannot include execution-environment changes outside the artifact-aware constructor"
        );
        anyhow::ensure!(
            !self.secure_boundary_escaped.get(),
            "Verified run inputs cannot follow direct access to the mutable system kernel boundary"
        );
        Ok(())
    }

    fn mark_measured_data_processed(&mut self, stream_origin: i32) -> anyhow::Result<()> {
        let Some(dataset) = self
            .measured_run_inputs
            .data
            .iter_mut()
            .find(|dataset| dataset.stream_origin == Some(stream_origin))
        else {
            return Ok(());
        };
        dataset.consumed_records = dataset
            .consumed_records
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Measured dataset consumption count overflow"))?;
        anyhow::ensure!(
            dataset.consumed_records <= dataset.binding.record_count(),
            "Measured dataset '{}' consumed more records than were materialized",
            dataset.binding.source().id()
        );
        Ok(())
    }

    /// Returns when the last run started, if any.
    #[must_use]
    pub const fn run_started(&self) -> Option<UnixNanos> {
        self.run_started
    }

    /// Returns when the last run finished, if any.
    #[must_use]
    pub const fn run_finished(&self) -> Option<UnixNanos> {
        self.run_finished
    }

    /// Returns the last backtest range start, if any.
    #[must_use]
    pub const fn backtest_start(&self) -> Option<UnixNanos> {
        self.backtest_start
    }

    /// Returns the last backtest range end, if any.
    #[must_use]
    pub const fn backtest_end(&self) -> Option<UnixNanos> {
        self.backtest_end
    }

    /// Returns the list of registered venue identifiers.
    #[must_use]
    pub fn list_venues(&self) -> Vec<Venue> {
        self.venues.keys().copied().collect()
    }

    /// # Errors
    ///
    /// Returns an error if the venue is already registered, initializing the simulated exchange
    /// fails, or registering its execution client fails.
    pub fn add_venue(&mut self, config: SimulatedVenueConfig) -> anyhow::Result<()> {
        // `routing` and `frozen_account` flow to the exec client, so capture
        // them before the config is consumed by the exchange constructor.
        let venue = config.venue;
        if self.venues.contains_key(&venue) {
            anyhow::bail!("Venue {venue} is already registered");
        }

        let routing = Some(config.routing);
        let frozen_account = Some(config.frozen_account);

        let exchange =
            SimulatedExchange::new(config, self.kernel.cache.clone(), self.kernel.clock.clone())?;
        let exchange = Rc::new(RefCell::new(exchange));

        let account_id = AccountId::from(format!("{venue}-001").as_str());

        let exec_client = BacktestExecutionClient::new(
            self.config.trader_id(),
            account_id,
            &exchange,
            self.kernel.cache.clone(),
            self.kernel.clock.clone(),
            routing,
            frozen_account,
        );

        exchange
            .borrow_mut()
            .register_client(Rc::new(exec_client.clone()));

        self.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(exec_client.clone()))?;

        SimulatedExchange::register_spread_quote_endpoint(&exchange);
        self.venues.insert(venue, exchange);
        self.exec_clients.push(exec_client);
        self.unmeasured_environment_mutation = true;

        log::info!("Adding exchange {venue} to engine");

        Ok(())
    }

    /// Sets the settlement price for the specified venue instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the venue has not been added to the engine.
    pub fn set_settlement_price(
        &mut self,
        venue: Venue,
        instrument_id: InstrumentId,
        price: Price,
    ) -> anyhow::Result<()> {
        let exchange = self
            .venues
            .get_mut(&venue)
            .ok_or_else(|| anyhow::anyhow!("Unknown venue {venue}"))?;
        exchange
            .borrow_mut()
            .set_settlement_price(instrument_id, price);
        self.unmeasured_environment_mutation = true;
        Ok(())
    }

    /// Changes the fill model for the specified venue.
    pub fn change_fill_model(&mut self, venue: Venue, fill_model: FillModelHandle) {
        if let Some(exchange) = self.venues.get_mut(&venue) {
            exchange.borrow_mut().set_fill_model(fill_model);
            self.unmeasured_environment_mutation = true;
        } else {
            log::warn!(
                "BacktestEngine::change_fill_model called for unknown venue {venue}, ignoring"
            );
        }
    }

    /// Adds an instrument to the backtest engine for the specified venue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The instrument's associated venue has not been added via `add_venue`.
    /// - Attempting to add a `CurrencyPair` instrument for a single-currency CASH account.
    ///
    pub fn add_instrument(&mut self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        let instrument_id = instrument.id();
        if let Some(exchange) = self.venues.get(&instrument.id().venue) {
            let previous_expiration_ns = exchange.borrow().instrument_expiration(instrument_id);

            if matches!(
                instrument,
                InstrumentAny::CurrencyPair(_) | InstrumentAny::TokenizedAsset(_)
            ) && exchange.borrow().account_type != AccountType::Margin
                && exchange.borrow().base_currency.is_some()
            {
                anyhow::bail!(
                    "Cannot add a multi-currency spot instrument {instrument_id} for a venue with a single-currency CASH account"
                )
            }
            exchange.borrow_mut().add_instrument(instrument.clone())?;
            if let Some(expiration_ns) = instrument.expiration_ns() {
                self.set_instrument_expiration_timer(exchange, instrument_id, expiration_ns)?;
            }

            if let Some(previous_expiration_ns) = previous_expiration_ns
                && instrument.expiration_ns() != Some(previous_expiration_ns)
                && !exchange
                    .borrow()
                    .has_unprocessed_instrument_expiration(previous_expiration_ns)
            {
                let timer_name = Self::instrument_expiration_timer_name(
                    instrument_id.venue,
                    previous_expiration_ns,
                );
                self.kernel.clock.borrow_mut().cancel_timer(&timer_name);
            }
        } else {
            anyhow::bail!(
                "Cannot add an `Instrument` object without first adding its associated venue {}",
                instrument.id().venue
            )
        }

        self.add_market_data_client_if_not_exists_inner(instrument.id().venue);

        self.kernel
            .data_engine
            .borrow_mut()
            .process(instrument as &dyn Any);
        log::info!(
            "Added instrument {} to exchange {}",
            instrument_id,
            instrument_id.venue
        );
        self.unmeasured_environment_mutation = true;
        Ok(())
    }

    /// Adds data to the engine for replay during the backtest run.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `data` is empty.
    /// - `validate` is `true`, the first element is built-in market data (excluding
    ///   custom and DeFi data), and its instrument has not been added to the cache via
    ///   [`add_instrument`](Self::add_instrument).
    /// - `validate` is `true` and the first element is a [`Data::Bar`] whose
    ///   `aggregation_source` is not [`AggregationSource::External`].
    pub fn add_data(
        &mut self,
        data: Vec<Data>,
        client_id: Option<ClientId>,
        validate: bool,
        sort: bool,
    ) -> anyhow::Result<()> {
        self.add_data_inner(data, client_id, validate, sort, false)
            .map(|_| ())
    }

    fn add_data_inner(
        &mut self,
        data: Vec<Data>,
        client_id: Option<ClientId>,
        validate: bool,
        sort: bool,
        artifact_aware: bool,
    ) -> anyhow::Result<i32> {
        #[cfg(not(feature = "defi"))]
        let _ = client_id;

        anyhow::ensure!(!data.is_empty(), "data was empty");

        let count = data.len();
        let mut to_add = data;

        if artifact_aware {
            for (index, item) in to_add.iter().enumerate() {
                #[cfg(feature = "defi")]
                let is_defi = matches!(item, Data::Defi(_));
                #[cfg(not(feature = "defi"))]
                let is_defi = false;

                if is_defi || matches!(item, Data::Custom(_)) {
                    continue;
                }
                let instrument_id = item.instrument_id();
                anyhow::ensure!(
                    self.kernel
                        .cache
                        .borrow()
                        .instrument(&instrument_id)
                        .is_some(),
                    "Instrument {instrument_id} for artifact record {index} not found in the cache. \
                     Add the instrument through `add_instrument()` prior to adding related data."
                );
                if let Data::Bar(bar) = item {
                    anyhow::ensure!(
                        bar.bar_type.aggregation_source() == AggregationSource::External,
                        "Artifact record {index} bar_type.aggregation_source must be External, was {:?}",
                        bar.bar_type.aggregation_source(),
                    );
                }
            }
        }

        if sort {
            to_add.sort_by_key(HasTsInit::ts_init);
        }

        if validate && !artifact_aware {
            // Mirror Cython: validate against the first element only and assume the
            // batch is homogeneous (documented contract on add_data).
            let first = &to_add[0];
            #[cfg(feature = "defi")]
            let first_is_defi = matches!(first, Data::Defi(_));
            #[cfg(not(feature = "defi"))]
            let first_is_defi = false;

            if !first_is_defi && !matches!(first, Data::Custom(_)) {
                let first_instrument_id = first.instrument_id();
                anyhow::ensure!(
                    self.kernel
                        .cache
                        .borrow()
                        .instrument(&first_instrument_id)
                        .is_some(),
                    "Instrument {first_instrument_id} for the given data not found in the cache. \
                     Add the instrument through `add_instrument()` prior to adding related data."
                );

                if let Data::Bar(bar) = first {
                    anyhow::ensure!(
                        bar.bar_type.aggregation_source() == AggregationSource::External,
                        "bar_type.aggregation_source must be External, was {:?}",
                        bar.bar_type.aggregation_source(),
                    );
                }
            }
        }

        // Track has_data / has_book_data unconditionally so the depth-vs-data
        // run-time check still fires for callers that pass validate=false
        // (e.g. node.rs run_oneshot loading from a catalog). Time bounds are
        // also tracked here so start/end defaults are correct even when the
        // batch was added with sort=false.
        let mut batch_min_ts: Option<UnixNanos> = None;
        let mut batch_max_ts: Option<UnixNanos> = None;

        #[cfg(feature = "defi")]
        if to_add.iter().any(|item| matches!(item, Data::Defi(_))) {
            self.add_defi_data_client_if_not_exists(client_id);
        }

        for item in &to_add {
            let ts = item.ts_init();
            batch_min_ts = Some(batch_min_ts.map_or(ts, |cur| cur.min(ts)));
            batch_max_ts = Some(batch_max_ts.map_or(ts, |cur| cur.max(ts)));

            #[cfg(feature = "defi")]
            if matches!(item, Data::Defi(_)) {
                continue;
            }

            if matches!(item, Data::Custom(_)) {
                // Custom data routes by DataType and is independent of market venue bookkeeping.
                continue;
            }

            let instr_id = item.instrument_id();
            self.has_data.insert(instr_id);

            if item.is_order_book_data() {
                self.has_book_data.insert(instr_id);
            }

            self.add_market_data_client_if_not_exists_inner(instr_id.venue);
        }

        if let Some(ts) = batch_min_ts
            && self.ts_first.is_none_or(|t| ts < t)
        {
            self.ts_first = Some(ts);
        }

        if let Some(ts) = batch_max_ts
            && self.ts_last_data.is_none_or(|t| ts > t)
        {
            self.ts_last_data = Some(ts);
        }

        self.data_len += count;
        let stream_name = format!("backtest_data_{}", self.data_stream_counter);
        self.data_stream_counter += 1;
        let stream_origin = self.data_iterator.add_data(&stream_name, to_add, true);

        self.sorted = sort;

        log::info!(
            "Added {count} data element{} to BacktestEngine ({} total)",
            if count == 1 { "" } else { "s" },
            self.data_len,
        );

        if !artifact_aware {
            self.unmeasured_data_loaded = true;
        }

        Ok(stream_origin)
    }

    /// Adds an actor to the backtest engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is already registered or the trader is in an invalid
    /// state for actor registration.
    pub fn add_actor<T>(&mut self, actor: T) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        self.kernel.trader.borrow_mut().add_actor(actor)?;
        self.unmeasured_actor_loaded = true;
        Ok(())
    }

    /// Adds the given actors to the backtest engine. Stops at the first error.
    ///
    /// # Errors
    ///
    /// Returns an error if any actor fails to register; preceding actors remain registered.
    pub fn add_actors<T>(&mut self, actors: Vec<T>) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        for actor in actors {
            self.add_actor(actor)?;
        }
        Ok(())
    }

    /// Adds a strategy to the backtest engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is already registered or the trader is in an invalid
    /// state for strategy registration.
    pub fn add_strategy<T>(&mut self, strategy: T) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        self.add_strategy_inner(strategy)
    }

    fn add_strategy_inner<T>(&mut self, mut strategy: T) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let strategy_id = self
            .kernel
            .trader
            .borrow()
            .prepare_strategy_for_registration(&mut strategy)?;
        let oms_type = StrategyNative::strategy_core(&strategy).config.oms_type;

        self.kernel.trader.borrow_mut().add_strategy(strategy)?;

        if let Some(oms_type) = oms_type {
            self.kernel
                .exec_engine
                .borrow_mut()
                .register_oms_type(strategy_id, oms_type);
        }

        self.unmeasured_strategy_loaded = true;

        Ok(())
    }

    /// Adds the given strategies to the backtest engine. Stops at the first error.
    ///
    /// # Errors
    ///
    /// Returns an error if any strategy fails to register; preceding strategies remain registered.
    pub fn add_strategies<T>(&mut self, strategies: Vec<T>) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        for strategy in strategies {
            self.add_strategy(strategy)?;
        }
        Ok(())
    }

    /// Adds an execution algorithm to the backtest engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the algorithm is already registered or the trader is running.
    pub fn add_exec_algorithm<T>(&mut self, exec_algorithm: T) -> anyhow::Result<()>
    where
        T: ExecutionAlgorithm + ExecutionAlgorithmNative + Component + Debug + 'static,
    {
        self.kernel
            .trader
            .borrow_mut()
            .add_exec_algorithm(exec_algorithm)?;
        self.unmeasured_exec_algorithm_loaded = true;
        Ok(())
    }

    /// Adds the given execution algorithms to the backtest engine. Stops at the first error.
    ///
    /// # Errors
    ///
    /// Returns an error if any execution algorithm fails to register; preceding algorithms remain
    /// registered.
    pub fn add_exec_algorithms<T>(&mut self, exec_algorithms: Vec<T>) -> anyhow::Result<()>
    where
        T: ExecutionAlgorithm + ExecutionAlgorithmNative + Component + Debug + 'static,
    {
        for exec_algorithm in exec_algorithms {
            self.add_exec_algorithm(exec_algorithm)?;
        }
        Ok(())
    }

    /// Run a backtest.
    ///
    /// Processes all data chronologically. When `streaming` is false (default),
    /// finalizes the run via [`end`](Self::end). When `streaming` is true, the
    /// run pauses without finalizing so additional data batches can be loaded.
    /// Timer advancement stops at data exhaustion to avoid producing synthetic
    /// events (e.g. zero-volume bars) past the current batch.
    ///
    /// Each streaming batch must include every data item with its final `ts_init`;
    /// splitting one replay timestamp across calls can finalize timers and venue
    /// modules before later items at that timestamp. [`BacktestNode`](crate::node::BacktestNode)
    /// aligns its chunks to this boundary.
    ///
    /// Streaming workflow:
    /// 1. Add initial data and strategies
    /// 2. Loop: call `run(streaming=true)`, `clear_data()`, `add_data(next_batch)`
    /// 3. After all batches: call `end()` to finalize
    ///
    /// # Errors
    ///
    /// Returns an error if the backtest encounters an unrecoverable state.
    pub fn run(
        &mut self,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        run_config_id: Option<String>,
        streaming: bool,
    ) -> anyhow::Result<()> {
        if let Some(error) = &self.funding_error {
            anyhow::bail!("{error}");
        }

        if let Err(e) = self.run_impl(start, end, run_config_id, streaming) {
            if self.run_id.is_some() {
                self.run_failure = Some(format!("{e:#}"));
            }
            if self.funding_error.is_some() || self.kernel.is_event_store_halted() {
                self.abort_run();
            }
            return Err(e);
        }

        // Finalize on non-streaming runs, or when a shutdown was triggered
        // at any point during the run (including the trailing settle, module,
        // and flush callbacks that execute after the main data loop) so the
        // trader and engines actually stop.
        if !streaming || self.force_stop || self.kernel.is_shutdown_requested() {
            self.end_with_result()?;
        }

        Ok(())
    }

    fn run_impl(
        &mut self,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        run_config_id: Option<String>,
        streaming: bool,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.run_finished.is_none(),
            "Backtest run is already finalized; call reset before starting another run"
        );
        if self.iteration == 0 {
            self.validate_run_provenance()?;
        }
        anyhow::ensure!(
            self.sorted,
            "Data has been added but not sorted, call `engine.sort_data()` or use \
             `engine.add_data(..., sort=true)` before running"
        );

        for exchange in self.venues.values() {
            let exchange = exchange.borrow();
            let book_type_has_depth = exchange.book_type() as u8 > BookType::L1_MBP as u8;
            if !book_type_has_depth {
                continue;
            }

            for instrument_id in exchange.instrument_ids() {
                let has_data = self.has_data.contains(instrument_id);
                let missing_book_data = !self.has_book_data.contains(instrument_id)
                    && !self.has_book_processed.contains(instrument_id);

                if has_data && missing_book_data {
                    anyhow::bail!(
                        "No order book data found for instrument '{instrument_id}' when `book_type` \
                         is '{:?}'. Set the venue `book_type` to 'L1_MBP' (for top-of-book data \
                         like quotes, trades, and bars) or provide order book data for this \
                         instrument.",
                        exchange.book_type()
                    );
                }
            }
        }

        // Authenticated environments own an exact half-open replay window. Callers may repeat
        // those bounds explicitly, but cannot narrow, widen, or shift certified execution.
        let (start_ns, end_ns) = self.resolve_run_bounds(start, end)?;
        self.end_ns = end_ns;
        self.last_ns = start_ns;
        self.last_module_ns = None;

        // Set all component clocks to start
        let clocks = self.collect_all_clocks();
        Self::set_all_clocks_time(&clocks, start_ns);

        // First-iteration initialization
        if self.iteration == 0 {
            self.set_instrument_expiration_timers()?;

            self.run_config_id = run_config_id;
            self.run_id = Some(UUID4::new());
            self.run_succeeded = false;
            self.run_inputs_verified = false;
            self.event_store_receipt = None;
            self.completed_result = None;
            self.run_failure = None;
            self.run_started = Some(UnixNanos::from(nanos_since_unix_epoch()));
            self.backtest_start = Some(start_ns);

            for exchange in self.venues.values() {
                let mut ex = exchange.borrow_mut();
                ex.initialize_account();
                ex.load_open_orders();
            }

            // Re-set clocks after account init
            Self::set_all_clocks_time(&clocks, start_ns);

            // Reset force stop flag
            self.force_stop = false;
            self.kernel.reset_shutdown_flag();

            // Initialize sync command senders (once per thread)
            Self::init_command_senders();

            // Set logging to static clock mode for deterministic timestamps
            logging_clock_set_static_mode();
            logging_clock_set_static_time(start_ns.as_u64());

            // Bind the exact engine-measured provenance into the immutable event-store
            // RunStarted header before the kernel opens the run.
            self.kernel
                .set_event_store_run_provenance(self.run_provenance.clone())?;

            // Start kernel, then stop before trader startup for event-store replay
            self.kernel.start();
            anyhow::ensure!(
                !self.kernel.is_shutdown_requested(),
                "kernel startup requested terminal shutdown; event-store startup may have failed"
            );
            if self.kernel.is_event_store_replay() {
                self.log_pre_run();
                return Ok(());
            }

            if self.kernel.is_event_store_replay_configured() {
                anyhow::bail!("event-store replay did not start");
            }
            self.kernel.start_trader()?;

            // Drain on_start data subscriptions so aggregators subscribe before the first data
            // point, else internal aggregation drops the first tick. Trading/exec stay queued
            while !data_cmd_queue_is_empty() {
                drain_data_cmd_queue();
            }

            self.log_pre_run();
        }

        self.log_run();

        // Skip data before start_ns
        let mut data = self.data_iterator.next_item_with_origin();
        while let Some((_, ref d)) = data {
            if d.ts_init() >= start_ns {
                break;
            }
            data = self.data_iterator.next_item_with_origin();
        }

        // Initialize last_ns before first data point
        if let Some((_, ref d)) = data {
            let ts = d.ts_init();
            self.last_ns = if ts.as_u64() > 0 {
                UnixNanos::from(ts.as_u64() - 1)
            } else {
                UnixNanos::default()
            };
        } else {
            self.last_ns = start_ns;
        }

        loop {
            anyhow::ensure!(
                !self.kernel.is_event_store_halted(),
                "event store fail-stopped during backtest; terminating the run"
            );

            if self.kernel.is_shutdown_requested() {
                log::info!("Shutdown requested via ShutdownSystem, ending backtest");
                self.force_stop = true;
            }

            if self.force_stop {
                log::info!("Force stop triggered, ending backtest");
                break;
            }

            if data.is_none() {
                if streaming {
                    // In streaming mode, don't advance timers past the
                    // current batch. The next batch will provide more data
                    // and timers will fire naturally as time advances.
                    break;
                }
                let done = self.process_next_timer(&clocks)?;
                data = self.data_iterator.next_item_with_origin();
                if data.is_none() && done {
                    break;
                }
                continue;
            }

            let (stream_origin, d) = data.as_ref().unwrap();
            let ts_init = d.ts_init();

            if (self.run_provenance_required && ts_init >= end_ns)
                || (!self.run_provenance_required && ts_init > end_ns)
            {
                break;
            }

            if ts_init > self.last_ns {
                self.last_ns = ts_init;
                self.advance_time_impl(ts_init, &clocks)?;
            }

            // A timer fired during clock advance may have requested shutdown,
            // skip delivering this data point in that case
            if self.kernel.is_shutdown_requested() {
                self.force_stop = true;
                break;
            }

            self.route_data_to_exchange(d);
            self.kernel.data_engine.borrow_mut().process_data(d.clone());

            // Drain deferred commands, then process exchange queues
            self.drain_command_queues();
            self.settle_venues(ts_init);
            self.mark_measured_data_processed(*stream_origin)?;

            let prev_last_ns = self.last_ns;
            data = self.data_iterator.next_item_with_origin();

            // If timestamp changed (or exhausted), flush timers then run modules
            if data.is_none() || data.as_ref().unwrap().1.ts_init() > prev_last_ns {
                self.flush_accumulator_events(&clocks, prev_last_ns)?;
                self.finalize_timestamp(&clocks, prev_last_ns)?;
            }

            self.iteration += 1;
        }

        // Process remaining exchange messages
        let ts_now = self.kernel.clock.borrow().timestamp_ns();
        self.finalize_timestamp(&clocks, ts_now)?;

        // Cap at last_ns when streaming or after shutdown to avoid firing
        // timers past the current batch or the graceful stop
        let flush_ts = if streaming || self.force_stop || self.kernel.is_shutdown_requested() {
            self.last_ns
        } else {
            self.timer_flush_boundary(end_ns)
        };
        self.flush_accumulator_events(&clocks, flush_ts)?;
        anyhow::ensure!(
            !self.kernel.is_event_store_halted(),
            "event store fail-stopped during backtest finalization"
        );

        Ok(())
    }

    fn abort_run(&mut self) {
        self.run_succeeded = false;
        self.run_inputs_verified = false;
        self.event_store_receipt = None;
        self.force_stop = true;
        self.accumulator.clear();
        self.kernel.stop_trader();
        self.kernel.data_engine.borrow_mut().stop();
        self.kernel.risk_engine.borrow_mut().stop();
        self.kernel.exec_engine.borrow_mut().stop();
        self.run_finished = Some(UnixNanos::from(nanos_since_unix_epoch()));
        self.backtest_end = Some(self.kernel.clock.borrow().timestamp_ns());
        logging_clock_set_realtime_mode();
    }

    /// Manually end the backtest.
    pub fn end(&mut self) {
        if let Err(e) = self.end_with_result() {
            log::error!("Error ending backtest: {e}");
        }
    }

    /// Ends the backtest and reports lifecycle persistence failures.
    ///
    /// # Errors
    ///
    /// Returns an error if actor or strategy state cannot be saved.
    pub(crate) fn end_with_result(&mut self) -> anyhow::Result<()> {
        if let Some(error) = &self.funding_error {
            anyhow::bail!("{error}");
        }
        if self.run_id.is_some() {
            anyhow::ensure!(
                self.run_finished.is_none(),
                "Backtest run is already finalized; call reset before ending another run"
            );
        }

        // Flush remaining timer events to the backtest end boundary so that
        // tail alerts/expiries scheduled after the last data point still fire.
        // Must run before stopping engines since DataEngine::stop() cancels
        // bar aggregator timers. When a shutdown was requested, cap the flush
        // at the last processed timestamp so timers scheduled past the stop
        // point do not fire extra callbacks after the graceful stop request.
        if self.end_ns.as_u64() > 0 {
            let clocks = self.collect_all_clocks();
            let flush_ts = if self.force_stop || self.kernel.is_shutdown_requested() {
                self.last_ns
            } else {
                self.timer_flush_boundary(self.end_ns)
            };

            if let Err(e) = self.flush_accumulator_events(&clocks, flush_ts) {
                if self.funding_error.is_some() {
                    self.abort_run();
                }
                return Err(e);
            }
        }

        self.kernel.stop_trader();

        // Settle residual on_stop commands before stopping engines. Venue modules are
        // not re-run; process_modules is once per timestamp.
        let mut ts_now = self.kernel.clock.borrow().timestamp_ns();

        // Drain first so latency-deferred commands reach venue inflight queues
        self.drain_command_queues();

        // Advance the clock to the latest inflight arrival; otherwise commands deferred
        // by a LatencyModel sit past ts_now and never settle.
        if let Some(max_inflight_ts) = self.max_inflight_command_ts()
            && max_inflight_ts > ts_now
        {
            anyhow::ensure!(
                !self.run_provenance_required || max_inflight_ts < self.end_ns,
                "Authenticated replay produced a deferred command at {max_inflight_ts} outside environment scope ending at {}",
                self.end_ns
            );
            ts_now = max_inflight_ts;
            let clocks = self.collect_all_clocks();
            Self::set_all_clocks_time(&clocks, ts_now);
        }

        self.settle_venues(ts_now);

        // A successful authenticated replay owns the exact half-open environment window. Move
        // clocks to its exclusive end only after all in-scope callbacks and residual commands
        // have settled. Interrupted or failed runs retain their actual stop time and therefore
        // cannot satisfy the evidence-boundary check below.
        if self.run_provenance_required
            && !self.force_stop
            && !self.kernel.is_shutdown_requested()
            && self.run_failure.is_none()
        {
            let clocks = self.collect_all_clocks();
            Self::set_all_clocks_time(&clocks, self.end_ns);
        }

        let save_result = self.kernel.save_trader_state();
        self.kernel.portfolio.borrow_mut().finalize_equity_curve();

        // Stop engines
        self.kernel.data_engine.borrow_mut().stop();
        self.kernel.risk_engine.borrow_mut().stop();
        self.kernel.exec_engine.borrow_mut().stop();

        // This is deliberately the final strategy trust-boundary read: `on_stop` and residual
        // command settlement execute user code after trader stop begins, so checking earlier would
        // allow a late in-process configuration mutation to escape the evidence commitment.
        let artifact_config_error = self
            .kernel
            .trader
            .borrow()
            .ensure_artifact_configurations_current()
            .err();

        self.run_finished = Some(UnixNanos::from(nanos_since_unix_epoch()));
        self.backtest_end = Some(self.kernel.clock.borrow().timestamp_ns());
        let measured_inputs_error = self.verify_measured_run_inputs().err();

        // Switch logging back to realtime mode
        logging_clock_set_realtime_mode();

        self.log_post_run();
        self.run_inputs_verified = self.library_owned_run_identity
            && artifact_config_error.is_none()
            && measured_inputs_error.is_none();
        let run_failure = self.run_failure.as_ref().map(|failure| {
            anyhow::anyhow!("Backtest execution failed before finalization: {failure}")
        });
        let completion_validation_error = artifact_config_error.or_else(|| {
            if self.run_provenance_required {
                measured_inputs_error
            } else {
                None
            }
        });
        let completion_result = match (save_result, completion_validation_error, run_failure) {
            (Err(error), _, _) | (Ok(()), Some(error), _) | (Ok(()), None, Some(error)) => {
                Err(error)
            }
            (Ok(()), None, None) => Ok(()),
        };
        let event_store_ts = self.kernel.clock.borrow().timestamp_ns();
        let event_store_result = self.kernel.seal_event_store(event_store_ts);
        let completion_result = match (completion_result, event_store_result) {
            (Ok(()), Ok(receipt)) => {
                self.event_store_receipt = receipt;
                Ok(())
            }
            (Err(run_error), Ok(receipt)) => {
                self.event_store_receipt = receipt;
                Err(run_error)
            }
            (Ok(()), Err(seal_error)) => Err(anyhow::anyhow!(
                "event-store close/verification failed before backtest evidence seal: {seal_error:#}"
            )),
            (Err(run_error), Err(seal_error)) => Err(anyhow::anyhow!(
                "backtest finalization failed: {run_error:#}; event-store close/verification also failed: {seal_error:#}"
            )),
        };
        self.run_succeeded = completion_result.is_ok() && self.run_id.is_some();
        self.completed_result = self
            .run_succeeded
            .then(|| self.build_result().issue_engine_evidence_seal());
        completion_result
    }

    /// Reset the backtest engine.
    ///
    /// All stateful fields are reset to their initial value. Data and instruments
    /// persist across resets to enable repeated runs with different strategies.
    pub fn reset(&mut self) {
        log::debug!("Resetting");

        if self.kernel.trader.borrow().is_running() {
            self.end();
        }

        // Stop and reset engines
        self.kernel.data_engine.borrow_mut().stop();
        self.kernel.data_engine.borrow_mut().reset();

        self.kernel.exec_engine.borrow_mut().stop();

        // Reset exchanges before the exec engine wipes the cache so
        // exchange.reset() can see the prior run's account.
        for exchange in self.venues.values() {
            exchange.borrow_mut().reset();
        }
        self.kernel.exec_engine.borrow_mut().reset();

        self.kernel.risk_engine.borrow_mut().stop();
        self.kernel.risk_engine.borrow_mut().reset();

        self.kernel.order_emulator.reset();

        // Reset trader
        if let Err(e) = self.kernel.trader.borrow_mut().reset() {
            log::error!("Error resetting trader: {e:?}");
        }

        self.kernel.portfolio.borrow_mut().reset();

        // Clear run state
        self.run_config_id = None;
        self.run_id = None;
        self.run_provenance = RunProvenance::default();
        if self.streamed_data_evicted {
            self.measured_run_inputs.data.clear();
            self.unmeasured_data_loaded = self.data_len > 0;
        } else {
            for dataset in &mut self.measured_run_inputs.data {
                dataset.consumed_records = 0;
            }
        }
        self.run_inputs_verified = false;
        self.event_store_receipt = None;
        self.run_started = None;
        self.run_finished = None;
        self.backtest_start = None;
        self.backtest_end = None;
        self.funding_error = None;
        self.run_failure = None;
        self.run_succeeded = false;
        self.completed_result = None;
        self.streamed_data_evicted = false;
        self.incomplete_artifact_data_evicted = false;
        self.iteration = 0;
        self.force_stop = false;
        self.last_ns = UnixNanos::default();
        self.last_module_ns = None;
        self.last_liquidation_ns = None;
        self.end_ns = UnixNanos::default();
        self.has_book_processed.clear();

        self.accumulator.clear();
        self.cancel_funding_settlement_timers();

        // Reset all iterator cursors to beginning (data persists)
        self.data_iterator.reset_all_cursors();

        log::info!("Reset");
    }

    /// Sort the engine's internal data stream by timestamp.
    ///
    /// Useful when data has been added with `sort=false` for batch performance,
    /// then sorted once before running.
    pub fn sort_data(&mut self) {
        // Each add call creates its own stream; the iterator merges streams by
        // replay timestamp across streams. Mark the engine as sorted so `run`
        // no longer rejects it.
        self.sorted = true;
        log::info!("Data sort requested (iterator merges streams by replay timestamp)");
    }

    /// Clear the engine's internal data stream. Does not clear instruments.
    pub fn clear_data(&mut self) {
        let active_stream = self.run_id.is_some() && self.run_finished.is_none();
        if active_stream {
            self.streamed_data_evicted = true;
            for dataset in &mut self.measured_run_inputs.data {
                if dataset.stream_origin.is_some()
                    && dataset.consumed_records != dataset.binding.record_count()
                {
                    self.incomplete_artifact_data_evicted = true;
                }
                dataset.stream_origin = None;
            }
        } else {
            self.measured_run_inputs.data.clear();
            self.run_provenance.data.clear();
            self.unmeasured_data_loaded = false;
            self.streamed_data_evicted = false;
            self.incomplete_artifact_data_evicted = false;
        }
        self.has_data.clear();
        self.has_book_data.clear();
        self.data_iterator = BacktestDataIterator::new();
        self.data_len = 0;
        if !active_stream {
            self.data_stream_counter = 0;
        }
        self.ts_first = None;
        self.ts_last_data = None;
        self.sorted = true;
    }

    /// Clear all actors from the engine's internal trader.
    ///
    /// # Errors
    ///
    /// Returns an error if any actor fails to dispose.
    pub fn clear_actors(&mut self) -> anyhow::Result<()> {
        self.kernel.trader.borrow_mut().clear_actors()?;
        self.unmeasured_actor_loaded = false;
        Ok(())
    }

    /// Clear all trading strategies from the engine's internal trader.
    ///
    /// # Errors
    ///
    /// Returns an error if any strategy fails to dispose.
    pub fn clear_strategies(&mut self) -> anyhow::Result<()> {
        self.kernel.trader.borrow_mut().clear_strategies()?;
        self.measured_run_inputs.strategy = None;
        self.measured_run_inputs.config = None;
        self.run_provenance.strategy = None;
        self.run_provenance.config = None;
        self.unmeasured_strategy_loaded = false;
        Ok(())
    }

    /// Clear all execution algorithms from the engine's internal trader.
    ///
    /// # Errors
    ///
    /// Returns an error if any execution algorithm fails to dispose.
    pub fn clear_exec_algorithms(&mut self) -> anyhow::Result<()> {
        self.kernel.trader.borrow_mut().clear_exec_algorithms()?;
        self.unmeasured_exec_algorithm_loaded = false;
        Ok(())
    }

    /// Dispose of the backtest engine, releasing all resources.
    pub fn dispose(&mut self) {
        self.clear_data();
        self.accumulator.clear();
        self.kernel.dispose();
    }

    /// Return the backtest result from the last run.
    #[must_use]
    pub fn get_result(&self) -> BacktestResult {
        self.completed_result
            .clone()
            .unwrap_or_else(|| self.build_result())
    }

    fn build_result(&self) -> BacktestResult {
        let elapsed_time_secs = match (self.backtest_start, self.backtest_end) {
            (Some(start), Some(end)) => (end.as_f64() - start.as_f64()) / 1_000_000_000.0,
            _ => 0.0,
        };

        let cache = self.kernel.cache.borrow();
        let orders = cache.orders(None, None, None, None, None);
        let total_events = event_count_as_usize(self.kernel.exec_engine.borrow().event_count());
        let total_orders = orders.len();
        let positions: Vec<Position> = cache
            .positions(None, None, None, None, None)
            .into_iter()
            .map(|p| p.cloned())
            .collect();
        let cached_positions_count = positions.len();
        let snapshot_positions = cache.position_snapshots(None, None).len();
        let total_positions = Self::total_positions_with_snapshots(&cache, cached_positions_count);
        let summary = self.build_result_summary(
            &cache,
            total_events,
            total_orders,
            cached_positions_count,
            snapshot_positions,
        );

        let stats = self.kernel.portfolio.borrow().statistics();
        let stats_pnls = stats.pnls;
        let stats_returns = stats.returns;
        let stats_general = stats.general;
        let returns_series = stats.returns_series;

        BacktestResult {
            trader_id: self.config.trader_id().to_string(),
            machine_id: self.kernel.machine_id.clone(),
            instance_id: self.instance_id,
            run_config_id: self.run_config_id.clone(),
            run_id: self.run_id,
            provenance: self.run_provenance.clone(),
            run_started: self.run_started,
            run_finished: self.run_finished,
            backtest_start: self.backtest_start,
            backtest_end: self.backtest_end,
            elapsed_time_secs,
            iterations: self.iteration,
            total_events,
            total_orders,
            total_positions,
            summary,
            stats_pnls,
            stats_returns,
            stats_general,
            returns_series,
            event_store_receipt: self.event_store_receipt.clone(),
            evidence_seal: None,
            run_inputs_verified: self.run_inputs_verified,
        }
    }

    fn build_result_summary(
        &self,
        cache: &Cache,
        total_events: usize,
        total_orders: usize,
        cached_positions_count: usize,
        snapshot_positions: usize,
    ) -> AHashMap<String, String> {
        let mut summary = AHashMap::new();
        summary.insert("iterations".to_string(), self.iteration.to_string());
        summary.insert("total_events".to_string(), total_events.to_string());
        summary.insert("orders.total".to_string(), total_orders.to_string());
        summary.insert(
            "orders.open".to_string(),
            cache
                .orders_open_count(None, None, None, None, None)
                .to_string(),
        );
        summary.insert(
            "orders.closed".to_string(),
            cache
                .orders_closed_count(None, None, None, None, None)
                .to_string(),
        );
        summary.insert(
            "orders.emulated".to_string(),
            cache
                .orders_emulated_count(None, None, None, None, None)
                .to_string(),
        );
        summary.insert(
            "orders.inflight".to_string(),
            cache
                .orders_inflight_count(None, None, None, None, None)
                .to_string(),
        );
        summary.insert(
            "positions.total".to_string(),
            cached_positions_count.to_string(),
        );
        summary.insert(
            "positions.open".to_string(),
            cache
                .positions_open_count(None, None, None, None, None)
                .to_string(),
        );
        summary.insert(
            "positions.closed".to_string(),
            cache
                .positions_closed_count(None, None, None, None, None)
                .to_string(),
        );
        summary.insert(
            "positions.snapshots".to_string(),
            snapshot_positions.to_string(),
        );
        summary.insert(
            "positions.total_with_snapshots".to_string(),
            (cached_positions_count + snapshot_positions).to_string(),
        );

        let mut venues: Vec<Venue> = self.venues.keys().copied().collect();
        venues.sort_by_key(ToString::to_string);
        summary.insert("venues.total".to_string(), venues.len().to_string());

        for venue in venues {
            let Some(account) = cache.account_for_venue(&venue) else {
                continue;
            };

            let venue_key = venue.to_string();
            let account_key = format!("account.{venue_key}");
            summary.insert(format!("{account_key}.id"), account.id().to_string());
            summary.insert(
                format!("{account_key}.type"),
                account.account_type().to_string(),
            );
            summary.insert(
                format!("{account_key}.base_currency"),
                account
                    .base_currency()
                    .map_or_else(|| "None".to_string(), |currency| currency.code.to_string()),
            );
            summary.insert(
                format!("{account_key}.event_count"),
                account.event_count().to_string(),
            );

            let mut balances: Vec<_> = account.balances().into_iter().collect();
            balances.sort_by_key(|(currency, _)| currency.code.to_string());

            for (currency, balance) in balances {
                let balance_key = format!("{account_key}.balance.{}", currency.code);
                summary.insert(format!("{balance_key}.total"), balance.total.to_string());
                summary.insert(format!("{balance_key}.free"), balance.free.to_string());
                summary.insert(format!("{balance_key}.locked"), balance.locked.to_string());
            }
        }

        summary
    }

    fn route_data_to_exchange(&mut self, data: &Data) {
        if matches!(
            data,
            Data::MarkPriceUpdate(_)
                | Data::IndexPriceUpdate(_)
                | Data::OptionGreeks(_)
                | Data::Custom(_)
        ) {
            return;
        }
        #[cfg(feature = "defi")]
        if matches!(data, Data::Defi(_)) {
            return;
        }

        let venue = data.instrument_id().venue;
        if let Some(exchange) = self.venues.get(&venue) {
            let mut exchange_ref = exchange.borrow_mut();
            let mut processed_book_data = false;

            match data {
                Data::Delta(delta) => {
                    exchange_ref.process_order_book_delta(*delta);
                    processed_book_data = true;
                }
                Data::Deltas(deltas) => {
                    exchange_ref.process_order_book_deltas(deltas);
                    processed_book_data = true;
                }
                Data::Depth10(depth) => {
                    exchange_ref.process_order_book_depth10(depth);
                    processed_book_data = true;
                }
                Data::Quote(quote) => exchange_ref.process_quote_tick(quote),
                Data::Trade(trade) => exchange_ref.process_trade_tick(trade),
                Data::Bar(bar) => exchange_ref.process_bar(*bar),
                Data::InstrumentStatus(status) => exchange_ref.process_instrument_status(*status),
                Data::InstrumentClose(close) => exchange_ref.process_instrument_close(*close),
                Data::FundingRateUpdate(funding) => {
                    let settlement_ns =
                        exchange_ref.process_funding_rate_deferred(*funding, data.ts_init());
                    self.schedule_funding_settlement_if_required(venue, settlement_ns);
                }
                _ => {}
            }

            drop(exchange_ref);

            if processed_book_data {
                self.has_book_processed.insert(data.instrument_id());
            }
        } else {
            log::warn!("No exchange found for venue {venue}, data not routed");
        }
    }

    fn advance_time_impl(
        &mut self,
        ts_now: UnixNanos,
        clocks: &[Rc<RefCell<dyn Clock>>],
    ) -> anyhow::Result<()> {
        for clock in clocks {
            Self::advance_clock_on_accumulator(&mut self.accumulator, clock, ts_now, false);
        }

        // Process events with ts_event < ts_now
        let ts_before = if ts_now.as_u64() > 0 {
            UnixNanos::from(ts_now.as_u64() - 1)
        } else {
            UnixNanos::default()
        };

        let mut shutdown_at: Option<UnixNanos> = None;

        while let Some(ts_event) = self
            .accumulator
            .peek_next_time()
            .filter(|ts_event| *ts_event <= ts_before)
        {
            self.run_timer_handlers_at(clocks, ts_event, ts_now);

            if self.kernel.is_shutdown_requested() {
                self.accumulator.clear();
                shutdown_at = Some(ts_event);
                break;
            }
            self.finalize_timestamp(clocks, ts_event)?;

            if self.kernel.is_shutdown_requested() {
                self.accumulator.clear();
                shutdown_at = Some(ts_event);
                break;
            }

            for clock in clocks {
                Self::advance_clock_on_accumulator(&mut self.accumulator, clock, ts_now, false);
            }
        }

        // On a mid-drain shutdown, anchor state at the firing timer's ts so
        // post-run settlement and backtest_end reflect the graceful stop
        if let Some(ts_event) = shutdown_at {
            self.last_ns = ts_event;
        } else {
            Self::set_all_clocks_time(clocks, ts_now);
            logging_clock_set_static_time(ts_now.as_u64());
        }

        Ok(())
    }

    fn flush_accumulator_events(
        &mut self,
        clocks: &[Rc<RefCell<dyn Clock>>],
        ts_now: UnixNanos,
    ) -> anyhow::Result<()> {
        // Bail after shutdown so handler-scheduled alerts do not fire post-stop
        if self.kernel.is_shutdown_requested() {
            self.accumulator.clear();
            return Ok(());
        }

        for clock in clocks {
            Self::advance_clock_on_accumulator(&mut self.accumulator, clock, ts_now, false);
        }

        while let Some(ts_event) = self
            .accumulator
            .peek_next_time()
            .filter(|ts_event| *ts_event <= ts_now)
        {
            self.run_timer_handlers_at(clocks, ts_event, ts_now);

            if self.kernel.is_shutdown_requested() {
                self.accumulator.clear();
                break;
            }
            self.finalize_timestamp(clocks, ts_event)?;

            if self.kernel.is_shutdown_requested() {
                self.accumulator.clear();
                break;
            }

            for clock in clocks {
                Self::advance_clock_on_accumulator(&mut self.accumulator, clock, ts_now, false);
            }
        }

        Ok(())
    }

    fn process_next_timer(&mut self, clocks: &[Rc<RefCell<dyn Clock>>]) -> anyhow::Result<bool> {
        self.flush_accumulator_events(clocks, self.last_ns)?;

        // Find minimum next timer time across all component clocks
        let mut min_next_time: Option<UnixNanos> = None;

        for clock in clocks {
            let clock_ref = clock.borrow();
            for name in clock_ref.timer_names() {
                if let Some(next_time) = clock_ref.next_time_ns(name)
                    && next_time > self.last_ns
                {
                    min_next_time = Some(match min_next_time {
                        Some(current_min) => next_time.min(current_min),
                        None => next_time,
                    });
                }
            }
        }

        match min_next_time {
            None => Ok(true),
            Some(t)
                if (self.run_provenance_required && t >= self.end_ns)
                    || (!self.run_provenance_required && t > self.end_ns) =>
            {
                Ok(true)
            }
            Some(t) => {
                self.last_ns = t;
                self.flush_accumulator_events(clocks, t)?;
                Ok(false)
            }
        }
    }

    fn run_timer_handlers_at(
        &mut self,
        clocks: &[Rc<RefCell<dyn Clock>>],
        ts_event: UnixNanos,
        advance_to: UnixNanos,
    ) {
        while self.accumulator.peek_next_time() == Some(ts_event) {
            let handler = self
                .accumulator
                .pop_next_at_or_before(ts_event)
                .expect("timer exists at timestamp");
            Self::set_all_clocks_time(clocks, ts_event);
            logging_clock_set_static_time(ts_event.as_u64());
            handler.run();
            self.drain_command_queues();

            if self.kernel.is_shutdown_requested() {
                return;
            }

            for clock in clocks {
                Self::advance_clock_on_accumulator(&mut self.accumulator, clock, advance_to, false);
            }
        }
    }

    fn finalize_timestamp(
        &mut self,
        clocks: &[Rc<RefCell<dyn Clock>>],
        ts_now: UnixNanos,
    ) -> anyhow::Result<()> {
        loop {
            self.settle_venues(ts_now);

            if self.kernel.is_shutdown_requested() {
                self.accumulator.clear();
                break;
            }

            for clock in clocks {
                Self::advance_clock_on_accumulator(&mut self.accumulator, clock, ts_now, false);
            }

            if self.accumulator.peek_next_time() == Some(ts_now) {
                self.run_timer_handlers_at(clocks, ts_now, ts_now);
                continue;
            }

            if !self.settle_funding_rates(ts_now)? {
                break;
            }
        }

        self.run_venue_modules(ts_now);
        self.run_venue_liquidations(ts_now);
        Ok(())
    }

    fn settle_funding_rates(&mut self, ts_now: UnixNanos) -> anyhow::Result<bool> {
        let mut due = self
            .venues
            .iter()
            .flat_map(|(venue, exchange)| {
                exchange
                    .borrow()
                    .funding_boundaries_due(ts_now)
                    .into_iter()
                    .map(|(boundary, instrument_id)| (boundary, *venue, instrument_id))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        due.sort_unstable();

        if let Some((boundary, venue, instrument_id)) = due
            .iter()
            .copied()
            .find(|(boundary, _, _)| *boundary < ts_now)
        {
            return self.fail_funding(format!(
                "Late funding boundary for {instrument_id} on {venue}: {boundary} < replay timestamp {ts_now}"
            ));
        }

        if due.is_empty() {
            return Ok(false);
        }

        for (boundary, venue, instrument_id) in due {
            if !self.venues[&venue]
                .borrow_mut()
                .settle_funding_boundary(boundary, instrument_id)
            {
                return self.fail_funding(format!(
                    "Funding settlement failed for {instrument_id} on {venue} at {boundary}"
                ));
            }
        }

        let next_boundaries = self
            .venues
            .iter()
            .filter_map(|(venue, exchange)| {
                exchange
                    .borrow()
                    .next_funding_boundary()
                    .map(|boundary| (*venue, boundary))
            })
            .collect::<Vec<_>>();

        for (venue, boundary) in next_boundaries {
            self.schedule_funding_settlement_if_required(venue, Some(boundary));
        }

        Ok(true)
    }

    fn fail_funding<T>(&mut self, error: String) -> anyhow::Result<T> {
        if self.funding_error.is_none() {
            self.funding_error = Some(error.clone());
        }
        Err(anyhow::anyhow!(error))
    }

    fn set_instrument_expiration_timers(&self) -> anyhow::Result<()> {
        for exchange in self.venues.values() {
            let expirations = exchange.borrow().instrument_expirations();
            for (instrument_id, expiration_ns) in expirations {
                self.set_instrument_expiration_timer(exchange, instrument_id, expiration_ns)?;
            }
        }

        Ok(())
    }

    fn set_instrument_expiration_timer(
        &self,
        exchange: &Rc<RefCell<SimulatedExchange>>,
        instrument_id: InstrumentId,
        expiration_ns: UnixNanos,
    ) -> anyhow::Result<()> {
        if expiration_ns == UnixNanos::default() {
            return Ok(());
        }

        let timer_name = Self::instrument_expiration_timer_name(instrument_id.venue, expiration_ns);
        let timer_key = ustr::Ustr::from(timer_name.as_str());
        if self.kernel.clock.borrow().timer_exists(&timer_key) {
            return Ok(());
        }

        let exchange: Weak<RefCell<SimulatedExchange>> = Rc::downgrade(exchange);
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |event: TimeEvent| {
            if let Some(exchange) = exchange.upgrade() {
                exchange
                    .borrow_mut()
                    .process_instrument_expirations(event.ts_event);
            }
        });
        let mut clock = self.kernel.clock.borrow_mut();

        clock.set_time_alert_ns(
            &timer_name,
            expiration_ns,
            Some(TimeEventCallback::from(callback)),
            None,
        )?;

        Ok(())
    }

    fn instrument_expiration_timer_name(venue: Venue, expiration_ns: UnixNanos) -> String {
        format!("INSTRUMENT-EXPIRATION:{venue}:{expiration_ns}")
    }

    fn schedule_funding_settlement_if_required(
        &self,
        venue: Venue,
        settlement_ns: Option<UnixNanos>,
    ) {
        let Some(settlement_ns) = settlement_ns else {
            return;
        };

        if let Err(e) = self.set_funding_settlement_timer(venue, settlement_ns) {
            log::error!("Cannot schedule funding settlement for {venue}: {e}");
        }
    }

    fn set_funding_settlement_timer(
        &self,
        venue: Venue,
        settlement_ns: UnixNanos,
    ) -> anyhow::Result<()> {
        let timer_name = Self::funding_settlement_timer_name(venue);
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(|_| {});
        let mut clock = self.kernel.clock.borrow_mut();

        clock.set_time_alert_ns(
            &timer_name,
            settlement_ns,
            Some(TimeEventCallback::from(callback)),
            None,
        )?;

        Ok(())
    }

    fn funding_settlement_timer_name(venue: Venue) -> String {
        format!("FUNDING-SETTLEMENT:{venue}")
    }

    fn cancel_funding_settlement_timers(&self) {
        let mut clock = self.kernel.clock.borrow_mut();
        for venue in self.venues.keys() {
            clock.cancel_timer(&Self::funding_settlement_timer_name(*venue));
        }
    }

    fn collect_all_clocks(&self) -> Vec<Rc<RefCell<dyn Clock>>> {
        let mut clocks = vec![self.kernel.clock.clone()];
        clocks.extend(self.kernel.trader.borrow().get_component_clocks());
        clocks
    }

    fn max_inflight_command_ts(&self) -> Option<UnixNanos> {
        self.venues
            .values()
            .filter_map(|v| v.borrow().max_inflight_command_ts())
            .max()
    }

    fn settle_venues(&self, ts_now: UnixNanos) {
        // Advance venue clocks so modules and event generators see the
        // correct timestamp even when no commands are pending
        for exchange in self.venues.values() {
            exchange.borrow().set_clock_time(ts_now);
        }

        // Drain commands then iterate matching engines to fill newly added
        // orders. Fills may enqueue further commands (e.g. hedge orders
        // submitted from on_order_filled), so loop until quiescent.
        // Only process and iterate venues that had pending commands each
        // pass, to avoid extra fill-model rolls on untouched venues.
        loop {
            // Drain first so commands buffered in the trading queue (e.g. from
            // on_stop handlers) reach the venues before we check for activity.
            self.drain_command_queues();

            let active_venues: Vec<Venue> = self
                .venues
                .iter()
                .filter(|(_, ex)| ex.borrow().has_pending_commands(ts_now))
                .map(|(id, _)| *id)
                .collect();

            if active_venues.is_empty() {
                break;
            }

            for venue_id in &active_venues {
                self.venues[venue_id].borrow_mut().process(ts_now);
            }
            self.drain_command_queues();

            for venue_id in &active_venues {
                self.venues[venue_id]
                    .borrow_mut()
                    .iterate_matching_engines(ts_now);
            }

            // Drain again so fill-triggered commands (e.g. hedge orders
            // from on_order_filled) are visible to has_pending_commands
            self.drain_command_queues();
        }
    }

    fn run_venue_modules(&mut self, ts_now: UnixNanos) {
        if self.last_module_ns == Some(ts_now) {
            return;
        }
        self.last_module_ns = Some(ts_now);

        // Pre-settle handler-generated work so modules see final state
        self.drain_command_queues();
        self.settle_venues(ts_now);

        for exchange in self.venues.values() {
            exchange.borrow_mut().process_modules(ts_now);
        }

        // Post-settle any commands emitted by modules
        self.drain_command_queues();
        self.settle_venues(ts_now);
    }

    fn run_venue_liquidations(&mut self, ts_now: UnixNanos) {
        if self.last_liquidation_ns == Some(ts_now) {
            return;
        }
        self.last_liquidation_ns = Some(ts_now);

        for exchange in self.venues.values() {
            exchange.borrow_mut().process_liquidations(ts_now);
        }

        self.drain_command_queues();
        self.settle_venues(ts_now);
    }

    fn drain_exec_client_events(&self) {
        for client in &self.exec_clients {
            client.drain_queued_events();
        }
    }

    fn drain_command_queues(&self) {
        // Drain trading commands, exec client events, and data commands
        // in a loop until all queues settle. Handles cascading re-entrancy
        // (e.g. strategy submits order from on_order_filled).
        loop {
            drain_trading_cmd_queue();
            drain_data_cmd_queue();
            self.drain_exec_client_events();

            if trading_cmd_queue_is_empty() && data_cmd_queue_is_empty() {
                break;
            }
        }
    }

    fn init_command_senders() {
        replace_data_cmd_sender(Arc::new(SyncDataCommandSender));
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));
    }

    fn advance_clock_on_accumulator(
        accumulator: &mut TimeEventAccumulator,
        clock: &Rc<RefCell<dyn Clock>>,
        to_time_ns: UnixNanos,
        set_time: bool,
    ) {
        let mut clock_ref = clock.borrow_mut();
        let test_clock = clock_ref
            .as_any_mut()
            .downcast_mut::<TestClock>()
            .expect("BacktestEngine requires TestClock");
        accumulator.advance_clock(test_clock, to_time_ns, set_time);
    }

    fn set_all_clocks_time(clocks: &[Rc<RefCell<dyn Clock>>], ts: UnixNanos) {
        for clock in clocks {
            let mut clock_ref = clock.borrow_mut();
            let test_clock = clock_ref
                .as_any_mut()
                .downcast_mut::<TestClock>()
                .expect("BacktestEngine requires TestClock");
            test_clock.set_time(ts);
        }
    }

    #[rustfmt::skip]
    fn log_pre_run(&self) {
        log_info!("=================================================================", color = LogColor::Cyan);
        log_info!(" BACKTEST PRE-RUN", color = LogColor::Cyan);
        log_info!("=================================================================", color = LogColor::Cyan);

        let cache = self.kernel.cache.borrow();
        for exchange in self.venues.values() {
            let ex = exchange.borrow();
            log_info!("=================================================================", color = LogColor::Cyan);
            log::info!(" SimulatedVenue {} ({})", ex.id, ex.account_type);
            log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);

            if let Some(account) = cache.account_for_venue(&ex.id) {
                log::info!("Balances starting:");
                let account_ref: &dyn Account = match &*account {
                    AccountAny::Margin(margin) => margin,
                    AccountAny::Cash(cash) => cash,
                    AccountAny::Betting(betting) => betting,
                };

                for balance in account_ref.starting_balances().values() {
                    log::info!("  {balance}");
                }
            }
        }

        log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);
    }

    #[rustfmt::skip]
    fn log_run(&self) {
        let config_id = self.run_config_id.as_deref().unwrap_or("None");
        let id = format_optional_uuid(self.run_id.as_ref());
        let start = format_optional_nanos(self.backtest_start);

        log_info!("=================================================================", color = LogColor::Cyan);
        log_info!(" BACKTEST RUN", color = LogColor::Cyan);
        log_info!("=================================================================", color = LogColor::Cyan);
        log::info!("Run config ID:  {config_id}");
        log::info!("Run ID:         {id}");
        log::info!("Backtest start: {start}");
        log::info!("Data elements:  {}", self.data_len);
        log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);
    }

    #[rustfmt::skip]
    fn log_post_run(&self) {
        let cache = self.kernel.cache.borrow();
        let orders = cache.orders(None, None, None, None, None);
        let total_events = event_count_as_usize(self.kernel.exec_engine.borrow().event_count());
        let total_orders = orders.len();
        let positions: Vec<Position> = cache
            .positions(None, None, None, None, None)
            .into_iter()
            .map(|p| p.cloned())
            .collect();
        let total_positions = Self::total_positions_with_snapshots(&cache, positions.len());

        let config_id = self.run_config_id.as_deref().unwrap_or("None");
        let id = format_optional_uuid(self.run_id.as_ref());
        let started = format_optional_nanos(self.run_started);
        let finished = format_optional_nanos(self.run_finished);
        let elapsed = format_optional_duration(self.run_started, self.run_finished);
        let bt_start = format_optional_nanos(self.backtest_start);
        let bt_end = format_optional_nanos(self.backtest_end);
        let bt_range = format_optional_duration(self.backtest_start, self.backtest_end);
        let iterations = self.iteration.separate_with_underscores();
        let events = total_events.separate_with_underscores();
        let num_orders = total_orders.separate_with_underscores();
        let num_positions = total_positions.separate_with_underscores();

        log_info!("=================================================================", color = LogColor::Cyan);
        log_info!(" BACKTEST POST-RUN", color = LogColor::Cyan);
        log_info!("=================================================================", color = LogColor::Cyan);
        log::info!("Run config ID:  {config_id}");
        log::info!("Run ID:         {id}");
        log::info!("Run started:    {started}");
        log::info!("Run finished:   {finished}");
        log::info!("Elapsed time:   {elapsed}");
        log::info!("Backtest start: {bt_start}");
        log::info!("Backtest end:   {bt_end}");
        log::info!("Backtest range: {bt_range}");
        log::info!("Iterations: {iterations}");
        log::info!("Total events: {events}");
        log::info!("Total orders: {num_orders}");
        log::info!("Total positions: {num_positions}");

        if !self.config.run_analysis {
            return;
        }

        let accounts = cache.accounts_all_owned();
        let mut snapshots = Vec::new();
        for position in &positions {
            snapshots.extend(cache.position_snapshots(Some(&position.id), None));
        }
        let recorded = self.kernel.portfolio.borrow().recorded_realized_pnls();
        let portfolio = self.kernel.portfolio.borrow();
        let portfolio_snapshots = accounts
            .iter()
            .flat_map(|account| portfolio.snapshots(&account.id()))
            .collect::<Vec<_>>();
        let analyzer = PortfolioAnalyzer::from_accounts_with_snapshots(
            &accounts,
            &positions,
            &snapshots,
            &portfolio_snapshots,
            recorded,
        );
        log_portfolio_performance(&analyzer);
    }

    fn total_positions_with_snapshots(cache: &Cache, cached_positions_count: usize) -> usize {
        cached_positions_count + cache.position_snapshots(None, None).len()
    }

    /// Registers a data client for the given `client_id` if one does not already exist.
    pub fn add_data_client_if_not_exists(&mut self, client_id: ClientId) {
        self.unmeasured_environment_mutation = true;
        if self
            .kernel
            .data_engine
            .borrow()
            .registered_clients()
            .contains(&client_id)
        {
            return;
        }

        let venue = Venue::from(client_id.as_str());
        let backtest_client = BacktestDataClient::new(client_id, venue, self.kernel.cache.clone());
        let data_client_adapter = DataClientAdapter::new(
            backtest_client.client_id,
            None,
            false,
            false,
            Box::new(backtest_client),
        );

        self.kernel
            .data_engine
            .borrow_mut()
            .register_client(data_client_adapter, None);
    }

    /// Registers a market data client for the given `venue` if one does not already exist.
    pub fn add_market_data_client_if_not_exists(&mut self, venue: Venue) {
        self.unmeasured_environment_mutation = true;
        self.add_market_data_client_if_not_exists_inner(venue);
    }

    fn add_market_data_client_if_not_exists_inner(&self, venue: Venue) {
        let client_id = ClientId::from(venue.as_str());

        if !self
            .kernel
            .data_engine
            .borrow()
            .registered_clients()
            .contains(&client_id)
        {
            let backtest_client =
                BacktestDataClient::new(client_id, venue, self.kernel.cache.clone());
            let data_client_adapter = DataClientAdapter::new(
                client_id,
                Some(venue),
                false,
                false,
                Box::new(backtest_client),
            );
            self.kernel
                .data_engine
                .borrow_mut()
                .register_client(data_client_adapter, Some(venue));
        }
    }
}

fn format_optional_nanos(nanos: Option<UnixNanos>) -> String {
    nanos.map_or("None".to_string(), unix_nanos_to_iso8601)
}

fn format_optional_uuid(uuid: Option<&UUID4>) -> String {
    uuid.map_or("None".to_string(), ToString::to_string)
}

fn event_count_as_usize(event_count: u64) -> usize {
    usize::try_from(event_count).expect("execution event count fits usize")
}

fn format_optional_duration(start: Option<UnixNanos>, end: Option<UnixNanos>) -> String {
    match (start, end) {
        (Some(s), Some(e)) => {
            let delta = e.to_datetime_utc() - s.to_datetime_utc();
            let days = delta.num_days().abs();
            let hours = delta.num_hours().abs() % 24;
            let minutes = delta.num_minutes().abs() % 60;
            let seconds = delta.num_seconds().abs() % 60;
            let micros = delta.subsec_nanos().unsigned_abs() / 1_000;
            format!("{days} days {hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
        }
        _ => "None".to_string(),
    }
}

#[rustfmt::skip]
fn log_portfolio_performance(analyzer: &PortfolioAnalyzer) {
    log_info!("=================================================================", color = LogColor::Cyan);
    log_info!(" PORTFOLIO PERFORMANCE", color = LogColor::Cyan);
    log_info!("=================================================================", color = LogColor::Cyan);

    for currency in analyzer.currencies() {
        log::info!(" PnL Statistics ({})", currency.code);
        log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);

        if let Ok(pnl_lines) = analyzer.get_stats_pnls_formatted(Some(currency), None) {
            for line in &pnl_lines {
                log::info!("{line}");
            }
        }

        log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);
    }

    log::info!(" Returns Statistics");
    log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);

    for line in &analyzer.get_stats_returns_formatted() {
        log::info!("{line}");
    }
    log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);

    log::info!(" General Statistics");
    log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);

    for line in &analyzer.get_stats_general_formatted() {
        log::info!("{line}");
    }
    log_info!("-----------------------------------------------------------------", color = LogColor::Cyan);
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use indexmap::IndexMap;
    use nautilus_analysis::{EvaluationSuite, Gate};
    use nautilus_common::{
        actor::DataActor,
        enums::Environment,
        messages::{
            data::{DataCommand, UnsubscribeCommand},
            execution::{SubmitOrder, TradingCommand},
        },
        msgbus::{
            self, MessagingSwitchboard, TypedHandler,
            stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
        },
    };
    use nautilus_event_store::EventStoreConfig;
    use nautilus_execution::engine::{SnapshotAnchorer, stubs::StubExecutionClient};
    #[cfg(feature = "defi")]
    use nautilus_model::defi::{Block, Blockchain, DefiData};
    use nautilus_model::{
        data::{
            Bar, BarSpecification, BarType, Data, InstrumentStatus, QuoteTick,
            stubs::stub_custom_data,
        },
        enums::{
            AccountType, BarAggregation, BookType, MarketStatus, MarketStatusAction, OmsType,
            OrderSide, OrderStatus, OrderType, PriceType, TriggerType,
        },
        identifiers::{
            AccountId, ActorId, ClientId, ComponentId, InstrumentId, PositionId, StrategyId, Venue,
        },
        instruments::{
            CryptoPerpetual, Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt,
        },
        orders::{Order, OrderAny, OrderTestBuilder},
        types::{Money, Price, Quantity},
    };
    use nautilus_provenance::{
        ArtifactId, ArtifactRef, ContentDigest, ExecutionEnvironmentBinding,
    };
    use nautilus_system::{KernelEventStore, RegisteredComponents};
    use nautilus_testkit::{
        cache::TestCacheDatabaseControl,
        components::{StateActor, StateStrategy},
    };
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{config::StrategyConfig, core::StrategyCore},
    };
    use rstest::*;
    use serde::{Deserialize, Serialize};
    use ustr::Ustr;

    use super::*;
    use crate::environment::{BacktestVenueSpec, MarketKind};

    #[derive(Debug)]
    struct BacktestReplayKernelEventStore {
        fail_restore: bool,
        halted: bool,
    }

    impl KernelEventStore for BacktestReplayKernelEventStore {
        fn set_run_provenance(&mut self, _provenance: RunProvenance) -> anyhow::Result<()> {
            Ok(())
        }

        fn restore_parent_cache(
            &mut self,
            _instance_id: UUID4,
            _cache: &mut Cache,
        ) -> anyhow::Result<()> {
            if self.fail_restore {
                anyhow::bail!("replay restore failed");
            }

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

        fn seal(
            &mut self,
            _ts_init: UnixNanos,
        ) -> anyhow::Result<Option<nautilus_system::EventStoreSealReceipt>> {
            Ok(None)
        }

        fn run_id(&self) -> Option<&str> {
            Some("replay-child")
        }

        fn parent_run_id(&self) -> Option<&str> {
            Some("seed-run")
        }

        fn is_event_store_replay_configured(&self) -> bool {
            true
        }

        fn is_halted(&self) -> bool {
            self.halted
        }
    }

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

    impl ArtifactConfiguredStrategy for TestStrategy {
        type ArtifactConfig = StrategyConfig;

        fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            Ok(Self::new(config))
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            self.core.config()
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
    enum ArtifactConfigMutationPhase {
        Start,
        Quote,
        Stop,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct MutatingBacktestArtifactConfig {
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        phase: ArtifactConfigMutationPhase,
        revision: Cell<u64>,
    }

    #[derive(Debug)]
    struct MutatingBacktestArtifactStrategy {
        core: StrategyCore,
        config: MutatingBacktestArtifactConfig,
    }

    impl DataActor for MutatingBacktestArtifactStrategy {
        fn on_start(&mut self) -> anyhow::Result<()> {
            self.subscribe_quotes(self.config.instrument_id, None, None);
            if self.config.phase == ArtifactConfigMutationPhase::Start {
                self.config.revision.set(1);
            }
            Ok(())
        }

        fn on_quote(&mut self, _quote: &QuoteTick) -> anyhow::Result<()> {
            if self.config.phase == ArtifactConfigMutationPhase::Quote {
                self.config.revision.set(1);
            }
            Ok(())
        }

        fn on_stop(&mut self) -> anyhow::Result<()> {
            if self.config.phase == ArtifactConfigMutationPhase::Stop {
                self.config.revision.set(1);
            }
            Ok(())
        }
    }

    nautilus_strategy!(MutatingBacktestArtifactStrategy);

    impl ArtifactConfiguredStrategy for MutatingBacktestArtifactStrategy {
        type ArtifactConfig = MutatingBacktestArtifactConfig;

        fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            Ok(Self {
                core: StrategyCore::new(StrategyConfig {
                    strategy_id: Some(config.strategy_id),
                    ..Default::default()
                }),
                config,
            })
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            &self.config
        }
    }

    #[derive(Debug)]
    struct IgnoringBacktestConfigStrategy {
        core: StrategyCore,
    }

    static IGNORING_BACKTEST_CONFIG_CONSTRUCTOR_CALLS: AtomicUsize = AtomicUsize::new(0);
    static IGNORING_BACKTEST_CONFIG_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    impl DataActor for IgnoringBacktestConfigStrategy {}

    nautilus_strategy!(IgnoringBacktestConfigStrategy);

    impl ArtifactConfiguredStrategy for IgnoringBacktestConfigStrategy {
        type ArtifactConfig = StrategyConfig;

        fn from_artifact_config(_config: Self::ArtifactConfig) -> anyhow::Result<Self> {
            IGNORING_BACKTEST_CONFIG_CONSTRUCTOR_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(Self {
                core: StrategyCore::new(StrategyConfig::default()),
            })
        }

        fn artifact_config(&self) -> &Self::ArtifactConfig {
            self.core.config()
        }
    }

    fn test_strategy_config_artifact() -> Vec<u8> {
        serde_json::to_vec(&StrategyConfig {
            strategy_id: Some(StrategyId::from("TestStrategy-001")),
            ..Default::default()
        })
        .unwrap()
    }

    fn create_engine() -> BacktestEngine {
        create_engine_from_environment(b"canonical test execution environment")
    }

    fn create_engine_from_environment(version: &[u8]) -> BacktestEngine {
        create_engine_from_environment_for_replay(version, 0, 10)
    }

    fn create_engine_from_environment_for_replay(
        version: &[u8],
        replay_start_ns: u64,
        replay_end_ns: u64,
    ) -> BacktestEngine {
        let content = test_environment_artifact_content(version, replay_start_ns, replay_end_ns);
        BacktestEngine::from_environment_artifact(
            ArtifactId::new("environment/test/binance-v1").unwrap(),
            &content,
        )
        .unwrap()
    }

    fn test_environment_artifact_content(
        version: &[u8],
        replay_start_ns: u64,
        replay_end_ns: u64,
    ) -> Vec<u8> {
        let mut venue = BacktestVenueSpec::with_builtin_defaults(
            Venue::from("BINANCE"),
            MarketKind::Crypto,
            std::collections::BTreeMap::default(),
            OmsType::Netting,
            AccountType::Margin,
            BookType::L1_MBP,
            vec![Money::from("1_000_000 USDT")],
        );
        venue.price_protection_points = version.iter().map(|byte| u32::from(*byte)).sum();
        let environment = BacktestEnvironmentSpec::new_for_replay(
            vec![venue],
            vec![InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt())],
            None,
            replay_start_ns,
            replay_end_ns,
            0,
        );
        environment.to_canonical_json().unwrap()
    }

    fn provenance_ref(id: &str) -> ArtifactRef {
        ArtifactRef::new(
            ArtifactId::new(id).unwrap(),
            ContentDigest::digest_bytes(id.as_bytes()),
        )
    }

    fn provenance_dataset(id: &str) -> DatasetBinding {
        DatasetBinding::new(
            provenance_ref(id),
            ContentDigest::digest_bytes(format!("materialized/{id}").as_bytes()),
            ProcessIdentity::new("test-decoder/v1").unwrap(),
            0,
            1,
        )
        .unwrap()
    }

    fn provenance_environment() -> ExecutionEnvironmentBinding {
        ExecutionEnvironmentBinding::new(
            provenance_ref("environment/test/cross-market-v1"),
            ProcessIdentity::new("test-environment-loader/v1").unwrap(),
            vec![provenance_ref("market-context/test/v1")],
            vec!["BINANCE".to_string()],
            vec!["ETHUSDT-PERP.BINANCE".to_string()],
        )
        .unwrap()
    }

    fn provenance_strategy(id: &str) -> StrategyBinding {
        StrategyBinding::new(
            provenance_ref(id),
            "test::MomentumStrategy",
            "MomentumStrategy-001",
        )
        .unwrap()
    }

    fn test_quote(instrument_id: InstrumentId, bid: &str, ask: &str, ts_init: u64) -> QuoteTick {
        QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(ts_init),
            UnixNanos::from(ts_init),
        )
    }

    fn add_test_instrument(
        engine: &mut BacktestEngine,
        instrument: &CryptoPerpetual,
    ) -> InstrumentId {
        let instrument = InstrumentAny::CryptoPerpetual(instrument.clone());
        let instrument_id = instrument.id();
        if !engine
            .venues
            .get(&instrument_id.venue)
            .is_some_and(|exchange| {
                exchange
                    .borrow()
                    .instrument_ids()
                    .any(|id| id == &instrument_id)
            })
        {
            engine.add_instrument(&instrument).unwrap();
        }
        instrument_id
    }

    fn add_measured_quote(
        engine: &mut BacktestEngine,
        artifact_id: &str,
        quote: QuoteTick,
    ) -> DatasetBinding {
        let source = encode_backtest_data_artifact(&[Data::Quote(quote)]).unwrap();
        engine
            .add_data_from_artifact(
                ArtifactId::new(artifact_id).unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap()
    }

    fn planned_measured_quote(
        artifact_id: &str,
        replay_ordinal: u64,
        quote: QuoteTick,
    ) -> DatasetBinding {
        let data = [Data::Quote(quote)];
        let source = encode_backtest_data_artifact(&data).unwrap();
        DatasetBinding::new(
            ArtifactRef::new(
                ArtifactId::new(artifact_id).unwrap(),
                ContentDigest::digest_bytes(&source),
            ),
            canonical_materialized_data_digest(&data).unwrap(),
            ProcessIdentity::new(BACKTEST_DATA_ARTIFACT_DECODER_ID).unwrap(),
            replay_ordinal,
            1,
        )
        .unwrap()
    }

    fn add_measured_test_strategy(engine: &mut BacktestEngine) {
        let config = test_strategy_config_artifact();
        engine
            .add_strategy_from_artifact::<TestStrategy>(
                StrategyId::from("TestStrategy-001"),
                ArtifactId::new("strategy/test/v1").unwrap(),
                ArtifactId::new("config/test/v1").unwrap(),
                &config,
            )
            .unwrap();
    }

    fn predeclare_dataset(engine: &mut BacktestEngine, binding: DatasetBinding) {
        let mut provenance = engine.run_provenance().clone();
        provenance.data.push(binding);
        engine.set_run_provenance(provenance).unwrap();
    }

    #[rstest]
    fn test_required_run_provenance_fails_closed_until_complete() {
        let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        engine.require_run_provenance().unwrap();
        assert!(
            engine
                .validate_run_provenance()
                .unwrap_err()
                .to_string()
                .contains("dataset")
        );

        engine
            .set_run_provenance(RunProvenance {
                data: vec![provenance_dataset("dataset/btc-usdt")],
                environment: Some(provenance_environment()),
                strategy: Some(provenance_strategy("strategy/momentum")),
                config: Some(provenance_ref("config/research")),
                ..Default::default()
            })
            .unwrap();

        assert!(
            engine
                .validate_run_provenance()
                .unwrap_err()
                .to_string()
                .contains("artifact-aware")
        );
    }

    #[rstest]
    fn test_new_pins_host_executable_startup_snapshot() {
        let engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        let executable_path = std::env::current_exe().unwrap();
        let executable = std::fs::read(executable_path).unwrap();

        assert_eq!(
            engine.host_executable_startup_digest,
            ContentDigest::digest_bytes(&executable)
        );
    }

    #[rstest]
    fn test_artifact_aware_loaders_measure_complete_run_inputs(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt);
        let instrument_id = instrument.id();
        assert!(
            engine
                .venues
                .get(&instrument_id.venue)
                .unwrap()
                .borrow()
                .instrument_ids()
                .any(|id| id == &instrument_id)
        );
        let quote = QuoteTick::new(
            instrument_id,
            Price::from("1000.00"),
            Price::from("1001.00"),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        let source = encode_backtest_data_artifact(&[Data::Quote(quote)]).unwrap();
        let dataset = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/btc-usdt/v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap();
        let config = test_strategy_config_artifact();
        let strategy = engine
            .add_strategy_from_artifact::<TestStrategy>(
                StrategyId::from("TestStrategy-001"),
                ArtifactId::new("strategy/test/v1").unwrap(),
                ArtifactId::new("config/test/v1").unwrap(),
                &config,
            )
            .unwrap();
        engine.require_run_provenance().unwrap();

        engine.validate_run_provenance().unwrap();
        assert_eq!(engine.run_provenance().data, vec![dataset]);
        assert_eq!(
            strategy.artifact().digest(),
            &engine.host_executable_startup_digest
        );
        assert_eq!(
            engine.run_provenance().config.as_ref().unwrap().digest(),
            &ContentDigest::digest_bytes(&config)
        );
        engine.run(None, None, None, false).unwrap();
        engine.get_result().verify_engine_evidence_seal().unwrap();
    }

    #[rstest]
    fn test_library_owned_run_identity_replaces_every_caller_supplied_claim() {
        let event_store_dir = tempfile::TempDir::new().unwrap();
        let content = test_environment_artifact_content(b"real-redb-release", 0, 10);
        let mut event_store = EventStoreConfig {
            base_dir: event_store_dir.path().to_path_buf(),
            identity: RunIdentity {
                binary_hash: "caller-forged-binary".to_string(),
                schema_version: u32::MAX,
                crate_versions: "caller-forged-lockfile".to_string(),
                feature_flags: vec!["caller-forged-feature".to_string()],
                adapter_versions: IndexMap::from([(
                    "caller-forged-adapter".to_string(),
                    "999".to_string(),
                )]),
                config_hash: "caller-forged-config".to_string(),
                provenance: RunProvenance {
                    promotion: Some(provenance_ref("promotion/caller-forged")),
                    ..Default::default()
                },
                seed: Some(u64::MAX),
            },
            ..Default::default()
        };

        let environment_digest = ContentDigest::digest_bytes(&content);
        let host_executable_digest = ContentDigest::digest_bytes(b"measured-host-executable");
        install_library_owned_run_identity(
            &mut event_store,
            environment_digest,
            host_executable_digest,
        )
        .unwrap();
        let secure_identity = event_store.identity;
        assert_eq!(
            secure_identity.binary_hash,
            host_executable_digest.to_string()
        );
        assert_eq!(
            secure_identity.crate_versions,
            ContentDigest::digest_bytes(WORKSPACE_CARGO_LOCK).to_string()
        );
        assert_eq!(
            secure_identity.feature_flags,
            compiled_backtest_feature_flags()
        );
        assert!(secure_identity.adapter_versions.is_empty());
        assert_eq!(secure_identity.provenance, RunProvenance::default());
        assert_eq!(secure_identity.seed, None);
        for digest in [
            &secure_identity.binary_hash,
            &secure_identity.crate_versions,
            &secure_identity.config_hash,
        ] {
            assert_eq!(digest.len(), 64);
            assert!(digest.parse::<ContentDigest>().is_ok());
        }
    }

    #[rstest]
    fn test_authenticated_inputs_without_preexecution_enforcement_cannot_promote() {
        let content = test_environment_artifact_content(b"unenforced-real-redb", 0, 10);
        let mut engine = BacktestEngine::from_environment_artifact(
            ArtifactId::new("environment/test/unenforced-real-redb-v1").unwrap(),
            &content,
        )
        .unwrap();
        let instrument_id = InstrumentId::from("ETHUSDT-PERP.BINANCE");
        add_measured_quote(
            &mut engine,
            "dataset/test/unenforced-real-redb-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);

        engine.run(None, None, None, false).unwrap();

        let result = engine.get_result();
        result.verify_engine_evidence_seal().unwrap();
        assert_eq!(result.backtest_start, Some(UnixNanos::from(1)));
        assert_eq!(result.backtest_end, Some(UnixNanos::from(1)));
        assert!(!result.run_inputs_verified);
        let suite = EvaluationSuite::new(
            "unenforced-research",
            "1",
            vec![Gate::Minimum {
                metric: "backtest.total_orders".to_string(),
                minimum: 0.0,
            }],
        )
        .unwrap();
        assert!(result.evaluate_for_promotion(&suite, None).passed());
        assert_eq!(
            result
                .promotion_attestation_for_release(
                    &suite,
                    None,
                    "must-not-release",
                    UnixNanos::new(20),
                )
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::UnverifiedRunInputs
        );
    }

    #[rstest]
    fn test_run_provenance_cannot_be_required_after_strategy_execution_starts(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/late-enforcement-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);

        engine.run(None, None, None, true).unwrap();
        let error = engine.require_run_provenance().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("after backtest or strategy execution")
        );
        assert!(!engine.run_provenance_required);

        engine.end_with_result().unwrap();
        let result = engine.get_result();
        result.verify_engine_evidence_seal().unwrap();
        assert!(!result.run_inputs_verified);
    }

    #[rstest]
    fn test_data_artifact_rejects_trailing_bytes(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let mut source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            1,
        ))])
        .unwrap();
        source.push(0xff);

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/trailing-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("trailing bytes"));
        assert!(engine.measured_run_inputs.data.is_empty());
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_data_artifact_rejects_trailing_bytes_inside_sbe_frame(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let mut source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            1,
        ))])
        .unwrap();
        let frame_len = u32::from_be_bytes(source[16..20].try_into().unwrap());
        source[16..20].copy_from_slice(&(frame_len + 1).to_be_bytes());
        source.push(0xff);

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/frame-trailing-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("not an exact canonical SBE frame")
        );
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_data_artifact_rejects_unknown_schema_version(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let mut source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            1,
        ))])
        .unwrap();
        source[8..10].copy_from_slice(&(BACKTEST_DATA_ARTIFACT_SCHEMA_VERSION + 1).to_be_bytes());

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/future-schema-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Unsupported backtest data artifact schema")
        );
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_data_artifact_rejects_precision_mismatch(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let mut source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            1,
        ))])
        .unwrap();
        source[10] ^= 1;

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/precision-mismatch-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not match compiled precision")
        );
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_data_artifact_rejects_bad_frame_length(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let mut source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            1,
        ))])
        .unwrap();
        let frame_len = u32::from_be_bytes(source[16..20].try_into().unwrap());
        source[16..20].copy_from_slice(&(frame_len + 1).to_be_bytes());

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/bad-frame-length-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("record frame is truncated"));
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_data_artifact_rejects_empty_batch(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        assert!(
            encode_backtest_data_artifact(&[])
                .unwrap_err()
                .to_string()
                .contains("must not be empty")
        );
        let mut source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            1,
        ))])
        .unwrap();
        source.truncate(BACKTEST_DATA_ARTIFACT_HEADER_BYTES);
        source[12..16].copy_from_slice(&0_u32.to_be_bytes());

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/empty-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("must not be empty"));
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    #[case(0)]
    #[case(10)]
    fn test_data_artifact_rejects_timestamp_outside_half_open_environment_scope(
        crypto_perpetual_ethusdt: CryptoPerpetual,
        #[case] ts_init: u64,
    ) {
        let mut engine = create_engine_from_environment_for_replay(b"scope-1-10", 1, 10);
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let source = encode_backtest_data_artifact(&[Data::Quote(test_quote(
            instrument_id,
            "1000.00",
            "1001.00",
            ts_init,
        ))])
        .unwrap();

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new(format!("dataset/test/outside-scope-{ts_init}-v1")).unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("outside environment replay scope")
        );
        assert!(engine.measured_run_inputs.data.is_empty());
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    #[case(0, 10, "start")]
    #[case(1, 11, "end")]
    fn test_authenticated_run_rejects_caller_window_override_before_start(
        crypto_perpetual_ethusdt: CryptoPerpetual,
        #[case] start_ns: u64,
        #[case] end_ns: u64,
        #[case] mismatched_boundary: &str,
    ) {
        let mut engine = create_engine_from_environment_for_replay(b"scope-1-10", 1, 10);
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/window-override-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);
        engine.require_run_provenance().unwrap();

        let error = engine
            .run(
                Some(UnixNanos::from(start_ns)),
                Some(UnixNanos::from(end_ns)),
                None,
                false,
            )
            .unwrap_err();

        assert!(error.to_string().contains(mismatched_boundary));
        assert!(engine.run_id.is_none());
        assert!(engine.run_started.is_none());
        assert!(engine.completed_result.is_none());
    }

    #[rstest]
    fn test_artifact_validation_checks_unknown_instrument_in_second_record(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let source = encode_backtest_data_artifact(&[
            Data::Quote(test_quote(instrument_id, "1000.00", "1001.00", 1)),
            Data::Quote(test_quote(
                InstrumentId::from("UNKNOWN.BINANCE"),
                "1000.00",
                "1001.00",
                2,
            )),
        ])
        .unwrap();

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/unknown-second-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("artifact record 1"));
        assert!(error.to_string().contains("not found in the cache"));
        assert!(engine.measured_run_inputs.data.is_empty());
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_artifact_validation_rejects_internally_aggregated_bar(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let bar_type = BarType::new(
            instrument_id,
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::Internal,
        );
        let bar = Bar::new(
            bar_type,
            Price::from("1000.00"),
            Price::from("1001.00"),
            Price::from("999.00"),
            Price::from("1000.50"),
            Quantity::from("1.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        let source = encode_backtest_data_artifact(&[Data::Bar(bar)]).unwrap();

        let error = engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/internal-bar-v1").unwrap(),
                &source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("aggregation_source must be External")
        );
        assert!(engine.measured_run_inputs.data.is_empty());
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_artifact_strategy_rejects_constructor_which_discards_authenticated_config() {
        let _guard = IGNORING_BACKTEST_CONFIG_TEST_LOCK
            .lock()
            .expect("constructor counter test lock");
        let mut engine = create_engine();
        let configured = StrategyConfig {
            strategy_id: Some(StrategyId::from("AUTHENTICATED-001")),
            ..Default::default()
        };
        let content = serde_json::to_vec(&configured).unwrap();
        IGNORING_BACKTEST_CONFIG_CONSTRUCTOR_CALLS.store(0, Ordering::SeqCst);

        let error = engine
            .add_strategy_from_artifact::<IgnoringBacktestConfigStrategy>(
                StrategyId::from("AUTHENTICATED-001"),
                ArtifactId::new("strategy/ignores-config/v1").unwrap(),
                ArtifactId::new("config/must-be-retained/v1").unwrap(),
                &content,
            )
            .unwrap_err();

        assert!(error.to_string().contains("did not retain"));
        assert!(engine.run_provenance().strategy.is_none());
        assert!(engine.run_provenance().config.is_none());
        assert!(engine.strategy_artifact_attempt_tainted);
        assert_eq!(
            IGNORING_BACKTEST_CONFIG_CONSTRUCTOR_CALLS.load(Ordering::SeqCst),
            1
        );
        engine.clear_strategies().unwrap();
        assert!(engine.strategy_artifact_attempt_tainted);
    }

    #[rstest]
    #[case(ArtifactConfigMutationPhase::Start)]
    #[case(ArtifactConfigMutationPhase::Quote)]
    #[case(ArtifactConfigMutationPhase::Stop)]
    fn test_artifact_configuration_mutation_during_run_fails_completion_and_evidence(
        crypto_perpetual_ethusdt: CryptoPerpetual,
        #[case] phase: ArtifactConfigMutationPhase,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/mutating-config-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        let strategy_id = StrategyId::from("MutatingBacktestArtifact-001");
        let config = serde_json::to_vec(&MutatingBacktestArtifactConfig {
            strategy_id,
            instrument_id,
            phase,
            revision: Cell::new(0),
        })
        .unwrap();
        engine
            .add_strategy_from_artifact::<MutatingBacktestArtifactStrategy>(
                strategy_id,
                ArtifactId::new("strategy/test/mutating-config/v1").unwrap(),
                ArtifactId::new("config/test/mutating-config/v1").unwrap(),
                &config,
            )
            .unwrap();
        engine.require_run_provenance().unwrap();

        let error = engine.run(None, None, None, false).unwrap_err();

        assert!(
            error.to_string().contains("trust-boundary verification"),
            "completion should name retained configuration drift: {error}",
        );
        assert_eq!(
            engine
                .get_result()
                .verify_engine_evidence_seal()
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_artifact_strategy_preflight_rejects_identity_before_constructor() {
        let _guard = IGNORING_BACKTEST_CONFIG_TEST_LOCK
            .lock()
            .expect("constructor counter test lock");
        let mut engine = create_engine();
        let target = StrategyId::from("SUBSTITUTED-001");
        let configured = StrategyConfig {
            strategy_id: Some(target),
            ..Default::default()
        };
        let content = serde_json::to_vec(&configured).unwrap();
        let strategy_artifact_id = ArtifactId::new("strategy/preflight-identity/v1").unwrap();
        let config_artifact_id = ArtifactId::new("config/preflight-identity/v1").unwrap();
        let mut provenance = engine.run_provenance().clone();
        provenance.strategy = Some(
            StrategyBinding::new(
                ArtifactRef::new(
                    strategy_artifact_id.clone(),
                    engine.host_executable_startup_digest,
                ),
                std::any::type_name::<IgnoringBacktestConfigStrategy>(),
                "DECLARED-001",
            )
            .unwrap(),
        );
        provenance.config = Some(ArtifactRef::new(
            config_artifact_id.clone(),
            ContentDigest::digest_bytes(&content),
        ));
        engine.set_run_provenance(provenance).unwrap();
        IGNORING_BACKTEST_CONFIG_CONSTRUCTOR_CALLS.store(0, Ordering::SeqCst);

        let error = engine
            .add_strategy_from_artifact::<IgnoringBacktestConfigStrategy>(
                target,
                strategy_artifact_id,
                config_artifact_id,
                &content,
            )
            .unwrap_err();

        assert!(error.to_string().contains("declared provenance"));
        assert_eq!(
            IGNORING_BACKTEST_CONFIG_CONSTRUCTOR_CALLS.load(Ordering::SeqCst),
            0
        );
        assert!(!engine.strategy_artifact_attempt_tainted);
    }

    #[rstest]
    fn test_environment_artifact_binds_source_and_materialized_universe() {
        let first = create_engine_from_environment(b"environment-version-a");
        let second = create_engine_from_environment(b"environment-version-b");
        let first_binding = first.run_provenance().environment.as_ref().unwrap();
        let second_binding = second.run_provenance().environment.as_ref().unwrap();

        assert_eq!(first_binding.loader(), second_binding.loader());
        assert_eq!(first_binding.venues(), ["BINANCE"]);
        assert_eq!(first_binding.instruments(), ["ETHUSDT-PERP.BINANCE"]);
        let scope = first_binding.scope().unwrap();
        assert_eq!(scope.replay_start_ns(), 0);
        assert_eq!(scope.replay_end_ns(), 10);
        assert_eq!(scope.knowledge_cutoff_ns(), 0);
        assert_eq!(scope.market_scopes().len(), 1);
        assert_eq!(
            scope.market_scopes()[0].market_profile_id(),
            "market-profile/generic-crypto-perpetual/v1"
        );
        assert_eq!(
            scope.market_scopes()[0].listing_venue_id(),
            "listing/exchange/BINANCE"
        );
        assert_eq!(
            scope.market_scopes()[0].execution_route_id(),
            "execution-route/direct-venue/BINANCE"
        );
        assert_ne!(
            first_binding.artifact().digest(),
            second_binding.artifact().digest()
        );
        assert_ne!(
            first.run_provenance().canonical_digest(),
            second.run_provenance().canonical_digest()
        );
    }

    #[rstest]
    fn test_environment_mutation_after_artifact_load_rejects_secure_run(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/environment-mutation-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);
        engine
            .set_settlement_price(Venue::from("BINANCE"), instrument_id, Price::from("999.00"))
            .unwrap();
        engine.require_run_provenance().unwrap();

        let error = engine.run(None, None, None, false).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("execution-environment changes outside the artifact-aware constructor")
        );
        assert_eq!(
            engine
                .get_result()
                .verify_engine_evidence_seal()
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_measured_data_rejects_plain_data_mixing(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/a-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);
        engine
            .add_data(
                vec![Data::Quote(test_quote(
                    instrument_id,
                    "1002.00",
                    "1003.00",
                    2,
                ))],
                None,
                true,
                true,
            )
            .unwrap();
        engine.require_run_provenance().unwrap();

        let error = engine.run(None, None, None, false).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("data loaded outside the artifact-aware path")
        );
        assert_eq!(
            engine
                .get_result()
                .verify_engine_evidence_seal()
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_measured_data_source_tamper_is_rejected_against_declared_provenance(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut first_engine = create_engine();
        let instrument_id = add_test_instrument(&mut first_engine, &crypto_perpetual_ethusdt);
        let original_quote = test_quote(instrument_id, "1000.00", "1001.00", 1);
        let original_source =
            encode_backtest_data_artifact(&[Data::Quote(original_quote)]).unwrap();
        let first = first_engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/source-tamper-v1").unwrap(),
                &original_source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap();
        let divergent_quote = test_quote(instrument_id, "2000.00", "2001.00", 1);
        let tampered_source =
            encode_backtest_data_artifact(&[Data::Quote(divergent_quote)]).unwrap();
        let divergent = planned_measured_quote("dataset/test/source-tamper-v1", 0, divergent_quote);

        assert_ne!(original_source, tampered_source);
        assert_ne!(first.source(), divergent.source());
        assert_eq!(first.decoder(), divergent.decoder());
        assert_eq!(first.replay_ordinal(), divergent.replay_ordinal());
        assert_eq!(first.record_count(), divergent.record_count());
        assert_ne!(first.materialized_digest(), divergent.materialized_digest());
        assert_ne!(first, divergent);

        let mut enforcing_engine = create_engine();
        add_test_instrument(&mut enforcing_engine, &crypto_perpetual_ethusdt);
        let mut provenance = enforcing_engine.run_provenance().clone();
        provenance.data = vec![first];
        enforcing_engine.set_run_provenance(provenance).unwrap();
        let error = enforcing_engine
            .add_data_from_artifact(
                ArtifactId::new("dataset/test/source-tamper-v1").unwrap(),
                &tampered_source,
                ArtifactDataLoadOptions::default(),
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from the declared provenance")
        );
        assert!(enforcing_engine.measured_run_inputs.data.is_empty());
        assert_eq!(enforcing_engine.data_len, 0);
    }

    #[rstest]
    fn test_measured_data_end_rejects_predeclared_batch_that_was_never_loaded(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let dataset_a = add_measured_quote(
            &mut engine,
            "dataset/test/a-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        let dataset_b = planned_measured_quote(
            "dataset/test/b-v1",
            1,
            test_quote(instrument_id, "1002.00", "1003.00", 2),
        );
        add_measured_test_strategy(&mut engine);
        predeclare_dataset(&mut engine, dataset_b);
        engine.require_run_provenance().unwrap();

        engine.run(None, None, None, true).unwrap();
        assert_eq!(engine.measured_run_inputs.data[0].binding, dataset_a);
        assert_eq!(engine.measured_run_inputs.data[0].consumed_records, 1);
        engine.clear_data();
        let error = engine.end_with_result().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Not every declared dataset artifact was measured")
        );
        assert_eq!(
            engine
                .get_result()
                .verify_engine_evidence_seal()
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_measured_data_streaming_succeeds_after_next_batch_is_processed(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let dataset_a = add_measured_quote(
            &mut engine,
            "dataset/test/a-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        let quote_b = test_quote(instrument_id, "1002.00", "1003.00", 2);
        let dataset_b = planned_measured_quote("dataset/test/b-v1", 1, quote_b);
        add_measured_test_strategy(&mut engine);
        predeclare_dataset(&mut engine, dataset_b.clone());
        engine.require_run_provenance().unwrap();

        engine.run(None, None, None, true).unwrap();
        engine.clear_data();
        let loaded_b = add_measured_quote(&mut engine, "dataset/test/b-v1", quote_b);
        assert_eq!(loaded_b, dataset_b);
        engine.run(None, None, None, true).unwrap();
        engine.end_with_result().unwrap();

        let result = engine.get_result();
        assert_eq!(result.provenance.data, vec![dataset_a, dataset_b]);
        result.verify_engine_evidence_seal().unwrap();
    }

    #[rstest]
    fn test_measured_data_incomplete_clear_is_sticky_and_unsealable(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/a-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        let quote_b = test_quote(instrument_id, "1002.00", "1003.00", 2);
        let dataset_b = planned_measured_quote("dataset/test/b-v1", 1, quote_b);
        add_measured_test_strategy(&mut engine);
        predeclare_dataset(&mut engine, dataset_b);
        engine.require_run_provenance().unwrap();

        engine.run(None, None, None, true).unwrap();
        engine.clear_data();
        add_measured_quote(&mut engine, "dataset/test/b-v1", quote_b);
        engine.clear_data();
        assert!(engine.incomplete_artifact_data_evicted);
        let error = engine.end_with_result().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("artifact batch evicted before full consumption")
        );
        assert_eq!(
            engine
                .get_result()
                .verify_engine_evidence_seal()
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_measured_data_final_result_is_frozen_and_second_run_is_denied(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        let dataset = add_measured_quote(
            &mut engine,
            "dataset/test/a-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);
        engine.require_run_provenance().unwrap();
        engine.run(None, None, None, false).unwrap();

        let finalized = engine.get_result();
        let finalized_digest = finalized.evidence_digest();
        let finalized_seal = finalized.engine_evidence_seal_digest();
        assert_eq!(finalized.provenance.data, vec![dataset.clone()]);
        engine.clear_data();
        assert!(engine.run_provenance().data.is_empty());

        let error = engine.run(None, None, None, false).unwrap_err();
        assert!(error.to_string().contains("already finalized"));
        let frozen = engine.get_result();
        assert_eq!(frozen.provenance.data, vec![dataset]);
        assert_eq!(frozen.evidence_digest(), finalized_digest);
        assert_eq!(frozen.engine_evidence_seal_digest(), finalized_seal);
        frozen.verify_engine_evidence_seal().unwrap();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_measured_data_rejects_run_after_public_kernel_escape(
        crypto_perpetual_ethusdt: CryptoPerpetual,
        #[case] mutable_access: bool,
    ) {
        let mut engine = create_engine();
        let instrument_id = add_test_instrument(&mut engine, &crypto_perpetual_ethusdt);
        add_measured_quote(
            &mut engine,
            "dataset/test/a-v1",
            test_quote(instrument_id, "1000.00", "1001.00", 1),
        );
        add_measured_test_strategy(&mut engine);
        engine.require_run_provenance().unwrap();
        if mutable_access {
            let _ = engine.kernel_mut();
        } else {
            let _ = engine.kernel();
        }

        let error = engine.run(None, None, None, false).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("direct access to the mutable system kernel boundary")
        );
        assert_eq!(
            engine
                .get_result()
                .verify_engine_evidence_seal()
                .unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_measured_data_custom_type_fails_closed_without_canonical_codec() {
        let engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();

        let error =
            encode_backtest_data_artifact(&[Data::Custom(stub_custom_data(1, 42, None, None))])
                .unwrap_err();

        assert!(error.to_string().contains("without a canonical codec"));
        assert!(engine.run_provenance().data.is_empty());
        assert!(engine.measured_run_inputs.data.is_empty());
        assert_eq!(engine.data_len, 0);
    }

    #[cfg(feature = "defi")]
    #[rstest]
    fn test_measured_data_defi_type_fails_closed_without_canonical_codec() {
        let engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        let block = Block::new(
            "0x1".to_string(),
            "0x0".to_string(),
            1,
            Ustr::from("miner"),
            30_000_000,
            21_000,
            UnixNanos::from(1),
            Some(Blockchain::Ethereum),
        );

        let error = encode_backtest_data_artifact(&[Data::Defi(Box::new(DefiData::Block(block)))])
            .unwrap_err();

        assert!(error.to_string().contains("without a canonical codec"));
        assert!(engine.run_provenance().data.is_empty());
        assert!(engine.measured_run_inputs.data.is_empty());
        assert_eq!(engine.data_len, 0);
    }

    #[rstest]
    fn test_required_run_provenance_rejects_preclaimed_promotion() {
        let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        engine.require_run_provenance().unwrap();
        engine
            .set_run_provenance(RunProvenance {
                data: vec![provenance_dataset("dataset/btc-usdt")],
                environment: Some(provenance_environment()),
                strategy: Some(provenance_strategy("strategy/momentum")),
                config: Some(provenance_ref("config/research")),
                promotion: Some(provenance_ref("promotion/not-yet-earned")),
                ..Default::default()
            })
            .unwrap();

        assert!(
            engine
                .validate_run_provenance()
                .unwrap_err()
                .to_string()
                .contains("cannot claim a promotion")
        );
    }

    #[rstest]
    fn test_get_result_before_success_is_unsealed() {
        let engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        let result = engine.get_result();

        assert_eq!(
            result.verify_engine_evidence_seal().unwrap_err(),
            crate::evidence::BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn test_successful_run_issues_engine_evidence_seal() {
        let mut engine = create_engine();
        engine.run(None, None, None, false).unwrap();

        engine.get_result().verify_engine_evidence_seal().unwrap();
    }

    #[rstest]
    fn test_add_duplicate_venue_preserves_original_exchange(
        crypto_perpetual_ethusdt: CryptoPerpetual,
    ) {
        let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        let venue = Venue::from("BINANCE");
        let venue_config = SimulatedVenueConfig::builder()
            .venue(venue)
            .oms_type(OmsType::Netting)
            .account_type(AccountType::Margin)
            .book_type(BookType::L1_MBP)
            .starting_balances(vec![Money::from("1_000_000 USDT")])
            .build()
            .unwrap();
        engine.add_venue(venue_config).unwrap();

        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt);
        let instrument_id = instrument.id();
        engine.add_instrument(&instrument).unwrap();

        let initial_quote = QuoteTick::new(
            instrument_id,
            Price::from("1000.00"),
            Price::from("1001.00"),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        msgbus::send_quote(
            format!("SimulatedExchange.process_new_quote.{venue}").into(),
            &initial_quote,
        );

        let best_bid_before = engine
            .venues
            .get(&venue)
            .unwrap()
            .borrow()
            .best_bid_price(instrument_id);
        let best_ask_before = engine
            .venues
            .get(&venue)
            .unwrap()
            .borrow()
            .best_ask_price(instrument_id);
        let original_exchange = Rc::downgrade(engine.venues.get(&venue).unwrap());
        let venues_before = engine.list_venues();
        let exec_clients_len_before = engine.exec_clients.len();
        let client_ids_before = engine.kernel.exec_engine.borrow().client_ids();
        let duplicate_config = SimulatedVenueConfig::builder()
            .venue(venue)
            .oms_type(OmsType::Netting)
            .account_type(AccountType::Margin)
            .book_type(BookType::L1_MBP)
            .starting_balances(vec![Money::from("1_000_000 USDT")])
            .build()
            .unwrap();
        assert!(engine.add_venue(duplicate_config).is_err());

        let original_exchange = original_exchange
            .upgrade()
            .expect("the original exchange must remain alive");
        assert!(Rc::ptr_eq(
            &original_exchange,
            engine.venues.get(&venue).unwrap()
        ));
        assert_eq!(engine.list_venues(), venues_before);
        assert_eq!(engine.exec_clients.len(), exec_clients_len_before);
        assert_eq!(
            engine.kernel.exec_engine.borrow().client_ids(),
            client_ids_before
        );

        let distinct_quote = QuoteTick::new(
            instrument_id,
            Price::from("2000.00"),
            Price::from("2001.00"),
            Quantity::from("2.000"),
            Quantity::from("2.000"),
            UnixNanos::from(2),
            UnixNanos::from(2),
        );
        msgbus::send_quote(
            format!("SimulatedExchange.process_new_quote.{venue}").into(),
            &distinct_quote,
        );

        let original_exchange = original_exchange.borrow();
        let best_bid_after = original_exchange.best_bid_price(instrument_id);
        let best_ask_after = original_exchange.best_ask_price(instrument_id);
        assert_ne!(best_bid_after, best_bid_before);
        assert_ne!(best_ask_after, best_ask_before);
        assert_eq!(best_bid_after, Some(Price::from("2000.00")));
        assert_eq!(best_ask_after, Some(Price::from("2001.00")));
    }

    #[rstest]
    fn test_add_venue_execution_registration_failure_publishes_nothing() {
        let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        let venue = Venue::from("SIM");
        engine
            .kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                ClientId::from(venue.as_str()),
                AccountId::from("SIM-001"),
                venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        let client_ids_before = engine.kernel.exec_engine.borrow().client_ids();

        let endpoint = format!("SimulatedExchange.process_new_quote.{venue}");
        let received_quotes = Rc::new(RefCell::new(Vec::new()));
        let received_quotes_handler = Rc::clone(&received_quotes);
        let sentinel = TypedHandler::from_with_id("venue-setup-sentinel", move |quote| {
            received_quotes_handler.borrow_mut().push(*quote);
        });
        msgbus::register_quote_endpoint(endpoint.as_str().into(), sentinel);

        let venue_config = SimulatedVenueConfig::builder()
            .venue(venue)
            .oms_type(OmsType::Netting)
            .account_type(AccountType::Margin)
            .book_type(BookType::L1_MBP)
            .starting_balances(vec![Money::from("1_000_000 USD")])
            .build()
            .unwrap();
        assert!(engine.add_venue(venue_config).is_err());

        assert!(!engine.venues.contains_key(&venue));
        assert!(engine.exec_clients.is_empty());
        assert_eq!(
            engine.kernel.exec_engine.borrow().client_ids(),
            client_ids_before
        );

        let quote = QuoteTick::new(
            InstrumentId::from("TEST.SIM"),
            Price::from("100.00"),
            Price::from("101.00"),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        msgbus::send_quote(endpoint.as_str().into(), &quote);
        assert_eq!(received_quotes.borrow().as_slice(), &[quote]);
    }

    #[rstest]
    fn test_add_strategy_registers_configured_hedging_oms_type() {
        let mut engine = create_engine();
        let instrument = crypto_perpetual_ethusdt();
        let strategy_id = StrategyId::from("FUNDING_ARBITRAGE-001");

        engine
            .add_instrument(&InstrumentAny::CryptoPerpetual(instrument.clone()))
            .unwrap();
        engine
            .add_strategy(TestStrategy::new(StrategyConfig {
                strategy_id: Some(strategy_id),
                oms_type: Some(OmsType::Hedging),
                ..Default::default()
            }))
            .unwrap();

        let order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(engine.trader_id())
            .strategy_id(strategy_id)
            .instrument_id(instrument.id())
            .quantity(Quantity::from("1.000"))
            .build();
        let position_id = PositionId::new("CUSTOM-POSITION-001");

        engine
            .kernel
            .exec_engine
            .borrow()
            .cache()
            .borrow_mut()
            .add_order(
                order.clone(),
                Some(position_id),
                Some(ClientId::from("BINANCE")),
                true,
            )
            .unwrap();

        let submit_order = SubmitOrder::new(
            order.trader_id(),
            Some(ClientId::from("BINANCE")),
            strategy_id,
            instrument.id(),
            order.client_order_id(),
            order.init_event().clone(),
            order.exec_algorithm_id(),
            Some(position_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );

        engine
            .kernel
            .exec_engine
            .borrow()
            .execute(TradingCommand::SubmitOrder(submit_order));

        let exec_engine = engine.kernel.exec_engine.borrow();
        let cache = exec_engine.cache().borrow();
        let cached_order = cache
            .order(&order.client_order_id())
            .expect("Order should be cached");

        assert_eq!(cached_order.status(), OrderStatus::Initialized);
    }

    fn create_engine_with_replay_store(fail_restore: bool, halted: bool) -> BacktestEngine {
        let config = BacktestEngineConfig {
            load_state: true,
            run_analysis: false,
            ..Default::default()
        };
        let mut engine = BacktestEngine::new(config.clone()).unwrap();
        let event_store_factory = move |_instance_id: UUID4, _clock: Rc<RefCell<dyn Clock>>| {
            Ok::<_, anyhow::Error>(Box::new(BacktestReplayKernelEventStore {
                fail_restore,
                halted,
            }) as Box<dyn KernelEventStore>)
        };

        engine.kernel = NautilusKernel::new_with(
            "BacktestEngine".to_string(),
            config,
            None,
            Some(Box::new(event_store_factory)),
        )
        .unwrap();
        engine.instance_id = engine.kernel.instance_id;
        engine
    }

    fn create_stop_market_order(instrument: &CryptoPerpetual) -> OrderAny {
        OrderTestBuilder::new(OrderType::StopMarket)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .trigger_price(Price::from("5100.00"))
            .quantity(Quantity::from(1))
            .emulation_trigger(TriggerType::BidAsk)
            .build()
    }

    fn create_submit_order_command(order: &OrderAny) -> SubmitOrder {
        SubmitOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.init_event().clone(),
            order.exec_algorithm_id(),
            None,
            None,
            UUID4::new(),
            0.into(),
            None, // correlation_id
        )
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
    fn test_run_impl_event_store_replay_skips_trader_start() {
        let mut engine = create_engine_with_replay_store(false, false);

        engine
            .run_impl(
                Some(UnixNanos::from(0)),
                Some(UnixNanos::from(1)),
                None,
                true,
            )
            .unwrap();

        assert!(engine.kernel.is_event_store_replay_configured());
        assert!(engine.kernel.is_event_store_replay());
        assert!(!engine.kernel.trader.borrow().is_running());
    }

    #[rstest]
    fn test_run_impl_event_store_replay_config_failure_errors() {
        let mut engine = create_engine_with_replay_store(true, false);

        let error = engine
            .run_impl(
                Some(UnixNanos::from(0)),
                Some(UnixNanos::from(1)),
                None,
                true,
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "kernel startup requested terminal shutdown; event-store startup may have failed"
        );
        assert!(engine.kernel.is_event_store_replay_configured());
        assert!(!engine.kernel.is_event_store_replay());
        assert!(!engine.kernel.trader.borrow().is_running());
    }

    #[rstest]
    fn test_event_store_halt_terminates_backtest_before_trader_start() {
        let mut engine = create_engine_with_replay_store(false, true);

        let error = engine
            .run(
                Some(UnixNanos::from(0)),
                Some(UnixNanos::from(1)),
                None,
                true,
            )
            .expect_err("halted event store must terminate the backtest");

        assert!(
            error
                .to_string()
                .contains("kernel startup requested terminal shutdown"),
            "unexpected error: {error:#}",
        );
        assert!(engine.kernel.is_event_store_halted());
        assert!(engine.kernel.is_shutdown_requested());
        assert!(engine.force_stop);
        assert!(engine.run_finished.is_some());
        assert!(!engine.kernel.trader.borrow().is_running());
    }

    #[rstest]
    fn test_backtest_state_persistence_loads_before_start_and_saves_after_settle() {
        let actor_id = ActorId::from("BACKTEST-STATE-ACTOR");
        let strategy_id = StrategyId::from("BACKTEST-STATE-STRATEGY-001");
        let actor_load = IndexMap::from([("actor-load".to_string(), b"actor-loaded".to_vec())]);
        let strategy_load =
            IndexMap::from([("strategy-load".to_string(), b"strategy-loaded".to_vec())]);
        let actor_save = IndexMap::from([("actor-save".to_string(), b"actor-saved".to_vec())]);
        let strategy_save =
            IndexMap::from([("strategy-save".to_string(), b"strategy-saved".to_vec())]);
        let (database, control) = TestCacheDatabaseControl::create();
        control.set_actor_state(ComponentId::from(actor_id.as_str()), &actor_load);
        control.set_strategy_state(strategy_id, &strategy_load);
        let config = BacktestEngineConfig {
            load_state: true,
            save_state: true,
            run_analysis: false,
            ..Default::default()
        };
        let mut engine = BacktestEngine::new(config).unwrap();
        engine
            .kernel
            .cache
            .borrow_mut()
            .set_database(Box::new(database));
        engine
            .add_actor(StateActor::new(
                actor_id,
                control.clone(),
                actor_save.clone(),
            ))
            .unwrap();
        engine
            .add_strategy(StateStrategy::new(
                strategy_id,
                control.clone(),
                strategy_save.clone(),
            ))
            .unwrap();

        engine
            .run(
                Some(UnixNanos::from(0)),
                Some(UnixNanos::from(1)),
                None,
                false,
            )
            .unwrap();
        engine.dispose();

        assert_eq!(
            control.events(),
            vec![
                "actor.load:BACKTEST-STATE-ACTOR",
                "actor.on_load",
                "strategy.load:BACKTEST-STATE-STRATEGY-001",
                "strategy.on_load",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:BACKTEST-STATE-ACTOR",
                "strategy.on_save",
                "strategy.update:BACKTEST-STATE-STRATEGY-001",
                "database.close",
            ]
        );
        assert_eq!(
            control.actor_state(&ComponentId::from(actor_id.as_str())),
            Some(actor_save)
        );
        assert_eq!(control.strategy_state(&strategy_id), Some(strategy_save));
        assert_eq!(engine.backtest_end, Some(UnixNanos::from(0)));
    }

    #[rstest]
    fn test_backtest_state_persistence_reports_callback_errors_after_shutdown() {
        let actor_id = ActorId::from("BACKTEST-FAIL-SAVE-ACTOR");
        let strategy_id = StrategyId::from("BACKTEST-FAIL-SAVE-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();
        let config = BacktestEngineConfig {
            save_state: true,
            run_analysis: false,
            ..Default::default()
        };
        let mut engine = BacktestEngine::new(config).unwrap();
        engine
            .kernel
            .cache
            .borrow_mut()
            .set_database(Box::new(database));
        engine
            .add_actor(StateActor::new(actor_id, control.clone(), IndexMap::new()).with_fail_save())
            .unwrap();
        engine
            .add_strategy(
                StateStrategy::new(strategy_id, control.clone(), IndexMap::new()).with_fail_save(),
            )
            .unwrap();

        let error = engine
            .run(
                Some(UnixNanos::from(0)),
                Some(UnixNanos::from(1)),
                None,
                false,
            )
            .unwrap_err();
        engine.dispose();

        assert_eq!(
            error.to_string(),
            "Failed to save component state: actor BACKTEST-FAIL-SAVE-ACTOR callback: test actor \
             on_save failure; strategy BACKTEST-FAIL-SAVE-STRATEGY-001 callback: test strategy \
             on_save failure"
        );
        assert_eq!(
            control.events(),
            vec![
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "strategy.on_save",
                "database.close",
            ]
        );
        assert!(!engine.kernel.trader.borrow().is_running());
        assert_eq!(engine.backtest_end, Some(UnixNanos::from(0)));
    }

    #[rstest]
    #[case(None)]
    #[case(Some(true))]
    #[case(Some(false))]
    fn test_new_forces_drop_instruments_on_reset_false(
        crypto_perpetual_ethusdt: CryptoPerpetual,
        #[case] user_value: Option<bool>,
    ) {
        use nautilus_common::cache::CacheConfig;

        let config = match user_value {
            None => BacktestEngineConfig::builder().build(),
            Some(value) => BacktestEngineConfig::builder()
                .cache(
                    CacheConfig::builder()
                        .drop_instruments_on_reset(value)
                        .build()
                        .unwrap(),
                )
                .build(),
        };
        let mut engine = BacktestEngine::new(config).unwrap();

        let venue_config = SimulatedVenueConfig::builder()
            .venue(Venue::from("BINANCE"))
            .oms_type(OmsType::Netting)
            .account_type(AccountType::Margin)
            .book_type(BookType::L1_MBP)
            .starting_balances(vec![Money::from("1_000_000 USDT")])
            .build()
            .unwrap();
        engine.add_venue(venue_config).unwrap();

        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt);
        let instrument_id = instrument.id();
        engine.add_instrument(&instrument).unwrap();

        engine.reset();

        assert!(
            engine
                .kernel()
                .cache
                .borrow()
                .instrument(&instrument_id)
                .is_some(),
            "instrument must survive engine.reset(); user-supplied \
             drop_instruments_on_reset={user_value:?} must not leak through",
        );
    }

    #[rstest]
    fn test_reset_resets_order_emulator_state(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let data_commands =
            register_data_command_handler("DataEngine.queue_execute.backtest_reset");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt.clone());
        let instrument_id = instrument.id();
        engine.add_instrument(&instrument).unwrap();
        let order = create_stop_market_order(&crypto_perpetual_ethusdt);
        let command = create_submit_order_command(&order);
        engine
            .kernel
            .cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();
        let order_emulator = engine.kernel.order_emulator.emulator();
        let mut order_emulator = order_emulator.borrow_mut();
        order_emulator.cache_submit_order_command(command.clone());
        order_emulator.handle_submit_order(&command);
        drop(order_emulator);
        data_commands.clear();

        engine.reset();

        let commands = data_commands.get_messages();
        let emulator = engine.kernel.order_emulator.get_emulator();
        assert!(emulator.subscribed_quotes().is_empty());
        assert!(emulator.subscribed_trades().is_empty());
        assert!(emulator.get_matching_core(&instrument_id).is_none());
        assert!(commands.iter().any(|command| matches!(
            command,
            DataCommand::Unsubscribe(UnsubscribeCommand::Quotes(command))
                if command.instrument_id == instrument_id
        )));
    }

    #[rstest]
    fn test_route_data_to_exchange_instrument_status(crypto_perpetual_ethusdt: CryptoPerpetual) {
        let mut engine = create_engine();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt);
        let instrument_id = instrument.id();
        engine.add_instrument(&instrument).unwrap();

        let status = InstrumentStatus::new(
            instrument_id,
            MarketStatusAction::Close,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
            None,
            None,
            None,
            None,
        );

        engine.route_data_to_exchange(&Data::InstrumentStatus(status));

        let exchange = engine.venues.get(&instrument_id.venue).unwrap().borrow();
        let market_status = exchange
            .get_matching_engine(&instrument_id)
            .unwrap()
            .market_status;
        assert_eq!(market_status, MarketStatus::Closed);
    }
}
