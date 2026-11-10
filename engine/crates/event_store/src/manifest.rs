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

//! The per-run manifest stored alongside captured entries.

use indexmap::IndexMap;
use nautilus_core::UnixNanos;
use nautilus_provenance::{ContentDigest, RunProvenance};
pub use nautilus_system::{
    RegisteredComponents,
    event_store::{EVENT_STORE_ENTRY_SCHEMA_VERSION, RunId},
};
use serde::{Deserialize, Serialize};

use crate::{entry::EventStoreEntry, error::EventStoreError, wire};

/// Current run-manifest envelope version (`NRMF ++ version ++ codec-frame`).
pub const RUN_MANIFEST_WIRE_VERSION: u8 = 3;

/// Current `RunStarted` payload version.
pub const RUN_STARTED_PAYLOAD_VERSION: u8 = 2;

/// Canonical lifecycle topic required for the first entry of every run.
pub(crate) const RUN_STARTED_TOPIC: &str = "run.lifecycle.RunStarted";

/// Canonical payload type required for the first entry of every run.
pub(crate) const RUN_STARTED_PAYLOAD_TYPE: &str = "RunStarted";

/// Canonical lifecycle topic required for the terminal entry of a cleanly ended run.
pub(crate) const RUN_ENDED_TOPIC: &str = "run.lifecycle.RunEnded";

/// Canonical payload type required for the terminal entry of a cleanly ended run.
pub(crate) const RUN_ENDED_PAYLOAD_TYPE: &str = "RunEnded";

/// Returns whether a topic is reserved for kernel-owned lifecycle envelopes.
#[must_use]
pub(crate) fn is_lifecycle_topic(topic: &str) -> bool {
    matches!(topic, RUN_STARTED_TOPIC | RUN_ENDED_TOPIC)
}

/// Returns whether a payload tag is reserved for kernel-owned lifecycle envelopes.
#[must_use]
pub(crate) fn is_lifecycle_payload_type(payload_type: &str) -> bool {
    matches!(
        payload_type,
        RUN_STARTED_PAYLOAD_TYPE | RUN_ENDED_PAYLOAD_TYPE
    )
}

/// Returns whether `entry` is the canonical empty terminal envelope emitted by the kernel.
///
/// Sequence placement and sidecar-index absence are validated by the verifier/recovery caller,
/// which owns the durable HWM and index tables.
#[must_use]
pub(crate) fn is_canonical_run_ended_envelope(entry: &EventStoreEntry) -> bool {
    entry.topic.as_ref() == RUN_ENDED_TOPIC
        && entry.payload_type.as_str() == RUN_ENDED_PAYLOAD_TYPE
        && entry.payload.is_empty()
        && entry.headers.is_empty()
}

/// Returns whether `entry` is the canonical first envelope committed for `manifest`.
///
/// Sidecar-index absence is validated by callers because indices are stored outside the
/// authoritative entry envelope.
#[must_use]
pub(crate) fn is_canonical_run_started_envelope(
    entry: &EventStoreEntry,
    manifest: &RunManifest,
) -> bool {
    if entry.seq != 1
        || entry.topic.as_ref() != RUN_STARTED_TOPIC
        || entry.payload_type.as_str() != RUN_STARTED_PAYLOAD_TYPE
        || !entry.headers.is_empty()
        || entry.ts_init != manifest.start_ts_init
        || !manifest.registered_components.is_canonical()
    {
        return false;
    }

    crate::codec::decode_from_slice::<RunStartedPayload>(&entry.payload).is_ok_and(|payload| {
        payload.version() == RUN_STARTED_PAYLOAD_VERSION
            && payload.header() == RunStartedPayload::from_manifest(manifest).header()
    })
}

/// Returns whether `entry` is the canonical terminal envelope at `high_watermark`.
#[must_use]
pub(crate) fn is_canonical_run_ended_at_hwm(entry: &EventStoreEntry, high_watermark: u64) -> bool {
    high_watermark >= 2 && entry.seq == high_watermark && is_canonical_run_ended_envelope(entry)
}

/// Lifecycle state of a captured run.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RunStatus {
    /// The run is open and accepting writes.
    Running,
    /// The run was sealed by graceful shutdown after a `RunEnded` entry.
    Ended,
    /// The run was sealed on boot after the writer found no `RunEnded` entry.
    CrashedRecovered,
    /// The run failed an integrity check and is unsafe to replay.
    Quarantined,
}

/// Per-run manifest persisted alongside captured entries.
///
/// Every field is recorded at run start; `end_ts_init`, `high_watermark`, and `status` are
/// updated when the run is sealed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunManifest {
    /// The id of this run.
    pub run_id: RunId,
    /// The id of the predecessor run that this run resumes from, if any.
    pub parent_run_id: Option<RunId>,
    /// The id of the trading instance owning this run.
    pub instance_id: String,
    /// A hex-encoded hash of the trader binary.
    pub binary_hash: String,
    /// Bumps when the entry payload schema changes.
    pub schema_version: u32,
    /// A hex-encoded hash of `Cargo.lock` or an equivalent crate version manifest.
    pub crate_versions: String,
    /// The active Cargo features for the trader binary.
    pub feature_flags: Vec<String>,
    /// Per-adapter version stamp keyed by adapter name.
    pub adapter_versions: IndexMap<String, String>,
    /// A hex-encoded hash of the kernel configuration.
    pub config_hash: String,
    /// Registered actor, strategy, and algorithm identities along with subscription bindings.
    pub registered_components: RegisteredComponents,
    /// The deterministic seed, populated when the run executes under a seeded mode.
    pub seed: Option<u64>,
    /// The first `ts_init` observed by the writer for this run.
    #[serde(with = "wire::nanos_as_u64")]
    pub start_ts_init: UnixNanos,
    /// The last `ts_init` observed by the writer for this run, populated on seal.
    #[serde(with = "wire::opt_nanos_as_u64")]
    pub end_ts_init: Option<UnixNanos>,
    /// The largest `seq` durably acknowledged by the backend at end of run.
    pub high_watermark: u64,
    /// The lifecycle state of this run.
    pub status: RunStatus,
    /// Immutable data, strategy, model, policy, evaluation, and promotion evidence.
    pub provenance: RunProvenance,
}

/// Immutable manifest header committed by the first `RunStarted` entry.
///
/// Mutable lifecycle fields (`end_ts_init`, `high_watermark`, and `status`) are deliberately
/// excluded. Every other manifest field is copied verbatim, except provenance which is bound by
/// its domain-separated canonical digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunHeaderCommitment {
    run_id: RunId,
    parent_run_id: Option<RunId>,
    instance_id: String,
    binary_hash: String,
    manifest_wire_version: u8,
    entry_schema_version: u32,
    crate_versions: String,
    feature_flags: Vec<String>,
    adapter_versions: IndexMap<String, String>,
    config_hash: String,
    registered_components: RegisteredComponents,
    seed: Option<u64>,
    #[serde(with = "wire::nanos_as_u64")]
    start_ts_init: UnixNanos,
    provenance_digest: ContentDigest,
}

impl RunHeaderCommitment {
    /// Recomputes the immutable header commitment for `manifest`.
    #[must_use]
    pub fn from_manifest(manifest: &RunManifest) -> Self {
        Self {
            run_id: manifest.run_id.clone(),
            parent_run_id: manifest.parent_run_id.clone(),
            instance_id: manifest.instance_id.clone(),
            binary_hash: manifest.binary_hash.clone(),
            manifest_wire_version: RUN_MANIFEST_WIRE_VERSION,
            entry_schema_version: manifest.schema_version,
            crate_versions: manifest.crate_versions.clone(),
            feature_flags: manifest.feature_flags.clone(),
            adapter_versions: manifest.adapter_versions.clone(),
            config_hash: manifest.config_hash.clone(),
            registered_components: manifest.registered_components.clone(),
            seed: manifest.seed,
            start_ts_init: manifest.start_ts_init,
            provenance_digest: manifest.provenance.canonical_digest(),
        }
    }
}

/// Versioned payload stored in the first `RunStarted` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStartedPayload {
    version: u8,
    header: RunHeaderCommitment,
}

impl RunStartedPayload {
    /// Builds the current payload for `manifest`.
    #[must_use]
    pub fn from_manifest(manifest: &RunManifest) -> Self {
        Self {
            version: RUN_STARTED_PAYLOAD_VERSION,
            header: RunHeaderCommitment::from_manifest(manifest),
        }
    }

    /// Returns the payload wire version.
    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// Returns the immutable manifest header commitment.
    #[must_use]
    pub const fn header(&self) -> &RunHeaderCommitment {
        &self.header
    }
}

impl RunManifest {
    /// Returns `true` once `status` is anything other than [`RunStatus::Running`].
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        !matches!(self.status, RunStatus::Running)
    }

    /// Rejects manifests whose entry payload schema is not exactly supported.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::UnsupportedSchemaVersion`] for older or newer schemas.
    pub const fn validate_schema_version(&self) -> Result<(), EventStoreError> {
        if self.schema_version == EVENT_STORE_ENTRY_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(EventStoreError::UnsupportedSchemaVersion {
                found: self.schema_version,
                supported: EVENT_STORE_ENTRY_SCHEMA_VERSION,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn manifest_with(status: RunStatus) -> RunManifest {
        RunManifest {
            run_id: "1700000000-abcd1234".to_string(),
            parent_run_id: None,
            instance_id: "trader-001".to_string(),
            binary_hash: "deadbeef".to_string(),
            schema_version: 1,
            crate_versions: "feedface".to_string(),
            feature_flags: vec!["live".to_string()],
            adapter_versions: IndexMap::new(),
            config_hash: "cafebabe".to_string(),
            provenance: RunProvenance::default(),
            registered_components: RegisteredComponents::default(),
            seed: None,
            start_ts_init: UnixNanos::from(0),
            end_ts_init: None,
            high_watermark: 0,
            status,
        }
    }

    #[rstest]
    #[case(RunStatus::Running, false)]
    #[case(RunStatus::Ended, true)]
    #[case(RunStatus::CrashedRecovered, true)]
    #[case(RunStatus::Quarantined, true)]
    fn is_sealed_matches_status(#[case] status: RunStatus, #[case] expected: bool) {
        assert_eq!(manifest_with(status).is_sealed(), expected);
    }

    #[rstest]
    fn run_started_header_commits_nonempty_component_identity_inventory() {
        let empty = manifest_with(RunStatus::Running);
        let mut populated = empty.clone();
        populated.registered_components = RegisteredComponents {
            actors: vec!["Actor-001".to_string()],
            strategies: vec!["Strategy-001".to_string()],
            algorithms: vec!["Algo-001".to_string()],
        };

        let empty_payload = RunStartedPayload::from_manifest(&empty);
        let populated_payload = RunStartedPayload::from_manifest(&populated);

        assert_eq!(populated_payload.version(), RUN_STARTED_PAYLOAD_VERSION);
        assert_ne!(empty_payload.header(), populated_payload.header());
        assert_eq!(
            populated_payload.header().registered_components,
            populated.registered_components
        );
    }
}
