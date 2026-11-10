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

//! Off-trader verifier that proves a run file's integrity before the trader opens it.
//!
//! See `README.md` "Storage backend" and "Determinism contract" sections for the SPEC
//! posture: redb 4.x does not framewise-checksum data pages, so a `zero-tail` corruption
//! opens cleanly and panics on first read. The verifier therefore exercises every entry
//! and every stored index pair, accumulating findings so a single run produces one
//! actionable report rather than failing fast on the first hit. The supervisor runs the
//! verifier in an isolated process so a bad file aborts the verifier, not trading.
//!
//! Scope:
//!
//! - Walk every `seq` over `[1, high_watermark]` and recompute [`crate::EntryHash`].
//! - Detect gaps in the seq sequence (the SPEC's gap-detection idempotency primitive).
//! - Validate that every `client_order_id` and `venue_order_id` stored target seq still
//!   resolves to a clean entry; full payload-derived rebuild is deferred until the
//!   wrapper-type encoders land.
//! - Validate manifest invariants: `high_watermark` matches the durable last seq, the
//!   recorded `start_ts_init` and `end_ts_init` bracket the entry stream, and a sealed
//!   manifest's status is a terminal state.
//!
//! The library API stays narrow: a single [`Verifier`] type that owns a backend, a
//! [`VerifyReport`] structured for downstream operator tooling, and a [`VerifyError`]
//! reserved for failures that prevent the verifier from producing any report at all.

use std::{fmt::Debug, ops::Deref, path::Path};

use nautilus_provenance::{ContentDigest, ContentDigestBuilder};

use crate::{
    backend::{EventStore, IndexKind, RedbBackend},
    codec,
    entry::EventStoreEntry,
    error::EventStoreError,
    manifest::{
        RUN_STARTED_PAYLOAD_TYPE, RUN_STARTED_PAYLOAD_VERSION, RUN_STARTED_TOPIC, RunId,
        RunManifest, RunStartedPayload, RunStatus, is_canonical_run_ended_at_hwm,
        is_canonical_run_started_envelope, is_lifecycle_payload_type, is_lifecycle_topic,
    },
    snapshot::SnapshotAnchor,
};

const VERIFIED_RUN_DIGEST_DOMAIN: &[u8] = b"nautilus-event-store/verified-run/v1";

/// Default maximum durable sequence inspected by one verifier run.
pub const DEFAULT_VERIFY_MAX_HIGH_WATERMARK: u64 = 1_000_000;
/// Default maximum number of durable entry rows inspected by one verifier run.
pub const DEFAULT_VERIFY_MAX_ENTRIES: u64 = 1_000_000;
/// Default maximum combined client/venue sidecar-index rows inspected by one verifier run.
pub const DEFAULT_VERIFY_MAX_INDEX_ENTRIES: u64 = 2_000_000;
/// Default largest encoded entry row decoded by the verifier (16 MiB).
pub const DEFAULT_VERIFY_MAX_ENTRY_BYTES: u64 = 16 * 1024 * 1024;
/// Default cumulative encoded entry bytes decoded during one verification (4 GiB).
pub const DEFAULT_VERIFY_MAX_TOTAL_ENTRY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Default cumulative UTF-8 sidecar-index key bytes inspected (256 MiB).
pub const DEFAULT_VERIFY_MAX_TOTAL_INDEX_KEY_BYTES: u64 = 256 * 1024 * 1024;
/// Default maximum findings retained in a report.
pub const DEFAULT_VERIFY_MAX_FINDINGS: u64 = 10_000;
/// Default largest encoded snapshot-anchor row decoded (4 MiB).
pub const DEFAULT_VERIFY_MAX_SNAPSHOT_ANCHOR_BYTES: u64 = 4 * 1024 * 1024;
/// Default largest encoded run-manifest row decoded (4 MiB).
pub const DEFAULT_VERIFY_MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

/// Hard read-side resource budgets applied before and during verification.
///
/// Defaults intentionally bound attacker-controlled loops and allocations. Large trusted
/// research runs can opt into higher values explicitly; accepting them is a deployment policy
/// decision rather than an implicit consequence of an on-disk high-watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifyLimits {
    /// Largest durable high-watermark whose complete sequence space may be scanned.
    pub max_high_watermark: u64,
    /// Largest number of stored entry rows whose payloads may be inspected.
    pub max_entries: u64,
    /// Largest combined number of client- and venue-order sidecar rows.
    pub max_index_entries: u64,
    /// Largest sealed redb file that may be opened.
    pub max_file_bytes: u64,
    /// Largest encoded entry row which may be decoded.
    pub max_entry_bytes: u64,
    /// Largest cumulative encoded entry bytes decoded by one scan.
    pub max_total_entry_bytes: u64,
    /// Largest cumulative UTF-8 bytes across sidecar-index keys.
    pub max_total_index_key_bytes: u64,
    /// Largest number of findings retained before verification fails closed.
    pub max_findings: u64,
    /// Largest encoded snapshot-anchor row which may be decoded.
    pub max_snapshot_anchor_bytes: u64,
    /// Largest encoded run-manifest row which may be decoded.
    pub max_manifest_bytes: u64,
}

impl VerifyLimits {
    /// Constructs explicit verification budgets.
    #[must_use]
    pub const fn new(
        max_high_watermark: u64,
        max_entries: u64,
        max_index_entries: u64,
        max_file_bytes: u64,
    ) -> Self {
        Self {
            max_high_watermark,
            max_entries,
            max_index_entries,
            max_file_bytes,
            max_entry_bytes: DEFAULT_VERIFY_MAX_ENTRY_BYTES,
            max_total_entry_bytes: DEFAULT_VERIFY_MAX_TOTAL_ENTRY_BYTES,
            max_total_index_key_bytes: DEFAULT_VERIFY_MAX_TOTAL_INDEX_KEY_BYTES,
            max_findings: DEFAULT_VERIFY_MAX_FINDINGS,
            max_snapshot_anchor_bytes: DEFAULT_VERIFY_MAX_SNAPSHOT_ANCHOR_BYTES,
            max_manifest_bytes: DEFAULT_VERIFY_MAX_MANIFEST_BYTES,
        }
    }
}

impl Default for VerifyLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_VERIFY_MAX_HIGH_WATERMARK,
            DEFAULT_VERIFY_MAX_ENTRIES,
            DEFAULT_VERIFY_MAX_INDEX_ENTRIES,
            crate::backend::redb::DEFAULT_MAX_SEALED_FILE_BYTES,
        )
    }
}

/// Resource dimension whose verification budget was exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VerifyLimitResource {
    /// Durable sequence space (`1..=high_watermark`).
    HighWatermark,
    /// Stored entry rows.
    Entries,
    /// Combined client/venue sidecar-index rows.
    IndexEntries,
    /// Sealed redb file bytes.
    FileBytes,
    /// One encoded entry row.
    EntryBytes,
    /// Cumulative encoded entry bytes.
    TotalEntryBytes,
    /// Cumulative UTF-8 sidecar-index key bytes.
    TotalIndexKeyBytes,
    /// Findings retained in memory/output.
    Findings,
    /// Encoded snapshot-anchor bytes.
    SnapshotAnchorBytes,
    /// Encoded run-manifest bytes.
    ManifestBytes,
}

impl std::fmt::Display for VerifyLimitResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::HighWatermark => "high-watermark",
            Self::Entries => "entries",
            Self::IndexEntries => "index entries",
            Self::FileBytes => "file bytes",
            Self::EntryBytes => "entry bytes",
            Self::TotalEntryBytes => "total entry bytes",
            Self::TotalIndexKeyBytes => "total index key bytes",
            Self::Findings => "findings",
            Self::SnapshotAnchorBytes => "snapshot anchor bytes",
            Self::ManifestBytes => "manifest bytes",
        })
    }
}

/// Verifier over a single open run.
///
/// Constructed either by passing an already-open backend ([`Verifier::new`]) or by
/// opening a sealed redb file directly ([`Verifier::open_redb`] or
/// [`Verifier::open_redb_file`]). The verifier never mutates the backend; it walks the
/// entry table and the secondary indices, then emits a typed [`VerifyReport`].
enum VerifierBackend {
    Erased(Box<dyn EventStore>),
    Redb(Box<RedbBackend>),
}

impl Deref for VerifierBackend {
    type Target = dyn EventStore;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Erased(backend) => backend.as_ref(),
            Self::Redb(backend) => backend.as_ref(),
        }
    }
}

impl AsRef<dyn EventStore> for VerifierBackend {
    fn as_ref(&self) -> &(dyn EventStore + 'static) {
        match self {
            Self::Erased(backend) => backend.as_ref(),
            Self::Redb(backend) => backend.as_ref(),
        }
    }
}

pub struct Verifier {
    backend: VerifierBackend,
    limits: VerifyLimits,
}

impl Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Verifier))
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Verifier {
    /// Wraps an already-open backend for read-only verification.
    #[must_use]
    pub fn new(backend: Box<dyn EventStore>) -> Self {
        Self::with_limits(backend, VerifyLimits::default())
    }

    /// Wraps an already-open backend with explicit hard verification budgets.
    #[must_use]
    pub fn with_limits(backend: Box<dyn EventStore>, limits: VerifyLimits) -> Self {
        Self {
            backend: VerifierBackend::Erased(backend),
            limits,
        }
    }

    /// Opens a sealed redb run file at `<base_dir>/<instance_id>/<run_id>.redb` and
    /// wraps it for verification.
    ///
    /// Mirrors [`crate::backend::RedbBackend::open_sealed`]: only sealed files are
    /// accepted, since opening a still-`Running` file would race with a live writer
    /// and break the off-trader-process posture.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::Backend`] when the underlying backend rejects the open
    /// (file missing, run still `Running`, header corruption).
    pub fn open_redb(
        base_dir: impl AsRef<Path>,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Self, VerifyError> {
        Self::open_redb_with_limits(base_dir, instance_id, run_id, VerifyLimits::default())
    }

    /// Opens a sealed redb run under explicit verification and file-size budgets.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::LimitExceeded`] before scanning when the file exceeds the byte
    /// budget, or the same backend/open errors as [`Self::open_redb`].
    pub fn open_redb_with_limits(
        base_dir: impl AsRef<Path>,
        instance_id: &str,
        run_id: &str,
        limits: VerifyLimits,
    ) -> Result<Self, VerifyError> {
        let backend = RedbBackend::open_sealed_with_read_limits(
            base_dir.as_ref().to_path_buf(),
            instance_id,
            run_id,
            limits.max_file_bytes,
            limits.max_manifest_bytes,
        )
        .map_err(map_redb_open_error)?;
        Ok(Self {
            backend: VerifierBackend::Redb(Box::new(backend)),
            limits,
        })
    }

    /// Opens a sealed redb run file directly by path and wraps it for verification.
    ///
    /// The backend uses a read-only database handle for this path. The verifier
    /// reports findings, but it never seals or quarantines the file; a supervisor or
    /// operator process decides that policy from the returned [`VerifyReport`].
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::Backend`] when the underlying backend rejects the open
    /// (file missing, run still `Running`, header corruption).
    pub fn open_redb_file(path: impl AsRef<Path>) -> Result<Self, VerifyError> {
        Self::open_redb_file_with_limits(path, VerifyLimits::default())
    }

    /// Opens a sealed redb file directly under explicit verification and file-size budgets.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::LimitExceeded`] before scanning when the file exceeds the byte
    /// budget, or the same backend/open errors as [`Self::open_redb_file`].
    pub fn open_redb_file_with_limits(
        path: impl AsRef<Path>,
        limits: VerifyLimits,
    ) -> Result<Self, VerifyError> {
        let backend = RedbBackend::open_sealed_file_with_read_limits(
            path.as_ref().to_path_buf(),
            limits.max_file_bytes,
            limits.max_manifest_bytes,
        )
        .map_err(map_redb_open_error)?;
        Ok(Self {
            backend: VerifierBackend::Redb(Box::new(backend)),
            limits,
        })
    }

    /// Returns a reference to the wrapped backend.
    #[must_use]
    pub fn backend(&self) -> &dyn EventStore {
        self.backend.as_ref()
    }

    /// Consumes a verifier opened through a Redb constructor and returns the exact backend handle
    /// which was scanned.
    ///
    /// Returning the same read-only handle lets replay consumers avoid a verify-then-reopen race
    /// in which the path could be replaced after verification. Verifiers constructed from an
    /// erased caller backend return `None` because their concrete type is intentionally hidden.
    #[must_use]
    pub fn into_verified_redb_backend(self) -> Option<RedbBackend> {
        match self.backend {
            VerifierBackend::Redb(backend) => Some(*backend),
            VerifierBackend::Erased(_) => None,
        }
    }

    /// Returns the hard budgets applied by this verifier.
    #[must_use]
    pub const fn limits(&self) -> VerifyLimits {
        self.limits
    }

    /// Performs a full integrity scan of the open run and returns the typed report.
    ///
    /// `verify` reads the manifest, walks every `seq` in `[1, high_watermark]`,
    /// cross-checks the stored client- and venue-order-id indices, and validates manifest
    /// invariants. Hash mismatches, gaps, index drift, and manifest mismatches surface as
    /// [`VerifyFinding`]s on the returned report; only failures that prevent the verifier
    /// from producing a report at all surface as [`VerifyError`].
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::Backend`] when the backend refuses a read-side
    /// operation (no run open, disk pressure, manifest decode failure).
    pub fn verify(&self) -> Result<VerifyReport, VerifyError> {
        let manifest = self.backend.manifest()?;
        manifest.validate_schema_version()?;
        let high_watermark = self.backend.high_watermark()?;
        self.preflight_limits(high_watermark)?;

        let mut findings = Vec::new();
        let snapshot_anchor = check_snapshot_anchor(
            self.backend.as_ref(),
            high_watermark,
            self.limits.max_snapshot_anchor_bytes,
            &mut findings,
        )?;
        self.enforce_findings_limit(&findings)?;
        let (mut digest_builder, anchor_bytes) =
            begin_verified_run_digest(&manifest, high_watermark, snapshot_anchor.as_ref())?;
        let scan = self.scan_entries(
            &manifest,
            high_watermark,
            &mut findings,
            &mut digest_builder,
        )?;

        self.cross_check_indices(&manifest, high_watermark, &scan, &mut findings)?;
        validate_run_started(&manifest, scan.first_entry.as_ref(), &mut findings);
        validate_lifecycle_markers(&manifest, high_watermark, &scan, &mut findings);
        validate_manifest(&manifest, high_watermark, &scan, &mut findings);
        self.enforce_findings_limit(&findings)?;
        let store_digest = if findings.is_empty() {
            let anchor_len =
                u64::try_from(anchor_bytes.len()).map_err(|_| VerifyError::ArithmeticOverflow {
                    operation: "convert verified snapshot-anchor length to u64",
                })?;
            digest_builder
                .update(&anchor_len.to_be_bytes())
                .map_err(map_digest_builder_error)?;
            digest_builder
                .update(&anchor_bytes)
                .map_err(map_digest_builder_error)?;
            Some(digest_builder.finish().map_err(map_digest_builder_error)?)
        } else {
            None
        };

        Ok(VerifyReport {
            run_id: manifest.run_id.clone(),
            status: manifest.status,
            high_watermark,
            entries_scanned: scan.scanned,
            store_digest,
            findings,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the streaming scan keeps resource accounting adjacent to every decode and finding"
    )]
    fn scan_entries(
        &self,
        manifest: &RunManifest,
        high_watermark: u64,
        findings: &mut Vec<VerifyFinding>,
        digest_builder: &mut ContentDigestBuilder,
    ) -> Result<EntryScan, VerifyError> {
        let mut scanned: u64 = 0;
        let mut total_entry_bytes = 0_u64;
        let mut min_ts: Option<u64> = None;
        let mut max_ts: Option<u64> = None;
        let mut gap_cursor: Option<u64> = None;
        let mut first_entry = None;
        let mut last_entry = None;

        for seq in 1..=high_watermark {
            match self
                .backend
                .scan_seq_bounded(seq, self.limits.max_entry_bytes)
            {
                Ok(Some((entry, row_bytes))) => {
                    total_entry_bytes = total_entry_bytes.checked_add(row_bytes).ok_or(
                        VerifyError::ArithmeticOverflow {
                            operation: "sum decoded entry row bytes",
                        },
                    )?;
                    enforce_limit(
                        VerifyLimitResource::TotalEntryBytes,
                        total_entry_bytes,
                        self.limits.max_total_entry_bytes,
                    )?;
                    flush_pending_gap(seq, &mut gap_cursor, findings, self.limits.max_findings)?;

                    // The recomputed hash check inside scan_seq covers the entry
                    // contents, but the entry's embedded seq is one of those
                    // contents: a row whose value is moved or duplicated under a
                    // different table key still hashes correctly. Cross-check the
                    // table key against the embedded seq so the verifier catches
                    // that class of corruption rather than reporting a clean run.
                    if entry.seq != seq {
                        self.push_finding(
                            findings,
                            VerifyFinding::SeqMismatch {
                                table_key: seq,
                                embedded_seq: entry.seq,
                            },
                        )?;
                        scanned = increment_with_limit(
                            scanned,
                            self.limits.max_entries,
                            VerifyLimitResource::Entries,
                        )?;
                        continue;
                    }
                    record_entry(&entry, &mut min_ts, &mut max_ts);
                    if seq == 1 {
                        first_entry = Some(entry.clone());
                    }
                    if seq == high_watermark {
                        last_entry = Some(entry.clone());
                    }
                    if is_lifecycle_topic(entry.topic.as_ref())
                        || is_lifecycle_payload_type(entry.payload_type.as_str())
                    {
                        let canonical = is_canonical_run_started_envelope(&entry, manifest)
                            || is_canonical_run_ended_at_hwm(&entry, high_watermark);
                        if !canonical {
                            self.push_finding(
                                findings,
                                lifecycle_marker_finding(format!(
                                    "reserved lifecycle topic/tag at seq={} is not a canonical RunStarted at seq=1 or RunEnded at durable high_watermark {high_watermark} (topic=`{}`, payload_type=`{}`)",
                                    entry.seq,
                                    entry.topic.as_ref(),
                                    entry.payload_type.as_str(),
                                )),
                            )?;
                        }
                    }
                    digest_builder
                        .update(entry.entry_hash.as_bytes())
                        .map_err(map_digest_builder_error)?;
                    scanned = increment_with_limit(
                        scanned,
                        self.limits.max_entries,
                        VerifyLimitResource::Entries,
                    )?;
                }
                Ok(None) | Err(EventStoreError::Gap { .. }) => {
                    extend_pending_gap(seq, &mut gap_cursor);
                }
                Err(scan_err) => {
                    let finding = match scan_err {
                        EventStoreError::EntrySizeLimitExceeded {
                            observed, limit, ..
                        } => {
                            return Err(VerifyError::LimitExceeded {
                                resource: VerifyLimitResource::EntryBytes,
                                observed,
                                limit,
                            });
                        }
                        EventStoreError::HashMismatch { seq: bad } => {
                            VerifyFinding::HashMismatch { seq: bad }
                        }
                        EventStoreError::SeqMismatch {
                            table_key,
                            embedded_seq,
                        } => VerifyFinding::SeqMismatch {
                            table_key,
                            embedded_seq,
                        },
                        EventStoreError::Corrupted(reason) => {
                            VerifyFinding::Undecodable { seq, reason }
                        }
                        other => return Err(VerifyError::Backend(other)),
                    };
                    flush_pending_gap(seq, &mut gap_cursor, findings, self.limits.max_findings)?;
                    self.push_finding(findings, finding)?;
                    scanned = increment_with_limit(
                        scanned,
                        self.limits.max_entries,
                        VerifyLimitResource::Entries,
                    )?;
                }
            }
        }

        flush_trailing_gap(
            high_watermark,
            &mut gap_cursor,
            findings,
            self.limits.max_findings,
        )?;

        Ok(EntryScan {
            scanned,
            total_entry_bytes,
            min_ts,
            max_ts,
            first_entry,
            last_entry,
        })
    }

    fn cross_check_indices(
        &self,
        manifest: &RunManifest,
        high_watermark: u64,
        scan: &EntryScan,
        findings: &mut Vec<VerifyFinding>,
    ) -> Result<(), VerifyError> {
        let mut scanned = 0_u64;
        let mut total_entry_bytes = scan.total_entry_bytes;
        let mut total_key_bytes = 0_u64;
        let has_canonical_start = scan
            .first_entry
            .as_ref()
            .is_some_and(|entry| is_canonical_run_started_envelope(entry, manifest));
        let has_canonical_terminal = scan
            .last_entry
            .as_ref()
            .is_some_and(|entry| is_canonical_run_ended_at_hwm(entry, high_watermark));
        for kind in [IndexKind::ClientOrderId, IndexKind::VenueOrderId] {
            let mut callback_error = None;
            self.backend.visit_index_keys(kind, &mut |key, stored_seq| {
                let result = (|| {
                    scanned = increment_with_limit(
                        scanned,
                        self.limits.max_index_entries,
                        VerifyLimitResource::IndexEntries,
                    )?;
                    let key_bytes = u64::try_from(key.len()).map_err(|_| {
                        VerifyError::ArithmeticOverflow {
                            operation: "convert index key length to u64",
                        }
                    })?;
                    total_key_bytes = total_key_bytes.checked_add(key_bytes).ok_or(
                        VerifyError::ArithmeticOverflow {
                            operation: "sum sidecar index key bytes",
                        },
                    )?;
                    enforce_limit(
                        VerifyLimitResource::TotalIndexKeyBytes,
                        total_key_bytes,
                        self.limits.max_total_index_key_bytes,
                    )?;
                    if let Some(drift) = self.classify_index_target(
                        stored_seq,
                        high_watermark,
                        &mut total_entry_bytes,
                    )? {
                        self.push_finding(
                            findings,
                            VerifyFinding::IndexDrift {
                                kind,
                                key: key.to_string(),
                                drift,
                            },
                        )?;
                    }
                    if has_canonical_start && stored_seq == 1 {
                        self.push_finding(
                            findings,
                            lifecycle_marker_finding(format!(
                                "canonical RunStarted at seq=1 must not carry {kind:?} sidecar index key `{key}`"
                            )),
                        )?;
                    }
                    if has_canonical_terminal && stored_seq == high_watermark {
                        self.push_finding(
                            findings,
                            lifecycle_marker_finding(format!(
                                "canonical RunEnded at seq={high_watermark} must not carry {kind:?} sidecar index key `{key}`"
                            )),
                        )?;
                    }
                    Ok::<(), VerifyError>(())
                })();
                if let Err(error) = result {
                    callback_error = Some(error);
                    return false;
                }
                true
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
        }

        Ok(())
    }

    fn classify_index_target(
        &self,
        stored_seq: u64,
        high_watermark: u64,
        total_entry_bytes: &mut u64,
    ) -> Result<Option<IndexDrift>, VerifyError> {
        if stored_seq == 0 || stored_seq > high_watermark {
            return Ok(Some(IndexDrift::DanglingTarget { stored_seq }));
        }
        match self
            .backend
            .scan_seq_bounded(stored_seq, self.limits.max_entry_bytes)
        {
            Ok(Some((entry, row_bytes))) => {
                *total_entry_bytes = total_entry_bytes.checked_add(row_bytes).ok_or(
                    VerifyError::ArithmeticOverflow {
                        operation: "sum decoded entry bytes during index validation",
                    },
                )?;
                enforce_limit(
                    VerifyLimitResource::TotalEntryBytes,
                    *total_entry_bytes,
                    self.limits.max_total_entry_bytes,
                )?;
                if entry.seq == stored_seq {
                    Ok(None)
                } else {
                    Ok(Some(IndexDrift::TargetCorrupted { stored_seq }))
                }
            }
            Ok(None) | Err(EventStoreError::Gap { .. }) => {
                Ok(Some(IndexDrift::DanglingTarget { stored_seq }))
            }
            Err(
                EventStoreError::HashMismatch { .. }
                | EventStoreError::SeqMismatch { .. }
                | EventStoreError::Corrupted(_),
            ) => Ok(Some(IndexDrift::TargetCorrupted { stored_seq })),
            Err(EventStoreError::EntrySizeLimitExceeded {
                observed, limit, ..
            }) => Err(VerifyError::LimitExceeded {
                resource: VerifyLimitResource::EntryBytes,
                observed,
                limit,
            }),
            Err(error) => Err(VerifyError::Backend(error)),
        }
    }

    fn push_finding(
        &self,
        findings: &mut Vec<VerifyFinding>,
        finding: VerifyFinding,
    ) -> Result<(), VerifyError> {
        push_finding_with_limit(findings, finding, self.limits.max_findings)
    }

    fn enforce_findings_limit(&self, findings: &[VerifyFinding]) -> Result<(), VerifyError> {
        let observed =
            u64::try_from(findings.len()).map_err(|_| VerifyError::ArithmeticOverflow {
                operation: "convert finding count to u64",
            })?;
        enforce_limit(
            VerifyLimitResource::Findings,
            observed,
            self.limits.max_findings,
        )
    }

    fn preflight_limits(&self, high_watermark: u64) -> Result<(), VerifyError> {
        enforce_limit(
            VerifyLimitResource::HighWatermark,
            high_watermark,
            self.limits.max_high_watermark,
        )?;
        let entry_count = self.backend.entry_count()?;
        enforce_limit(
            VerifyLimitResource::Entries,
            entry_count,
            self.limits.max_entries,
        )?;

        let mut index_count = 0_u64;
        for kind in [IndexKind::ClientOrderId, IndexKind::VenueOrderId] {
            index_count = index_count
                .checked_add(self.backend.index_entry_count(kind)?)
                .ok_or(VerifyError::ArithmeticOverflow {
                    operation: "sum preflight index entries",
                })?;
            enforce_limit(
                VerifyLimitResource::IndexEntries,
                index_count,
                self.limits.max_index_entries,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct EntryScan {
    scanned: u64,
    total_entry_bytes: u64,
    min_ts: Option<u64>,
    max_ts: Option<u64>,
    first_entry: Option<EventStoreEntry>,
    last_entry: Option<EventStoreEntry>,
}

fn validate_lifecycle_markers(
    manifest: &RunManifest,
    high_watermark: u64,
    scan: &EntryScan,
    findings: &mut Vec<VerifyFinding>,
) {
    let has_canonical_terminal = scan
        .last_entry
        .as_ref()
        .is_some_and(|entry| is_canonical_run_ended_at_hwm(entry, high_watermark));

    match manifest.status {
        RunStatus::Ended if high_watermark < 2 => {
            findings.push(lifecycle_marker_finding(format!(
                "Ended run requires RunStarted followed by a canonical RunEnded, but durable high_watermark was {high_watermark}",
            )));
        }
        RunStatus::Ended if !has_canonical_terminal => {
            findings.push(lifecycle_marker_finding(format!(
                "Ended run is missing a canonical RunEnded at durable high_watermark {high_watermark}",
            )));
        }
        RunStatus::CrashedRecovered if has_canonical_terminal => {
            findings.push(lifecycle_marker_finding(
                "CrashedRecovered run must not carry a canonical terminal RunEnded".to_string(),
            ));
        }
        RunStatus::Running if has_canonical_terminal => {
            findings.push(lifecycle_marker_finding(
                "Running run must not carry a canonical terminal RunEnded".to_string(),
            ));
        }
        _ => {}
    }
}

fn lifecycle_marker_finding(reason: String) -> VerifyFinding {
    VerifyFinding::ManifestMismatch {
        kind: ManifestField::LifecycleMarker,
        reason,
    }
}

fn validate_run_started(
    manifest: &RunManifest,
    first_entry: Option<&EventStoreEntry>,
    findings: &mut Vec<VerifyFinding>,
) {
    let Some(entry) = first_entry else {
        findings.push(run_header_finding(
            "missing a readable RunStarted entry at seq=1".to_string(),
        ));
        return;
    };

    if entry.topic.as_ref() != RUN_STARTED_TOPIC {
        findings.push(run_header_finding(format!(
            "seq=1 topic was `{}`, expected `{RUN_STARTED_TOPIC}`",
            entry.topic.as_ref(),
        )));
        return;
    }
    if entry.payload_type.as_str() != RUN_STARTED_PAYLOAD_TYPE {
        findings.push(run_header_finding(format!(
            "seq=1 payload type was `{}`, expected `{RUN_STARTED_PAYLOAD_TYPE}`",
            entry.payload_type.as_str(),
        )));
        return;
    }
    if !entry.headers.is_empty() {
        findings.push(run_header_finding(
            "RunStarted must not carry correlation or causation headers".to_string(),
        ));
    }
    if entry.ts_init != manifest.start_ts_init {
        findings.push(run_header_finding(format!(
            "RunStarted ts_init {} disagrees with manifest start_ts_init {}",
            entry.ts_init.as_u64(),
            manifest.start_ts_init.as_u64(),
        )));
    }

    let payload = match codec::decode_from_slice::<RunStartedPayload>(&entry.payload) {
        Ok(payload) => payload,
        Err(error) => {
            findings.push(run_header_finding(format!(
                "RunStarted payload failed to decode: {error}",
            )));
            return;
        }
    };
    if payload.version() != RUN_STARTED_PAYLOAD_VERSION {
        findings.push(run_header_finding(format!(
            "unsupported RunStarted payload version {}; supported version is {RUN_STARTED_PAYLOAD_VERSION}",
            payload.version(),
        )));
        return;
    }

    let expected = RunStartedPayload::from_manifest(manifest);
    if payload.header() != expected.header() {
        findings.push(run_header_finding(
            "RunStarted immutable header commitment disagrees with the current manifest"
                .to_string(),
        ));
    }
}

fn run_header_finding(reason: String) -> VerifyFinding {
    VerifyFinding::ManifestMismatch {
        kind: ManifestField::RunHeaderCommitment,
        reason,
    }
}

fn record_entry(entry: &EventStoreEntry, min_ts: &mut Option<u64>, max_ts: &mut Option<u64>) {
    let ts = entry.ts_init.as_u64();
    *min_ts = Some(min_ts.map_or(ts, |cur| cur.min(ts)));
    *max_ts = Some(max_ts.map_or(ts, |cur| cur.max(ts)));
}

fn extend_pending_gap(seq: u64, gap_cursor: &mut Option<u64>) {
    if gap_cursor.is_none() {
        *gap_cursor = Some(seq);
    }
}

fn flush_pending_gap(
    next_seq: u64,
    gap_cursor: &mut Option<u64>,
    findings: &mut Vec<VerifyFinding>,
    max_findings: u64,
) -> Result<(), VerifyError> {
    if let Some(start) = gap_cursor.take() {
        let end = next_seq
            .checked_sub(1)
            .ok_or(VerifyError::ArithmeticOverflow {
                operation: "compute inclusive gap end",
            })?;
        push_finding_with_limit(
            findings,
            VerifyFinding::Gap {
                range: GapRange {
                    from: start,
                    to: end,
                },
            },
            max_findings,
        )?;
    }
    Ok(())
}

fn flush_trailing_gap(
    high_watermark: u64,
    gap_cursor: &mut Option<u64>,
    findings: &mut Vec<VerifyFinding>,
    max_findings: u64,
) -> Result<(), VerifyError> {
    if let Some(start) = gap_cursor.take() {
        push_finding_with_limit(
            findings,
            VerifyFinding::Gap {
                range: GapRange {
                    from: start,
                    to: high_watermark,
                },
            },
            max_findings,
        )?;
    }
    Ok(())
}

fn push_finding_with_limit(
    findings: &mut Vec<VerifyFinding>,
    finding: VerifyFinding,
    max_findings: u64,
) -> Result<(), VerifyError> {
    let observed = u64::try_from(findings.len())
        .map_err(|_| VerifyError::ArithmeticOverflow {
            operation: "convert finding count to u64",
        })?
        .checked_add(1)
        .ok_or(VerifyError::ArithmeticOverflow {
            operation: "increment finding count",
        })?;
    enforce_limit(VerifyLimitResource::Findings, observed, max_findings)?;
    findings.push(finding);
    Ok(())
}

fn enforce_limit(
    resource: VerifyLimitResource,
    observed: u64,
    limit: u64,
) -> Result<(), VerifyError> {
    if observed > limit {
        Err(VerifyError::LimitExceeded {
            resource,
            observed,
            limit,
        })
    } else {
        Ok(())
    }
}

fn increment_with_limit(
    current: u64,
    limit: u64,
    resource: VerifyLimitResource,
) -> Result<u64, VerifyError> {
    let observed = current
        .checked_add(1)
        .ok_or(VerifyError::ArithmeticOverflow {
            operation: "increment verifier resource counter",
        })?;
    enforce_limit(resource, observed, limit)?;
    Ok(observed)
}

fn map_redb_open_error(error: EventStoreError) -> VerifyError {
    match error {
        EventStoreError::FileSizeLimitExceeded { observed, limit } => VerifyError::LimitExceeded {
            resource: VerifyLimitResource::FileBytes,
            observed,
            limit,
        },
        EventStoreError::FileChangedDuringOpen { path } => {
            VerifyError::InputChangedDuringOpen { path }
        }
        EventStoreError::MetadataSizeLimitExceeded {
            resource: "manifest",
            observed,
            limit,
        } => VerifyError::LimitExceeded {
            resource: VerifyLimitResource::ManifestBytes,
            observed,
            limit,
        },
        other => VerifyError::Backend(other),
    }
}

// The restore path reads the snapshot anchor before tail replay, so an anchor that
// fails to decode or points past the durable watermark must not verify clean. Other
// read failures (disk pressure, storage errors) propagate: suppressing them would
// pass a run whose restore would fail reading the same anchor.
fn check_snapshot_anchor(
    backend: &dyn EventStore,
    high_watermark: u64,
    max_row_bytes: u64,
    findings: &mut Vec<VerifyFinding>,
) -> Result<Option<SnapshotAnchor>, VerifyError> {
    match backend.latest_snapshot_anchor_bounded(max_row_bytes) {
        Ok(Some(anchor)) if anchor.high_watermark > high_watermark => {
            findings.push(VerifyFinding::SnapshotAnchorInvalid {
                reason: format!(
                    "snapshot anchor high_watermark {} exceeds durable high_watermark {high_watermark}",
                    anchor.high_watermark,
                ),
            });
            Ok(Some(anchor))
        }
        Ok(anchor) => Ok(anchor),
        Err(EventStoreError::Corrupted(msg)) => {
            findings.push(VerifyFinding::SnapshotAnchorInvalid {
                reason: format!("snapshot anchor unreadable: {msg}"),
            });
            Ok(None)
        }
        Err(EventStoreError::MetadataSizeLimitExceeded {
            observed, limit, ..
        }) => Err(VerifyError::LimitExceeded {
            resource: VerifyLimitResource::SnapshotAnchorBytes,
            observed,
            limit,
        }),
        Err(other) => Err(VerifyError::Backend(other)),
    }
}

fn begin_verified_run_digest(
    manifest: &RunManifest,
    high_watermark: u64,
    snapshot_anchor: Option<&SnapshotAnchor>,
) -> Result<(ContentDigestBuilder, Vec<u8>), VerifyError> {
    let manifest_bytes = codec::encode_run_manifest(manifest).map_err(|error| {
        VerifyError::Backend(EventStoreError::Corrupted(format!(
            "encode verified run manifest: {error}"
        )))
    })?;
    let anchor_bytes = codec::encode_to_vec(&snapshot_anchor).map_err(|error| {
        VerifyError::Backend(EventStoreError::Corrupted(format!(
            "encode verified snapshot anchor: {error}"
        )))
    })?;

    let manifest_len =
        u64::try_from(manifest_bytes.len()).map_err(|_| VerifyError::ArithmeticOverflow {
            operation: "convert verified manifest length to u64",
        })?;
    let anchor_len =
        u64::try_from(anchor_bytes.len()).map_err(|_| VerifyError::ArithmeticOverflow {
            operation: "convert verified snapshot-anchor length to u64",
        })?;
    let hashes_len = high_watermark
        .checked_mul(32)
        .ok_or(VerifyError::ArithmeticOverflow {
            operation: "compute verified entry-hash transcript bytes",
        })?;
    let transcript_len = u64::try_from(VERIFIED_RUN_DIGEST_DOMAIN.len())
        .map_err(|_| VerifyError::ArithmeticOverflow {
            operation: "convert verified-run domain length to u64",
        })?
        .checked_add(8)
        .and_then(|value| value.checked_add(manifest_len))
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(hashes_len))
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(anchor_len))
        .ok_or(VerifyError::ArithmeticOverflow {
            operation: "compute verified-run transcript length",
        })?;
    let mut builder = ContentDigestBuilder::new(transcript_len);
    builder
        .update(VERIFIED_RUN_DIGEST_DOMAIN)
        .map_err(map_digest_builder_error)?;
    builder
        .update(&manifest_len.to_be_bytes())
        .map_err(map_digest_builder_error)?;
    builder
        .update(&manifest_bytes)
        .map_err(map_digest_builder_error)?;
    builder
        .update(&high_watermark.to_be_bytes())
        .map_err(map_digest_builder_error)?;
    Ok((builder, anchor_bytes))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "this is a direct `Result::map_err` adapter and consumes the owned source error"
)]
fn map_digest_builder_error(error: nautilus_provenance::ProvenanceError) -> VerifyError {
    VerifyError::Backend(EventStoreError::Corrupted(format!(
        "build verified-run digest: {error}"
    )))
}

fn validate_manifest(
    manifest: &RunManifest,
    high_watermark: u64,
    scan: &EntryScan,
    findings: &mut Vec<VerifyFinding>,
) {
    if manifest.high_watermark != high_watermark {
        findings.push(VerifyFinding::ManifestMismatch {
            kind: ManifestField::HighWatermark,
            reason: format!(
                "manifest high_watermark {} disagrees with durable high_watermark {high_watermark}",
                manifest.high_watermark,
            ),
        });
    }

    if let Some(min_ts) = scan.min_ts
        && manifest.start_ts_init.as_u64() > min_ts
    {
        findings.push(VerifyFinding::ManifestMismatch {
            kind: ManifestField::StartTsInit,
            reason: format!(
                "manifest start_ts_init {} sits above earliest entry ts_init {min_ts}",
                manifest.start_ts_init.as_u64(),
            ),
        });
    }

    if manifest.is_sealed() {
        match (manifest.end_ts_init.map(|t| t.as_u64()), scan.max_ts) {
            (Some(stored), Some(observed)) if stored != observed => {
                findings.push(VerifyFinding::ManifestMismatch {
                    kind: ManifestField::EndTsInit,
                    reason: format!(
                        "manifest end_ts_init {stored} disagrees with last observed ts_init {observed}",
                    ),
                });
            }
            (None, Some(observed)) => findings.push(VerifyFinding::ManifestMismatch {
                kind: ManifestField::EndTsInit,
                reason: format!(
                    "sealed manifest is missing end_ts_init while entries up to ts_init {observed} exist",
                ),
            }),
            (Some(stored), None) => findings.push(VerifyFinding::ManifestMismatch {
                kind: ManifestField::EndTsInit,
                reason: format!(
                    "sealed manifest carries end_ts_init {stored} despite empty entry table",
                ),
            }),
            _ => {}
        }
    }
}

/// The structured report produced by [`Verifier::verify`].
///
/// Operators key on [`VerifyReport::is_clean`] for the binary verdict and walk
/// [`VerifyReport::findings`] for the actionable items. The verifier never
/// quarantines on its own: that is the supervisor's call given the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    /// The id of the verified run, copied from the manifest.
    pub run_id: RunId,
    /// The lifecycle status the run carried at verification time.
    pub status: RunStatus,
    /// The durable high-watermark the verifier walked up to.
    pub high_watermark: u64,
    /// The number of `seq` slots the verifier successfully read (clean or hash-mismatched).
    pub entries_scanned: u64,
    /// Commitment to the manifest, ordered entry hashes, and snapshot anchor.
    ///
    /// Present only when the verifier accumulated zero findings. Release consumers must also
    /// require [`RunStatus::Ended`] before accepting this as clean-shutdown evidence.
    pub store_digest: Option<ContentDigest>,
    /// Every integrity finding the verifier accumulated.
    pub findings: Vec<VerifyFinding>,
}

impl VerifyReport {
    /// Returns `true` when the verifier accumulated no findings.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }
}

/// One actionable integrity finding from a verifier run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyFinding {
    /// The recomputed canonical hash of `seq` did not match the stored value.
    HashMismatch {
        /// The sequence number whose hash diverged.
        seq: u64,
    },
    /// One or more contiguous `seq` slots inside the high-watermark are missing.
    Gap {
        /// The inclusive range of missing seqs.
        range: GapRange,
    },
    /// The entry stored at table key `table_key` carries an `entry.seq` that
    /// disagrees with the key.
    ///
    /// The canonical hash hashes `entry.seq` rather than the table key, so a row
    /// whose bytes were moved or duplicated under a different key still passes the
    /// hash check. The verifier surfaces the divergence so that class of
    /// corruption never reads as clean.
    SeqMismatch {
        /// The redb table key (the slot the verifier was reading).
        table_key: u64,
        /// The seq embedded inside the decoded entry value.
        embedded_seq: u64,
    },
    /// The row stored at `seq` failed to decode into an entry.
    ///
    /// Recorded per slot so one bad row cannot mask every other finding.
    Undecodable {
        /// The sequence number whose stored bytes failed to decode.
        seq: u64,
        /// Operator-readable explanation of the decode failure.
        reason: String,
    },
    /// A stored sidecar index entry diverges from the projection rebuilt from the
    /// entry table.
    IndexDrift {
        /// Which sidecar index the finding applies to.
        kind: IndexKind,
        /// The stringified key inside that index.
        key: String,
        /// The kind of drift observed.
        drift: IndexDrift,
    },
    /// A manifest field disagrees with the entry table or violates a sealed-state
    /// invariant.
    ManifestMismatch {
        /// Which manifest field the finding applies to.
        kind: ManifestField,
        /// Operator-readable explanation of the mismatch.
        reason: String,
    },
    /// The recorded snapshot anchor cannot support a restore: it fails to decode or
    /// points past the durable high-watermark.
    SnapshotAnchorInvalid {
        /// Operator-readable explanation of the failure.
        reason: String,
    },
}

/// An inclusive `[from, to]` range of missing seqs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GapRange {
    /// First missing seq.
    pub from: u64,
    /// Last missing seq.
    pub to: u64,
}

/// The kind of drift observed for a sidecar index key.
///
/// Today the verifier only reports target reachability for the `client_order_id` and
/// `venue_order_id` indices because the rebuild is not yet payload-aware. Variants for
/// rebuild-vs-stored mismatches (missing from stored, divergent seq, unknown key) will
/// land when wrapper-type encoders provide a payload-derived projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexDrift {
    /// The stored index points at a seq that does not exist inside the high-watermark.
    DanglingTarget {
        /// The seq the stored index recorded.
        stored_seq: u64,
    },
    /// The stored index points at a seq whose entry failed the hash check.
    TargetCorrupted {
        /// The seq the stored index recorded.
        stored_seq: u64,
    },
}

/// A manifest field flagged by [`VerifyFinding::ManifestMismatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ManifestField {
    /// The first `RunStarted` entry is absent, malformed, or disagrees with immutable manifest data.
    RunHeaderCommitment,
    /// A reserved lifecycle envelope is misplaced or malformed, or the terminal marker
    /// disagrees with the manifest status.
    LifecycleMarker,
    /// `manifest.high_watermark` does not match the durable last seq.
    HighWatermark,
    /// `manifest.start_ts_init` sits above the earliest observed `ts_init`.
    StartTsInit,
    /// `manifest.end_ts_init` does not bracket the entry stream as expected for a
    /// sealed run.
    EndTsInit,
}

/// Errors that prevent the verifier from producing a report at all.
///
/// Findings on a successful report cover the operator's actionable surface; this
/// type captures the verifier's own failure modes (no run open, disk pressure on a
/// read, manifest header damage that prevents loading the manifest).
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// An attacker-controlled input exceeded an explicit hard verification budget.
    #[error("verification {resource} limit exceeded: observed {observed}, limit {limit}")]
    LimitExceeded {
        /// Resource dimension that was refused.
        resource: VerifyLimitResource,
        /// Value measured from the untrusted store.
        observed: u64,
        /// Configured maximum accepted value.
        limit: u64,
    },
    /// Checked verifier bookkeeping could not represent an arithmetic result.
    #[error("verification arithmetic overflow while attempting to {operation}")]
    ArithmeticOverflow {
        /// Static description of the rejected computation.
        operation: &'static str,
    },
    /// A redb path changed metadata identity while its read-only handle was being opened.
    #[error("sealed event-store input changed while opening: {path}")]
    InputChangedDuringOpen {
        /// Path whose metadata fingerprint changed.
        path: String,
    },
    /// A backend operation refused service before the verifier could produce a report.
    #[error("backend access failed: {0}")]
    Backend(#[from] EventStoreError),
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use indexmap::IndexMap;
    use nautilus_core::{UUID4, UnixNanos};
    use rstest::{fixture, rstest};
    use ustr::Ustr;

    use super::*;
    use crate::{
        backend::{AppendEntry, IndexKey, MemoryBackend, ScanDirection},
        compute_entry_hash,
        entry::Topic,
        headers::Headers,
        manifest::{RegisteredComponents, RunManifest, RunStatus},
    };

    fn manifest(run_id: &str) -> RunManifest {
        RunManifest {
            run_id: run_id.to_string(),
            parent_run_id: None,
            instance_id: "trader-001".to_string(),
            binary_hash: "deadbeef".to_string(),
            schema_version: 1,
            crate_versions: "feedface".to_string(),
            feature_flags: Vec::new(),
            adapter_versions: IndexMap::new(),
            config_hash: "cafebabe".to_string(),
            provenance: nautilus_provenance::RunProvenance::default(),
            registered_components: RegisteredComponents::default(),
            seed: None,
            start_ts_init: UnixNanos::from(10),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
        }
    }

    fn build_entry(seq: u64, headers: Headers, ts_init: u64) -> EventStoreEntry {
        let (topic, payload_type, payload): (Topic, Ustr, Bytes) = if seq == 1 {
            let payload = RunStartedPayload::from_manifest(&manifest("1700000000-aaaa1111"));
            (
                RUN_STARTED_TOPIC.into(),
                Ustr::from(RUN_STARTED_PAYLOAD_TYPE),
                Bytes::from(codec::encode_to_vec(&payload).expect("encode RunStarted")),
            )
        } else {
            (
                "exec.command.SubmitOrder".into(),
                Ustr::from("SubmitOrder"),
                Bytes::from_static(b"\x01\x02\x03\x04"),
            )
        };
        let ts_publish = UnixNanos::from(ts_init + 1);
        let ts_init = UnixNanos::from(ts_init);
        let hash = compute_entry_hash(
            seq,
            ts_init,
            ts_publish,
            topic.as_ref(),
            payload_type.as_str(),
            &payload,
            &headers,
        );

        EventStoreEntry::new(
            hash,
            seq,
            headers,
            topic,
            payload_type,
            payload,
            ts_init,
            ts_publish,
        )
    }

    fn append_with(seq: u64, ts_init: u64, index_keys: Vec<IndexKey>) -> AppendEntry {
        AppendEntry::new(build_entry(seq, Headers::empty(), ts_init), index_keys)
    }

    fn run_started_entry(manifest: &RunManifest) -> EventStoreEntry {
        let topic: Topic = RUN_STARTED_TOPIC.into();
        let payload_type = Ustr::from(RUN_STARTED_PAYLOAD_TYPE);
        let payload = Bytes::from(
            codec::encode_to_vec(&RunStartedPayload::from_manifest(manifest))
                .expect("encode RunStarted"),
        );
        let headers = Headers::empty();
        let ts_init = manifest.start_ts_init;
        let ts_publish = UnixNanos::from(ts_init.as_u64() + 1);
        let hash = compute_entry_hash(
            1,
            ts_init,
            ts_publish,
            topic.as_ref(),
            payload_type.as_str(),
            &payload,
            &headers,
        );
        EventStoreEntry::new(
            hash,
            1,
            headers,
            topic,
            payload_type,
            payload,
            ts_init,
            ts_publish,
        )
    }

    fn run_ended_entry(
        seq: u64,
        ts_init: u64,
        payload: Bytes,
        headers: Headers,
    ) -> EventStoreEntry {
        let topic: Topic = crate::manifest::RUN_ENDED_TOPIC.into();
        let payload_type = Ustr::from(crate::manifest::RUN_ENDED_PAYLOAD_TYPE);
        let ts_publish = UnixNanos::from(ts_init + 1);
        let ts_init = UnixNanos::from(ts_init);
        let hash = compute_entry_hash(
            seq,
            ts_init,
            ts_publish,
            topic.as_ref(),
            payload_type.as_str(),
            &payload,
            &headers,
        );
        EventStoreEntry::new(
            hash,
            seq,
            headers,
            topic,
            payload_type,
            payload,
            ts_init,
            ts_publish,
        )
    }

    fn canonical_run_ended(seq: u64, ts_init: u64) -> EventStoreEntry {
        run_ended_entry(seq, ts_init, Bytes::new(), Headers::empty())
    }

    /// Test-only wrapper that delegates every call to an inner backend except the
    /// manifest, which it returns verbatim, and (optionally) the high-watermark.
    /// Lets unit tests drive manifest-mismatch and trailing-gap findings the
    /// public `MemoryBackend` API would normalize away on seal.
    struct ManifestOverrideBackend {
        inner: MemoryBackend,
        manifest_override: RunManifest,
        high_watermark_override: Option<u64>,
    }

    impl ManifestOverrideBackend {
        fn new(inner: MemoryBackend, manifest_override: RunManifest) -> Self {
            Self {
                inner,
                manifest_override,
                high_watermark_override: None,
            }
        }

        fn with_high_watermark(mut self, hwm: u64) -> Self {
            self.high_watermark_override = Some(hwm);
            self
        }
    }

    impl EventStore for ManifestOverrideBackend {
        fn open_run(&mut self, m: RunManifest) -> Result<(), EventStoreError> {
            self.inner.open_run(m)
        }

        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.inner.append_batch(entries)
        }

        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.inner.scan_range(from, to, direction)
        }

        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.inner.scan_seq(seq)
        }

        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.inner.lookup(kind, key)
        }

        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.inner.iter_index_keys(kind)
        }

        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.inner.seal(status)
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            Ok(self.manifest_override.clone())
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            if let Some(hwm) = self.high_watermark_override {
                return Ok(hwm);
            }
            self.inner.high_watermark()
        }
    }

    #[fixture]
    fn open_backend() -> MemoryBackend {
        let mut backend = MemoryBackend::new();
        backend
            .open_run(manifest("1700000000-aaaa1111"))
            .expect("open run");
        backend
    }

    fn verifier_for(backend: MemoryBackend) -> Verifier {
        Verifier::new(Box::new(backend))
    }

    #[rstest]
    fn attacker_controlled_max_high_watermark_is_rejected_before_scan(open_backend: MemoryBackend) {
        let manifest = open_backend.manifest().expect("manifest");
        let backend =
            ManifestOverrideBackend::new(open_backend, manifest).with_high_watermark(u64::MAX);

        let error = Verifier::new(Box::new(backend))
            .verify()
            .expect_err("unbounded sequence space must fail closed");

        assert!(matches!(
            error,
            VerifyError::LimitExceeded {
                resource: VerifyLimitResource::HighWatermark,
                observed: u64::MAX,
                limit: DEFAULT_VERIFY_MAX_HIGH_WATERMARK,
            }
        ));
    }

    #[rstest]
    fn stored_entry_budget_is_enforced_before_scan(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 11, Vec::new()),
            ])
            .expect("append");
        let limits = VerifyLimits {
            max_entries: 1,
            ..VerifyLimits::default()
        };

        let error = Verifier::with_limits(Box::new(open_backend), limits)
            .verify()
            .expect_err("entry budget must fail closed");

        assert!(matches!(
            error,
            VerifyError::LimitExceeded {
                resource: VerifyLimitResource::Entries,
                observed: 2,
                limit: 1,
            }
        ));
    }

    #[rstest]
    fn combined_index_budget_is_enforced_before_scan(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[append_with(
                1,
                10,
                vec![IndexKey::new(IndexKind::ClientOrderId, "O-1".to_string())],
            )])
            .expect("append");
        let limits = VerifyLimits {
            max_index_entries: 0,
            ..VerifyLimits::default()
        };

        let error = Verifier::with_limits(Box::new(open_backend), limits)
            .verify()
            .expect_err("index budget must fail closed");

        assert!(matches!(
            error,
            VerifyError::LimitExceeded {
                resource: VerifyLimitResource::IndexEntries,
                observed: 1,
                limit: 0,
            }
        ));
    }

    #[rstest]
    fn clean_run_reports_no_findings(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 11, Vec::new()),
                AppendEntry::without_indices(canonical_run_ended(3, 12)),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        // Lock the canonical clean case to zero findings exactly: any spurious
        // additional finding must fail this test rather than slip past is_clean()
        // matchers in the more targeted suites below.
        assert!(report.is_clean(), "findings was: {:?}", report.findings);
        assert_eq!(report.findings.len(), 0);
        assert_eq!(report.high_watermark, 3);
        assert_eq!(report.entries_scanned, 3);
        assert_eq!(report.status, RunStatus::Ended);
        assert!(report.store_digest.is_some());
    }

    #[rstest]
    fn ended_without_terminal_marker_is_a_finding(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 11, Vec::new()),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        let reason = find_manifest_mismatch(&report.findings, ManifestField::LifecycleMarker);
        assert!(reason.contains("missing a canonical RunEnded"), "{reason}");
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn non_final_duplicate_run_ended_is_a_finding(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(canonical_run_ended(2, 11)),
                AppendEntry::without_indices(canonical_run_ended(3, 12)),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        let reason = find_manifest_mismatch(&report.findings, ManifestField::LifecycleMarker);
        assert!(reason.contains("seq=2"), "{reason}");
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn terminal_run_ended_with_payload_is_a_finding(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(run_ended_entry(
                    2,
                    11,
                    Bytes::from_static(b"forged"),
                    Headers::empty(),
                )),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(report.findings.iter().any(|finding| matches!(
            finding,
            VerifyFinding::ManifestMismatch {
                kind: ManifestField::LifecycleMarker,
                reason,
            } if reason.contains("seq=2")
        )));
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn terminal_run_ended_with_headers_is_a_finding(mut open_backend: MemoryBackend) {
        let headers = Headers {
            correlation_id: Some(UUID4::new()),
            causation_id: None,
        };
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(run_ended_entry(2, 11, Bytes::new(), headers)),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(report.findings.iter().any(|finding| matches!(
            finding,
            VerifyFinding::ManifestMismatch {
                kind: ManifestField::LifecycleMarker,
                reason,
            } if reason.contains("seq=2")
        )));
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    #[case::client(IndexKind::ClientOrderId)]
    #[case::venue(IndexKind::VenueOrderId)]
    fn terminal_run_ended_with_sidecar_index_is_a_finding(
        mut open_backend: MemoryBackend,
        #[case] kind: IndexKind,
    ) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::new(
                    canonical_run_ended(2, 11),
                    vec![IndexKey::new(kind, "forged-index".to_string())],
                ),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(report.findings.iter().any(|finding| matches!(
            finding,
            VerifyFinding::ManifestMismatch {
                kind: ManifestField::LifecycleMarker,
                reason,
            } if reason.contains("sidecar index") && reason.contains("forged-index")
        )));
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    #[case::client(IndexKind::ClientOrderId)]
    #[case::venue(IndexKind::VenueOrderId)]
    fn run_started_with_sidecar_index_is_a_finding(
        mut open_backend: MemoryBackend,
        #[case] kind: IndexKind,
    ) {
        open_backend
            .append_batch(&[
                AppendEntry::new(
                    build_entry(1, Headers::empty(), 10),
                    vec![IndexKey::new(kind, "forged-start-index".to_string())],
                ),
                AppendEntry::without_indices(canonical_run_ended(2, 11)),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(report.findings.iter().any(|finding| matches!(
            finding,
            VerifyFinding::ManifestMismatch {
                kind: ManifestField::LifecycleMarker,
                reason,
            } if reason.contains("RunStarted") && reason.contains("forged-start-index")
        )));
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    #[case::topic_only(true)]
    #[case::tag_only(false)]
    fn partial_run_ended_topic_tag_is_a_finding(
        mut open_backend: MemoryBackend,
        #[case] keep_topic: bool,
    ) {
        let mut terminal = canonical_run_ended(2, 11);
        if keep_topic {
            terminal.payload_type = Ustr::from("SubmitOrder");
        } else {
            terminal.topic = "exec.command.SubmitOrder".into();
        }
        terminal.entry_hash = terminal.recompute_hash();
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(terminal),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        let reason = find_manifest_mismatch(&report.findings, ManifestField::LifecycleMarker);
        assert!(reason.contains("reserved lifecycle topic/tag"), "{reason}");
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn crashed_recovered_cannot_carry_canonical_terminal_marker(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(canonical_run_ended(2, 11)),
            ])
            .expect("append");
        open_backend
            .seal(RunStatus::CrashedRecovered)
            .expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        let reason = find_manifest_mismatch(&report.findings, ManifestField::LifecycleMarker);
        assert!(reason.contains("CrashedRecovered"), "{reason}");
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn later_run_started_marker_is_a_finding(mut open_backend: MemoryBackend) {
        let run_manifest = open_backend.manifest().expect("manifest");
        let mut duplicate = run_started_entry(&run_manifest);
        duplicate.seq = 2;
        duplicate.ts_init = UnixNanos::from(11);
        duplicate.ts_publish = UnixNanos::from(12);
        duplicate.entry_hash = duplicate.recompute_hash();
        open_backend
            .append_batch(&[
                AppendEntry::without_indices(run_started_entry(&run_manifest)),
                AppendEntry::without_indices(duplicate),
                AppendEntry::without_indices(canonical_run_ended(3, 12)),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        let reason = find_manifest_mismatch(&report.findings, ManifestField::LifecycleMarker);
        assert!(reason.contains("seq=2"), "{reason}");
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn clean_store_digest_binds_snapshot_anchor() {
        let mut first = MemoryBackend::new();
        first
            .open_run(manifest("1700000000-aaaa1111"))
            .expect("open first run");
        first
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(canonical_run_ended(2, 11)),
            ])
            .expect("append first run");
        first
            .record_snapshot_anchor(SnapshotAnchor::new(2, "cache://first", "blake3:first"))
            .expect("anchor first run");
        first.seal(RunStatus::Ended).expect("seal first run");

        let mut second = MemoryBackend::new();
        second
            .open_run(manifest("1700000000-aaaa1111"))
            .expect("open second run");
        second
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::without_indices(canonical_run_ended(2, 11)),
            ])
            .expect("append second run");
        second
            .record_snapshot_anchor(SnapshotAnchor::new(2, "cache://second", "blake3:second"))
            .expect("anchor second run");
        second.seal(RunStatus::Ended).expect("seal second run");

        let first = verifier_for(first).verify().expect("verify first run");
        let second = verifier_for(second).verify().expect("verify second run");

        assert!(first.is_clean());
        assert!(second.is_clean());
        assert_ne!(first.store_digest, second.store_digest);
    }

    #[rstest]
    fn verifier_rejects_backend_manifest_with_unsupported_entry_schema(
        open_backend: MemoryBackend,
    ) {
        let mut future = open_backend.manifest().unwrap();
        future.schema_version = crate::EVENT_STORE_ENTRY_SCHEMA_VERSION + 1;
        let verifier = Verifier::new(Box::new(ManifestOverrideBackend::new(open_backend, future)));

        assert!(matches!(
            verifier.verify(),
            Err(VerifyError::Backend(
                EventStoreError::UnsupportedSchemaVersion { .. }
            ))
        ));
    }

    #[rstest]
    fn empty_run_reports_missing_run_header(mut open_backend: MemoryBackend) {
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(report.findings.iter().any(|finding| matches!(
            finding,
            VerifyFinding::ManifestMismatch {
                kind: ManifestField::RunHeaderCommitment,
                ..
            }
        )));
        assert_eq!(report.entries_scanned, 0);
        assert!(report.store_digest.is_none());
    }

    #[rstest]
    fn replacing_manifest_provenance_breaks_run_header_commitment(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[append_with(1, 10, Vec::new())])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let mut replaced = open_backend.manifest().expect("manifest");
        replaced.provenance.promotion = Some(nautilus_provenance::ArtifactRef::new(
            nautilus_provenance::ArtifactId::new("promotion/replaced").unwrap(),
            nautilus_provenance::ContentDigest::digest_bytes(b"replacement"),
        ));
        let report = Verifier::new(Box::new(ManifestOverrideBackend::new(
            open_backend,
            replaced,
        )))
        .verify()
        .expect("verify");

        let reason = find_manifest_mismatch(&report.findings, ManifestField::RunHeaderCommitment);
        assert!(reason.contains("disagrees"), "reason was: {reason}");
    }

    #[rstest]
    fn malformed_run_started_payload_is_a_finding(mut open_backend: MemoryBackend) {
        let mut entry = run_started_entry(&open_backend.manifest().expect("manifest"));
        entry.payload = Bytes::from_static(b"not-a-run-header");
        entry.entry_hash = entry.recompute_hash();
        open_backend
            .append_batch(&[AppendEntry::without_indices(entry)])
            .expect("append");

        let report = verifier_for(open_backend).verify().expect("verify");
        let reason = find_manifest_mismatch(&report.findings, ManifestField::RunHeaderCommitment);
        assert!(reason.contains("failed to decode"), "reason was: {reason}");
    }

    #[rstest]
    fn hash_mismatch_surfaces_per_seq(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[append_with(1, 10, Vec::new())])
            .expect("append");
        let mut tampered = build_entry(2, Headers::empty(), 11);
        tampered.payload = Bytes::from_static(b"\xFF");
        open_backend
            .append_batch(&[AppendEntry::without_indices(tampered)])
            .expect("append");
        open_backend
            .append_batch(&[append_with(3, 12, Vec::new())])
            .expect("append");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(
            report
                .findings
                .iter()
                .any(|f| matches!(f, VerifyFinding::HashMismatch { seq: 2 })),
            "findings was: {:?}",
            report.findings,
        );
        assert_eq!(report.entries_scanned, 3);
        assert_eq!(report.high_watermark, 3);
    }

    #[rstest]
    fn multiple_hash_mismatches_all_surface(mut open_backend: MemoryBackend) {
        // Confirms the verifier walks past hash mismatches instead of bailing on the
        // first hit: seq=2 and seq=4 are both tampered, and both must appear in the
        // report.
        for seq in 1..=4u64 {
            let mut entry = build_entry(seq, Headers::empty(), 10 + seq);
            if seq == 2 || seq == 4 {
                entry.payload = Bytes::from_static(b"\xFF");
            }
            open_backend
                .append_batch(&[AppendEntry::without_indices(entry)])
                .expect("append");
        }

        let report = verifier_for(open_backend).verify().expect("verify");

        let mismatch_seqs: Vec<u64> = report
            .findings
            .iter()
            .filter_map(|f| match f {
                VerifyFinding::HashMismatch { seq } => Some(*seq),
                _ => None,
            })
            .collect();
        assert_eq!(mismatch_seqs, vec![2, 4]);
    }

    #[rstest]
    fn client_order_id_index_clean_when_target_resolves(mut open_backend: MemoryBackend) {
        open_backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::new(
                    build_entry(2, Headers::empty(), 11),
                    vec![IndexKey::new(IndexKind::ClientOrderId, "O-1".to_string())],
                ),
                AppendEntry::without_indices(canonical_run_ended(3, 12)),
            ])
            .expect("append");
        open_backend.seal(RunStatus::Ended).expect("seal");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(report.is_clean(), "findings was: {:?}", report.findings);
    }

    #[rstest]
    #[case::client_order_id(IndexKind::ClientOrderId)]
    #[case::venue_order_id(IndexKind::VenueOrderId)]
    fn entity_index_target_corrupted_drift(
        mut open_backend: MemoryBackend,
        #[case] kind: IndexKind,
    ) {
        // Stored entity-index entry points at seq=1 whose stored hash no longer
        // matches the recomputed hash. The verifier must surface TargetCorrupted
        // for both ClientOrderId and VenueOrderId so a drop of either kind from
        // the cross-check loop fails this test.
        let mut tampered = build_entry(1, Headers::empty(), 10);
        tampered.payload = Bytes::from_static(b"\xFF");
        open_backend
            .append_batch(&[AppendEntry::new(
                tampered,
                vec![IndexKey::new(kind, "K-1".to_string())],
            )])
            .expect("append");

        let report = verifier_for(open_backend).verify().expect("verify");

        assert!(
            report.findings.iter().any(|f| matches!(
                f,
                VerifyFinding::IndexDrift {
                    kind: drift_kind,
                    drift: IndexDrift::TargetCorrupted { stored_seq: 1 },
                    ..
                } if *drift_kind == kind
            )),
            "findings was: {:?}",
            report.findings,
        );
    }

    fn find_manifest_mismatch(findings: &[VerifyFinding], target: ManifestField) -> &str {
        findings
            .iter()
            .find_map(|f| match f {
                VerifyFinding::ManifestMismatch { kind, reason } if *kind == target => {
                    Some(reason.as_str())
                }
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!("expected ManifestMismatch({target:?}), findings was: {findings:?}")
            })
    }

    #[rstest]
    fn manifest_high_watermark_drift() {
        // Real durable hwm is 1, but the manifest reports 99. Verifier must surface
        // a HighWatermark mismatch whose reason carries both values so a swap of
        // observed and stored sides would fail this test.
        let mut inner = MemoryBackend::new();
        inner.open_run(manifest("run-hwm")).expect("open run");
        inner
            .append_batch(&[append_with(1, 10, Vec::new())])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let mut stale = inner.manifest().expect("manifest");
        stale.high_watermark = 99;
        let backend = ManifestOverrideBackend::new(inner, stale);

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");
        let reason = find_manifest_mismatch(&report.findings, ManifestField::HighWatermark);

        assert!(reason.contains("99"), "reason was: {reason}");
        assert!(reason.contains('1'), "reason was: {reason}");
    }

    #[rstest]
    fn manifest_end_ts_init_drift_when_sealed() {
        // Real durable max ts_init is 25, but the sealed manifest's end_ts_init
        // claims 99. The reason must surface both values; without that assertion,
        // a min/max swap inside record_entry would still pass.
        let mut inner = MemoryBackend::new();
        inner.open_run(manifest("run-end-ts")).expect("open run");
        inner
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 25, Vec::new()),
            ])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let mut drifted = inner.manifest().expect("manifest");
        drifted.end_ts_init = Some(UnixNanos::from(99));
        let backend = ManifestOverrideBackend::new(inner, drifted);

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");
        let reason = find_manifest_mismatch(&report.findings, ManifestField::EndTsInit);

        assert!(reason.contains("99"), "reason was: {reason}");
        assert!(reason.contains("25"), "reason was: {reason}");
    }

    #[rstest]
    fn manifest_end_ts_init_missing_when_sealed_with_entries() {
        // Sealed manifest forgot to record end_ts_init while the entry stream is
        // non-empty: validate_manifest's (None, Some) arm must fire and the
        // reason must carry the observed last ts_init.
        let mut inner = MemoryBackend::new();
        inner
            .open_run(manifest("run-end-ts-missing"))
            .expect("open run");
        inner
            .append_batch(&[append_with(1, 42, Vec::new())])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let mut drifted = inner.manifest().expect("manifest");
        drifted.end_ts_init = None;
        let backend = ManifestOverrideBackend::new(inner, drifted);

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");
        let reason = find_manifest_mismatch(&report.findings, ManifestField::EndTsInit);

        assert!(reason.contains("missing"), "reason was: {reason}");
        assert!(reason.contains("42"), "reason was: {reason}");
    }

    #[rstest]
    fn manifest_end_ts_init_set_on_sealed_empty_run() {
        // Sealed manifest carries end_ts_init even though the entry table is
        // empty: validate_manifest's (Some, None) arm must fire and the reason
        // must carry the spurious stored value.
        let mut inner = MemoryBackend::new();
        inner
            .open_run(manifest("run-end-ts-empty"))
            .expect("open run");
        inner.seal(RunStatus::Ended).expect("seal");

        let mut drifted = inner.manifest().expect("manifest");
        drifted.end_ts_init = Some(UnixNanos::from(77));
        let backend = ManifestOverrideBackend::new(inner, drifted);

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");
        let reason = find_manifest_mismatch(&report.findings, ManifestField::EndTsInit);

        assert!(reason.contains("77"), "reason was: {reason}");
        assert!(reason.contains("empty"), "reason was: {reason}");
    }

    #[rstest]
    fn manifest_start_ts_init_drift() {
        // Earliest entry ts_init is 10, but the manifest's start_ts_init is 50.
        // Reason must carry both values so a flipped comparison or wrong-side
        // formatting fails the test.
        let mut inner = MemoryBackend::new();
        inner.open_run(manifest("run-start-ts")).expect("open run");
        inner
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 25, Vec::new()),
            ])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let mut drifted = inner.manifest().expect("manifest");
        drifted.start_ts_init = UnixNanos::from(50);
        let backend = ManifestOverrideBackend::new(inner, drifted);

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");
        let reason = find_manifest_mismatch(&report.findings, ManifestField::StartTsInit);

        assert!(reason.contains("50"), "reason was: {reason}");
        assert!(reason.contains("10"), "reason was: {reason}");
    }

    #[rstest]
    fn trailing_gap_surfaces_when_last_seqs_missing() {
        // Inner backend holds seqs 1..=3, but both the manifest and the advertised
        // high-watermark claim 5. The verifier must walk to seq=5, find seqs 4-5
        // missing, and emit a single trailing GapRange{4,5}. Removing the
        // `flush_pending_gap(high_watermark + 1, ...)` call after the loop would
        // drop this finding entirely.
        let mut inner = MemoryBackend::new();
        inner
            .open_run(manifest("run-trailing-gap"))
            .expect("open run");
        inner
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 11, Vec::new()),
                append_with(3, 12, Vec::new()),
            ])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let mut drifted = inner.manifest().expect("manifest");
        drifted.high_watermark = 5;
        // Advertise hwm=5 on both sides so the HighWatermark mismatch path stays
        // quiet and the test pins only the trailing-gap behavior.
        let backend = ManifestOverrideBackend::new(inner, drifted).with_high_watermark(5);

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");

        let gaps: Vec<GapRange> = report
            .findings
            .iter()
            .filter_map(|f| match f {
                VerifyFinding::Gap { range } => Some(*range),
                _ => None,
            })
            .collect();
        assert_eq!(gaps, vec![GapRange { from: 4, to: 5 }]);
        assert_eq!(report.entries_scanned, 3);
        assert_eq!(report.high_watermark, 5);
    }

    /// Test backend that rewrites a single `scan_seq` result so the value's
    /// embedded seq disagrees with the requested table key. Lets the unit suite
    /// exercise the redb-only "row moved under wrong key" corruption class
    /// without setting up a real on-disk file.
    struct SeqRewriteBackend {
        inner: MemoryBackend,
        target_key: u64,
        substitute: EventStoreEntry,
    }

    impl EventStore for SeqRewriteBackend {
        fn open_run(&mut self, m: RunManifest) -> Result<(), EventStoreError> {
            self.inner.open_run(m)
        }
        fn append_batch(&mut self, e: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.inner.append_batch(e)
        }
        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.inner.scan_range(from, to, direction)
        }
        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            if seq == self.target_key {
                return Ok(Some(self.substitute.clone()));
            }
            self.inner.scan_seq(seq)
        }
        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.inner.lookup(kind, key)
        }
        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.inner.iter_index_keys(kind)
        }
        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.inner.seal(status)
        }
        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.inner.manifest()
        }
        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.inner.high_watermark()
        }
    }

    /// Test backend that fails `scan_seq` for one slot with a decode-style
    /// `Corrupted` error, exercising the accumulate-don't-abort contract.
    struct UndecodableBackend {
        inner: MemoryBackend,
        target_key: u64,
    }

    impl EventStore for UndecodableBackend {
        fn open_run(&mut self, m: RunManifest) -> Result<(), EventStoreError> {
            self.inner.open_run(m)
        }
        fn append_batch(&mut self, e: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.inner.append_batch(e)
        }
        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.inner.scan_range(from, to, direction)
        }
        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            if seq == self.target_key {
                return Err(EventStoreError::Corrupted(format!(
                    "decode entry seq={seq}: bad length prefix",
                )));
            }
            self.inner.scan_seq(seq)
        }
        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.inner.lookup(kind, key)
        }
        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.inner.iter_index_keys(kind)
        }
        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.inner.seal(status)
        }
        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.inner.manifest()
        }
        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.inner.high_watermark()
        }
    }

    #[rstest]
    fn undecodable_row_is_recorded_and_scan_continues() {
        // One row fails to decode at seq 2; the walk must continue so later entries
        // still count and indices pointing at the bad row classify as TargetCorrupted.
        let mut inner = MemoryBackend::new();
        let run_manifest = manifest("run-undecodable");
        inner.open_run(run_manifest.clone()).expect("open run");
        inner
            .append_batch(&[
                AppendEntry::without_indices(run_started_entry(&run_manifest)),
                AppendEntry::new(
                    build_entry(2, Headers::empty(), 11),
                    vec![IndexKey::new(IndexKind::ClientOrderId, "O-1".to_string())],
                ),
                AppendEntry::without_indices(canonical_run_ended(3, 12)),
            ])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let backend = UndecodableBackend {
            inner,
            target_key: 2,
        };

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");

        assert!(!report.is_clean());
        assert_eq!(report.entries_scanned, 3);
        assert_eq!(
            report.findings.len(),
            2,
            "findings was: {:?}",
            report.findings,
        );
        assert!(
            report
                .findings
                .iter()
                .any(|f| matches!(f, VerifyFinding::Undecodable { seq: 2, .. },)),
            "findings was: {:?}",
            report.findings,
        );
        assert!(
            report.findings.iter().any(|f| matches!(
                f,
                VerifyFinding::IndexDrift {
                    drift: IndexDrift::TargetCorrupted { stored_seq: 2 },
                    ..
                },
            )),
            "findings was: {:?}",
            report.findings,
        );
    }

    #[rstest]
    fn seq_mismatch_surfaces_when_row_value_disagrees_with_key() {
        // Row at table_key=2 holds the bytes of an entry whose embedded seq is 99.
        // The hash recomputes correctly (because the hash covers entry.seq=99),
        // so scan_seq returns Ok(Some(entry)) without raising HashMismatch. The
        // verifier must catch the key/embedded-seq divergence rather than mark
        // the slot clean.
        let mut inner = MemoryBackend::new();
        inner
            .open_run(manifest("run-seq-mismatch"))
            .expect("open run");
        inner
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 11, Vec::new()),
                append_with(3, 12, Vec::new()),
            ])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let substitute = build_entry(99, Headers::empty(), 11);
        let backend = SeqRewriteBackend {
            inner,
            target_key: 2,
            substitute,
        };

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");

        assert!(
            report.findings.iter().any(|f| matches!(
                f,
                VerifyFinding::SeqMismatch {
                    table_key: 2,
                    embedded_seq: 99,
                }
            )),
            "findings was: {:?}",
            report.findings,
        );
    }

    #[rstest]
    fn seq_mismatch_marks_target_corrupted_for_dependent_indices() {
        // Same row corruption as above, but the stored client_order_id index
        // points at the rewritten slot. The slot must be classified as corrupted
        // so the index drift surfaces TargetCorrupted rather than silently
        // accepting the lookup.
        let mut inner = MemoryBackend::new();
        inner
            .open_run(manifest("run-seq-mismatch-idx"))
            .expect("open run");
        inner
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                AppendEntry::new(
                    build_entry(2, Headers::empty(), 11),
                    vec![IndexKey::new(IndexKind::ClientOrderId, "O-1".to_string())],
                ),
            ])
            .expect("append");
        inner.seal(RunStatus::Ended).expect("seal");

        let substitute = build_entry(99, Headers::empty(), 11);
        let backend = SeqRewriteBackend {
            inner,
            target_key: 2,
            substitute,
        };

        let report = Verifier::new(Box::new(backend)).verify().expect("verify");

        assert!(
            report.findings.iter().any(|f| matches!(
                f,
                VerifyFinding::IndexDrift {
                    kind: IndexKind::ClientOrderId,
                    drift: IndexDrift::TargetCorrupted { stored_seq: 2 },
                    ..
                }
            )),
            "findings was: {:?}",
            report.findings,
        );
    }

    #[rstest]
    fn verify_propagates_no_run_open_as_error() {
        let backend = MemoryBackend::new();
        let verifier = Verifier::new(Box::new(backend));

        let err = verifier.verify().expect_err("must fail");

        match err {
            VerifyError::Backend(EventStoreError::Backend(msg)) => {
                assert!(msg.contains("no run open"), "msg was: {msg}");
            }
            VerifyError::Backend(other) => {
                panic!("expected Backend(no run open), was {other:?}")
            }
            other => panic!("expected Backend(no run open), was {other:?}"),
        }
    }
}
