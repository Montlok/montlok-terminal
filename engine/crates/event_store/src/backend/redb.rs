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

//! redb-backed [`EventStore`] implementation.
//!
//! One redb file per run at `<base>/<instance_id>/<run_id>.redb`. Every commit uses
//! [`Durability::Immediate`] so a crashed writer never leaves the in-flight tail visible
//! after reopen, and the high-watermark only advances after a durable acknowledgement.

use std::{
    fmt::Debug,
    fs::{self, OpenOptions},
    io::ErrorKind,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(not(unix))]
use std::time::SystemTime;

use nautilus_core::UnixNanos;
use redb::{
    CommitError, Database, DatabaseError, Durability, ReadOnlyDatabase, ReadTransaction,
    ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
    TableError, TransactionError, WriteTransaction,
};

use crate::{
    backend::{AppendEntry, EventStore, IndexKey, IndexKind, ScanDirection},
    codec,
    entry::EventStoreEntry,
    error::EventStoreError,
    format,
    manifest::{RUN_MANIFEST_WIRE_VERSION, RunManifest, RunStatus},
    snapshot::{SnapshotAnchor, validate_new_anchor},
};

const ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
const MANIFEST_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("manifest");
const CLIENT_ORDER_INDEX: TableDefinition<&str, u64> = TableDefinition::new("client_order_id_idx");
const VENUE_ORDER_INDEX: TableDefinition<&str, u64> = TableDefinition::new("venue_order_id_idx");
const SNAPSHOT_ANCHOR_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshot_anchor");

const MANIFEST_KEY: &str = "current";
const SNAPSHOT_ANCHOR_KEY: &str = "latest";

/// Default hard byte budget for opening one sealed redb run file (8 GiB).
pub const DEFAULT_MAX_SEALED_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Default hard byte budget for decoding a run manifest (4 MiB).
pub const DEFAULT_MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

/// On-disk [`EventStore`] backed by a per-run [`redb`] file.
///
/// One backend instance owns at most one open run at a time. Opening a fresh run creates
/// `<base>/<instance_id>/<run_id>.redb` and writes the manifest with status
/// [`RunStatus::Running`] before returning. Reopening a path whose manifest is still
/// [`RunStatus::Running`] returns [`EventStoreError::CrashedPredecessor`]; the caller seals
/// it as [`RunStatus::CrashedRecovered`] (or [`RunStatus::Quarantined`]) and then opens a new
/// run, mirroring the in-memory backend's contract.
#[derive(Debug)]
pub struct RedbBackend {
    base_dir: PathBuf,
    state: Option<RunState>,
}

#[derive(Debug)]
struct RunState {
    db: RunDatabase,
    manifest: RunManifest,
    high_watermark: u64,
    max_ts_init: UnixNanos,
    file_path: PathBuf,
}

enum RunDatabase {
    ReadWrite(Database),
    ReadOnly(ReadOnlyDatabase),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SealedFileFingerprint {
    len: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(not(unix))]
    modified: Option<SystemTime>,
}

impl Debug for RunDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadWrite(_) => f.write_str("RunDatabase::ReadWrite"),
            Self::ReadOnly(_) => f.write_str("RunDatabase::ReadOnly"),
        }
    }
}

impl RunDatabase {
    fn readable(&self) -> &dyn ReadableDatabase {
        match self {
            Self::ReadWrite(db) => db,
            Self::ReadOnly(db) => db,
        }
    }

    fn read_write(&self) -> Result<&Database, EventStoreError> {
        match self {
            Self::ReadWrite(db) => Ok(db),
            Self::ReadOnly(_) => Err(EventStoreError::Closed),
        }
    }

    fn begin_read(&self) -> Result<ReadTransaction, EventStoreError> {
        self.readable().begin_read().map_err(map_transaction_err)
    }
}

impl RedbBackend {
    /// Creates a new [`RedbBackend`] rooted at `base_dir`.
    ///
    /// The backend creates `<base_dir>/<instance_id>/` lazily on the first
    /// [`EventStore::open_run`] call.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            state: None,
        }
    }

    /// Returns the directory the backend writes run files to for `instance_id`.
    #[must_use]
    pub fn run_dir(&self, instance_id: &str) -> PathBuf {
        self.base_dir.join(instance_id)
    }

    /// Returns the on-disk path the backend uses for `(instance_id, run_id)`.
    #[must_use]
    pub fn run_path(&self, instance_id: &str, run_id: &str) -> PathBuf {
        self.run_dir(instance_id).join(format!("{run_id}.redb"))
    }

    /// Returns the path of the currently open run file.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when no run is open.
    pub fn current_path(&self) -> Result<&Path, EventStoreError> {
        Ok(self.state()?.file_path.as_path())
    }

    /// Opens the sealed run file at `<base>/<instance_id>/<run_id>.redb` for read-only replay.
    ///
    /// # Design
    ///
    /// The standard [`EventStore::open_run`] path rejects sealed files: that is the
    /// crash-recovery guard, a successor must not silently reopen a predecessor's log
    /// without going through seal. Event-store replay is the legitimate case for touching
    /// a sealed file, so the reader uses this constructor instead.
    ///
    /// The shared [`EventStore`] trait is held intentionally narrow and is locked by
    /// design; adding a sealed-open method to it would force the in-memory backend
    /// (whose sealed runs stay readable in place without a reopen step) to carry a
    /// useless second entry point, and would conflate the writer's open-or-recover
    /// lifecycle with the reader's pure read-only path. The sealed-open path therefore
    /// lives as a backend-specific constructor: each backend adds the entry points it
    /// actually needs. The resulting [`RedbBackend`] still implements [`EventStore`],
    /// so the reader composes over the locked trait without pulling in writer-only
    /// methods. [`crate::backend::MemoryBackend`] has no equivalent constructor: a
    /// sealed in-memory run keeps its state accessible to any reader holding the
    /// backend instance, and the reader receives that instance directly.
    ///
    /// The returned backend holds a read-only database handle, rejects
    /// [`EventStore::append_batch`] with [`EventStoreError::Closed`] (the manifest is
    /// already sealed), and exposes every read path: [`EventStore::scan_range`],
    /// [`EventStore::scan_seq`], [`EventStore::lookup`], and [`EventStore::manifest`].
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when the run file does not exist or its
    /// status is not a sealed terminal state (use [`EventStore::open_run`] for that
    /// path); [`EventStoreError::Corrupted`] when the run file lacks a manifest or
    /// fails to decode.
    pub fn open_sealed(
        base_dir: impl Into<PathBuf>,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Self, EventStoreError> {
        Self::open_sealed_with_max_file_bytes(
            base_dir,
            instance_id,
            run_id,
            DEFAULT_MAX_SEALED_FILE_BYTES,
        )
    }

    /// Opens a sealed run with an explicit hard file-size budget.
    ///
    /// The file's size and metadata identity are sampled before and after redb opens it. A file
    /// that exceeds `max_file_bytes` or changes identity during that interval is rejected before
    /// any verifier scan begins.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::FileSizeLimitExceeded`] when the file is too large,
    /// [`EventStoreError::FileChangedDuringOpen`] when its metadata identity changes, or the same
    /// structural/open errors as [`Self::open_sealed`].
    pub fn open_sealed_with_max_file_bytes(
        base_dir: impl Into<PathBuf>,
        instance_id: &str,
        run_id: &str,
        max_file_bytes: u64,
    ) -> Result<Self, EventStoreError> {
        Self::open_sealed_with_read_limits(
            base_dir,
            instance_id,
            run_id,
            max_file_bytes,
            DEFAULT_MAX_MANIFEST_BYTES,
        )
    }

    /// Opens a sealed run with explicit file and manifest-row budgets.
    ///
    /// # Errors
    ///
    /// Returns a typed size-limit error before manifest decode, or the same structural errors as
    /// [`Self::open_sealed`].
    pub fn open_sealed_with_read_limits(
        base_dir: impl Into<PathBuf>,
        instance_id: &str,
        run_id: &str,
        max_file_bytes: u64,
        max_manifest_bytes: u64,
    ) -> Result<Self, EventStoreError> {
        validate_path_identity("instance_id", instance_id)?;
        validate_path_identity("run_id", run_id)?;
        let base = base_dir.into();
        let path = base.join(instance_id).join(format!("{run_id}.redb"));
        Self::open_sealed_path(
            base,
            path,
            Some((instance_id, run_id)),
            max_file_bytes,
            max_manifest_bytes,
        )
    }

    /// Opens a sealed redb run file directly by path for read-only replay or verification.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when the run file does not exist or its
    /// status is not a sealed terminal state (use [`EventStore::open_run`] for that
    /// path); [`EventStoreError::Corrupted`] when the run file lacks a manifest or
    /// fails to decode.
    pub fn open_sealed_file(path: impl Into<PathBuf>) -> Result<Self, EventStoreError> {
        Self::open_sealed_file_with_max_file_bytes(path, DEFAULT_MAX_SEALED_FILE_BYTES)
    }

    /// Opens a sealed run file directly with an explicit hard file-size budget.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::FileSizeLimitExceeded`] when the file is too large,
    /// [`EventStoreError::FileChangedDuringOpen`] when its metadata identity changes, or the same
    /// structural/open errors as [`Self::open_sealed_file`].
    pub fn open_sealed_file_with_max_file_bytes(
        path: impl Into<PathBuf>,
        max_file_bytes: u64,
    ) -> Result<Self, EventStoreError> {
        Self::open_sealed_file_with_read_limits(path, max_file_bytes, DEFAULT_MAX_MANIFEST_BYTES)
    }

    /// Opens a sealed run file with explicit file and manifest-row budgets.
    ///
    /// # Errors
    ///
    /// Returns a typed size-limit error before manifest decode, or the same structural errors as
    /// [`Self::open_sealed_file`].
    pub fn open_sealed_file_with_read_limits(
        path: impl Into<PathBuf>,
        max_file_bytes: u64,
        max_manifest_bytes: u64,
    ) -> Result<Self, EventStoreError> {
        let path = path.into();
        let base = path
            .parent()
            .and_then(Path::parent)
            .map_or_else(PathBuf::new, Path::to_path_buf);
        Self::open_sealed_path(base, path, None, max_file_bytes, max_manifest_bytes)
    }

    fn open_sealed_path(
        base: PathBuf,
        path: PathBuf,
        expected_identity: Option<(&str, &str)>,
        max_file_bytes: u64,
        max_manifest_bytes: u64,
    ) -> Result<Self, EventStoreError> {
        let before = sealed_file_fingerprint(&path, max_file_bytes)?;
        let db = match ReadOnlyDatabase::open(&path) {
            Ok(db) => db,
            Err(DatabaseError::Storage(StorageError::Io(error)))
                if error.kind() == ErrorKind::NotFound =>
            {
                return Err(EventStoreError::Backend(format!(
                    "no run file at {}",
                    path.display()
                )));
            }
            Err(error) => return Err(map_read_only_database_err(error)),
        };
        let after = sealed_file_fingerprint(&path, max_file_bytes)?;
        if before != after {
            return Err(EventStoreError::FileChangedDuringOpen {
                path: path.display().to_string(),
            });
        }
        format::verify_store_format(&db)?;
        let manifest = Self::read_manifest_bounded(&db, max_manifest_bytes)?.ok_or_else(|| {
            EventStoreError::Corrupted(format!(
                "missing manifest in run file at {}",
                path.display()
            ))
        })?;
        validate_manifest_path_identity(&path, &manifest, expected_identity)?;

        if !manifest.is_sealed() {
            return Err(EventStoreError::Backend(format!(
                "run file at {} is not sealed, status was {:?}",
                path.display(),
                manifest.status,
            )));
        }
        // A sealed reader must not decode the table before the verifier has applied its
        // entry/HWM budgets. Only read the final B-tree key here; verification computes its own
        // timestamp bounds while scanning after preflight. Writable crash recovery still uses
        // `compute_progress`, because it needs the maximum timestamp before accepting appends.
        let high_watermark = Self::read_high_watermark(&db)?;

        Ok(Self {
            base_dir: base,
            state: Some(RunState {
                db: RunDatabase::ReadOnly(db),
                manifest,
                high_watermark,
                max_ts_init: UnixNanos::default(),
                file_path: path,
            }),
        })
    }

    /// Lists the manifests of every run file under `<base_dir>/<instance_id>/*.redb`.
    ///
    /// Used by the reader for forensics navigation across runs without requiring an
    /// active backend instance per run. The result is sorted by `start_ts_init` so
    /// chronologically-newer runs appear last.
    ///
    /// Opens each run file with a read-only database handle. A run file whose process
    /// died hard (kill, OOM, power loss) lacks redb's allocator-state table and refuses
    /// the read-only open; the listing falls back to a writable open, which performs
    /// redb's repair pass and leaves the file readable again. Files that still cannot
    /// be opened or that lack a manifest are skipped with a logged error so one
    /// current-format damaged file cannot block recovery or retention over the healthy
    /// runs; such files never become recovery parents or reclaim candidates and are
    /// left in place for manual inspection. Unsupported store formats are returned as
    /// errors rather than skipped because they require operator action.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when the directory iterator fails, or
    /// [`EventStoreError::Corrupted`] when a run file uses an unsupported store format.
    pub fn list_runs(
        base_dir: &Path,
        instance_id: &str,
    ) -> Result<Vec<RunManifest>, EventStoreError> {
        Self::list_runs_with_read_limits(
            base_dir,
            instance_id,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
        )
    }

    /// Lists run manifests after file-count, individual-file, and manifest-row preflight.
    ///
    /// This is the recovery-worker entry point. Every O(N) manifest decode is preceded by a
    /// metadata-only count/size budget; each singleton manifest row is bounded before decode.
    pub(crate) fn list_runs_with_read_limits(
        base_dir: &Path,
        instance_id: &str,
        max_files: u64,
        max_file_bytes: u64,
        max_manifest_bytes: u64,
        max_total_manifest_bytes: u64,
    ) -> Result<Vec<RunManifest>, EventStoreError> {
        validate_path_identity("instance_id", instance_id)?;
        let dir = base_dir.join(instance_id);
        let entries = match fs::read_dir(&dir) {
            Ok(it) => it,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(EventStoreError::Backend(format!(
                    "read_dir {}: {e}",
                    dir.display()
                )));
            }
        };

        let mut paths = Vec::new();
        let mut directory_entries = 0_u64;

        for entry in entries {
            directory_entries = directory_entries.checked_add(1).ok_or_else(|| {
                EventStoreError::Backend("recovery directory-entry count overflow".to_string())
            })?;
            if directory_entries > 4_096 {
                return Err(EventStoreError::Backend(
                    "recovery directory-entry count exceeds limit 4096".to_string(),
                ));
            }
            let entry = entry.map_err(|e| {
                EventStoreError::Backend(format!("read_dir entry in {}: {e}", dir.display()))
            })?;
            let path = entry.path();

            if !is_run_file(&path) {
                continue;
            }

            let observed_files = u64::try_from(paths.len())
                .map_err(|error| EventStoreError::Backend(error.to_string()))?
                .checked_add(1)
                .ok_or_else(|| EventStoreError::Backend("run-file count overflow".to_string()))?;
            if observed_files > max_files {
                return Err(EventStoreError::Backend(format!(
                    "event-store run-file count {observed_files} exceeds recovery limit {max_files}"
                )));
            }
            sealed_file_fingerprint(&path, max_file_bytes)?;
            paths.push(path);
        }

        let mut manifests = Vec::new();
        let mut total_manifest_bytes = 0_u64;
        for path in paths {
            match Self::read_run_manifest_bounded_with_size(
                &path,
                max_manifest_bytes,
                &mut total_manifest_bytes,
                max_total_manifest_bytes,
            )
            .and_then(|(manifest, _)| {
                validate_manifest_path_identity(
                    &path,
                    &manifest,
                    Some((instance_id, &manifest.run_id)),
                )?;
                Ok(manifest)
            }) {
                Ok(manifest) => manifests.push(manifest),
                Err(e) if format::is_unsupported_store_format(&e) => return Err(e),
                Err(e) if is_unsupported_manifest(&e) => return Err(e),
                // Resource exhaustion makes the whole inventory ambiguous. Do not skip the
                // offending run and continue as if the inventory were complete.
                Err(e @ EventStoreError::MetadataSizeLimitExceeded { .. }) => return Err(e),
                Err(e) => {
                    log::error!("Skipping unreadable run file {}: {e}", path.display());
                }
            }
        }
        // Break start-time ties on the run id so parent selection and retention stay
        // deterministic: stable sort alone preserves the platform-dependent `read_dir`
        // order.
        manifests.sort_by(|a, b| {
            a.start_ts_init
                .cmp(&b.start_ts_init)
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        Ok(manifests)
    }

    #[cfg(test)]
    fn read_run_manifest(path: &Path) -> Result<RunManifest, EventStoreError> {
        let mut total_manifest_bytes = 0;
        Self::read_run_manifest_bounded_with_size(
            path,
            u64::MAX,
            &mut total_manifest_bytes,
            u64::MAX,
        )
        .map(|(manifest, _)| manifest)
    }

    fn read_run_manifest_bounded_with_size(
        path: &Path,
        max_manifest_bytes: u64,
        total_manifest_bytes: &mut u64,
        max_total_manifest_bytes: u64,
    ) -> Result<(RunManifest, u64), EventStoreError> {
        let manifest = match ReadOnlyDatabase::open(path) {
            Ok(db) => {
                Self::verify_listed_store_format(&db, path)?;
                Self::read_manifest_bounded_with_total(
                    &db,
                    max_manifest_bytes,
                    total_manifest_bytes,
                    max_total_manifest_bytes,
                )?
            }
            // Each durable commit deletes redb's allocator-state table and only a clean
            // `Database::drop` rewrites it, so a hard-killed process leaves a file the
            // read-only open refuses. A writable open repairs it for future opens.
            Err(DatabaseError::RepairAborted) => {
                log::warn!(
                    "Run file {} was not shut down cleanly, repairing",
                    path.display()
                );
                let db = Database::open(path).map_err(map_database_err)?;
                Self::verify_listed_store_format(&db, path)?;
                Self::read_manifest_bounded_with_total(
                    &db,
                    max_manifest_bytes,
                    total_manifest_bytes,
                    max_total_manifest_bytes,
                )?
            }
            Err(e) => return Err(map_read_only_database_err(e)),
        };
        manifest.ok_or_else(|| missing_manifest(path))
    }

    fn verify_listed_store_format<D: ReadableDatabase + ?Sized>(
        db: &D,
        path: &Path,
    ) -> Result<(), EventStoreError> {
        match format::verify_store_format(db) {
            Ok(()) => Ok(()),
            Err(e) if format::is_missing_store_format(&e) && !Self::manifest_row_exists(db)? => {
                Err(missing_manifest(path))
            }
            Err(e) => Err(e),
        }
    }

    fn manifest_row_exists<D: ReadableDatabase + ?Sized>(db: &D) -> Result<bool, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = match txn.open_table(MANIFEST_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(map_table_err(e)),
        };

        table
            .get(MANIFEST_KEY)
            .map(|value| value.is_some())
            .map_err(map_storage_err)
    }

    fn state(&self) -> Result<&RunState, EventStoreError> {
        self.state
            .as_ref()
            .ok_or_else(|| EventStoreError::Backend("no run open".to_string()))
    }

    fn state_mut(&mut self) -> Result<&mut RunState, EventStoreError> {
        self.state
            .as_mut()
            .ok_or_else(|| EventStoreError::Backend("no run open".to_string()))
    }

    fn initialize_fresh(db: &Database, manifest: &RunManifest) -> Result<(), EventStoreError> {
        let txn = begin_immediate_write(db)?;
        {
            txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
            txn.open_table(CLIENT_ORDER_INDEX).map_err(map_table_err)?;
            txn.open_table(VENUE_ORDER_INDEX).map_err(map_table_err)?;
            txn.open_table(SNAPSHOT_ANCHOR_TABLE)
                .map_err(map_table_err)?;
        }
        format::write_store_format(&txn)?;
        insert_run_manifest(&txn, manifest)?;
        txn.commit().map_err(map_commit_err)?;
        Ok(())
    }

    fn write_manifest(db: &Database, manifest: &RunManifest) -> Result<(), EventStoreError> {
        let txn = begin_immediate_write(db)?;
        insert_run_manifest(&txn, manifest)?;
        txn.commit().map_err(map_commit_err)?;
        Ok(())
    }

    fn read_manifest<D: ReadableDatabase + ?Sized>(
        db: &D,
    ) -> Result<Option<RunManifest>, EventStoreError> {
        Self::read_manifest_bounded(db, u64::MAX)
    }

    fn read_manifest_bounded<D: ReadableDatabase + ?Sized>(
        db: &D,
        max_row_bytes: u64,
    ) -> Result<Option<RunManifest>, EventStoreError> {
        Self::read_manifest_bounded_with_size(db, max_row_bytes)
            .map(|manifest| manifest.map(|(manifest, _)| manifest))
    }

    fn read_manifest_bounded_with_size<D: ReadableDatabase + ?Sized>(
        db: &D,
        max_row_bytes: u64,
    ) -> Result<Option<(RunManifest, u64)>, EventStoreError> {
        let mut total_manifest_bytes = 0;
        Self::read_manifest_bounded_with_total(
            db,
            max_row_bytes,
            &mut total_manifest_bytes,
            u64::MAX,
        )
    }

    fn read_manifest_bounded_with_total<D: ReadableDatabase + ?Sized>(
        db: &D,
        max_row_bytes: u64,
        total_manifest_bytes: &mut u64,
        max_total_manifest_bytes: u64,
    ) -> Result<Option<(RunManifest, u64)>, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = txn.open_table(MANIFEST_TABLE).map_err(map_table_err)?;
        let Some(value) = table.get(MANIFEST_KEY).map_err(map_storage_err)? else {
            return Ok(None);
        };
        let bytes = value.value();
        let observed = u64::try_from(bytes.len()).map_err(|error| {
            EventStoreError::Backend(format!("manifest length does not fit u64: {error}"))
        })?;
        if observed > max_row_bytes {
            return Err(EventStoreError::MetadataSizeLimitExceeded {
                resource: "manifest",
                observed,
                limit: max_row_bytes,
            });
        }
        let next_total = total_manifest_bytes.checked_add(observed).ok_or_else(|| {
            EventStoreError::Backend("recovery manifest byte count overflow".to_string())
        })?;
        if next_total > max_total_manifest_bytes {
            return Err(EventStoreError::MetadataSizeLimitExceeded {
                resource: "recovery manifest total",
                observed: next_total,
                limit: max_total_manifest_bytes,
            });
        }
        // Charge the raw row before decoding it. Undecodable manifests still consume the
        // recovery operation's cumulative budget and cannot trigger an unbounded decode loop.
        *total_manifest_bytes = next_total;
        let manifest = codec::decode_run_manifest(bytes).map_err(map_manifest_decode_err)?;
        Ok(Some((manifest, observed)))
    }

    fn read_snapshot_anchor<D: ReadableDatabase + ?Sized>(
        db: &D,
    ) -> Result<Option<SnapshotAnchor>, EventStoreError> {
        Self::read_snapshot_anchor_bounded(db, u64::MAX)
    }

    fn read_snapshot_anchor_bounded<D: ReadableDatabase + ?Sized>(
        db: &D,
        max_row_bytes: u64,
    ) -> Result<Option<SnapshotAnchor>, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = match txn.open_table(SNAPSHOT_ANCHOR_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(map_table_err(e)),
        };
        let Some(value) = table.get(SNAPSHOT_ANCHOR_KEY).map_err(map_storage_err)? else {
            return Ok(None);
        };
        let bytes = value.value();
        let observed = u64::try_from(bytes.len()).map_err(|error| {
            EventStoreError::Backend(format!("snapshot anchor length does not fit u64: {error}"))
        })?;
        if observed > max_row_bytes {
            return Err(EventStoreError::MetadataSizeLimitExceeded {
                resource: "snapshot anchor",
                observed,
                limit: max_row_bytes,
            });
        }
        let anchor = codec::decode_from_slice::<SnapshotAnchor>(bytes)
            .map_err(|e| EventStoreError::Corrupted(format!("decode snapshot anchor: {e}")))?;
        Ok(Some(anchor))
    }

    fn compute_progress<D: ReadableDatabase + ?Sized>(
        db: &D,
    ) -> Result<(u64, UnixNanos), EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;

        let Some((last_key, _)) = table.last().map_err(map_storage_err)? else {
            return Ok((0, UnixNanos::default()));
        };
        let high_watermark = last_key.value();

        // Walk the entry table once to recover the maximum `ts_init`. Memory.rs tracks this
        // across appends; on crash recovery we have nothing to fall back on, so we recompute
        // it from the durable rows. An undecodable row must not make the run unopenable:
        // max ts_init is best-effort, and the corruption itself surfaces on the scan paths,
        // where the recovery sweep quarantines the run.
        let mut max_ts = UnixNanos::default();
        let iter = table.iter().map_err(map_storage_err)?;

        for row in iter {
            let (key, value) = row.map_err(map_storage_err)?;
            let bytes = value.value();

            match codec::decode_from_slice::<EventStoreEntry>(bytes) {
                Ok(entry) => {
                    if entry.ts_init > max_ts {
                        max_ts = entry.ts_init;
                    }
                }
                Err(e) => {
                    log::error!("Undecodable entry at seq {} on load: {e}", key.value());
                }
            }
        }

        Ok((high_watermark, max_ts))
    }

    fn compute_progress_bounded<D: ReadableDatabase + ?Sized>(
        db: &D,
        max_high_watermark: u64,
        max_entries: u64,
        max_entry_bytes: u64,
        max_total_entry_bytes: u64,
    ) -> Result<(u64, UnixNanos), EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
        let high_watermark = table
            .last()
            .map_err(map_storage_err)?
            .map_or(0, |(key, _)| key.value());
        if high_watermark > max_high_watermark {
            return Err(EventStoreError::Backend(format!(
                "recovery high-watermark {high_watermark} exceeds limit {max_high_watermark}"
            )));
        }
        let row_count = table.len().map_err(map_storage_err)?;
        if row_count > max_entries {
            return Err(EventStoreError::Backend(format!(
                "recovery entry count {row_count} exceeds limit {max_entries}"
            )));
        }

        let mut max_ts = UnixNanos::default();
        let mut total_entry_bytes = 0_u64;
        for row in table.iter().map_err(map_storage_err)? {
            let (key, value) = row.map_err(map_storage_err)?;
            let bytes = value.value();
            let row_bytes = u64::try_from(bytes.len()).map_err(|error| {
                EventStoreError::Backend(format!(
                    "entry seq={} length does not fit u64: {error}",
                    key.value()
                ))
            })?;
            if row_bytes > max_entry_bytes {
                return Err(EventStoreError::EntrySizeLimitExceeded {
                    seq: key.value(),
                    observed: row_bytes,
                    limit: max_entry_bytes,
                });
            }
            total_entry_bytes = total_entry_bytes.checked_add(row_bytes).ok_or_else(|| {
                EventStoreError::Backend("recovery decoded-byte count overflow".to_string())
            })?;
            if total_entry_bytes > max_total_entry_bytes {
                return Err(EventStoreError::Backend(format!(
                    "recovery decoded entry bytes {total_entry_bytes} exceed limit {max_total_entry_bytes}"
                )));
            }
            match codec::decode_from_slice::<EventStoreEntry>(bytes) {
                Ok(entry) if entry.ts_init > max_ts => max_ts = entry.ts_init,
                Ok(_) => {}
                Err(error) => log::error!(
                    "Undecodable entry at seq {} during bounded recovery load: {error}",
                    key.value()
                ),
            }
        }
        Ok((high_watermark, max_ts))
    }

    /// Reopens one `Running` run for crash recovery under explicit pre-decode budgets.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_crashed_run_with_read_limits(
        &mut self,
        manifest: &RunManifest,
        max_high_watermark: u64,
        max_entries: u64,
        max_file_bytes: u64,
        max_entry_bytes: u64,
        max_total_entry_bytes: u64,
        max_manifest_bytes: u64,
    ) -> Result<(), EventStoreError> {
        manifest.validate_schema_version()?;
        validate_path_identity("instance_id", &manifest.instance_id)?;
        validate_path_identity("run_id", &manifest.run_id)?;
        let path = self.run_path(&manifest.instance_id, &manifest.run_id);
        sealed_file_fingerprint(&path, max_file_bytes)?;
        let db = Database::open(&path).map_err(map_database_err)?;
        format::verify_store_format(&db)?;
        let on_disk = Self::read_manifest_bounded(&db, max_manifest_bytes)?.ok_or_else(|| {
            EventStoreError::Corrupted(format!(
                "missing manifest in existing run file at {}",
                path.display()
            ))
        })?;
        validate_manifest_path_identity(
            &path,
            &on_disk,
            Some((&manifest.instance_id, &manifest.run_id)),
        )?;
        if on_disk.status != RunStatus::Running {
            return Err(EventStoreError::Backend(format!(
                "recovery run file at {} was not Running, status was {:?}",
                path.display(),
                on_disk.status
            )));
        }
        let (high_watermark, max_ts_init) = Self::compute_progress_bounded(
            &db,
            max_high_watermark,
            max_entries,
            max_entry_bytes,
            max_total_entry_bytes,
        )?;
        sealed_file_fingerprint(&path, max_file_bytes)?;
        let mut recovered = on_disk;
        recovered.high_watermark = high_watermark;
        self.state = Some(RunState {
            db: RunDatabase::ReadWrite(db),
            manifest: recovered,
            high_watermark,
            max_ts_init,
            file_path: path,
        });
        Err(EventStoreError::CrashedPredecessor)
    }

    fn read_high_watermark<D: ReadableDatabase + ?Sized>(db: &D) -> Result<u64, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
        Ok(table
            .last()
            .map_err(map_storage_err)?
            .map_or(0, |(last_key, _)| last_key.value()))
    }
}

impl EventStore for RedbBackend {
    fn open_run(&mut self, mut manifest: RunManifest) -> Result<(), EventStoreError> {
        manifest.validate_schema_version()?;
        validate_path_identity("instance_id", &manifest.instance_id)?;
        validate_path_identity("run_id", &manifest.run_id)?;
        if let Some(state) = &self.state {
            if matches!(state.db, RunDatabase::ReadOnly(_)) {
                return Err(EventStoreError::Closed);
            }

            if !state.manifest.is_sealed() {
                return Err(EventStoreError::CrashedPredecessor);
            }
        }

        let dir = self.run_dir(&manifest.instance_id);
        fs::create_dir_all(&dir).map_err(|e| {
            let msg = format!("create dir {}: {e}", dir.display());

            if is_disk_pressure(e.kind()) {
                EventStoreError::Disk(msg)
            } else {
                EventStoreError::Backend(msg)
            }
        })?;
        let path = self.run_path(&manifest.instance_id, &manifest.run_id);
        let (db, created) = create_or_open_database(&path)?;

        if !created {
            format::verify_store_format(&db)?;
            let on_disk = Self::read_manifest(&db)?.ok_or_else(|| {
                EventStoreError::Corrupted(format!(
                    "missing manifest in existing run file at {}",
                    path.display()
                ))
            })?;
            validate_manifest_path_identity(
                &path,
                &on_disk,
                Some((&manifest.instance_id, &manifest.run_id)),
            )?;

            if !matches!(on_disk.status, RunStatus::Running) {
                return Err(EventStoreError::Backend(format!(
                    "run file at {} already sealed, status was {:?}",
                    path.display(),
                    on_disk.status
                )));
            }

            let (high_watermark, max_ts_init) = Self::compute_progress(&db)?;
            let mut recovered = on_disk;
            recovered.high_watermark = high_watermark;
            self.state = Some(RunState {
                db: RunDatabase::ReadWrite(db),
                manifest: recovered,
                high_watermark,
                max_ts_init,
                file_path: path,
            });
            return Err(EventStoreError::CrashedPredecessor);
        }

        manifest.status = RunStatus::Running;
        manifest.end_ts_init = None;
        manifest.high_watermark = 0;
        Self::initialize_fresh(&db, &manifest)?;

        self.state = Some(RunState {
            db: RunDatabase::ReadWrite(db),
            manifest,
            high_watermark: 0,
            max_ts_init: UnixNanos::default(),
            file_path: path,
        });
        Ok(())
    }

    fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
        let state = self.state_mut()?;

        if state.manifest.is_sealed() {
            return Err(EventStoreError::Closed);
        }

        if entries.is_empty() {
            return Ok(state.high_watermark);
        }

        let first_expected =
            state
                .high_watermark
                .checked_add(1)
                .ok_or_else(|| EventStoreError::OutOfOrder {
                    high_watermark: state.high_watermark,
                    seq: entries[0].entry.seq,
                })?;
        for (offset, append) in entries.iter().enumerate() {
            let offset = u64::try_from(offset).map_err(|_| EventStoreError::OutOfOrder {
                high_watermark: state.high_watermark,
                seq: append.entry.seq,
            })?;
            let expected =
                first_expected
                    .checked_add(offset)
                    .ok_or(EventStoreError::OutOfOrder {
                        high_watermark: state.high_watermark,
                        seq: append.entry.seq,
                    })?;
            if append.entry.seq != expected {
                // Atomically rejected: surface the durable high-watermark, not the within-batch
                // validation cursor, so callers that resync from this value never skip entries
                // that were never committed.
                return Err(EventStoreError::OutOfOrder {
                    high_watermark: state.high_watermark,
                    seq: append.entry.seq,
                });
            }
        }

        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|append| {
                codec::encode_to_vec(&append.entry).map_err(|e| {
                    EventStoreError::Backend(format!("encode entry seq={}: {e}", append.entry.seq))
                })
            })
            .collect::<Result<_, _>>()?;

        let db = state.db.read_write()?;
        let txn = begin_immediate_write(db)?;
        {
            let mut entries_table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
            let mut client_table = txn.open_table(CLIENT_ORDER_INDEX).map_err(map_table_err)?;
            let mut venue_table = txn.open_table(VENUE_ORDER_INDEX).map_err(map_table_err)?;

            for (append, bytes) in entries.iter().zip(encoded.iter()) {
                entries_table
                    .insert(append.entry.seq, bytes.as_slice())
                    .map_err(map_storage_err)?;

                for IndexKey { kind, key } in &append.index_keys {
                    let table = match kind {
                        IndexKind::ClientOrderId => &mut client_table,
                        IndexKind::VenueOrderId => &mut venue_table,
                    };
                    let already = table.get(key.as_str()).map_err(map_storage_err)?.is_some();

                    if !already {
                        table
                            .insert(key.as_str(), append.entry.seq)
                            .map_err(map_storage_err)?;
                    }
                }
            }
        }
        txn.commit().map_err(map_commit_err)?;

        let mut max_ts = state.max_ts_init;
        let mut new_hwm = state.high_watermark;

        for append in entries {
            if append.entry.ts_init > max_ts {
                max_ts = append.entry.ts_init;
            }
            new_hwm = append.entry.seq;
        }
        state.high_watermark = new_hwm;
        state.max_ts_init = max_ts;
        state.manifest.high_watermark = new_hwm;

        Ok(new_hwm)
    }

    fn scan_range(
        &self,
        from: u64,
        to: u64,
        direction: ScanDirection,
    ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
        let state = self.state()?;

        if from > to || from == 0 || state.high_watermark == 0 {
            return Ok(Vec::new());
        }

        let lo = from;
        let hi = to.min(state.high_watermark);

        if lo > hi {
            return Ok(Vec::new());
        }

        let txn = state.db.begin_read()?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;

        // hi is capped to high_watermark above, so every seq in [lo, hi] is supposed to be
        // present. redb iterates only existing keys, so a missing row inside this range
        // means a committed sequence has been lost (corruption, external tampering); we
        // surface Gap rather than silently shortening the result.
        let mut out = Vec::new();
        let mut expected = Some(lo);
        let iter = table.range(lo..=hi).map_err(map_storage_err)?;

        for row in iter {
            let (k, v) = row.map_err(map_storage_err)?;
            let seq = k.value();

            let Some(expected_seq) = expected else {
                return Err(EventStoreError::OutOfOrder {
                    high_watermark: state.high_watermark,
                    seq,
                });
            };
            if seq != expected_seq {
                return Err(EventStoreError::Gap {
                    prev: expected_seq.saturating_sub(1),
                    next: seq,
                    missing: expected_seq,
                });
            }
            let bytes = v.value();
            let entry = codec::decode_from_slice::<EventStoreEntry>(bytes)
                .map_err(|e| EventStoreError::Corrupted(format!("decode entry seq={seq}: {e}")))?;

            check_embedded_seq(seq, &entry)?;

            if entry.recompute_hash() != entry.entry_hash {
                return Err(EventStoreError::HashMismatch { seq });
            }
            out.push(entry);
            expected = seq.checked_add(1);
        }

        if let Some(expected) = expected.filter(|expected| *expected <= hi) {
            return Err(EventStoreError::Gap {
                prev: expected.saturating_sub(1),
                next: hi.saturating_add(1),
                missing: expected,
            });
        }

        if matches!(direction, ScanDirection::Reverse) {
            out.reverse();
        }
        Ok(out)
    }

    fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
        self.scan_seq_bounded(seq, u64::MAX)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    fn scan_seq_bounded(
        &self,
        seq: u64,
        max_row_bytes: u64,
    ) -> Result<Option<(EventStoreEntry, u64)>, EventStoreError> {
        let state = self.state()?;

        if seq == 0 || seq > state.high_watermark {
            return Ok(None);
        }

        let txn = state.db.begin_read()?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
        let Some(value) = table.get(seq).map_err(map_storage_err)? else {
            // seq is inside the watermark per the guard above, so the row must exist;
            // its absence is a committed-but-missing entry.
            return Err(EventStoreError::Gap {
                prev: seq.saturating_sub(1),
                next: seq.saturating_add(1),
                missing: seq,
            });
        };

        let bytes = value.value();
        let row_bytes = u64::try_from(bytes.len()).map_err(|error| {
            EventStoreError::Backend(format!("entry seq={seq} length does not fit u64: {error}"))
        })?;
        if row_bytes > max_row_bytes {
            return Err(EventStoreError::EntrySizeLimitExceeded {
                seq,
                observed: row_bytes,
                limit: max_row_bytes,
            });
        }
        let entry = codec::decode_from_slice::<EventStoreEntry>(bytes)
            .map_err(|e| EventStoreError::Corrupted(format!("decode entry seq={seq}: {e}")))?;

        check_embedded_seq(seq, &entry)?;

        if entry.recompute_hash() != entry.entry_hash {
            return Err(EventStoreError::HashMismatch { seq });
        }
        Ok(Some((entry, row_bytes)))
    }

    fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
        let state = self.state()?;
        let txn = state.db.begin_read()?;
        let definition = match kind {
            IndexKind::ClientOrderId => CLIENT_ORDER_INDEX,
            IndexKind::VenueOrderId => VENUE_ORDER_INDEX,
        };
        let table = txn.open_table(definition).map_err(map_table_err)?;
        let value = table.get(key).map_err(map_storage_err)?;
        Ok(value.map(|v| v.value()))
    }

    fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
        let state = self.state()?;
        let txn = state.db.begin_read()?;
        let definition = match kind {
            IndexKind::ClientOrderId => CLIENT_ORDER_INDEX,
            IndexKind::VenueOrderId => VENUE_ORDER_INDEX,
        };
        let table = txn.open_table(definition).map_err(map_table_err)?;
        let iter = table.iter().map_err(map_storage_err)?;
        let mut out = Vec::new();

        for row in iter {
            let (k, v) = row.map_err(map_storage_err)?;
            out.push((k.value().to_string(), v.value()));
        }
        Ok(out)
    }

    fn visit_index_keys(
        &self,
        kind: IndexKind,
        visitor: &mut dyn FnMut(&str, u64) -> bool,
    ) -> Result<(), EventStoreError> {
        let state = self.state()?;
        let txn = state.db.begin_read()?;
        let definition = match kind {
            IndexKind::ClientOrderId => CLIENT_ORDER_INDEX,
            IndexKind::VenueOrderId => VENUE_ORDER_INDEX,
        };
        let table = txn.open_table(definition).map_err(map_table_err)?;
        for row in table.iter().map_err(map_storage_err)? {
            let (key, seq) = row.map_err(map_storage_err)?;
            if !visitor(key.value(), seq.value()) {
                break;
            }
        }
        Ok(())
    }

    fn entry_count(&self) -> Result<u64, EventStoreError> {
        let txn = self.state()?.db.begin_read()?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
        table.len().map_err(map_storage_err)
    }

    fn index_entry_count(&self, kind: IndexKind) -> Result<u64, EventStoreError> {
        let txn = self.state()?.db.begin_read()?;
        let definition = match kind {
            IndexKind::ClientOrderId => CLIENT_ORDER_INDEX,
            IndexKind::VenueOrderId => VENUE_ORDER_INDEX,
        };
        let table = txn.open_table(definition).map_err(map_table_err)?;
        table.len().map_err(map_storage_err)
    }

    fn record_snapshot_anchor(&mut self, anchor: SnapshotAnchor) -> Result<(), EventStoreError> {
        let state = self.state_mut()?;

        if state.manifest.is_sealed() {
            return Err(EventStoreError::Closed);
        }

        let latest = Self::read_snapshot_anchor(state.db.readable())?;
        validate_new_anchor(&anchor, state.high_watermark, latest.as_ref())?;

        let bytes = codec::encode_to_vec(&anchor)
            .map_err(|e| EventStoreError::Backend(format!("encode snapshot anchor: {e}")))?;
        let db = state.db.read_write()?;
        let txn = begin_immediate_write(db)?;
        {
            let mut table = txn
                .open_table(SNAPSHOT_ANCHOR_TABLE)
                .map_err(map_table_err)?;
            table
                .insert(SNAPSHOT_ANCHOR_KEY, bytes.as_slice())
                .map_err(map_storage_err)?;
        }
        txn.commit().map_err(map_commit_err)?;
        Ok(())
    }

    fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
        Self::read_snapshot_anchor(self.state()?.db.readable())
    }

    fn latest_snapshot_anchor_bounded(
        &self,
        max_row_bytes: u64,
    ) -> Result<Option<SnapshotAnchor>, EventStoreError> {
        Self::read_snapshot_anchor_bounded(self.state()?.db.readable(), max_row_bytes)
    }

    fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
        let state = self.state_mut()?;

        // Running is not a terminal state; accepting it would leave `is_sealed()` returning
        // false while the seal call returned Ok, so subsequent appends would not see Closed.
        if matches!(status, RunStatus::Running) {
            return Err(EventStoreError::Backend(
                "seal status must be a terminal state, was Running".to_string(),
            ));
        }

        if state.manifest.is_sealed() {
            return Err(EventStoreError::Closed);
        }

        let mut updated = state.manifest.clone();
        updated.status = status;
        updated.high_watermark = state.high_watermark;

        if state.high_watermark > 0 {
            updated.end_ts_init = Some(state.max_ts_init);
        }

        Self::write_manifest(state.db.read_write()?, &updated)?;
        state.manifest = updated;
        Ok(())
    }

    fn manifest(&self) -> Result<RunManifest, EventStoreError> {
        Ok(self.state()?.manifest.clone())
    }

    fn high_watermark(&self) -> Result<u64, EventStoreError> {
        Ok(self.state()?.high_watermark)
    }
}

fn sealed_file_fingerprint(
    path: &Path,
    max_file_bytes: u64,
) -> Result<SealedFileFingerprint, EventStoreError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(EventStoreError::Backend(format!(
                "no run file at {}",
                path.display(),
            )));
        }
        Err(error) => {
            return Err(EventStoreError::Backend(format!(
                "inspect sealed run file {}: {error}",
                path.display(),
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EventStoreError::Corrupted(format!(
            "sealed run source {} is not a regular non-symlink file",
            path.display(),
        )));
    }
    let len = metadata.len();
    if len > max_file_bytes {
        return Err(EventStoreError::FileSizeLimitExceeded {
            observed: len,
            limit: max_file_bytes,
        });
    }

    Ok(SealedFileFingerprint {
        len,
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
        #[cfg(unix)]
        modified_seconds: metadata.mtime(),
        #[cfg(unix)]
        modified_nanoseconds: metadata.mtime_nsec(),
        #[cfg(not(unix))]
        modified: metadata.modified().ok(),
    })
}

fn validate_path_identity(field: &str, value: &str) -> Result<(), EventStoreError> {
    const MAX_ID_BYTES: usize = 128;
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || value == "."
        || value == ".."
        || value.starts_with('.')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(EventStoreError::Backend(format!(
            "{field} must be one canonical ASCII path component"
        )));
    }
    Ok(())
}

fn validate_manifest_path_identity(
    path: &Path,
    manifest: &RunManifest,
    expected_identity: Option<(&str, &str)>,
) -> Result<(), EventStoreError> {
    validate_path_identity("manifest instance_id", &manifest.instance_id)?;
    validate_path_identity("manifest run_id", &manifest.run_id)?;
    let path_instance_id = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            EventStoreError::Corrupted(format!(
                "run file path {} has no UTF-8 instance component",
                path.display()
            ))
        })?;
    let path_run_id = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            EventStoreError::Corrupted(format!(
                "run file path {} has no UTF-8 run stem",
                path.display()
            ))
        })?;
    validate_path_identity("path instance_id", path_instance_id)?;
    validate_path_identity("path run_id", path_run_id)?;
    if manifest.instance_id != path_instance_id || manifest.run_id != path_run_id {
        return Err(EventStoreError::Corrupted(format!(
            "run manifest identity ({}, {}) does not match file path ({path_instance_id}, {path_run_id})",
            manifest.instance_id, manifest.run_id
        )));
    }
    if let Some((expected_instance_id, expected_run_id)) = expected_identity
        && (manifest.instance_id != expected_instance_id || manifest.run_id != expected_run_id)
    {
        return Err(EventStoreError::Corrupted(format!(
            "run manifest identity ({}, {}) does not match requested identity ({expected_instance_id}, {expected_run_id})",
            manifest.instance_id, manifest.run_id
        )));
    }
    Ok(())
}

fn missing_manifest(path: &Path) -> EventStoreError {
    EventStoreError::Corrupted(format!(
        "missing manifest in run file at {}",
        path.display()
    ))
}

fn map_manifest_decode_err(error: codec::CodecError) -> EventStoreError {
    match error {
        codec::CodecError::UnsupportedEntrySchemaVersion { found, supported } => {
            EventStoreError::UnsupportedSchemaVersion { found, supported }
        }
        codec::CodecError::UnsupportedRunManifestVersion(found) => {
            EventStoreError::UnsupportedManifestVersion {
                found,
                supported: RUN_MANIFEST_WIRE_VERSION,
            }
        }
        other => EventStoreError::Corrupted(format!("decode manifest: {other}")),
    }
}

const fn is_unsupported_manifest(error: &EventStoreError) -> bool {
    matches!(
        error,
        EventStoreError::UnsupportedSchemaVersion { .. }
            | EventStoreError::UnsupportedManifestVersion { .. }
    )
}

// The entry hash covers the embedded seq, not the table key, so a moved or
// duplicated row still hashes correctly; both read paths refuse it here.
fn check_embedded_seq(seq: u64, entry: &EventStoreEntry) -> Result<(), EventStoreError> {
    if entry.seq != seq {
        return Err(EventStoreError::SeqMismatch {
            table_key: seq,
            embedded_seq: entry.seq,
        });
    }
    Ok(())
}

fn begin_immediate_write(db: &Database) -> Result<WriteTransaction, EventStoreError> {
    let mut txn = db.begin_write().map_err(map_transaction_err)?;
    txn.set_durability(Durability::Immediate)
        .map_err(|e| EventStoreError::Backend(format!("set durability: {e}")))?;
    Ok(txn)
}

fn create_or_open_database(path: &Path) -> Result<(Database, bool), EventStoreError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Database::builder()
            .create_file(file)
            .map(|db| (db, true))
            .map_err(map_database_err),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Database::open(path)
            .map(|db| (db, false))
            .map_err(map_database_err),
        Err(error) => {
            let message = format!("atomically create run file {}: {error}", path.display());
            if is_disk_pressure(error.kind()) {
                Err(EventStoreError::Disk(message))
            } else {
                Err(EventStoreError::Backend(message))
            }
        }
    }
}

fn insert_run_manifest(
    txn: &WriteTransaction,
    manifest: &RunManifest,
) -> Result<(), EventStoreError> {
    let bytes = codec::encode_run_manifest(manifest)
        .map_err(|e| EventStoreError::Backend(format!("encode manifest: {e}")))?;
    let mut table = txn.open_table(MANIFEST_TABLE).map_err(map_table_err)?;
    table
        .insert(MANIFEST_KEY, bytes.as_slice())
        .map_err(map_storage_err)?;
    Ok(())
}

fn map_storage_err(err: StorageError) -> EventStoreError {
    match err {
        StorageError::Io(io_err) if is_disk_pressure(io_err.kind()) => {
            EventStoreError::Disk(io_err.to_string())
        }
        StorageError::Corrupted(msg) => EventStoreError::Corrupted(msg),
        other => EventStoreError::Backend(other.to_string()),
    }
}

// `EventStoreError::Disk` documents ENOSPC and `RLIMIT_FSIZE` as its targets. On the
// stable toolchain `ENOSPC` surfaces as `StorageFull`, `RLIMIT_FSIZE`/`EFBIG` as
// `FileTooLarge`, and `EDQUOT` as `QuotaExceeded`; the kernel halt path keys off
// `Disk`, so all three must classify the same way.
fn is_disk_pressure(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::FileTooLarge | ErrorKind::StorageFull | ErrorKind::QuotaExceeded
    )
}

fn map_database_err(err: DatabaseError) -> EventStoreError {
    match err {
        DatabaseError::RepairAborted => EventStoreError::Corrupted(
            "database requires repair and cannot be verified read-only".to_string(),
        ),
        DatabaseError::UpgradeRequired(version) => EventStoreError::Corrupted(format!(
            "database file format version {version} requires manual upgrade",
        )),
        DatabaseError::Storage(storage) => map_storage_err(storage),
        other => EventStoreError::Backend(other.to_string()),
    }
}

fn map_read_only_database_err(err: DatabaseError) -> EventStoreError {
    match err {
        DatabaseError::Storage(StorageError::Io(io_err)) if is_corrupt_read(io_err.kind()) => {
            EventStoreError::Corrupted(format!("read-only open failed: {io_err}"))
        }
        other => map_database_err(other),
    }
}

fn is_corrupt_read(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::UnexpectedEof | ErrorKind::InvalidData)
}

fn map_table_err(err: TableError) -> EventStoreError {
    // Mirror redb's own classification: schema-shape failures (missing table, type
    // mismatch, definition drift) are structural corruption, not generic backend
    // errors. Programmer-error variants (`TableAlreadyOpen`, `TableExists`) stay
    // Backend so they surface as bugs rather than quarantine triggers.
    match err {
        TableError::Storage(storage) => map_storage_err(storage),
        TableError::TableDoesNotExist(_)
        | TableError::TableTypeMismatch { .. }
        | TableError::TableIsMultimap(_)
        | TableError::TableIsNotMultimap(_)
        | TableError::TypeDefinitionChanged { .. } => EventStoreError::Corrupted(err.to_string()),
        other => EventStoreError::Backend(other.to_string()),
    }
}

fn map_commit_err(err: CommitError) -> EventStoreError {
    match err {
        CommitError::Storage(storage) => map_storage_err(storage),
        other => EventStoreError::Backend(other.to_string()),
    }
}

pub(crate) fn is_run_file(path: &Path) -> bool {
    path.extension().and_then(|s| s.to_str()) == Some("redb")
        && path
            .file_name()
            .and_then(|s| s.to_str())
            .is_none_or(|name| !name.ends_with(".markers.redb"))
}

fn map_transaction_err(err: TransactionError) -> EventStoreError {
    match err {
        TransactionError::Storage(storage) => map_storage_err(storage),
        other => EventStoreError::Backend(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;

    fn raw_run_path(base: &Path, run_id: &str) -> PathBuf {
        let dir = base.join("trader-001");
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir.join(format!("{run_id}.redb"))
    }

    fn create_pre_codec_run_file(path: &Path) {
        let entries: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
        let manifest: TableDefinition<&str, &[u8]> = TableDefinition::new("manifest");
        let db = Database::create(path).expect("create redb");
        let txn = db.begin_write().expect("begin write");
        {
            txn.open_table(entries).expect("open entries");
            let mut table = txn.open_table(manifest).expect("open manifest");
            table
                .insert("current", b"old-format".as_slice())
                .expect("insert");
        }
        txn.commit().expect("commit");
    }

    fn create_current_format_invalid_manifest_file(path: &Path, bytes: &[u8]) {
        let db = Database::create(path).expect("create redb");
        let txn = db.begin_write().expect("begin write");
        format::write_store_format(&txn).expect("write store format");
        {
            let mut table = txn.open_table(MANIFEST_TABLE).expect("open manifest");
            table.insert(MANIFEST_KEY, bytes).expect("insert manifest");
        }
        txn.commit().expect("commit");
    }

    #[rstest]
    fn read_run_manifest_rejects_store_without_format_marker() {
        let tmp = TempDir::new().expect("tempdir");
        let path = raw_run_path(tmp.path(), "run-old-format");
        create_pre_codec_run_file(&path);

        let err = RedbBackend::read_run_manifest(&path).expect_err("must reject old format");

        match err {
            EventStoreError::Corrupted(msg) => {
                assert!(msg.contains("regenerated"), "msg was: {msg}");
            }
            other => panic!("expected Corrupted, was {other:?}"),
        }
    }

    #[rstest]
    fn recovery_charges_undecodable_manifest_rows_to_the_cumulative_budget() {
        let tmp = TempDir::new().expect("tempdir");
        let bytes = b"invalid";
        create_current_format_invalid_manifest_file(
            &raw_run_path(tmp.path(), "run-invalid-1"),
            bytes,
        );
        create_current_format_invalid_manifest_file(
            &raw_run_path(tmp.path(), "run-invalid-2"),
            bytes,
        );

        let err = RedbBackend::list_runs_with_read_limits(
            tmp.path(),
            "trader-001",
            2,
            u64::MAX,
            1_024,
            u64::try_from(bytes.len()).expect("manifest length"),
        )
        .expect_err("second invalid row must exceed the cumulative decode budget");

        match err {
            EventStoreError::MetadataSizeLimitExceeded {
                resource,
                observed,
                limit,
            } => {
                assert_eq!(resource, "recovery manifest total");
                assert_eq!(observed, 14);
                assert_eq!(limit, 7);
            }
            other => panic!("expected MetadataSizeLimitExceeded, was {other:?}"),
        }
    }

    #[rstest]
    #[case::file_too_large(ErrorKind::FileTooLarge, true)]
    #[case::storage_full(ErrorKind::StorageFull, true)]
    #[case::quota_exceeded(ErrorKind::QuotaExceeded, true)]
    #[case::other(ErrorKind::Other, false)]
    #[case::not_found(ErrorKind::NotFound, false)]
    #[case::permission_denied(ErrorKind::PermissionDenied, false)]
    #[case::interrupted(ErrorKind::Interrupted, false)]
    fn is_disk_pressure_matches_documented_kinds(#[case] kind: ErrorKind, #[case] expected: bool) {
        assert_eq!(is_disk_pressure(kind), expected);
    }

    #[rstest]
    fn map_storage_err_classifies_disk_pressure_as_disk() {
        let io_err = std::io::Error::from(ErrorKind::StorageFull);
        let mapped = map_storage_err(StorageError::Io(io_err));

        match mapped {
            EventStoreError::Disk(_) => {}
            other => panic!("expected Disk, was {other:?}"),
        }
    }

    #[rstest]
    fn map_storage_err_classifies_corrupted_as_corrupted() {
        let mapped = map_storage_err(StorageError::Corrupted("boom".to_string()));

        match mapped {
            EventStoreError::Corrupted(msg) => assert!(msg.contains("boom")),
            other => panic!("expected Corrupted, was {other:?}"),
        }
    }

    #[rstest]
    fn map_storage_err_falls_back_to_backend_for_unrelated_io() {
        let io_err = std::io::Error::from(ErrorKind::PermissionDenied);
        let mapped = map_storage_err(StorageError::Io(io_err));

        match mapped {
            EventStoreError::Backend(_) => {}
            other => panic!("expected Backend, was {other:?}"),
        }
    }
}
