// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Process-isolated verification for untrusted redb files.

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    str::FromStr,
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom},
};

use nautilus_provenance::{ContentDigest, ContentDigestBuilder};

use crate::{
    EventStore, RecoveredRun, RecoveryOutcome, RedbBackend, RunStatus, Verifier, VerifyLimits,
    kernel::recover_predecessors_with_limits,
};

/// Machine protocol spoken by the trusted verifier helper.
pub const ISOLATED_VERIFY_PROTOCOL: &str = "NEV1";
const EXIT_CLEAN: i32 = 0;
const EXIT_REJECTED: i32 = 1;
const EXIT_ERROR: i32 = 2;
const MAX_PROTOCOL_OUTPUT_BYTES: usize = 4 * 1024;
const IO_CHUNK_BYTES: usize = 8 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_mins(1);
const MAX_HELPER_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RECOVERY_INVENTORY_FILES: u64 = 1_024;
const MAX_RECOVERED_RUNS: usize = 16;
const MAX_INVENTORY_NAME_BYTES: usize = 255;
const MAX_RECOVERY_INVENTORY_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Content and metadata identity measured for a trusted executable or evidence file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMeasurement {
    /// File length.
    pub len: u64,
    /// Domain-separated digest of the exact file bytes.
    pub digest: ContentDigest,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
}

/// Clean verifier result returned through the bounded machine protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IsolatedVerifyReceipt {
    /// Verified run id.
    pub run_id: String,
    /// Verified terminal status.
    pub status: RunStatus,
    /// Durable verified high-watermark.
    pub high_watermark: u64,
    /// Rows scanned by the helper.
    pub entries_scanned: u64,
    /// Canonical verified-store digest.
    pub store_digest: ContentDigest,
    /// Exact source bytes read by the helper.
    pub source_measurement: FileMeasurement,
    /// Exact trusted helper executable bytes.
    pub helper_measurement: FileMeasurement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InventoryMeasurement {
    files: u64,
    total_bytes: u64,
    digest: ContentDigest,
    marker_sidecar_present: bool,
}

/// Fail-closed errors from helper measurement, execution, or protocol validation.
#[derive(Debug, thiserror::Error)]
pub enum IsolatedVerifyError {
    /// A path was not an absolute, canonical, regular non-symlink file.
    #[error("invalid isolated verifier path `{path}`: {reason}")]
    InvalidPath { path: String, reason: String },
    /// File measurement failed.
    #[error("could not measure `{path}`: {reason}")]
    Measurement { path: String, reason: String },
    /// Trusted helper bytes did not match deployment policy.
    #[error("isolated verifier helper measurement mismatch")]
    HelperMeasurementMismatch,
    /// Evidence changed before, during, or immediately after isolated verification.
    #[error("isolated verifier source changed during verification")]
    SourceChanged,
    /// Worker could not be spawned or supervised.
    #[error("isolated verifier worker I/O failed: {0}")]
    WorkerIo(String),
    /// Worker exceeded its wall-clock deadline.
    #[error("isolated verifier worker exceeded timeout of {0:?}")]
    Timeout(Duration),
    /// Worker emitted more bytes than the machine protocol permits.
    #[error("isolated verifier worker output exceeded {MAX_PROTOCOL_OUTPUT_BYTES} bytes")]
    OutputLimitExceeded,
    /// Worker exited through a signal or an unknown status.
    #[error("isolated verifier worker terminated abnormally: {0}")]
    WorkerAborted(ExitStatus),
    /// Worker rejected the file as corrupt or over budget.
    #[error("isolated verifier rejected the event-store file")]
    Rejected,
    /// Worker could not produce a verification verdict.
    #[error("isolated verifier worker returned an internal error")]
    WorkerError,
    /// Worker output was not the exact bounded protocol.
    #[error("invalid isolated verifier protocol: {0}")]
    Protocol(String),
}

/// Supervisor bound to one explicitly measured trusted helper executable.
#[derive(Clone, Debug)]
pub struct IsolatedVerifier {
    helper_path: PathBuf,
    expected_helper_digest: ContentDigest,
    timeout: Duration,
}

/// One invocation's private, digest-pinned executable copy.
///
/// The temporary directory guard must outlive the worker. Keeping it in this value makes that
/// lifetime explicit and prevents an eager cleanup from invalidating the path between pinning and
/// `exec`.
#[derive(Debug)]
struct PinnedHelper {
    directory: tempfile::TempDir,
    path: PathBuf,
    source_measurement: FileMeasurement,
    pinned_measurement: FileMeasurement,
}

impl PinnedHelper {
    fn executable_path(&self) -> &Path {
        debug_assert_eq!(self.path.parent(), Some(self.directory.path()));
        &self.path
    }
}

impl IsolatedVerifier {
    /// Constructs a verifier only when the current helper bytes match deployment policy.
    ///
    /// # Errors
    ///
    /// Returns [`IsolatedVerifyError`] for a non-canonical path, symlink, non-regular file,
    /// untrusted Unix ownership or mode, unsupported platform, read failure, or digest mismatch.
    pub fn new(
        helper_path: impl AsRef<Path>,
        expected_helper_digest: ContentDigest,
    ) -> Result<Self, IsolatedVerifyError> {
        let helper_path = canonical_regular_file(helper_path.as_ref())?;
        let measurement = measure_trusted_helper_source(&helper_path, MAX_HELPER_BYTES)?;
        if measurement.digest != expected_helper_digest {
            return Err(IsolatedVerifyError::HelperMeasurementMismatch);
        }
        Ok(Self {
            helper_path,
            expected_helper_digest,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Overrides the wall-clock worker deadline.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the deployment-pinned digest of the trusted helper executable.
    #[must_use]
    pub const fn expected_helper_digest(&self) -> ContentDigest {
        self.expected_helper_digest
    }

    /// Verifies one sealed file exclusively inside the measured helper process.
    ///
    /// # Errors
    ///
    /// An unexpected helper at pin time, private-copy mutation, source replacement, timeout,
    /// signal, output overflow, malformed response, corruption finding, or verifier error fails
    /// closed. Deployment-path replacement after its source descriptor is pinned cannot affect the
    /// worker selected for that invocation.
    pub fn verify_file(
        &self,
        path: impl AsRef<Path>,
        limits: VerifyLimits,
    ) -> Result<IsolatedVerifyReceipt, IsolatedVerifyError> {
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            IsolatedVerifyError::WorkerIo("timeout deadline overflow".to_string())
        })?;
        let path = canonical_regular_file(path.as_ref())?;
        let pinned_helper = pin_trusted_helper(
            &self.helper_path,
            self.expected_helper_digest,
            MAX_HELPER_BYTES,
            deadline,
            self.timeout,
        )?;
        let helper_before = pinned_helper.source_measurement.clone();
        let source_before =
            measure_regular_file_before(&path, limits.max_file_bytes, deadline, self.timeout)?;
        let marker_path = marker_sidecar_path(&path)?;
        if marker_path.exists() {
            return Err(IsolatedVerifyError::Rejected);
        }
        let output = self.run_worker(&pinned_helper, &path, limits, deadline)?;
        if std::env::var_os("NAUTILUS_EVENT_STORE_VERIFY_DEBUG_RECEIPT").is_some()
            && !output.stderr.is_empty()
        {
            eprintln!(
                "isolated receipt worker stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let pinned_after = measure_regular_file_before(
            &pinned_helper.path,
            MAX_HELPER_BYTES,
            deadline,
            self.timeout,
        )?;
        if pinned_after != pinned_helper.pinned_measurement
            || pinned_after.digest != self.expected_helper_digest
        {
            return Err(IsolatedVerifyError::HelperMeasurementMismatch);
        }
        let source_after =
            measure_regular_file_before(&path, limits.max_file_bytes, deadline, self.timeout)?;
        if source_before != source_after {
            return Err(IsolatedVerifyError::SourceChanged);
        }
        if marker_path.exists() {
            return Err(IsolatedVerifyError::SourceChanged);
        }
        let mut receipt = parse_worker_output(output.status, &output.stdout, &output.stderr)?;
        if receipt.source_measurement.len != source_before.len
            || receipt.source_measurement.digest != source_before.digest
        {
            return Err(IsolatedVerifyError::SourceChanged);
        }
        if receipt.helper_measurement.len != pinned_helper.pinned_measurement.len
            || receipt.helper_measurement.digest != pinned_helper.pinned_measurement.digest
        {
            return Err(IsolatedVerifyError::HelperMeasurementMismatch);
        }
        // Preserve the parent's platform metadata fingerprint in the returned trust record.
        receipt.source_measurement = source_before;
        receipt.helper_measurement = helper_before;
        Ok(receipt)
    }

    /// Inventories and repairs predecessor runs exclusively inside the measured helper process.
    ///
    /// The parent process reads directory metadata only. It never opens or decodes a redb page;
    /// the worker applies OS limits before inventory, repair, manifest decoding, and lifecycle
    /// classification. The bounded response is tied to the post-recovery directory fingerprint
    /// and to the exact helper executable measurement.
    ///
    /// # Errors
    ///
    /// Any missing or unexpected helper at pin time, private-copy mutation, inventory race,
    /// resource-limit rejection, malformed response, timeout, signal, or redb recovery failure
    /// fails closed. Deployment-path replacement after its source descriptor is pinned cannot
    /// affect the worker selected for that invocation.
    pub fn recover_predecessors(
        &self,
        base_dir: &Path,
        instance_id: &str,
        limits: VerifyLimits,
    ) -> Result<RecoveryOutcome, IsolatedVerifyError> {
        validate_instance_id(instance_id)?;
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            IsolatedVerifyError::WorkerIo("timeout deadline overflow".to_string())
        })?;
        match fs::create_dir_all(base_dir) {
            Ok(()) => {}
            Err(error) => {
                return Err(IsolatedVerifyError::Measurement {
                    path: base_dir.display().to_string(),
                    reason: format!("create trusted event-store base directory: {error}"),
                });
            }
        }
        let base_dir = canonical_directory(base_dir)?;
        let pinned_helper = pin_trusted_helper(
            &self.helper_path,
            self.expected_helper_digest,
            MAX_HELPER_BYTES,
            deadline,
            self.timeout,
        )?;
        // Metadata-only preflight applies the file-count, individual-size, and aggregate-size
        // budgets before the worker can open a redb page.
        let _before = measure_inventory(&base_dir, instance_id, limits.max_file_bytes)?;
        let output =
            self.run_recovery_worker(&pinned_helper, &base_dir, instance_id, limits, deadline)?;
        let pinned_after = measure_regular_file_before(
            &pinned_helper.path,
            MAX_HELPER_BYTES,
            deadline,
            self.timeout,
        )?;
        if pinned_after != pinned_helper.pinned_measurement
            || pinned_after.digest != self.expected_helper_digest
        {
            return Err(IsolatedVerifyError::HelperMeasurementMismatch);
        }
        let (outcome, worker_inventory, worker_helper) =
            parse_recovery_output(output.status, &output.stdout, &output.stderr)?;
        let inventory_after = measure_inventory(&base_dir, instance_id, limits.max_file_bytes)?;
        if inventory_after != worker_inventory {
            return Err(IsolatedVerifyError::SourceChanged);
        }
        if worker_helper.len != pinned_after.len || worker_helper.digest != pinned_after.digest {
            return Err(IsolatedVerifyError::HelperMeasurementMismatch);
        }
        Ok(outcome)
    }

    fn run_worker(
        &self,
        helper: &PinnedHelper,
        path: &Path,
        limits: VerifyLimits,
        deadline: Instant,
    ) -> Result<WorkerOutput, IsolatedVerifyError> {
        if Instant::now() >= deadline {
            return Err(IsolatedVerifyError::Timeout(self.timeout));
        }
        let mut child = Command::new(helper.executable_path())
            .arg("--receipt-worker-v1")
            .arg(path)
            .args(limit_arguments(limits))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
        let stdout = child.stdout.take().ok_or_else(|| {
            IsolatedVerifyError::WorkerIo("worker stdout pipe missing".to_string())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            IsolatedVerifyError::WorkerIo("worker stderr pipe missing".to_string())
        })?;
        let stdout_reader = thread::spawn(move || read_bounded(stdout));
        let stderr_reader = thread::spawn(move || read_bounded(stderr));
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?
            {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(IsolatedVerifyError::Timeout(self.timeout));
            }
            thread::sleep(Duration::from_millis(10));
        };
        let stdout = join_reader(stdout_reader)?;
        let stderr = join_reader(stderr_reader)?;
        if stdout.exceeded || stderr.exceeded {
            return Err(IsolatedVerifyError::OutputLimitExceeded);
        }
        Ok(WorkerOutput {
            status,
            stdout: stdout.bytes,
            stderr: stderr.bytes,
        })
    }

    fn run_recovery_worker(
        &self,
        helper: &PinnedHelper,
        base_dir: &Path,
        instance_id: &str,
        limits: VerifyLimits,
        deadline: Instant,
    ) -> Result<WorkerOutput, IsolatedVerifyError> {
        if Instant::now() >= deadline {
            return Err(IsolatedVerifyError::Timeout(self.timeout));
        }
        let mut child = Command::new(helper.executable_path())
            .arg("--recovery-worker-v1")
            .arg(base_dir)
            .arg(instance_id)
            .args(limit_arguments(limits))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
        supervise_child(&mut child, deadline, self.timeout)
    }
}

fn supervise_child(
    child: &mut std::process::Child,
    deadline: Instant,
    timeout: Duration,
) -> Result<WorkerOutput, IsolatedVerifyError> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| IsolatedVerifyError::WorkerIo("worker stdout pipe missing".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| IsolatedVerifyError::WorkerIo("worker stderr pipe missing".to_string()))?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout));
    let stderr_reader = thread::spawn(move || read_bounded(stderr));
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(IsolatedVerifyError::Timeout(timeout));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;
    if stdout.exceeded || stderr.exceeded {
        return Err(IsolatedVerifyError::OutputLimitExceeded);
    }
    Ok(WorkerOutput {
        status,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
    })
}

#[derive(Debug)]
struct WorkerOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Debug)]
struct BoundedOutput {
    bytes: Vec<u8>,
    exceeded: bool,
}

fn read_bounded(mut reader: impl Read) -> io::Result<BoundedOutput> {
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut chunk = [0_u8; IO_CHUNK_BYTES];
    loop {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        let available = MAX_PROTOCOL_OUTPUT_BYTES.saturating_sub(bytes.len());
        let retained = read.min(available);
        bytes.extend_from_slice(&chunk[..retained]);
        exceeded |= retained != read;
    }
    Ok(BoundedOutput { bytes, exceeded })
}

fn join_reader(
    reader: thread::JoinHandle<io::Result<BoundedOutput>>,
) -> Result<BoundedOutput, IsolatedVerifyError> {
    reader
        .join()
        .map_err(|_| IsolatedVerifyError::WorkerIo("output reader panicked".to_string()))?
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))
}

fn limit_arguments(limits: VerifyLimits) -> [String; 10] {
    [
        limits.max_high_watermark.to_string(),
        limits.max_entries.to_string(),
        limits.max_index_entries.to_string(),
        limits.max_file_bytes.to_string(),
        limits.max_entry_bytes.to_string(),
        limits.max_total_entry_bytes.to_string(),
        limits.max_total_index_key_bytes.to_string(),
        limits.max_findings.to_string(),
        limits.max_snapshot_anchor_bytes.to_string(),
        limits.max_manifest_bytes.to_string(),
    ]
}

#[cfg(unix)]
fn current_filesystem_owner_uid() -> Result<u32, IsolatedVerifyError> {
    use std::os::unix::fs::MetadataExt;

    let probe = tempfile::tempfile().map_err(|error| {
        IsolatedVerifyError::WorkerIo(format!("create effective-owner probe: {error}"))
    })?;
    probe
        .metadata()
        .map(|metadata| metadata.uid())
        .map_err(|error| {
            IsolatedVerifyError::WorkerIo(format!("inspect effective-owner probe: {error}"))
        })
}

#[cfg(unix)]
const fn trusted_helper_owner(owner_uid: u32, effective_uid: u32) -> bool {
    owner_uid == 0 || owner_uid == effective_uid
}

#[cfg(unix)]
fn validate_trusted_helper_metadata(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), IsolatedVerifyError> {
    use std::os::unix::fs::MetadataExt;

    if !metadata.is_file() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "trusted helper is not a regular file".to_string(),
        });
    }
    let effective_uid = current_filesystem_owner_uid()?;
    // A deployment helper may be installed by root under `/opt` and executed by an unprivileged
    // trader. Root ownership is acceptable only with the non-writable/non-set-id checks below.
    if !trusted_helper_owner(metadata.uid(), effective_uid) {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: format!(
                "trusted helper owner uid {} is neither root nor effective uid {effective_uid}",
                metadata.uid()
            ),
        });
    }
    let mode = metadata.mode();
    if mode & 0o100 == 0 {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "trusted helper is not executable by its owner".to_string(),
        });
    }
    if mode & 0o022 != 0 {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "trusted helper is writable by group or other users".to_string(),
        });
    }
    if mode & 0o7000 != 0 {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "trusted helper has set-id or sticky permission bits".to_string(),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn trusted_helper_metadata_unchanged(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    same_file_identity(before, after)
        && before.uid() == after.uid()
        && before.gid() == after.gid()
        && before.mode() == after.mode()
}

#[cfg(unix)]
fn open_trusted_helper(path: &Path) -> Result<(File, fs::Metadata), IsolatedVerifyError> {
    use std::os::unix::fs::OpenOptionsExt;

    if !path.is_absolute() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "path is not absolute".to_string(),
        });
    }
    let path_metadata =
        fs::symlink_metadata(path).map_err(|error| IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "expected a regular non-symlink helper".to_string(),
        });
    }
    validate_trusted_helper_metadata(path, &path_metadata)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: format!("open trusted helper without following symlinks: {error}"),
        })?;
    let metadata = file
        .metadata()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    validate_trusted_helper_metadata(path, &metadata)?;
    if !same_file_identity(&path_metadata, &metadata) {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    Ok((file, metadata))
}

#[cfg(unix)]
fn measure_open_helper(
    file: &mut File,
    path: &Path,
    metadata_before: &fs::Metadata,
    max_bytes: u64,
    deadline: Option<(Instant, Duration)>,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    if metadata_before.len() > max_bytes {
        return Err(IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: format!(
                "file size {} exceeds limit {max_bytes}",
                metadata_before.len()
            ),
        });
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    let mut builder = ContentDigestBuilder::new(metadata_before.len());
    let mut observed = 0_u64;
    let mut chunk = [0_u8; IO_CHUNK_BYTES];
    loop {
        if let Some((deadline, timeout)) = deadline
            && Instant::now() >= deadline
        {
            return Err(IsolatedVerifyError::Timeout(timeout));
        }
        let read = file
            .read(&mut chunk)
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(
                u64::try_from(read).map_err(|error| IsolatedVerifyError::Measurement {
                    path: path.display().to_string(),
                    reason: error.to_string(),
                })?,
            )
            .ok_or_else(|| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: "trusted helper length overflow".to_string(),
            })?;
        if observed > max_bytes {
            return Err(IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: format!("trusted helper grew beyond byte limit {max_bytes}"),
            });
        }
        builder
            .update(&chunk[..read])
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    let digest = builder
        .finish()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    let metadata_after = file
        .metadata()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    validate_trusted_helper_metadata(path, &metadata_after)?;
    if observed != metadata_before.len()
        || !trusted_helper_metadata_unchanged(metadata_before, &metadata_after)
    {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    let before = measurement_from_metadata(metadata_before, digest);
    let after = measurement_from_metadata(&metadata_after, digest);
    if before != after {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    Ok(after)
}

#[cfg(unix)]
fn measure_trusted_helper_source(
    path: &Path,
    max_bytes: u64,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    let (mut file, metadata) = open_trusted_helper(path)?;
    measure_open_helper(&mut file, path, &metadata, max_bytes, None)
}

#[cfg(not(unix))]
fn measure_trusted_helper_source(
    path: &Path,
    _max_bytes: u64,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    Err(IsolatedVerifyError::InvalidPath {
        path: path.display().to_string(),
        reason: "isolated helper pinning is unsupported on this platform".to_string(),
    })
}

#[cfg(unix)]
fn pin_trusted_helper(
    path: &Path,
    expected_digest: ContentDigest,
    max_bytes: u64,
    deadline: Instant,
    timeout: Duration,
) -> Result<PinnedHelper, IsolatedVerifyError> {
    pin_trusted_helper_with_open_hook(path, expected_digest, max_bytes, deadline, timeout, || {})
}

#[cfg(unix)]
#[expect(
    clippy::too_many_lines,
    reason = "pin copy, source/destination measurement, fsync, and immutable-path checks form one auditable trust boundary"
)]
fn pin_trusted_helper_with_open_hook(
    path: &Path,
    expected_digest: ContentDigest,
    max_bytes: u64,
    deadline: Instant,
    timeout: Duration,
    after_source_open: impl FnOnce(),
) -> Result<PinnedHelper, IsolatedVerifyError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let (mut source, source_metadata_before) = open_trusted_helper(path)?;
    if source_metadata_before.len() > max_bytes {
        return Err(IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: format!(
                "file size {} exceeds limit {max_bytes}",
                source_metadata_before.len()
            ),
        });
    }
    // Tests coordinate a pathname ABA exactly here. Production supplies a no-op: every byte below
    // is read from the already-open descriptor, never by reopening `path`.
    after_source_open();

    let directory = tempfile::Builder::new()
        .prefix("nautilus-event-store-helper-")
        .tempdir()
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let directory_metadata = fs::symlink_metadata(directory.path())
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let effective_uid = current_filesystem_owner_uid()?;
    if directory_metadata.file_type().is_symlink()
        || !directory_metadata.is_dir()
        || directory_metadata.uid() != effective_uid
        || directory_metadata.mode() & 0o777 != 0o700
    {
        return Err(IsolatedVerifyError::WorkerIo(
            "private helper directory owner or mode validation failed".to_string(),
        ));
    }
    let pinned_path = directory.path().join("verify");
    let mut pinned = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(&pinned_path)
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;

    let mut builder = ContentDigestBuilder::new(source_metadata_before.len());
    let mut copied = 0_u64;
    let mut chunk = [0_u8; IO_CHUNK_BYTES];
    loop {
        if Instant::now() >= deadline {
            return Err(IsolatedVerifyError::Timeout(timeout));
        }
        let read = source
            .read(&mut chunk)
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(
                u64::try_from(read).map_err(|error| IsolatedVerifyError::Measurement {
                    path: path.display().to_string(),
                    reason: error.to_string(),
                })?,
            )
            .ok_or_else(|| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: "trusted helper copy length overflow".to_string(),
            })?;
        if copied > max_bytes {
            return Err(IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: format!("trusted helper grew beyond byte limit {max_bytes}"),
            });
        }
        builder
            .update(&chunk[..read])
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        pinned
            .write_all(&chunk[..read])
            .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    }
    let digest = builder
        .finish()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    let source_metadata_after =
        source
            .metadata()
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    validate_trusted_helper_metadata(path, &source_metadata_after)?;
    let source_measurement = measurement_from_metadata(&source_metadata_after, digest);
    if copied != source_metadata_before.len()
        || !trusted_helper_metadata_unchanged(&source_metadata_before, &source_metadata_after)
        || measurement_from_metadata(&source_metadata_before, digest) != source_measurement
    {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    if digest != expected_digest {
        return Err(IsolatedVerifyError::HelperMeasurementMismatch);
    }

    pinned
        .flush()
        .and_then(|()| pinned.sync_all())
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    pinned
        .set_permissions(fs::Permissions::from_mode(0o500))
        .and_then(|()| pinned.sync_all())
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let pinned_metadata_before = pinned
        .metadata()
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let pinned_measurement = measure_open_helper(
        &mut pinned,
        &pinned_path,
        &pinned_metadata_before,
        max_bytes,
        Some((deadline, timeout)),
    )?;
    if pinned_measurement.digest != expected_digest || pinned_measurement.len != copied {
        return Err(IsolatedVerifyError::HelperMeasurementMismatch);
    }
    let pinned_metadata = pinned
        .metadata()
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let pinned_path_metadata = fs::symlink_metadata(&pinned_path)
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    if pinned_path_metadata.file_type().is_symlink()
        || !same_file_identity(&pinned_path_metadata, &pinned_metadata)
        || pinned_metadata.uid() != effective_uid
        || pinned_metadata.mode() & 0o777 != 0o500
    {
        return Err(IsolatedVerifyError::WorkerIo(
            "private helper file owner, mode, or identity validation failed".to_string(),
        ));
    }
    File::open(directory.path())
        .and_then(|directory_file| directory_file.sync_all())
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;

    Ok(PinnedHelper {
        directory,
        path: pinned_path,
        source_measurement,
        pinned_measurement: measurement_from_metadata(&pinned_metadata, expected_digest),
    })
}

#[cfg(not(unix))]
fn pin_trusted_helper(
    path: &Path,
    _expected_digest: ContentDigest,
    _max_bytes: u64,
    _deadline: Instant,
    _timeout: Duration,
) -> Result<PinnedHelper, IsolatedVerifyError> {
    Err(IsolatedVerifyError::InvalidPath {
        path: path.display().to_string(),
        reason: "isolated helper pinning is unsupported on this platform".to_string(),
    })
}

/// Measures one canonical regular file without buffering it in memory.
///
/// # Errors
///
/// Returns an error for symlinks, non-regular files, files larger than `max_bytes`, read errors,
/// or metadata changes while hashing.
pub fn measure_regular_file(
    path: &Path,
    max_bytes: u64,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    measure_regular_file_inner(path, max_bytes, None)
}

fn measure_regular_file_before(
    path: &Path,
    max_bytes: u64,
    deadline: Instant,
    timeout: Duration,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    measure_regular_file_inner(path, max_bytes, Some((deadline, timeout)))
}

fn measure_regular_file_inner(
    path: &Path,
    max_bytes: u64,
    deadline: Option<(Instant, Duration)>,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    let path_metadata_before =
        fs::symlink_metadata(path).map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    if path_metadata_before.file_type().is_symlink() || !path_metadata_before.is_file() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "expected a regular non-symlink file".to_string(),
        });
    }
    let mut file = File::open(path).map_err(|error| IsolatedVerifyError::Measurement {
        path: path.display().to_string(),
        reason: error.to_string(),
    })?;
    let metadata_before = file
        .metadata()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    if !metadata_before.is_file() || !same_file_identity(&path_metadata_before, &metadata_before) {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    if metadata_before.len() > max_bytes {
        return Err(IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: format!(
                "file size {} exceeds limit {max_bytes}",
                metadata_before.len()
            ),
        });
    }
    let mut builder = ContentDigestBuilder::new(metadata_before.len());
    let mut chunk = [0_u8; IO_CHUNK_BYTES];
    loop {
        if let Some((deadline, timeout)) = deadline
            && Instant::now() >= deadline
        {
            return Err(IsolatedVerifyError::Timeout(timeout));
        }
        let read = file
            .read(&mut chunk)
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        builder
            .update(&chunk[..read])
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    let digest = builder
        .finish()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    let metadata_after = file
        .metadata()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    let path_metadata_after =
        fs::symlink_metadata(path).map_err(|error| IsolatedVerifyError::Measurement {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    if path_metadata_after.file_type().is_symlink()
        || !path_metadata_after.is_file()
        || !same_file_identity(&path_metadata_after, &metadata_after)
    {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    let before = measurement_from_metadata(&metadata_before, digest);
    let after = measurement_from_metadata(&metadata_after, digest);
    if before != after {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    Ok(after)
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

fn canonical_regular_file(path: &Path) -> Result<PathBuf, IsolatedVerifyError> {
    if !path.is_absolute() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "path is not absolute".to_string(),
        });
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|error| IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "expected a regular non-symlink file".to_string(),
        });
    }
    let canonical = fs::canonicalize(path).map_err(|error| IsolatedVerifyError::InvalidPath {
        path: path.display().to_string(),
        reason: error.to_string(),
    })?;
    Ok(canonical)
}

fn marker_sidecar_path(path: &Path) -> Result<PathBuf, IsolatedVerifyError> {
    let stem = path
        .file_stem()
        .ok_or_else(|| IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "source has no file stem".to_string(),
        })?;
    let mut name = stem.to_os_string();
    name.push(".markers.redb");
    Ok(path.with_file_name(name))
}

fn canonical_directory(path: &Path) -> Result<PathBuf, IsolatedVerifyError> {
    if !path.is_absolute() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "path is not absolute".to_string(),
        });
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|error| IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: path.display().to_string(),
            reason: "expected a directory which is not a symlink".to_string(),
        });
    }
    fs::canonicalize(path).map_err(|error| IsolatedVerifyError::InvalidPath {
        path: path.display().to_string(),
        reason: error.to_string(),
    })
}

fn validate_instance_id(instance_id: &str) -> Result<(), IsolatedVerifyError> {
    if instance_id.is_empty()
        || instance_id.len() > 128
        || !instance_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(IsolatedVerifyError::Protocol(
            "invalid instance id for recovery worker".to_string(),
        ));
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "inventory validation keeps the complete fail-closed path and budgets together"
)]
#[expect(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "the canonical on-disk event-store suffix is exactly lowercase `.redb`"
)]
fn measure_inventory(
    base_dir: &Path,
    instance_id: &str,
    max_file_bytes: u64,
) -> Result<InventoryMeasurement, IsolatedVerifyError> {
    validate_instance_id(instance_id)?;
    let run_dir = base_dir.join(instance_id);
    let run_dir_metadata = match fs::symlink_metadata(&run_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(InventoryMeasurement {
                files: 0,
                total_bytes: 0,
                digest: ContentDigest::digest_bytes(&[]),
                marker_sidecar_present: false,
            });
        }
        Err(error) => {
            return Err(IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: error.to_string(),
            });
        }
    };
    if run_dir_metadata.file_type().is_symlink() || !run_dir_metadata.is_dir() {
        return Err(IsolatedVerifyError::InvalidPath {
            path: run_dir.display().to_string(),
            reason: "instance inventory must be a real non-symlink directory".to_string(),
        });
    }
    let canonical_run_dir =
        fs::canonicalize(&run_dir).map_err(|error| IsolatedVerifyError::Measurement {
            path: run_dir.display().to_string(),
            reason: error.to_string(),
        })?;
    if canonical_run_dir.parent() != Some(base_dir) {
        return Err(IsolatedVerifyError::InvalidPath {
            path: run_dir.display().to_string(),
            reason: "instance inventory escaped the canonical base directory".to_string(),
        });
    }
    let entries = match fs::read_dir(&canonical_run_dir) {
        Ok(entries) => entries,
        Err(error) => {
            return Err(IsolatedVerifyError::Measurement {
                path: canonical_run_dir.display().to_string(),
                reason: error.to_string(),
            });
        }
    };
    let mut records = Vec::new();
    let mut total_bytes = 0_u64;
    let mut directory_entries = 0_u64;
    for entry in entries {
        directory_entries =
            directory_entries
                .checked_add(1)
                .ok_or_else(|| IsolatedVerifyError::Measurement {
                    path: run_dir.display().to_string(),
                    reason: "inventory directory-entry count overflow".to_string(),
                })?;
        if directory_entries > 4_096 {
            return Err(IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: "inventory directory-entry count exceeds limit 4096".to_string(),
            });
        }
        let entry = entry.map_err(|error| IsolatedVerifyError::Measurement {
            path: run_dir.display().to_string(),
            reason: error.to_string(),
        })?;
        let name =
            entry
                .file_name()
                .into_string()
                .map_err(|_| IsolatedVerifyError::Measurement {
                    path: run_dir.display().to_string(),
                    reason: "non-UTF-8 inventory file name".to_string(),
                })?;
        if !name.ends_with(".redb") {
            continue;
        }
        if name.len() > MAX_INVENTORY_NAME_BYTES {
            return Err(IsolatedVerifyError::Measurement {
                path: entry.path().display().to_string(),
                reason: format!("inventory name exceeds {MAX_INVENTORY_NAME_BYTES} bytes"),
            });
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
            IsolatedVerifyError::Measurement {
                path: entry.path().display().to_string(),
                reason: error.to_string(),
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(IsolatedVerifyError::InvalidPath {
                path: entry.path().display().to_string(),
                reason: "inventory redb entry must be a regular non-symlink file".to_string(),
            });
        }
        if metadata.len() > max_file_bytes {
            return Err(IsolatedVerifyError::Measurement {
                path: entry.path().display().to_string(),
                reason: format!(
                    "inventory file size {} exceeds limit {max_file_bytes}",
                    metadata.len()
                ),
            });
        }
        total_bytes = total_bytes.checked_add(metadata.len()).ok_or_else(|| {
            IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: "inventory byte count overflow".to_string(),
            }
        })?;
        if total_bytes > MAX_RECOVERY_INVENTORY_BYTES {
            return Err(IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: format!(
                    "inventory bytes {total_bytes} exceed limit {MAX_RECOVERY_INVENTORY_BYTES}"
                ),
            });
        }
        records.push(inventory_record(name, &metadata));
        if u64::try_from(records.len()).unwrap_or(u64::MAX) > MAX_RECOVERY_INVENTORY_FILES {
            return Err(IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: format!(
                    "inventory file count exceeds limit {MAX_RECOVERY_INVENTORY_FILES}"
                ),
            });
        }
    }
    records.sort_by(|left, right| left.name.cmp(&right.name));
    let transcript_len = records.iter().try_fold(0_u64, |total, record| {
        let name_len = u64::try_from(record.name.len()).ok()?;
        total.checked_add(8)?.checked_add(name_len)?.checked_add(48)
    });
    let Some(transcript_len) = transcript_len else {
        return Err(IsolatedVerifyError::Measurement {
            path: run_dir.display().to_string(),
            reason: "inventory transcript length overflow".to_string(),
        });
    };
    let mut builder = ContentDigestBuilder::new(transcript_len);
    for record in &records {
        let name_len =
            u64::try_from(record.name.len()).map_err(|error| IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: format!("inventory name length does not fit u64: {error}"),
            })?;
        builder
            .update(&name_len.to_be_bytes())
            .and_then(|()| builder.update(record.name.as_bytes()))
            .and_then(|()| builder.update(&record.len.to_be_bytes()))
            .and_then(|()| builder.update(&record.fingerprint))
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: run_dir.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    let digest = builder
        .finish()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: run_dir.display().to_string(),
            reason: error.to_string(),
        })?;
    let marker_sidecar_present = records
        .iter()
        .any(|record| record.name.ends_with(".markers.redb"));
    Ok(InventoryMeasurement {
        files: u64::try_from(records.len()).unwrap_or(u64::MAX),
        total_bytes,
        digest,
        marker_sidecar_present,
    })
}

struct InventoryRecord {
    name: String,
    len: u64,
    fingerprint: [u8; 40],
}

#[cfg(unix)]
fn inventory_record(name: String, metadata: &fs::Metadata) -> InventoryRecord {
    use std::os::unix::fs::MetadataExt;
    let mut fingerprint = [0_u8; 40];
    fingerprint[0..8].copy_from_slice(&metadata.dev().to_be_bytes());
    fingerprint[8..16].copy_from_slice(&metadata.ino().to_be_bytes());
    fingerprint[16..24].copy_from_slice(&metadata.mtime().to_be_bytes());
    fingerprint[24..32].copy_from_slice(&metadata.mtime_nsec().to_be_bytes());
    fingerprint[32..40].copy_from_slice(&metadata.ctime().to_be_bytes());
    InventoryRecord {
        name,
        len: metadata.len(),
        fingerprint,
    }
}

#[cfg(not(unix))]
fn inventory_record(name: String, metadata: &fs::Metadata) -> InventoryRecord {
    use std::time::UNIX_EPOCH;
    let mut fingerprint = [0_u8; 40];
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok());
    if let Some(modified) = modified {
        fingerprint[0..8].copy_from_slice(&modified.as_secs().to_be_bytes());
        fingerprint[8..12].copy_from_slice(&modified.subsec_nanos().to_be_bytes());
    }
    InventoryRecord {
        name,
        len: metadata.len(),
        fingerprint,
    }
}

#[cfg(unix)]
fn measurement_from_metadata(metadata: &fs::Metadata, digest: ContentDigest) -> FileMeasurement {
    use std::os::unix::fs::MetadataExt;
    FileMeasurement {
        len: metadata.len(),
        digest,
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
    }
}

#[cfg(not(unix))]
fn measurement_from_metadata(metadata: &fs::Metadata, digest: ContentDigest) -> FileMeasurement {
    FileMeasurement {
        len: metadata.len(),
        digest,
    }
}

fn parse_worker_output(
    status: ExitStatus,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<IsolatedVerifyReceipt, IsolatedVerifyError> {
    if !stderr.is_empty() {
        return Err(IsolatedVerifyError::Protocol(
            "worker stderr was not empty".to_string(),
        ));
    }
    let line = std::str::from_utf8(stdout)
        .map_err(|_| IsolatedVerifyError::Protocol("stdout was not UTF-8".to_string()))?;
    match status.code() {
        Some(EXIT_REJECTED) if line == "NEV1\tREJECTED\n" => {
            return Err(IsolatedVerifyError::Rejected);
        }
        Some(EXIT_ERROR) if line == "NEV1\tERROR\n" => {
            return Err(IsolatedVerifyError::WorkerError);
        }
        Some(EXIT_CLEAN) => {}
        Some(EXIT_REJECTED | EXIT_ERROR) => {
            return Err(IsolatedVerifyError::Protocol(
                "error exit carried malformed response".to_string(),
            ));
        }
        _ => return Err(IsolatedVerifyError::WorkerAborted(status)),
    }
    if !line.ends_with('\n') || line[..line.len() - 1].contains('\n') {
        return Err(IsolatedVerifyError::Protocol(
            "response was not exactly one newline-terminated record".to_string(),
        ));
    }
    let fields: Vec<&str> = line.trim_end_matches('\n').split('\t').collect();
    if fields.len() != 12
        || fields[0] != ISOLATED_VERIFY_PROTOCOL
        || fields[1] != "CLEAN"
        || fields[11] != "0"
    {
        return Err(IsolatedVerifyError::Protocol(
            "unexpected clean response shape".to_string(),
        ));
    }
    let run_id = fields[2];
    if run_id.is_empty()
        || run_id.len() > 128
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(IsolatedVerifyError::Protocol(
            "invalid run id in response".to_string(),
        ));
    }
    let status = match fields[3] {
        "Ended" => RunStatus::Ended,
        "CrashedRecovered" => RunStatus::CrashedRecovered,
        _ => {
            return Err(IsolatedVerifyError::Protocol(
                "disallowed run status in clean response".to_string(),
            ));
        }
    };
    Ok(IsolatedVerifyReceipt {
        run_id: run_id.to_string(),
        status,
        high_watermark: parse_u64(fields[4], "high_watermark")?,
        entries_scanned: parse_u64(fields[5], "entries_scanned")?,
        store_digest: parse_digest(fields[6], "store_digest")?,
        source_measurement: protocol_measurement(fields[7], fields[8], "source")?,
        helper_measurement: protocol_measurement(fields[9], fields[10], "helper")?,
    })
}

fn parse_recovery_output(
    status: ExitStatus,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<(RecoveryOutcome, InventoryMeasurement, FileMeasurement), IsolatedVerifyError> {
    if !stderr.is_empty() {
        return Err(IsolatedVerifyError::Protocol(
            "worker stderr was not empty".to_string(),
        ));
    }
    let line = std::str::from_utf8(stdout)
        .map_err(|_| IsolatedVerifyError::Protocol("stdout was not UTF-8".to_string()))?;
    match status.code() {
        Some(EXIT_REJECTED) if line == "NEV1\tREJECTED\n" => {
            return Err(IsolatedVerifyError::Rejected);
        }
        Some(EXIT_ERROR) if line == "NEV1\tERROR\n" => {
            return Err(IsolatedVerifyError::WorkerError);
        }
        Some(EXIT_CLEAN) => {}
        Some(EXIT_REJECTED | EXIT_ERROR) => {
            return Err(IsolatedVerifyError::Protocol(
                "error exit carried malformed response".to_string(),
            ));
        }
        _ => return Err(IsolatedVerifyError::WorkerAborted(status)),
    }
    if !line.ends_with('\n') || line[..line.len() - 1].contains('\n') {
        return Err(IsolatedVerifyError::Protocol(
            "response was not exactly one newline-terminated record".to_string(),
        ));
    }
    let fields: Vec<&str> = line.trim_end_matches('\n').split('\t').collect();
    if fields.len() != 12
        || fields[0] != ISOLATED_VERIFY_PROTOCOL
        || fields[1] != "RECOVERY"
        || fields[11] != "0"
    {
        return Err(IsolatedVerifyError::Protocol(
            "unexpected recovery response shape".to_string(),
        ));
    }
    let force_halted = match fields[2] {
        "0" => false,
        "1" => true,
        _ => {
            return Err(IsolatedVerifyError::Protocol(
                "invalid force_halted flag".to_string(),
            ));
        }
    };
    let parent_run_id = parse_optional_run_id(fields[3])?;
    let risk_source_run_id = parse_optional_run_id(fields[4])?;
    let recovered = parse_recovered_runs(fields[5])?;
    let inventory = InventoryMeasurement {
        files: parse_u64(fields[6], "inventory_files")?,
        total_bytes: parse_u64(fields[7], "inventory_bytes")?,
        digest: parse_digest(fields[8], "inventory_digest")?,
        marker_sidecar_present: false,
    };
    let helper = protocol_measurement(fields[9], fields[10], "helper")?;
    Ok((
        RecoveryOutcome {
            recovered,
            parent_run_id,
            risk_source_run_id,
            force_halted,
        },
        inventory,
        helper,
    ))
}

fn parse_optional_run_id(value: &str) -> Result<Option<String>, IsolatedVerifyError> {
    if value == "-" {
        return Ok(None);
    }
    validate_run_id(value)?;
    Ok(Some(value.to_string()))
}

fn validate_run_id(value: &str) -> Result<(), IsolatedVerifyError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(IsolatedVerifyError::Protocol(
            "invalid run id in response".to_string(),
        ));
    }
    Ok(())
}

fn parse_recovered_runs(value: &str) -> Result<Vec<RecoveredRun>, IsolatedVerifyError> {
    if value == "-" {
        return Ok(Vec::new());
    }
    let mut recovered = Vec::new();
    for encoded in value.split(',') {
        if recovered.len() >= MAX_RECOVERED_RUNS {
            return Err(IsolatedVerifyError::Protocol(
                "too many recovered runs in response".to_string(),
            ));
        }
        let Some((run_id, status)) = encoded.split_once(':') else {
            return Err(IsolatedVerifyError::Protocol(
                "invalid recovered run record".to_string(),
            ));
        };
        validate_run_id(run_id)?;
        let status = match status {
            "CrashedRecovered" => RunStatus::CrashedRecovered,
            "Quarantined" => RunStatus::Quarantined,
            _ => {
                return Err(IsolatedVerifyError::Protocol(
                    "invalid recovered run status".to_string(),
                ));
            }
        };
        recovered.push(RecoveredRun {
            run_id: run_id.to_string(),
            status,
        });
    }
    Ok(recovered)
}

fn protocol_measurement(
    len: &str,
    digest: &str,
    field: &str,
) -> Result<FileMeasurement, IsolatedVerifyError> {
    Ok(FileMeasurement {
        len: parse_u64(len, field)?,
        digest: parse_digest(digest, field)?,
        #[cfg(unix)]
        device: 0,
        #[cfg(unix)]
        inode: 0,
        #[cfg(unix)]
        modified_seconds: 0,
        #[cfg(unix)]
        modified_nanoseconds: 0,
    })
}

fn parse_u64(value: &str, field: &str) -> Result<u64, IsolatedVerifyError> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return Err(IsolatedVerifyError::Protocol(format!(
            "non-canonical integer for {field}"
        )));
    }
    value
        .parse()
        .map_err(|_| IsolatedVerifyError::Protocol(format!("invalid integer for {field}")))
}

fn parse_digest(value: &str, field: &str) -> Result<ContentDigest, IsolatedVerifyError> {
    ContentDigest::from_str(value)
        .map_err(|_| IsolatedVerifyError::Protocol(format!("invalid digest for {field}")))
}

/// Executes the worker-side clean verification and produces one bounded protocol record.
///
/// The binary must apply OS resource limits before calling this function.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "the worker response keeps every receipt trust check in one auditable fail-closed path"
)]
pub fn receipt_worker_response(path: &Path, limits: VerifyLimits) -> (&'static str, i32, String) {
    let result = (|| {
        let helper_path = match std::env::current_exe() {
            Ok(path) => path,
            Err(error) => {
                debug_receipt_rejection(&format!("current executable: {error}"));
                return None;
            }
        };
        let helper_measurement = match measure_regular_file(&helper_path, MAX_HELPER_BYTES) {
            Ok(measurement) => measurement,
            Err(error) => {
                debug_receipt_rejection(&format!("helper measurement: {error}"));
                return None;
            }
        };
        // Marker verification is not yet stream-bounded. Refuse the entire evidence bundle when
        // a sidecar exists rather than silently minting a receipt from only the main store.
        let marker_path = marker_sidecar_path(path).ok()?;
        if marker_path.exists() {
            debug_receipt_rejection("marker sidecar present");
            return None;
        }
        // Verify a worker-private immutable snapshot. Measuring a pathname before and after a
        // redb open is insufficient against an ABA replacement; the receipt therefore binds the
        // exact bytes copied from one held source handle, and redb never reopens the untrusted
        // source pathname.
        let (_snapshot_dir, snapshot_path, source_measurement) =
            match snapshot_untrusted_file(path, limits.max_file_bytes) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    debug_receipt_rejection(&format!("source snapshot: {error}"));
                    return None;
                }
            };
        let verifier = match Verifier::open_redb_file_with_limits(&snapshot_path, limits) {
            Ok(verifier) => verifier,
            Err(error) => {
                debug_receipt_rejection(&format!("snapshot open: {error}"));
                return None;
            }
        };
        let report = match verifier.verify() {
            Ok(report) => report,
            Err(error) => {
                debug_receipt_rejection(&format!("snapshot verify: {error}"));
                return None;
            }
        };
        if !report.is_clean()
            || !matches!(
                report.status,
                RunStatus::Ended | RunStatus::CrashedRecovered
            )
        {
            debug_receipt_rejection("verifier report not clean or terminal");
            return None;
        }
        let commitment_backend = match RedbBackend::open_sealed_file_with_read_limits(
            &snapshot_path,
            limits.max_file_bytes,
            limits.max_manifest_bytes,
        ) {
            Ok(backend) => backend,
            Err(error) => {
                debug_receipt_rejection(&format!("commitment manifest open: {error}"));
                return None;
            }
        };
        let manifest = match commitment_backend.manifest() {
            Ok(manifest) => manifest,
            Err(error) => {
                debug_receipt_rejection(&format!("commitment manifest read: {error}"));
                return None;
            }
        };
        if !is_canonical_content_digest(&manifest.binary_hash)
            || !is_canonical_content_digest(&manifest.crate_versions)
            || !is_canonical_content_digest(&manifest.config_hash)
        {
            debug_receipt_rejection("missing canonical host commitments");
            return None;
        }
        let store_digest = report.store_digest?;
        if marker_path.exists() {
            debug_receipt_rejection("marker sidecar appeared during verification");
            return None;
        }
        Some(format!(
            "{ISOLATED_VERIFY_PROTOCOL}\tCLEAN\t{}\t{:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t0\n",
            report.run_id,
            report.status,
            report.high_watermark,
            report.entries_scanned,
            store_digest,
            source_measurement.len,
            source_measurement.digest,
            helper_measurement.len,
            helper_measurement.digest,
        ))
    })();
    match result {
        Some(response) if response.len() <= MAX_PROTOCOL_OUTPUT_BYTES => {
            ("clean", EXIT_CLEAN, response)
        }
        _ => (
            "rejected",
            EXIT_REJECTED,
            format!("{ISOLATED_VERIFY_PROTOCOL}\tREJECTED\n"),
        ),
    }
}

fn debug_receipt_rejection(reason: &str) {
    if std::env::var_os("NAUTILUS_EVENT_STORE_VERIFY_DEBUG_RECEIPT").is_some() {
        eprintln!("receipt rejection: {reason}");
    }
}

fn is_canonical_content_digest(value: &str) -> bool {
    ContentDigest::from_str(value).is_ok_and(|digest| digest.to_string() == value)
}

#[expect(
    clippy::too_many_lines,
    reason = "snapshot copy, measurement, and immutable-file checks form one atomic trust boundary"
)]
fn snapshot_untrusted_file(
    path: &Path,
    max_bytes: u64,
) -> Result<(tempfile::TempDir, PathBuf, FileMeasurement), IsolatedVerifyError> {
    let canonical = canonical_regular_file(path)?;
    let mut source = File::open(&canonical).map_err(|error| IsolatedVerifyError::Measurement {
        path: canonical.display().to_string(),
        reason: error.to_string(),
    })?;
    let metadata_before = source
        .metadata()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: canonical.display().to_string(),
            reason: error.to_string(),
        })?;
    if !metadata_before.is_file() || metadata_before.len() > max_bytes {
        return Err(IsolatedVerifyError::Measurement {
            path: canonical.display().to_string(),
            reason: format!(
                "source is not a bounded regular file: observed={}, limit={max_bytes}",
                metadata_before.len()
            ),
        });
    }
    let snapshot_dir = tempfile::Builder::new()
        .prefix("nautilus-event-store-verify-")
        .tempdir()
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let instance_name = canonical
        .parent()
        .and_then(Path::file_name)
        .ok_or_else(|| IsolatedVerifyError::InvalidPath {
            path: canonical.display().to_string(),
            reason: "source path has no instance directory".to_string(),
        })?;
    let file_name = canonical
        .file_name()
        .ok_or_else(|| IsolatedVerifyError::InvalidPath {
            path: canonical.display().to_string(),
            reason: "source path has no file name".to_string(),
        })?;
    let snapshot_instance = snapshot_dir.path().join(instance_name);
    fs::create_dir(&snapshot_instance)
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let snapshot_path = snapshot_instance.join(file_name);
    let mut snapshot = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&snapshot_path)
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let mut builder = ContentDigestBuilder::new(metadata_before.len());
    let mut copied = 0_u64;
    let mut chunk = [0_u8; IO_CHUNK_BYTES];
    loop {
        let read = source
            .read(&mut chunk)
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: canonical.display().to_string(),
                reason: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).map_err(|error| IsolatedVerifyError::Measurement {
            path: canonical.display().to_string(),
            reason: error.to_string(),
        })?;
        copied = copied
            .checked_add(read_u64)
            .ok_or_else(|| IsolatedVerifyError::Measurement {
                path: canonical.display().to_string(),
                reason: "source copy length overflow".to_string(),
            })?;
        if copied > max_bytes {
            return Err(IsolatedVerifyError::Measurement {
                path: canonical.display().to_string(),
                reason: format!("source grew beyond byte limit {max_bytes}"),
            });
        }
        builder
            .update(&chunk[..read])
            .map_err(|error| IsolatedVerifyError::Measurement {
                path: canonical.display().to_string(),
                reason: error.to_string(),
            })?;
        snapshot
            .write_all(&chunk[..read])
            .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    }
    let digest = builder
        .finish()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: canonical.display().to_string(),
            reason: error.to_string(),
        })?;
    let metadata_after = source
        .metadata()
        .map_err(|error| IsolatedVerifyError::Measurement {
            path: canonical.display().to_string(),
            reason: error.to_string(),
        })?;
    let measurement_before = measurement_from_metadata(&metadata_before, digest);
    let measurement_after = measurement_from_metadata(&metadata_after, digest);
    if measurement_before != measurement_after || copied != measurement_after.len {
        return Err(IsolatedVerifyError::SourceChanged);
    }
    snapshot
        .sync_all()
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    let mut permissions = snapshot
        .metadata()
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?
        .permissions();
    permissions.set_readonly(true);
    drop(snapshot);
    fs::set_permissions(&snapshot_path, permissions)
        .map_err(|error| IsolatedVerifyError::WorkerIo(error.to_string()))?;
    Ok((snapshot_dir, snapshot_path, measurement_after))
}

/// Executes bounded predecessor recovery and emits one machine-protocol record.
///
/// The binary must apply OS resource limits before calling this function.
#[must_use]
pub fn recovery_worker_response(
    base_dir: &Path,
    instance_id: &str,
    limits: VerifyLimits,
) -> (&'static str, i32, String) {
    let result = (|| {
        validate_instance_id(instance_id).ok()?;
        let base_dir = canonical_directory(base_dir).ok()?;
        let inventory_before =
            measure_inventory(&base_dir, instance_id, limits.max_file_bytes).ok()?;
        // Marker recovery is not yet stream-bounded. Refuse the whole inventory rather than
        // recovering only the main stores and silently desynchronizing their sidecars.
        if inventory_before.marker_sidecar_present {
            return None;
        }
        let outcome = recover_predecessors_with_limits(&base_dir, instance_id, limits).ok()?;
        if outcome.recovered.len() > MAX_RECOVERED_RUNS {
            return None;
        }
        let inventory_after =
            measure_inventory(&base_dir, instance_id, limits.max_file_bytes).ok()?;
        if inventory_after.marker_sidecar_present {
            return None;
        }
        let helper_path = std::env::current_exe().ok()?;
        let helper_measurement = measure_regular_file(&helper_path, MAX_HELPER_BYTES).ok()?;
        let recovered = if outcome.recovered.is_empty() {
            "-".to_string()
        } else {
            outcome
                .recovered
                .iter()
                .map(|run| format!("{}:{:?}", run.run_id, run.status))
                .collect::<Vec<_>>()
                .join(",")
        };
        let parent = outcome.parent_run_id.as_deref().unwrap_or("-");
        let risk = outcome.risk_source_run_id.as_deref().unwrap_or("-");
        let force = u8::from(outcome.force_halted);
        Some(format!(
            "{ISOLATED_VERIFY_PROTOCOL}\tRECOVERY\t{force}\t{parent}\t{risk}\t{recovered}\t{}\t{}\t{}\t{}\t{}\t0\n",
            inventory_after.files,
            inventory_after.total_bytes,
            inventory_after.digest,
            helper_measurement.len,
            helper_measurement.digest,
        ))
    })();
    match result {
        Some(response) if response.len() <= MAX_PROTOCOL_OUTPUT_BYTES => {
            ("clean", EXIT_CLEAN, response)
        }
        _ => (
            "rejected",
            EXIT_REJECTED,
            format!("{ISOLATED_VERIFY_PROTOCOL}\tREJECTED\n"),
        ),
    }
}

/// Parses the ten decimal verification-limit arguments accepted by the worker protocol.
///
/// # Errors
///
/// Returns an error unless there are exactly ten canonical `u64` values.
pub fn parse_worker_limits(values: &[String]) -> Result<VerifyLimits, IsolatedVerifyError> {
    if values.len() != 10 {
        return Err(IsolatedVerifyError::Protocol(
            "worker expected ten limit values".to_string(),
        ));
    }
    Ok(VerifyLimits {
        max_high_watermark: parse_u64(&values[0], "max_high_watermark")?,
        max_entries: parse_u64(&values[1], "max_entries")?,
        max_index_entries: parse_u64(&values[2], "max_index_entries")?,
        max_file_bytes: parse_u64(&values[3], "max_file_bytes")?,
        max_entry_bytes: parse_u64(&values[4], "max_entry_bytes")?,
        max_total_entry_bytes: parse_u64(&values[5], "max_total_entry_bytes")?,
        max_total_index_key_bytes: parse_u64(&values[6], "max_total_index_key_bytes")?,
        max_findings: parse_u64(&values[7], "max_findings")?,
        max_snapshot_anchor_bytes: parse_u64(&values[8], "max_snapshot_anchor_bytes")?,
        max_manifest_bytes: parse_u64(&values[9], "max_manifest_bytes")?,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt},
        process::Command,
        time::{Duration, Instant},
    };

    use tempfile::TempDir;

    use super::{
        MAX_HELPER_BYTES, measure_regular_file, pin_trusted_helper_with_open_hook,
        trusted_helper_owner,
    };

    fn write_executable(path: &std::path::Path, contents: &[u8]) {
        fs::write(path, contents).expect("write helper fixture");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod helper fixture");
    }

    #[test]
    fn root_owned_deployment_helper_is_an_explicitly_supported_policy() {
        assert!(trusted_helper_owner(0, 501));
        assert!(trusted_helper_owner(501, 501));
        assert!(!trusted_helper_owner(502, 501));
    }

    #[test]
    fn private_helper_copy_is_fd_pinned_across_coordinated_pathname_aba() {
        let tmp = TempDir::new().expect("tempdir");
        let helper = tmp.path().join("verify");
        let malicious = tmp.path().join("malicious");
        let trusted_saved = tmp.path().join("trusted-saved");
        write_executable(&helper, b"#!/bin/sh\nprintf 'trusted\\n'\n");
        write_executable(&malicious, b"#!/bin/sh\nprintf 'malicious\\n'\n");
        let trusted_measurement =
            measure_regular_file(&helper, MAX_HELPER_BYTES).expect("measure trusted helper");

        let pinned = pin_trusted_helper_with_open_hook(
            &helper,
            trusted_measurement.digest,
            MAX_HELPER_BYTES,
            Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
            || {
                // The source descriptor is already open. Leave malicious bytes at the original
                // path throughout the copy; a pathname reopen would copy the wrong executable.
                fs::rename(&helper, &trusted_saved).expect("move trusted helper");
                fs::rename(&malicious, &helper).expect("install malicious helper");
            },
        )
        .expect("pin trusted source descriptor");

        // Complete the trusted -> malicious -> trusted pathname ABA after the private copy.
        fs::rename(&helper, &malicious).expect("remove malicious helper");
        fs::rename(&trusted_saved, &helper).expect("restore trusted helper");
        let pinned_measurement =
            measure_regular_file(&pinned.path, MAX_HELPER_BYTES).expect("measure pinned helper");
        assert_eq!(pinned_measurement.digest, trusted_measurement.digest);
        assert_eq!(pinned_measurement, pinned.pinned_measurement);
        assert_ne!(pinned.path, helper);

        let directory_metadata = fs::metadata(pinned.directory.path()).expect("private dir");
        let pinned_metadata = fs::metadata(&pinned.path).expect("private helper");
        assert_eq!(directory_metadata.mode() & 0o777, 0o700);
        assert_eq!(pinned_metadata.mode() & 0o777, 0o500);
        assert_eq!(directory_metadata.uid(), pinned_metadata.uid());

        // Replace the deployment path again immediately before execution. The worker path remains
        // the immutable private copy and therefore cannot execute the replacement.
        fs::rename(&helper, &trusted_saved).expect("move trusted helper again");
        fs::rename(&malicious, &helper).expect("replace deployment helper again");
        let output = Command::new(&pinned.path)
            .output()
            .expect("execute pinned helper");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"trusted\n");
    }
}
