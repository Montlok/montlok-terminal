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
    any::Any,
    env,
    ffi::OsStr,
    io::{self, Read},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::{Command, ExitCode, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use nautilus_event_store::{
    EventStoreError, IndexDrift, Verifier, VerifyError, VerifyFinding, VerifyReport,
    isolated::{parse_worker_limits, receipt_worker_response, recovery_worker_response},
};

const EXIT_CLEAN: u8 = 0;
const EXIT_CORRUPT: u8 = 1;
const EXIT_ERROR: u8 = 2;
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_WORKER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_WORKER_OUTPUT_BYTES: usize = 1024 * 1024;
const TIMEOUT_ENV: &str = "NAUTILUS_EVENT_STORE_VERIFY_TIMEOUT_SECS";
#[cfg(debug_assertions)]
const WORKER_SLEEP_ENV: &str = "NAUTILUS_EVENT_STORE_VERIFY_SLEEP_WORKER_MS";
#[cfg(debug_assertions)]
const WORKER_FLOOD_ENV: &str = "NAUTILUS_EVENT_STORE_VERIFY_FLOOD_WORKER_BYTES";

enum WorkerOutput {
    Exited(Output),
    TimedOut { output: Output, timeout: Duration },
    OutputLimitExceeded(Output),
}

fn main() -> ExitCode {
    let mut args = env::args_os();
    let program = args.next().unwrap_or_else(|| OsStr::new("verify").into());
    let Some(first) = args.next() else {
        print_usage(&program);
        return ExitCode::from(EXIT_ERROR);
    };

    if first.as_os_str() == OsStr::new("--worker") {
        let Some(path) = args.next() else {
            print_usage(&program);
            return ExitCode::from(EXIT_ERROR);
        };

        if args.next().is_some() {
            print_usage(&program);
            return ExitCode::from(EXIT_ERROR);
        }

        if apply_worker_resource_limits().is_err() {
            println!(
                "corrupt path={} worker_status=resource-limit-error quarantine=not-performed",
                Path::new(&path).display()
            );
            return ExitCode::from(EXIT_CORRUPT);
        }

        let path = PathBuf::from(path);
        return verify_run_file_worker(path.as_path());
    }

    if first.as_os_str() == OsStr::new("--receipt-worker-v1") {
        let Some(path) = args.next() else {
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        };
        let values: Vec<String> = args
            .map(|value| value.to_string_lossy().into_owned())
            .collect();
        if let Err(_error) = apply_worker_resource_limits() {
            if env::var_os("NAUTILUS_EVENT_STORE_VERIFY_DEBUG_RLIMIT").is_some() {
                eprintln!("resource limit error: {_error}");
            }
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        }
        let Ok(limits) = parse_worker_limits(&values) else {
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        };
        let (_, code, response) = receipt_worker_response(Path::new(&path), limits);
        print!("{response}");
        return ExitCode::from(u8::try_from(code).unwrap_or(EXIT_ERROR));
    }

    if first.as_os_str() == OsStr::new("--recovery-worker-v1") {
        let Some(base_dir) = args.next() else {
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        };
        let Some(instance_id) = args.next() else {
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        };
        let values: Vec<String> = args
            .map(|value| value.to_string_lossy().into_owned())
            .collect();
        if apply_worker_resource_limits().is_err() {
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        }
        let Ok(limits) = parse_worker_limits(&values) else {
            println!("NEV1\tERROR");
            return ExitCode::from(EXIT_ERROR);
        };
        let (_, code, response) = recovery_worker_response(
            Path::new(&base_dir),
            instance_id.to_string_lossy().as_ref(),
            limits,
        );
        print!("{response}");
        return ExitCode::from(u8::try_from(code).unwrap_or(EXIT_ERROR));
    }

    if args.next().is_some() {
        print_usage(&program);
        return ExitCode::from(EXIT_ERROR);
    }

    let path = PathBuf::from(first);
    verify_run_file(path.as_path())
}

fn verify_run_file(path: &Path) -> ExitCode {
    let output = match run_worker(path) {
        Ok(output) => output,
        Err(e) => {
            eprintln!("error path={} error=\"worker: {e}\"", path.display());
            return ExitCode::from(EXIT_ERROR);
        }
    };

    match output {
        WorkerOutput::Exited(output) => classify_worker_exit(path, &output),
        WorkerOutput::TimedOut { output, timeout } => {
            println!(
                "corrupt path={} worker_status=\"timeout after {}s\" quarantine=not-performed",
                path.display(),
                timeout.as_secs(),
            );
            relay_output(&output);
            ExitCode::from(EXIT_CORRUPT)
        }
        WorkerOutput::OutputLimitExceeded(output) => {
            println!(
                "corrupt path={} worker_status=output-limit-exceeded quarantine=not-performed",
                path.display(),
            );
            relay_output(&output);
            ExitCode::from(EXIT_CORRUPT)
        }
    }
}

fn classify_worker_exit(path: &Path, output: &Output) -> ExitCode {
    match output.status.code() {
        Some(code)
            if code == i32::from(EXIT_CLEAN)
                || code == i32::from(EXIT_CORRUPT)
                || code == i32::from(EXIT_ERROR) =>
        {
            relay_output(output);
            ExitCode::from(u8::try_from(code).expect("known verifier exit code"))
        }
        _ => {
            println!(
                "corrupt path={} worker_status=\"{}\" quarantine=not-performed",
                path.display(),
                output.status,
            );
            relay_output(output);
            ExitCode::from(EXIT_CORRUPT)
        }
    }
}

fn verify_run_file_worker(path: &Path) -> ExitCode {
    flood_worker_output_when_requested();
    abort_worker_when_requested();
    sleep_worker_when_requested();

    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(AssertUnwindSafe(|| verify_run_file_inner(path)));
    panic::set_hook(previous_hook);

    match result {
        Ok(code) => code,
        Err(payload) => {
            println!(
                "corrupt path={} panic=\"{}\" quarantine=not-performed",
                path.display(),
                panic_message(payload.as_ref()),
            );
            ExitCode::from(EXIT_CORRUPT)
        }
    }
}

fn verify_run_file_inner(path: &Path) -> ExitCode {
    match Verifier::open_redb_file(path).and_then(|verifier| verifier.verify()) {
        Ok(report) => match reject_unbounded_marker_sidecar(path) {
            Ok(()) => print_report(&report),
            Err((marker_path, err)) => {
                if report.is_clean() {
                    return print_marker_error(marker_path.as_path(), &err);
                }

                // Relay the entry findings before the marker error so a failing
                // sidecar cannot mask them.
                println!(
                    "corrupt run_id={} status={:?} high_watermark={} entries_scanned={} findings={} markers=error quarantine=not-performed",
                    report.run_id,
                    report.status,
                    report.high_watermark,
                    report.entries_scanned,
                    report.findings.len(),
                );

                for finding in &report.findings {
                    print_finding(finding);
                }

                print_marker_error(marker_path.as_path(), &err);
                ExitCode::from(EXIT_CORRUPT)
            }
        },
        Err(e) => print_error(path, &e),
    }
}

fn reject_unbounded_marker_sidecar(path: &Path) -> Result<(), (PathBuf, VerifyError)> {
    let Some(marker_path) = marker_sidecar_path(path) else {
        return Ok(());
    };
    match std::fs::symlink_metadata(&marker_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err((
            marker_path,
            VerifyError::Backend(EventStoreError::Corrupted(format!(
                "marker sidecar metadata could not be read safely: {error}"
            ))),
        )),
        Ok(_) => Err((
            marker_path,
            VerifyError::Backend(EventStoreError::Corrupted(
                "marker sidecar verification requires the bounded isolated helper protocol"
                    .to_string(),
            )),
        )),
    }
}

fn marker_sidecar_path(path: &Path) -> Option<PathBuf> {
    let stem = path.file_stem()?;
    let mut file_name = stem.to_os_string();
    file_name.push(".markers.redb");
    Some(path.with_file_name(file_name))
}

fn print_report(report: &VerifyReport) -> ExitCode {
    if report.is_clean() {
        println!(
            "clean run_id={} status={:?} high_watermark={} entries_scanned={} markers=absent",
            report.run_id, report.status, report.high_watermark, report.entries_scanned,
        );
        return ExitCode::from(EXIT_CLEAN);
    }

    println!(
        "corrupt run_id={} status={:?} high_watermark={} entries_scanned={} findings={} marker_findings=0 markers=absent quarantine=not-performed",
        report.run_id,
        report.status,
        report.high_watermark,
        report.entries_scanned,
        report.findings.len(),
    );

    for finding in &report.findings {
        print_finding(finding);
    }

    ExitCode::from(EXIT_CORRUPT)
}

fn print_error(path: &Path, err: &VerifyError) -> ExitCode {
    if matches!(err, VerifyError::Backend(EventStoreError::Corrupted(_))) {
        println!(
            "corrupt path={} error=\"{}\" quarantine=not-performed",
            path.display(),
            err,
        );
        return ExitCode::from(EXIT_CORRUPT);
    }

    eprintln!("error path={} error=\"{}\"", path.display(), err);
    ExitCode::from(EXIT_ERROR)
}

fn print_marker_error(path: &Path, err: &VerifyError) -> ExitCode {
    if matches!(err, VerifyError::Backend(EventStoreError::Corrupted(_))) {
        println!(
            "corrupt path={} markers=error error=\"{}\" quarantine=not-performed",
            path.display(),
            err,
        );
        return ExitCode::from(EXIT_CORRUPT);
    }

    eprintln!(
        "error path={} markers=error error=\"{}\"",
        path.display(),
        err
    );
    ExitCode::from(EXIT_ERROR)
}

fn print_finding(finding: &VerifyFinding) {
    match finding {
        VerifyFinding::HashMismatch { seq } => {
            println!("- hash mismatch at seq {seq}");
        }
        VerifyFinding::Gap { range } => {
            println!("- gap from seq {} to {}", range.from, range.to);
        }
        VerifyFinding::SeqMismatch {
            table_key,
            embedded_seq,
        } => {
            println!("- seq mismatch at table key {table_key}: embedded seq was {embedded_seq}");
        }
        VerifyFinding::Undecodable { seq, reason } => {
            println!("- undecodable entry at seq {seq}: {reason}");
        }
        VerifyFinding::IndexDrift { kind, key, drift } => {
            print_index_drift(*kind, key, *drift);
        }
        VerifyFinding::ManifestMismatch { kind, reason } => {
            println!("- manifest mismatch {kind:?}: {reason}");
        }
        VerifyFinding::SnapshotAnchorInvalid { reason } => {
            println!("- snapshot anchor invalid: {reason}");
        }
    }
}

fn print_index_drift(kind: nautilus_event_store::IndexKind, key: &str, drift: IndexDrift) {
    match drift {
        IndexDrift::DanglingTarget { stored_seq } => {
            println!("- index drift {kind:?} key={key}: dangling target seq {stored_seq}");
        }
        IndexDrift::TargetCorrupted { stored_seq } => {
            println!("- index drift {kind:?} key={key}: corrupted target seq {stored_seq}");
        }
    }
}

fn print_usage(program: &OsStr) {
    eprintln!("usage: {} <run-file.redb>", program.to_string_lossy());
}

fn run_worker(path: &Path) -> io::Result<WorkerOutput> {
    let current_exe = env::current_exe()?;
    let mut child = Command::new(current_exe)
        .arg("--worker")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("verifier worker stdout pipe missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("verifier worker stderr pipe missing"))?;
    // Drain both pipes while the child runs so an attacker-controlled finding cannot deadlock the
    // worker. Retain only a fixed prefix; continuing to read after the cap keeps the pipe flowing
    // without allowing parent-process memory growth.
    let stdout_reader = thread::spawn(move || read_bounded_worker_pipe(stdout));
    let stderr_reader = thread::spawn(move || read_bounded_worker_pipe(stderr));
    let timeout = worker_timeout();
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "verifier worker timeout deadline overflow",
        )
    })?;

    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait()? {
            break (status, false);
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            break (child.wait()?, true);
        }

        thread::sleep(WORKER_POLL_INTERVAL);
    };
    let stdout = join_worker_pipe_reader(stdout_reader)?;
    let stderr = join_worker_pipe_reader(stderr_reader)?;
    let exceeded = stdout.exceeded || stderr.exceeded;
    let output = Output {
        status,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
    };
    if timed_out {
        Ok(WorkerOutput::TimedOut { output, timeout })
    } else if exceeded {
        Ok(WorkerOutput::OutputLimitExceeded(output))
    } else {
        Ok(WorkerOutput::Exited(output))
    }
}

struct BoundedWorkerPipe {
    bytes: Vec<u8>,
    exceeded: bool,
}

fn read_bounded_worker_pipe(mut pipe: impl Read) -> io::Result<BoundedWorkerPipe> {
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        let read = pipe.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        let available = MAX_WORKER_OUTPUT_BYTES.saturating_sub(bytes.len());
        let retained = read.min(available);
        bytes.extend_from_slice(&chunk[..retained]);
        exceeded |= retained != read;
    }
    Ok(BoundedWorkerPipe { bytes, exceeded })
}

fn join_worker_pipe_reader(
    reader: thread::JoinHandle<io::Result<BoundedWorkerPipe>>,
) -> io::Result<BoundedWorkerPipe> {
    reader
        .join()
        .map_err(|_| io::Error::other("verifier worker output reader panicked"))?
}

fn relay_output(output: &Output) {
    if !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }

    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "unknown panic"
    }
}

fn worker_timeout() -> Duration {
    env::var(TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(DEFAULT_WORKER_TIMEOUT, Duration::from_secs)
}

fn abort_worker_when_requested() {
    #[cfg(debug_assertions)]
    if env::var_os("NAUTILUS_EVENT_STORE_VERIFY_ABORT_WORKER").is_some() {
        std::process::abort();
    }
}

fn sleep_worker_when_requested() {
    #[cfg(debug_assertions)]
    if let Some(raw) = env::var_os(WORKER_SLEEP_ENV)
        && let Ok(ms) = raw.to_string_lossy().parse::<u64>()
    {
        thread::sleep(Duration::from_millis(ms));
    }
}

fn flood_worker_output_when_requested() {
    #[cfg(debug_assertions)]
    if let Some(raw) = env::var_os(WORKER_FLOOD_ENV)
        && let Ok(requested) = raw.to_string_lossy().parse::<usize>()
    {
        use std::io::Write as _;

        let mut remaining = requested.min(MAX_WORKER_OUTPUT_BYTES.saturating_mul(4));
        let mut stdout = io::stdout().lock();
        let chunk = [b'X'; 8 * 1024];
        while remaining != 0 {
            let write = remaining.min(chunk.len());
            if stdout.write_all(&chunk[..write]).is_err() {
                return;
            }
            remaining -= write;
        }
        let _ = stdout.flush();
    }
}

#[cfg(unix)]
fn apply_worker_resource_limits() -> io::Result<()> {
    const ADDRESS_SPACE_BYTES: libc::rlim_t = 2 * 1024 * 1024 * 1024;
    const RESIDENT_BYTES: libc::rlim_t = 1536 * 1024 * 1024;
    const CPU_SECONDS: libc::rlim_t = 30;
    const OUTPUT_FILE_BYTES: libc::rlim_t = 8 * 1024 * 1024 * 1024;
    const OPEN_FILES: libc::rlim_t = 64;

    #[cfg(not(target_os = "macos"))]
    set_resource_limit(libc::RLIMIT_AS, ADDRESS_SPACE_BYTES)
        .map_err(|error| io::Error::new(error.kind(), format!("RLIMIT_AS: {error}")))?;
    // Darwin exposes RLIMIT_AS/RLIMIT_RSS but rejects finite values with EINVAL. The helper still
    // runs in a killable child under strict row/byte budgets and CPU/FSIZE/NOFILE limits; Linux
    // production deployments additionally enforce the hard address-space ceiling above.
    #[cfg(target_os = "macos")]
    let _ = ADDRESS_SPACE_BYTES;
    // RLIMIT_RSS is advisory or unsupported on several Unix kernels; RLIMIT_AS is the hard memory
    // ceiling. Apply RSS where available without weakening the mandatory address-space limit.
    let _ = set_resource_limit(libc::RLIMIT_RSS, RESIDENT_BYTES);
    set_resource_limit(libc::RLIMIT_CPU, CPU_SECONDS)
        .map_err(|error| io::Error::new(error.kind(), format!("RLIMIT_CPU: {error}")))?;
    // Receipt verification writes one private immutable source snapshot; recovery may also
    // repair/seal one already-budgeted redb file. Cap either write at the same 8 GiB file budget.
    set_resource_limit(libc::RLIMIT_FSIZE, OUTPUT_FILE_BYTES)
        .map_err(|error| io::Error::new(error.kind(), format!("RLIMIT_FSIZE: {error}")))?;
    set_resource_limit(libc::RLIMIT_NOFILE, OPEN_FILES)
        .map_err(|error| io::Error::new(error.kind(), format!("RLIMIT_NOFILE: {error}")))
}

#[cfg(any(target_os = "linux", target_os = "hurd"))]
fn set_resource_limit(resource: libc::__rlimit_resource_t, value: libc::rlim_t) -> io::Result<()> {
    set_resource_limit_inner(resource, value)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "hurd"))))]
fn set_resource_limit(resource: libc::c_int, value: libc::rlim_t) -> io::Result<()> {
    set_resource_limit_inner(resource, value)
}

#[cfg(any(target_os = "linux", target_os = "hurd"))]
fn set_resource_limit_inner(
    resource: libc::__rlimit_resource_t,
    value: libc::rlim_t,
) -> io::Result<()> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid writable `libc::rlimit`; the resource constant is provided by
    // libc. The call only reads the current helper-process limit.
    if unsafe { libc::getrlimit(resource, std::ptr::addr_of_mut!(limit)) } != 0 {
        return Err(io::Error::last_os_error());
    }
    limit.rlim_cur = value.min(limit.rlim_max);
    // SAFETY: `limit` points to a fully initialized `libc::rlimit` for the duration of the call;
    // the resource constants are provided by libc for this target. This runs before untrusted redb
    // input is opened and only reduces limits of the current helper process.
    let result = unsafe { libc::setrlimit(resource, std::ptr::addr_of!(limit)) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "hurd"))))]
fn set_resource_limit_inner(resource: libc::c_int, value: libc::rlim_t) -> io::Result<()> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid writable `libc::rlimit`; the resource constant is provided by
    // libc. The call only reads the current helper-process limit.
    if unsafe { libc::getrlimit(resource, std::ptr::addr_of_mut!(limit)) } != 0 {
        return Err(io::Error::last_os_error());
    }
    limit.rlim_cur = value.min(limit.rlim_max);
    // SAFETY: `limit` points to a fully initialized `libc::rlimit` for the duration of the call;
    // the resource constants are provided by libc for this target. This runs before untrusted redb
    // input is opened and only reduces limits of the current helper process.
    let result = unsafe { libc::setrlimit(resource, std::ptr::addr_of!(limit)) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn apply_worker_resource_limits() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "isolated verifier OS resource limits are unavailable on this platform",
    ))
}
