// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Narrow isolated event-store worker shipped with the backtest host.
//!
//! The parent process measures this exact executable before spawning it. Only the two bounded
//! machine-protocol worker modes are exposed; normal verification CLI behavior remains in the
//! event-store crate's `verify` binary.

use std::{env, ffi::OsStr, io, path::Path, process::ExitCode};

use nautilus_event_store::isolated::{
    parse_worker_limits, receipt_worker_response, recovery_worker_response,
};

const EXIT_ERROR: u8 = 2;

fn main() -> ExitCode {
    let mut args = env::args_os();
    let _program = args.next();
    let Some(mode) = args.next() else {
        return protocol_error();
    };

    if mode.as_os_str() == OsStr::new("--receipt-worker-v1") {
        let Some(path) = args.next() else {
            return protocol_error();
        };
        let values = args
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if apply_worker_resource_limits().is_err() {
            return protocol_error();
        }
        let Ok(limits) = parse_worker_limits(&values) else {
            return protocol_error();
        };
        let (_, code, response) = receipt_worker_response(Path::new(&path), limits);
        print!("{response}");
        return ExitCode::from(u8::try_from(code).unwrap_or(EXIT_ERROR));
    }

    if mode.as_os_str() == OsStr::new("--recovery-worker-v1") {
        let Some(base_dir) = args.next() else {
            return protocol_error();
        };
        let Some(instance_id) = args.next() else {
            return protocol_error();
        };
        let values = args
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if apply_worker_resource_limits().is_err() {
            return protocol_error();
        }
        let Ok(limits) = parse_worker_limits(&values) else {
            return protocol_error();
        };
        let (_, code, response) = recovery_worker_response(
            Path::new(&base_dir),
            instance_id.to_string_lossy().as_ref(),
            limits,
        );
        print!("{response}");
        return ExitCode::from(u8::try_from(code).unwrap_or(EXIT_ERROR));
    }

    protocol_error()
}

fn protocol_error() -> ExitCode {
    println!("NEV1\tERROR");
    ExitCode::from(EXIT_ERROR)
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
    #[cfg(target_os = "macos")]
    let _ = ADDRESS_SPACE_BYTES;
    let _ = set_resource_limit(libc::RLIMIT_RSS, RESIDENT_BYTES);
    set_resource_limit(libc::RLIMIT_CPU, CPU_SECONDS)
        .map_err(|error| io::Error::new(error.kind(), format!("RLIMIT_CPU: {error}")))?;
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
    // SAFETY: `limit` is writable and `resource` is one of libc's target-specific constants.
    if unsafe { libc::getrlimit(resource, &raw mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    limit.rlim_cur = value.min(limit.rlim_max);
    // SAFETY: `limit` is initialized and lives through the call; this only lowers this helper's
    // own resource limit before any untrusted redb bytes are opened.
    if unsafe { libc::setrlimit(resource, &raw const limit) } == 0 {
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
    // SAFETY: `limit` is writable and `resource` is one of libc's target-specific constants.
    if unsafe { libc::getrlimit(resource, &raw mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    limit.rlim_cur = value.min(limit.rlim_max);
    // SAFETY: `limit` is initialized and lives through the call; this only lowers this helper's
    // own resource limit before any untrusted redb bytes are opened.
    if unsafe { libc::setrlimit(resource, &raw const limit) } == 0 {
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
