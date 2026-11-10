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

//! Process-wide panic hook that routes panics through the logging system.
//!
//! Release builds use `panic = "abort"`, so a panic on any thread (including tokio worker
//! threads running adapter tasks) terminates the process without unwinding. The default hook
//! only writes to stderr, which is frequently lost on remote deployments. Installing this hook
//! guarantees that the panic location, message, thread and (when enabled) backtrace are written
//! through the logger and that file logs are flushed before the process dies, so a crash can
//! be diagnosed post-mortem from the log files alone.

use std::{
    backtrace::{Backtrace, BacktraceStatus},
    panic::{AssertUnwindSafe, PanicHookInfo},
    sync::Once,
    time::Duration,
};

use ustr::Ustr;

use crate::logging::logger::sync_to_disk_with_timeout;

/// Component name under which panics are logged.
pub const PANIC_COMPONENT: &str = "PANIC";

/// Maximum time the hook waits for file logs to reach disk before yielding to the default
/// panic behaviour. Bounded so that a wedged logging thread can never turn a crash into a hang.
pub const PANIC_LOG_FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

static INSTALL_PANIC_HOOK: Once = Once::new();

/// Installs the logging panic hook exactly once for the process.
///
/// The previously installed hook (normally the std default that prints to stderr) is chained
/// and still runs after logging, so existing behaviour is preserved. Calling this multiple
/// times is safe and cheap.
pub fn install_panic_logging_hook() {
    INSTALL_PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            log_panic(info);
            previous(info);
        }));
    });
}

/// Returns whether the logging panic hook has been installed.
#[must_use]
pub fn is_panic_logging_hook_installed() -> bool {
    INSTALL_PANIC_HOOK.is_completed()
}

/// Renders a single-line description of a panic suitable for a log record.
#[must_use]
pub fn format_panic_message(info: &PanicHookInfo<'_>) -> String {
    let thread = std::thread::current();
    let thread_name = thread.name().unwrap_or("<unnamed>");
    let location = info.location().map_or_else(
        || "<unknown location>".to_string(),
        |loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()),
    );
    let payload = payload_message(info.payload());

    format!("thread '{thread_name}' panicked at {location}: {payload}")
}

fn payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

fn log_panic(info: &PanicHookInfo<'_>) {
    // A panic inside the hook itself would abort with a far less useful message, so every
    // step is isolated and best-effort.
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let component = Ustr::from(PANIC_COMPONENT);
        let message = format_panic_message(info);
        log::error!(component = component.to_string(); "{message}");

        let backtrace = Backtrace::capture();
        if backtrace.status() == BacktraceStatus::Captured {
            log::error!(component = component.to_string(); "backtrace:\n{backtrace}");
        }

        if let Err(e) = sync_to_disk_with_timeout(Some(PANIC_LOG_FLUSH_TIMEOUT)) {
            eprintln!("failed to flush logs after panic: {e}");
        }
    }));
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_install_is_idempotent_and_preserves_unwinding() {
        install_panic_logging_hook();
        install_panic_logging_hook();
        assert!(is_panic_logging_hook_installed());

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            panic!("supervised test panic");
        }));
        let payload = result.expect_err("panic should propagate through the hook");
        assert_eq!(payload_message(payload.as_ref()), "supervised test panic");
    }

    #[rstest]
    fn test_payload_message_handles_string_payloads() {
        let owned: Box<dyn std::any::Any + Send> = Box::new(String::from("owned"));
        let borrowed: Box<dyn std::any::Any + Send> = Box::new("borrowed");
        let other: Box<dyn std::any::Any + Send> = Box::new(42_u8);

        assert_eq!(payload_message(owned.as_ref()), "owned");
        assert_eq!(payload_message(borrowed.as_ref()), "borrowed");
        assert_eq!(
            payload_message(other.as_ref()),
            "<non-string panic payload>"
        );
    }
}
