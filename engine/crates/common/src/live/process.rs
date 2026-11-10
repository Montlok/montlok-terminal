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

//! Lightweight sampling of the current process' resource usage.
//!
//! Long-running live nodes need a cheap, allocation-free way to observe their own memory and
//! CPU so that leaks and runaway loops show up in the logs long before the OS OOM killer or
//! the venue notices. [`ProcessMonitor`] keeps a single [`sysinfo::System`] and refreshes only
//! this process, so periodic sampling costs a handful of syscalls.

use std::time::Duration;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Minimum interval between samples for the CPU usage figure to be meaningful.
pub const MIN_CPU_SAMPLE_INTERVAL: Duration = sysinfo::MINIMUM_CPU_UPDATE_INTERVAL;

/// A point-in-time view of the current process' resource usage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessSample {
    /// Resident set size in bytes.
    pub rss_bytes: u64,
    /// Virtual memory size in bytes.
    pub virtual_bytes: u64,
    /// CPU usage in percent of a single core since the previous sample (may exceed 100 on
    /// multi-core hosts). Zero for the first sample.
    pub cpu_percent: f32,
}

impl ProcessSample {
    /// Returns the resident set size in mebibytes.
    #[must_use]
    pub fn rss_mib(&self) -> f64 {
        self.rss_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Returns the virtual memory size in mebibytes.
    #[must_use]
    pub fn virtual_mib(&self) -> f64 {
        self.virtual_bytes as f64 / (1024.0 * 1024.0)
    }
}

/// Samples memory and CPU usage of the current process.
#[derive(Debug)]
pub struct ProcessMonitor {
    system: System,
    pid: Pid,
    refresh_kind: ProcessRefreshKind,
}

impl ProcessMonitor {
    /// Creates a monitor for the current process.
    ///
    /// Returns `None` when the platform cannot report the current pid, in which case callers
    /// should simply omit process metrics rather than fail.
    #[must_use]
    pub fn new() -> Option<Self> {
        let pid = sysinfo::get_current_pid().ok()?;
        Some(Self {
            system: System::new(),
            pid,
            refresh_kind: ProcessRefreshKind::nothing().with_memory().with_cpu(),
        })
    }

    /// Refreshes and returns the latest sample, or `None` if the process could not be read.
    pub fn sample(&mut self) -> Option<ProcessSample> {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            false,
            self.refresh_kind,
        );
        let process = self.system.process(self.pid)?;
        Some(ProcessSample {
            rss_bytes: process.memory(),
            virtual_bytes: process.virtual_memory(),
            cpu_percent: process.cpu_usage(),
        })
    }
}

/// Formats a byte count as a compact human-readable string (e.g. `512.0 MiB`).
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_sample_reports_resident_memory() {
        let mut monitor = ProcessMonitor::new().expect("current pid should be available");
        let sample = monitor
            .sample()
            .expect("current process should be readable");
        assert!(sample.rss_bytes > 0);
        assert!(sample.rss_mib() > 0.0);
        assert!(sample.virtual_bytes >= sample.rss_bytes || sample.virtual_bytes == 0);
    }

    #[rstest]
    #[case(0, "0 B")]
    #[case(1023, "1023 B")]
    #[case(1024, "1.0 KiB")]
    #[case(1536, "1.5 KiB")]
    #[case(5 * 1024 * 1024, "5.0 MiB")]
    #[case(3 * 1024 * 1024 * 1024, "3.0 GiB")]
    fn test_format_bytes(#[case] bytes: u64, #[case] expected: &str) {
        assert_eq!(format_bytes(bytes), expected);
    }
}
