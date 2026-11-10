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

//! Periodic runtime health reporting for `LiveNode::run`.
//!
//! Operators of a remote node need one place to answer "was the process healthy before it
//! died?" without attaching a profiler. [`HealthReporter`] turns the lock-free runner metrics
//! plus a process sample into a single INFO line per window, and raises a WARN the first time
//! any bounded ingress lane's high-watermark crosses the configured pressure ratio, so backlog
//! growth is visible long before the fail-stop overflow path fires.
//!
//! Formatting is separated from logging (see [`HealthReport`]) so the exact output is unit
//! testable and stable enough to grep in post-mortems.

use std::{fmt, time::Duration};

use nautilus_common::live::{LiveIngressLane, ProcessMonitor, ProcessSample, dst, format_bytes};

use super::metrics::{RunnerChannelMetricsSnapshot, RunnerMetricsDelta, RunnerMetricsSnapshot};

/// Default interval between health lines when the config does not specify one.
pub const DEFAULT_HEALTH_REPORT_INTERVAL: Duration = Duration::from_mins(1);

/// High-watermark to capacity ratio above which a lane is reported as under pressure.
pub const DEFAULT_QUEUE_PRESSURE_WARN_RATIO: f64 = 0.5;

const LANES: [LiveIngressLane; 5] = [
    LiveIngressLane::TimeEvents,
    LiveIngressLane::ExecEvents,
    LiveIngressLane::ExecCommands,
    LiveIngressLane::DataEvents,
    LiveIngressLane::DataCommands,
];

/// Policy controlling how often health is reported and when queue pressure is flagged.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HealthReportPolicy {
    /// Interval between INFO health lines.
    pub interval: Duration,
    /// High-watermark to capacity ratio which triggers a one-time WARN per lane.
    pub queue_pressure_warn_ratio: f64,
}

impl HealthReportPolicy {
    /// Creates a policy with the given interval and the default pressure ratio.
    #[must_use]
    pub const fn with_interval(interval: Duration) -> Self {
        Self {
            interval,
            queue_pressure_warn_ratio: DEFAULT_QUEUE_PRESSURE_WARN_RATIO,
        }
    }
}

impl Default for HealthReportPolicy {
    fn default() -> Self {
        Self::with_interval(DEFAULT_HEALTH_REPORT_INTERVAL)
    }
}

/// Drives periodic health reporting from the runner's maintenance tick.
#[derive(Debug)]
pub(crate) struct HealthReporter {
    policy: HealthReportPolicy,
    next_due: dst::time::Instant,
    last_snapshot: RunnerMetricsSnapshot,
    process: Option<ProcessMonitor>,
    pressure_warned: [bool; LANES.len()],
}

impl HealthReporter {
    /// Creates a reporter whose first report is due one interval after `now`.
    ///
    /// The process monitor is primed immediately so the first CPU figure covers a full window.
    pub(crate) fn new(
        policy: HealthReportPolicy,
        now: dst::time::Instant,
        initial_snapshot: &RunnerMetricsSnapshot,
    ) -> Self {
        let mut process = ProcessMonitor::new();
        if let Some(monitor) = process.as_mut() {
            let _ = monitor.sample();
        }
        Self {
            policy,
            next_due: now + policy.interval,
            last_snapshot: *initial_snapshot,
            process,
            pressure_warned: [false; LANES.len()],
        }
    }

    pub(crate) fn is_due(&self, now: dst::time::Instant) -> bool {
        now >= self.next_due
    }

    /// Emits the INFO health line (and any first-time lane pressure WARNs) for the window
    /// ending at `snapshot`, then schedules the next report relative to `now`.
    pub(crate) fn report(&mut self, now: dst::time::Instant, snapshot: &RunnerMetricsSnapshot) {
        let delta = RunnerMetricsDelta::from_snapshots(self.last_snapshot, *snapshot);
        let process = self.process.as_mut().and_then(ProcessMonitor::sample);
        let report = HealthReport {
            delta,
            snapshot: *snapshot,
            process,
        };
        log::info!("{report}");

        for (lane, metrics) in lanes_under_pressure(snapshot, self.policy.queue_pressure_warn_ratio)
        {
            let warned = &mut self.pressure_warned[lane_index(lane)];
            if !*warned {
                *warned = true;
                log::warn!(
                    "Runner ingress lane {lane} under pressure: high-watermark {} of capacity {} \
                     ({:.0}%); sustained growth ends in fail-stop overflow",
                    metrics.queue_high_watermark,
                    metrics.queue_capacity,
                    ratio(metrics.queue_high_watermark, metrics.queue_capacity) * 100.0,
                );
            }
        }

        self.last_snapshot = *snapshot;
        self.next_due = now + self.policy.interval;
    }
}

/// One window of derived health figures, formatted as a single log line.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HealthReport {
    /// Deltas across the window.
    pub delta: RunnerMetricsDelta,
    /// Point-in-time snapshot at the end of the window.
    pub snapshot: RunnerMetricsSnapshot,
    /// Process memory/CPU sample, if the platform exposes one.
    pub process: Option<ProcessSample>,
}

impl fmt::Display for HealthReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let delta = &self.delta;
        let dispatched = delta.time_events
            + delta.exec_events
            + delta.exec_commands
            + delta.data_events
            + delta.data_commands;

        write!(
            f,
            "Runner health: window={:.1}s loop_util={:.1}% dispatch_util={:.1}% dispatched={dispatched} \
             (time={} exec_evt={} exec_cmd={} data_evt={} data_cmd={}) mean_dispatch={}",
            Duration::from_nanos(delta.elapsed_ns).as_secs_f64(),
            delta.loop_utilization() * 100.0,
            delta.dispatch_utilization() * 100.0,
            delta.time_events,
            delta.exec_events,
            delta.exec_commands,
            delta.data_events,
            delta.data_commands,
            format_nanos(delta.mean_dispatch_ns()),
        )?;

        match self.process {
            Some(process) => write!(
                f,
                " rss={} cpu={:.1}%",
                format_bytes(process.rss_bytes),
                process.cpu_percent
            )?,
            None => f.write_str(" rss=n/a cpu=n/a")?,
        }

        f.write_str(" lanes[depth/hwm/cap/overflow]:")?;
        for lane in LANES {
            let metrics = lane_metrics(&self.snapshot, lane);
            write!(
                f,
                " {lane}={}/{}/{}/{}",
                metrics.queue_depth,
                metrics.queue_high_watermark,
                metrics.queue_capacity,
                metrics.overflows
            )?;
        }
        Ok(())
    }
}

/// Returns lanes whose high-watermark has reached `warn_ratio` of a non-zero capacity.
pub fn lanes_under_pressure(
    snapshot: &RunnerMetricsSnapshot,
    warn_ratio: f64,
) -> impl Iterator<Item = (LiveIngressLane, RunnerChannelMetricsSnapshot)> + '_ {
    LANES.into_iter().filter_map(move |lane| {
        let metrics = lane_metrics(snapshot, lane);
        (metrics.queue_capacity > 0
            && ratio(metrics.queue_high_watermark, metrics.queue_capacity) >= warn_ratio)
            .then_some((lane, metrics))
    })
}

const fn lane_metrics(
    snapshot: &RunnerMetricsSnapshot,
    lane: LiveIngressLane,
) -> RunnerChannelMetricsSnapshot {
    match lane {
        LiveIngressLane::TimeEvents => snapshot.time_events,
        LiveIngressLane::ExecEvents => snapshot.exec_events,
        LiveIngressLane::ExecCommands => snapshot.exec_commands,
        LiveIngressLane::DataEvents => snapshot.data_events,
        LiveIngressLane::DataCommands => snapshot.data_commands,
    }
}

const fn lane_index(lane: LiveIngressLane) -> usize {
    match lane {
        LiveIngressLane::TimeEvents => 0,
        LiveIngressLane::ExecEvents => 1,
        LiveIngressLane::ExecCommands => 2,
        LiveIngressLane::DataEvents => 3,
        LiveIngressLane::DataCommands => 4,
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "queue pressure is an approximate ratio"
)]
fn ratio(value: usize, capacity: usize) -> f64 {
    if capacity == 0 {
        0.0
    } else {
        value as f64 / capacity as f64
    }
}

fn format_nanos(nanos: u64) -> String {
    let duration = Duration::from_nanos(nanos);
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.1}µs", duration.as_secs_f64() * 1e6)
    } else if nanos < 1_000_000_000 {
        format!("{:.2}ms", duration.as_secs_f64() * 1e3)
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn lane(depth: usize, hwm: usize, capacity: usize) -> RunnerChannelMetricsSnapshot {
        RunnerChannelMetricsSnapshot {
            queue_depth: depth,
            queue_high_watermark: hwm,
            queue_capacity: capacity,
            ..RunnerChannelMetricsSnapshot::default()
        }
    }

    #[rstest]
    fn test_lanes_under_pressure_ignores_unbounded_and_quiet_lanes() {
        let snapshot = RunnerMetricsSnapshot {
            time_events: lane(0, 10, 0),
            exec_events: lane(0, 49, 100),
            exec_commands: lane(1, 50, 100),
            data_events: lane(900, 1000, 1000),
            data_commands: lane(0, 0, 100),
            ..RunnerMetricsSnapshot::default()
        };

        let pressured: Vec<LiveIngressLane> = lanes_under_pressure(&snapshot, 0.5)
            .map(|(lane, _)| lane)
            .collect();

        assert_eq!(
            pressured,
            vec![LiveIngressLane::ExecCommands, LiveIngressLane::DataEvents]
        );
    }

    #[rstest]
    fn test_health_report_display_is_single_greppable_line() {
        let before = RunnerMetricsSnapshot::default();
        let after = RunnerMetricsSnapshot {
            time_events: RunnerChannelMetricsSnapshot {
                dispatched: 10,
                ..lane(0, 2, 1024)
            },
            exec_events: RunnerChannelMetricsSnapshot {
                dispatched: 30,
                ..lane(1, 12, 4096)
            },
            data_events: RunnerChannelMetricsSnapshot {
                dispatched: 60,
                ..lane(0, 700, 1000)
            },
            dispatch_busy_ns: 100 * 12_500,
            maintenance_busy_ns: 500_000_000,
            elapsed_ns: 60_000_000_000,
            ..RunnerMetricsSnapshot::default()
        };
        let report = HealthReport {
            delta: RunnerMetricsDelta::from_snapshots(before, after),
            snapshot: after,
            process: Some(ProcessSample {
                rss_bytes: 256 * 1024 * 1024,
                virtual_bytes: 0,
                cpu_percent: 3.25,
            }),
        };

        let line = report.to_string();

        assert!(!line.contains('\n'));
        assert!(line.starts_with("Runner health: window=60.0s"));
        assert!(
            line.contains("dispatched=100 (time=10 exec_evt=30 exec_cmd=0 data_evt=60 data_cmd=0)")
        );
        assert!(line.contains("mean_dispatch=12.5µs"), "{line}");
        assert!(line.contains("rss=256.0 MiB cpu=3.2%"), "{line}");
        assert!(line.contains("data-events=0/700/1000/0"), "{line}");
    }

    #[rstest]
    fn test_health_report_without_process_sample() {
        let report = HealthReport {
            delta: RunnerMetricsDelta::default(),
            snapshot: RunnerMetricsSnapshot::default(),
            process: None,
        };

        let line = report.to_string();

        assert!(line.contains("rss=n/a cpu=n/a"), "{line}");
        assert!(line.contains("mean_dispatch=0ns"), "{line}");
    }

    #[rstest]
    #[case(999, "999ns")]
    #[case(12_500, "12.5µs")]
    #[case(2_500_000, "2.50ms")]
    #[case(3_000_000_000, "3.00s")]
    fn test_format_nanos(#[case] nanos: u64, #[case] expected: &str) {
        assert_eq!(format_nanos(nanos), expected);
    }

    #[rstest]
    fn test_reporter_schedules_and_warns_once_per_lane() {
        let start = dst::time::Instant::now();
        let policy = HealthReportPolicy::with_interval(Duration::from_mins(1));
        let mut reporter = HealthReporter::new(policy, start, &RunnerMetricsSnapshot::default());

        assert!(!reporter.is_due(start));
        assert!(reporter.is_due(start + Duration::from_mins(1)));

        let pressured = RunnerMetricsSnapshot {
            exec_events: lane(0, 80, 100),
            ..RunnerMetricsSnapshot::default()
        };
        let first = start + Duration::from_mins(1);
        reporter.report(first, &pressured);
        assert!(reporter.pressure_warned[lane_index(LiveIngressLane::ExecEvents)]);
        assert!(!reporter.pressure_warned[lane_index(LiveIngressLane::DataEvents)]);
        assert_eq!(reporter.last_snapshot, pressured);
        assert!(!reporter.is_due(first));
        assert!(reporter.is_due(first + Duration::from_mins(1)));
    }
}
