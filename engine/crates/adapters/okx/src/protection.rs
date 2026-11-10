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

//! Pure decision logic for exchange-side order protection (OKX Cancel All After).
//!
//! OKX's Cancel All After (CAA) is a one-shot venue-side dead man's switch: the client arms a
//! countdown and must renew it before it expires, otherwise the venue cancels every order
//! carrying the configured tag. The policy below decides *when* to renew and *how* to react to
//! renewal failures without touching I/O, so it can be unit tested exhaustively and tuned via
//! configuration rather than code changes.
//!
//! The key property: a single failed renewal must never halt the node while the venue-side
//! countdown still has enough headroom for another attempt. Halting is reserved for the moment
//! at which protection can no longer be proven to remain armed.

use std::time::{Duration, Instant};

/// Venue rate limit for the Cancel All After endpoint (one request per second).
pub const OKX_CANCEL_ALL_AFTER_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

/// Number of consecutive renewal failures after which every failure is logged at warn level.
pub const OKX_CANCEL_ALL_AFTER_DEGRADED_FAILURES: u32 = 2;

/// Timing policy for keeping a Cancel All After countdown armed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelAllAfterPolicy {
    /// Venue-side countdown armed on each renewal.
    pub countdown: Duration,
    /// Cadence at which the countdown is renewed while healthy.
    pub heartbeat: Duration,
    /// Time budget for a single renewal request.
    pub request_timeout: Duration,
    /// Minimum spacing between renewal attempts after a failure.
    pub retry_interval: Duration,
}

impl CancelAllAfterPolicy {
    /// Builds a policy from the configured countdown and heartbeat (both in seconds).
    ///
    /// The request timeout is a third of the countdown clamped to 1-5 seconds so a hung
    /// request can never consume the whole protection window.
    #[must_use]
    pub fn from_secs(countdown_secs: u64, heartbeat_secs: u64) -> Self {
        Self {
            countdown: Duration::from_secs(countdown_secs),
            heartbeat: Duration::from_secs(heartbeat_secs),
            request_timeout: Duration::from_secs((countdown_secs / 3).clamp(1, 5)),
            retry_interval: OKX_CANCEL_ALL_AFTER_MIN_REQUEST_INTERVAL,
        }
    }

    /// Returns the instant at which the venue fires the countdown armed at `armed_at`.
    #[must_use]
    pub fn venue_deadline(&self, armed_at: Instant) -> Instant {
        armed_at + self.countdown
    }

    /// Decides what to do after a renewal attempt that failed at `now`.
    ///
    /// `armed_at` is the instant of the last successful renewal. Another attempt is only
    /// worthwhile when it can start after the retry interval and still complete (including its
    /// timeout) before the venue-side countdown fires; otherwise protection is lost.
    #[must_use]
    pub fn after_failure(&self, armed_at: Instant, now: Instant) -> RenewalDecision {
        let remaining = self.venue_deadline(armed_at).saturating_duration_since(now);
        let needed = self.retry_interval + self.request_timeout;
        if remaining > needed {
            RenewalDecision::Retry {
                delay: self.retry_interval,
                remaining,
            }
        } else {
            RenewalDecision::Halt { remaining }
        }
    }

    /// Validates that the timing leaves room for at least one retry after a failed renewal.
    ///
    /// # Errors
    ///
    /// Returns a human-readable reason when the policy cannot tolerate a single failure.
    pub fn validate(&self) -> Result<(), String> {
        if self.heartbeat.is_zero() {
            return Err("Cancel All After heartbeat must be greater than zero".to_string());
        }
        if self.heartbeat >= self.countdown {
            return Err(
                "Cancel All After heartbeat must be shorter than its countdown".to_string(),
            );
        }
        let worst_case =
            self.heartbeat + self.request_timeout + self.retry_interval + self.request_timeout;
        if worst_case >= self.countdown {
            return Err(format!(
                "Cancel All After heartbeat {}s leaves no room to retry a failed renewal before the \
                 {}s countdown fires (needs heartbeat + 2 x request timeout {}s + retry interval {}s < countdown)",
                self.heartbeat.as_secs(),
                self.countdown.as_secs(),
                self.request_timeout.as_secs(),
                self.retry_interval.as_secs(),
            ));
        }
        Ok(())
    }
}

/// Outcome of a failed renewal attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewalDecision {
    /// Try again after `delay`; the venue countdown still has `remaining` headroom.
    Retry {
        delay: Duration,
        remaining: Duration,
    },
    /// No further attempt can land before the venue fires; protection must be treated as lost.
    Halt { remaining: Duration },
}

/// Tracks consecutive renewal failures so degraded periods and recoveries are logged once
/// each rather than on every tick.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RenewalHealth {
    consecutive_failures: u32,
}

impl RenewalHealth {
    /// Records a failure and returns the updated consecutive failure count.
    pub fn record_failure(&mut self) -> u32 {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.consecutive_failures
    }

    /// Records a success and returns how many failures preceded it (zero when already healthy).
    pub fn record_success(&mut self) -> u32 {
        std::mem::take(&mut self.consecutive_failures)
    }

    /// Returns the current consecutive failure count.
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Returns whether renewals have failed often enough to be considered degraded.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        self.consecutive_failures >= OKX_CANCEL_ALL_AFTER_DEGRADED_FAILURES
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(10, 3)]
    #[case(30, 5)]
    #[case(60, 5)]
    #[case(120, 5)]
    fn test_request_timeout_is_clamped_third_of_countdown(
        #[case] countdown: u64,
        #[case] expected_timeout: u64,
    ) {
        let policy = CancelAllAfterPolicy::from_secs(countdown, 1);
        assert_eq!(
            policy.request_timeout,
            Duration::from_secs(expected_timeout)
        );
    }

    #[rstest]
    fn test_single_failure_with_headroom_retries() {
        let policy = CancelAllAfterPolicy::from_secs(60, 5);
        let armed_at = Instant::now();
        let now = armed_at + Duration::from_secs(5);

        match policy.after_failure(armed_at, now) {
            RenewalDecision::Retry { delay, remaining } => {
                assert_eq!(delay, OKX_CANCEL_ALL_AFTER_MIN_REQUEST_INTERVAL);
                assert_eq!(remaining, Duration::from_secs(55));
            }
            other => panic!("expected retry, got {other:?}"),
        }
    }

    #[rstest]
    fn test_failure_without_headroom_halts() {
        let policy = CancelAllAfterPolicy::from_secs(10, 5);
        let armed_at = Instant::now();
        // request_timeout=3s, retry_interval=1s => needs > 4s of headroom
        let now = armed_at + Duration::from_secs(6);

        assert_eq!(
            policy.after_failure(armed_at, now),
            RenewalDecision::Halt {
                remaining: Duration::from_secs(4),
            }
        );
    }

    #[rstest]
    fn test_failure_past_deadline_halts_with_zero_remaining() {
        let policy = CancelAllAfterPolicy::from_secs(10, 5);
        let armed_at = Instant::now();
        let now = armed_at + Duration::from_secs(30);

        assert_eq!(
            policy.after_failure(armed_at, now),
            RenewalDecision::Halt {
                remaining: Duration::ZERO,
            }
        );
    }

    #[rstest]
    fn test_retry_boundary_is_strict() {
        let policy = CancelAllAfterPolicy::from_secs(30, 5);
        let armed_at = Instant::now();
        // request_timeout=5s, retry_interval=1s => needs remaining > 6s
        let exactly_needed = armed_at + Duration::from_secs(24);
        let just_enough = armed_at + Duration::from_secs(23);

        assert!(matches!(
            policy.after_failure(armed_at, exactly_needed),
            RenewalDecision::Halt { .. }
        ));
        assert!(matches!(
            policy.after_failure(armed_at, just_enough),
            RenewalDecision::Retry { .. }
        ));
    }

    #[rstest]
    #[case(10, 5, false)]
    #[case(10, 1, true)]
    #[case(10, 2, true)]
    #[case(30, 5, true)]
    #[case(60, 5, true)]
    #[case(60, 30, true)]
    #[case(60, 59, false)]
    #[case(120, 100, true)]
    #[case(120, 110, false)]
    #[case(20, 0, false)]
    fn test_validate(#[case] countdown: u64, #[case] heartbeat: u64, #[case] valid: bool) {
        let policy = CancelAllAfterPolicy::from_secs(countdown, heartbeat);
        assert_eq!(policy.validate().is_ok(), valid, "{policy:?}");
    }

    #[rstest]
    fn test_health_tracks_degradation_and_recovery() {
        let mut health = RenewalHealth::default();
        assert!(!health.is_degraded());

        assert_eq!(health.record_failure(), 1);
        assert!(!health.is_degraded());
        assert_eq!(health.record_failure(), 2);
        assert!(health.is_degraded());

        assert_eq!(health.record_success(), 2);
        assert_eq!(health.consecutive_failures(), 0);
        assert!(!health.is_degraded());
        assert_eq!(health.record_success(), 0);
    }
}
