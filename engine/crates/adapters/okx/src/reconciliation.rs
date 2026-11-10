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

//! Venue snapshot sourcing and post-reconnect reconciliation for OKX execution.
//!
//! The execution client needs the same REST-backed snapshot logic in three places: startup
//! mass status, on-demand report generation, and closing the event gap after a WebSocket
//! reconnect. [`OKXReconciliationSource`] owns that logic once, is cheap to clone, and has no
//! dependency on the client itself, so background tasks can use it directly.
//!
//! [`ReconnectReconcilePolicy`] is pure decision logic (what window to request, how often to
//! retry) so it is unit-testable and tunable from configuration.

use std::time::Duration;

use anyhow::Context;
use nautilus_common::messages::ExecutionReport;
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, VenueOrderId},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
};
use tokio::sync::mpsc;

use crate::{
    common::{
        consts::OKX_VENUE,
        enums::OKXInstrumentType,
        parse::{is_okx_spread_symbol, nanos_to_datetime, okx_instrument_type_from_symbol},
    },
    http::client::OKXHttpClient,
    websocket::{
        client::OKXWebSocketQueueFailureGuard,
        dispatch::{ExecutionDispatchError, halt_on_execution_dispatch_error},
    },
};

/// Returns the instrument types an execution client operates on, defaulting to Spot when the
/// configuration leaves the list empty.
pub(crate) fn effective_instrument_types(
    configured: &[OKXInstrumentType],
) -> Vec<OKXInstrumentType> {
    if configured.is_empty() {
        vec![OKXInstrumentType::Spot]
    } else {
        configured.to_vec()
    }
}

/// Returns whether OKX exposes algo (conditional) order endpoints for the instrument type.
pub(crate) fn supports_algo_orders(instrument_type: OKXInstrumentType) -> bool {
    !matches!(
        instrument_type,
        OKXInstrumentType::Option | OKXInstrumentType::Events
    )
}

/// Returns whether the instrument is an OKX Nitro spread.
pub(crate) fn is_spread_instrument(instrument_id: InstrumentId) -> bool {
    is_okx_spread_symbol(instrument_id.symbol.as_str())
}

/// Converts an optional endpoint result into a required reconciliation component.
///
/// A partial snapshot is unsafe: downstream reconciliation can interpret an order omitted by a
/// failed endpoint as closed. Callers therefore propagate this error through mass status instead
/// of returning whatever other endpoints happened to provide.
pub(crate) fn require_complete_reconciliation_component<T>(
    result: anyhow::Result<Vec<T>>,
    component: &str,
) -> anyhow::Result<Vec<T>> {
    result.with_context(|| format!("required reconciliation component unavailable: {component}"))
}

/// Filters applied to an order status snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OrderSnapshotQuery {
    pub instrument_id: Option<InstrumentId>,
    pub open_only: bool,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
}

/// Filters applied to a fill snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FillSnapshotQuery {
    pub instrument_id: Option<InstrumentId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
}

/// REST-backed source of venue execution snapshots for one OKX account.
#[derive(Debug, Clone)]
pub(crate) struct OKXReconciliationSource {
    http_client: OKXHttpClient,
    client_id: ClientId,
    account_id: AccountId,
    instrument_types: Vec<OKXInstrumentType>,
    load_spreads: bool,
    clock: &'static AtomicTime,
}

impl OKXReconciliationSource {
    pub(crate) fn new(
        http_client: OKXHttpClient,
        client_id: ClientId,
        account_id: AccountId,
        instrument_types: &[OKXInstrumentType],
        load_spreads: bool,
        clock: &'static AtomicTime,
    ) -> Self {
        Self {
            http_client,
            client_id,
            account_id,
            instrument_types: effective_instrument_types(instrument_types),
            load_spreads,
            clock,
        }
    }

    /// Fetches order status reports (regular, algo and optionally spread) matching `query`.
    pub(crate) async fn order_status_reports(
        &self,
        query: OrderSnapshotQuery,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut reports = Vec::new();
        let start_dt = nanos_to_datetime(query.start);
        let end_dt = nanos_to_datetime(query.end);

        if let Some(instrument_id) = query.instrument_id {
            let mut fetched = self
                .http_client
                .request_order_status_reports(
                    self.account_id,
                    None,
                    Some(instrument_id),
                    start_dt,
                    end_dt,
                    false,
                    None,
                )
                .await?;
            reports.append(&mut fetched);

            if !is_spread_instrument(instrument_id)
                && supports_algo_orders(okx_instrument_type_from_symbol(
                    instrument_id.symbol.as_str(),
                ))
            {
                // Algo orders are a required part of the snapshot. Returning only regular orders
                // would let downstream reconciliation treat absent conditional orders as closed.
                let algo = require_complete_reconciliation_component(
                    self.http_client
                        .request_algo_order_status_reports(
                            self.account_id,
                            None,
                            Some(instrument_id),
                            None,
                            None,
                            None,
                            None,
                        )
                        .await,
                    &format!("algo order status reports for {instrument_id}"),
                )?;
                reports.extend(algo);
            }
        } else {
            for inst_type in &self.instrument_types {
                let mut fetched = self
                    .http_client
                    .request_order_status_reports(
                        self.account_id,
                        Some(*inst_type),
                        None,
                        start_dt,
                        end_dt,
                        false,
                        None,
                    )
                    .await?;
                reports.append(&mut fetched);

                if supports_algo_orders(*inst_type) {
                    let algo = require_complete_reconciliation_component(
                        self.http_client
                            .request_algo_order_status_reports(
                                self.account_id,
                                Some(*inst_type),
                                None,
                                None,
                                None,
                                None,
                                None,
                            )
                            .await,
                        &format!("algo order status reports for {inst_type:?}"),
                    )?;
                    reports.extend(algo);
                }
            }

            if self.load_spreads {
                let spreads = require_complete_reconciliation_component(
                    self.http_client
                        .request_order_status_reports(
                            self.account_id,
                            None,
                            None,
                            start_dt,
                            end_dt,
                            false,
                            None,
                        )
                        .await,
                    "spread order status reports",
                )?;
                reports.extend(spreads);
            }
        }

        if query.open_only {
            reports.retain(|r| r.order_status.is_open());
        }
        if let Some(start) = query.start {
            reports.retain(|r| r.ts_last >= start);
        }
        if let Some(end) = query.end {
            reports.retain(|r| r.ts_last <= end);
        }

        Ok(reports)
    }

    /// Fetches fill reports matching `query`.
    pub(crate) async fn fill_reports(
        &self,
        query: FillSnapshotQuery,
    ) -> anyhow::Result<Vec<FillReport>> {
        let start_dt = nanos_to_datetime(query.start);
        let end_dt = nanos_to_datetime(query.end);
        let mut reports = Vec::new();

        if let Some(instrument_id) = query.instrument_id {
            let mut fetched = self
                .http_client
                .request_fill_reports(
                    self.account_id,
                    None,
                    Some(instrument_id),
                    start_dt,
                    end_dt,
                    None,
                )
                .await?;
            reports.append(&mut fetched);
        } else {
            for inst_type in &self.instrument_types {
                let mut fetched = self
                    .http_client
                    .request_fill_reports(
                        self.account_id,
                        Some(*inst_type),
                        None,
                        start_dt,
                        end_dt,
                        None,
                    )
                    .await?;
                reports.append(&mut fetched);
            }

            if self.load_spreads {
                let spreads = require_complete_reconciliation_component(
                    self.http_client
                        .request_fill_reports(self.account_id, None, None, start_dt, end_dt, None)
                        .await,
                    "spread fill reports",
                )?;
                reports.extend(spreads);
            }
        }

        if let Some(venue_order_id) = query.venue_order_id {
            reports.retain(|report| report.venue_order_id.as_str() == venue_order_id.as_str());
        }

        Ok(reports)
    }

    /// Fetches derivative and spot-margin position reports, optionally for one instrument.
    pub(crate) async fn position_status_reports(
        &self,
        instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let mut reports = Vec::new();

        // Derivative positions (SWAP/FUTURES/OPTION) come from /api/v5/account/positions, which
        // does not support Spot or Margin; those are sourced from the balance sheet below.
        if let Some(instrument_id) = instrument_id {
            if is_spread_instrument(instrument_id) {
                return Ok(reports);
            }

            let inst_type = okx_instrument_type_from_symbol(instrument_id.symbol.as_str());
            if inst_type != OKXInstrumentType::Spot && inst_type != OKXInstrumentType::Margin {
                let mut fetched = self
                    .http_client
                    .request_position_status_reports(self.account_id, None, Some(instrument_id))
                    .await?;
                reports.append(&mut fetched);
            }
        } else {
            for inst_type in &self.instrument_types {
                if *inst_type == OKXInstrumentType::Spot || *inst_type == OKXInstrumentType::Margin
                {
                    continue;
                }
                let mut fetched = self
                    .http_client
                    .request_position_status_reports(self.account_id, Some(*inst_type), None)
                    .await?;
                reports.append(&mut fetched);
            }
        }

        // Spot margin positions appear as balance sheet items (liab/spotInUseAmt fields)
        let mut margin_reports = self
            .http_client
            .request_spot_margin_position_reports(self.account_id)
            .await?;

        if let Some(instrument_id) = instrument_id {
            margin_reports.retain(|report| report.instrument_id == instrument_id);
        }

        reports.append(&mut margin_reports);

        Ok(reports)
    }

    /// Builds a complete [`ExecutionMassStatus`] for orders, fills and positions with activity
    /// at or after `start` (all history the venue offers when `None`).
    ///
    /// The three components are fetched concurrently and the snapshot is rejected as a whole if
    /// any of them fails, because a partial snapshot could be reconciled as closures.
    pub(crate) async fn mass_status(
        &self,
        start: Option<UnixNanos>,
    ) -> anyhow::Result<ExecutionMassStatus> {
        let ts_now = self.clock.get_time_ns();

        let (order_reports, fill_reports, position_reports) = tokio::try_join!(
            self.order_status_reports(OrderSnapshotQuery {
                start,
                ..OrderSnapshotQuery::default()
            }),
            self.fill_reports(FillSnapshotQuery {
                start,
                ..FillSnapshotQuery::default()
            }),
            self.position_status_reports(None),
        )?;

        log::info!(
            "Venue snapshot: {} OrderStatusReports, {} FillReports, {} PositionReports (start={start:?})",
            order_reports.len(),
            fill_reports.len(),
            position_reports.len(),
        );

        let mut mass_status =
            ExecutionMassStatus::new(self.client_id, self.account_id, *OKX_VENUE, ts_now, None);
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(fill_reports);
        mass_status.add_position_reports(position_reports);

        Ok(mass_status)
    }
}

/// Timing policy for closing an execution event gap after a WebSocket reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconnectReconcilePolicy {
    /// Safety margin subtracted from the last observed message time.
    pub lookback_margin: Duration,
    /// Gaps longer than this request an unbounded snapshot instead of a windowed one, because
    /// the window start would no longer be trustworthy.
    pub full_snapshot_threshold: Duration,
    /// Maximum snapshot attempts before the gap is declared unrecoverable.
    pub max_attempts: u32,
    /// Initial delay between attempts (doubles per attempt).
    pub retry_delay: Duration,
}

impl Default for ReconnectReconcilePolicy {
    fn default() -> Self {
        Self {
            lookback_margin: Duration::from_secs(60),
            full_snapshot_threshold: Duration::from_secs(6 * 60 * 60),
            max_attempts: 4,
            retry_delay: Duration::from_secs(1),
        }
    }
}

impl ReconnectReconcilePolicy {
    /// Returns the snapshot start for a gap whose last observed message was at
    /// `last_message_at`, evaluated at `now`.
    ///
    /// Returns `None` (full snapshot) when the gap exceeds the full-snapshot threshold.
    #[must_use]
    pub(crate) fn snapshot_start(
        &self,
        last_message_at: UnixNanos,
        now: UnixNanos,
    ) -> Option<UnixNanos> {
        let gap = Duration::from_nanos(now.as_u64().saturating_sub(last_message_at.as_u64()));
        if gap > self.full_snapshot_threshold {
            return None;
        }
        let margin_ns = u64::try_from(self.lookback_margin.as_nanos()).unwrap_or(u64::MAX);
        Some(UnixNanos::from(
            last_message_at.as_u64().saturating_sub(margin_ns),
        ))
    }

    /// Returns the delay before attempt number `attempt` (1-based) is retried, or `None` when
    /// the attempt budget is exhausted.
    #[must_use]
    pub(crate) fn retry_delay_after(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        Some(self.retry_delay.saturating_mul(factor))
    }
}

/// Notification that an execution WebSocket reconnected and may have missed venue events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconnectNotice {
    /// Which consumer observed the reconnect (for logging).
    pub consumer: &'static str,
    /// Time of the last message observed before the reconnect.
    pub last_message_at: UnixNanos,
    /// Time the reconnect was observed.
    pub detected_at: UnixNanos,
}

/// Bounded queue of reconnect notices awaiting reconciliation. Several reconnects while a
/// snapshot is in flight are legitimate; more than this indicates a flapping transport.
pub(crate) const RECONNECT_NOTICE_CAPACITY: usize = 8;

/// Consumes reconnect notices, fetches the corresponding venue snapshot and feeds it to the
/// execution engine as an [`ExecutionReport::MassStatus`].
///
/// Exits when the notice channel closes, when the shared failure domain is already latched, or
/// after latching `ReconnectGap` because a snapshot could not be obtained.
pub(crate) async fn run_reconnect_reconciler(
    mut notices: mpsc::Receiver<ReconnectNotice>,
    source: OKXReconciliationSource,
    emitter: ExecutionEventEmitter,
    queue_failure: OKXWebSocketQueueFailureGuard,
    policy: ReconnectReconcilePolicy,
    clock: &'static AtomicTime,
) {
    while let Some(notice) = notices.recv().await {
        if queue_failure.is_failed() {
            break;
        }

        let now = clock.get_time_ns();
        let start = policy.snapshot_start(notice.last_message_at, now);
        log::warn!(
            "OKX {} execution WebSocket reconnected; reconciling venue events since {} (detected at {}, snapshot start {:?})",
            notice.consumer,
            notice.last_message_at,
            notice.detected_at,
            start,
        );

        match Box::pin(fetch_snapshot_with_retries(&source, start, &policy)).await {
            Ok(mass_status) => {
                log::info!(
                    "OKX post-reconnect snapshot obtained for {} consumer; handing to execution engine",
                    notice.consumer
                );
                emitter.send_execution_report(ExecutionReport::MassStatus(Box::new(mass_status)));
            }
            Err(error) => {
                log::error!(
                    "OKX post-reconnect reconciliation failed after {} attempt(s): {error:#}",
                    policy.max_attempts
                );
                halt_on_execution_dispatch_error(
                    &queue_failure,
                    &ExecutionDispatchError::ReconnectGap,
                    notice.consumer,
                );
                break;
            }
        }
    }
}

async fn fetch_snapshot_with_retries(
    source: &OKXReconciliationSource,
    start: Option<UnixNanos>,
    policy: &ReconnectReconcilePolicy,
) -> anyhow::Result<ExecutionMassStatus> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match Box::pin(source.mass_status(start)).await {
            Ok(mass_status) => return Ok(mass_status),
            Err(error) => match policy.retry_delay_after(attempt) {
                Some(delay) => {
                    log::warn!(
                        "OKX post-reconnect snapshot attempt {attempt}/{} failed; retrying in {delay:?}: {error:#}",
                        policy.max_attempts
                    );
                    tokio::time::sleep(delay).await;
                }
                None => return Err(error),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const SEC: u64 = 1_000_000_000;

    #[rstest]
    fn test_snapshot_start_subtracts_margin() {
        let policy = ReconnectReconcilePolicy::default();
        let last = UnixNanos::from(1_000 * SEC);
        let now = UnixNanos::from(1_010 * SEC);

        assert_eq!(
            policy.snapshot_start(last, now),
            Some(UnixNanos::from(940 * SEC))
        );
    }

    #[rstest]
    fn test_snapshot_start_saturates_at_epoch() {
        let policy = ReconnectReconcilePolicy::default();
        let last = UnixNanos::from(10 * SEC);
        let now = UnixNanos::from(20 * SEC);

        assert_eq!(policy.snapshot_start(last, now), Some(UnixNanos::from(0)));
    }

    #[rstest]
    fn test_snapshot_start_falls_back_to_full_snapshot_for_long_gaps() {
        let policy = ReconnectReconcilePolicy {
            full_snapshot_threshold: Duration::from_secs(60),
            ..ReconnectReconcilePolicy::default()
        };
        let last = UnixNanos::from(1_000 * SEC);

        assert!(
            policy
                .snapshot_start(last, UnixNanos::from(1_060 * SEC))
                .is_some()
        );
        assert!(
            policy
                .snapshot_start(last, UnixNanos::from(1_061 * SEC))
                .is_none()
        );
    }

    #[rstest]
    fn test_snapshot_start_handles_clock_going_backwards() {
        let policy = ReconnectReconcilePolicy::default();
        let last = UnixNanos::from(1_000 * SEC);
        let now = UnixNanos::from(900 * SEC);

        assert_eq!(
            policy.snapshot_start(last, now),
            Some(UnixNanos::from(940 * SEC))
        );
    }

    #[rstest]
    fn test_retry_delay_doubles_then_exhausts() {
        let policy = ReconnectReconcilePolicy {
            max_attempts: 4,
            retry_delay: Duration::from_secs(1),
            ..ReconnectReconcilePolicy::default()
        };

        assert_eq!(policy.retry_delay_after(1), Some(Duration::from_secs(1)));
        assert_eq!(policy.retry_delay_after(2), Some(Duration::from_secs(2)));
        assert_eq!(policy.retry_delay_after(3), Some(Duration::from_secs(4)));
        assert_eq!(policy.retry_delay_after(4), None);
        assert_eq!(policy.retry_delay_after(5), None);
    }

    #[rstest]
    fn test_single_attempt_policy_never_retries() {
        let policy = ReconnectReconcilePolicy {
            max_attempts: 1,
            ..ReconnectReconcilePolicy::default()
        };
        assert_eq!(policy.retry_delay_after(1), None);
    }
}
