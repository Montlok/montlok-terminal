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

//! WebSocket client implementation for the OKX v5 API.
//!
//! This module provides real-time streaming connectivity to OKX WebSocket endpoints,
//! supporting:
//!
//! - Market data streaming (order books, trades, tickers, bars).
//! - Private data streaming (account updates, positions, orders).
//! - Order management (place, cancel, amend) via WebSocket.
//! - Authentication and automatic reconnection.
//! - Channel subscription management.

use std::sync::Arc;

use nautilus_common::live::{LiveIngressControl, LiveIngressLane};

pub mod client;
pub mod dispatch;
pub mod enums;
pub mod error;
pub mod handler;
pub mod messages;
pub mod parse;
pub(crate) mod replay_cache;
pub mod subscription;

// Re-exports
pub use crate::websocket::client::{
    OKX_WS_COMMAND_QUEUE_CAPACITY, OKX_WS_INGRESS_BYTE_BUDGET, OKX_WS_MAX_BATCH_SIZE,
    OKX_WS_MAX_FRAME_SIZE, OKX_WS_MAX_MESSAGE_SIZE, OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE,
    OKX_WS_OUTPUT_QUEUE_CAPACITY, OKX_WS_PENDING_OPERATION_CAPACITY,
    OKX_WS_PENDING_OPERATION_TIMEOUT_SECS, OKX_WS_RAW_QUEUE_CAPACITY, OKX_WS_WRITER_QUEUE_CAPACITY,
    OKXWebSocketClient, OKXWebSocketQueue, OKXWebSocketQueueFailureCallback,
    OKXWebSocketQueueFailureKind,
};
pub use crate::websocket::replay_cache::OKX_EXECUTION_REPLAY_CACHE_CAPACITY;

pub(crate) fn live_ingress_failure_callback(
    control: LiveIngressControl,
    lane: LiveIngressLane,
) -> OKXWebSocketQueueFailureCallback {
    Arc::new(move |_queue, kind| match kind {
        OKXWebSocketQueueFailureKind::Full
        | OKXWebSocketQueueFailureKind::Oversized
        | OKXWebSocketQueueFailureKind::ByteBudgetExhausted
        | OKXWebSocketQueueFailureKind::PendingCapacityExhausted => control.fail_full(lane),
        OKXWebSocketQueueFailureKind::Closed
        | OKXWebSocketQueueFailureKind::Conversion
        | OKXWebSocketQueueFailureKind::PendingExpired
        | OKXWebSocketQueueFailureKind::PendingAmbiguous
        | OKXWebSocketQueueFailureKind::PendingStaleConnection => {
            control.fail_closed(lane);
        }
        OKXWebSocketQueueFailureKind::ReconnectGap
        | OKXWebSocketQueueFailureKind::ProtectionLost
        | OKXWebSocketQueueFailureKind::ConsumerStopped => control.fail_integrity(lane),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_common::live::{
        LiveIngressFailure, LiveIngressFailureKind, LiveIngressLaneMetrics,
    };

    use super::*;

    #[test]
    fn test_queue_full_callback_joins_data_ingress_fail_stop_domain() {
        let control = LiveIngressControl::detached();
        let callback = live_ingress_failure_callback(control.clone(), LiveIngressLane::DataEvents);

        callback(OKXWebSocketQueue::Raw, OKXWebSocketQueueFailureKind::Full);

        assert_eq!(
            control.failure(),
            Some(LiveIngressFailure {
                lane: LiveIngressLane::DataEvents,
                kind: LiveIngressFailureKind::Full,
            })
        );
        assert_eq!(
            control.lane_metrics(LiveIngressLane::DataEvents),
            LiveIngressLaneMetrics {
                overflows: 1,
                ..LiveIngressLaneMetrics::default()
            }
        );
    }

    #[test]
    fn test_queue_closed_callback_joins_execution_ingress_fail_stop_domain() {
        let control = LiveIngressControl::detached();
        let callback = live_ingress_failure_callback(control.clone(), LiveIngressLane::ExecEvents);

        callback(
            OKXWebSocketQueue::Writer,
            OKXWebSocketQueueFailureKind::Closed,
        );

        assert_eq!(
            control.failure(),
            Some(LiveIngressFailure {
                lane: LiveIngressLane::ExecEvents,
                kind: LiveIngressFailureKind::Closed,
            })
        );
        assert_eq!(control.lane_metrics(LiveIngressLane::ExecEvents).closed, 1);
    }
}
