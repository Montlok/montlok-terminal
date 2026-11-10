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

//! Provides the WebSocket client integration for the [OKX](https://okx.com) WebSocket API.
//!
//! The [`OKXWebSocketClient`] ties together several recurring patterns:
//! - Heartbeats use text `ping`/`pong`, responding to both text and control-frame pings.
//! - Authentication re-runs on reconnect before resubscribing and skips private channels when
//!   credentials are unavailable.
//! - Subscriptions cache instrument type/family/ID groupings so reconnects rebuild the same set of
//!   channels while respecting the authentication guard described above.

use std::{
    fmt::Debug,
    num::NonZeroU32,
    sync::{
        Arc, LazyLock, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures_util::Stream;
use nautilus_common::live::get_runtime;
use nautilus_core::{
    AtomicMap,
    consts::NAUTILUS_USER_AGENT,
    env::{get_env_var, get_or_env_var},
    string::secret::REDACTED,
};
use nautilus_model::{
    data::BarType,
    enums::{OrderSide, OrderType, PositionSide, TimeInForce, TriggerType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_network::{
    http::USER_AGENT,
    mode::ConnectionMode,
    ratelimiter::quota::Quota,
    websocket::{
        AUTHENTICATION_TIMEOUT_SECS, AuthTracker, BoundedWriterConfig, PingHandler,
        SubscriptionState, TEXT_PING, TransportBackend, WebSocketClient, WebSocketConfig,
        WriterQueueFailure,
    },
};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Error;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use super::{
    enums::{OKXWsChannel, OKXWsOperation},
    error::OKXWsError,
    handler::{HandlerCommand, OKXWsFeedHandler},
    messages::{
        OKXAuthentication, OKXAuthenticationArg, OKXSubscriptionArg, OKXWsMessage, OKXWsRequest,
        WsAmendOrderParamsBuilder, WsAttachAlgoOrdParams, WsCancelOrderParamsBuilder,
        WsMassCancelParams, WsPostAlgoOrderParamsBuilder, WsPostOrderParamsBuilder,
    },
    subscription::topic_from_subscription_arg,
};
use crate::common::{
    consts::{
        OKX_NAUTILUS_BROKER_ID, OKX_SUPPORTED_ORDER_TYPES, OKX_SUPPORTED_TIME_IN_FORCE,
        OKX_WS_TOPIC_DELIMITER, select_book_channel,
    },
    credential::Credential,
    enums::{
        OKXBookChannel, OKXEnvironment, OKXGreeksType, OKXInstrumentType, OKXOrderStatus,
        OKXOrderType, OKXPositionSide, OKXRegion, OKXTargetCurrency, OKXTradeMode, OKXTriggerType,
        OKXVipLevel, conditional_order_to_algo_type, is_conditional_order,
    },
    parse::{
        bar_spec_as_okx_channel, okx_instrument_type, okx_instrument_type_from_symbol,
        parse_base_quote_from_symbol,
    },
    urls::get_ws_base_url_public,
};

/// Default OKX WebSocket connection rate limit: 3 requests per second.
///
/// This applies to establishing WebSocket connections, not to subscribe/unsubscribe operations.
pub static OKX_WS_CONNECTION_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(3).expect("non-zero")).expect("valid constant")
});

/// Hard capacity of the raw venue-frame queue.
pub const OKX_WS_RAW_QUEUE_CAPACITY: usize = 65_536;

/// Hard capacity of the parsed consumer-output queue.
pub const OKX_WS_OUTPUT_QUEUE_CAPACITY: usize = 65_536;

/// Hard capacity of the command queue feeding the OKX handler.
pub const OKX_WS_COMMAND_QUEUE_CAPACITY: usize = 4_096;

/// Hard capacity of the network writer queue and reconnect buffer used by OKX.
pub const OKX_WS_WRITER_QUEUE_CAPACITY: usize = 4_096;

/// Maximum payload of one OKX inbound WebSocket frame (1 MiB).
pub const OKX_WS_MAX_FRAME_SIZE: usize = 1024 * 1024;

/// Maximum reassembled OKX inbound WebSocket message size (4 MiB).
pub const OKX_WS_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Shared weighted serialized-byte budget spanning the raw and parsed OKX ingress queues (64 MiB).
///
/// This bounds queued venue payloads, not total process RSS. JSON parsing has representation
/// overhead; the conservative multiplier below reserves additional headroom while the parsed
/// value and original payload are both resident.
pub const OKX_WS_INGRESS_BYTE_BUDGET: usize = 64 * 1024 * 1024;

/// Conservative weight applied to raw payload bytes while parsed JSON is resident.
pub const OKX_WS_INGRESS_MEMORY_WEIGHT: usize = 16;

/// Maximum serialized outbound OKX WebSocket command (16 KiB).
///
/// Together with [`OKX_WS_WRITER_QUEUE_CAPACITY`], this proves a 64 MiB upper bound on serialized
/// application payload bytes admitted to the network writer/reconnect buffer. Transport and queue
/// bookkeeping add overhead but cannot introduce an unbounded payload path.
pub const OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE: usize = 16 * 1024;

/// Maximum order operations accepted in one OKX WebSocket batch.
pub const OKX_WS_MAX_BATCH_SIZE: usize = 20;

/// Shared hard limit across outstanding order, cancel, and amend requests.
pub const OKX_WS_PENDING_OPERATION_CAPACITY: usize = 4_096;

/// Maximum time an acknowledged-write order operation may remain unresolved.
pub const OKX_WS_PENDING_OPERATION_TIMEOUT_SECS: u64 = 30;

const OKX_WS_PENDING_WATCHDOG_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(crate) struct OKXBudgetedMessage<T> {
    pub(crate) message: T,
    pub(crate) permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<T> OKXBudgetedMessage<T> {
    fn budgeted(message: T, permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self {
            message,
            permit: Some(permit),
        }
    }

    pub(crate) const fn unbudgeted(message: T) -> Self {
        Self {
            message,
            permit: None,
        }
    }
}

/// Identifies an internal OKX WebSocket queue at the failure callback boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OKXWebSocketQueue {
    /// Raw venue frames waiting for parsing.
    Raw,
    /// Commands waiting for handler execution.
    Command,
    /// Parsed messages waiting for the adapter consumer.
    Output,
    /// Serialized messages waiting for the network writer.
    Writer,
    /// Order operations written or queued for the venue but not yet resolved.
    PendingOperations,
    /// Exchange-side protection which must remain armed while execution is active.
    VenueProtection,
}

/// Terminal reason reported for an OKX WebSocket queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OKXWebSocketQueueFailureKind {
    /// The queue reached its fixed capacity.
    Full,
    /// The queue receiver was closed.
    Closed,
    /// A transport message could not be converted at the compatibility boundary.
    Conversion,
    /// One message exceeded its explicit serialized byte limit.
    Oversized,
    /// The shared weighted ingress byte budget was exhausted.
    ByteBudgetExhausted,
    /// The shared outstanding order-operation capacity was exhausted.
    PendingCapacityExhausted,
    /// An outstanding order operation exceeded its resolution deadline.
    PendingExpired,
    /// An outstanding operation became ambiguous after an asynchronous send failure.
    PendingAmbiguous,
    /// An outstanding operation belongs to an earlier transport connection epoch.
    PendingStaleConnection,
    /// The execution stream reconnected after a period in which venue events may be missing.
    ReconnectGap,
    /// Exchange-side order protection could no longer be renewed or proven active.
    ProtectionLost,
    /// An execution consumer task stopped (panic or unexpected stream end) while the client was
    /// still expected to observe venue events.
    ConsumerStopped,
}

/// Callback invoked for the first terminal internal queue failure.
pub type OKXWebSocketQueueFailureCallback =
    Arc<dyn Fn(OKXWebSocketQueue, OKXWebSocketQueueFailureKind) + Send + Sync>;

const OKX_WS_TERMINAL_ADMISSION_BIT: usize = 1 << (usize::BITS - 1);

/// Linearization token for an OKX trading command admitted before terminal queue failure.
///
/// Acquiring a token and latching terminal failure both update the same atomic word. Therefore a
/// command queued after the terminal bit is visible cannot enter the transport send path. A token
/// acquired first represents a command whose admission linearized before the failure.
pub(crate) struct OKXWebSocketTradingAdmission {
    state: Arc<AtomicUsize>,
}

impl Drop for OKXWebSocketTradingAdmission {
    fn drop(&mut self) {
        let previous = self.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(
            previous & !OKX_WS_TERMINAL_ADMISSION_BIT,
            0,
            "trading admission counter underflow"
        );
    }
}

#[derive(Clone)]
pub(crate) struct OKXWebSocketQueueFailureGuard {
    trading_admission: Arc<AtomicUsize>,
    state: Arc<Mutex<OKXWebSocketQueueFailureState>>,
    signal: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
}

struct OKXWebSocketQueueFailureState {
    failure: Option<(OKXWebSocketQueue, OKXWebSocketQueueFailureKind)>,
    callback: OKXWebSocketQueueFailureCallback,
    subscription_failure: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

impl std::fmt::Debug for OKXWebSocketQueueFailureGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(OKXWebSocketQueueFailureGuard))
            .field("failed", &self.is_failed())
            .field("failure", &self.failure())
            .finish_non_exhaustive()
    }
}

impl OKXWebSocketQueueFailureGuard {
    pub(crate) fn new(signal: Arc<AtomicBool>, shutdown_notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            trading_admission: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(Mutex::new(OKXWebSocketQueueFailureState {
                failure: None,
                callback: Arc::new(|_, _| {}),
                subscription_failure: None,
            })),
            signal,
            shutdown_notify,
        }
    }

    fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "registration takes ownership of the caller's callback handle"
    )]
    fn set_callback(&self, callback: OKXWebSocketQueueFailureCallback) {
        let failure = {
            let mut state = Self::lock_unpoisoned(&self.state);
            state.callback = Arc::clone(&callback);
            state.failure
        };
        if let Some((queue, kind)) = failure {
            callback(queue, kind);
        }
    }

    /// Opt-in notification for an explicit Python data owner. Native runner
    /// clients leave this unset, retaining their existing subscription policy.
    #[cfg(any(feature = "python", test))]
    pub(crate) fn set_subscription_failure_callback(
        &self,
        callback: Arc<dyn Fn(String) + Send + Sync>,
    ) {
        Self::lock_unpoisoned(&self.state).subscription_failure = Some(callback);
    }

    pub(crate) fn report_subscription_failure(&self, reason: String) {
        let callback = Self::lock_unpoisoned(&self.state)
            .subscription_failure
            .clone();
        if let Some(callback) = callback {
            callback(reason);
        }
    }

    #[cfg(test)]
    pub(crate) fn state_is_subscription_default_for_test(&self) -> bool {
        Self::lock_unpoisoned(&self.state)
            .subscription_failure
            .is_none()
    }

    pub(crate) fn failure(&self) -> Option<(OKXWebSocketQueue, OKXWebSocketQueueFailureKind)> {
        Self::lock_unpoisoned(&self.state).failure
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.trading_admission.load(Ordering::Acquire) & OKX_WS_TERMINAL_ADMISSION_BIT != 0
    }

    /// Atomically admits one trading command unless terminal queue failure already won.
    pub(crate) fn try_acquire_trading_admission(&self) -> Option<OKXWebSocketTradingAdmission> {
        let mut current = self.trading_admission.load(Ordering::Acquire);
        loop {
            if current & OKX_WS_TERMINAL_ADMISSION_BIT != 0 {
                return None;
            }
            let active = current & !OKX_WS_TERMINAL_ADMISSION_BIT;
            if active == OKX_WS_TERMINAL_ADMISSION_BIT - 1 {
                return None;
            }
            match self.trading_admission.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(OKXWebSocketTradingAdmission {
                        state: Arc::clone(&self.trading_admission),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn fail(&self, queue: OKXWebSocketQueue, kind: OKXWebSocketQueueFailureKind) {
        let previous = self
            .trading_admission
            .fetch_or(OKX_WS_TERMINAL_ADMISSION_BIT, Ordering::AcqRel);
        if previous & OKX_WS_TERMINAL_ADMISSION_BIT != 0 {
            return;
        }

        let callback = {
            let mut state = Self::lock_unpoisoned(&self.state);
            state.failure = Some((queue, kind));
            Arc::clone(&state.callback)
        };
        self.signal.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        log::error!("Terminal OKX WebSocket queue failure: queue={queue:?}, kind={kind:?}");
        callback(queue, kind);
    }

    pub(crate) fn try_send<T>(
        &self,
        queue: OKXWebSocketQueue,
        sender: &tokio::sync::mpsc::Sender<T>,
        value: T,
    ) -> Result<(), OKXWebSocketQueueFailureKind> {
        if self.is_failed() {
            return Err(self
                .failure()
                .map_or(OKXWebSocketQueueFailureKind::Closed, |(_, kind)| kind));
        }

        sender.try_send(value).map_err(|error| {
            let kind = match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    OKXWebSocketQueueFailureKind::Full
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    OKXWebSocketQueueFailureKind::Closed
                }
            };
            self.fail(queue, kind);
            kind
        })
    }
}

/// OKX WebSocket subscription rate limit: 480 requests per hour per connection.
///
/// This applies to subscribe/unsubscribe/login operations.
/// 480 per hour = 8 per minute, but we use per-hour for accurate limiting.
pub static OKX_WS_SUBSCRIPTION_QUOTA: LazyLock<Quota> =
    LazyLock::new(|| Quota::per_hour(NonZeroU32::new(480).expect("non-zero")));

/// Rate limit for single order, cancel, and amend WebSocket operations: 30 requests per second.
pub static OKX_WS_ORDER_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(30).expect("non-zero")).expect("valid constant")
});

/// Rate limit for batch order, cancel, and amend WebSocket operations: 7 requests per second.
pub static OKX_WS_BATCH_ORDER_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(7).expect("non-zero")).expect("valid constant")
});

/// Rate limit for mass cancel WebSocket operations: 2 requests per second.
pub static OKX_WS_MASS_CANCEL_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(2).expect("non-zero")).expect("valid constant")
});

/// Rate limit for algo order WebSocket operations: 10 requests per second.
pub static OKX_WS_ALGO_ORDER_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(10).expect("non-zero")).expect("valid constant")
});

/// Rate limit for algo cancel WebSocket operations: 1 request per second.
pub static OKX_WS_ALGO_CANCEL_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(1).expect("non-zero")).expect("valid constant")
});

/// Pre-interned rate limit key for subscription operations (subscribe/unsubscribe/login).
///
/// See: <https://www.okx.com/docs-v5/en/#websocket-api-login>
/// See: <https://www.okx.com/docs-v5/en/#websocket-api-subscribe>
pub static OKX_RATE_LIMIT_KEY_SUBSCRIPTION: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("subscription")]);

/// Pre-interned rate limit key for single regular order operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-place-order>
pub static OKX_RATE_LIMIT_KEY_ORDER: LazyLock<[Ustr; 1]> = LazyLock::new(|| [Ustr::from("order")]);

/// Pre-interned rate limit key for batch order operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-place-multiple-orders>
pub static OKX_RATE_LIMIT_KEY_BATCH_ORDER: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("batch-order")]);

/// Pre-interned rate limit key for single regular cancel operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-cancel-order>
pub static OKX_RATE_LIMIT_KEY_CANCEL: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("cancel")]);

/// Pre-interned rate limit key for batch cancel operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-cancel-multiple-orders>
pub static OKX_RATE_LIMIT_KEY_BATCH_CANCEL: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("batch-cancel")]);

/// Pre-interned rate limit key for mass cancel operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-mass-cancel-order>
pub static OKX_RATE_LIMIT_KEY_MASS_CANCEL: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("mass-cancel")]);

/// Pre-interned rate limit key for amend operations (amend orders).
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-amend-order>
pub static OKX_RATE_LIMIT_KEY_AMEND: LazyLock<[Ustr; 1]> = LazyLock::new(|| [Ustr::from("amend")]);

/// Pre-interned rate limit key for batch amend operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-amend-multiple-orders>
pub static OKX_RATE_LIMIT_KEY_BATCH_AMEND: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("batch-amend")]);

/// Pre-interned rate limit key for algo order operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-ws-place-algo-order>
pub static OKX_RATE_LIMIT_KEY_ALGO_ORDER: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("algo-order")]);

/// Pre-interned rate limit key for algo cancel operations.
///
/// See: <https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-ws-cancel-algo-order>
pub static OKX_RATE_LIMIT_KEY_ALGO_CANCEL: LazyLock<[Ustr; 1]> =
    LazyLock::new(|| [Ustr::from("algo-cancel")]);

enum OKXWebSocketTaskPhase {
    Idle,
    Connecting {
        generation: u64,
        task_exited: bool,
    },
    Running {
        generation: u64,
        handle: tokio::task::JoinHandle<()>,
    },
    Closing {
        generation: u64,
        abort_handle: tokio::task::AbortHandle,
    },
}

struct OKXWebSocketTaskOwner {
    signal: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    next_generation: AtomicU64,
    phase: Mutex<OKXWebSocketTaskPhase>,
}

impl OKXWebSocketTaskOwner {
    fn new(signal: Arc<AtomicBool>, shutdown_notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            signal,
            shutdown_notify,
            next_generation: AtomicU64::new(1),
            phase: Mutex::new(OKXWebSocketTaskPhase::Idle),
        }
    }

    fn lock_phase(&self) -> MutexGuard<'_, OKXWebSocketTaskPhase> {
        self.phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn begin_connect(self: &Arc<Self>) -> anyhow::Result<OKXWebSocketConnectAttempt> {
        let mut phase = self.lock_phase();
        if !matches!(*phase, OKXWebSocketTaskPhase::Idle) {
            anyhow::bail!("OKX WebSocket client already has an active connection attempt or task");
        }

        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
        *phase = OKXWebSocketTaskPhase::Connecting {
            generation,
            task_exited: false,
        };
        drop(phase);

        Ok(OKXWebSocketConnectAttempt {
            owner: Arc::clone(self),
            generation,
            completed: false,
        })
    }

    fn install_task(
        &self,
        generation: u64,
        handle: tokio::task::JoinHandle<()>,
    ) -> anyhow::Result<()> {
        let mut phase = self.lock_phase();
        if let OKXWebSocketTaskPhase::Connecting {
            generation: current,
            task_exited,
        } = &*phase
            && *current == generation
        {
            if *task_exited || handle.is_finished() {
                *phase = OKXWebSocketTaskPhase::Idle;
                drop(phase);
                handle.abort_handle().abort();
                anyhow::bail!("OKX WebSocket handler task exited before task installation");
            }

            *phase = OKXWebSocketTaskPhase::Running { generation, handle };
            return Ok(());
        }
        drop(phase);

        handle.abort_handle().abort();
        anyhow::bail!("OKX WebSocket connection attempt was superseded before task installation")
    }

    fn is_running(&self, generation: u64) -> bool {
        matches!(
            *self.lock_phase(),
            OKXWebSocketTaskPhase::Running {
                generation: current,
                ..
            } if current == generation
        )
    }

    fn task_exited(&self, generation: u64) {
        let mut phase = self.lock_phase();
        match &mut *phase {
            OKXWebSocketTaskPhase::Connecting {
                generation: current,
                task_exited,
            } if *current == generation => {
                // The spawned future can run to completion before the caller installs its
                // JoinHandle. Remember that fact in the same generation state so install_task
                // cannot resurrect a completed task as Running.
                *task_exited = true;
            }
            OKXWebSocketTaskPhase::Running {
                generation: current,
                ..
            } if *current == generation => {
                *phase = OKXWebSocketTaskPhase::Idle;
            }
            _ => {}
        }
    }

    fn abort_generation_sync(&self, generation: u64) {
        let mut phase = self.lock_phase();
        match std::mem::replace(&mut *phase, OKXWebSocketTaskPhase::Idle) {
            OKXWebSocketTaskPhase::Connecting {
                generation: current,
                ..
            } if current == generation => {}
            OKXWebSocketTaskPhase::Running {
                generation: current,
                handle,
            } if current == generation => handle.abort_handle().abort(),
            OKXWebSocketTaskPhase::Closing {
                generation: current,
                abort_handle,
            } if current == generation => abort_handle.abort(),
            other => {
                *phase = other;
                return;
            }
        }

        // Signal only while the matching generation still owns the phase lock. A stale attempt
        // must never stop a newer generation that entered after its task exited.
        self.signal.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    async fn rollback_generation(&self, generation: u64) {
        let handle = {
            let mut phase = self.lock_phase();
            let handle = match std::mem::replace(&mut *phase, OKXWebSocketTaskPhase::Idle) {
                OKXWebSocketTaskPhase::Connecting {
                    generation: current,
                    ..
                } if current == generation => None,
                OKXWebSocketTaskPhase::Running {
                    generation: current,
                    handle,
                } if current == generation => {
                    *phase = OKXWebSocketTaskPhase::Closing {
                        generation,
                        abort_handle: handle.abort_handle(),
                    };
                    Some(handle)
                }
                OKXWebSocketTaskPhase::Closing {
                    generation: current,
                    abort_handle,
                } if current == generation => {
                    abort_handle.abort();
                    None
                }
                other => {
                    *phase = other;
                    return;
                }
            };
            // Keep signal publication ordered under the generation phase lock so a newer
            // begin_connect cannot be poisoned by this rollback.
            self.signal.store(true, Ordering::Release);
            self.shutdown_notify.notify_waiters();
            handle
        };

        if let Some(handle) = handle {
            handle.abort_handle().abort();
            let _ = handle.await;
            let mut phase = self.lock_phase();
            if matches!(
                *phase,
                OKXWebSocketTaskPhase::Closing {
                    generation: current,
                    ..
                } if current == generation
            ) {
                *phase = OKXWebSocketTaskPhase::Idle;
            }
        }
    }

    fn begin_close(&self) -> Option<(u64, tokio::task::JoinHandle<()>)> {
        self.signal.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();

        let mut phase = self.lock_phase();
        match std::mem::replace(&mut *phase, OKXWebSocketTaskPhase::Idle) {
            OKXWebSocketTaskPhase::Running { generation, handle } => {
                *phase = OKXWebSocketTaskPhase::Closing {
                    generation,
                    abort_handle: handle.abort_handle(),
                };
                Some((generation, handle))
            }
            OKXWebSocketTaskPhase::Connecting { .. } => None,
            OKXWebSocketTaskPhase::Closing { abort_handle, .. } => {
                // A previous close future may have been cancelled after extracting the
                // JoinHandle. The retained AbortHandle makes a repeated close fail-safe.
                abort_handle.abort();
                None
            }
            other => {
                *phase = other;
                None
            }
        }
    }

    fn finish_close(&self, generation: u64) {
        let mut phase = self.lock_phase();
        if matches!(
            *phase,
            OKXWebSocketTaskPhase::Closing {
                generation: current,
                ..
            } if current == generation
        ) {
            *phase = OKXWebSocketTaskPhase::Idle;
        }
    }
}

impl Drop for OKXWebSocketTaskOwner {
    fn drop(&mut self) {
        self.signal.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();

        let phase = self
            .phase
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match std::mem::replace(phase, OKXWebSocketTaskPhase::Idle) {
            OKXWebSocketTaskPhase::Running { handle, .. } => handle.abort_handle().abort(),
            OKXWebSocketTaskPhase::Closing { abort_handle, .. } => abort_handle.abort(),
            _ => {}
        }
    }
}

struct OKXWebSocketConnectAttempt {
    owner: Arc<OKXWebSocketTaskOwner>,
    generation: u64,
    completed: bool,
}

impl OKXWebSocketConnectAttempt {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn install_task(&self, handle: tokio::task::JoinHandle<()>) -> anyhow::Result<()> {
        self.owner.install_task(self.generation, handle)
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.owner.is_running(self.generation),
            "OKX WebSocket handler task exited during connection setup"
        );
        self.completed = true;
        Ok(())
    }

    async fn rollback(&mut self) {
        self.owner.rollback_generation(self.generation).await;
        self.completed = true;
    }
}

impl Drop for OKXWebSocketConnectAttempt {
    fn drop(&mut self) {
        if !self.completed {
            self.owner.abort_generation_sync(self.generation);
        }
    }
}

struct OKXWebSocketTaskExitGuard {
    owner: Weak<OKXWebSocketTaskOwner>,
    generation: u64,
}

impl Drop for OKXWebSocketTaskExitGuard {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.task_exited(self.generation);
        }
    }
}

/// Immutable Nautilus routing identity attached to a singular venue request.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingOrderRouting {
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
}

/// Context stored at request admission time for correlating venue responses.
///
/// Batch and mass-cancel requests deliberately carry no single routing identity: one top-level
/// venue request owns one pending-capacity permit, while item-level identities remain in the
/// execution dispatch state.
#[derive(Debug)]
pub(crate) struct PendingOrderInfo {
    pub routing: Option<PendingOrderRouting>,
    pub client_order_id: Option<ClientOrderId>,
    operation: OKXWsOperation,
    request_id: String,
    connection_epoch: u64,
    deadline: Instant,
    capacity_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum OKXPendingOperationKind {
    Order,
    Cancel,
    Amend,
}

pub(crate) struct OKXPendingOperations {
    orders: DashMap<String, PendingOrderInfo>,
    cancels: DashMap<String, PendingOrderInfo>,
    amends: DashMap<String, PendingOrderInfo>,
    observed_resolutions: DashMap<String, PendingOrderInfo>,
    slots: Arc<tokio::sync::Semaphore>,
    capacity: usize,
    timeout: Duration,
    queue_failure: OKXWebSocketQueueFailureGuard,
}

impl Debug for OKXPendingOperations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(OKXPendingOperations))
            .field("orders", &self.orders.len())
            .field("cancels", &self.cancels.len())
            .field("amends", &self.amends.len())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl OKXPendingOperations {
    fn new(queue_failure: OKXWebSocketQueueFailureGuard) -> Self {
        Self::with_limits(
            queue_failure,
            OKX_WS_PENDING_OPERATION_CAPACITY,
            Duration::from_secs(OKX_WS_PENDING_OPERATION_TIMEOUT_SECS),
        )
    }

    fn with_limits(
        queue_failure: OKXWebSocketQueueFailureGuard,
        capacity: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            orders: DashMap::new(),
            cancels: DashMap::new(),
            amends: DashMap::new(),
            observed_resolutions: DashMap::new(),
            slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity,
            timeout,
            queue_failure,
        }
    }

    fn map(&self, kind: OKXPendingOperationKind) -> &DashMap<String, PendingOrderInfo> {
        match kind {
            OKXPendingOperationKind::Order => &self.orders,
            OKXPendingOperationKind::Cancel => &self.cancels,
            OKXPendingOperationKind::Amend => &self.amends,
        }
    }

    fn admit(
        &self,
        operation: OKXWsOperation,
        routing: Option<PendingOrderRouting>,
        client_order_id: Option<ClientOrderId>,
        request_id: String,
        connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        let kind = pending_kind_for_operation(&operation).ok_or_else(|| {
            OKXWsError::ClientError(format!(
                "OKX operation {operation:?} cannot be admitted as a pending trading request"
            ))
        })?;
        let permit = Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
            self.queue_failure.fail(
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingCapacityExhausted,
            );
            OKXWsError::ClientError(format!(
                "OKX pending operation capacity {} exhausted",
                self.capacity
            ))
        })?;
        let info = PendingOrderInfo {
            routing,
            client_order_id,
            operation,
            request_id: request_id.clone(),
            connection_epoch,
            deadline: Instant::now() + self.timeout,
            capacity_permit: Some(permit),
        };

        match self.map(kind).entry(request_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(info);
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                self.queue_failure.fail(
                    OKXWebSocketQueue::PendingOperations,
                    OKXWebSocketQueueFailureKind::PendingAmbiguous,
                );
                Err(OKXWsError::ClientError(format!(
                    "Duplicate unresolved OKX {kind:?} operation"
                )))
            }
        }
    }

    fn admit_order(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        request_id: String,
        connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        self.admit(
            OKXWsOperation::Order,
            Some(PendingOrderRouting {
                trader_id,
                strategy_id,
                instrument_id,
            }),
            Some(client_order_id),
            request_id,
            connection_epoch,
        )
    }

    fn admit_cancel(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        request_id: String,
        connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        self.admit(
            OKXWsOperation::CancelOrder,
            Some(PendingOrderRouting {
                trader_id,
                strategy_id,
                instrument_id,
            }),
            client_order_id,
            request_id,
            connection_epoch,
        )
    }

    fn admit_amend(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        request_id: String,
        connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        self.admit(
            OKXWsOperation::AmendOrder,
            Some(PendingOrderRouting {
                trader_id,
                strategy_id,
                instrument_id,
            }),
            client_order_id,
            request_id,
            connection_epoch,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "routed pending admission binds immutable domain and transport identities"
    )]
    fn admit_routed(
        &self,
        operation: OKXWsOperation,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        request_id: String,
        connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        self.admit(
            operation,
            Some(PendingOrderRouting {
                trader_id,
                strategy_id,
                instrument_id,
            }),
            client_order_id,
            request_id,
            connection_epoch,
        )
    }

    fn admit_aggregate(
        &self,
        operation: OKXWsOperation,
        request_id: String,
        connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        self.admit(operation, None, None, request_id, connection_epoch)
    }

    fn remove_unsent(&self, operation: &OKXWsOperation, request_id: &str) {
        let Some(kind) = pending_kind_for_operation(operation) else {
            return;
        };
        if let dashmap::mapref::entry::Entry::Occupied(entry) =
            self.map(kind).entry(request_id.to_string())
            && entry.get().request_id == request_id
            && entry.get().operation == *operation
        {
            entry.remove();
        }
    }

    fn resolve(
        &self,
        operation: &OKXWsOperation,
        response_request_id: Option<&str>,
        response_client_order_id: Option<&str>,
        current_epoch: u64,
    ) -> Option<PendingOrderInfo> {
        let kind = pending_kind_for_operation(operation)?;
        let Some(response_request_id) = response_request_id else {
            self.queue_failure.fail(
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            );
            return None;
        };
        let active_entry = self.map(kind).entry(response_request_id.to_string());
        let dashmap::mapref::entry::Entry::Occupied(entry) = active_entry else {
            // The authenticated orders channel can prove that an individual request reached a
            // terminal or venue-known state before its direct operation response arrives. Keep a
            // bounded tombstone so that a later direct response remains exactly correlated.
            let dashmap::mapref::entry::Entry::Occupied(observed) = self
                .observed_resolutions
                .entry(response_request_id.to_string())
            else {
                self.queue_failure.fail(
                    OKXWebSocketQueue::PendingOperations,
                    OKXWebSocketQueueFailureKind::PendingAmbiguous,
                );
                return None;
            };
            let response_matches = response_request_id == observed.get().request_id
                && observed.get().operation == *operation;
            let client_order_id_matches = observed.get().client_order_id.is_none_or(|expected| {
                response_client_order_id.is_none_or(|actual| actual == expected.as_str())
            });
            let epoch_matches = observed.get().connection_epoch == current_epoch;
            if response_matches && client_order_id_matches && epoch_matches {
                return Some(observed.remove());
            }
            drop(observed);
            let failure_kind = if epoch_matches {
                OKXWebSocketQueueFailureKind::PendingAmbiguous
            } else {
                OKXWebSocketQueueFailureKind::PendingStaleConnection
            };
            self.queue_failure
                .fail(OKXWebSocketQueue::PendingOperations, failure_kind);
            return None;
        };
        let response_matches =
            response_request_id == entry.get().request_id && entry.get().operation == *operation;
        let client_order_id_matches = entry.get().client_order_id.is_none_or(|expected| {
            response_client_order_id.is_none_or(|actual| actual == expected.as_str())
        });
        let epoch_matches = entry.get().connection_epoch == current_epoch;
        if response_matches && client_order_id_matches && epoch_matches {
            return Some(entry.remove());
        }
        drop(entry);
        let failure_kind = if epoch_matches {
            OKXWebSocketQueueFailureKind::PendingAmbiguous
        } else {
            OKXWebSocketQueueFailureKind::PendingStaleConnection
        };
        self.queue_failure
            .fail(OKXWebSocketQueue::PendingOperations, failure_kind);
        None
    }

    fn observe_kind(
        &self,
        kind: OKXPendingOperationKind,
        client_order_id: &str,
        current_epoch: u64,
    ) {
        let map = self.map(kind);
        let matching_request_ids = map
            .iter()
            .filter(|entry| {
                entry.connection_epoch == current_epoch
                    && entry
                        .client_order_id
                        .is_some_and(|expected| expected.as_str() == client_order_id)
            })
            .map(|entry| entry.request_id.clone())
            .collect::<Vec<_>>();

        if matching_request_ids.len() > 1 {
            self.queue_failure.fail(
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            );
            return;
        }
        let Some(request_id) = matching_request_ids.first() else {
            return;
        };

        self.purge_observed_resolutions(Instant::now());
        if self.observed_resolutions.len() >= self.capacity {
            // Leave the active entry intact. The normal pending timeout will surface sustained
            // response loss instead of allowing the recovery ledger to grow without a bound.
            return;
        }

        let dashmap::mapref::entry::Entry::Occupied(entry) = map.entry(request_id.clone()) else {
            return;
        };
        if entry.get().connection_epoch != current_epoch
            || !entry
                .get()
                .client_order_id
                .is_some_and(|expected| expected.as_str() == client_order_id)
        {
            return;
        }

        let mut info = entry.remove();
        drop(info.capacity_permit.take());
        info.deadline = Instant::now() + self.timeout;
        match self.observed_resolutions.entry(info.request_id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(info);
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                self.queue_failure.fail(
                    OKXWebSocketQueue::PendingOperations,
                    OKXWebSocketQueueFailureKind::PendingAmbiguous,
                );
            }
        }
    }

    fn observe_order_update(
        &self,
        client_order_id: &str,
        state: OKXOrderStatus,
        current_epoch: u64,
    ) {
        if client_order_id.is_empty() {
            return;
        }

        // Every authenticated order-channel update proves that a placement reached OKX. A cancel
        // is only proven once the same order reaches a terminal venue state. Amend requests remain
        // response-correlated because an ordinary order update does not prove which amendment won.
        self.observe_kind(
            OKXPendingOperationKind::Order,
            client_order_id,
            current_epoch,
        );
        if matches!(
            state,
            OKXOrderStatus::Canceled | OKXOrderStatus::Filled | OKXOrderStatus::MmpCanceled
        ) {
            self.observe_kind(
                OKXPendingOperationKind::Cancel,
                client_order_id,
                current_epoch,
            );
        }
    }

    fn purge_observed_resolutions(&self, now: Instant) {
        self.observed_resolutions
            .retain(|_, info| info.deadline > now);
    }

    #[cfg(test)]
    fn resolve_order(
        &self,
        response_request_id: Option<&str>,
        response_client_order_id: Option<&str>,
        current_epoch: u64,
    ) -> Option<PendingOrderInfo> {
        self.resolve(
            &OKXWsOperation::Order,
            response_request_id,
            response_client_order_id,
            current_epoch,
        )
    }

    #[cfg(test)]
    fn resolve_amend(
        &self,
        response_request_id: Option<&str>,
        response_client_order_id: Option<&str>,
        current_epoch: u64,
    ) -> Option<PendingOrderInfo> {
        self.resolve(
            &OKXWsOperation::AmendOrder,
            response_request_id,
            response_client_order_id,
            current_epoch,
        )
    }

    pub(crate) fn mark_send_failed(
        &self,
        operation: &OKXWsOperation,
        request_id: &str,
        client_order_id: Option<ClientOrderId>,
    ) {
        let Some(kind) = pending_kind_for_operation(operation) else {
            return;
        };
        let matches = self.map(kind).get(request_id).is_some_and(|entry| {
            entry.request_id == request_id
                && entry.operation == *operation
                && entry.client_order_id == client_order_id
        });
        let already_observed = self
            .observed_resolutions
            .get(request_id)
            .is_some_and(|entry| {
                entry.request_id == request_id
                    && entry.operation == *operation
                    && entry.client_order_id == client_order_id
            });
        if already_observed {
            log::warn!(
                "OKX asynchronous send reported failure after the authenticated orders channel \
                 proved the operation: operation={operation:?}, request_id={request_id}, \
                 client_order_id={client_order_id:?}"
            );
            return;
        }
        if !matches {
            log::error!(
                "OKX asynchronous send failure did not match its pending operation: \
                 operation={operation:?}, request_id={request_id}, \
                 client_order_id={client_order_id:?}"
            );
        }
        // A failure from the asynchronous one-shot transport path is ambiguous regardless of
        // whether the bookkeeping entry can still be located. Never erase the pending record or
        // turn this into a local rejection; halt and require venue reconciliation.
        self.queue_failure.fail(
            OKXWebSocketQueue::PendingOperations,
            OKXWebSocketQueueFailureKind::PendingAmbiguous,
        );
    }

    fn health_failure(
        &self,
        now: Instant,
        current_epoch: u64,
    ) -> Option<OKXWebSocketQueueFailureKind> {
        self.purge_observed_resolutions(now);
        for map in [&self.orders, &self.cancels, &self.amends] {
            for entry in map {
                if entry.connection_epoch != current_epoch {
                    return Some(OKXWebSocketQueueFailureKind::PendingStaleConnection);
                }
                if entry.deadline <= now {
                    return Some(OKXWebSocketQueueFailureKind::PendingExpired);
                }
            }
        }
        None
    }

    fn enforce_health(&self, current_epoch: u64) -> bool {
        let Some(kind) = self.health_failure(Instant::now(), current_epoch) else {
            return true;
        };
        self.queue_failure
            .fail(OKXWebSocketQueue::PendingOperations, kind);
        false
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.orders.len() + self.cancels.len() + self.amends.len()
    }

    #[cfg(test)]
    fn observed_len(&self) -> usize {
        self.observed_resolutions.len()
    }
}

fn pending_kind_for_operation(op: &OKXWsOperation) -> Option<OKXPendingOperationKind> {
    match op {
        OKXWsOperation::Order | OKXWsOperation::BatchOrders | OKXWsOperation::OrderAlgo => {
            Some(OKXPendingOperationKind::Order)
        }
        OKXWsOperation::CancelOrder
        | OKXWsOperation::BatchCancelOrders
        | OKXWsOperation::MassCancel
        | OKXWsOperation::CancelAlgos => Some(OKXPendingOperationKind::Cancel),
        OKXWsOperation::AmendOrder | OKXWsOperation::BatchAmendOrders => {
            Some(OKXPendingOperationKind::Amend)
        }
        OKXWsOperation::Subscribe | OKXWsOperation::Unsubscribe => None,
    }
}

/// Lightweight response-correlation state shared with consumers.
///
/// This handle intentionally does not own [`OKXWebSocketTaskOwner`], so a consumer task cannot
/// keep the transport alive after the last externally-owned client clone is dropped.
#[derive(Clone, Debug)]
pub(crate) struct OKXPendingResponseHandle {
    pending_operations: Arc<OKXPendingOperations>,
    connection_epoch: Arc<ArcSwap<AtomicU64>>,
}

impl OKXPendingResponseHandle {
    pub(crate) fn tracks_operation(&self, op: &OKXWsOperation) -> bool {
        pending_kind_for_operation(op).is_some()
    }

    pub(crate) fn current_connection_epoch(&self) -> u64 {
        self.connection_epoch.load().load(Ordering::Acquire)
    }

    pub(crate) fn resolve(
        &self,
        op: &OKXWsOperation,
        response_request_id: Option<&str>,
        response_client_order_id: Option<&str>,
    ) -> Option<PendingOrderInfo> {
        let epoch = self.current_connection_epoch();
        self.pending_operations
            .resolve(op, response_request_id, response_client_order_id, epoch)
    }

    pub(crate) fn observe_order_update(&self, client_order_id: &str, state: OKXOrderStatus) {
        let epoch = self.current_connection_epoch();
        self.pending_operations
            .observe_order_update(client_order_id, state, epoch);
    }

    pub(crate) fn mark_send_failed(
        &self,
        op: Option<&OKXWsOperation>,
        request_id: &str,
        client_order_id: Option<ClientOrderId>,
    ) {
        let Some(op) = op.filter(|operation| pending_kind_for_operation(operation).is_some())
        else {
            return;
        };
        self.pending_operations
            .mark_send_failed(op, request_id, client_order_id);
    }
}

/// Runtime operations needed by a detached message consumer.
///
/// The owner is weak by design. Python and Rust consumer loops may retain this handle, but doing so
/// must not prevent the shared RAII owner from observing the last external client drop.
#[derive(Clone)]
#[cfg(feature = "python")]
pub(crate) struct OKXWebSocketRuntimeHandle {
    task_owner: Weak<OKXWebSocketTaskOwner>,
    cmd_tx: Arc<tokio::sync::RwLock<tokio::sync::mpsc::Sender<HandlerCommand>>>,
    queue_failure: OKXWebSocketQueueFailureGuard,
    subscriptions_state: SubscriptionState,
    pending_responses: OKXPendingResponseHandle,
}

#[cfg(feature = "python")]
impl OKXWebSocketRuntimeHandle {
    pub(crate) fn pending_responses(&self) -> &OKXPendingResponseHandle {
        &self.pending_responses
    }

    pub(crate) fn queue_failure_guard(&self) -> OKXWebSocketQueueFailureGuard {
        self.queue_failure.clone()
    }

    pub(crate) async fn resubscribe_ws_channel(
        &self,
        instrument_id: InstrumentId,
        channel: OKXWsChannel,
    ) -> Result<(), OKXWsError> {
        let owner = self.task_owner.upgrade().ok_or_else(|| {
            OKXWsError::ClientError("OKX WebSocket external owner was dropped".to_string())
        })?;
        if owner.signal.load(Ordering::Acquire) {
            return Err(OKXWsError::ClientError(
                "OKX WebSocket shutdown rejects resubscribe".to_string(),
            ));
        }

        let arg = OKXSubscriptionArg {
            channel,
            inst_type: None,
            inst_family: None,
            inst_id: Some(instrument_id.symbol.inner()),
        };
        let topic = topic_from_subscription_arg(&arg);
        let sender = self.cmd_tx.read().await;
        try_send_handler_command(
            &sender,
            &self.queue_failure,
            HandlerCommand::Unsubscribe {
                args: vec![arg.clone()],
            },
            "book recovery unsubscribe command",
        )?;
        self.subscriptions_state.mark_unsubscribe(&topic);
        try_send_handler_command(
            &sender,
            &self.queue_failure,
            HandlerCommand::Subscribe { args: vec![arg] },
            "book recovery subscribe command",
        )?;
        self.subscriptions_state.mark_subscribe(&topic);
        drop(sender);
        drop(owner);
        Ok(())
    }
}

/// Provides a WebSocket client for connecting to [OKX](https://okx.com).
#[derive(Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.okx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.okx")
)]
pub struct OKXWebSocketClient {
    url: String,
    #[allow(dead_code)] // Read by Python bindings
    pub(crate) account_id: AccountId,
    vip_level: Arc<AtomicU8>,
    credential: Option<Credential>,
    heartbeat: Option<u64>,
    auth_timeout_secs: u64,
    auth_tracker: AuthTracker,
    signal: Arc<AtomicBool>,
    queue_failure: OKXWebSocketQueueFailureGuard,
    task_owner: Arc<OKXWebSocketTaskOwner>,
    connection_mode: Arc<ArcSwap<AtomicU8>>,
    connection_epoch: Arc<ArcSwap<AtomicU64>>,
    cmd_tx: Arc<tokio::sync::RwLock<tokio::sync::mpsc::Sender<HandlerCommand>>>,
    out_rx: Option<Arc<tokio::sync::mpsc::Receiver<OKXBudgetedMessage<OKXWsMessage>>>>,
    subscriptions_inst_type: Arc<DashMap<OKXWsChannel, AHashSet<OKXInstrumentType>>>,
    subscriptions_inst_family: Arc<DashMap<OKXWsChannel, AHashSet<Ustr>>>,
    subscriptions_inst_id: Arc<DashMap<OKXWsChannel, AHashSet<Ustr>>>,
    subscriptions_bare: Arc<DashMap<OKXWsChannel, bool>>,
    subscriptions_state: SubscriptionState,
    request_id_counter: Arc<AtomicU64>,
    instruments_cache: Arc<AtomicMap<Ustr, InstrumentAny>>,
    inst_id_code_cache: Arc<AtomicMap<Ustr, u64>>,
    pub(crate) pending_operations: Arc<OKXPendingOperations>,
    option_greeks_subs: Arc<AtomicMap<InstrumentId, AHashSet<OKXGreeksType>>>,
    /// Per-base-pair refcount for the `index-tickers` channel. Multiple
    /// instruments commonly share one base pair (e.g. `BTC-USDT-SWAP` and
    /// `BTC-USDT-240628` both depend on `BTC-USDT`), so the venue
    /// (un)subscribe must only fire on the 0↔1 transitions. Without this
    /// refcount, a Python caller unsubscribing one instrument would tear
    /// down the channel for every other subscriber on the same pair.
    index_pair_subscribers: Arc<DashMap<Ustr, usize>>,
    /// Serializes index-tickers transitions so a concurrent
    /// subscribe/unsubscribe pair on the same base pair cannot interleave
    /// the refcount check with the venue send and leave the channel
    /// unsubscribed while the local count says it is live.
    index_pair_transition: Arc<tokio::sync::Mutex<()>>,
    /// WebSocket transport backend (defaults to `Tungstenite`).
    transport_backend: TransportBackend,
    /// Optional proxy URL for the WebSocket transport.
    proxy_url: Option<String>,
    cancellation_token: CancellationToken,
}

impl Default for OKXWebSocketClient {
    fn default() -> Self {
        Self::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap()
    }
}

impl Debug for OKXWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(OKXWebSocketClient))
            .field("url", &self.url)
            .field("credential", &self.credential.as_ref().map(|_| REDACTED))
            .field("heartbeat", &self.heartbeat)
            .finish_non_exhaustive()
    }
}

impl OKXWebSocketClient {
    /// Creates a new [`OKXWebSocketClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: Option<String>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        account_id: Option<AccountId>,
        heartbeat: Option<u64>,
        auth_timeout_secs: Option<u64>,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> anyhow::Result<Self> {
        let url = url.unwrap_or_else(|| {
            get_ws_base_url_public(OKXRegion::Global, OKXEnvironment::Demo).to_string()
        });
        let account_id = account_id.unwrap_or(AccountId::from("OKX-master"));

        let credential = match (api_key, api_secret, api_passphrase) {
            (Some(key), Some(secret), Some(passphrase)) => {
                Some(Credential::new(key, secret, passphrase))
            }
            (None, None, None) => None,
            _ => anyhow::bail!(
                "`api_key`, `api_secret`, `api_passphrase` credentials must be provided together"
            ),
        };

        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let task_owner = Arc::new(OKXWebSocketTaskOwner::new(
            Arc::clone(&signal),
            Arc::clone(&shutdown_notify),
        ));
        let pending_operations = Arc::new(OKXPendingOperations::new(queue_failure.clone()));
        let subscriptions_inst_type = Arc::new(DashMap::new());
        let subscriptions_inst_family = Arc::new(DashMap::new());
        let subscriptions_inst_id = Arc::new(DashMap::new());
        let subscriptions_bare = Arc::new(DashMap::new());
        let subscriptions_state = SubscriptionState::new(OKX_WS_TOPIC_DELIMITER);

        Ok(Self {
            url,
            account_id,
            vip_level: Arc::new(AtomicU8::new(0)),
            credential,
            heartbeat,
            auth_timeout_secs: auth_timeout_secs.unwrap_or(AUTHENTICATION_TIMEOUT_SECS),
            auth_tracker: AuthTracker::new(),
            signal,
            queue_failure,
            task_owner,
            connection_mode: Arc::new(ArcSwap::from_pointee(AtomicU8::new(
                ConnectionMode::Closed.as_u8(),
            ))),
            connection_epoch: Arc::new(ArcSwap::from_pointee(AtomicU64::new(0))),
            cmd_tx: {
                // Placeholder channel until connect() creates the real handler and replays queued instruments
                let (tx, _) = tokio::sync::mpsc::channel(1);
                Arc::new(tokio::sync::RwLock::new(tx))
            },
            out_rx: None,
            subscriptions_inst_type,
            subscriptions_inst_family,
            subscriptions_inst_id,
            subscriptions_bare,
            subscriptions_state,
            request_id_counter: Arc::new(AtomicU64::new(1)),
            instruments_cache: Arc::new(AtomicMap::new()),
            inst_id_code_cache: Arc::new(AtomicMap::new()),
            pending_operations,
            option_greeks_subs: Arc::new(AtomicMap::new()),
            index_pair_subscribers: Arc::new(DashMap::new()),
            index_pair_transition: Arc::new(tokio::sync::Mutex::new(())),
            transport_backend,
            proxy_url,
            cancellation_token: CancellationToken::new(),
        })
    }

    /// Creates a new [`OKXWebSocketClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if credential values cannot be loaded or if the
    /// client fails to initialize.
    #[allow(clippy::too_many_arguments)]
    pub fn with_credentials(
        url: Option<String>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        account_id: Option<AccountId>,
        heartbeat: Option<u64>,
        auth_timeout_secs: Option<u64>,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> anyhow::Result<Self> {
        let url = url.unwrap_or_else(|| {
            get_ws_base_url_public(OKXRegion::Global, OKXEnvironment::Demo).to_string()
        });
        let api_key = get_or_env_var(api_key, "OKX_API_KEY")?;
        let api_secret = get_or_env_var(api_secret, "OKX_API_SECRET")?;
        let api_passphrase = get_or_env_var(api_passphrase, "OKX_API_PASSPHRASE")?;

        Self::new(
            Some(url),
            Some(api_key),
            Some(api_secret),
            Some(api_passphrase),
            account_id,
            heartbeat,
            auth_timeout_secs,
            transport_backend,
            proxy_url,
        )
    }

    /// Creates a new authenticated [`OKXWebSocketClient`] using environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if required environment variables are missing or if
    /// the client fails to initialize.
    pub fn from_env() -> anyhow::Result<Self> {
        let url = get_env_var("OKX_WS_URL")?;
        let api_key = get_env_var("OKX_API_KEY")?;
        let api_secret = get_env_var("OKX_API_SECRET")?;
        let api_passphrase = get_env_var("OKX_API_PASSPHRASE")?;

        Self::new(
            Some(url),
            Some(api_key),
            Some(api_secret),
            Some(api_passphrase),
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
    }

    /// Cancel all pending WebSocket requests.
    pub fn cancel_all_requests(&self) {
        self.cancellation_token.cancel();
    }

    /// Get the cancellation token for this client.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Installs the callback invoked for the first terminal WebSocket queue failure.
    ///
    /// If a failure happened before registration, the callback is invoked immediately with the
    /// latched failure. Queue failure is sticky for the lifetime of this client and all clones.
    pub fn set_queue_failure_callback(&self, callback: OKXWebSocketQueueFailureCallback) {
        self.queue_failure.set_callback(callback);
    }

    /// Returns the first terminal internal queue failure, if any.
    #[must_use]
    pub fn queue_failure(&self) -> Option<(OKXWebSocketQueue, OKXWebSocketQueueFailureKind)> {
        self.queue_failure.failure()
    }

    /// Returns the websocket url being used by the client.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    /// Returns the public API key being used by the client.
    pub fn api_key(&self) -> Option<&str> {
        self.credential.as_ref().map(|c| c.api_key())
    }

    /// Returns a masked version of the API key for logging purposes.
    #[must_use]
    pub fn api_key_masked(&self) -> Option<String> {
        self.credential.as_ref().map(|c| c.api_key_masked())
    }

    /// Returns a value indicating whether the client is active.
    pub fn is_active(&self) -> bool {
        let connection_mode_arc = self.connection_mode.load();
        ConnectionMode::from_atomic(&connection_mode_arc).is_active()
            && !self.signal.load(Ordering::Acquire)
    }

    /// Returns a value indicating whether the client is closed.
    pub fn is_closed(&self) -> bool {
        let connection_mode_arc = self.connection_mode.load();
        ConnectionMode::from_atomic(&connection_mode_arc).is_closed()
            || self.signal.load(Ordering::Acquire)
    }

    /// Caches multiple instruments.
    ///
    /// Any existing instruments with the same symbols will be replaced.
    pub fn cache_instruments(&self, instruments: &[InstrumentAny]) {
        self.instruments_cache.rcu(|m| {
            for inst in instruments {
                m.insert(inst.symbol().inner(), inst.clone());
            }
        });
    }

    /// Caches a single instrument.
    ///
    /// Any existing instrument with the same symbol will be replaced.
    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        self.instruments_cache
            .insert(instrument.symbol().inner(), instrument);
    }

    /// Returns a snapshot of the instruments cache as an `AHashMap`.
    pub fn instruments_snapshot(&self) -> AHashMap<Ustr, InstrumentAny> {
        (**self.instruments_cache.load()).clone()
    }

    /// Returns a shared handle to the live instruments cache.
    pub fn instruments_cache_arc(&self) -> Arc<AtomicMap<Ustr, InstrumentAny>> {
        Arc::clone(&self.instruments_cache)
    }

    /// Caches the instIdCode mapping for an instrument.
    ///
    /// The instIdCode is required for WebSocket order operations per OKX API deprecation.
    pub fn cache_inst_id_code(&self, inst_id: Ustr, inst_id_code: u64) {
        self.inst_id_code_cache.insert(inst_id, inst_id_code);
    }

    /// Caches multiple instIdCode mappings for instruments.
    ///
    /// This is typically called after loading instruments from the HTTP API.
    pub fn cache_inst_id_codes(&self, mappings: impl IntoIterator<Item = (Ustr, u64)>) {
        let entries: Vec<_> = mappings.into_iter().collect();
        self.inst_id_code_cache.rcu(|m| {
            for (inst_id, inst_id_code) in &entries {
                m.insert(*inst_id, *inst_id_code);
            }
        });
    }

    /// Gets the instIdCode for an instrument.
    ///
    /// Returns `None` if the instrument is not in the cache.
    #[must_use]
    pub fn get_inst_id_code(&self, inst_id: &Ustr) -> Option<u64> {
        self.inst_id_code_cache.load().get(inst_id).copied()
    }

    fn inst_id_symbol_and_code_from_snapshot(
        inst_id_codes: &AHashMap<Ustr, u64>,
        inst_id: &InstrumentId,
        action: &str,
    ) -> Result<(Ustr, u64), OKXWsError> {
        let inst_id_symbol = inst_id.symbol.inner();
        let inst_id_code = inst_id_codes.get(&inst_id_symbol).copied().ok_or_else(|| {
            OKXWsError::ClientError(format!(
                "No instIdCode cached for {inst_id}, cannot {action} order"
            ))
        })?;
        Ok((inst_id_symbol, inst_id_code))
    }

    /// Sets the VIP level for this client.
    ///
    /// The VIP level determines which WebSocket channels are available.
    pub fn set_vip_level(&self, vip_level: OKXVipLevel) {
        self.vip_level.store(vip_level as u8, Ordering::Relaxed);
    }

    /// Gets the current VIP level.
    pub fn vip_level(&self) -> OKXVipLevel {
        let level = self.vip_level.load(Ordering::Relaxed);
        OKXVipLevel::from(level)
    }

    /// Connect to the OKX WebSocket server.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection process fails.
    ///
    /// # Panics
    ///
    /// Panics if subscription arguments fail to serialize to JSON.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if let Some((queue, kind)) = self.queue_failure.failure() {
            anyhow::bail!(
                "OKX WebSocket client has a terminal queue failure: queue={queue:?}, kind={kind:?}"
            );
        }
        let mut connection_attempt = self.task_owner.begin_connect()?;

        // Reset signal so is_active()/is_closed() work after a previous close()
        self.signal.store(false, Ordering::Release);

        let (message_handler, raw_rx) = okx_raw_message_channel(
            OKX_WS_RAW_QUEUE_CAPACITY,
            OKX_WS_MAX_MESSAGE_SIZE,
            OKX_WS_INGRESS_BYTE_BUDGET,
            self.queue_failure.clone(),
        );

        // No-op ping handler: handler owns the WebSocketClient and responds to pings directly
        // in the message loop for minimal latency (see handler.rs TEXT_PONG response)
        let ping_handler: PingHandler = Arc::new(move |_payload: Vec<u8>| {
            // Handler responds to pings internally via select! loop
        });

        let headers = vec![(USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string())];

        let config = WebSocketConfig {
            url: self.url.clone(),
            headers,
            heartbeat: self.heartbeat,
            heartbeat_msg: Some(TEXT_PING.to_string()),
            reconnect_timeout_ms: Some(5_000),
            reconnect_delay_initial_ms: None,
            reconnect_delay_max_ms: None,
            reconnect_backoff_factor: None,
            reconnect_jitter_ms: None,
            reconnect_max_attempts: None,
            idle_timeout_ms: None,
            max_frame_size: OKX_WS_MAX_FRAME_SIZE,
            max_message_size: OKX_WS_MAX_MESSAGE_SIZE,
            backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        };

        let keyed_quotas = vec![
            (
                OKX_RATE_LIMIT_KEY_SUBSCRIPTION[0].as_str().to_string(),
                *OKX_WS_SUBSCRIPTION_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_ORDER[0].as_str().to_string(),
                *OKX_WS_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_BATCH_ORDER[0].as_str().to_string(),
                *OKX_WS_BATCH_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_CANCEL[0].as_str().to_string(),
                *OKX_WS_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_BATCH_CANCEL[0].as_str().to_string(),
                *OKX_WS_BATCH_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_MASS_CANCEL[0].as_str().to_string(),
                *OKX_WS_MASS_CANCEL_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_AMEND[0].as_str().to_string(),
                *OKX_WS_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_BATCH_AMEND[0].as_str().to_string(),
                *OKX_WS_BATCH_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_ALGO_ORDER[0].as_str().to_string(),
                *OKX_WS_ALGO_ORDER_QUOTA,
            ),
            (
                OKX_RATE_LIMIT_KEY_ALGO_CANCEL[0].as_str().to_string(),
                *OKX_WS_ALGO_CANCEL_QUOTA,
            ),
        ];

        let writer_failure_guard = self.queue_failure.clone();
        let writer_failure_callback = Arc::new(move |failure| {
            let kind = match failure {
                WriterQueueFailure::Full => OKXWebSocketQueueFailureKind::Full,
                WriterQueueFailure::Closed => OKXWebSocketQueueFailureKind::Closed,
            };
            writer_failure_guard.fail(OKXWebSocketQueue::Writer, kind);
        });
        let client = WebSocketClient::connect_with_bounded_writer(
            config,
            BoundedWriterConfig {
                capacity: OKX_WS_WRITER_QUEUE_CAPACITY,
                on_failure: writer_failure_callback,
            },
            Some(message_handler),
            Some(ping_handler),
            None, // post_reconnection
            keyed_quotas,
            Some(*OKX_WS_CONNECTION_QUOTA), // Default quota for connection operations
        )
        .await?;

        // Replace connection state so all clones see the underlying WebSocketClient's state
        self.connection_mode.store(client.connection_mode_atomic());
        self.connection_epoch
            .store(client.connection_epoch_atomic());

        let (msg_tx, rx) = tokio::sync::mpsc::channel::<OKXBudgetedMessage<OKXWsMessage>>(
            OKX_WS_OUTPUT_QUEUE_CAPACITY,
        );

        self.out_rx = Some(Arc::new(rx));

        let (cmd_tx, cmd_rx) =
            tokio::sync::mpsc::channel::<HandlerCommand>(OKX_WS_COMMAND_QUEUE_CAPACITY);
        *self.cmd_tx.write().await = cmd_tx.clone();

        let auth_tracker = self.auth_tracker.clone();
        let subscriptions_state = self.subscriptions_state.clone();
        let task_owner = Arc::downgrade(&self.task_owner);
        let task_generation = connection_attempt.generation();
        let pending_operations = Arc::clone(&self.pending_operations);
        let connection_epoch = Arc::clone(&self.connection_epoch);

        let stream_handle = get_runtime().spawn({
            let auth_tracker = auth_tracker.clone();
            let queue_failure = self.queue_failure.clone();
            let credential = self.credential.clone();
            let cmd_tx_for_reconnect = cmd_tx.clone();
            let subscriptions_bare = self.subscriptions_bare.clone();
            let subscriptions_inst_type = self.subscriptions_inst_type.clone();
            let subscriptions_inst_family = self.subscriptions_inst_family.clone();
            let subscriptions_inst_id = self.subscriptions_inst_id.clone();
            let mut has_reconnected = false;

            async move {
                let _task_exit = OKXWebSocketTaskExitGuard {
                    owner: Weak::clone(&task_owner),
                    generation: task_generation,
                };
                let Some(owner) = task_owner.upgrade() else {
                    return;
                };
                let signal = Arc::clone(&owner.signal);
                let shutdown_notify = Arc::clone(&owner.shutdown_notify);
                drop(owner);

                let mut handler = OKXWsFeedHandler::new(
                    signal.clone(),
                    shutdown_notify,
                    queue_failure.clone(),
                    cmd_rx,
                    raw_rx,
                    msg_tx,
                    auth_tracker.clone(),
                    subscriptions_state.clone(),
                );

                // Helper closure to resubscribe all tracked subscriptions after reconnection
                let resubscribe_all = || {
                    for entry in subscriptions_inst_id.iter() {
                        let (channel, inst_ids) = entry.pair();
                        for inst_id in inst_ids {
                            let arg = OKXSubscriptionArg {
                                channel: channel.clone(),
                                inst_type: None,
                                inst_family: None,
                                inst_id: Some(*inst_id),
                            };

                            if let Err(e) = try_send_handler_command(&cmd_tx_for_reconnect, &queue_failure, HandlerCommand::Subscribe { args: vec![arg] }, "resubscribe") {
                                log::error!("Failed to send resubscribe command: error={e}");
                            }
                        }
                    }

                    for entry in subscriptions_bare.iter() {
                        let channel = entry.key();
                        let arg = OKXSubscriptionArg {
                            channel: channel.clone(),
                            inst_type: None,
                            inst_family: None,
                            inst_id: None,
                        };

                        if let Err(e) = try_send_handler_command(&cmd_tx_for_reconnect, &queue_failure, HandlerCommand::Subscribe { args: vec![arg] }, "resubscribe") {
                            log::error!("Failed to send resubscribe command: error={e}");
                        }
                    }

                    for entry in subscriptions_inst_type.iter() {
                        let (channel, inst_types) = entry.pair();
                        for inst_type in inst_types {
                            let arg = OKXSubscriptionArg {
                                channel: channel.clone(),
                                inst_type: Some(*inst_type),
                                inst_family: None,
                                inst_id: None,
                            };

                            if let Err(e) = try_send_handler_command(&cmd_tx_for_reconnect, &queue_failure, HandlerCommand::Subscribe { args: vec![arg] }, "resubscribe") {
                                log::error!("Failed to send resubscribe command: error={e}");
                            }
                        }
                    }

                    for entry in subscriptions_inst_family.iter() {
                        let (channel, inst_families) = entry.pair();
                        for inst_family in inst_families {
                            let arg = OKXSubscriptionArg {
                                channel: channel.clone(),
                                inst_type: None,
                                inst_family: Some(*inst_family),
                                inst_id: None,
                            };

                            if let Err(e) = try_send_handler_command(&cmd_tx_for_reconnect, &queue_failure, HandlerCommand::Subscribe { args: vec![arg] }, "resubscribe") {
                                log::error!("Failed to send resubscribe command: error={e}");
                            }
                        }
                    }
                };

                let mut pending_watchdog =
                    tokio::time::interval(OKX_WS_PENDING_WATCHDOG_INTERVAL);
                pending_watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    let next = tokio::select! {
                        biased;
                        _ = pending_watchdog.tick() => {
                            let epoch = connection_epoch.load().load(Ordering::Acquire);
                            if !pending_operations.enforce_health(epoch) {
                                break;
                            }
                            continue;
                        }
                        event = handler.next() => event,
                    };
                    match next {
                        Some(event) if matches!(&event.message, OKXWsMessage::Reconnected) => {
                            if signal.load(Ordering::Acquire) {
                                continue;
                            }

                            has_reconnected = true;

                            // Mark all confirmed subscriptions as failed so they transition to pending state
                            let confirmed_topics_vec: Vec<String> = {
                                let confirmed = subscriptions_state.confirmed();
                                let mut topics = Vec::new();

                                for entry in confirmed.iter() {
                                    let channel = entry.key();
                                    for symbol in entry.value() {
                                        if symbol.as_str() == "#" {
                                            topics.push(channel.to_string());
                                        } else {
                                            topics.push(format!("{channel}{OKX_WS_TOPIC_DELIMITER}{symbol}"));
                                        }
                                    }
                                }
                                topics
                            };

                            if !confirmed_topics_vec.is_empty() {
                                log::debug!("Marking confirmed subscriptions as pending for replay: count={}", confirmed_topics_vec.len());
                                for topic in confirmed_topics_vec {
                                    subscriptions_state.mark_failure(&topic);
                                }
                            }

                            if let Some(cred) = &credential {
                                log::debug!("Re-authenticating after reconnection");
                                let timestamp = std::time::SystemTime::now()
                                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                                    .expect("System time should be after UNIX epoch")
                                    .as_secs()
                                    .to_string();
                                let signature = cred.sign(&timestamp, "GET", "/users/self/verify", "");

                                let auth_message = super::messages::OKXAuthentication {
                                    op: "login",
                                    args: vec![super::messages::OKXAuthenticationArg {
                                        api_key: cred.api_key().to_string(),
                                        passphrase: cred.api_passphrase().to_string(),
                                        timestamp,
                                        sign: signature,
                                    }],
                                };

                                if let Ok(payload) = serde_json::to_string(&auth_message) {
                                    if let Err(e) = try_send_handler_command(&cmd_tx_for_reconnect, &queue_failure, HandlerCommand::Authenticate { payload }, "reconnection authentication") {
                                        log::error!("Failed to send reconnection auth command: error={e}");
                                    }
                                } else {
                                    log::error!("Failed to serialize reconnection auth message");
                                }
                            }

                            // Unauthenticated sessions resubscribe immediately after reconnection,
                            // authenticated sessions wait for Authenticated message
                            if credential.is_none() {
                                log::debug!("No authentication required, resubscribing immediately");
                                resubscribe_all();
                            }

                            // Forward Reconnected to consumers so they can reset state
                            if handler.send(event).is_err() {
                                break;
                            }
                        }
                        Some(event) if matches!(&event.message, OKXWsMessage::Authenticated) => {
                            if has_reconnected {
                                resubscribe_all();
                            }
                        }
                        Some(event) => {
                            if handler.send(event).is_err() {
                                break;
                            }
                        }
                        None => {
                            if handler.is_stopped() {
                                log::debug!(
                                    "Stop signal received, ending message processing",
                                );
                                break;
                            }
                            log::debug!("WebSocket stream closed");
                            break;
                        }
                    }
                }

                log::debug!("Handler task exiting");
            }
        });

        if let Err(error) = connection_attempt.install_task(stream_handle) {
            self.out_rx = None;
            self.connection_mode
                .load()
                .store(ConnectionMode::Closed.as_u8(), Ordering::Release);
            connection_attempt.rollback().await;
            return Err(error);
        }

        if let Err(error) = try_send_handler_command(
            &cmd_tx,
            &self.queue_failure,
            HandlerCommand::SetClient(client),
            "WebSocket client",
        ) {
            self.out_rx = None;
            self.connection_mode
                .load()
                .store(ConnectionMode::Closed.as_u8(), Ordering::Release);
            connection_attempt.rollback().await;
            return Err(error.into());
        }
        log::debug!("Sent WebSocket client to handler");

        if self.credential.is_some()
            && let Err(e) = self.authenticate().await
        {
            self.out_rx = None;
            self.connection_mode
                .load()
                .store(ConnectionMode::Closed.as_u8(), Ordering::Release);
            connection_attempt.rollback().await;
            anyhow::bail!("Authentication failed: {e}");
        }

        if let Err(error) = connection_attempt.commit() {
            self.out_rx = None;
            self.connection_mode
                .load()
                .store(ConnectionMode::Closed.as_u8(), Ordering::Release);
            connection_attempt.rollback().await;
            return Err(error);
        }
        Ok(())
    }

    /// Authenticates the WebSocket session with OKX.
    async fn authenticate(&self) -> Result<(), Error> {
        let credential = self.credential.as_ref().ok_or_else(|| {
            Error::Io(std::io::Error::other(
                "API credentials not available to authenticate",
            ))
        })?;

        let rx = self.auth_tracker.begin();

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should be after UNIX epoch")
            .as_secs()
            .to_string();
        let signature = credential.sign(&timestamp, "GET", "/users/self/verify", "");

        let auth_message = OKXAuthentication {
            op: "login",
            args: vec![OKXAuthenticationArg {
                api_key: credential.api_key().to_string(),
                passphrase: credential.api_passphrase().to_string(),
                timestamp,
                sign: signature,
            }],
        };

        let payload = serde_json::to_string(&auth_message).map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to serialize auth message: {e}"
            )))
        })?;

        let sender = self.cmd_tx.read().await;
        try_send_handler_command(
            &sender,
            &self.queue_failure,
            HandlerCommand::Authenticate { payload },
            "authenticate command",
        )
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        drop(sender);

        match self
            .auth_tracker
            .wait_for_result::<OKXWsError>(Duration::from_secs(self.auth_timeout_secs), rx)
            .await
        {
            Ok(()) => {
                log::debug!("WebSocket authenticated");
                Ok(())
            }
            Err(e) => {
                log::error!("WebSocket authentication failed: error={e}");
                Err(Error::Io(std::io::Error::other(e.to_string())))
            }
        }
    }

    /// Provides the internal data stream as a channel-based stream.
    ///
    /// # Panics
    ///
    /// This function panics if:
    /// - The websocket is not connected.
    /// - `stream_data` has already been called somewhere else (stream receiver is then taken).
    pub fn stream(&mut self) -> impl Stream<Item = OKXWsMessage> + 'static {
        let rx = self
            .out_rx
            .take()
            .expect("Data stream receiver already taken or not connected");
        let mut rx = Arc::try_unwrap(rx).expect("Cannot take ownership - other references exist");
        async_stream::stream! {
            while let Some(data) = rx.recv().await {
                yield data.message;
            }
        }
    }

    /// Wait until the WebSocket connection is active.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection times out.
    pub async fn wait_until_active(&self, timeout_secs: f64) -> Result<(), OKXWsError> {
        let timeout = tokio::time::Duration::from_secs_f64(timeout_secs);

        tokio::time::timeout(timeout, async {
            while !self.is_active() {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            OKXWsError::ClientError(format!(
                "WebSocket connection timeout after {timeout_secs} seconds"
            ))
        })?;

        Ok(())
    }

    /// Closes the client.
    ///
    /// # Errors
    ///
    /// Returns an error if disconnecting the websocket or cleaning up the
    /// client fails.
    pub async fn close(&mut self) -> Result<(), Error> {
        log::debug!("Starting close process");

        if let Some((generation, stream_handle)) = self.task_owner.begin_close() {
            log::debug!("Waiting for stream handle to complete");
            let abort_handle = stream_handle.abort_handle();
            match tokio::time::timeout(Duration::from_secs(2), stream_handle).await {
                Ok(Ok(())) => log::debug!("Stream handle completed successfully"),
                Ok(Err(e)) => log::error!("Stream handle encountered an error: {e:?}"),
                Err(_) => {
                    log::warn!("Timeout waiting for stream handle, aborting task");
                    abort_handle.abort();
                }
            }
            self.task_owner.finish_close(generation);
        } else {
            log::debug!("No stream handle to await");
        }
        self.connection_mode
            .load()
            .store(ConnectionMode::Closed.as_u8(), Ordering::Release);

        // Wipe per-base-pair refcounts so a subsequent reconnect can re-arm
        // the index-tickers channel. Otherwise the stale count short-circuits
        // every future `subscribe_index_prices` call and the feed stays dark.
        self.index_pair_subscribers.clear();

        log::debug!("Close process completed");

        Ok(())
    }

    /// Get active subscriptions for a specific instrument.
    pub fn get_subscriptions(&self, instrument_id: InstrumentId) -> Vec<OKXWsChannel> {
        let symbol = instrument_id.symbol.inner();
        let mut channels = Vec::new();

        for entry in self.subscriptions_inst_id.iter() {
            let (channel, instruments) = entry.pair();
            if instruments.contains(&symbol) {
                channels.push(channel.clone());
            }
        }

        channels
    }

    fn generate_unique_request_id(&self) -> String {
        self.request_id_counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
    }

    pub(crate) fn current_connection_epoch(&self) -> u64 {
        self.connection_epoch.load().load(Ordering::Acquire)
    }

    pub(crate) fn pending_response_handle(&self) -> OKXPendingResponseHandle {
        OKXPendingResponseHandle {
            pending_operations: Arc::clone(&self.pending_operations),
            connection_epoch: Arc::clone(&self.connection_epoch),
        }
    }

    #[cfg(test)]
    pub(crate) fn admit_aggregate_for_test(
        &self,
        operation: OKXWsOperation,
        request_id: &str,
    ) -> Result<(), OKXWsError> {
        self.pending_operations.admit_aggregate(
            operation,
            request_id.to_string(),
            self.current_connection_epoch(),
        )
    }

    #[cfg(test)]
    pub(crate) fn pending_operation_count_for_test(&self) -> usize {
        self.pending_operations.len()
    }

    #[cfg(feature = "python")]
    pub(crate) fn runtime_handle(&self) -> OKXWebSocketRuntimeHandle {
        OKXWebSocketRuntimeHandle {
            task_owner: Arc::downgrade(&self.task_owner),
            cmd_tx: Arc::clone(&self.cmd_tx),
            queue_failure: self.queue_failure.clone(),
            subscriptions_state: self.subscriptions_state.clone(),
            pending_responses: self.pending_response_handle(),
        }
    }

    pub(crate) fn queue_failure_guard(&self) -> OKXWebSocketQueueFailureGuard {
        self.queue_failure.clone()
    }

    async fn subscribe(&self, args: Vec<OKXSubscriptionArg>) -> Result<(), OKXWsError> {
        // Send the command first; only update local state on success
        let sender = self.cmd_tx.read().await;
        try_send_handler_command(
            &sender,
            &self.queue_failure,
            HandlerCommand::Subscribe { args: args.clone() },
            "subscribe command",
        )?;
        drop(sender);

        for arg in &args {
            let topic = topic_from_subscription_arg(arg);
            self.subscriptions_state.mark_subscribe(&topic);

            // Check if this is a bare channel (no inst params)
            if arg.inst_type.is_none() && arg.inst_family.is_none() && arg.inst_id.is_none() {
                self.subscriptions_bare.insert(arg.channel.clone(), true);
            } else {
                if let Some(inst_type) = &arg.inst_type {
                    self.subscriptions_inst_type
                        .entry(arg.channel.clone())
                        .or_default()
                        .insert(*inst_type);
                }

                if let Some(inst_family) = &arg.inst_family {
                    self.subscriptions_inst_family
                        .entry(arg.channel.clone())
                        .or_default()
                        .insert(*inst_family);
                }

                if let Some(inst_id) = &arg.inst_id {
                    self.subscriptions_inst_id
                        .entry(arg.channel.clone())
                        .or_default()
                        .insert(*inst_id);
                }
            }
        }

        Ok(())
    }

    #[expect(clippy::collapsible_if)]
    async fn unsubscribe(&self, args: Vec<OKXSubscriptionArg>) -> Result<(), OKXWsError> {
        // Send the command first; only update local state on success
        let sender = self.cmd_tx.read().await;
        try_send_handler_command(
            &sender,
            &self.queue_failure,
            HandlerCommand::Unsubscribe { args: args.clone() },
            "unsubscribe command",
        )?;
        drop(sender);

        for arg in &args {
            let topic = topic_from_subscription_arg(arg);
            self.subscriptions_state.mark_unsubscribe(&topic);

            if arg.inst_type.is_none() && arg.inst_family.is_none() && arg.inst_id.is_none() {
                self.subscriptions_bare.remove(&arg.channel);
            } else {
                if let Some(inst_type) = &arg.inst_type {
                    if let Some(mut entry) = self.subscriptions_inst_type.get_mut(&arg.channel) {
                        entry.remove(inst_type);
                        if entry.is_empty() {
                            drop(entry);
                            self.subscriptions_inst_type.remove(&arg.channel);
                        }
                    }
                }

                if let Some(inst_family) = &arg.inst_family {
                    if let Some(mut entry) = self.subscriptions_inst_family.get_mut(&arg.channel) {
                        entry.remove(inst_family);
                        if entry.is_empty() {
                            drop(entry);
                            self.subscriptions_inst_family.remove(&arg.channel);
                        }
                    }
                }

                if let Some(inst_id) = &arg.inst_id {
                    if let Some(mut entry) = self.subscriptions_inst_id.get_mut(&arg.channel) {
                        entry.remove(inst_id);
                        if entry.is_empty() {
                            drop(entry);
                            self.subscriptions_inst_id.remove(&arg.channel);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn subscribe_inst_id(
        &self,
        channel: OKXWsChannel,
        inst_id: Ustr,
    ) -> Result<(), OKXWsError> {
        self.subscribe(vec![OKXSubscriptionArg {
            channel,
            inst_type: None,
            inst_family: None,
            inst_id: Some(inst_id),
        }])
        .await
    }

    async fn unsubscribe_inst_id(
        &self,
        channel: OKXWsChannel,
        inst_id: Ustr,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe(vec![OKXSubscriptionArg {
            channel,
            inst_type: None,
            inst_family: None,
            inst_id: Some(inst_id),
        }])
        .await
    }

    /// Unsubscribes from all active subscriptions in batched messages.
    ///
    /// Collects all confirmed subscriptions and sends unsubscribe requests in batches,
    /// which is significantly more efficient than individual unsubscribes during disconnect.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe request fails to send.
    pub async fn unsubscribe_all(&self) -> Result<(), OKXWsError> {
        const BATCH_SIZE: usize = 256;

        let mut all_args = Vec::new();

        for entry in self.subscriptions_inst_type.iter() {
            let (channel, inst_types) = entry.pair();
            for inst_type in inst_types {
                all_args.push(OKXSubscriptionArg {
                    channel: channel.clone(),
                    inst_type: Some(*inst_type),
                    inst_family: None,
                    inst_id: None,
                });
            }
        }

        for entry in self.subscriptions_inst_family.iter() {
            let (channel, inst_families) = entry.pair();
            for inst_family in inst_families {
                all_args.push(OKXSubscriptionArg {
                    channel: channel.clone(),
                    inst_type: None,
                    inst_family: Some(*inst_family),
                    inst_id: None,
                });
            }
        }

        for entry in self.subscriptions_inst_id.iter() {
            let (channel, inst_ids) = entry.pair();
            for inst_id in inst_ids {
                all_args.push(OKXSubscriptionArg {
                    channel: channel.clone(),
                    inst_type: None,
                    inst_family: None,
                    inst_id: Some(*inst_id),
                });
            }
        }

        for entry in self.subscriptions_bare.iter() {
            let channel = entry.key();
            all_args.push(OKXSubscriptionArg {
                channel: channel.clone(),
                inst_type: None,
                inst_family: None,
                inst_id: None,
            });
        }

        if all_args.is_empty() {
            log::debug!("No active subscriptions to unsubscribe from");
            return Ok(());
        }

        log::debug!("Batched unsubscribe from {} channels", all_args.len());

        for chunk in all_args.chunks(BATCH_SIZE) {
            self.unsubscribe(chunk.to_vec()).await?;
        }

        // The index-pair refcount mirrors live subscriptions; after a bulk
        // unsubscribe the venue knows nothing, so any retained count would
        // wedge the next `subscribe_index_prices`.
        self.index_pair_subscribers.clear();

        Ok(())
    }

    /// Subscribes to instrument updates for a specific instrument type.
    ///
    /// Provides updates when instrument specifications change.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-instruments-channel>.
    pub async fn subscribe_instruments(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Instruments,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Subscribes to instrument updates for a specific instrument.
    ///
    /// Since OKX doesn't support subscribing to individual instruments via `instId`,
    /// this method subscribes to the entire instrument type. OKX handles duplicate
    /// subscriptions gracefully and pushes a fresh snapshot on each subscribe.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-instruments-channel>.
    pub async fn subscribe_instrument(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        let inst_type = okx_instrument_type_from_symbol(instrument_id.symbol.as_str());
        log::debug!("Subscribing to instrument type {inst_type:?} for {instrument_id}");
        self.subscribe_instruments(inst_type).await
    }

    /// Subscribes to order book data for an instrument.
    ///
    /// This is a convenience method that calls [`Self::subscribe_book_with_depth`] with depth 0,
    /// which automatically selects the appropriate channel based on VIP level.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_book(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        self.subscribe_book_with_depth(instrument_id, 0).await
    }

    /// Subscribes to the standard books channel (internal method).
    pub(crate) async fn subscribe_books_channel(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::Books, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to the Retail Price Improvement order book channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_book_rpi(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::BooksRpi, instrument_id.symbol.inner())
            .await
    }

    /// Requests a fresh snapshot by replacing the current incremental book subscription.
    pub(crate) async fn resubscribe_book_channel(
        &self,
        instrument_id: InstrumentId,
        channel: OKXBookChannel,
    ) -> Result<(), OKXWsError> {
        let channel = ws_channel_for_book(channel);
        self.resubscribe_ws_channel(instrument_id, channel).await
    }

    /// Replaces an instrument subscription on the specified WebSocket channel.
    pub(crate) async fn resubscribe_ws_channel(
        &self,
        instrument_id: InstrumentId,
        channel: OKXWsChannel,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(channel.clone(), instrument_id.symbol.inner())
            .await?;
        self.subscribe_inst_id(channel, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to 5-level order book snapshot data for an instrument.
    ///
    /// Updates every 100ms when there are changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-5-depth-channel>.
    pub async fn subscribe_book_depth5(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::Books5, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to 50-level tick-by-tick order book data for an instrument.
    ///
    /// Provides real-time updates whenever order book changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-50-depth-tbt-channel>.
    pub async fn subscribe_book50_l2_tbt(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::Books50Tbt, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to tick-by-tick full depth (400 levels) order book data for an instrument.
    ///
    /// Provides real-time updates with all depth levels whenever order book changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-400-depth-tbt-channel>.
    pub async fn subscribe_book_l2_tbt(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::BooksTbt, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to order book data with automatic channel selection based on VIP level and depth.
    ///
    /// Selects the optimal channel based on user's VIP tier and requested depth:
    /// - depth 50: Requires VIP4+, subscribes to `books50-l2-tbt`
    /// - depth 0 or 400:
    ///   - VIP5+: subscribes to `books-l2-tbt` (400 depth, fastest)
    ///   - Below VIP5: subscribes to `books` (standard depth)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Subscription request fails
    /// - depth is 50 but VIP level is below 4
    pub async fn subscribe_book_with_depth(
        &self,
        instrument_id: InstrumentId,
        depth: u16,
    ) -> anyhow::Result<()> {
        let vip = self.vip_level();

        if !matches!(depth, 0 | 50 | 400) {
            anyhow::bail!("Invalid depth {depth}, must be 0, 50, or 400");
        }

        if depth == 50 && vip < OKXVipLevel::Vip4 {
            anyhow::bail!("VIP level {vip} insufficient for 50 depth subscription (requires VIP4)");
        }

        let channel = select_book_channel(depth as usize, vip);
        self.subscribe_inst_id(ws_channel_for_book(channel), instrument_id.symbol.inner())
            .await?;
        Ok(())
    }

    /// Subscribes to best bid/ask quote data for an instrument.
    ///
    /// Provides tick-by-tick updates of the best bid and ask prices using the bbo-tbt channel.
    /// Supports all instrument types: SPOT, MARGIN, SWAP, FUTURES, OPTION.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-best-bid-offer-channel>.
    pub async fn subscribe_quotes(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::BboTbt, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to trade data for an instrument.
    ///
    /// When `aggregated` is `false`, subscribes to the `trades` channel (per-match updates).
    /// When `aggregated` is `true`, subscribes to the `trades-all` channel (aggregated updates).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-trades-channel>.
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-all-trades-channel>.
    pub async fn subscribe_trades(
        &self,
        instrument_id: InstrumentId,
        aggregated: bool,
    ) -> Result<(), OKXWsError> {
        let channel = if aggregated {
            OKXWsChannel::TradesAll
        } else {
            OKXWsChannel::Trades
        };
        self.subscribe_inst_id(channel, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to 24hr rolling ticker data for an instrument.
    ///
    /// Updates every 100ms with trading statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-tickers-channel>.
    pub async fn subscribe_ticker(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::Tickers, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to mark price data for derivatives instruments.
    ///
    /// Updates every 200ms for perpetual swaps, or at settlement for futures.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-mark-price-channel>.
    pub async fn subscribe_mark_prices(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::MarkPrice, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to index price data for an instrument.
    ///
    /// Updates every second with the underlying index price.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-index-tickers-channel>.
    pub async fn subscribe_index_prices(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        // Index-tickers channel requires base pair format (e.g., BTC-USDT)
        let symbol = instrument_id.symbol.inner();
        let (base, quote) = parse_base_quote_from_symbol(symbol.as_str())
            .map_err(|e| OKXWsError::ClientError(e.to_string()))?;
        let base_pair = Ustr::from(&format!("{base}-{quote}"));

        // Hold the transition lock across both the refcount update and the
        // venue send so a concurrent `unsubscribe_index_prices` cannot
        // observe a transient 0 state between our decrement and the venue
        // unsubscribe, or vice versa. Without this, contract rolls can
        // leave the venue unsubscribed while the local count says active.
        let _guard = self.index_pair_transition.lock().await;

        // Bump the per-base-pair refcount so a later unsubscribe can decide
        // whether it is the last subscriber. Only the 0→1 transition fires
        // a venue subscribe; subsequent callers piggy-back on the existing
        // channel.
        let is_first = {
            let mut count = self.index_pair_subscribers.entry(base_pair).or_insert(0);
            *count += 1;
            *count == 1
        };

        if !is_first {
            return Ok(());
        }

        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::IndexTickers,
            inst_type: None,
            inst_family: None,
            inst_id: Some(base_pair),
        };

        match self.subscribe(vec![arg]).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // When the venue subscribe fails there is no live channel,
                // even though other local callers may have piggy-backed on
                // the in-flight attempt (they saw `!is_first` and returned
                // `Ok`). Removing the entry entirely ensures the next
                // caller re-enters the 0→1 branch and re-arms the venue
                // subscription; a mere decrement would leave the map at 1+
                // without a matching feed and every later subscribe would
                // short-circuit into a silent no-op.
                self.index_pair_subscribers.remove(&base_pair);
                Err(e)
            }
        }
    }

    /// Subscribes to option summary data for an instrument family.
    ///
    /// Streams greeks (delta, gamma, vega, theta), implied volatility, and other
    /// option metrics for all instruments in the specified family.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-option-summary-channel>.
    pub async fn subscribe_option_summary(&self, inst_family: Ustr) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::OptionSummary,
            inst_type: None,
            inst_family: Some(inst_family),
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Subscribes to event contract market updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-event-contract-markets-channel>.
    pub async fn subscribe_event_contract_markets(&self) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::EventContractMarkets,
            inst_type: Some(OKXInstrumentType::Events),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Returns a reference to the option greeks subscription map.
    ///
    /// The map stores the set of greeks conventions to emit for each subscribed instrument.
    pub fn option_greeks_subs(&self) -> &Arc<AtomicMap<InstrumentId, AHashSet<OKXGreeksType>>> {
        &self.option_greeks_subs
    }

    /// Adds an instrument to the option greeks subscription filter, emitting both
    /// Black-Scholes and price-adjusted greeks.
    pub fn add_option_greeks_sub(&self, instrument_id: InstrumentId) {
        let both: AHashSet<OKXGreeksType> =
            [OKXGreeksType::Bs, OKXGreeksType::Pa].into_iter().collect();
        self.option_greeks_subs.insert(instrument_id, both);
    }

    /// Adds an instrument to the option greeks subscription filter with an explicit
    /// set of greeks conventions to emit. An empty set is treated as "emit both".
    pub fn add_option_greeks_sub_with_conventions(
        &self,
        instrument_id: InstrumentId,
        conventions: AHashSet<OKXGreeksType>,
    ) {
        let set = if conventions.is_empty() {
            [OKXGreeksType::Bs, OKXGreeksType::Pa].into_iter().collect()
        } else {
            conventions
        };
        self.option_greeks_subs.insert(instrument_id, set);
    }

    /// Removes an instrument from the option greeks subscription filter.
    pub fn remove_option_greeks_sub(&self, instrument_id: &InstrumentId) {
        self.option_greeks_subs.remove(instrument_id);
    }

    /// Subscribes to funding rate data for perpetual swap instruments.
    ///
    /// Updates when funding rate changes or at funding intervals.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#public-data-websocket-funding-rate-channel>.
    pub async fn subscribe_funding_rates(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::FundingRate, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to candlestick/bar data for an instrument.
    ///
    /// Supports various time intervals from 1s to 3M.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-candlesticks-channel>.
    pub async fn subscribe_bars(&self, bar_type: BarType) -> Result<(), OKXWsError> {
        // Use regular trade-price candlesticks which work for all instrument types
        let channel = bar_spec_as_okx_channel(bar_type.spec())
            .map_err(|e| OKXWsError::ClientError(e.to_string()))?;
        self.subscribe_inst_id(channel, bar_type.instrument_id().symbol.inner())
            .await
    }

    /// Unsubscribes from instrument updates for a specific instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_instruments(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Instruments,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Unsubscribe from instrument updates for a specific instrument.
    ///
    /// No-op: the instruments channel is per-type (SWAP, FUTURES, etc.) and
    /// other instruments of the same type may still need it. The channel
    /// stays subscribed; overhead is negligible.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    pub async fn unsubscribe_instrument(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        log::debug!("Instrument unsubscribe is a no-op (shared per-type channel): {instrument_id}");
        Ok(())
    }

    /// Unsubscribe from full order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_book(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::Books, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from Retail Price Improvement order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    pub async fn unsubscribe_book_rpi(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::BooksRpi, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from 5-level order book snapshot data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_book_depth5(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::Books5, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from 50-level tick-by-tick order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_book50_l2_tbt(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::Books50Tbt, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from tick-by-tick full depth order book data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_book_l2_tbt(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::BooksTbt, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from best bid/ask quote data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_quotes(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::BboTbt, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from 24hr rolling ticker data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_ticker(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::Tickers, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from mark price data for a derivatives instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_mark_prices(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::MarkPrice, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from index price data for the base pair derived from
    /// `instrument_id`.
    ///
    /// Refcounting is handled internally so any caller (Rust data client,
    /// Python wrapper, etc.) can pair every `subscribe_index_prices` with
    /// exactly one `unsubscribe_index_prices`. The OKX `index-tickers`
    /// channel is keyed by base pair (e.g. `BTC-USDT`), so the venue
    /// unsubscribe only fires when the last subscriber for that pair drops.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    pub async fn unsubscribe_index_prices(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        let symbol = instrument_id.symbol.inner();
        let (base, quote) = parse_base_quote_from_symbol(symbol.as_str())
            .map_err(|e| OKXWsError::ClientError(e.to_string()))?;
        let base_pair = Ustr::from(&format!("{base}-{quote}"));

        // Serialize with any concurrent `subscribe_index_prices` on the same
        // base pair. See the subscribe path for the race this prevents.
        let _guard = self.index_pair_transition.lock().await;

        let is_last = {
            let Some(mut count) = self.index_pair_subscribers.get_mut(&base_pair) else {
                // No matching subscriber recorded; nothing to do.
                return Ok(());
            };
            *count = count.saturating_sub(1);
            *count == 0
        };

        if !is_last {
            return Ok(());
        }

        self.index_pair_subscribers
            .remove_if(&base_pair, |_, count| *count == 0);

        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::IndexTickers,
            inst_type: None,
            inst_family: None,
            inst_id: Some(base_pair),
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Unsubscribe from option summary data for an instrument family.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    pub async fn unsubscribe_option_summary(&self, inst_family: Ustr) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::OptionSummary,
            inst_type: None,
            inst_family: Some(inst_family),
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Unsubscribes from event contract market updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscription request fails.
    pub async fn unsubscribe_event_contract_markets(&self) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::EventContractMarkets,
            inst_type: Some(OKXInstrumentType::Events),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Unsubscribe from funding rate data for a perpetual swap instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_funding_rates(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::FundingRate, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from trade data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_trades(
        &self,
        instrument_id: InstrumentId,
        aggregated: bool,
    ) -> Result<(), OKXWsError> {
        let channel = if aggregated {
            OKXWsChannel::TradesAll
        } else {
            OKXWsChannel::Trades
        };
        self.unsubscribe_inst_id(channel, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribe from candlestick/bar data for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_bars(&self, bar_type: BarType) -> Result<(), OKXWsError> {
        let channel = bar_spec_as_okx_channel(bar_type.spec())
            .map_err(|e| OKXWsError::ClientError(e.to_string()))?;
        self.unsubscribe_inst_id(channel, bar_type.instrument_id().symbol.inner())
            .await
    }

    /// Subscribes to order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_orders(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Orders,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_orders(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Orders,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Subscribes to Nitro spread order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_spread_orders(&self) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::SprdOrders,
            inst_type: None,
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from Nitro spread order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_spread_orders(&self) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::SprdOrders,
            inst_type: None,
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Subscribes to best bid/offer quotes for a spread instrument (`sprd-bbo-tbt`).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_spread_quotes(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::SprdBboTbt, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to 5-level book snapshots for a spread instrument (`sprd-books5`).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_spread_book(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::SprdBooks5, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to public trades for a spread instrument (`sprd-public-trades`).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_spread_trades(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.subscribe_inst_id(OKXWsChannel::SprdPublicTrades, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribes from spread quotes (`sprd-bbo-tbt`).
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe request fails.
    pub async fn unsubscribe_spread_quotes(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::SprdBboTbt, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribes from spread book snapshots (`sprd-books5`).
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe request fails.
    pub async fn unsubscribe_spread_book(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::SprdBooks5, instrument_id.symbol.inner())
            .await
    }

    /// Unsubscribes from spread public trades (`sprd-public-trades`).
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe request fails.
    pub async fn unsubscribe_spread_trades(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), OKXWsError> {
        self.unsubscribe_inst_id(OKXWsChannel::SprdPublicTrades, instrument_id.symbol.inner())
            .await
    }

    /// Subscribes to algo order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_orders_algo(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::OrdersAlgo,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from algo order updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_orders_algo(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::OrdersAlgo,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Subscribes to advance algo order updates (trailing stops, iceberg, twap).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_algo_advance(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::AlgoAdvance,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from advance algo order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_algo_advance(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::AlgoAdvance,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Subscribes to fill updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_fills(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Fills,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from fill updates for the given instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_fills(
        &self,
        instrument_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Fills,
            inst_type: Some(instrument_type),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Subscribes to account balance updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn subscribe_account(&self) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Account,
            inst_type: None,
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from account balance updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_account(&self) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Account,
            inst_type: None,
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Subscribes to position updates for a specific instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#websocket-api-private-channel-positions-channel>
    pub async fn subscribe_positions(
        &self,
        inst_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Positions,
            inst_type: Some(inst_type),
            inst_family: None,
            inst_id: None,
        };
        self.subscribe(vec![arg]).await
    }

    /// Unsubscribes from position updates for a specific instrument type.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    pub async fn unsubscribe_positions(
        &self,
        inst_type: OKXInstrumentType,
    ) -> Result<(), OKXWsError> {
        let arg = OKXSubscriptionArg {
            channel: OKXWsChannel::Positions,
            inst_type: Some(inst_type),
            inst_family: None,
            inst_id: None,
        };
        self.unsubscribe(vec![arg]).await
    }

    /// Place multiple orders in a single batch via WebSocket.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-batch-orders>
    async fn ws_batch_place_orders(&self, args: Vec<Value>) -> Result<(), OKXWsError> {
        validate_batch_size("batch place orders", args.len())?;
        let request_id = self.generate_unique_request_id();
        let operation = OKXWsOperation::BatchOrders;
        let request = OKXWsRequest::<Value> {
            id: Some(request_id.clone()),
            op: operation.clone(),
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize batch orders: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_aggregate(
            operation.clone(),
            request_id.clone(),
            connection_epoch,
        )?;
        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_BATCH_ORDER.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id: None,
            op: Some(operation.clone()),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;
        if result.is_err() {
            self.pending_operations
                .remove_unsent(&operation, &request_id);
        }
        result
    }

    /// Cancel multiple orders in a single batch via WebSocket.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-batch-cancel-orders>
    async fn ws_batch_cancel_orders(&self, args: Vec<Value>) -> Result<(), OKXWsError> {
        validate_batch_size("batch cancel orders", args.len())?;
        let request_id = self.generate_unique_request_id();
        let operation = OKXWsOperation::BatchCancelOrders;
        let request = OKXWsRequest::<Value> {
            id: Some(request_id.clone()),
            op: operation.clone(),
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize batch cancel: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_aggregate(
            operation.clone(),
            request_id.clone(),
            connection_epoch,
        )?;
        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_BATCH_CANCEL.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id: None,
            op: Some(operation.clone()),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;
        if result.is_err() {
            self.pending_operations
                .remove_unsent(&operation, &request_id);
        }
        result
    }

    /// Amend multiple orders in a single batch via WebSocket.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-batch-amend-orders>
    async fn ws_batch_amend_orders(&self, args: Vec<Value>) -> Result<(), OKXWsError> {
        validate_batch_size("batch amend orders", args.len())?;
        let request_id = self.generate_unique_request_id();
        let operation = OKXWsOperation::BatchAmendOrders;
        let request = OKXWsRequest::<Value> {
            id: Some(request_id.clone()),
            op: operation.clone(),
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize batch amend: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_aggregate(
            operation.clone(),
            request_id.clone(),
            connection_epoch,
        )?;
        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_BATCH_AMEND.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id: None,
            op: Some(operation.clone()),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;
        if result.is_err() {
            self.pending_operations
                .remove_unsent(&operation, &request_id);
        }
        result
    }

    /// Submits an order, automatically routing conditional orders to the algo endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the order parameters are invalid or if the request
    /// cannot be sent to the websocket client.
    ///
    /// # References
    ///
    /// - Regular orders: <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-place-order>
    /// - Algo orders: <https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-place-algo-order>
    #[expect(clippy::too_many_arguments)]
    pub async fn submit_order(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        td_mode: OKXTradeMode,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        time_in_force: Option<TimeInForce>,
        price: Option<Price>,
        trigger_price: Option<Price>,
        post_only: Option<bool>,
        reduce_only: Option<bool>,
        quote_quantity: Option<bool>,
        position_side: Option<PositionSide>,
        attach_algo_ords: Option<Vec<WsAttachAlgoOrdParams>>,
        px_usd: Option<String>,
        px_vol: Option<String>,
        speed_bump: Option<String>,
        outcome: Option<String>,
        slippage_pct: Option<String>,
        rpi: Option<bool>,
        rpi_taker_access: Option<bool>,
        rpi_px_round: Option<bool>,
    ) -> Result<(), OKXWsError> {
        let rpi = rpi.unwrap_or(false);

        if !OKX_SUPPORTED_ORDER_TYPES.contains(&order_type) {
            return Err(OKXWsError::ClientError(format!(
                "Unsupported order type: {order_type:?}",
            )));
        }

        if let Some(tif) = time_in_force
            && !OKX_SUPPORTED_TIME_IN_FORCE.contains(&tif)
        {
            return Err(OKXWsError::ClientError(format!(
                "Unsupported time in force: {tif:?}",
            )));
        }

        let mut builder = WsPostOrderParamsBuilder::default();

        let inst_id_code = self
            .get_inst_id_code(&instrument_id.symbol.inner())
            .ok_or_else(|| {
                OKXWsError::ClientError(format!(
                    "No instIdCode cached for {instrument_id}, cannot submit order"
                ))
            })?;
        builder.inst_id_code(inst_id_code);

        builder.td_mode(td_mode);
        builder.cl_ord_id(client_order_id.as_str());

        let (instrument_type, quote_currency) = {
            let instruments = self.instruments_cache.load();
            let symbol = instrument_id.symbol.inner();
            let instrument = instruments.get(&symbol).ok_or_else(|| {
                OKXWsError::ClientError(format!("Unknown instrument {instrument_id}"))
            })?;
            let instrument_type = okx_instrument_type(instrument)
                .map_err(|e| OKXWsError::ClientError(e.to_string()))?;
            (instrument_type, instrument.quote_currency())
        };

        // OKX options only support limit-style orders
        if instrument_type == OKXInstrumentType::Option
            && matches!(order_type, OrderType::Market | OrderType::MarketToLimit)
        {
            return Err(OKXWsError::ClientError(
                "Market orders are not supported for OKX options, use Limit orders instead"
                    .to_string(),
            ));
        }

        match instrument_type {
            OKXInstrumentType::Spot => {
                // SPOT: ccy parameter is required by OKX for spot trading
                builder.ccy(quote_currency.to_string());
            }
            OKXInstrumentType::Margin => {
                builder.ccy(quote_currency.to_string());

                if let Some(ro) = reduce_only
                    && ro
                {
                    builder.reduce_only(ro);
                }
            }
            OKXInstrumentType::Swap | OKXInstrumentType::Futures => {
                // SWAP/FUTURES: use quote currency for margin (required by OKX)
                builder.ccy(quote_currency.to_string());

                // For derivatives, posSide is required by OKX
                // Use Net for one-way mode (default for NETTING OMS)
                if position_side.is_none() {
                    builder.pos_side(OKXPositionSide::Net);
                }
            }
            OKXInstrumentType::Option => {
                builder.ccy(quote_currency.to_string());

                if position_side.is_none() {
                    builder.pos_side(OKXPositionSide::Net);
                }
                // reduceOnly is not applicable to options per OKX docs
            }
            OKXInstrumentType::Events => {}
            _ => {
                builder.ccy(quote_currency.to_string());

                if position_side.is_none() {
                    builder.pos_side(OKXPositionSide::Net);
                }

                if let Some(ro) = reduce_only
                    && ro
                {
                    builder.reduce_only(ro);
                }
            }
        }

        if let Some(attach_algo_ords) = attach_algo_ords {
            builder.attach_algo_ords(attach_algo_ords);
        }

        // For SPOT market orders in Cash mode, handle tgtCcy parameter
        // https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order
        // OKX API default behavior for SPOT market orders:
        // - BUY orders default to tgtCcy=quote_ccy (sz represents quote currency amount)
        // - SELL orders default to tgtCcy=base_ccy (sz represents base currency amount)
        // Note: tgtCcy is ONLY supported for Cash trading mode, not for margin modes (Cross/Isolated)
        if instrument_type == OKXInstrumentType::Spot
            && order_type == OrderType::Market
            && td_mode == OKXTradeMode::Cash
        {
            match quote_quantity {
                Some(true) => {
                    builder.tgt_ccy(OKXTargetCurrency::QuoteCcy);
                }
                // For BUY orders, must explicitly set to base_ccy to override OKX default
                Some(false) if order_side == OrderSide::Buy => {
                    builder.tgt_ccy(OKXTargetCurrency::BaseCcy);
                }
                // For SELL orders with quote_quantity=false, omit tgtCcy (OKX defaults to base_ccy correctly)
                Some(false) | None => {}
            }
        }

        builder.side(order_side.as_specified());

        if let Some(pos_side) = position_side {
            builder.pos_side(pos_side);
        }

        // OKX implements FOK/IOC as order types rather than separate time-in-force
        // Market + FOK is unsupported (FOK requires a limit price)
        // optimal_limit_ioc is only supported for SWAP/FUTURES, not SPOT or OPTION
        if rpi && order_type != OrderType::Limit {
            return Err(OKXWsError::ClientError(
                "OKX RPI orders require a limit order".to_string(),
            ));
        }

        let (okx_ord_type, price) = if rpi {
            (OKXOrderType::Rpi, price)
        } else if post_only.unwrap_or(false) {
            (OKXOrderType::PostOnly, price)
        } else if let Some(tif) = time_in_force {
            match (order_type, tif) {
                (OrderType::Market, TimeInForce::Fok) => {
                    return Err(OKXWsError::ClientError(
                        "Market orders with FOK time-in-force are not supported by OKX. Use Limit order with FOK instead.".to_string()
                    ));
                }
                (OrderType::Market, TimeInForce::Ioc) => {
                    // optimal_limit_ioc only works for SWAP/FUTURES
                    if matches!(
                        instrument_type,
                        OKXInstrumentType::Spot | OKXInstrumentType::Option
                    ) {
                        (OKXOrderType::Market, price)
                    } else {
                        (OKXOrderType::OptimalLimitIoc, price)
                    }
                }
                (OrderType::Limit, TimeInForce::Fok) => {
                    // OKX uses op_fok for options FOK orders
                    if instrument_type == OKXInstrumentType::Option {
                        (OKXOrderType::OpFok, price)
                    } else {
                        (OKXOrderType::Fok, price)
                    }
                }
                (OrderType::Limit, TimeInForce::Ioc) => (OKXOrderType::Ioc, price),
                _ => (OKXOrderType::from(order_type), price),
            }
        } else {
            (OKXOrderType::from(order_type), price)
        };

        log::debug!(
            "Order type mapping: order_type={order_type:?}, time_in_force={time_in_force:?}, post_only={post_only:?} -> okx_ord_type={okx_ord_type:?}"
        );

        let speed_bump = if instrument_type == OKXInstrumentType::Events {
            if outcome.is_none() {
                return Err(OKXWsError::ClientError(
                    "OKX event contract orders require `outcome`".to_string(),
                ));
            }

            if okx_ord_type == OKXOrderType::PostOnly {
                speed_bump
            } else {
                Some(speed_bump.unwrap_or_else(|| "1".to_string()))
            }
        } else {
            speed_bump
        };

        if let Some(speed_bump) = speed_bump {
            builder.speed_bump(speed_bump);
        }

        if let Some(outcome) = outcome {
            builder.outcome(outcome);
        }

        if let Some(slippage) = slippage_pct {
            builder.slippage_pct(slippage);
        }

        if let Some(rpi_taker_access) = rpi_taker_access {
            builder.rpi_taker_access(rpi_taker_access);
        }

        if let Some(rpi_px_round) = rpi_px_round {
            builder.rpi_px_round(rpi_px_round);
        }

        builder.ord_type(okx_ord_type);
        builder.sz(quantity.to_string());

        // For options: pxUsd/pxVol are mutually exclusive with px
        if let Some(usd) = px_usd {
            builder.px_usd(usd);
        } else if let Some(vol) = px_vol {
            builder.px_vol(vol);
        } else if let Some(tp) = trigger_price {
            builder.px(tp.to_string());
        } else if let Some(p) = price {
            builder.px(p.to_string());
        }

        builder.tag(OKX_NAUTILUS_BROKER_ID);

        let params = builder
            .build()
            .map_err(|e| OKXWsError::ClientError(format!("Build order params error: {e}")))?;

        let request_id = self.generate_unique_request_id();
        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: super::enums::OKXWsOperation::Order,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize order: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_order(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            request_id.clone(),
            connection_epoch,
        )?;

        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_ORDER.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id: Some(client_order_id),
            op: Some(super::enums::OKXWsOperation::Order),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;

        if result.is_err() {
            self.pending_operations
                .remove_unsent(&OKXWsOperation::Order, &request_id);
        }

        result
    }

    /// Place a new order via WebSocket.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-place-order>
    /// Modifies an existing order.
    ///
    /// # Errors
    ///
    /// Returns an error if the amend parameters are invalid or if the
    /// websocket request fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-amend-order>.
    #[expect(clippy::too_many_arguments)]
    pub async fn modify_order(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        price: Option<Price>,
        quantity: Option<Quantity>,
        venue_order_id: Option<VenueOrderId>,
        new_px_usd: Option<String>,
        new_px_vol: Option<String>,
        speed_bump: Option<String>,
        rpi_taker_access: Option<bool>,
        rpi_px_round: Option<bool>,
    ) -> Result<(), OKXWsError> {
        let mut builder = WsAmendOrderParamsBuilder::default();

        let inst_id_code = self
            .get_inst_id_code(&instrument_id.symbol.inner())
            .ok_or_else(|| {
                OKXWsError::ClientError(format!(
                    "No instIdCode cached for {instrument_id}, cannot amend order"
                ))
            })?;
        builder.inst_id_code(inst_id_code);

        if let Some(venue_order_id) = venue_order_id {
            builder.ord_id(venue_order_id.as_str());
        }

        if let Some(client_order_id) = client_order_id {
            builder.cl_ord_id(client_order_id.as_str());
        }

        // For options: newPxUsd/newPxVol are mutually exclusive with newPx
        if let Some(usd) = new_px_usd {
            builder.new_px_usd(usd);
        } else if let Some(vol) = new_px_vol {
            builder.new_px_vol(vol);
        } else if let Some(price) = price {
            builder.new_px(price.to_string());
        }

        if let Some(quantity) = quantity {
            builder.new_sz(quantity.to_string());
        }

        if let Some(speed_bump) = speed_bump {
            builder.speed_bump(speed_bump);
        }

        if let Some(rpi_taker_access) = rpi_taker_access {
            builder.rpi_taker_access(rpi_taker_access);
        }

        if let Some(rpi_px_round) = rpi_px_round {
            builder.rpi_px_round(rpi_px_round);
        }

        let params = builder
            .build()
            .map_err(|e| OKXWsError::ClientError(format!("Build amend params error: {e}")))?;

        let request_id = self.generate_unique_request_id();
        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: super::enums::OKXWsOperation::AmendOrder,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize amend: {e}")))?;
        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_amend(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            request_id.clone(),
            connection_epoch,
        )?;

        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_AMEND.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id,
            op: Some(super::enums::OKXWsOperation::AmendOrder),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;

        if result.is_err() {
            self.pending_operations
                .remove_unsent(&OKXWsOperation::AmendOrder, &request_id);
        }

        result
    }

    /// Cancels an existing order.
    ///
    /// # Errors
    ///
    /// Returns an error if the cancel parameters are invalid or if the
    /// cancellation request fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-cancel-order>.
    pub async fn cancel_order(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> Result<(), OKXWsError> {
        let mut builder = WsCancelOrderParamsBuilder::default();

        let inst_id_code = self
            .get_inst_id_code(&instrument_id.symbol.inner())
            .ok_or_else(|| {
                OKXWsError::ClientError(format!(
                    "No instIdCode cached for {instrument_id}, cannot cancel order"
                ))
            })?;
        builder.inst_id_code(inst_id_code);

        if let Some(venue_order_id) = venue_order_id {
            builder.ord_id(venue_order_id.as_str());
        }

        if let Some(client_order_id) = client_order_id {
            builder.cl_ord_id(client_order_id.as_str());
        }

        let params = builder
            .build()
            .map_err(|e| OKXWsError::ClientError(format!("Build cancel params error: {e}")))?;

        let request_id = self.generate_unique_request_id();
        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: super::enums::OKXWsOperation::CancelOrder,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize cancel: {e}")))?;
        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_cancel(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            request_id.clone(),
            connection_epoch,
        )?;

        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_CANCEL.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id,
            op: Some(super::enums::OKXWsOperation::CancelOrder),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;

        if result.is_err() {
            self.pending_operations
                .remove_unsent(&OKXWsOperation::CancelOrder, &request_id);
        }

        result
    }

    /// Mass cancels all orders for a given instrument via WebSocket.
    ///
    /// # Errors
    ///
    /// Returns an error if instrument metadata cannot be resolved or if the
    /// cancel request fails to send.
    ///
    /// # References
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-mass-cancel-order>
    pub async fn mass_cancel_orders(&self, instrument_id: InstrumentId) -> Result<(), OKXWsError> {
        let (inst_type, inst_family) = {
            let instrument = self
                .instruments_cache
                .get_cloned(&instrument_id.symbol.inner())
                .ok_or_else(|| {
                    OKXWsError::ClientError(format!("Unknown instrument {instrument_id}"))
                })?;

            let inst_type = okx_instrument_type(&instrument)
                .map_err(|e| OKXWsError::ClientError(e.to_string()))?;

            let symbol = instrument.symbol().inner();
            let inst_family = match &instrument {
                InstrumentAny::CurrencyPair(_) => symbol.as_str().to_string(),
                InstrumentAny::CryptoPerpetual(_) => symbol
                    .as_str()
                    .strip_suffix("-SWAP")
                    .unwrap_or(symbol.as_str())
                    .to_string(),
                InstrumentAny::CryptoFuture(_) => {
                    let s = symbol.as_str();
                    if let Some(idx) = s.rfind('-') {
                        s[..idx].to_string()
                    } else {
                        s.to_string()
                    }
                }
                _ => {
                    return Err(OKXWsError::ClientError(
                        "Unsupported instrument type for mass cancel".to_string(),
                    ));
                }
            };

            (inst_type, inst_family)
        };

        let params = WsMassCancelParams {
            inst_type,
            inst_family: Ustr::from(&inst_family),
        };

        let request_id = self.generate_unique_request_id();
        let operation = OKXWsOperation::MassCancel;
        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: operation.clone(),
            exp_time: None,
            args: vec![
                serde_json::to_value(params).map_err(|e| OKXWsError::JsonError(e.to_string()))?,
            ],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize mass cancel: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_aggregate(
            operation.clone(),
            request_id.clone(),
            connection_epoch,
        )?;
        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_MASS_CANCEL.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id: None,
            op: Some(operation.clone()),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;
        if result.is_err() {
            self.pending_operations
                .remove_unsent(&operation, &request_id);
        }
        result
    }

    /// Submits multiple orders.
    ///
    /// # Errors
    ///
    /// Returns an error if any batch order parameters are invalid or if the
    /// batch request fails to send.
    #[expect(clippy::type_complexity)]
    pub async fn batch_submit_orders(
        &self,
        orders: Vec<(
            OKXInstrumentType,
            InstrumentId,
            OKXTradeMode,
            ClientOrderId,
            OrderSide,
            Option<PositionSide>,
            OrderType,
            Quantity,
            Option<Price>,
            Option<Price>,
            Option<bool>,
            Option<bool>,
            Option<String>,
            Option<String>,
            Option<bool>,
            Option<bool>,
            Option<bool>,
        )>,
    ) -> Result<(), OKXWsError> {
        validate_batch_size("batch submit orders", orders.len())?;
        let args: Vec<Value> = {
            let mut args = Vec::with_capacity(orders.len());
            let inst_id_codes = self.inst_id_code_cache.load();
            let instruments = self.instruments_cache.load();

            for (
                inst_type,
                inst_id,
                td_mode,
                cl_ord_id,
                ord_side,
                pos_side,
                ord_type,
                qty,
                pr,
                tp,
                post_only,
                reduce_only,
                speed_bump,
                outcome,
                rpi,
                rpi_taker_access,
                rpi_px_round,
            ) in orders
            {
                let rpi = rpi.unwrap_or(false);
                let mut builder = WsPostOrderParamsBuilder::default();

                let (inst_id_symbol, inst_id_code) = Self::inst_id_symbol_and_code_from_snapshot(
                    &inst_id_codes,
                    &inst_id,
                    "submit",
                )?;
                builder.inst_id_code(inst_id_code);

                builder.td_mode(td_mode);
                builder.cl_ord_id(cl_ord_id.as_str());
                builder.side(ord_side.as_specified());

                if inst_type != OKXInstrumentType::Events
                    && let Some(instrument) = instruments.get(&inst_id_symbol)
                {
                    builder.ccy(instrument.quote_currency().to_string());
                }

                if let Some(ps) = pos_side {
                    builder.pos_side(OKXPositionSide::from(ps));
                } else if matches!(
                    inst_type,
                    OKXInstrumentType::Swap
                        | OKXInstrumentType::Futures
                        | OKXInstrumentType::Option
                ) {
                    builder.pos_side(OKXPositionSide::Net);
                }

                if rpi && ord_type != OrderType::Limit {
                    return Err(OKXWsError::ClientError(
                        "OKX RPI batch orders require limit orders".to_string(),
                    ));
                }

                let okx_ord_type = if rpi {
                    OKXOrderType::Rpi
                } else if post_only.unwrap_or(false) {
                    OKXOrderType::PostOnly
                } else {
                    match ord_type {
                        OrderType::Market => OKXOrderType::Market,
                        OrderType::Limit => OKXOrderType::Limit,
                        OrderType::MarketToLimit => OKXOrderType::Ioc,
                        _ => {
                            return Err(OKXWsError::ClientError(format!(
                                "Unsupported order type for batch submit: {ord_type:?}"
                            )));
                        }
                    }
                };

                builder.ord_type(okx_ord_type);
                builder.sz(qty.to_string());

                if let Some(p) = pr {
                    builder.px(p.to_string());
                } else if let Some(p) = tp {
                    builder.px(p.to_string());
                }

                if let Some(ro) = reduce_only {
                    builder.reduce_only(ro);
                }

                let speed_bump = if inst_type == OKXInstrumentType::Events {
                    if outcome.is_none() {
                        return Err(OKXWsError::ClientError(
                            "OKX event contract orders require `outcome`".to_string(),
                        ));
                    }

                    if okx_ord_type == OKXOrderType::PostOnly {
                        speed_bump
                    } else {
                        Some(speed_bump.unwrap_or_else(|| "1".to_string()))
                    }
                } else {
                    speed_bump
                };

                if let Some(speed_bump) = speed_bump {
                    builder.speed_bump(speed_bump);
                }

                if let Some(outcome) = outcome {
                    builder.outcome(outcome);
                }

                if let Some(rpi_taker_access) = rpi_taker_access {
                    builder.rpi_taker_access(rpi_taker_access);
                }

                if let Some(rpi_px_round) = rpi_px_round {
                    builder.rpi_px_round(rpi_px_round);
                }

                builder.tag(OKX_NAUTILUS_BROKER_ID);

                let params = builder.build().map_err(|e| {
                    OKXWsError::ClientError(format!("Build order params error: {e}"))
                })?;
                let val = serde_json::to_value(params)
                    .map_err(|e| OKXWsError::JsonError(e.to_string()))?;
                args.push(val);
            }
            args
        };

        self.ws_batch_place_orders(args).await
    }

    /// Modifies multiple orders.
    ///
    /// # Errors
    ///
    /// Returns an error if amend parameters are invalid or if the batch request
    /// fails to send.
    #[expect(clippy::type_complexity)]
    pub async fn batch_modify_orders(
        &self,
        orders: Vec<(
            OKXInstrumentType,
            InstrumentId,
            ClientOrderId,
            Option<String>,
            Option<Price>,
            Option<Quantity>,
            Option<String>,
            Option<bool>,
            Option<bool>,
        )>,
    ) -> Result<(), OKXWsError> {
        validate_batch_size("batch modify orders", orders.len())?;
        let args: Vec<Value> = {
            let mut args = Vec::with_capacity(orders.len());
            let inst_id_codes = self.inst_id_code_cache.load();

            for (
                _inst_type,
                inst_id,
                cl_ord_id,
                request_id,
                pr,
                sz,
                speed_bump,
                rpi_taker_access,
                rpi_px_round,
            ) in orders
            {
                let mut builder = WsAmendOrderParamsBuilder::default();

                let (_, inst_id_code) =
                    Self::inst_id_symbol_and_code_from_snapshot(&inst_id_codes, &inst_id, "amend")?;
                builder.inst_id_code(inst_id_code);

                builder.cl_ord_id(cl_ord_id.as_str());

                if let Some(request_id) = request_id {
                    builder.req_id(request_id);
                }

                if let Some(p) = pr {
                    builder.new_px(p.to_string());
                }

                if let Some(q) = sz {
                    builder.new_sz(q.to_string());
                }

                if let Some(speed_bump) = speed_bump {
                    builder.speed_bump(speed_bump);
                }

                if let Some(rpi_taker_access) = rpi_taker_access {
                    builder.rpi_taker_access(rpi_taker_access);
                }

                if let Some(rpi_px_round) = rpi_px_round {
                    builder.rpi_px_round(rpi_px_round);
                }

                let params = builder.build().map_err(|e| {
                    OKXWsError::ClientError(format!("Build amend batch params error: {e}"))
                })?;
                let val = serde_json::to_value(params)
                    .map_err(|e| OKXWsError::JsonError(e.to_string()))?;
                args.push(val);
            }
            args
        };

        self.ws_batch_amend_orders(args).await
    }

    /// Cancels multiple orders.
    ///
    /// Supports up to 20 orders per batch.
    ///
    /// # Errors
    ///
    /// Returns an error if cancel parameters are invalid or if the batch
    /// request fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-websocket-batch-cancel-orders>
    pub async fn batch_cancel_orders(
        &self,
        orders: Vec<(InstrumentId, Option<ClientOrderId>, Option<VenueOrderId>)>,
    ) -> Result<(), OKXWsError> {
        validate_batch_size("batch cancel orders", orders.len())?;
        let args: Vec<Value> = {
            let mut args = Vec::with_capacity(orders.len());
            let inst_id_codes = self.inst_id_code_cache.load();

            for (inst_id, cl_ord_id, ord_id) in orders {
                let mut builder = WsCancelOrderParamsBuilder::default();

                let (_, inst_id_code) = Self::inst_id_symbol_and_code_from_snapshot(
                    &inst_id_codes,
                    &inst_id,
                    "cancel",
                )?;
                builder.inst_id_code(inst_id_code);

                if let Some(c) = cl_ord_id {
                    builder.cl_ord_id(c.as_str());
                }

                if let Some(o) = ord_id {
                    builder.ord_id(o.as_str());
                }

                let params = builder.build().map_err(|e| {
                    OKXWsError::ClientError(format!("Build cancel batch params error: {e}"))
                })?;
                let val = serde_json::to_value(params)
                    .map_err(|e| OKXWsError::JsonError(e.to_string()))?;
                args.push(val);
            }
            args
        };

        self.ws_batch_cancel_orders(args).await
    }

    /// Submits an algo order (conditional/stop order).
    ///
    /// # Errors
    ///
    /// Returns an error if the order parameters are invalid or if the request
    /// cannot be sent.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-place-algo-order>
    #[expect(clippy::too_many_arguments)]
    pub async fn submit_algo_order(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        td_mode: OKXTradeMode,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        trigger_price: Option<Price>,
        trigger_type: Option<TriggerType>,
        limit_price: Option<Price>,
        reduce_only: Option<bool>,
        callback_ratio: Option<String>,
        callback_spread: Option<String>,
        activation_price: Option<Price>,
    ) -> Result<(), OKXWsError> {
        if !is_conditional_order(order_type) {
            return Err(OKXWsError::ClientError(format!(
                "Order type {order_type:?} is not a conditional order"
            )));
        }

        let mut builder = WsPostAlgoOrderParamsBuilder::default();

        if !matches!(order_side, OrderSide::Buy | OrderSide::Sell) {
            return Err(OKXWsError::ClientError(
                "Invalid order side for OKX".to_string(),
            ));
        }

        let inst_id_code = self
            .get_inst_id_code(&instrument_id.symbol.inner())
            .ok_or_else(|| {
                OKXWsError::ClientError(format!(
                    "No instIdCode cached for {instrument_id}, cannot submit algo order"
                ))
            })?;
        builder.inst_id_code(inst_id_code);

        builder.td_mode(td_mode);
        builder.cl_ord_id(client_order_id.as_str());
        builder.side(order_side.as_specified());
        builder.ord_type(
            conditional_order_to_algo_type(order_type)
                .map_err(|e| OKXWsError::ClientError(e.to_string()))?,
        );
        builder.sz(quantity.to_string());

        if let Some(tp) = trigger_price {
            builder.trigger_px(tp.to_string());
        }

        // Map Nautilus TriggerType to OKX trigger type
        let okx_trigger_type = trigger_type.map_or(OKXTriggerType::Last, Into::into);
        builder.trigger_px_type(okx_trigger_type);

        // For stop-limit orders, set the limit price
        if matches!(order_type, OrderType::StopLimit | OrderType::LimitIfTouched)
            && let Some(price) = limit_price
        {
            builder.order_px(price.to_string());
        }

        if let Some(reduce) = reduce_only {
            builder.reduce_only(reduce);
        }

        if let Some(ratio) = callback_ratio {
            builder.callback_ratio(ratio);
        }

        if let Some(spread) = callback_spread {
            builder.callback_spread(spread);
        }

        if let Some(active) = activation_price {
            builder.active_px(active.to_string());
        }

        builder.tag(OKX_NAUTILUS_BROKER_ID);

        let params = builder
            .build()
            .map_err(|e| OKXWsError::ClientError(format!("Build algo order params error: {e}")))?;

        let request_id = self.generate_unique_request_id();
        let operation = OKXWsOperation::OrderAlgo;
        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: operation.clone(),
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize algo order: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_routed(
            operation.clone(),
            trader_id,
            strategy_id,
            instrument_id,
            Some(client_order_id),
            request_id.clone(),
            connection_epoch,
        )?;
        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_ALGO_ORDER.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id: Some(client_order_id),
            op: Some(operation.clone()),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;
        if result.is_err() {
            self.pending_operations
                .remove_unsent(&operation, &request_id);
        }
        result
    }

    /// Cancels an algo order.
    ///
    /// # Errors
    ///
    /// Returns an error if cancel parameters are invalid or if the request
    /// fails to send.
    ///
    /// # References
    ///
    /// <https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-cancel-algo-order>
    pub async fn cancel_algo_order(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        algo_order_id: Option<String>,
    ) -> Result<(), OKXWsError> {
        let mut builder = super::messages::WsCancelAlgoOrderParamsBuilder::default();

        let inst_id_code = self
            .get_inst_id_code(&instrument_id.symbol.inner())
            .ok_or_else(|| {
                OKXWsError::ClientError(format!(
                    "No instIdCode cached for {instrument_id}, cannot cancel algo order"
                ))
            })?;
        builder.inst_id_code(inst_id_code);

        if let Some(algo_id) = algo_order_id {
            builder.algo_id(algo_id);
        }

        if let Some(cl_ord_id) = client_order_id {
            builder.algo_cl_ord_id(cl_ord_id.to_string());
        }

        let params = builder
            .build()
            .map_err(|e| OKXWsError::ClientError(format!("Build cancel algo params error: {e}")))?;

        let request_id = self.generate_unique_request_id();
        let operation = OKXWsOperation::CancelAlgos;
        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: operation.clone(),
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| OKXWsError::JsonError(format!("Failed to serialize cancel algo: {e}")))?;

        let connection_epoch = self.current_connection_epoch();
        self.pending_operations.admit_routed(
            operation.clone(),
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            request_id.clone(),
            connection_epoch,
        )?;
        let cmd = HandlerCommand::Send {
            payload,
            rate_limit_keys: Some(OKX_RATE_LIMIT_KEY_ALGO_CANCEL.to_vec()),
            request_id: Some(request_id.clone()),
            client_order_id,
            op: Some(operation.clone()),
            connection_epoch,
        };

        let result = self.send_cmd(cmd).await;
        if result.is_err() {
            self.pending_operations
                .remove_unsent(&operation, &request_id);
        }
        result
    }

    /// Sends a command to the handler.
    async fn send_cmd(&self, cmd: HandlerCommand) -> Result<(), OKXWsError> {
        let sender = self.cmd_tx.read().await;
        try_send_handler_command(&sender, &self.queue_failure, cmd, "handler command")
    }
}

fn try_send_handler_command(
    sender: &tokio::sync::mpsc::Sender<HandlerCommand>,
    queue_failure: &OKXWebSocketQueueFailureGuard,
    command: HandlerCommand,
    context: &str,
) -> Result<(), OKXWsError> {
    if let HandlerCommand::Send { payload, .. } | HandlerCommand::Authenticate { payload } =
        &command
        && payload.len() > OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE
    {
        return Err(OKXWsError::ClientError(format!(
            "Serialized OKX WebSocket {context} is {} bytes, limit is {} bytes",
            payload.len(),
            OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE
        )));
    }

    queue_failure
        .try_send(OKXWebSocketQueue::Command, sender, command)
        .map_err(|kind| {
            OKXWsError::ClientError(format!(
                "Failed to enqueue {context}: command queue {kind:?}"
            ))
        })
}

fn validate_batch_size(operation: &str, len: usize) -> Result<(), OKXWsError> {
    if !(1..=OKX_WS_MAX_BATCH_SIZE).contains(&len) {
        return Err(OKXWsError::ClientError(format!(
            "OKX {operation} contains {len} items; allowed range is 1..={OKX_WS_MAX_BATCH_SIZE}"
        )));
    }
    Ok(())
}

fn okx_raw_message_channel(
    capacity: usize,
    max_message_size: usize,
    byte_budget: usize,
    queue_failure: OKXWebSocketQueueFailureGuard,
) -> (
    nautilus_network::websocket::MessageHandler,
    tokio::sync::mpsc::Receiver<OKXBudgetedMessage<tokio_tungstenite::tungstenite::Message>>,
) {
    assert!(capacity > 0, "OKX raw queue capacity must be non-zero");
    assert!(max_message_size > 0, "OKX message limit must be non-zero");
    assert!(
        byte_budget >= max_message_size.saturating_mul(OKX_WS_INGRESS_MEMORY_WEIGHT),
        "OKX ingress budget must admit one maximum-size weighted message"
    );
    assert!(
        u32::try_from(byte_budget).is_ok(),
        "OKX ingress budget exceeds semaphore range"
    );

    let permits = Arc::new(tokio::sync::Semaphore::new(byte_budget));
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    let handler: nautilus_network::websocket::MessageHandler = Arc::new(move |message| {
        if queue_failure.is_failed() {
            return;
        }

        let payload_len = message.as_bytes().len();
        if payload_len > max_message_size {
            queue_failure.fail(
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::Oversized,
            );
            return;
        }

        let weighted_bytes = payload_len
            .max(1)
            .saturating_mul(OKX_WS_INGRESS_MEMORY_WEIGHT);
        let Ok(weighted_bytes) = u32::try_from(weighted_bytes) else {
            queue_failure.fail(
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::Oversized,
            );
            return;
        };
        let permit = match Arc::clone(&permits).try_acquire_many_owned(weighted_bytes) {
            Ok(permit) => permit,
            Err(_) => {
                queue_failure.fail(
                    OKXWebSocketQueue::Raw,
                    OKXWebSocketQueueFailureKind::ByteBudgetExhausted,
                );
                return;
            }
        };

        let legacy = match tokio_tungstenite::tungstenite::Message::try_from(message) {
            Ok(message) => message,
            Err(_) => {
                queue_failure.fail(
                    OKXWebSocketQueue::Raw,
                    OKXWebSocketQueueFailureKind::Conversion,
                );
                return;
            }
        };
        if let Err(error) = tx.try_send(OKXBudgetedMessage::budgeted(legacy, permit)) {
            let kind = match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    OKXWebSocketQueueFailureKind::Full
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    OKXWebSocketQueueFailureKind::Closed
                }
            };
            queue_failure.fail(OKXWebSocketQueue::Raw, kind);
        }
    });
    (handler, rx)
}

fn ws_channel_for_book(channel: OKXBookChannel) -> OKXWsChannel {
    match channel {
        OKXBookChannel::Book => OKXWsChannel::Books,
        OKXBookChannel::BookL2Tbt => OKXWsChannel::BooksTbt,
        OKXBookChannel::Books50L2Tbt => OKXWsChannel::Books50Tbt,
        OKXBookChannel::BooksRpi => OKXWsChannel::BooksRpi,
        OKXBookChannel::SprdBooks5 => OKXWsChannel::SprdBooks5,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_model::instruments::stubs::crypto_perpetual_ethusdt;
    use nautilus_network::RECONNECTED;
    use rstest::rstest;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::{
        common::{
            consts::OKX_POST_ONLY_CANCEL_SOURCE,
            enums::{
                OKXExecType, OKXOrderCategory, OKXOrderStatus, OKXPriceType, OKXQuickMarginType,
                OKXSelfTradePreventionMode, OKXSide,
            },
        },
        websocket::{
            enums::OKXWsOperation,
            handler::is_post_only_auto_cancel,
            messages::{OKXOrderMsg, OKXWebSocketError, OKXWsFrame},
        },
    };

    type QueueFailureRecord = (OKXWebSocketQueue, OKXWebSocketQueueFailureKind);
    type QueueFailureTestGuard = (
        Arc<AtomicBool>,
        OKXWebSocketQueueFailureGuard,
        Arc<Mutex<Vec<QueueFailureRecord>>>,
    );

    fn queue_failure_test_guard() -> QueueFailureTestGuard {
        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let guard = OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), notify);
        let failures = Arc::new(Mutex::new(Vec::new()));
        let callback_failures = Arc::clone(&failures);
        guard.set_callback(Arc::new(move |queue, kind| {
            callback_failures.lock().unwrap().push((queue, kind));
        }));
        (signal, guard, failures)
    }

    fn pending_test_ids() -> (TraderId, StrategyId, InstrumentId, ClientOrderId) {
        (
            TraderId::from("TRADER-001"),
            StrategyId::from("STRATEGY-001"),
            InstrumentId::from("BTC-USDT-SWAP.OKX"),
            ClientOrderId::from("O-001"),
        )
    }

    #[rstest]
    fn every_trading_operation_is_pending_tracked() {
        for operation in [
            OKXWsOperation::Order,
            OKXWsOperation::BatchOrders,
            OKXWsOperation::OrderAlgo,
            OKXWsOperation::CancelOrder,
            OKXWsOperation::BatchCancelOrders,
            OKXWsOperation::MassCancel,
            OKXWsOperation::CancelAlgos,
            OKXWsOperation::AmendOrder,
            OKXWsOperation::BatchAmendOrders,
        ] {
            assert!(
                pending_kind_for_operation(&operation).is_some(),
                "trading operation escaped pending tracking: {operation:?}"
            );
        }
        assert!(pending_kind_for_operation(&OKXWsOperation::Subscribe).is_none());
        assert!(pending_kind_for_operation(&OKXWsOperation::Unsubscribe).is_none());
    }

    #[rstest]
    fn pending_capacity_is_shared_across_order_cancel_and_amend() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 2, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();

        pending
            .admit_order(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                "request-order".to_string(),
                7,
            )
            .unwrap();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-cancel".to_string(),
                7,
            )
            .unwrap();

        let error = pending
            .admit_amend(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-amend".to_string(),
                7,
            )
            .unwrap_err();

        assert!(error.to_string().contains("capacity 2 exhausted"));
        assert_eq!(
            pending.len(),
            2,
            "capacity failure must not evict an older request"
        );
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingCapacityExhausted,
            ))
        );
    }

    #[rstest]
    fn pending_timeout_halts_without_silent_deletion() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::ZERO);
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_order(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                "request-expired".to_string(),
                11,
            )
            .unwrap();

        assert!(!pending.enforce_health(11));
        assert_eq!(
            pending.len(),
            1,
            "expired request remains for reconciliation"
        );
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingExpired,
            ))
        );
    }

    #[rstest]
    fn pending_stale_epoch_halts_without_silent_deletion() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-stale".to_string(),
                3,
            )
            .unwrap();

        assert!(!pending.enforce_health(4));
        assert_eq!(pending.len(), 1, "stale request remains for reconciliation");
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingStaleConnection,
            ))
        );
    }

    #[rstest]
    fn authenticated_terminal_order_update_resolves_placement_and_cancel() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 2, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_order(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                "request-order-stream".to_string(),
                13,
            )
            .unwrap();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-cancel-stream".to_string(),
                13,
            )
            .unwrap();

        pending.observe_order_update(client_order_id.as_str(), OKXOrderStatus::Canceled, 13);

        assert_eq!(pending.len(), 0);
        assert_eq!(pending.observed_len(), 2);
        assert_eq!(pending.slots.available_permits(), 2);
        assert_eq!(guard.failure(), None);

        let placed = pending
            .resolve(
                &OKXWsOperation::Order,
                Some("request-order-stream"),
                Some(client_order_id.as_str()),
                13,
            )
            .expect("late placement response matches the order-channel observation");
        let canceled = pending
            .resolve(
                &OKXWsOperation::CancelOrder,
                Some("request-cancel-stream"),
                Some(client_order_id.as_str()),
                13,
            )
            .expect("late cancel response matches the terminal order-channel observation");
        assert!(placed.capacity_permit.is_none());
        assert!(canceled.capacity_permit.is_none());
        assert_eq!(pending.observed_len(), 0);
        assert_eq!(guard.failure(), None);
    }

    #[rstest]
    fn live_order_update_resolves_placement_but_not_cancel() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 2, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_order(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                "request-live-order".to_string(),
                17,
            )
            .unwrap();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-live-cancel".to_string(),
                17,
            )
            .unwrap();

        pending.observe_order_update(client_order_id.as_str(), OKXOrderStatus::Live, 17);

        assert_eq!(
            pending.len(),
            1,
            "a live state is not proof of cancellation"
        );
        assert_eq!(pending.observed_len(), 1);
        assert_eq!(pending.cancels.len(), 1);
        assert_eq!(pending.orders.len(), 0);
        assert_eq!(guard.failure(), None);
    }

    #[rstest]
    fn stale_order_update_does_not_resolve_current_pending_operation() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-current-cancel".to_string(),
                21,
            )
            .unwrap();

        pending.observe_order_update(client_order_id.as_str(), OKXOrderStatus::Canceled, 22);

        assert_eq!(pending.len(), 1);
        assert_eq!(pending.observed_len(), 0);
        assert_eq!(guard.failure(), None);
        assert!(!pending.enforce_health(22));
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingStaleConnection,
            ))
        );
    }

    #[rstest]
    fn duplicate_pending_cancel_for_one_order_is_not_stream_resolved() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 2, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        for request_id in ["request-cancel-first", "request-cancel-second"] {
            pending
                .admit_cancel(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    Some(client_order_id),
                    request_id.to_string(),
                    25,
                )
                .unwrap();
        }

        pending.observe_order_update(client_order_id.as_str(), OKXOrderStatus::Canceled, 25);

        assert_eq!(pending.len(), 2);
        assert_eq!(pending.observed_len(), 0);
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[rstest]
    fn observed_resolution_expires_without_failing_connection_health() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::ZERO);
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-observed-expired".to_string(),
                27,
            )
            .unwrap();

        pending.observe_order_update(client_order_id.as_str(), OKXOrderStatus::Canceled, 27);
        assert_eq!(pending.len(), 0);
        assert_eq!(pending.observed_len(), 1);
        assert!(pending.enforce_health(27));
        assert_eq!(pending.observed_len(), 0);
        assert_eq!(guard.failure(), None);
    }

    #[rstest]
    fn observed_venue_result_wins_over_late_asynchronous_send_failure() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_cancel(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-observed-send".to_string(),
                28,
            )
            .unwrap();
        pending.observe_order_update(client_order_id.as_str(), OKXOrderStatus::Canceled, 28);

        pending.mark_send_failed(
            &OKXWsOperation::CancelOrder,
            "request-observed-send",
            Some(client_order_id),
        );

        assert_eq!(pending.len(), 0);
        assert_eq!(pending.observed_len(), 1);
        assert_eq!(guard.failure(), None);
    }

    #[rstest]
    fn wrong_request_id_cannot_swallow_pending_ack() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_order(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                "request-right".to_string(),
                19,
            )
            .unwrap();

        assert!(
            pending
                .resolve_order(Some("request-wrong"), Some(client_order_id.as_str()), 19)
                .is_none()
        );
        assert_eq!(
            pending.len(),
            1,
            "unmatched ACK must not remove pending state"
        );
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[rstest]
    fn exact_request_id_client_id_and_epoch_resolve_once() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_amend(
                trader_id,
                strategy_id,
                instrument_id,
                Some(client_order_id),
                "request-exact".to_string(),
                23,
            )
            .unwrap();

        let resolved = pending
            .resolve_amend(Some("request-exact"), Some(client_order_id.as_str()), 23)
            .expect("exact response resolves pending operation");

        assert_eq!(resolved.client_order_id, Some(client_order_id));
        assert_eq!(pending.len(), 0);
        drop(resolved);
        assert_eq!(pending.slots.available_permits(), 1);
        assert_eq!(guard.failure(), None);
    }

    #[rstest]
    fn aggregate_request_resolves_once_without_single_order_routing() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        pending
            .admit_aggregate(OKXWsOperation::BatchOrders, "request-batch".to_string(), 31)
            .unwrap();

        let resolved = pending
            .resolve(
                &OKXWsOperation::BatchOrders,
                Some("request-batch"),
                Some("ORDER-001"),
                31,
            )
            .expect("exact aggregate response resolves once");

        assert!(resolved.routing.is_none());
        assert_eq!(resolved.client_order_id, None);
        assert_eq!(pending.len(), 0);
        drop(resolved);
        assert_eq!(pending.slots.available_permits(), 1);
        assert_eq!(guard.failure(), None);
    }

    #[rstest]
    fn wrong_aggregate_operation_cannot_swallow_pending_ack() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        pending
            .admit_aggregate(
                OKXWsOperation::BatchOrders,
                "request-batch-op".to_string(),
                37,
            )
            .unwrap();

        assert!(
            pending
                .resolve(
                    &OKXWsOperation::OrderAlgo,
                    Some("request-batch-op"),
                    None,
                    37,
                )
                .is_none()
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[rstest]
    fn old_epoch_aggregate_ack_is_retained_and_halts() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        pending
            .admit_aggregate(
                OKXWsOperation::BatchCancelOrders,
                "request-batch-epoch".to_string(),
                41,
            )
            .unwrap();

        assert!(
            pending
                .resolve(
                    &OKXWsOperation::BatchCancelOrders,
                    Some("request-batch-epoch"),
                    None,
                    42,
                )
                .is_none()
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingStaleConnection,
            ))
        );
    }

    #[rstest]
    fn aggregate_send_failure_retains_pending_and_halts() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        pending
            .admit_aggregate(
                OKXWsOperation::BatchAmendOrders,
                "request-batch-send".to_string(),
                43,
            )
            .unwrap();

        pending.mark_send_failed(
            &OKXWsOperation::BatchAmendOrders,
            "request-batch-send",
            None,
        );

        assert_eq!(pending.len(), 1);
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[rstest]
    fn asynchronous_send_failure_retains_pending_and_halts() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let pending = OKXPendingOperations::with_limits(guard.clone(), 1, Duration::from_secs(30));
        let (trader_id, strategy_id, instrument_id, client_order_id) = pending_test_ids();
        pending
            .admit_order(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                "request-ambiguous".to_string(),
                29,
            )
            .unwrap();

        pending.mark_send_failed(
            &OKXWsOperation::Order,
            "request-ambiguous",
            Some(client_order_id),
        );

        assert_eq!(
            pending.len(),
            1,
            "ambiguous send remains pending for reconciliation"
        );
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            ))
        );
    }

    #[tokio::test]
    #[cfg(feature = "python")]
    async fn detached_python_style_runtime_handle_exits_without_owning_task_owner() {
        let client = OKXWebSocketClient::default();
        let weak_owner = Arc::downgrade(&client.task_owner);
        let runtime_handle = client.runtime_handle();
        let consumer_clone = runtime_handle.clone();
        let signal = Arc::clone(&client.signal);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let consumer = tokio::spawn(async move {
            let _ = ready_tx.send(());
            while consumer_clone.task_owner.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        });

        assert_eq!(Arc::strong_count(&client.task_owner), 1);
        ready_rx.await.unwrap();
        drop(client);

        tokio::time::timeout(Duration::from_secs(1), consumer)
            .await
            .expect("Python-style consumer retained the external TaskOwner")
            .expect("Python-style consumer task panicked");
        assert!(weak_owner.upgrade().is_none());
        assert!(signal.load(Ordering::Acquire));
        assert!(runtime_handle.task_owner.upgrade().is_none());
    }

    #[rstest]
    fn double_connect_attempt_is_rejected() {
        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(signal, notify));
        let _first = owner.begin_connect().unwrap();

        let error = owner
            .begin_connect()
            .err()
            .expect("second connection attempt must be rejected");

        assert!(error.to_string().contains("already has an active"));
    }

    #[tokio::test]
    async fn task_exit_before_install_cannot_resurrect_connecting_generation() {
        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(signal, notify));
        let mut attempt = owner.begin_connect().unwrap();
        let generation = attempt.generation();
        let weak_owner = Arc::downgrade(&owner);
        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _exit_guard = OKXWebSocketTaskExitGuard {
                owner: weak_owner,
                generation,
            };
            let _ = exited_tx.send(());
        });
        exited_rx.await.unwrap();
        tokio::task::yield_now().await;

        let error = attempt.install_task(handle).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("exited before task installation")
        );
        assert!(matches!(*owner.lock_phase(), OKXWebSocketTaskPhase::Idle));
        attempt.rollback().await;
    }

    #[tokio::test]
    async fn failed_setup_rollback_aborts_task_and_allows_fresh_generation() {
        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(
            Arc::clone(&signal),
            Arc::clone(&notify),
        ));
        let mut attempt = owner.begin_connect().unwrap();
        let handle = tokio::spawn(std::future::pending::<()>());
        attempt.install_task(handle).unwrap();

        // This is the common rollback exercised by both failed SetClient enqueue and failed auth.
        attempt.rollback().await;

        assert!(signal.load(Ordering::Acquire));
        assert!(matches!(*owner.lock_phase(), OKXWebSocketTaskPhase::Idle));
        assert!(owner.begin_connect().is_ok());
    }

    #[tokio::test]
    async fn exited_old_generation_cannot_clear_new_running_task() {
        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(signal, notify));
        let mut first = owner.begin_connect().unwrap();
        let first_generation = first.generation();
        first
            .install_task(tokio::spawn(std::future::pending::<()>()))
            .unwrap();
        first.commit().unwrap();
        first.rollback().await;

        let mut second = owner.begin_connect().unwrap();
        let second_generation = second.generation();
        second
            .install_task(tokio::spawn(std::future::pending::<()>()))
            .unwrap();
        second.commit().unwrap();

        owner.task_exited(first_generation);

        assert!(owner.is_running(second_generation));
        second.rollback().await;
    }

    #[tokio::test]
    async fn stale_rollback_cannot_signal_or_abort_new_running_generation() {
        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(Arc::clone(&signal), notify));
        let mut first = owner.begin_connect().unwrap();
        let first_generation = first.generation();
        let weak_owner = Arc::downgrade(&owner);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        first
            .install_task(tokio::spawn(async move {
                let _exit_guard = OKXWebSocketTaskExitGuard {
                    owner: weak_owner,
                    generation: first_generation,
                };
                let _ = release_rx.await;
            }))
            .unwrap();
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(*owner.lock_phase(), OKXWebSocketTaskPhase::Idle) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first generation did not exit");

        let mut second = owner.begin_connect().unwrap();
        let second_generation = second.generation();
        second
            .install_task(tokio::spawn(std::future::pending::<()>()))
            .unwrap();
        second.commit().unwrap();

        first.rollback().await;

        assert!(!signal.load(Ordering::Acquire));
        assert!(owner.is_running(second_generation));
        second.rollback().await;
    }

    #[tokio::test]
    async fn stale_attempt_drop_cannot_signal_or_abort_new_running_generation() {
        let signal = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(OKXWebSocketTaskOwner::new(
            Arc::clone(&signal),
            Arc::new(tokio::sync::Notify::new()),
        ));
        let first = owner.begin_connect().unwrap();
        let first_generation = first.generation();
        let weak_owner = Arc::downgrade(&owner);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        first
            .install_task(tokio::spawn(async move {
                let _exit_guard = OKXWebSocketTaskExitGuard {
                    owner: weak_owner,
                    generation: first_generation,
                };
                let _ = release_rx.await;
            }))
            .unwrap();
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(*owner.lock_phase(), OKXWebSocketTaskPhase::Idle) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first generation did not exit");

        let mut second = owner.begin_connect().unwrap();
        let second_generation = second.generation();
        second
            .install_task(tokio::spawn(std::future::pending::<()>()))
            .unwrap();
        second.commit().unwrap();

        drop(first);

        assert!(!signal.load(Ordering::Acquire));
        assert!(owner.is_running(second_generation));
        second.rollback().await;
    }

    #[tokio::test]
    async fn last_external_owner_drop_signals_notifies_and_aborts_running_task() {
        struct DropProbe(Arc<AtomicBool>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(
            Arc::clone(&signal),
            Arc::clone(&notify),
        ));
        let weak_owner = Arc::downgrade(&owner);
        let mut attempt = owner.begin_connect().unwrap();
        let generation = attempt.generation();
        let task_dropped = Arc::new(AtomicBool::new(false));
        let task_probe = Arc::clone(&task_dropped);
        let handle = tokio::spawn(async move {
            let _exit_guard = OKXWebSocketTaskExitGuard {
                owner: weak_owner,
                generation,
            };
            let _drop_probe = DropProbe(task_probe);
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        attempt.install_task(handle).unwrap();
        attempt.commit().unwrap();
        drop(attempt);

        let notify_waiter = {
            let notify = Arc::clone(&notify);
            tokio::spawn(async move { notify.notified().await })
        };
        tokio::task::yield_now().await;

        assert_eq!(Arc::strong_count(&owner), 1);
        drop(owner);

        tokio::time::timeout(Duration::from_secs(1), async {
            while !task_dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner drop must abort the detached task");
        tokio::time::timeout(Duration::from_secs(1), notify_waiter)
            .await
            .expect("owner drop must notify shutdown waiters")
            .expect("shutdown waiter task must complete");
        assert!(signal.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn owner_drop_during_cancelled_close_still_aborts_via_closing_abort_handle() {
        struct DropProbe(Arc<AtomicBool>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let signal = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let owner = Arc::new(OKXWebSocketTaskOwner::new(signal, notify));
        let weak_owner = Arc::downgrade(&owner);
        let mut attempt = owner.begin_connect().unwrap();
        let generation = attempt.generation();
        let task_dropped = Arc::new(AtomicBool::new(false));
        let task_probe = Arc::clone(&task_dropped);
        let handle = tokio::spawn(async move {
            let _exit_guard = OKXWebSocketTaskExitGuard {
                owner: weak_owner,
                generation,
            };
            let _drop_probe = DropProbe(task_probe);
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        attempt.install_task(handle).unwrap();
        attempt.commit().unwrap();
        drop(attempt);

        let (_generation, extracted_handle) = owner.begin_close().expect("running task");
        drop(extracted_handle); // Simulate cancellation of close() after it extracted the handle.
        drop(owner);

        tokio::time::timeout(Duration::from_secs(1), async {
            while !task_dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Closing abort handle must remain cancellation-safe");
    }

    #[rstest]
    fn raw_queue_full_is_terminal_and_reported_once() {
        let (signal, guard, failures) = queue_failure_test_guard();
        let (handler, mut receiver) = okx_raw_message_channel(1, 64, 1024, guard.clone());

        handler(nautilus_network::transport::Message::text("first"));
        handler(nautilus_network::transport::Message::text("overflow"));
        handler(nautilus_network::transport::Message::text("rejected"));

        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            guard.failure(),
            Some((OKXWebSocketQueue::Raw, OKXWebSocketQueueFailureKind::Full))
        );
        assert_eq!(failures.lock().unwrap().len(), 1);
        assert!(matches!(
            receiver.try_recv(),
            Ok(OKXBudgetedMessage {
                message: Message::Text(text),
                ..
            }) if text == "first"
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn raw_weighted_budget_is_terminal_on_cumulative_exhaustion() {
        let (signal, guard, failures) = queue_failure_test_guard();
        let (handler, mut receiver) = okx_raw_message_channel(8, 16, 256, guard.clone());

        handler(nautilus_network::transport::Message::text("a".repeat(16)));
        handler(nautilus_network::transport::Message::text("b"));

        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::ByteBudgetExhausted
            ))
        );
        assert_eq!(failures.lock().unwrap().len(), 1);
        assert!(receiver.try_recv().is_ok());
    }

    #[rstest]
    fn high_expansion_json_array_consumes_weighted_budget_before_parse() {
        let (signal, guard, failures) = queue_failure_test_guard();
        let (handler, mut receiver) = okx_raw_message_channel(8, 64, 1024, guard.clone());
        let array_payload = format!("[{}]", vec!["0"; 31].join(","));
        assert_eq!(array_payload.len(), 63);

        handler(nautilus_network::transport::Message::text(array_payload));
        handler(nautilus_network::transport::Message::text("[]"));

        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::ByteBudgetExhausted
            ))
        );
        assert_eq!(failures.lock().unwrap().len(), 1);
        assert!(receiver.try_recv().is_ok());
    }

    #[rstest]
    fn raw_weighted_budget_permit_is_released_after_consumption() {
        let (_signal, guard, _failures) = queue_failure_test_guard();
        let (handler, mut receiver) = okx_raw_message_channel(8, 16, 256, guard.clone());

        handler(nautilus_network::transport::Message::text("a".repeat(16)));
        drop(receiver.try_recv().expect("near-limit message admitted"));
        handler(nautilus_network::transport::Message::text("b".repeat(16)));

        assert!(receiver.try_recv().is_ok());
        assert!(guard.failure().is_none());
    }

    #[rstest]
    fn raw_single_message_limit_is_terminal_before_queue_admission() {
        let (signal, guard, failures) = queue_failure_test_guard();
        let (handler, mut receiver) = okx_raw_message_channel(8, 16, 256, guard.clone());

        handler(nautilus_network::transport::Message::text("x".repeat(17)));

        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::Oversized
            ))
        );
        assert_eq!(failures.lock().unwrap().len(), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn command_queue_full_is_terminal_and_future_commands_are_rejected() {
        let (signal, guard, failures) = queue_failure_test_guard();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        try_send_handler_command(&sender, &guard, HandlerCommand::Disconnect, "first").unwrap();
        let overflow =
            try_send_handler_command(&sender, &guard, HandlerCommand::Disconnect, "overflow");
        assert!(overflow.is_err());
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::Command,
                OKXWebSocketQueueFailureKind::Full
            ))
        );

        let _ = receiver.try_recv().unwrap();
        assert!(
            try_send_handler_command(&sender, &guard, HandlerCommand::Disconnect, "future")
                .is_err(),
            "terminal queue failure must remain sticky after capacity becomes available"
        );
        assert_eq!(failures.lock().unwrap().len(), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn oversized_serialized_trading_command_is_rejected_before_enqueue() {
        let (_signal, guard, failures) = queue_failure_test_guard();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let payload = "x".repeat(OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE + 1);

        let result = try_send_handler_command(
            &sender,
            &guard,
            HandlerCommand::Send {
                payload,
                rate_limit_keys: None,
                request_id: Some("oversized".to_string()),
                client_order_id: None,
                op: Some(OKXWsOperation::Order),
                connection_epoch: 0,
            },
            "order command",
        );

        assert!(result.is_err());
        assert!(receiver.try_recv().is_err());
        assert!(guard.failure().is_none());
        assert!(failures.lock().unwrap().is_empty());
    }

    #[rstest]
    #[case(0)]
    #[case(OKX_WS_MAX_BATCH_SIZE + 1)]
    fn invalid_batch_size_is_rejected(#[case] len: usize) {
        let error = validate_batch_size("test batch", len)
            .expect_err("invalid batch size must fail before serialization");
        assert!(error.to_string().contains("allowed range"));
    }

    #[rstest]
    fn maximum_batch_size_is_accepted() {
        assert!(validate_batch_size("test batch", OKX_WS_MAX_BATCH_SIZE).is_ok());
    }

    #[rstest]
    #[case(OKXBookChannel::Book, OKXWsChannel::Books)]
    #[case(OKXBookChannel::BookL2Tbt, OKXWsChannel::BooksTbt)]
    #[case(OKXBookChannel::Books50L2Tbt, OKXWsChannel::Books50Tbt)]
    #[case(OKXBookChannel::BooksRpi, OKXWsChannel::BooksRpi)]
    #[case(OKXBookChannel::SprdBooks5, OKXWsChannel::SprdBooks5)]
    fn test_ws_channel_for_book(#[case] channel: OKXBookChannel, #[case] expected: OKXWsChannel) {
        assert_eq!(ws_channel_for_book(channel), expected);
    }

    #[rstest]
    fn test_timestamp_format_for_websocket_auth() {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should be after UNIX epoch")
            .as_secs()
            .to_string();

        timestamp.parse::<u64>().unwrap();
        assert_eq!(timestamp.len(), 10);
        assert!(timestamp.chars().all(|c| c.is_ascii_digit()));
    }

    #[rstest]
    fn test_new_without_credentials() {
        let client = OKXWebSocketClient::default();
        assert!(client.credential.is_none());
        assert_eq!(client.api_key(), None);
        assert_eq!(client.url(), "wss://wspap.okx.com:8443/ws/v5/public");
    }

    #[rstest]
    fn test_instruments_cache_arc_observes_post_clone_writes() {
        let client = OKXWebSocketClient::default();
        let cache = client.instruments_cache_arc();
        assert!(cache.load().is_empty());

        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let symbol = instrument.symbol().inner();
        client.cache_instruments(std::slice::from_ref(&instrument));

        let loaded = cache.load();
        assert_eq!(loaded.len(), 1);
        let stored = loaded.get(&symbol).expect("instrument not refreshed");
        assert_eq!(stored.id(), instrument.id());
    }

    #[rstest]
    fn test_add_option_greeks_sub_defaults_to_both_conventions() {
        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("BTC-USD-250328-92000-C.OKX");

        client.add_option_greeks_sub(instrument_id);

        let subs = client.option_greeks_subs().load();
        let stored = subs.get(&instrument_id).expect("instrument not registered");
        assert_eq!(stored.len(), 2);
        assert!(stored.contains(&OKXGreeksType::Bs));
        assert!(stored.contains(&OKXGreeksType::Pa));
    }

    #[rstest]
    #[case::bs_only(vec![OKXGreeksType::Bs])]
    #[case::pa_only(vec![OKXGreeksType::Pa])]
    #[case::both(vec![OKXGreeksType::Bs, OKXGreeksType::Pa])]
    fn test_add_option_greeks_sub_with_conventions_stores_requested_set(
        #[case] conventions: Vec<OKXGreeksType>,
    ) {
        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("BTC-USD-250328-92000-C.OKX");
        let set: AHashSet<OKXGreeksType> = conventions.iter().copied().collect();

        client.add_option_greeks_sub_with_conventions(instrument_id, set.clone());

        let subs = client.option_greeks_subs().load();
        let stored = subs.get(&instrument_id).expect("instrument not registered");
        assert_eq!(stored, &set);
    }

    #[rstest]
    fn test_add_option_greeks_sub_with_empty_conventions_falls_back_to_both() {
        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("BTC-USD-250328-92000-C.OKX");

        client.add_option_greeks_sub_with_conventions(instrument_id, AHashSet::new());

        let subs = client.option_greeks_subs().load();
        let stored = subs.get(&instrument_id).expect("instrument not registered");
        assert_eq!(stored.len(), 2);
    }

    #[rstest]
    fn test_remove_option_greeks_sub_clears_entry() {
        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("BTC-USD-250328-92000-C.OKX");

        client.add_option_greeks_sub(instrument_id);
        client.remove_option_greeks_sub(&instrument_id);

        let subs = client.option_greeks_subs().load();
        assert!(!subs.contains_key(&instrument_id));
    }

    #[rstest]
    fn test_new_with_credentials() {
        let client = OKXWebSocketClient::new(
            None,
            Some("test_key".to_string()),
            Some("test_secret".to_string()),
            Some("test_passphrase".to_string()),
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();
        assert!(client.credential.is_some());
        assert_eq!(client.api_key(), Some("test_key"));
        assert_eq!(client.url(), "wss://wspap.okx.com:8443/ws/v5/public");
    }

    #[rstest]
    fn test_with_credentials_without_url_defaults_to_demo_public_endpoint() {
        let client = OKXWebSocketClient::with_credentials(
            None,
            Some("test_key".to_string()),
            Some("test_secret".to_string()),
            Some("test_passphrase".to_string()),
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();

        assert_eq!(client.url(), "wss://wspap.okx.com:8443/ws/v5/public");
    }

    #[rstest]
    fn test_explicit_live_websocket_url_is_preserved() {
        let live_url = "wss://ws.okx.com:8443/ws/v5/public";
        let client = OKXWebSocketClient::new(
            Some(live_url.to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();

        assert_eq!(client.url(), live_url);
    }

    #[rstest]
    fn test_new_partial_credentials_fails() {
        let result = OKXWebSocketClient::new(
            None,
            Some("test_key".to_string()),
            None,
            Some("test_passphrase".to_string()),
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        );
        result.unwrap_err();
    }

    #[rstest]
    fn test_request_id_generation() {
        let client = OKXWebSocketClient::default();

        let initial_counter = client.request_id_counter.load(Ordering::SeqCst);

        let id1 = client.request_id_counter.fetch_add(1, Ordering::SeqCst);
        let id2 = client.request_id_counter.fetch_add(1, Ordering::SeqCst);

        assert_eq!(id1, initial_counter);
        assert_eq!(id2, initial_counter + 1);
        assert_eq!(
            client.request_id_counter.load(Ordering::SeqCst),
            initial_counter + 2
        );
    }

    #[rstest]
    fn test_client_state_management() {
        let client = OKXWebSocketClient::default();

        assert!(client.is_closed());
        assert!(!client.is_active());

        let client_with_heartbeat = OKXWebSocketClient::new(
            None,
            None,
            None,
            None,
            None,
            Some(30),
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();

        assert!(client_with_heartbeat.heartbeat.is_some());
        assert_eq!(client_with_heartbeat.heartbeat.unwrap(), 30);
    }

    #[rstest]
    fn test_websocket_error_handling() {
        let clock = get_atomic_clock_realtime();
        let ts = clock.get_time_ns().as_u64();

        let error = OKXWebSocketError {
            code: "60012".to_string(),
            message: "Invalid request".to_string(),
            conn_id: None,
            timestamp: ts,
        };

        assert_eq!(error.code, "60012");
        assert_eq!(error.message, "Invalid request");
        assert_eq!(error.timestamp, ts);

        let nautilus_msg = OKXWsMessage::Error(error);
        match nautilus_msg {
            OKXWsMessage::Error(e) => {
                assert_eq!(e.code, "60012");
                assert_eq!(e.message, "Invalid request");
            }
            _ => panic!("Expected Error variant"),
        }
    }

    #[rstest]
    fn test_request_id_generation_sequence() {
        let client = OKXWebSocketClient::default();

        let initial_counter = client
            .request_id_counter
            .load(std::sync::atomic::Ordering::SeqCst);
        let mut ids = Vec::new();

        for _ in 0..10 {
            let id = client
                .request_id_counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ids.push(id);
        }

        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(id, initial_counter + i as u64);
        }

        assert_eq!(
            client
                .request_id_counter
                .load(std::sync::atomic::Ordering::SeqCst),
            initial_counter + 10
        );
    }

    #[rstest]
    fn test_client_state_transitions() {
        let client = OKXWebSocketClient::default();

        assert!(client.is_closed());
        assert!(!client.is_active());

        let client_with_heartbeat = OKXWebSocketClient::new(
            None,
            None,
            None,
            None,
            None,
            Some(30), // 30 second heartbeat
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();

        assert!(client_with_heartbeat.heartbeat.is_some());
        assert_eq!(client_with_heartbeat.heartbeat.unwrap(), 30);

        let account_id = AccountId::from("test-account-123");
        let client_with_account = OKXWebSocketClient::new(
            None,
            None,
            None,
            None,
            Some(account_id),
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();

        assert_eq!(client_with_account.account_id, account_id);
    }

    #[rstest]
    fn test_websocket_error_scenarios() {
        let clock = get_atomic_clock_realtime();
        let ts = clock.get_time_ns().as_u64();

        let error_scenarios = vec![
            ("60012", "Invalid request", None),
            ("60009", "Invalid API key", Some("conn-123".to_string())),
            ("60014", "Too many requests", None),
            ("50001", "Order not found", None),
        ];

        for (code, message, conn_id) in error_scenarios {
            let error = OKXWebSocketError {
                code: code.to_string(),
                message: message.to_string(),
                conn_id: conn_id.clone(),
                timestamp: ts,
            };

            assert_eq!(error.code, code);
            assert_eq!(error.message, message);
            assert_eq!(error.conn_id, conn_id);
            assert_eq!(error.timestamp, ts);

            let nautilus_msg = OKXWsMessage::Error(error);
            match nautilus_msg {
                OKXWsMessage::Error(e) => {
                    assert_eq!(e.code, code);
                    assert_eq!(e.message, message);
                    assert_eq!(e.conn_id, conn_id);
                }
                _ => panic!("Expected Error variant"),
            }
        }
    }

    #[rstest]
    fn test_feed_handler_reconnection_detection() {
        let msg = Message::Text(RECONNECTED.to_string().into());
        let result = OKXWsFeedHandler::parse_raw_message(msg);
        assert!(matches!(result, Ok(Some(OKXWsFrame::Reconnected))));
    }

    #[rstest]
    fn test_feed_handler_normal_message_processing() {
        let ping_msg = Message::Text(TEXT_PING.to_string().into());
        let result = OKXWsFeedHandler::parse_raw_message(ping_msg);
        assert!(matches!(result, Ok(Some(OKXWsFrame::Ping))));

        let sub_msg = r#"{
            "event": "subscribe",
            "arg": {
                "channel": "tickers",
                "instType": "SPOT"
            },
            "connId": "a4d3ae55"
        }"#;

        let sub_result =
            OKXWsFeedHandler::parse_raw_message(Message::Text(sub_msg.to_string().into()));
        assert!(matches!(
            sub_result,
            Ok(Some(OKXWsFrame::Subscription { .. }))
        ));
    }

    #[rstest]
    fn test_feed_handler_close_message() {
        let result = OKXWsFeedHandler::parse_raw_message(Message::Close(None));
        assert!(matches!(result, Ok(None)));
    }

    #[rstest]
    fn test_reconnection_message_constant() {
        assert_eq!(RECONNECTED, "__RECONNECTED__");
    }

    #[rstest]
    fn test_multiple_reconnection_signals() {
        for _ in 0..3 {
            let msg = Message::Text(RECONNECTED.to_string().into());
            let result = OKXWsFeedHandler::parse_raw_message(msg);
            assert!(matches!(result, Ok(Some(OKXWsFrame::Reconnected))));
        }
    }

    #[tokio::test]
    async fn test_wait_until_active_timeout() {
        let client = OKXWebSocketClient::new(
            None,
            Some("test_key".to_string()),
            Some("test_secret".to_string()),
            Some("test_passphrase".to_string()),
            Some(AccountId::from("test-account")),
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .unwrap();

        let result = client.wait_until_active(0.1).await;

        assert!(result.is_err());
        assert!(!client.is_active());
    }

    fn sample_canceled_order_msg() -> OKXOrderMsg {
        OKXOrderMsg {
            acc_fill_sz: Some("0".to_string()),
            avg_px: "0".to_string(),
            c_time: 0,
            cancel_source: None,
            cancel_source_reason: None,
            category: OKXOrderCategory::Normal,
            ccy: Ustr::from("USDT"),
            cl_ord_id: "order-1".to_string(),
            algo_cl_ord_id: None,
            attach_algo_cl_ord_id: None,
            attach_algo_ords: Vec::new(),
            outcome: None,
            fee: None,
            fee_ccy: Ustr::from("USDT"),
            fill_px: "0".to_string(),
            fill_sz: "0".to_string(),
            fill_time: 0,
            inst_id: Ustr::from("ETH-USDT-SWAP"),
            inst_type: OKXInstrumentType::Swap,
            lever: "1".to_string(),
            ord_id: Ustr::from("123456"),
            ord_type: OKXOrderType::Limit,
            pnl: "0".to_string(),
            pos_side: OKXPositionSide::Net,
            px: "0".to_string(),
            reduce_only: "false".to_string(),
            side: OKXSide::Buy,
            state: OKXOrderStatus::Canceled,
            exec_type: OKXExecType::None,
            sz: "1".to_string(),
            td_mode: OKXTradeMode::Cross,
            tgt_ccy: None,
            trade_id: String::new(),
            algo_id: None,
            fill_fee: None,
            fill_fee_ccy: None,
            fill_mark_px: None,
            fill_mark_vol: None,
            fill_px_vol: None,
            fill_px_usd: None,
            fill_fwd_px: None,
            fill_notional_usd: None,
            fill_pnl: None,
            is_tp_limit: None,
            linked_algo_ord: None,
            notional_usd: None,
            px_type: OKXPriceType::None,
            px_usd: None,
            px_vol: None,
            quick_mgn_type: OKXQuickMarginType::None,
            rebate: None,
            rebate_ccy: None,
            sl_ord_px: None,
            sl_trigger_px: None,
            sl_trigger_px_type: None,
            source: None,
            stp_id: None,
            stp_mode: OKXSelfTradePreventionMode::None,
            tag: None,
            tp_ord_px: None,
            tp_trigger_px: None,
            tp_trigger_px_type: None,
            amend_result: None,
            req_id: None,
            code: None,
            msg: None,
            u_time: 0,
        }
    }

    #[rstest]
    fn test_is_post_only_auto_cancel_detects_cancel_source() {
        let mut msg = sample_canceled_order_msg();
        msg.cancel_source = Some(OKX_POST_ONLY_CANCEL_SOURCE.to_string());

        assert!(is_post_only_auto_cancel(&msg));
    }

    #[rstest]
    fn test_is_post_only_auto_cancel_detects_reason() {
        let mut msg = sample_canceled_order_msg();
        msg.cancel_source_reason = Some("POST_ONLY would take liquidity".to_string());

        assert!(is_post_only_auto_cancel(&msg));
    }

    #[rstest]
    fn test_is_post_only_auto_cancel_false_without_markers() {
        let msg = sample_canceled_order_msg();

        assert!(!is_post_only_auto_cancel(&msg));
    }

    #[rstest]
    fn test_is_post_only_auto_cancel_false_for_order_type_only() {
        let mut msg = sample_canceled_order_msg();
        msg.ord_type = OKXOrderType::PostOnly;

        assert!(!is_post_only_auto_cancel(&msg));
    }

    #[tokio::test]
    async fn test_batch_cancel_orders_with_multiple_orders() {
        use nautilus_model::identifiers::{ClientOrderId, InstrumentId, VenueOrderId};

        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let instrument_id = InstrumentId::from("BTC-USDT.OKX");
        let client_order_id1 = ClientOrderId::new("order1");
        let client_order_id2 = ClientOrderId::new("order2");
        let venue_order_id1 = VenueOrderId::new("venue1");
        let venue_order_id2 = VenueOrderId::new("venue2");

        let orders = vec![
            (instrument_id, Some(client_order_id1), Some(venue_order_id1)),
            (instrument_id, Some(client_order_id2), Some(venue_order_id2)),
        ];

        let result = client.batch_cancel_orders(orders).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_cancel_orders_with_only_client_order_id() {
        use nautilus_model::identifiers::{ClientOrderId, InstrumentId};

        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let instrument_id = InstrumentId::from("BTC-USDT.OKX");
        let client_order_id = ClientOrderId::new("order1");

        let orders = vec![(instrument_id, Some(client_order_id), None)];

        let result = client.batch_cancel_orders(orders).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_cancel_orders_with_only_venue_order_id() {
        use nautilus_model::identifiers::{InstrumentId, VenueOrderId};

        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let instrument_id = InstrumentId::from("BTC-USDT.OKX");
        let venue_order_id = VenueOrderId::new("venue1");

        let orders = vec![(instrument_id, None, Some(venue_order_id))];

        let result = client.batch_cancel_orders(orders).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_cancel_orders_with_both_ids() {
        use nautilus_model::identifiers::{ClientOrderId, InstrumentId, VenueOrderId};

        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");
        let client_order_id = ClientOrderId::new("order1");
        let venue_order_id = VenueOrderId::new("venue1");

        let orders = vec![(instrument_id, Some(client_order_id), Some(venue_order_id))];

        let result = client.batch_cancel_orders(orders).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancel_order_fails_without_inst_id_code() {
        use nautilus_model::identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId};

        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");

        let result = client
            .cancel_order(
                TraderId::from("TESTER-001"),
                StrategyId::from("S-001"),
                instrument_id,
                Some(ClientOrderId::new("O-001")),
                None,
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No instIdCode cached for BTC-USDT-SWAP.OKX"),
            "Expected instIdCode error, found: {err}"
        );
    }

    #[tokio::test]
    async fn test_submit_order_fails_without_inst_id_code() {
        use nautilus_model::{
            enums::{OrderSide, OrderType},
            identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
            types::Quantity,
        };

        use crate::common::enums::OKXTradeMode;

        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("ETH-USDT-SWAP.OKX");

        let result = client
            .submit_order(
                TraderId::from("TESTER-001"),
                StrategyId::from("S-001"),
                instrument_id,
                OKXTradeMode::Cross,
                ClientOrderId::new("O-001"),
                OrderSide::Buy,
                OrderType::Limit,
                Quantity::from("0.01"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No instIdCode cached for ETH-USDT-SWAP.OKX"),
            "Expected instIdCode error, found: {err}"
        );
    }

    #[tokio::test]
    async fn test_cancel_order_passes_inst_id_code_lookup_when_cached() {
        use nautilus_model::identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId};
        use ustr::Ustr;

        let client = OKXWebSocketClient::default();
        let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");

        // Populate the cache so the lookup succeeds
        client.cache_inst_id_code(Ustr::from("BTC-USDT-SWAP"), 10459);

        let result = client
            .cancel_order(
                TraderId::from("TESTER-001"),
                StrategyId::from("S-001"),
                instrument_id,
                Some(ClientOrderId::new("O-001")),
                None,
            )
            .await;

        // Fails later (not connected) rather than at instIdCode lookup
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            !err.contains("No instIdCode cached"),
            "Should pass instIdCode lookup, found: {err}"
        );
    }

    #[rstest]
    fn test_race_unsubscribe_failure_recovery() {
        // Simulates the race condition where venue rejects an unsubscribe request.
        // The adapter must perform the 3-step recovery:
        // 1. confirm_unsubscribe() - clear pending_unsubscribe
        // 2. mark_subscribe() - mark as subscribing again
        // 3. confirm_subscribe() - restore to confirmed state
        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let topic = "trades:BTC-USDT-SWAP";

        // Initial subscribe flow
        client.subscriptions_state.mark_subscribe(topic);
        client.subscriptions_state.confirm_subscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 1);

        // User unsubscribes
        client.subscriptions_state.mark_unsubscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 0);
        assert_eq!(
            client.subscriptions_state.pending_unsubscribe_topics(),
            vec![topic]
        );

        // Venue REJECTS the unsubscribe (error message)
        // Adapter must perform 3-step recovery (from lines 4444-4446)
        client.subscriptions_state.confirm_unsubscribe(topic); // Step 1: clear pending_unsubscribe
        client.subscriptions_state.mark_subscribe(topic); // Step 2: mark as subscribing
        client.subscriptions_state.confirm_subscribe(topic); // Step 3: confirm subscription

        // Verify recovery: topic should be back in confirmed state
        assert_eq!(client.subscriptions_state.len(), 1);
        assert!(
            client
                .subscriptions_state
                .pending_unsubscribe_topics()
                .is_empty()
        );
        assert!(
            client
                .subscriptions_state
                .pending_subscribe_topics()
                .is_empty()
        );

        // Verify topic is in all_topics() for reconnect
        let all = client.subscriptions_state.all_topics();
        assert_eq!(all.len(), 1);
        assert!(all.contains(&topic.to_string()));
    }

    #[rstest]
    fn test_race_resubscribe_before_unsubscribe_ack() {
        // Simulates: User unsubscribes, then immediately resubscribes before
        // the unsubscribe ACK arrives from the venue.
        // This is the race condition fixed in the subscription tracker.
        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let topic = "books:BTC-USDT";

        // Initial subscribe
        client.subscriptions_state.mark_subscribe(topic);
        client.subscriptions_state.confirm_subscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 1);

        // User unsubscribes
        client.subscriptions_state.mark_unsubscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 0);
        assert_eq!(
            client.subscriptions_state.pending_unsubscribe_topics(),
            vec![topic]
        );

        // User immediately changes mind and resubscribes (before unsubscribe ACK)
        client.subscriptions_state.mark_subscribe(topic);
        assert_eq!(
            client.subscriptions_state.pending_subscribe_topics(),
            vec![topic]
        );

        // NOW the unsubscribe ACK arrives - should NOT clear pending_subscribe
        client.subscriptions_state.confirm_unsubscribe(topic);
        assert!(
            client
                .subscriptions_state
                .pending_unsubscribe_topics()
                .is_empty()
        );
        assert_eq!(
            client.subscriptions_state.pending_subscribe_topics(),
            vec![topic]
        );

        // Subscribe ACK arrives
        client.subscriptions_state.confirm_subscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 1);
        assert!(
            client
                .subscriptions_state
                .pending_subscribe_topics()
                .is_empty()
        );

        // Verify final state is correct
        let all = client.subscriptions_state.all_topics();
        assert_eq!(all.len(), 1);
        assert!(all.contains(&topic.to_string()));
    }

    #[rstest]
    fn test_race_late_subscribe_confirmation_after_unsubscribe() {
        // Simulates: User subscribes, then unsubscribes before subscribe ACK arrives.
        // The late subscribe ACK should be ignored.
        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let topic = "tickers:ETH-USDT";

        // User subscribes
        client.subscriptions_state.mark_subscribe(topic);
        assert_eq!(
            client.subscriptions_state.pending_subscribe_topics(),
            vec![topic]
        );

        // User immediately unsubscribes (before subscribe ACK)
        client.subscriptions_state.mark_unsubscribe(topic);
        assert!(
            client
                .subscriptions_state
                .pending_subscribe_topics()
                .is_empty()
        ); // Cleared
        assert_eq!(
            client.subscriptions_state.pending_unsubscribe_topics(),
            vec![topic]
        );

        // Late subscribe confirmation arrives - should be IGNORED
        client.subscriptions_state.confirm_subscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 0); // Not added to confirmed
        assert_eq!(
            client.subscriptions_state.pending_unsubscribe_topics(),
            vec![topic]
        );

        // Unsubscribe ACK arrives
        client.subscriptions_state.confirm_unsubscribe(topic);

        // Final state: completely empty
        assert!(client.subscriptions_state.is_empty());
        assert!(client.subscriptions_state.all_topics().is_empty());
    }

    #[rstest]
    fn test_race_reconnection_with_pending_states() {
        // Simulates reconnection with mixed pending states.
        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            Some("test_key".to_string()),
            Some("test_secret".to_string()),
            Some("test_passphrase".to_string()),
            Some(AccountId::new("OKX-TEST")),
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        // Set up mixed state before reconnection
        // Confirmed: trades:BTC-USDT-SWAP
        let trade_btc = "trades:BTC-USDT-SWAP";
        client.subscriptions_state.mark_subscribe(trade_btc);
        client.subscriptions_state.confirm_subscribe(trade_btc);

        // Pending subscribe: trades:ETH-USDT-SWAP
        let trade_eth = "trades:ETH-USDT-SWAP";
        client.subscriptions_state.mark_subscribe(trade_eth);

        // Pending unsubscribe: books:BTC-USDT (user cancelled)
        let book_btc = "books:BTC-USDT";
        client.subscriptions_state.mark_subscribe(book_btc);
        client.subscriptions_state.confirm_subscribe(book_btc);
        client.subscriptions_state.mark_unsubscribe(book_btc);

        // Get topics for reconnection
        let topics_to_restore = client.subscriptions_state.all_topics();

        // Should include: confirmed + pending_subscribe (NOT pending_unsubscribe)
        assert_eq!(topics_to_restore.len(), 2);
        assert!(topics_to_restore.contains(&trade_btc.to_string()));
        assert!(topics_to_restore.contains(&trade_eth.to_string()));
        assert!(!topics_to_restore.contains(&book_btc.to_string())); // Excluded
    }

    #[rstest]
    fn test_race_duplicate_subscribe_messages_idempotent() {
        // Simulates duplicate subscribe requests (e.g., from reconnection logic).
        // The subscription tracker should be idempotent and not create duplicate state.
        let client = OKXWebSocketClient::new(
            Some("wss://test.okx.com".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            TransportBackend::default(),
            None,
        )
        .expect("Failed to create client");

        let topic = "trades:BTC-USDT-SWAP";

        // Subscribe and confirm
        client.subscriptions_state.mark_subscribe(topic);
        client.subscriptions_state.confirm_subscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 1);

        // Duplicate mark_subscribe on already-confirmed topic (should be no-op)
        client.subscriptions_state.mark_subscribe(topic);
        assert!(
            client
                .subscriptions_state
                .pending_subscribe_topics()
                .is_empty()
        ); // Not re-added
        assert_eq!(client.subscriptions_state.len(), 1); // Still just 1

        // Duplicate confirm_subscribe (should be idempotent)
        client.subscriptions_state.confirm_subscribe(topic);
        assert_eq!(client.subscriptions_state.len(), 1);

        // Verify final state
        let all = client.subscriptions_state.all_topics();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0], topic);
    }
}
