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

//! Tokio-based bounded ingress senders for the live trading runtime.
//!
//! Production runners bind [`LiveEventSender`] instances backed by bounded Tokio channels.
//! A full or unexpectedly closed lane atomically poisons the shared [`LiveIngressControl`]; after
//! that first failure every lane rejects new traffic. The node consumes the failure notification
//! out-of-band, halts risk, and performs terminal shutdown instead of silently dropping traffic or
//! allowing an unbounded queue to exhaust the process.

use std::{
    cell::{Cell, RefCell},
    fmt::{Debug, Display},
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU16, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, ThreadId},
};

use crate::{
    messages::{DataEvent, ExecutionEvent, data::DataCommand},
    runner::{TimeEventMessage, TradingCommandMessage},
};

const INGRESS_LANE_COUNT: usize = 5;
const ALL_INGRESS_LANES: u8 = (1 << INGRESS_LANE_COUNT) - 1;

/// One bounded live-runner ingress lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LiveIngressLane {
    /// Timer callbacks entering the runner.
    TimeEvents = 0,
    /// Venue execution events entering the execution engine.
    ExecEvents = 1,
    /// Strategy/risk commands entering execution routing.
    ExecCommands = 2,
    /// Market data entering the data engine.
    DataEvents = 3,
    /// Subscription and data-client commands.
    DataCommands = 4,
}

impl LiveIngressLane {
    const fn index(self) -> usize {
        self as usize
    }

    const fn encoded(self) -> u8 {
        self as u8 + 1
    }

    const fn from_encoded(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::TimeEvents),
            2 => Some(Self::ExecEvents),
            3 => Some(Self::ExecCommands),
            4 => Some(Self::DataEvents),
            5 => Some(Self::DataCommands),
            _ => None,
        }
    }
}

impl Display for LiveIngressLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::TimeEvents => "time-events",
            Self::ExecEvents => "exec-events",
            Self::ExecCommands => "exec-commands",
            Self::DataEvents => "data-events",
            Self::DataCommands => "data-commands",
        })
    }
}

/// Terminal reason reported by the bounded ingress boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LiveIngressFailureKind {
    /// The configured queue capacity was exhausted.
    Full,
    /// The receiver disappeared while producers were still active.
    Closed,
    /// Execution or data integrity can no longer be proven.
    Integrity,
}

impl Display for LiveIngressFailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Full => "queue full",
            Self::Closed => "receiver closed",
            Self::Integrity => "integrity failure",
        })
    }
}

/// First terminal ingress failure for one runner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LiveIngressFailure {
    /// Lane which first failed.
    pub lane: LiveIngressLane,
    /// Failure class.
    pub kind: LiveIngressFailureKind,
}

impl Display for LiveIngressFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.lane, self.kind)
    }
}

/// Point-in-time producer metrics for one ingress lane.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveIngressLaneMetrics {
    /// Configured queue capacity. Zero denotes a debug/test compatibility sender.
    pub capacity: usize,
    /// Largest observed bounded-channel depth.
    pub high_watermark: usize,
    /// Number of send attempts which found the bounded queue full.
    pub overflows: u64,
    /// Number of sends rejected because another lane had already fail-stopped the runner.
    pub rejected_after_halt: u64,
    /// Number of send attempts which observed a closed receiver.
    pub closed: u64,
}

#[derive(Debug, Default)]
struct LiveIngressLaneCounters {
    capacity: AtomicUsize,
    high_watermark: AtomicUsize,
    overflows: AtomicU64,
    rejected_after_halt: AtomicU64,
    closed: AtomicU64,
}

impl LiveIngressLaneCounters {
    fn snapshot(&self) -> LiveIngressLaneMetrics {
        LiveIngressLaneMetrics {
            capacity: self.capacity.load(Ordering::Relaxed),
            high_watermark: self.high_watermark.load(Ordering::Relaxed),
            overflows: self.overflows.load(Ordering::Relaxed),
            rejected_after_halt: self.rejected_after_halt.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct LiveIngressControlInner {
    // Zero means healthy. A non-zero value atomically packs the first failing lane in the low byte
    // and its failure kind in the high byte, so readers can never observe a lane without its kind.
    failure: AtomicU16,
    registered_lanes: AtomicU8,
    failure_tx: tokio::sync::watch::Sender<Option<LiveIngressFailure>>,
    lanes: [LiveIngressLaneCounters; INGRESS_LANE_COUNT],
}

/// Shared fail-stop and producer-metrics state for all lanes owned by one runner.
#[derive(Clone, Debug)]
pub struct LiveIngressControl {
    inner: Arc<LiveIngressControlInner>,
}

impl LiveIngressControl {
    fn new() -> Self {
        let (failure_tx, _) = tokio::sync::watch::channel(None);
        Self {
            inner: Arc::new(LiveIngressControlInner {
                failure: AtomicU16::new(0),
                registered_lanes: AtomicU8::new(0),
                failure_tx,
                lanes: std::array::from_fn(|_| LiveIngressLaneCounters::default()),
            }),
        }
    }

    /// Creates a detached fail-stop domain for a staging boundary or isolated test.
    ///
    /// Detached controls cannot be used to construct or bind live-runner senders. Production
    /// runners obtain their control from [`LiveIngressBundle::control`].
    #[must_use]
    pub fn detached() -> Self {
        Self::new()
    }

    /// Returns the first terminal ingress failure, if any.
    #[must_use]
    pub fn failure(&self) -> Option<LiveIngressFailure> {
        let encoded = self.inner.failure.load(Ordering::Acquire);
        let lane = LiveIngressLane::from_encoded(encoded as u8)?;
        let kind = match (encoded >> 8) as u8 {
            1 => LiveIngressFailureKind::Full,
            2 => LiveIngressFailureKind::Closed,
            3 => LiveIngressFailureKind::Integrity,
            _ => return None,
        };
        Some(LiveIngressFailure { lane, kind })
    }

    /// Returns `true` once any lane has entered terminal fail-stop.
    #[must_use]
    pub fn is_halted(&self) -> bool {
        self.inner.failure.load(Ordering::Acquire) != 0
    }

    /// Subscribes to the first terminal failure without consuming it.
    #[must_use]
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<LiveIngressFailure>> {
        self.inner.failure_tx.subscribe()
    }

    /// Returns producer metrics for `lane`.
    #[must_use]
    pub fn lane_metrics(&self, lane: LiveIngressLane) -> LiveIngressLaneMetrics {
        self.inner.lanes[lane.index()].snapshot()
    }

    /// Marks a lane terminal. Only the first failure controls the node-wide outcome.
    ///
    /// This is public so bounded staging buffers and adapter-internal ingress can join the same
    /// fail-stop domain instead of inventing an independent drop policy.
    pub fn fail(&self, lane: LiveIngressLane, kind: LiveIngressFailureKind) {
        let kind_encoded = match kind {
            LiveIngressFailureKind::Full => 1_u16,
            LiveIngressFailureKind::Closed => 2_u16,
            LiveIngressFailureKind::Integrity => 3_u16,
        };
        let encoded = u16::from(lane.encoded()) | (kind_encoded << 8);
        if self
            .inner
            .failure
            .compare_exchange(0, encoded, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner
                .failure_tx
                .send_replace(Some(LiveIngressFailure { lane, kind }));
        }
    }

    /// Records capacity exhaustion in a bounded staging layer and terminally fails the lane.
    pub fn fail_full(&self, lane: LiveIngressLane) {
        self.record_overflow(lane);
        self.fail(lane, LiveIngressFailureKind::Full);
    }

    /// Records an unexpectedly closed bounded staging layer and terminally fails the lane.
    pub fn fail_closed(&self, lane: LiveIngressLane) {
        self.record_closed(lane);
        self.fail(lane, LiveIngressFailureKind::Closed);
    }

    /// Marks a lane terminal because its semantic integrity can no longer be proven.
    pub fn fail_integrity(&self, lane: LiveIngressLane) {
        self.fail(lane, LiveIngressFailureKind::Integrity);
    }

    fn register(&self, lane: LiveIngressLane, capacity: usize) {
        self.inner.lanes[lane.index()]
            .capacity
            .store(capacity, Ordering::Relaxed);
        self.inner
            .registered_lanes
            .fetch_or(1 << lane.index(), Ordering::Relaxed);
    }

    fn same_domain(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn registered_lanes(&self) -> u8 {
        self.inner.registered_lanes.load(Ordering::Acquire)
    }

    fn record_depth(&self, lane: LiveIngressLane, depth: usize) {
        self.inner.lanes[lane.index()]
            .high_watermark
            .fetch_max(depth, Ordering::Relaxed);
    }

    fn record_overflow(&self, lane: LiveIngressLane) {
        saturating_increment(&self.inner.lanes[lane.index()].overflows);
    }

    fn record_rejected_after_halt(&self, lane: LiveIngressLane) {
        saturating_increment(&self.inner.lanes[lane.index()].rejected_after_halt);
    }

    fn record_closed(&self, lane: LiveIngressLane) {
        saturating_increment(&self.inner.lanes[lane.index()].closed);
    }
}

fn saturating_increment(value: &AtomicU64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

enum LiveEventSenderInner<T> {
    Bounded(tokio::sync::mpsc::Sender<T>),
    // Debug/test compatibility for legacy harnesses. This variant and its conversion are absent
    // from optimized production builds, and LiveNode never constructs it in any build profile.
    #[cfg(any(test, debug_assertions))]
    LegacyUnbounded(tokio::sync::mpsc::UnboundedSender<T>),
}

impl<T> Clone for LiveEventSenderInner<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Bounded(sender) => Self::Bounded(sender.clone()),
            #[cfg(any(test, debug_assertions))]
            Self::LegacyUnbounded(sender) => Self::LegacyUnbounded(sender.clone()),
        }
    }
}

impl<T> Debug for LiveEventSenderInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bounded(_) => f.write_str("Bounded"),
            #[cfg(any(test, debug_assertions))]
            Self::LegacyUnbounded(_) => f.write_str("LegacyUnbounded"),
        }
    }
}

/// Synchronous fail-stop sender used by adapters and live engine components.
///
/// Bounded senders never await: producers call [`Self::send`], which uses `try_send`. Blocking an
/// exchange websocket task behind a slow event store would make liveness ambiguous, so capacity
/// exhaustion is instead a terminal, observable node failure.
pub struct LiveEventSender<T> {
    inner: LiveEventSenderInner<T>,
    lane: LiveIngressLane,
    control: LiveIngressControl,
}

/// Creates one bounded ingress lane for a test harness or a non-runner integration boundary.
///
/// The caller supplies the exact lane; the returned sender owns an isolated control domain.
/// Production runners must construct all lanes from their shared runner control instead.
#[must_use]
#[cfg(debug_assertions)]
fn bounded_ingress_channel<T>(
    capacity: usize,
    lane: LiveIngressLane,
) -> (LiveEventSender<T>, tokio::sync::mpsc::Receiver<T>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    (
        LiveEventSender::bounded(sender, lane, LiveIngressControl::new()),
        receiver,
    )
}

impl<T> Clone for LiveEventSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            lane: self.lane,
            control: self.control.clone(),
        }
    }
}

impl<T> Debug for LiveEventSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(LiveEventSender))
            .field("inner", &self.inner)
            .field("lane", &self.lane)
            .finish_non_exhaustive()
    }
}

impl<T> LiveEventSender<T> {
    /// Wraps a bounded channel in the runner-wide fail-stop domain.
    #[must_use]
    fn bounded(
        sender: tokio::sync::mpsc::Sender<T>,
        lane: LiveIngressLane,
        control: LiveIngressControl,
    ) -> Self {
        control.register(lane, sender.max_capacity());
        Self {
            inner: LiveEventSenderInner::Bounded(sender),
            lane,
            control,
        }
    }

    /// Sends one message or returns an explicit terminal error.
    ///
    /// # Errors
    ///
    /// Returns [`LiveIngressSendError::Full`] after atomically fail-stopping a full bounded lane,
    /// [`LiveIngressSendError::Closed`] when its receiver disappeared, or
    /// [`LiveIngressSendError::FailStopped`] when any lane had already failed.
    pub fn send(&self, message: T) -> Result<(), LiveIngressSendError<T>> {
        if self.control.is_halted() {
            self.control.record_rejected_after_halt(self.lane);
            return Err(LiveIngressSendError::FailStopped(message));
        }

        match &self.inner {
            LiveEventSenderInner::Bounded(sender) => match sender.try_send(message) {
                Ok(()) => {
                    let depth = sender.max_capacity().saturating_sub(sender.capacity());
                    self.control.record_depth(self.lane, depth);
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => {
                    self.control.record_depth(self.lane, sender.max_capacity());
                    self.control.record_overflow(self.lane);
                    self.control.fail(self.lane, LiveIngressFailureKind::Full);
                    Err(LiveIngressSendError::Full(message))
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(message)) => {
                    self.control.record_closed(self.lane);
                    self.control.fail(self.lane, LiveIngressFailureKind::Closed);
                    Err(LiveIngressSendError::Closed(message))
                }
            },
            #[cfg(any(test, debug_assertions))]
            LiveEventSenderInner::LegacyUnbounded(sender) => {
                sender.send(message).map_err(|error| {
                    self.control.record_closed(self.lane);
                    self.control.fail(self.lane, LiveIngressFailureKind::Closed);
                    LiveIngressSendError::Closed(error.0)
                })
            }
        }
    }

    /// Alias for [`Self::send`] matching Tokio's bounded sender terminology.
    ///
    /// # Errors
    ///
    /// Returns the same explicit terminal errors as [`Self::send`].
    pub fn try_send(&self, message: T) -> Result<(), LiveIngressSendError<T>> {
        self.send(message)
    }

    /// Returns the lane associated with this sender.
    #[must_use]
    pub const fn lane(&self) -> LiveIngressLane {
        self.lane
    }

    /// Returns the shared fail-stop control.
    #[must_use]
    pub const fn control(&self) -> &LiveIngressControl {
        &self.control
    }
}

// Compatibility exists only for tests and debug harnesses. Optimized production builds have no
// conversion from an unbounded sender, making the bounded constructor the only admissible path.
#[cfg(any(test, debug_assertions))]
impl<T> From<tokio::sync::mpsc::UnboundedSender<T>> for LiveEventSender<T> {
    fn from(sender: tokio::sync::mpsc::UnboundedSender<T>) -> Self {
        Self {
            inner: LiveEventSenderInner::LegacyUnbounded(sender),
            // Legacy conversions only exist for compatibility. Data is the least surprising
            // label; production runner construction always uses `bounded` with an exact lane.
            lane: LiveIngressLane::DataEvents,
            control: LiveIngressControl::new(),
        }
    }
}

// As above, implicit conversion cannot safely infer the lane or join the runner's shared control.
// Release callers must use `LiveEventSender::bounded(sender, exact_lane, shared_control)`.
#[cfg(any(test, debug_assertions))]
impl<T> From<tokio::sync::mpsc::Sender<T>> for LiveEventSender<T> {
    fn from(sender: tokio::sync::mpsc::Sender<T>) -> Self {
        Self::bounded(
            sender,
            LiveIngressLane::DataEvents,
            LiveIngressControl::new(),
        )
    }
}

/// Explicit result of a synchronous live-ingress send.
#[derive(Debug)]
pub enum LiveIngressSendError<T> {
    /// The bounded lane was full and caused terminal fail-stop.
    Full(T),
    /// The receiver was closed and caused terminal fail-stop.
    Closed(T),
    /// Another lane had already caused terminal fail-stop.
    FailStopped(T),
}

impl<T> LiveIngressSendError<T> {
    /// Returns `true` when this send was rejected after an earlier terminal failure.
    ///
    /// Producers should not emit a warning or error for this variant on every message: the first
    /// failure is delivered through [`LiveIngressControl::subscribe`], while subsequent rejects
    /// are counted in [`LiveIngressLaneMetrics::rejected_after_halt`].
    #[must_use]
    pub const fn is_fail_stopped(&self) -> bool {
        matches!(self, Self::FailStopped(_))
    }
}

impl<T> Display for LiveIngressSendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Full(_) => "live ingress queue full; terminal fail-stop triggered",
            Self::Closed(_) => "live ingress receiver closed; terminal fail-stop triggered",
            Self::FailStopped(_) => "live ingress already fail-stopped",
        })
    }
}

impl<T: Debug> std::error::Error for LiveIngressSendError<T> {}

/// Data-event sender stored in live-runner TLS.
pub type DataEventSender = LiveEventSender<DataEvent>;
/// Execution-event sender stored in live-runner TLS.
pub type ExecEventSender = LiveEventSender<ExecutionEvent>;

/// Error returned when a live-ingress capability is incomplete or belongs to another runner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LiveIngressBindingError {
    /// A bounded production lane was configured with zero capacity.
    #[error("{0} ingress capacity must be non-zero")]
    ZeroCapacity(&'static str),
    /// A typed sender does not carry its required lane identity.
    #[error("live ingress sender has lane {actual}, expected {expected}")]
    WrongLane {
        /// Lane required by the sender's message type.
        expected: LiveIngressLane,
        /// Lane attached to the supplied sender.
        actual: LiveIngressLane,
    },
    /// One member does not share the bundle's fail-stop domain.
    #[error("live ingress lane {lane} belongs to a different fail-stop domain")]
    MixedControl {
        /// Lane whose control did not match the bundle control.
        lane: LiveIngressLane,
    },
    /// The capability did not register every required production lane.
    #[error("live ingress bundle is incomplete (registered mask {registered:#07b})")]
    IncompleteBundle {
        /// Bit mask of registered lanes.
        registered: u8,
    },
    /// Another runner already owns the current thread's live-ingress TLS.
    #[error("another live ingress bundle is already active on this thread")]
    ActiveDomainConflict,
    /// A standalone sender was offered to production TLS without an active runner bundle.
    #[error("no live ingress bundle is active on this thread")]
    NoActiveDomain,
    /// The bundle was moved after binding and accessed from another OS thread.
    #[error("live ingress bundle is bound to a different OS thread")]
    WrongThread,
}

/// Receiver halves owned by one complete live-runner ingress boundary.
#[derive(Debug)]
pub struct LiveIngressReceivers {
    /// Timer callback receiver.
    pub time_events: tokio::sync::mpsc::Receiver<TimeEventMessage>,
    /// Venue execution-event receiver.
    pub exec_events: tokio::sync::mpsc::Receiver<ExecutionEvent>,
    /// Strategy/risk command receiver.
    pub exec_commands: tokio::sync::mpsc::Receiver<TradingCommandMessage>,
    /// Market-data receiver.
    pub data_events: tokio::sync::mpsc::Receiver<DataEvent>,
    /// Data-client command receiver.
    pub data_commands: tokio::sync::mpsc::Receiver<DataCommand>,
}

/// An indivisible, type-derived capability for all five production ingress lanes.
///
/// Construction hard-codes each message type to its lane and one shared fail-stop control.
/// Binding validates the complete capability before changing thread-local state. Individual
/// sender clones cannot replace an active runner's TLS unless their exact lane and control match.
#[derive(Debug)]
pub struct LiveIngressBundle {
    time_events: LiveEventSender<TimeEventMessage>,
    exec_events: LiveEventSender<ExecutionEvent>,
    exec_commands: LiveEventSender<TradingCommandMessage>,
    data_events: LiveEventSender<DataEvent>,
    data_commands: LiveEventSender<DataCommand>,
    control: LiveIngressControl,
    bound_thread: Cell<Option<ThreadId>>,
    // The live msgbus and sender TLS are thread-affine. Encoding that invariant in the type keeps
    // a bound bundle (and therefore `AsyncRunner`) off thread-migrating executors.
    _not_send: PhantomData<Rc<()>>,
}

impl LiveIngressBundle {
    /// Creates all five bounded lanes in one shared fail-stop domain.
    ///
    /// # Errors
    ///
    /// Returns [`LiveIngressBindingError::ZeroCapacity`] when any capacity is zero.
    pub fn bounded(
        data_capacity: usize,
        risk_capacity: usize,
        exec_capacity: usize,
    ) -> Result<(Self, LiveIngressReceivers), LiveIngressBindingError> {
        if data_capacity == 0 {
            return Err(LiveIngressBindingError::ZeroCapacity("data"));
        }
        if risk_capacity == 0 {
            return Err(LiveIngressBindingError::ZeroCapacity("risk"));
        }
        if exec_capacity == 0 {
            return Err(LiveIngressBindingError::ZeroCapacity("execution"));
        }

        let control = LiveIngressControl::new();
        let (time_events, time_events_rx) = tokio::sync::mpsc::channel(data_capacity);
        let (exec_events, exec_events_rx) = tokio::sync::mpsc::channel(exec_capacity);
        let (exec_commands, exec_commands_rx) = tokio::sync::mpsc::channel(risk_capacity);
        let (data_events, data_events_rx) = tokio::sync::mpsc::channel(data_capacity);
        let (data_commands, data_commands_rx) = tokio::sync::mpsc::channel(data_capacity);

        let bundle = Self {
            time_events: LiveEventSender::bounded(
                time_events,
                LiveIngressLane::TimeEvents,
                control.clone(),
            ),
            exec_events: LiveEventSender::bounded(
                exec_events,
                LiveIngressLane::ExecEvents,
                control.clone(),
            ),
            exec_commands: LiveEventSender::bounded(
                exec_commands,
                LiveIngressLane::ExecCommands,
                control.clone(),
            ),
            data_events: LiveEventSender::bounded(
                data_events,
                LiveIngressLane::DataEvents,
                control.clone(),
            ),
            data_commands: LiveEventSender::bounded(
                data_commands,
                LiveIngressLane::DataCommands,
                control.clone(),
            ),
            control,
            bound_thread: Cell::new(None),
            _not_send: PhantomData,
        };
        bundle.validate()?;

        Ok((
            bundle,
            LiveIngressReceivers {
                time_events: time_events_rx,
                exec_events: exec_events_rx,
                exec_commands: exec_commands_rx,
                data_events: data_events_rx,
                data_commands: data_commands_rx,
            },
        ))
    }

    /// Validates and binds the complete capability to the current thread.
    ///
    /// # Errors
    ///
    /// Returns without changing TLS if the bundle is incomplete or another runner is active.
    pub fn bind_current_thread(&self) -> Result<(), LiveIngressBindingError> {
        self.validate()?;
        let current_thread = thread::current().id();
        if self
            .bound_thread
            .get()
            .is_some_and(|owner| owner != current_thread)
        {
            return Err(LiveIngressBindingError::WrongThread);
        }
        ACTIVE_LIVE_INGRESS.with(|active| {
            if let Some(current) = active.borrow().as_ref()
                && !current.same_domain(&self.control)
            {
                return Err(LiveIngressBindingError::ActiveDomainConflict);
            }

            *active.borrow_mut() = Some(self.control.clone());
            DATA_EVENT_SENDER.with(|slot| {
                *slot.borrow_mut() = Some(self.data_events.clone());
            });
            EXEC_EVENT_SENDER.with(|slot| {
                *slot.borrow_mut() = Some(self.exec_events.clone());
            });
            self.bound_thread.set(Some(current_thread));
            Ok(())
        })
    }

    /// Releases this bundle's binding on the OS thread which performed the bind.
    ///
    /// Moving a bound bundle to another thread is unsupported: a different thread cannot access
    /// or clear the original thread's TLS and receives [`LiveIngressBindingError::WrongThread`].
    ///
    /// # Errors
    ///
    /// Returns an error if called from a different thread or if another domain is unexpectedly
    /// active. In either case the original thread's TLS is left unchanged (fail closed).
    pub fn unbind_current_thread(&self) -> Result<(), LiveIngressBindingError> {
        let Some(owner) = self.bound_thread.get() else {
            return Ok(());
        };
        if owner != thread::current().id() {
            return Err(LiveIngressBindingError::WrongThread);
        }

        ACTIVE_LIVE_INGRESS.with(|active| {
            let active_matches = if let Some(current) = active.borrow().as_ref() {
                current.same_domain(&self.control)
            } else {
                self.bound_thread.set(None);
                return Ok(());
            };
            if !active_matches {
                return Err(LiveIngressBindingError::ActiveDomainConflict);
            }

            DATA_EVENT_SENDER.with(|slot| {
                let should_clear = slot
                    .borrow()
                    .as_ref()
                    .is_some_and(|sender| sender.control.same_domain(&self.control));
                if should_clear {
                    *slot.borrow_mut() = None;
                }
            });
            EXEC_EVENT_SENDER.with(|slot| {
                let should_clear = slot
                    .borrow()
                    .as_ref()
                    .is_some_and(|sender| sender.control.same_domain(&self.control));
                if should_clear {
                    *slot.borrow_mut() = None;
                }
            });
            *active.borrow_mut() = None;
            self.bound_thread.set(None);
            Ok(())
        })
    }

    /// Returns the runner-wide fail-stop control.
    #[must_use]
    pub const fn control(&self) -> &LiveIngressControl {
        &self.control
    }

    /// Returns a clone of the time-event lane capability.
    ///
    /// # Panics
    ///
    /// Panics unless this complete bundle is active on the current thread. Debug builds also allow
    /// unbound extraction for isolated harnesses.
    #[must_use]
    pub fn time_event_sender(&self) -> LiveEventSender<TimeEventMessage> {
        self.validate_sender_access()
            .expect("live ingress bundle must be active before cloning a production sender");
        self.time_events.clone()
    }

    /// Returns a clone of the execution-event lane capability.
    ///
    /// # Panics
    ///
    /// Panics unless this complete bundle is active on the current thread. Debug builds also allow
    /// unbound extraction for isolated harnesses.
    #[must_use]
    pub fn exec_event_sender(&self) -> LiveEventSender<ExecutionEvent> {
        self.validate_sender_access()
            .expect("live ingress bundle must be active before cloning a production sender");
        self.exec_events.clone()
    }

    /// Returns a clone of the execution-command lane capability.
    ///
    /// # Panics
    ///
    /// Panics unless this complete bundle is active on the current thread. Debug builds also allow
    /// unbound extraction for isolated harnesses.
    #[must_use]
    pub fn exec_command_sender(&self) -> LiveEventSender<TradingCommandMessage> {
        self.validate_sender_access()
            .expect("live ingress bundle must be active before cloning a production sender");
        self.exec_commands.clone()
    }

    /// Returns a clone of the data-event lane capability.
    ///
    /// # Panics
    ///
    /// Panics unless this complete bundle is active on the current thread. Debug builds also allow
    /// unbound extraction for isolated harnesses.
    #[must_use]
    pub fn data_event_sender(&self) -> LiveEventSender<DataEvent> {
        self.validate_sender_access()
            .expect("live ingress bundle must be active before cloning a production sender");
        self.data_events.clone()
    }

    /// Returns a clone of the data-command lane capability.
    ///
    /// # Panics
    ///
    /// Panics unless this complete bundle is active on the current thread. Debug builds also allow
    /// unbound extraction for isolated harnesses.
    #[must_use]
    pub fn data_command_sender(&self) -> LiveEventSender<DataCommand> {
        self.validate_sender_access()
            .expect("live ingress bundle must be active before cloning a production sender");
        self.data_commands.clone()
    }

    fn validate_sender_access(&self) -> Result<(), LiveIngressBindingError> {
        if self
            .bound_thread
            .get()
            .is_some_and(|owner| owner != thread::current().id())
        {
            return Err(LiveIngressBindingError::WrongThread);
        }
        ACTIVE_LIVE_INGRESS.with(|active| match active.borrow().as_ref() {
            Some(control) if control.same_domain(&self.control) => Ok(()),
            Some(_) => Err(LiveIngressBindingError::ActiveDomainConflict),
            None if cfg!(debug_assertions) => Ok(()),
            None => Err(LiveIngressBindingError::NoActiveDomain),
        })
    }

    fn validate(&self) -> Result<(), LiveIngressBindingError> {
        self.validate_member(&self.time_events, LiveIngressLane::TimeEvents)?;
        self.validate_member(&self.exec_events, LiveIngressLane::ExecEvents)?;
        self.validate_member(&self.exec_commands, LiveIngressLane::ExecCommands)?;
        self.validate_member(&self.data_events, LiveIngressLane::DataEvents)?;
        self.validate_member(&self.data_commands, LiveIngressLane::DataCommands)?;
        let registered = self.control.registered_lanes();
        if registered != ALL_INGRESS_LANES {
            return Err(LiveIngressBindingError::IncompleteBundle { registered });
        }
        Ok(())
    }

    fn validate_member<T>(
        &self,
        sender: &LiveEventSender<T>,
        expected: LiveIngressLane,
    ) -> Result<(), LiveIngressBindingError> {
        validate_sender_lane(sender, expected)?;
        if !sender.control.same_domain(&self.control) {
            return Err(LiveIngressBindingError::MixedControl { lane: expected });
        }
        Ok(())
    }
}

impl Drop for LiveIngressBundle {
    fn drop(&mut self) {
        if let Err(error) = self.unbind_current_thread() {
            log::error!("Could not release live ingress TLS: {error}");
        }
    }
}

/// Creates a bounded data-event channel suitable for isolated tests and test harnesses.
///
/// This helper has the same fail-stop behavior as a live ingress lane, but owns a separate
/// control domain. Production runners must instead bind their five lanes from one shared
/// [`LiveIngressControl`].
///
/// # Panics
///
/// Panics in non-debug builds, where isolated ingress capabilities are forbidden.
#[must_use]
pub fn bounded_data_event_channel(
    capacity: usize,
) -> (DataEventSender, tokio::sync::mpsc::Receiver<DataEvent>) {
    #[cfg(not(debug_assertions))]
    {
        let _ = capacity;
        panic!("isolated data-event channels are test/debug harnesses only");
    }
    #[cfg(debug_assertions)]
    {
        bounded_ingress_channel(capacity, LiveIngressLane::DataEvents)
    }
}

/// Creates a bounded execution-event channel suitable for isolated tests and test harnesses.
///
/// This helper has the same fail-stop behavior as a live ingress lane, but owns a separate
/// control domain. Production runners must instead bind their five lanes from one shared
/// [`LiveIngressControl`].
///
/// # Panics
///
/// Panics in non-debug builds, where isolated ingress capabilities are forbidden.
#[must_use]
pub fn bounded_exec_event_channel(
    capacity: usize,
) -> (ExecEventSender, tokio::sync::mpsc::Receiver<ExecutionEvent>) {
    #[cfg(not(debug_assertions))]
    {
        let _ = capacity;
        panic!("isolated execution-event channels are test/debug harnesses only");
    }
    #[cfg(debug_assertions)]
    {
        bounded_ingress_channel(capacity, LiveIngressLane::ExecEvents)
    }
}

fn validate_sender_lane<T>(
    sender: &LiveEventSender<T>,
    expected: LiveIngressLane,
) -> Result<(), LiveIngressBindingError> {
    if sender.lane != expected {
        return Err(LiveIngressBindingError::WrongLane {
            expected,
            actual: sender.lane,
        });
    }
    Ok(())
}

/// Validates that a sender belongs to the active runner and carries its exact typed lane.
///
/// Debug/test harnesses may use an isolated sender when no production bundle is active. In
/// non-debug builds the absence of an active bundle is rejected, preventing detached helpers from
/// becoming a production ingress domain.
///
/// # Errors
///
/// Returns an error for a wrong lane, a different runner control, or no active production bundle.
pub fn validate_live_event_sender_for_active<T>(
    sender: &LiveEventSender<T>,
    expected: LiveIngressLane,
) -> Result<(), LiveIngressBindingError> {
    validate_sender_lane(sender, expected)?;
    ACTIVE_LIVE_INGRESS.with(|active| match active.borrow().as_ref() {
        Some(control) if control.same_domain(&sender.control) => Ok(()),
        Some(_) => Err(LiveIngressBindingError::ActiveDomainConflict),
        None if cfg!(debug_assertions) => Ok(()),
        None => Err(LiveIngressBindingError::NoActiveDomain),
    })
}

/// Gets the global data event sender.
///
/// # Panics
///
/// Panics if the sender is uninitialized.
#[must_use]
pub fn get_data_event_sender() -> DataEventSender {
    DATA_EVENT_SENDER.with(|sender| {
        sender
            .borrow()
            .as_ref()
            .expect("Data event sender should be initialized by runner")
            .clone()
    })
}

/// Attempts to get the global data event sender without panicking.
///
/// Returns `None` if the sender is not initialized (e.g., in Python/v1 bridge environments
/// before a runner or adapter bridge has registered a sender).
#[must_use]
pub fn try_get_data_event_sender() -> Option<DataEventSender> {
    DATA_EVENT_SENDER.with(|sender| sender.borrow().as_ref().cloned())
}

/// Sets the global data event sender.
///
/// Can only be called once per thread.
///
/// # Panics
///
/// Panics if a sender has already been set.
pub fn set_data_event_sender(sender: impl Into<DataEventSender>) {
    let sender = sender.into();
    validate_live_event_sender_for_active(&sender, LiveIngressLane::DataEvents)
        .expect("data event sender must belong to the active live ingress bundle");
    DATA_EVENT_SENDER.with(|s| {
        let mut slot = s.borrow_mut();
        assert!(slot.is_none(), "Data event sender can only be set once");
        *slot = Some(sender);
    });
}

/// Replaces the global data event sender for the current thread.
///
/// # Panics
///
/// Panics if the sender has the wrong lane or does not belong to the active live ingress bundle.
pub fn replace_data_event_sender(sender: impl Into<DataEventSender>) {
    let sender = sender.into();
    validate_live_event_sender_for_active(&sender, LiveIngressLane::DataEvents)
        .expect("data event sender must belong to the active live ingress bundle");
    DATA_EVENT_SENDER.with(|s| {
        *s.borrow_mut() = Some(sender);
    });
}

/// Gets the global execution event sender.
///
/// # Panics
///
/// Panics if the sender is uninitialized.
#[must_use]
pub fn get_exec_event_sender() -> ExecEventSender {
    EXEC_EVENT_SENDER.with(|sender| {
        sender
            .borrow()
            .as_ref()
            .expect("Execution event sender should be initialized by runner")
            .clone()
    })
}

/// Attempts to get the global execution event sender without panicking.
///
/// Returns `None` if the sender is not initialized (e.g., in test environments).
#[must_use]
pub fn try_get_exec_event_sender() -> Option<ExecEventSender> {
    EXEC_EVENT_SENDER.with(|sender| sender.borrow().as_ref().cloned())
}

/// Sets the global execution event sender.
///
/// Can only be called once per thread.
///
/// # Panics
///
/// Panics if a sender has already been set.
pub fn set_exec_event_sender(sender: impl Into<ExecEventSender>) {
    let sender = sender.into();
    validate_live_event_sender_for_active(&sender, LiveIngressLane::ExecEvents)
        .expect("execution event sender must belong to the active live ingress bundle");
    EXEC_EVENT_SENDER.with(|s| {
        let mut slot = s.borrow_mut();
        assert!(
            slot.is_none(),
            "Execution event sender can only be set once"
        );
        *slot = Some(sender);
    });
}

/// Replaces the global execution event sender for the current thread.
///
/// # Panics
///
/// Panics if the sender has the wrong lane or does not belong to the active live ingress bundle.
pub fn replace_exec_event_sender(sender: impl Into<ExecEventSender>) {
    let sender = sender.into();
    validate_live_event_sender_for_active(&sender, LiveIngressLane::ExecEvents)
        .expect("execution event sender must belong to the active live ingress bundle");
    EXEC_EVENT_SENDER.with(|s| {
        *s.borrow_mut() = Some(sender);
    });
}

thread_local! {
    static ACTIVE_LIVE_INGRESS: RefCell<Option<LiveIngressControl>> = const { RefCell::new(None) };
    static DATA_EVENT_SENDER: RefCell<Option<DataEventSender>> = const { RefCell::new(None) };
    static EXEC_EVENT_SENDER: RefCell<Option<ExecEventSender>> = const { RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_replace_data_event_sender_overwrites_previous() {
        std::thread::spawn(|| {
            let (tx1, _rx1) = bounded_data_event_channel(16);
            let (tx2, _rx2) = bounded_data_event_channel(16);
            replace_data_event_sender(tx1);
            replace_data_event_sender(tx2);
            let _sender = get_data_event_sender();
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_replace_exec_event_sender_overwrites_previous() {
        std::thread::spawn(|| {
            let (tx1, _rx1) = bounded_exec_event_channel(16);
            let (tx2, _rx2) = bounded_exec_event_channel(16);
            replace_exec_event_sender(tx1);
            replace_exec_event_sender(tx2);
            let _sender = get_exec_event_sender();
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_set_data_event_sender_panics_on_double_set() {
        let result = std::thread::spawn(|| {
            let (tx1, _rx1) = bounded_data_event_channel(16);
            let (tx2, _rx2) = bounded_data_event_channel(16);
            set_data_event_sender(tx1);
            set_data_event_sender(tx2);
        })
        .join();
        assert!(result.is_err());
    }

    #[rstest]
    fn test_set_exec_event_sender_panics_on_double_set() {
        let result = std::thread::spawn(|| {
            let (tx1, _rx1) = bounded_exec_event_channel(16);
            let (tx2, _rx2) = bounded_exec_event_channel(16);
            set_exec_event_sender(tx1);
            set_exec_event_sender(tx2);
        })
        .join();
        assert!(result.is_err());
    }

    #[rstest]
    fn test_try_get_exec_event_sender_returns_none_when_unset() {
        let result = std::thread::spawn(try_get_exec_event_sender)
            .join()
            .unwrap();
        assert!(result.is_none());
    }

    #[rstest]
    fn test_non_debug_release_rejects_detached_sender_paths() {
        if cfg!(debug_assertions) {
            return;
        }

        assert!(
            std::panic::catch_unwind(|| bounded_data_event_channel(1)).is_err(),
            "release must reject isolated data-event sender construction"
        );
        assert!(
            std::panic::catch_unwind(|| bounded_exec_event_channel(1)).is_err(),
            "release must reject isolated execution-event sender construction"
        );

        let (unbound, _unbound_receivers) = LiveIngressBundle::bounded(1, 1, 1).unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                unbound.data_event_sender()
            }))
            .is_err(),
            "release must reject sender extraction from an unbound bundle"
        );

        let (active, _active_receivers) = LiveIngressBundle::bounded(1, 1, 1).unwrap();
        let (different, _different_receivers) = LiveIngressBundle::bounded(1, 1, 1).unwrap();
        active.bind_current_thread().unwrap();
        let original = get_data_event_sender();
        assert!(
            active
                .data_event_sender()
                .control()
                .same_domain(active.control())
        );
        assert_eq!(
            different.bind_current_thread(),
            Err(LiveIngressBindingError::ActiveDomainConflict)
        );
        assert!(
            get_data_event_sender()
                .control()
                .same_domain(original.control())
        );
        active.unbind_current_thread().unwrap();
    }

    #[rstest]
    fn test_bundle_hard_codes_five_lanes_to_one_control() {
        let (bundle, _receivers) = LiveIngressBundle::bounded(11, 7, 5).unwrap();

        assert_eq!(
            bundle.time_event_sender().lane(),
            LiveIngressLane::TimeEvents
        );
        assert_eq!(
            bundle.exec_event_sender().lane(),
            LiveIngressLane::ExecEvents
        );
        assert_eq!(
            bundle.exec_command_sender().lane(),
            LiveIngressLane::ExecCommands
        );
        assert_eq!(
            bundle.data_event_sender().lane(),
            LiveIngressLane::DataEvents
        );
        assert_eq!(
            bundle.data_command_sender().lane(),
            LiveIngressLane::DataCommands
        );

        for control in [
            bundle.time_event_sender().control().clone(),
            bundle.exec_event_sender().control().clone(),
            bundle.exec_command_sender().control().clone(),
            bundle.data_event_sender().control().clone(),
            bundle.data_command_sender().control().clone(),
        ] {
            assert!(control.same_domain(bundle.control()));
        }
        assert_eq!(bundle.control().registered_lanes(), ALL_INGRESS_LANES);
        assert_eq!(
            bundle
                .control()
                .lane_metrics(LiveIngressLane::TimeEvents)
                .capacity,
            11
        );
        assert_eq!(
            bundle
                .control()
                .lane_metrics(LiveIngressLane::ExecCommands)
                .capacity,
            7
        );
        assert_eq!(
            bundle
                .control()
                .lane_metrics(LiveIngressLane::ExecEvents)
                .capacity,
            5
        );
    }

    #[rstest]
    fn test_active_bundle_rejects_different_or_isolated_sender_without_mutation() {
        std::thread::spawn(|| {
            let (bundle, _receivers) = LiveIngressBundle::bounded(16, 16, 16).unwrap();
            bundle.bind_current_thread().unwrap();
            let original_data = get_data_event_sender();
            let original_exec = get_exec_event_sender();

            let (different_bundle, _different_receivers) =
                LiveIngressBundle::bounded(16, 16, 16).unwrap();
            assert_eq!(
                different_bundle.bind_current_thread(),
                Err(LiveIngressBindingError::ActiveDomainConflict)
            );
            assert!(
                get_data_event_sender()
                    .control()
                    .same_domain(original_data.control())
            );
            assert!(
                get_exec_event_sender()
                    .control()
                    .same_domain(original_exec.control())
            );

            let (isolated, _isolated_rx) = bounded_data_event_channel(16);
            let isolated_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                replace_data_event_sender(isolated);
            }));
            assert!(isolated_result.is_err());
            assert!(
                get_data_event_sender()
                    .control()
                    .same_domain(original_data.control())
            );

            let (wrong_tx, _wrong_rx) = tokio::sync::mpsc::channel::<ExecutionEvent>(16);
            let wrong_lane = LiveEventSender::bounded(
                wrong_tx,
                LiveIngressLane::DataEvents,
                bundle.control().clone(),
            );
            let wrong_lane_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                replace_exec_event_sender(wrong_lane);
            }));
            assert!(wrong_lane_result.is_err());
            assert!(
                get_exec_event_sender()
                    .control()
                    .same_domain(original_exec.control())
            );

            drop(bundle);
            assert!(try_get_data_event_sender().is_none());
            assert!(try_get_exec_event_sender().is_none());
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_live_ingress_bundle_is_not_send() {
        trait AmbiguousIfSend<A> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfSend<()> for T {}
        impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

        // Type inference is unambiguous only while `LiveIngressBundle: !Send`.
        let _ = <LiveIngressBundle as AmbiguousIfSend<_>>::check;
    }

    #[rstest]
    fn test_every_lane_overflow_signals_shared_failure_and_halts_peers() {
        let lanes = [
            LiveIngressLane::TimeEvents,
            LiveIngressLane::ExecEvents,
            LiveIngressLane::ExecCommands,
            LiveIngressLane::DataEvents,
            LiveIngressLane::DataCommands,
        ];

        for (failed_index, failed_lane) in lanes.into_iter().enumerate() {
            let control = LiveIngressControl::new();
            let mut senders = Vec::with_capacity(INGRESS_LANE_COUNT);
            let mut receivers = Vec::with_capacity(INGRESS_LANE_COUNT);
            for lane in lanes {
                let (tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
                senders.push(LiveEventSender::bounded(tx, lane, control.clone()));
                receivers.push(rx);
            }
            let failure_rx = control.subscribe();

            senders[failed_index].send(1).unwrap();
            assert!(matches!(
                senders[failed_index].send(2),
                Err(LiveIngressSendError::Full(2))
            ));
            let expected = LiveIngressFailure {
                lane: failed_lane,
                kind: LiveIngressFailureKind::Full,
            };
            assert_eq!(control.failure(), Some(expected));
            assert_eq!(*failure_rx.borrow(), Some(expected));

            let peer_index = (failed_index + 1) % INGRESS_LANE_COUNT;
            assert!(matches!(
                senders[peer_index].send(3),
                Err(LiveIngressSendError::FailStopped(3))
            ));
            assert_eq!(
                control.lane_metrics(failed_lane).overflows,
                1,
                "overflow metric missing for {failed_lane}"
            );
            drop(receivers);
        }
    }

    #[rstest]
    fn test_integrity_failure_is_sticky_and_halts_peer_lanes() {
        let control = LiveIngressControl::new();
        let (exec_tx, _exec_rx) = tokio::sync::mpsc::channel(1);
        let (data_tx, _data_rx) = tokio::sync::mpsc::channel(1);
        let exec_sender =
            LiveEventSender::bounded(exec_tx, LiveIngressLane::ExecEvents, control.clone());
        let data_sender =
            LiveEventSender::bounded(data_tx, LiveIngressLane::DataEvents, control.clone());

        control.fail_integrity(LiveIngressLane::ExecEvents);
        let expected = LiveIngressFailure {
            lane: LiveIngressLane::ExecEvents,
            kind: LiveIngressFailureKind::Integrity,
        };
        assert_eq!(control.failure(), Some(expected));
        assert!(matches!(
            exec_sender.send(1_u8),
            Err(LiveIngressSendError::FailStopped(1))
        ));
        assert!(matches!(
            data_sender.send(2_u8),
            Err(LiveIngressSendError::FailStopped(2))
        ));

        control.fail_closed(LiveIngressLane::DataEvents);
        assert_eq!(control.failure(), Some(expected));
    }

    #[rstest]
    fn test_bounded_sender_full_fail_stops_every_lane_and_records_metrics() {
        let control = LiveIngressControl::new();
        let (data_tx, _data_rx) = tokio::sync::mpsc::channel(1);
        let (exec_tx, _exec_rx) = tokio::sync::mpsc::channel(1);
        let data_sender =
            LiveEventSender::bounded(data_tx, LiveIngressLane::DataEvents, control.clone());
        let exec_sender =
            LiveEventSender::bounded(exec_tx, LiveIngressLane::ExecEvents, control.clone());

        data_sender.send(1_u8).unwrap();
        assert!(matches!(
            data_sender.send(2_u8),
            Err(LiveIngressSendError::Full(2))
        ));
        assert_eq!(
            control.failure(),
            Some(LiveIngressFailure {
                lane: LiveIngressLane::DataEvents,
                kind: LiveIngressFailureKind::Full,
            })
        );

        assert!(matches!(
            exec_sender.send(3_u8),
            Err(LiveIngressSendError::FailStopped(3))
        ));
        assert_eq!(
            control.lane_metrics(LiveIngressLane::DataEvents),
            LiveIngressLaneMetrics {
                capacity: 1,
                high_watermark: 1,
                overflows: 1,
                rejected_after_halt: 0,
                closed: 0,
            }
        );
        assert_eq!(
            control
                .lane_metrics(LiveIngressLane::ExecEvents)
                .rejected_after_halt,
            1
        );
    }

    #[tokio::test]
    async fn test_bounded_sender_failure_notification_is_sticky() {
        let control = LiveIngressControl::new();
        let mut failure_rx = control.subscribe();
        let (tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
        let sender = LiveEventSender::bounded(tx, LiveIngressLane::DataCommands, control.clone());
        drop(rx);

        assert!(matches!(
            sender.send(7),
            Err(LiveIngressSendError::Closed(7))
        ));
        failure_rx.changed().await.unwrap();
        assert_eq!(
            failure_rx.borrow_and_update().as_ref(),
            control.failure().as_ref()
        );
    }
}
