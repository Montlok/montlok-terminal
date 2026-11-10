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

//! Dedicated writer for the event store.
//!
//! The writer owns a single backend instance and exposes a thread-safe `submit` entry
//! point. Captured entries enter via a bounded `std::sync::mpsc::sync_channel`; each
//! `submit` waits for an immediate backend commit acknowledgement before returning. The
//! high-watermark only advances on durable acknowledgement; a stalled submit or a backend-side
//! disk/corruption failure fires the registered halt callback.
//!
//! Entries are authoritative write-ahead-log intents, not proof that every downstream handler
//! observed or applied them. A submit which times out may still commit atomically after the
//! caller has fail-stopped; cache bootstrap replay may apply that state intent on restart, but
//! external side effects must not be replayed without their own idempotency protocol.
//!
//! Under `cfg(madsim)` the writer drops the channel and the dedicated thread, mirroring
//! the logger's simulation pattern: submits commit synchronously on the calling thread so
//! tests assert against an authoritative in-process log without thread scheduling.

pub mod halt;

// `batcher` carries thread-loop helpers gated out under cfg(madsim) since the
// synchronous path bypasses the channel and the run loop, but `build_append_entry`
// is reused in both paths so the module stays compiled either way.
mod batcher;

use std::time::Duration;

use bytes::Bytes;
pub use halt::{HaltCallback, HaltReason, noop_halt};
use nautilus_core::UnixNanos;

use crate::{
    backend::IndexKey,
    entry::{PayloadType, Topic},
    headers::Headers,
    snapshot::SnapshotAnchor,
};

/// Default channel capacity for entries pending the writer thread.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;
/// Default end-to-end submit ceiling that fires the halt callback.
pub const DEFAULT_HALT_THRESHOLD: Duration = Duration::from_millis(250);

/// Configuration knobs for the writer.
#[derive(Clone, Copy, Debug)]
pub struct WriterConfig {
    /// Capacity of the bounded `sync_channel` between submit and the writer thread.
    pub channel_capacity: usize,
    /// End-to-end enqueue and durable-ack ceiling. A submit that takes longer than this
    /// fires the halt callback once and returns [`SubmitError::HaltSignaled`].
    pub halt_threshold: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            halt_threshold: DEFAULT_HALT_THRESHOLD,
        }
    }
}

/// An unsealed entry handed to [`EventStoreWriter::submit`].
///
/// `seq`, `ts_publish`, and `entry_hash` are stamped by the writer; everything else is the
/// captured message identity plus encoder output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryDraft {
    /// First-class correlation headers.
    pub headers: Headers,
    /// The bus topic the entry was captured on.
    pub topic: Topic,
    /// The canonical payload type tag.
    pub payload_type: PayloadType,
    /// The encoded payload bytes.
    pub payload: Bytes,
    /// The domain timestamp from `AtomicTime`.
    pub ts_init: UnixNanos,
    /// Sidecar index keys produced by the encoder.
    pub index_keys: Vec<IndexKey>,
}

impl EntryDraft {
    /// Creates a new [`EntryDraft`] with no sidecar index keys.
    #[must_use]
    pub const fn without_indices(
        headers: Headers,
        topic: Topic,
        payload_type: PayloadType,
        payload: Bytes,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            headers,
            topic,
            payload_type,
            payload,
            ts_init,
            index_keys: Vec::new(),
        }
    }
}

/// Errors returned by [`EventStoreWriter::submit`].
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// The writer is shut down or the writer thread has exited.
    #[error("writer is closed")]
    Closed,
    /// The submit exceeded its configured end-to-end deadline; the halt callback fired.
    #[error("submit stalled for {stalled_for:?}, halt threshold {threshold:?}")]
    HaltSignaled {
        /// How long the submit blocked before signaling halt.
        stalled_for: Duration,
        /// The configured threshold the stall exceeded.
        threshold: Duration,
    },
}

#[cfg(not(madsim))]
mod imp {
    use std::{
        fmt::Debug,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc::{self, RecvTimeoutError, SyncSender, TrySendError},
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    };

    use nautilus_core::time::AtomicTime;

    use super::{
        EntryDraft, SnapshotAnchor, SubmitError, WriterConfig,
        batcher::{self, HaltSink, WriterMessage},
        halt::{self, HaltCallback, HaltReason},
    };
    use crate::{backend::EventStore, error::EventStoreError};

    const WRITER_THREAD_NAME: &str = "event-store-writer";
    const SUBMIT_RETRY_INTERVAL: Duration = Duration::from_micros(100);

    /// The dedicated event store writer.
    pub struct EventStoreWriter {
        tx: Option<SyncSender<WriterMessage>>,
        handle: Option<JoinHandle<()>>,
        high_watermark: Arc<AtomicU64>,
        halt: HaltCallback,
        halt_threshold: Duration,
        // Shared with the writer thread so any halt fire latches it exactly once;
        // subsequent submits return Closed instead of re-entering the retry loop.
        halted: Arc<AtomicBool>,
        clock: &'static AtomicTime,
    }

    impl Debug for EventStoreWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct(stringify!(EventStoreWriter))
                .field(
                    "high_watermark",
                    &self.high_watermark.load(Ordering::Acquire),
                )
                .field("halt_threshold", &self.halt_threshold)
                .field("halted", &self.halted.load(Ordering::Acquire))
                .field("tx_attached", &self.tx.is_some())
                .finish_non_exhaustive()
        }
    }

    impl EventStoreWriter {
        /// Spawns the writer thread and takes ownership of `backend`.
        ///
        /// The backend must already have an open run; the writer reads its current
        /// high-watermark to seed the next assigned `seq`.
        ///
        /// # Errors
        ///
        /// Returns [`EventStoreError::Backend`] when the backend has no open run or when
        /// the writer thread cannot be spawned.
        pub fn spawn(
            backend: Box<dyn EventStore + Send>,
            clock: &'static AtomicTime,
            halt: HaltCallback,
            config: WriterConfig,
        ) -> Result<Self, EventStoreError> {
            let initial_hwm = backend.high_watermark()?;
            let high_watermark = Arc::new(AtomicU64::new(initial_hwm));
            let halted = Arc::new(AtomicBool::new(false));
            let (tx, rx) = mpsc::sync_channel::<WriterMessage>(config.channel_capacity);

            let watermark_for_thread = Arc::clone(&high_watermark);
            let halt_for_thread = Arc::clone(&halt);
            let halted_for_thread = Arc::clone(&halted);
            let halt_threshold = config.halt_threshold;
            let handle = thread::Builder::new()
                .name(WRITER_THREAD_NAME.to_string())
                .spawn(move || {
                    batcher::run(
                        backend,
                        rx,
                        HaltSink::new(halt_for_thread, halted_for_thread),
                        watermark_for_thread,
                        clock,
                    );
                })
                .map_err(|e| EventStoreError::Backend(format!("spawn writer thread: {e}")))?;

            Ok(Self {
                tx: Some(tx),
                handle: Some(handle),
                high_watermark,
                halt,
                halt_threshold,
                halted,
                clock,
            })
        }

        /// Submits a captured entry and waits until the backend durably commits it.
        /// `ts_publish` is stamped from the clock before enqueue.
        ///
        /// Blocks (with retry) when the channel is full. If the cumulative wait exceeds
        /// the halt threshold, signals halt, firing the callback unless an earlier
        /// condition already did, and returns [`SubmitError::HaltSignaled`];
        /// subsequent submits return [`SubmitError::Closed`] without blocking.
        ///
        /// The halt callback fires exactly once across the submit-side stall path and
        /// every writer-thread failure path; the first condition to fire wins the
        /// recorded reason.
        ///
        /// # Errors
        ///
        /// Returns [`SubmitError::Closed`] when the writer is shut down, the writer
        /// thread has exited, or a prior halt fired, and
        /// [`SubmitError::HaltSignaled`] when this submit's stall first crosses the
        /// configured halt threshold.
        pub fn submit(&self, draft: EntryDraft) -> Result<(), SubmitError> {
            self.submit_with_timeout(draft, self.halt_threshold)
        }

        /// Submits a captured entry with an explicit end-to-end deadline.
        ///
        /// This is used by kernel boot so the `RunStarted` contract is measured from the
        /// actual submit entry point, including channel admission and the backend commit.
        pub(crate) fn submit_with_timeout(
            &self,
            draft: EntryDraft,
            timeout: Duration,
        ) -> Result<(), SubmitError> {
            // Refuse further entries once a halt has been signaled, even if the
            // channel later drains: halt is terminal for the run.
            if self.halted.load(Ordering::Acquire) {
                return Err(SubmitError::Closed);
            }

            let tx = self.tx.as_ref().ok_or(SubmitError::Closed)?;
            let ts_publish = self.clock.get_time_ns();
            let (ack_tx, ack_rx) = mpsc::sync_channel::<Result<u64, EventStoreError>>(1);
            let pending = WriterMessage::Entry {
                draft,
                ts_publish,
                ack: ack_tx,
            };
            let start = Instant::now();

            match self.enqueue_with_backpressure(tx, pending, start, timeout) {
                Ok(()) => {
                    let remaining = timeout.saturating_sub(start.elapsed());
                    match ack_rx.recv_timeout(remaining) {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) | Err(RecvTimeoutError::Disconnected) => {
                            Err(SubmitError::Closed)
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            let elapsed = start.elapsed();
                            self.signal_backpressure_stall(elapsed, timeout);
                            Err(SubmitError::HaltSignaled {
                                stalled_for: elapsed,
                                threshold: timeout,
                            })
                        }
                    }
                }
                Err(EnqueueFailure::Stalled(elapsed)) => Err(SubmitError::HaltSignaled {
                    stalled_for: elapsed,
                    threshold: timeout,
                }),
                Err(EnqueueFailure::Closed) => Err(SubmitError::Closed),
            }
        }

        /// Returns the largest seq durably acknowledged by the backend.
        ///
        /// Updated only after a successful `append_batch` ack; reflects what is safe to
        /// anchor a snapshot against.
        #[must_use]
        pub fn high_watermark(&self) -> u64 {
            self.high_watermark.load(Ordering::Acquire)
        }

        /// Flushes pending entries and records a snapshot anchor at the durable
        /// high-watermark.
        ///
        /// The cache owns `blob_ref` and `content_hash`; the writer derives the
        /// high-watermark only after earlier submitted entries have committed, so the
        /// anchor never points past durable event-store state.
        ///
        /// # Errors
        ///
        /// Returns [`EventStoreError::Closed`] when the writer is closed or halted, and
        /// forwards backend errors when flushing pending entries or recording the anchor
        /// fails.
        pub fn record_snapshot_anchor(
            &self,
            blob_ref: impl Into<String>,
            content_hash: impl Into<String>,
        ) -> Result<SnapshotAnchor, EventStoreError> {
            if self.halted.load(Ordering::Acquire) {
                return Err(EventStoreError::Closed);
            }

            let tx = self.tx.as_ref().ok_or(EventStoreError::Closed)?;
            let (ack_tx, ack_rx) = mpsc::sync_channel::<Result<SnapshotAnchor, EventStoreError>>(1);
            let pending = WriterMessage::RecordSnapshotAnchor {
                blob_ref: blob_ref.into(),
                content_hash: content_hash.into(),
                ack: ack_tx,
            };
            let start = Instant::now();

            if let Err(e) = self.enqueue_with_backpressure(tx, pending, start, self.halt_threshold)
            {
                match e {
                    EnqueueFailure::Stalled(elapsed) => {
                        return Err(EventStoreError::Backend(format!(
                            "snapshot anchor submit stalled for {elapsed:?}, halt threshold {:?}",
                            self.halt_threshold,
                        )));
                    }
                    EnqueueFailure::Closed => return Err(EventStoreError::Closed),
                }
            }

            let remaining = self.halt_threshold.saturating_sub(start.elapsed());
            match ack_rx.recv_timeout(remaining) {
                Ok(result) => result,
                Err(RecvTimeoutError::Timeout) => {
                    let elapsed = start.elapsed();
                    self.signal_backpressure_stall(elapsed, self.halt_threshold);
                    Err(EventStoreError::Backend(format!(
                        "snapshot anchor ack stalled for {elapsed:?}, halt threshold {:?}",
                        self.halt_threshold,
                    )))
                }
                Err(RecvTimeoutError::Disconnected) => Err(EventStoreError::Backend(
                    "snapshot anchor ack channel disconnected".to_string(),
                )),
            }
        }

        fn enqueue_with_backpressure(
            &self,
            tx: &SyncSender<WriterMessage>,
            mut pending: WriterMessage,
            start: Instant,
            timeout: Duration,
        ) -> Result<(), EnqueueFailure> {
            // Check elapsed before each try_send (including after a sleep) so that a
            // stall which exceeds the threshold fires halt even when the next attempt
            // would have succeeded. The first iteration's elapsed is ~0, so it falls
            // through to try_send.
            loop {
                // A halt latched while this submit waited is terminal: refuse the
                // entry instead of accepting one the doomed writer thread would drop.
                if self.halted.load(Ordering::Acquire) {
                    return Err(EnqueueFailure::Closed);
                }

                let elapsed = start.elapsed();

                if elapsed >= timeout {
                    self.signal_backpressure_stall(elapsed, timeout);
                    return Err(EnqueueFailure::Stalled(elapsed));
                }

                match tx.try_send(pending) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(returned)) => {
                        pending = returned;
                        thread::sleep(SUBMIT_RETRY_INTERVAL);
                    }
                    Err(TrySendError::Disconnected(_)) => return Err(EnqueueFailure::Closed),
                }
            }
        }

        fn signal_backpressure_stall(&self, stalled_for: Duration, threshold: Duration) {
            halt::fire_once(
                &self.halt,
                &self.halted,
                HaltReason::BackpressureStall {
                    stalled_for,
                    threshold,
                },
            );
        }

        /// Drains the channel, commits `run_ended` as the final entry, and seals the
        /// manifest with [`crate::manifest::RunStatus::Ended`]. Channel admission,
        /// durable append, and seal acknowledgement share the writer's end-to-end
        /// halt deadline.
        ///
        /// Consumes the writer; further submits are unrepresentable.
        ///
        /// # Errors
        ///
        /// Returns [`EventStoreError`] when the writer thread fails to commit the final
        /// batch, when seal fails, or when the writer thread panicked.
        pub fn close(mut self, run_ended: EntryDraft) -> Result<u64, EventStoreError> {
            let tx = self
                .tx
                .take()
                .ok_or_else(|| EventStoreError::Backend("writer already closed".to_string()))?;

            let (ack_tx, ack_rx) = mpsc::sync_channel::<Result<u64, EventStoreError>>(1);
            let pending = WriterMessage::Close {
                run_ended,
                ack: ack_tx,
            };
            let start = Instant::now();

            if let Err(error) =
                self.enqueue_with_backpressure(&tx, pending, start, self.halt_threshold)
            {
                drop(tx);
                return match error {
                    EnqueueFailure::Stalled(elapsed) => Err(EventStoreError::Backend(format!(
                        "writer close submit stalled for {elapsed:?}, halt threshold {:?}",
                        self.halt_threshold,
                    ))),
                    EnqueueFailure::Closed => Err(EventStoreError::Backend(
                        "writer thread disconnected before close".to_string(),
                    )),
                };
            }
            // Drop the producer so the writer's recv loop will see Disconnected after it
            // finishes Close handling, even if the path through Close returns earlier.
            drop(tx);

            let remaining = self.halt_threshold.saturating_sub(start.elapsed());
            let result = match ack_rx.recv_timeout(remaining) {
                Ok(result) => result,
                Err(RecvTimeoutError::Timeout) => {
                    let elapsed = start.elapsed();
                    self.signal_backpressure_stall(elapsed, self.halt_threshold);
                    return Err(EventStoreError::Backend(format!(
                        "writer close acknowledgement stalled for {elapsed:?}, halt threshold {:?}",
                        self.halt_threshold,
                    )));
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(EventStoreError::Backend(
                        "writer close acknowledgement channel disconnected".to_string(),
                    ));
                }
            };

            if let Some(handle) = self.handle.take() {
                handle
                    .join()
                    .map_err(|_| EventStoreError::Backend("writer thread panicked".to_string()))?;
            }
            result
        }
    }

    enum EnqueueFailure {
        Stalled(Duration),
        Closed,
    }

    impl Drop for EventStoreWriter {
        fn drop(&mut self) {
            // Implicit drop without close(): release the producer so the writer thread
            // exits cleanly, leaving the manifest unsealed so a later open of the same
            // run observes a CrashedPredecessor.
            self.tx.take();

            if let Some(handle) = self.handle.take() {
                if self.halted.load(Ordering::Acquire) {
                    // A timed-out synchronous backend operation cannot be cancelled through the
                    // current EventStore trait. Joining here would turn a bounded fail-stop into
                    // an unbounded shutdown. Dropping the handle deliberately detaches only this
                    // poisoned writer. The process supervisor must terminate/restart the process;
                    // continuing to trade in the same process is unsupported after a writer halt.
                    drop(handle);
                } else {
                    let _ = handle.join();
                }
            }
        }
    }
}

#[cfg(madsim)]
mod imp {
    use std::{
        fmt::Debug,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use nautilus_core::time::AtomicTime;

    use super::{
        EntryDraft, SnapshotAnchor, SubmitError, WriterConfig, batcher,
        halt::{HaltCallback, HaltReason},
    };
    use crate::{backend::EventStore, error::EventStoreError, manifest::RunStatus};

    /// Synchronous, in-thread writer used under simulation.
    ///
    /// The dedicated thread and bounded channel are dropped: each `submit` commits a
    /// single-entry batch on the calling thread so tests can assert against the
    /// authoritative in-process log without thread scheduling.
    pub struct EventStoreWriter {
        inner: Mutex<Inner>,
        high_watermark: Arc<AtomicU64>,
        halt: HaltCallback,
        clock: &'static AtomicTime,
    }

    impl Debug for EventStoreWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct(stringify!(EventStoreWriter))
                .field(
                    "high_watermark",
                    &self.high_watermark.load(Ordering::Acquire),
                )
                .finish_non_exhaustive()
        }
    }

    struct Inner {
        backend: Box<dyn EventStore + Send>,
        next_seq: u64,
        closed: bool,
    }

    impl EventStoreWriter {
        /// Constructs a synchronous writer over `backend`.
        ///
        /// # Errors
        ///
        /// Returns [`EventStoreError::Backend`] when the backend has no open run.
        #[expect(
            clippy::needless_pass_by_value,
            reason = "synchronous writer keeps ownership of the backend and halt callback"
        )]
        pub fn spawn(
            backend: Box<dyn EventStore + Send>,
            clock: &'static AtomicTime,
            halt: HaltCallback,
            _config: WriterConfig,
        ) -> Result<Self, EventStoreError> {
            let initial_hwm = backend.high_watermark()?;
            let high_watermark = Arc::new(AtomicU64::new(initial_hwm));
            let inner = Inner {
                backend,
                next_seq: initial_hwm + 1,
                closed: false,
            };

            Ok(Self {
                inner: Mutex::new(inner),
                high_watermark,
                halt,
                clock,
            })
        }

        /// Commits `draft` synchronously as a single-entry batch.
        ///
        /// # Errors
        ///
        /// Returns [`SubmitError::Closed`] if the writer has been closed or fail-stopped.
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned by a panic on a prior submit.
        pub fn submit(&self, draft: EntryDraft) -> Result<(), SubmitError> {
            let mut inner = self.inner.lock().expect("writer mutex poisoned");

            if inner.closed {
                return Err(SubmitError::Closed);
            }

            let ts_publish = self.clock.get_time_ns();
            let seq = inner.next_seq;
            let append = batcher::build_append_entry(draft, ts_publish, seq);

            match inner.backend.append_batch(std::slice::from_ref(&append)) {
                Ok(new_hwm) => {
                    inner.next_seq = seq + 1;
                    self.high_watermark.store(new_hwm, Ordering::Release);
                    Ok(())
                }
                Err(e) => {
                    (self.halt)(HaltReason::from_backend_error(&e));
                    inner.closed = true;
                    Err(SubmitError::Closed)
                }
            }
        }

        /// Returns the largest seq durably acknowledged by the backend.
        #[must_use]
        pub fn high_watermark(&self) -> u64 {
            self.high_watermark.load(Ordering::Acquire)
        }

        /// Records a snapshot anchor at the current durable high-watermark.
        ///
        /// # Errors
        ///
        /// Returns [`EventStoreError::Closed`] when the writer has closed, and forwards
        /// backend errors when recording the anchor fails.
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned by a panic on a prior submit.
        pub fn record_snapshot_anchor(
            &self,
            blob_ref: impl Into<String>,
            content_hash: impl Into<String>,
        ) -> Result<SnapshotAnchor, EventStoreError> {
            let mut inner = self.inner.lock().expect("writer mutex poisoned");

            if inner.closed {
                return Err(EventStoreError::Closed);
            }

            let anchor = SnapshotAnchor::new(
                self.high_watermark.load(Ordering::Acquire),
                blob_ref,
                content_hash,
            );

            match inner.backend.record_snapshot_anchor(anchor.clone()) {
                Ok(()) => Ok(anchor),
                Err(e) => {
                    (self.halt)(HaltReason::from_backend_error(&e));
                    inner.closed = true;
                    Err(e)
                }
            }
        }

        /// Commits `run_ended` synchronously as the final entry and seals the manifest.
        ///
        /// # Errors
        ///
        /// Returns [`EventStoreError`] when the final commit or seal fails.
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned by a panic on a prior submit.
        pub fn close(self, run_ended: EntryDraft) -> Result<u64, EventStoreError> {
            let mut inner = self.inner.lock().expect("writer mutex poisoned");

            if inner.closed {
                return Err(EventStoreError::Backend(
                    "writer already closed".to_string(),
                ));
            }

            let ts_publish = self.clock.get_time_ns();
            let seq = inner.next_seq;
            let append = batcher::build_append_entry(run_ended, ts_publish, seq);

            match inner.backend.append_batch(std::slice::from_ref(&append)) {
                Ok(new_hwm) => {
                    inner.next_seq = seq + 1;
                    self.high_watermark.store(new_hwm, Ordering::Release);
                }
                Err(e) => {
                    (self.halt)(HaltReason::from_backend_error(&e));
                    inner.closed = true;
                    return Err(e);
                }
            }

            match inner.backend.seal(RunStatus::Ended) {
                Ok(()) => {
                    inner.closed = true;
                    Ok(self.high_watermark.load(Ordering::Acquire))
                }
                Err(e) => {
                    (self.halt)(HaltReason::from_backend_error(&e));
                    inner.closed = true;
                    Err(e)
                }
            }
        }
    }
}

pub use imp::EventStoreWriter;

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use indexmap::IndexMap;
    use nautilus_core::{UnixNanos, time::get_atomic_clock_static};
    use rstest::{fixture, rstest};
    use ustr::Ustr;

    use super::*;
    use crate::{
        backend::{AppendEntry, EventStore, IndexKind, MemoryBackend, ScanDirection},
        entry::EventStoreEntry,
        error::EventStoreError,
        manifest::{RegisteredComponents, RunManifest, RunStatus},
    };

    fn manifest(run_id: &str) -> RunManifest {
        RunManifest {
            run_id: run_id.to_string(),
            parent_run_id: None,
            instance_id: "trader-001".to_string(),
            binary_hash: "deadbeef".to_string(),
            schema_version: 1,
            crate_versions: "feedface".to_string(),
            feature_flags: Vec::new(),
            adapter_versions: IndexMap::new(),
            config_hash: "cafebabe".to_string(),
            provenance: nautilus_provenance::RunProvenance::default(),
            registered_components: RegisteredComponents::default(),
            seed: None,
            start_ts_init: UnixNanos::from(0),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
        }
    }

    fn entry_draft(ts_init: u64) -> EntryDraft {
        EntryDraft {
            headers: Headers::empty(),
            topic: "exec.command.SubmitOrder".into(),
            payload_type: Ustr::from("SubmitOrder"),
            payload: Bytes::from_static(b"\x01\x02\x03\x04"),
            ts_init: UnixNanos::from(ts_init),
            index_keys: Vec::new(),
        }
    }

    fn run_ended_draft() -> EntryDraft {
        EntryDraft {
            headers: Headers::empty(),
            topic: "run.lifecycle.RunEnded".into(),
            payload_type: Ustr::from("RunEnded"),
            payload: Bytes::new(),
            ts_init: UnixNanos::from(9_999),
            index_keys: Vec::new(),
        }
    }

    /// Wraps `MemoryBackend` so tests can read the same instance the writer thread
    /// commits into.
    #[derive(Debug)]
    struct SharedMemory(Arc<Mutex<MemoryBackend>>);

    impl SharedMemory {
        fn new() -> (Self, Arc<Mutex<MemoryBackend>>) {
            let arc = Arc::new(Mutex::new(MemoryBackend::new()));
            (Self(Arc::clone(&arc)), arc)
        }
    }

    impl EventStore for SharedMemory {
        fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
            // Tests open the underlying backend directly.
            unreachable!("test wrapper does not forward open_run")
        }

        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .append_batch(entries)
        }

        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .scan_range(from, to, direction)
        }

        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.0.lock().expect("shared memory poisoned").scan_seq(seq)
        }

        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .lookup(kind, key)
        }

        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .iter_index_keys(kind)
        }

        fn record_snapshot_anchor(
            &mut self,
            anchor: SnapshotAnchor,
        ) -> Result<(), EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .record_snapshot_anchor(anchor)
        }

        fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .latest_snapshot_anchor()
        }

        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.0.lock().expect("shared memory poisoned").seal(status)
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.0.lock().expect("shared memory poisoned").manifest()
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .high_watermark()
        }
    }

    /// `EventStore` wrapper that blocks `append_batch` until a release flag flips.
    #[derive(Debug)]
    struct BlockingBackend {
        inner: Arc<Mutex<MemoryBackend>>,
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        appends_seen: Arc<AtomicUsize>,
        seal_gate: Option<Arc<(Mutex<bool>, std::sync::Condvar)>>,
        seals_seen: Option<Arc<AtomicUsize>>,
    }

    impl BlockingBackend {
        fn new(
            inner: Arc<Mutex<MemoryBackend>>,
            gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
            appends_seen: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                inner,
                gate,
                appends_seen,
                seal_gate: None,
                seals_seen: None,
            }
        }

        fn with_blocked_seal(
            inner: Arc<Mutex<MemoryBackend>>,
            seal_gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
            seals_seen: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                inner,
                gate: Arc::new((Mutex::new(true), std::sync::Condvar::new())),
                appends_seen: Arc::new(AtomicUsize::new(0)),
                seal_gate: Some(seal_gate),
                seals_seen: Some(seals_seen),
            }
        }
    }

    impl EventStore for BlockingBackend {
        fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
            unreachable!("test wrapper does not forward open_run")
        }

        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.appends_seen.fetch_add(1, Ordering::SeqCst);
            let (lock, cvar) = &*self.gate;
            let mut released = lock.lock().expect("gate poisoned");

            while !*released {
                released = cvar.wait(released).expect("gate wait");
            }
            self.inner
                .lock()
                .expect("inner poisoned")
                .append_batch(entries)
        }

        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .scan_range(from, to, direction)
        }

        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.inner.lock().expect("inner poisoned").scan_seq(seq)
        }

        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.inner.lock().expect("inner poisoned").lookup(kind, key)
        }

        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .iter_index_keys(kind)
        }

        fn record_snapshot_anchor(
            &mut self,
            anchor: SnapshotAnchor,
        ) -> Result<(), EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .record_snapshot_anchor(anchor)
        }

        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            if let Some(seals_seen) = &self.seals_seen {
                seals_seen.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(gate) = &self.seal_gate {
                let (lock, cvar) = &**gate;
                let mut released = lock.lock().expect("seal gate poisoned");

                while !*released {
                    released = cvar.wait(released).expect("seal gate wait");
                }
            }
            self.inner.lock().expect("inner poisoned").seal(status)
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.inner.lock().expect("inner poisoned").manifest()
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.inner.lock().expect("inner poisoned").high_watermark()
        }
    }

    /// `EventStore` wrapper that returns `EventStoreError::Disk` for every append.
    #[derive(Debug, Default)]
    struct DiskFailureBackend {
        appends_seen: Arc<AtomicUsize>,
    }

    impl EventStore for DiskFailureBackend {
        fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
            Ok(())
        }

        fn append_batch(&mut self, _: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.appends_seen.fetch_add(1, Ordering::SeqCst);
            Err(EventStoreError::Disk("ENOSPC".to_string()))
        }

        fn scan_range(
            &self,
            _: u64,
            _: u64,
            _: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            Ok(Vec::new())
        }

        fn scan_seq(&self, _: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            Ok(None)
        }

        fn lookup(&self, _: IndexKind, _: &str) -> Result<Option<u64>, EventStoreError> {
            Ok(None)
        }

        fn iter_index_keys(&self, _: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            Ok(Vec::new())
        }

        fn seal(&mut self, _: RunStatus) -> Result<(), EventStoreError> {
            Ok(())
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            Err(EventStoreError::Backend("disk failure".to_string()))
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            Ok(0)
        }
    }

    /// `EventStore` wrapper that blocks `append_batch` at a gate, then fails with
    /// `EventStoreError::Disk` once released.
    #[derive(Debug)]
    struct GatedDiskFailureBackend {
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        appends_seen: Arc<AtomicUsize>,
    }

    impl EventStore for GatedDiskFailureBackend {
        fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
            Ok(())
        }

        fn append_batch(&mut self, _: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.appends_seen.fetch_add(1, Ordering::SeqCst);
            let (lock, cvar) = &*self.gate;
            let mut released = lock.lock().expect("gate poisoned");

            while !*released {
                released = cvar.wait(released).expect("gate wait");
            }
            Err(EventStoreError::Disk("ENOSPC".to_string()))
        }

        fn scan_range(
            &self,
            _: u64,
            _: u64,
            _: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            Ok(Vec::new())
        }

        fn scan_seq(&self, _: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            Ok(None)
        }

        fn lookup(&self, _: IndexKind, _: &str) -> Result<Option<u64>, EventStoreError> {
            Ok(None)
        }

        fn iter_index_keys(&self, _: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            Ok(Vec::new())
        }

        fn seal(&mut self, _: RunStatus) -> Result<(), EventStoreError> {
            Ok(())
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            Err(EventStoreError::Backend("disk failure".to_string()))
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            Ok(0)
        }
    }

    /// `EventStore` wrapper that commits entries normally but blocks snapshot-anchor
    /// persistence until a release flag flips.
    #[derive(Debug)]
    struct BlockingSnapshotBackend {
        inner: Arc<Mutex<MemoryBackend>>,
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        anchors_seen: Arc<AtomicUsize>,
    }

    impl EventStore for BlockingSnapshotBackend {
        fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
            unreachable!("test wrapper does not forward open_run")
        }

        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .append_batch(entries)
        }

        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .scan_range(from, to, direction)
        }

        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.inner.lock().expect("inner poisoned").scan_seq(seq)
        }

        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.inner.lock().expect("inner poisoned").lookup(kind, key)
        }

        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .iter_index_keys(kind)
        }

        fn record_snapshot_anchor(
            &mut self,
            anchor: SnapshotAnchor,
        ) -> Result<(), EventStoreError> {
            self.anchors_seen.fetch_add(1, Ordering::SeqCst);
            let (lock, cvar) = &*self.gate;
            let mut released = lock.lock().expect("gate poisoned");
            while !*released {
                released = cvar.wait(released).expect("gate wait");
            }
            self.inner
                .lock()
                .expect("inner poisoned")
                .record_snapshot_anchor(anchor)
        }

        fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
            self.inner
                .lock()
                .expect("inner poisoned")
                .latest_snapshot_anchor()
        }

        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.inner.lock().expect("inner poisoned").seal(status)
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.inner.lock().expect("inner poisoned").manifest()
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.inner.lock().expect("inner poisoned").high_watermark()
        }
    }

    #[fixture]
    fn captured_halt() -> (HaltCallback, Arc<Mutex<Vec<HaltReason>>>) {
        let captured: Arc<Mutex<Vec<HaltReason>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_for_cb = Arc::clone(&captured);
        let halt: HaltCallback = Arc::new(move |reason| {
            captured_for_cb
                .lock()
                .expect("captured halt poisoned")
                .push(reason);
        });
        (halt, captured)
    }

    #[rstest]
    fn submit_then_close_records_entries_and_seals(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        let (halt, captured) = captured_halt;
        let (wrapper, shared) = SharedMemory::new();
        shared
            .lock()
            .expect("shared")
            .open_run(manifest("run-1"))
            .expect("open");

        let writer = EventStoreWriter::spawn(
            Box::new(wrapper),
            get_atomic_clock_static(),
            halt,
            WriterConfig::default(),
        )
        .expect("spawn");

        for ts in 10_u64..15_u64 {
            writer.submit(entry_draft(ts)).expect("submit");
        }

        let final_hwm = writer.close(run_ended_draft()).expect("close");

        // Five drafts plus the RunEnded entry.
        assert_eq!(final_hwm, 6);
        let backend = shared.lock().expect("shared");
        let m = backend.manifest().expect("manifest");
        assert_eq!(m.status, RunStatus::Ended);
        assert_eq!(m.high_watermark, 6);

        let last = backend.scan_seq(6).expect("scan").expect("present");
        assert_eq!(last.payload_type.as_str(), "RunEnded");
        assert!(captured.lock().expect("captured").is_empty());
    }

    #[rstest]
    fn record_snapshot_anchor_flushes_pending_entries_and_replay_tail_starts_after_anchor(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        let (halt, captured) = captured_halt;
        let (wrapper, shared) = SharedMemory::new();
        shared
            .lock()
            .expect("shared")
            .open_run(manifest("run-anchor"))
            .expect("open");

        let writer = EventStoreWriter::spawn(
            Box::new(wrapper),
            get_atomic_clock_static(),
            halt,
            WriterConfig::default(),
        )
        .expect("spawn");

        writer.submit(entry_draft(10)).expect("submit first");
        writer.submit(entry_draft(11)).expect("submit second");
        let anchor = writer
            .record_snapshot_anchor("cache://position-snapshots/P-1/0", "blake3:abc")
            .expect("record anchor");

        assert_eq!(anchor.high_watermark, 2);

        writer.submit(entry_draft(12)).expect("submit third");
        writer.submit(entry_draft(13)).expect("submit fourth");
        let final_hwm = writer.close(run_ended_draft()).expect("close");

        let backend = shared.lock().expect("shared");
        assert_eq!(
            backend.latest_snapshot_anchor().expect("latest anchor"),
            Some(anchor.clone()),
        );

        let tail_seqs: Vec<_> = backend
            .scan_range(anchor.high_watermark + 1, final_hwm, ScanDirection::Forward)
            .expect("scan tail")
            .into_iter()
            .map(|entry| entry.seq)
            .collect();

        assert_eq!(tail_seqs, vec![3, 4, 5]);
        assert!(captured.lock().expect("captured").is_empty());
    }

    #[rstest]
    fn submit_waits_for_each_durable_commit(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        // The authoritative dispatch path uses a per-entry commit barrier: a sequential submit
        // cannot return while its entry is only queued.
        let (halt, _) = captured_halt;
        let inner = Arc::new(Mutex::new(MemoryBackend::new()));
        inner
            .lock()
            .expect("inner")
            .open_run(manifest("run-batch"))
            .expect("open");

        let appends_seen = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(true), std::sync::Condvar::new()));
        let backend = BlockingBackend::new(
            Arc::clone(&inner),
            Arc::clone(&gate),
            Arc::clone(&appends_seen),
        );

        let config = WriterConfig {
            channel_capacity: 16,
            halt_threshold: Duration::from_secs(30),
        };

        let clock = get_atomic_clock_static();
        let boxed = Box::new(backend);

        let writer = EventStoreWriter::spawn(boxed, clock, halt, config).expect("spawn");

        for ts in 10_u64..16_u64 {
            writer.submit(entry_draft(ts)).expect("submit");
        }

        let final_hwm = writer.close(run_ended_draft()).expect("close");

        // Six acknowledged submits plus RunEnded are seven durable commits.
        assert_eq!(final_hwm, 7);
        assert_eq!(appends_seen.load(Ordering::SeqCst), 7);
    }

    #[rstest]
    fn submit_signals_halt_when_stalled_past_threshold(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        // A submit does not return merely because its entry reached the writer thread.
        // If the backend does not acknowledge the append by the deadline, that submit
        // fires the once-only halt and all later submissions are terminally closed.
        let (halt, captured) = captured_halt;
        let inner = Arc::new(Mutex::new(MemoryBackend::new()));
        inner
            .lock()
            .expect("inner")
            .open_run(manifest("run-halt"))
            .expect("open");

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let appends_seen = Arc::new(AtomicUsize::new(0));
        let backend = BlockingBackend::new(
            Arc::clone(&inner),
            Arc::clone(&gate),
            Arc::clone(&appends_seen),
        );

        let halt_threshold = Duration::from_millis(50);
        let config = WriterConfig {
            channel_capacity: 1,
            halt_threshold,
        };

        let clock = get_atomic_clock_static();
        let boxed = Box::new(backend);

        let writer = EventStoreWriter::spawn(boxed, clock, halt, config).expect("spawn");

        let stalled = std::thread::scope(|scope| {
            let submit = scope.spawn(|| writer.submit(entry_draft(10)));
            wait_for_counter(&appends_seen, 1, "writer did not enter gated append");
            submit.join().expect("submit thread panicked")
        })
        .expect_err("durable acknowledgement must time out");

        match stalled {
            SubmitError::HaltSignaled { .. } => {}
            SubmitError::Closed => panic!("expected HaltSignaled, was Closed"),
        }
        let captured_reasons = captured.lock().expect("captured");
        assert_eq!(
            captured_reasons.len(),
            1,
            "halt callback must fire exactly once",
        );
        assert_backpressure_stall(captured_reasons.first(), halt_threshold);
        drop(captured_reasons);

        // After a backpressure halt has fired, subsequent submits must reject without
        // re-entering the retry loop, even though the channel and writer thread are
        // still alive.
        let post_halt = writer
            .submit(entry_draft(11))
            .expect_err("post-halt submit");

        match post_halt {
            SubmitError::Closed => {}
            SubmitError::HaltSignaled { .. } => {
                panic!("expected Closed after halt, was HaltSignaled")
            }
        }
        // The halt callback must not refire on subsequent submits.
        assert_eq!(
            captured.lock().expect("captured").len(),
            1,
            "halt callback must not refire after the first stall",
        );

        // Release the gate so the writer thread can finish and the test can drop the
        // writer cleanly.
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        drop(writer);
    }

    #[rstest]
    fn dropping_poisoned_writer_is_bounded_when_backend_never_returns(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        // The synchronous EventStore trait cannot cancel an append already inside a backend.
        // Once the acknowledgement deadline poisons the writer, Drop must detach that thread
        // instead of silently turning the bounded fail-stop into an unbounded join.
        let (halt, captured) = captured_halt;
        let inner = Arc::new(Mutex::new(MemoryBackend::new()));
        inner
            .lock()
            .expect("inner")
            .open_run(manifest("run-permanent-stall"))
            .expect("open");

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let appends_seen = Arc::new(AtomicUsize::new(0));
        let backend = BlockingBackend::new(inner, gate, Arc::clone(&appends_seen));
        let halt_threshold = Duration::from_millis(20);
        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            halt,
            WriterConfig {
                channel_capacity: 1,
                halt_threshold,
            },
        )
        .expect("spawn");

        let started = std::time::Instant::now();
        let err = writer
            .submit(entry_draft(10))
            .expect_err("permanently blocked append must hit its deadline");
        assert!(matches!(err, SubmitError::HaltSignaled { .. }));
        assert_eq!(appends_seen.load(Ordering::SeqCst), 1);

        drop(writer);

        assert!(
            started.elapsed() < Duration::from_millis(500),
            "poisoned Drop must detach rather than join a permanently blocked backend",
        );
        assert_eq!(captured.lock().expect("captured").len(), 1);
    }

    #[rstest]
    fn close_is_bounded_when_run_ended_append_never_returns(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        let (halt, captured) = captured_halt;
        let inner = Arc::new(Mutex::new(MemoryBackend::new()));
        inner
            .lock()
            .expect("inner")
            .open_run(manifest("run-close-append-stall"))
            .expect("open");
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let appends_seen = Arc::new(AtomicUsize::new(0));
        let backend = BlockingBackend::new(inner, gate, Arc::clone(&appends_seen));
        let halt_threshold = Duration::from_millis(20);
        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            halt,
            WriterConfig {
                channel_capacity: 1,
                halt_threshold,
            },
        )
        .expect("spawn");

        let started = std::time::Instant::now();
        let error = writer
            .close(run_ended_draft())
            .expect_err("blocked RunEnded append must hit the close deadline");

        assert!(error.to_string().contains("close acknowledgement stalled"));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(appends_seen.load(Ordering::SeqCst), 1);
        assert_backpressure_stall(captured.lock().expect("captured").first(), halt_threshold);
    }

    #[rstest]
    fn close_is_bounded_when_manifest_seal_never_returns(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        let (halt, captured) = captured_halt;
        let inner = Arc::new(Mutex::new(MemoryBackend::new()));
        inner
            .lock()
            .expect("inner")
            .open_run(manifest("run-close-seal-stall"))
            .expect("open");
        let seal_gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let seals_seen = Arc::new(AtomicUsize::new(0));
        let backend = BlockingBackend::with_blocked_seal(
            Arc::clone(&inner),
            seal_gate,
            Arc::clone(&seals_seen),
        );
        let halt_threshold = Duration::from_millis(20);
        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            halt,
            WriterConfig {
                channel_capacity: 1,
                halt_threshold,
            },
        )
        .expect("spawn");

        let started = std::time::Instant::now();
        let error = writer
            .close(run_ended_draft())
            .expect_err("blocked manifest seal must hit the close deadline");

        assert!(error.to_string().contains("close acknowledgement stalled"));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(seals_seen.load(Ordering::SeqCst), 1);
        assert_eq!(
            inner.lock().expect("inner").high_watermark().expect("hwm"),
            1,
            "RunEnded must be atomically visible even when the following seal stalls",
        );
        assert_backpressure_stall(captured.lock().expect("captured").first(), halt_threshold);
    }

    #[rstest]
    fn halt_fires_once_across_stall_and_backend_failure(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        // The submit-side acknowledgement timeout fires first. When the gate is later
        // released and the backend reports a disk failure, the shared latch must keep
        // the original reason and suppress a second callback.
        let (halt, captured) = captured_halt;
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let appends_seen = Arc::new(AtomicUsize::new(0));
        let backend = GatedDiskFailureBackend {
            gate: Arc::clone(&gate),
            appends_seen: Arc::clone(&appends_seen),
        };

        let halt_threshold = Duration::from_millis(50);
        let config = WriterConfig {
            channel_capacity: 1,
            halt_threshold,
        };

        let clock = get_atomic_clock_static();
        let boxed = Box::new(backend);

        let writer = EventStoreWriter::spawn(boxed, clock, halt, config).expect("spawn");

        let stalled = std::thread::scope(|scope| {
            let submit = scope.spawn(|| writer.submit(entry_draft(10)));
            wait_for_counter(&appends_seen, 1, "writer did not enter gated append");
            submit.join().expect("submit thread panicked")
        })
        .expect_err("durable acknowledgement must time out");
        assert!(
            matches!(stalled, SubmitError::HaltSignaled { .. }),
            "was {stalled:?}",
        );

        // Release the gate so the append fails with Disk; without the latch this
        // fires a second, misclassified halt.
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();

        // Dropping joins the writer thread, so the failure has been observed
        drop(writer);

        let reasons = captured.lock().expect("captured");
        assert_eq!(
            reasons.len(),
            1,
            "halt must fire exactly once across stall and backend failure",
        );
        assert_backpressure_stall(reasons.first(), halt_threshold);
    }

    #[rstest]
    fn submit_after_writer_thread_halt_returns_closed(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        // A writer-thread halt latches the shared flag; post-halt submits must
        // reject rather than be accepted and silently dropped.
        let (halt, captured) = captured_halt;
        let config = WriterConfig::default();

        let writer = EventStoreWriter::spawn(
            Box::new(DiskFailureBackend::default()),
            get_atomic_clock_static(),
            halt,
            config,
        )
        .expect("spawn");

        let first = writer
            .submit(entry_draft(10))
            .expect_err("failed durable append must close submit");
        assert!(matches!(first, SubmitError::Closed), "was {first:?}");

        let reasons = captured.lock().expect("captured");
        assert_eq!(reasons.len(), 1, "writer-thread halt did not fire");
        assert!(
            matches!(reasons.first(), Some(HaltReason::BackendDisk(_))),
            "was {:?}",
            reasons.first(),
        );
        drop(reasons);

        let post_halt = writer
            .submit(entry_draft(11))
            .expect_err("post-halt submit must reject");
        assert!(
            matches!(post_halt, SubmitError::Closed),
            "was {post_halt:?}",
        );
        assert_eq!(
            captured.lock().expect("captured").len(),
            1,
            "halt must not refire on post-halt submits",
        );
    }

    #[rstest]
    fn record_snapshot_anchor_signals_halt_when_ack_stalls(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        let (halt, captured) = captured_halt;
        let inner = Arc::new(Mutex::new(MemoryBackend::new()));
        inner
            .lock()
            .expect("inner")
            .open_run(manifest("run-anchor-halt"))
            .expect("open");

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let anchors_seen = Arc::new(AtomicUsize::new(0));
        let backend = BlockingSnapshotBackend {
            inner: Arc::clone(&inner),
            gate: Arc::clone(&gate),
            anchors_seen: Arc::clone(&anchors_seen),
        };

        let halt_threshold = Duration::from_millis(50);

        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            halt,
            WriterConfig {
                channel_capacity: 2,
                halt_threshold,
            },
        )
        .expect("spawn");

        writer
            .submit(entry_draft(10))
            .expect("entry commit must complete before anchor request");
        let err = std::thread::scope(|scope| {
            let request = scope.spawn(|| {
                writer.record_snapshot_anchor("cache://position-snapshots/P-1/0", "blake3:abc")
            });
            wait_for_counter(
                &anchors_seen,
                1,
                "writer did not enter gated snapshot persistence",
            );
            request.join().expect("snapshot thread panicked")
        })
        .expect_err("snapshot anchor acknowledgement must time out");
        let post_halt = writer
            .submit(entry_draft(11))
            .expect_err("post-halt submit");

        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();

        match err {
            EventStoreError::Backend(msg) => {
                assert!(
                    msg.contains("snapshot anchor ack stalled"),
                    "msg was: {msg}"
                );
            }
            other => panic!("expected Backend, was {other:?}"),
        }

        match post_halt {
            SubmitError::Closed => {}
            SubmitError::HaltSignaled { .. } => {
                panic!("expected Closed after anchor halt, was HaltSignaled")
            }
        }

        let captured_reasons = captured.lock().expect("captured");
        assert_eq!(
            captured_reasons.len(),
            1,
            "halt callback must fire exactly once",
        );
        assert_backpressure_stall(captured_reasons.first(), halt_threshold);
        drop(captured_reasons);
        drop(writer);
    }

    #[rstest]
    fn backend_disk_error_fires_halt_and_closes_writer(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        let (halt, captured) = captured_halt;
        let backend = DiskFailureBackend::default();

        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            halt,
            WriterConfig {
                channel_capacity: 4,
                halt_threshold: Duration::from_millis(500),
            },
        )
        .expect("spawn");

        let first = writer
            .submit(entry_draft(10))
            .expect_err("failed durable append must close submit");
        assert!(matches!(first, SubmitError::Closed), "was {first:?}");

        let captured_reasons = captured.lock().expect("captured");
        assert!(matches!(
            captured_reasons.first(),
            Some(HaltReason::BackendDisk(_))
        ));
        drop(captured_reasons);

        let post_halt = writer
            .submit(entry_draft(11))
            .expect_err("post-halt submit must be closed");
        assert!(matches!(post_halt, SubmitError::Closed));

        // Close after fail-stop returns an error rather than panicking.
        let close_result = writer.close(run_ended_draft());
        assert!(close_result.is_err());
    }

    #[rstest]
    fn successful_submit_makes_high_watermark_immediately_visible(
        captured_halt: (HaltCallback, Arc<Mutex<Vec<HaltReason>>>),
    ) {
        // Returning from submit is the durable barrier, so no polling or latency
        // window is needed before the committed high-watermark becomes observable.
        let (halt, _) = captured_halt;
        let (wrapper, shared) = SharedMemory::new();
        shared
            .lock()
            .expect("shared")
            .open_run(manifest("run-time"))
            .expect("open");

        let writer = EventStoreWriter::spawn(
            Box::new(wrapper),
            get_atomic_clock_static(),
            halt,
            WriterConfig {
                channel_capacity: 32,
                halt_threshold: Duration::from_secs(30),
            },
        )
        .expect("spawn");

        writer.submit(entry_draft(10)).expect("submit");
        assert_eq!(
            writer.high_watermark(),
            1,
            "successful submit must expose its durable high-watermark immediately",
        );

        let final_hwm = writer.close(run_ended_draft()).expect("close");
        assert_eq!(final_hwm, 2);
    }

    #[rstest]
    fn entry_draft_without_indices_constructor() {
        let topic: crate::entry::Topic = "exec.command.SubmitOrder".into();
        let payload_type = Ustr::from("SubmitOrder");
        let payload = Bytes::from_static(b"\x01\x02");
        let ts_init = UnixNanos::from(42);
        let draft = EntryDraft::without_indices(
            Headers::empty(),
            topic,
            payload_type,
            payload.clone(),
            ts_init,
        );

        assert!(draft.headers.is_empty());
        assert_eq!(draft.topic.as_ref(), "exec.command.SubmitOrder");
        assert_eq!(draft.payload_type.as_str(), "SubmitOrder");
        assert_eq!(draft.payload, payload);
        assert_eq!(draft.ts_init, ts_init);
        assert!(draft.index_keys.is_empty());
    }

    fn wait_for_counter(counter: &AtomicUsize, expected: usize, failure: &str) {
        let mut waited = Duration::ZERO;
        while counter.load(Ordering::SeqCst) < expected && waited < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(2));
            waited += Duration::from_millis(2);
        }
        assert!(counter.load(Ordering::SeqCst) >= expected, "{failure}");
    }

    fn assert_backpressure_stall(reason: Option<&HaltReason>, expected_threshold: Duration) {
        match reason {
            Some(HaltReason::BackpressureStall {
                stalled_for,
                threshold,
            }) => {
                assert!(
                    *stalled_for >= expected_threshold,
                    "stalled_for {stalled_for:?} must be >= {expected_threshold:?}",
                );
                assert_eq!(*threshold, expected_threshold);
            }
            other => panic!("expected BackpressureStall, was {other:?}"),
        }
    }
}

#[cfg(test)]
#[cfg(madsim)]
mod madsim_tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use indexmap::IndexMap;
    use nautilus_core::{UnixNanos, time::get_atomic_clock_static};
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::{
        backend::{AppendEntry, EventStore, IndexKind, MemoryBackend, ScanDirection},
        entry::EventStoreEntry,
        error::EventStoreError,
        manifest::{RegisteredComponents, RunManifest, RunStatus},
    };

    fn manifest(run_id: &str) -> RunManifest {
        RunManifest {
            run_id: run_id.to_string(),
            parent_run_id: None,
            instance_id: "trader-001".to_string(),
            binary_hash: "deadbeef".to_string(),
            schema_version: 1,
            crate_versions: "feedface".to_string(),
            feature_flags: Vec::new(),
            adapter_versions: IndexMap::new(),
            config_hash: "cafebabe".to_string(),
            provenance: nautilus_provenance::RunProvenance::default(),
            registered_components: RegisteredComponents::default(),
            seed: None,
            start_ts_init: UnixNanos::from(0),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
        }
    }

    fn entry_draft(ts_init: u64) -> EntryDraft {
        EntryDraft {
            headers: Headers::empty(),
            topic: "exec.command.SubmitOrder".into(),
            payload_type: Ustr::from("SubmitOrder"),
            payload: Bytes::from_static(b"\x01\x02\x03\x04"),
            ts_init: UnixNanos::from(ts_init),
            index_keys: Vec::new(),
        }
    }

    #[derive(Debug)]
    struct SharedMemory(Arc<Mutex<MemoryBackend>>);

    impl SharedMemory {
        fn new() -> (Self, Arc<Mutex<MemoryBackend>>) {
            let arc = Arc::new(Mutex::new(MemoryBackend::new()));
            (Self(Arc::clone(&arc)), arc)
        }
    }

    impl EventStore for SharedMemory {
        fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
            unreachable!("test wrapper does not forward open_run")
        }

        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .append_batch(entries)
        }

        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .scan_range(from, to, direction)
        }

        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.0.lock().expect("shared memory poisoned").scan_seq(seq)
        }

        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .lookup(kind, key)
        }

        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .iter_index_keys(kind)
        }

        fn record_snapshot_anchor(
            &mut self,
            anchor: SnapshotAnchor,
        ) -> Result<(), EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .record_snapshot_anchor(anchor)
        }

        fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .latest_snapshot_anchor()
        }

        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.0.lock().expect("shared memory poisoned").seal(status)
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.0.lock().expect("shared memory poisoned").manifest()
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.0
                .lock()
                .expect("shared memory poisoned")
                .high_watermark()
        }
    }

    #[rstest]
    fn record_snapshot_anchor_records_current_watermark_under_madsim() {
        let (wrapper, shared) = SharedMemory::new();
        shared
            .lock()
            .expect("shared")
            .open_run(manifest("run-anchor"))
            .expect("open");

        let writer = EventStoreWriter::spawn(
            Box::new(wrapper),
            get_atomic_clock_static(),
            noop_halt(),
            WriterConfig::default(),
        )
        .expect("spawn");

        writer.submit(entry_draft(10)).expect("submit first");
        writer.submit(entry_draft(11)).expect("submit second");
        let anchor = writer
            .record_snapshot_anchor("cache://position-snapshots/P-1/0", "blake3:abc")
            .expect("record anchor");

        let backend = shared.lock().expect("shared");
        assert_eq!(anchor.high_watermark, 2);
        assert_eq!(
            backend.latest_snapshot_anchor().expect("latest anchor"),
            Some(anchor),
        );
    }
}
