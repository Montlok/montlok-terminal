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

//! The dedicated writer-thread loop that commits channel entries through atomic
//! `append_batch` calls.
//!
//! The loop owns the backend instance for the lifetime of the run: it stamps `seq`,
//! computes `entry_hash`, and immediately commits each submitted entry so the caller's
//! acknowledgement is a durable barrier. The high-watermark mirror only advances after
//! the backend acknowledges the commit. Disk-pressure or corruption errors fire the halt
//! callback and end the loop; a graceful Close drains entries already queued by concurrent
//! producers, writes the supplied `RunEnded` entry as the final row, and seals the manifest.

#[cfg(not(madsim))]
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{Receiver, SyncSender},
};

use nautilus_core::UnixNanos;
#[cfg(not(madsim))]
use nautilus_core::time::AtomicTime;

use crate::{
    backend::AppendEntry, entry::EventStoreEntry, hash::compute_entry_hash, writer::EntryDraft,
};
#[cfg(not(madsim))]
use crate::{
    backend::EventStore,
    error::EventStoreError,
    snapshot::SnapshotAnchor,
    writer::halt::{self, HaltCallback, HaltReason},
};

/// Internal message exchanged between submit and the writer thread.
#[cfg(not(madsim))]
pub(super) enum WriterMessage {
    /// A single captured entry plus its writer-receive timestamp.
    Entry {
        /// The unsealed entry handed in by the bus capture adapter.
        draft: EntryDraft,
        /// The writer-receive timestamp stamped at submit time.
        ts_publish: UnixNanos,
        /// One-shot durable commit acknowledgement returned to the dispatch thread.
        ack: SyncSender<Result<u64, EventStoreError>>,
    },
    /// Graceful shutdown signal. The writer thread drains pending entries, appends the
    /// `RunEnded` entry, seals the manifest, and reports the final high-watermark via
    /// `ack`.
    Close {
        /// The terminal `RunEnded` entry committed before seal.
        run_ended: EntryDraft,
        /// One-shot reply channel for the close result.
        ack: SyncSender<Result<u64, EventStoreError>>,
    },
    /// Records a cache snapshot anchor after all pending entries have been flushed.
    RecordSnapshotAnchor {
        /// Cache-owned reference to the snapshot blob.
        blob_ref: String,
        /// Cache-owned content hash for the snapshot blob.
        content_hash: String,
        /// One-shot reply channel for the recorded anchor.
        ack: SyncSender<Result<SnapshotAnchor, EventStoreError>>,
    },
}

#[cfg(not(madsim))]
struct PendingEntry {
    append: AppendEntry,
    ack: SyncSender<Result<u64, EventStoreError>>,
}

/// The writer thread's halt fire path: the shared callback plus the once-only latch
/// that keeps the fire exactly once across every failure origin.
#[cfg(not(madsim))]
pub(super) struct HaltSink {
    callback: HaltCallback,
    halted: Arc<AtomicBool>,
}

#[cfg(not(madsim))]
impl HaltSink {
    pub(super) fn new(callback: HaltCallback, halted: Arc<AtomicBool>) -> Self {
        Self { callback, halted }
    }

    fn fire(&self, reason: HaltReason) {
        halt::fire_once(&self.callback, &self.halted, reason);
    }
}

/// Runs the writer loop until the channel disconnects, a halt-worthy backend error fires,
/// or a Close message is received.
#[cfg(not(madsim))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "writer thread owns the backend, halt sink, and clock"
)]
pub(super) fn run(
    mut backend: Box<dyn EventStore + Send>,
    rx: Receiver<WriterMessage>,
    halt: HaltSink,
    high_watermark: Arc<AtomicU64>,
    clock: &'static AtomicTime,
) {
    // Recover the next seq from the backend so a writer attached to an existing run
    // continues from the durable high-watermark rather than colliding with already-
    // committed entries.
    let mut next_seq = match backend.high_watermark() {
        Ok(hwm) => hwm + 1,
        Err(e) => {
            halt.fire(HaltReason::from_backend_error(&e));
            return;
        }
    };

    let mut batch: Vec<PendingEntry> = Vec::with_capacity(1);

    while let Ok(message) = rx.recv() {
        match message {
            WriterMessage::Entry {
                draft,
                ts_publish,
                ack,
            } => {
                let append = build_append_entry(draft, ts_publish, next_seq);
                next_seq += 1;
                batch.push(PendingEntry { append, ack });

                // State-affecting bus dispatch waits on this entry's acknowledgement, so commit
                // now and never acknowledge an entry that exists only in the channel.
                if !flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref()) {
                    return;
                }
            }
            WriterMessage::Close { run_ended, ack } => {
                drain_pending(&rx, &mut batch, &mut next_seq);

                let now = clock.get_time_ns();
                let run_ended_append = build_append_entry(run_ended, now, next_seq);
                let (entry_ack, entry_ack_rx) = std::sync::mpsc::sync_channel(1);
                batch.push(PendingEntry {
                    append: run_ended_append,
                    ack: entry_ack,
                });

                let flush_ok = flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref());
                if !flush_ok {
                    let _ = ack.send(Err(EventStoreError::Backend(
                        "writer fail-stopped before seal".to_string(),
                    )));
                    return;
                }
                let _ = entry_ack_rx.recv();

                let seal_result = backend.seal(crate::manifest::RunStatus::Ended);
                let final_result = match seal_result {
                    Ok(()) => Ok(high_watermark.load(Ordering::Acquire)),
                    Err(e) => {
                        halt.fire(HaltReason::from_backend_error(&e));
                        Err(e)
                    }
                };
                let _ = ack.send(final_result);
                return;
            }
            WriterMessage::RecordSnapshotAnchor {
                blob_ref,
                content_hash,
                ack,
            } => {
                if !record_snapshot_anchor(
                    backend.as_mut(),
                    &mut batch,
                    &halt,
                    high_watermark.as_ref(),
                    blob_ref,
                    content_hash,
                    &ack,
                ) {
                    return;
                }
            }
        }
    }
}

#[cfg(not(madsim))]
fn record_snapshot_anchor(
    backend: &mut dyn EventStore,
    batch: &mut Vec<PendingEntry>,
    halt: &HaltSink,
    high_watermark: &AtomicU64,
    blob_ref: String,
    content_hash: String,
    ack: &SyncSender<Result<SnapshotAnchor, EventStoreError>>,
) -> bool {
    if !batch.is_empty() && !flush(backend, batch, halt, high_watermark) {
        let _ = ack.send(Err(EventStoreError::Backend(
            "writer fail-stopped before snapshot anchor".to_string(),
        )));

        return false;
    }

    let hwm = high_watermark.load(Ordering::Acquire);
    let anchor = SnapshotAnchor::new(hwm, blob_ref, content_hash);
    let result = match backend.record_snapshot_anchor(anchor.clone()) {
        Ok(()) => Ok(anchor),
        Err(e) => {
            halt.fire(HaltReason::from_backend_error(&e));
            Err(e)
        }
    };

    let keep_running = result.is_ok();
    let _ = ack.send(result);

    keep_running
}

#[cfg(not(madsim))]
fn drain_pending(rx: &Receiver<WriterMessage>, batch: &mut Vec<PendingEntry>, next_seq: &mut u64) {
    while let Ok(msg) = rx.try_recv() {
        match msg {
            WriterMessage::Entry {
                draft,
                ts_publish,
                ack,
            } => {
                let append = build_append_entry(draft, ts_publish, *next_seq);
                *next_seq += 1;
                batch.push(PendingEntry { append, ack });
            }
            // A second Close while draining: there is no producer that should send one
            // (close consumes the writer), but if a future API permits it, fall back
            // to the run_ended entry the outer loop already queued and ignore this one.
            WriterMessage::Close { ack, .. } => {
                let _ = ack.send(Err(EventStoreError::Backend(
                    "writer is already closing".to_string(),
                )));
            }
            WriterMessage::RecordSnapshotAnchor { ack, .. } => {
                let _ = ack.send(Err(EventStoreError::Backend(
                    "writer is closing before snapshot anchor".to_string(),
                )));
            }
        }
    }
}

/// Commits the current batch, advances the high-watermark mirror on success, or fires the
/// halt callback on backend failure. Returns `true` if the writer loop should continue.
#[cfg(not(madsim))]
fn flush(
    backend: &mut dyn EventStore,
    batch: &mut Vec<PendingEntry>,
    halt: &HaltSink,
    high_watermark: &AtomicU64,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let entries = batch
        .iter()
        .map(|pending| pending.append.clone())
        .collect::<Vec<_>>();
    match backend.append_batch(&entries) {
        Ok(new_hwm) => {
            high_watermark.store(new_hwm, Ordering::Release);
            for pending in batch.drain(..) {
                let _ = pending.ack.send(Ok(new_hwm));
            }
            true
        }
        Err(e) => {
            halt.fire(HaltReason::from_backend_error(&e));
            let message = e.to_string();
            for pending in batch.drain(..) {
                let _ = pending.ack.send(Err(EventStoreError::Backend(format!(
                    "durable entry commit failed: {message}"
                ))));
            }
            false
        }
    }
}

/// Builds a fully-formed [`AppendEntry`] from a draft, the writer-receive timestamp, and
/// the assigned sequence.
pub(super) fn build_append_entry(
    draft: EntryDraft,
    ts_publish: UnixNanos,
    seq: u64,
) -> AppendEntry {
    let EntryDraft {
        headers,
        topic,
        payload_type,
        payload,
        ts_init,
        index_keys,
    } = draft;

    let entry_hash = compute_entry_hash(
        seq,
        ts_init,
        ts_publish,
        topic.as_ref(),
        payload_type.as_str(),
        &payload,
        &headers,
    );

    let entry = EventStoreEntry::new(
        entry_hash,
        seq,
        headers,
        topic,
        payload_type,
        payload,
        ts_init,
        ts_publish,
    );

    AppendEntry::new(entry, index_keys)
}
