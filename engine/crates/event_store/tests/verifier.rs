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

//! Integration tests for the off-trader [`Verifier`] against a tempdir-backed
//! [`RedbBackend`].
//!
//! Mirrors the production posture: the run file lives on disk, sealed by a writer that
//! has already exited. The verifier opens the sealed file via [`Verifier::open_redb`]
//! and proves the run is intact end-to-end. A companion test rebuilds the file with a
//! manufactured gap in the entry table to confirm the verifier surfaces it as a typed
//! finding rather than aborting the integrity scan.

use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    process::Command,
};

use bytes::Bytes;
use indexmap::IndexMap;
use nautilus_core::UnixNanos;
use nautilus_event_store::{
    AppendEntry, DataClass, DataCursorSnapshot, EventStore, EventStoreEntry, GapRange, Headers,
    IndexKey, IndexKind, IsolatedVerifier, IsolatedVerifyError, ManifestField, MarkerBackend,
    MarkerManifest, RedbBackend, RedbMarkerBackend, RegisteredComponents, RunManifest,
    RunStartedPayload, RunStatus, SnapshotAnchor, StreamCursor, Topic, Verifier, VerifyError,
    VerifyFinding, VerifyLimitResource, VerifyLimits, codec, compute_entry_hash,
    compute_marker_hash, measure_regular_file,
};
use redb::ReadableTable;
use rstest::rstest;
use tempfile::TempDir;
use ustr::Ustr;

const INSTANCE_ID: &str = "trader-001";

fn manifest(run_id: &str) -> RunManifest {
    RunManifest {
        run_id: run_id.to_string(),
        parent_run_id: None,
        instance_id: INSTANCE_ID.to_string(),
        binary_hash: "11".repeat(32),
        schema_version: 1,
        crate_versions: "22".repeat(32),
        feature_flags: Vec::new(),
        adapter_versions: IndexMap::new(),
        config_hash: "33".repeat(32),
        provenance: nautilus_provenance::RunProvenance::default(),
        registered_components: RegisteredComponents::default(),
        seed: None,
        start_ts_init: UnixNanos::from(10),
        end_ts_init: None,
        high_watermark: 0,
        status: RunStatus::Running,
    }
}

fn build_entry(seq: u64, ts_init: u64) -> EventStoreEntry {
    let topic: Topic = "exec.command.SubmitOrder".into();
    let payload_type = Ustr::from("SubmitOrder");
    let payload = Bytes::from_static(b"\x01\x02\x03\x04");
    let headers = Headers::empty();
    let ts_publish = UnixNanos::from(ts_init + 1);
    let ts_init_ns = UnixNanos::from(ts_init);
    let hash = compute_entry_hash(
        seq,
        ts_init_ns,
        ts_publish,
        topic.as_ref(),
        payload_type.as_str(),
        &payload,
        &headers,
    );

    EventStoreEntry::new(
        hash,
        seq,
        headers,
        topic,
        payload_type,
        payload,
        ts_init_ns,
        ts_publish,
    )
}

fn append_with(seq: u64, ts_init: u64, index_keys: Vec<IndexKey>) -> AppendEntry {
    AppendEntry::new(build_entry(seq, ts_init), index_keys)
}

fn build_run_started_entry(manifest: &RunManifest) -> EventStoreEntry {
    let topic: Topic = "run.lifecycle.RunStarted".into();
    let payload_type = Ustr::from("RunStarted");
    let payload = Bytes::from(
        codec::encode_to_vec(&RunStartedPayload::from_manifest(manifest))
            .expect("encode RunStarted payload"),
    );
    let headers = Headers::empty();
    let ts_init = manifest.start_ts_init;
    let ts_publish = UnixNanos::from(ts_init.as_u64() + 1);
    let hash = compute_entry_hash(
        1,
        ts_init,
        ts_publish,
        topic.as_ref(),
        payload_type.as_str(),
        &payload,
        &headers,
    );
    EventStoreEntry::new(
        hash,
        1,
        headers,
        topic,
        payload_type,
        payload,
        ts_init,
        ts_publish,
    )
}

fn build_run_ended_entry(seq: u64, ts_init: u64) -> EventStoreEntry {
    let topic: Topic = "run.lifecycle.RunEnded".into();
    let payload_type = Ustr::from("RunEnded");
    let payload = Bytes::new();
    let headers = Headers::empty();
    let ts_publish = UnixNanos::from(ts_init + 1);
    let ts_init = UnixNanos::from(ts_init);
    let hash = compute_entry_hash(
        seq,
        ts_init,
        ts_publish,
        topic.as_ref(),
        payload_type.as_str(),
        &payload,
        &headers,
    );
    EventStoreEntry::new(
        hash,
        seq,
        headers,
        topic,
        payload_type,
        payload,
        ts_init,
        ts_publish,
    )
}

fn write_sealed_run(tmp: &TempDir, run_id: &str) {
    write_sealed_run_with_manifest(tmp, &manifest(run_id));
}

fn write_sealed_run_with_manifest(tmp: &TempDir, manifest: &RunManifest) {
    let mut backend = RedbBackend::new(tmp.path());
    backend.open_run(manifest.clone()).expect("open run");
    backend
        .append_batch(&[
            AppendEntry::without_indices(build_run_started_entry(manifest)),
            AppendEntry::new(
                build_entry(2, 11),
                vec![
                    IndexKey::new(IndexKind::ClientOrderId, "O-1".to_string()),
                    IndexKey::new(IndexKind::ClientOrderId, "O-2".to_string()),
                ],
            ),
            AppendEntry::without_indices(build_run_ended_entry(3, 12)),
        ])
        .expect("append");
    backend.seal(RunStatus::Ended).expect("seal");
    // Backend drops here, releasing the redb file lock so the verifier can open it.
}

fn write_sealed_run_of(tmp: &TempDir, run_id: &str, count: u64) {
    let mut backend = RedbBackend::new(tmp.path());
    let manifest = manifest(run_id);
    backend.open_run(manifest.clone()).expect("open run");
    let batch: Vec<AppendEntry> = (1..=count)
        .map(|seq| {
            if seq == 1 {
                AppendEntry::without_indices(build_run_started_entry(&manifest))
            } else if seq == count {
                AppendEntry::without_indices(build_run_ended_entry(seq, 10 + seq))
            } else {
                append_with(seq, 10 + seq, Vec::new())
            }
        })
        .collect();
    backend.append_batch(&batch).expect("append");
    backend.seal(RunStatus::Ended).expect("seal");
}

fn run_path(tmp: &TempDir, run_id: &str) -> std::path::PathBuf {
    tmp.path().join(INSTANCE_ID).join(format!("{run_id}.redb"))
}

fn marker_path(tmp: &TempDir, run_id: &str) -> std::path::PathBuf {
    tmp.path()
        .join(INSTANCE_ID)
        .join(format!("{run_id}.markers.redb"))
}

fn marker_manifest(run_id: &str) -> MarkerManifest {
    MarkerManifest {
        run_id: run_id.to_string(),
        enabled_classes: vec![DataClass::Quote],
        high_fidelity: false,
        snapshot_count: 0,
        hifi_count: 0,
        gap_count: 0,
        dict_count: 0,
        status: RunStatus::Running,
    }
}

fn marker_snapshot(marker_seq: u64, event_seq_before: u64) -> DataCursorSnapshot {
    DataCursorSnapshot {
        marker_seq,
        event_seq_before,
        ts_init: UnixNanos::from(1_700_000_000_000_000_000 + marker_seq),
        advanced: vec![StreamCursor {
            slot: 0,
            ts_init_hi: UnixNanos::from(1_700_000_000_000_000_000 + marker_seq),
            count: marker_seq,
        }],
    }
}

fn verify_bin(path: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg(path)
        .output()
        .expect("run verifier binary")
}

fn flip_stored_entry_payload_byte(path: &std::path::Path, seq: u64) {
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    let db = redb::Database::create(path).expect("open redb");
    let txn = db.begin_write().expect("begin write");
    {
        let mut table = txn.open_table(entries).expect("open entries");
        let mut bytes = {
            let row = table.get(seq).expect("get entry").expect("entry present");
            row.value().to_vec()
        };
        // `build_entry` uses this fixed payload for every entry; flipping inside it
        // preserves the stored hash and forces the verifier's recompute check to fail.
        let payload_offset = bytes
            .windows(4)
            .position(|window| window == b"\x01\x02\x03\x04")
            .expect("payload bytes present");
        bytes[payload_offset + 2] ^= 0xFF;
        table
            .insert(seq, bytes.as_slice())
            .expect("overwrite entry");
    }
    txn.commit().expect("commit flip");
}

fn truncate_stored_entry(path: &std::path::Path, seq: u64) {
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    let db = redb::Database::create(path).expect("open redb");
    let txn = db.begin_write().expect("begin write");
    {
        let mut table = txn.open_table(entries).expect("open entries");
        let mut bytes = {
            let row = table.get(seq).expect("get entry").expect("entry present");
            row.value().to_vec()
        };
        bytes.truncate(bytes.len() / 2);
        table
            .insert(seq, bytes.as_slice())
            .expect("overwrite entry");
    }
    txn.commit().expect("commit truncate");
}

fn zero_tail_truncate(path: &std::path::Path) {
    let original_len = fs::metadata(path).expect("metadata").len();
    let retained_len = original_len / 2;
    let zeroed_len = usize::try_from(original_len - retained_len).expect("tail length fits");
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open for zero-tail truncate");
    file.set_len(retained_len).expect("truncate tail");
    file.seek(SeekFrom::Start(retained_len)).expect("seek tail");
    file.write_all(&vec![0_u8; zeroed_len])
        .expect("write zero tail");
    file.flush().expect("flush zero tail");
}

#[rstest]
fn clean_run_reports_no_findings() {
    let tmp = TempDir::new().expect("tempdir");
    write_sealed_run(&tmp, "1700000000-cafe0001");

    let verifier =
        Verifier::open_redb(tmp.path(), INSTANCE_ID, "1700000000-cafe0001").expect("open verifier");
    let report = verifier.verify().expect("verify");

    assert!(report.is_clean(), "findings was: {:?}", report.findings);
    assert_eq!(report.high_watermark, 3);
    assert_eq!(report.entries_scanned, 3);
    assert_eq!(report.status, RunStatus::Ended);
    assert_eq!(report.run_id, "1700000000-cafe0001");
}

#[rstest]
fn ended_run_without_run_ended_is_corrupt() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-life0001";
    {
        let mut backend = RedbBackend::new(tmp.path());
        let run_manifest = manifest(run_id);
        backend.open_run(run_manifest.clone()).expect("open run");
        backend
            .append_batch(&[
                AppendEntry::without_indices(build_run_started_entry(&run_manifest)),
                append_with(2, 11, Vec::new()),
            ])
            .expect("append");
        backend.seal(RunStatus::Ended).expect("seal");
    }

    let report = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id)
        .expect("open verifier")
        .verify()
        .expect("verify");

    assert!(report.findings.iter().any(|finding| matches!(
        finding,
        VerifyFinding::ManifestMismatch {
            kind: ManifestField::LifecycleMarker,
            reason,
        } if reason.contains("missing a canonical RunEnded")
    )));
    assert!(report.store_digest.is_none());
}

#[rstest]
fn non_final_run_ended_is_corrupt_even_with_canonical_tail() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-life0002";
    {
        let mut backend = RedbBackend::new(tmp.path());
        let run_manifest = manifest(run_id);
        backend.open_run(run_manifest.clone()).expect("open run");
        backend
            .append_batch(&[
                AppendEntry::without_indices(build_run_started_entry(&run_manifest)),
                AppendEntry::without_indices(build_run_ended_entry(2, 11)),
                AppendEntry::without_indices(build_run_ended_entry(3, 12)),
            ])
            .expect("append");
        backend.seal(RunStatus::Ended).expect("seal");
    }

    let report = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id)
        .expect("open verifier")
        .verify()
        .expect("verify");

    assert!(report.findings.iter().any(|finding| matches!(
        finding,
        VerifyFinding::ManifestMismatch {
            kind: ManifestField::LifecycleMarker,
            reason,
        } if reason.contains("seq=2")
    )));
    assert!(report.store_digest.is_none());
}

#[rstest]
fn run_ended_with_payload_is_corrupt() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-life0003";
    {
        let mut backend = RedbBackend::new(tmp.path());
        let run_manifest = manifest(run_id);
        backend.open_run(run_manifest.clone()).expect("open run");
        let mut forged = build_run_ended_entry(2, 11);
        forged.payload = Bytes::from_static(b"not-empty");
        forged.entry_hash = forged.recompute_hash();
        backend
            .append_batch(&[
                AppendEntry::without_indices(build_run_started_entry(&run_manifest)),
                AppendEntry::without_indices(forged),
            ])
            .expect("append");
        backend.seal(RunStatus::Ended).expect("seal");
    }

    let report = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id)
        .expect("open verifier")
        .verify()
        .expect("verify");

    assert!(report.findings.iter().any(|finding| matches!(
        finding,
        VerifyFinding::ManifestMismatch {
            kind: ManifestField::LifecycleMarker,
            reason,
        } if reason.contains("seq=2")
    )));
    assert!(report.store_digest.is_none());
}

#[rstest]
#[case::client(IndexKind::ClientOrderId)]
#[case::venue(IndexKind::VenueOrderId)]
fn run_ended_with_sidecar_index_is_corrupt(#[case] kind: IndexKind) {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = match kind {
        IndexKind::ClientOrderId => "1700000000-life0004",
        IndexKind::VenueOrderId => "1700000000-life0005",
    };
    {
        let mut backend = RedbBackend::new(tmp.path());
        let run_manifest = manifest(run_id);
        backend.open_run(run_manifest.clone()).expect("open run");
        backend
            .append_batch(&[
                AppendEntry::without_indices(build_run_started_entry(&run_manifest)),
                AppendEntry::new(
                    build_run_ended_entry(2, 11),
                    vec![IndexKey::new(kind, "forged-index".to_string())],
                ),
            ])
            .expect("append");
        backend.seal(RunStatus::Ended).expect("seal");
    }

    let report = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id)
        .expect("open verifier")
        .verify()
        .expect("verify");

    assert!(report.findings.iter().any(|finding| matches!(
        finding,
        VerifyFinding::ManifestMismatch {
            kind: ManifestField::LifecycleMarker,
            reason,
        } if reason.contains("sidecar index") && reason.contains("forged-index")
    )));
    assert!(report.store_digest.is_none());
}

#[rstest]
fn crashed_recovered_run_with_canonical_run_ended_is_corrupt() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-life0006";
    {
        let mut backend = RedbBackend::new(tmp.path());
        let run_manifest = manifest(run_id);
        backend.open_run(run_manifest.clone()).expect("open run");
        backend
            .append_batch(&[
                AppendEntry::without_indices(build_run_started_entry(&run_manifest)),
                AppendEntry::without_indices(build_run_ended_entry(2, 11)),
            ])
            .expect("append");
        backend.seal(RunStatus::CrashedRecovered).expect("seal");
    }

    let report = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id)
        .expect("open verifier")
        .verify()
        .expect("verify");

    assert!(report.findings.iter().any(|finding| matches!(
        finding,
        VerifyFinding::ManifestMismatch {
            kind: ManifestField::LifecycleMarker,
            reason,
        } if reason.contains("CrashedRecovered")
    )));
    assert!(report.store_digest.is_none());
}

#[rstest]
fn binary_clean_run_exits_zero() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0101";
    write_sealed_run(&tmp, run_id);

    let output = verify_bin(&run_path(&tmp, run_id));

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert!(output.status.success(), "stdout={stdout} stderr={stderr}");
    assert!(stdout.contains("clean"), "stdout was: {stdout}");
    assert!(stdout.contains("high_watermark=3"), "stdout was: {stdout}");
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[rstest]
fn binary_missing_marker_sidecar_reports_absent_without_error() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0111";
    write_sealed_run(&tmp, run_id);

    let output = verify_bin(&run_path(&tmp, run_id));

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert!(output.status.success(), "stdout={stdout} stderr={stderr}");
    assert!(stdout.contains("clean"), "stdout was: {stdout}");
    assert!(stdout.contains("markers=absent"), "stdout was: {stdout}");
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[rstest]
fn binary_marker_sidecar_fails_closed_until_bounded_protocol_exists() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0113";
    write_sealed_run(&tmp, run_id);

    {
        let mut marker_backend = RedbMarkerBackend::new(marker_path(&tmp, run_id));
        marker_backend
            .open_run(marker_manifest(run_id))
            .expect("open marker run");
        let snapshot = marker_snapshot(1, 1);
        marker_backend
            .append_snapshot(&snapshot, compute_marker_hash(&snapshot))
            .expect("append marker snapshot");
        marker_backend.seal(RunStatus::Ended).expect("seal markers");
    }

    let output = verify_bin(&run_path(&tmp, run_id));

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}",
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(stdout.contains("markers=error"), "stdout was: {stdout}");
    assert!(
        stdout.contains("bounded isolated helper protocol"),
        "stdout was: {stdout}",
    );
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[rstest]
fn binary_corrupt_entry_run_with_failing_marker_sidecar_relays_entry_findings() {
    // A failing sidecar must not mask the entry run's corrupt findings
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0114";
    write_sealed_run(&tmp, run_id);
    flip_stored_entry_payload_byte(&run_path(&tmp, run_id), 2);

    // A valid redb file without a marker manifest fails the sidecar open cleanly
    drop(redb::Database::create(marker_path(&tmp, run_id)).expect("create marker file"));

    let output = verify_bin(&run_path(&tmp, run_id));

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}",
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(
        stdout.contains("hash mismatch at seq 2"),
        "stdout was: {stdout}",
    );
    assert!(stdout.contains("markers=error"), "stdout was: {stdout}");
}

#[rstest]
fn binary_undecodable_entry_is_reported_and_scan_continues() {
    // A truncated row fails to decode at seq 2; the walk must continue so later
    // entries count and the index pointing at the bad row reports drift.
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0115";
    write_sealed_run(&tmp, run_id);
    truncate_stored_entry(&run_path(&tmp, run_id), 2);

    let output = verify_bin(&run_path(&tmp, run_id));

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}",
    );
    assert!(
        stdout.contains("undecodable entry at seq 2"),
        "stdout was: {stdout}",
    );
    assert!(stdout.contains("entries_scanned=3"), "stdout was: {stdout}",);
    assert!(
        stdout.contains("corrupted target seq 2"),
        "stdout was: {stdout}",
    );
}

#[rstest]
fn binary_does_not_open_corrupt_unbounded_marker_sidecar() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0112";
    write_sealed_run(&tmp, run_id);

    {
        let mut marker_backend = RedbMarkerBackend::new(marker_path(&tmp, run_id));
        marker_backend
            .open_run(marker_manifest(run_id))
            .expect("open marker run");
        let snapshot = marker_snapshot(1, 1);
        marker_backend
            .append_snapshot(&snapshot, [0xAA; 32])
            .expect("append marker snapshot");
        marker_backend.seal(RunStatus::Ended).expect("seal markers");
    }

    let output = verify_bin(&run_path(&tmp, run_id));

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}",
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(
        stdout
            .contains("marker sidecar verification requires the bounded isolated helper protocol"),
        "stdout was: {stdout}",
    );
    assert!(
        stdout.contains("quarantine=not-performed"),
        "stdout was: {stdout}",
    );
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[rstest]
fn binary_hash_mismatch_exits_corrupt_without_quarantine() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0102";
    write_sealed_run(&tmp, run_id);

    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    let path = run_path(&tmp, run_id);
    let mut tampered = build_entry(2, 11);
    tampered.payload = Bytes::from_static(b"\xFF");
    let bytes = codec::encode_to_vec(&tampered).expect("encode");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open table");
            table
                .insert(2_u64, bytes.as_slice())
                .expect("overwrite seq 2");
        }
        txn.commit().expect("commit overwrite");
    }

    let output = verify_bin(&path);

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(
        stdout.contains("hash mismatch at seq 2"),
        "stdout was: {stdout}"
    );
    assert!(
        stdout.contains("quarantine=not-performed"),
        "stdout was: {stdout}",
    );
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[rstest]
fn flipped_entry_byte_reports_hash_mismatch() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0109";
    write_sealed_run(&tmp, run_id);
    let path = run_path(&tmp, run_id);

    flip_stored_entry_payload_byte(&path, 2);

    let verifier = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id).expect("open verifier");
    let report = verifier.verify().expect("verify");

    assert!(
        report
            .findings
            .iter()
            .any(|finding| matches!(finding, VerifyFinding::HashMismatch { seq: 2 })),
        "findings was: {:?}",
        report.findings,
    );
}

#[cfg(debug_assertions)]
#[rstest]
fn binary_worker_abort_exits_corrupt_without_quarantine() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0105";
    write_sealed_run(&tmp, run_id);

    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .env("NAUTILUS_EVENT_STORE_VERIFY_ABORT_WORKER", "1")
        .arg(run_path(&tmp, run_id))
        .output()
        .expect("run verifier binary");

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(stdout.contains("worker_status="), "stdout was: {stdout}");
    assert!(
        stdout.contains("quarantine=not-performed"),
        "stdout was: {stdout}",
    );
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[cfg(debug_assertions)]
#[rstest]
fn binary_worker_timeout_exits_corrupt_without_quarantine() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0106";
    write_sealed_run(&tmp, run_id);

    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .env("NAUTILUS_EVENT_STORE_VERIFY_SLEEP_WORKER_MS", "250")
        .env("NAUTILUS_EVENT_STORE_VERIFY_TIMEOUT_SECS", "0")
        .arg(run_path(&tmp, run_id))
        .output()
        .expect("run verifier binary");

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(stdout.contains("timeout after 0s"), "stdout was: {stdout}");
    assert!(
        stdout.contains("quarantine=not-performed"),
        "stdout was: {stdout}",
    );
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[cfg(debug_assertions)]
#[rstest]
fn binary_parent_bounds_worker_output_without_pipe_deadlock() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-output-flood";
    write_sealed_run(&tmp, run_id);

    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .env("NAUTILUS_EVENT_STORE_VERIFY_FLOOD_WORKER_BYTES", "2097152")
        .arg(run_path(&tmp, run_id))
        .output()
        .expect("run verifier binary");

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout length={} stderr={stderr}",
        stdout.len(),
    );
    assert!(
        stdout.starts_with("corrupt ") && stdout.contains("worker_status=output-limit-exceeded"),
        "stdout prefix was: {}",
        &stdout[..stdout.len().min(256)],
    );
    assert!(
        stdout.len() <= 1_049_000,
        "parent retained unbounded worker output: {} bytes",
        stdout.len(),
    );
    assert!(stderr.is_empty(), "stderr was: {stderr}");
}

#[cfg(unix)]
#[rstest]
fn verifier_opens_read_only_run_file() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0103";
    write_sealed_run(&tmp, run_id);

    let path = run_path(&tmp, run_id);
    let original_len = fs::metadata(&path).expect("metadata").len();
    let mut permissions = fs::metadata(&path).expect("metadata").permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions).expect("set read-only");

    let verifier = Verifier::open_redb_file(&path).expect("open read-only verifier");
    let report = verifier.verify().expect("verify");

    assert!(report.is_clean(), "findings was: {:?}", report.findings);
    assert_eq!(fs::metadata(&path).expect("metadata").len(), original_len);
}

#[rstest]
fn read_only_backend_rejects_open_run_reuse() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0107";
    write_sealed_run(&tmp, run_id);

    let path = run_path(&tmp, run_id);
    let mut backend = RedbBackend::open_sealed_file(path).expect("open sealed file");
    let err = backend
        .open_run(manifest("1700000000-cafe0108"))
        .expect_err("must reject writer reuse");

    assert!(matches!(err, nautilus_event_store::EventStoreError::Closed));
}

#[rstest]
fn truncated_run_file_reports_corrupt_open_error() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0104";
    write_sealed_run(&tmp, run_id);

    let path = run_path(&tmp, run_id);
    let original_len = fs::metadata(&path).expect("metadata").len();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open for truncate");
    file.set_len(original_len / 2).expect("truncate");

    let output = verify_bin(&path);

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(
        stdout.contains("quarantine=not-performed"),
        "stdout was: {stdout}",
    );
}

#[rstest]
fn zero_tail_truncated_run_file_reports_corrupt() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0110";
    write_sealed_run_of(&tmp, run_id, 128);

    let path = run_path(&tmp, run_id);
    zero_tail_truncate(&path);

    let output = verify_bin(&path);

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}",
    );
    assert!(stdout.contains("corrupt"), "stdout was: {stdout}");
    assert!(
        stdout.contains("quarantine=not-performed"),
        "stdout was: {stdout}",
    );
}

#[rstest]
fn binary_missing_run_file_exits_error() {
    let tmp = TempDir::new().expect("tempdir");
    let path = run_path(&tmp, "missing-run");

    let output = verify_bin(&path);

    let stdout = String::from_utf8(output.stdout).expect("stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.is_empty(), "stdout was: {stdout}");
    assert!(stderr.contains("error path="), "stderr was: {stderr}");
    assert!(stderr.contains("no run file"), "stderr was: {stderr}");
}

#[rstest]
fn open_redb_rejects_missing_run() {
    let tmp = TempDir::new().expect("tempdir");

    let err = Verifier::open_redb(tmp.path(), INSTANCE_ID, "missing-run").expect_err("must fail");

    let msg = err.to_string();
    assert!(msg.contains("missing-run"), "msg was: {msg}");
}

#[rstest]
fn sparse_redb_with_max_sequence_is_rejected_before_sequence_scan() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-max-seq";
    write_sealed_run(&tmp, run_id);

    let path = run_path(&tmp, run_id);
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    let max_entry = build_entry(u64::MAX, 13);
    let bytes = codec::encode_to_vec(&max_entry).expect("encode max-seq entry");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open entries");
            table
                .insert(u64::MAX, bytes.as_slice())
                .expect("insert sparse max-seq row");
        }
        txn.commit().expect("commit sparse row");
    }

    let error = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id)
        .expect("open verifier")
        .verify()
        .expect_err("max sequence space must fail closed");

    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::HighWatermark,
            observed: u64::MAX,
            ..
        }
    ));
}

#[rstest]
fn sealed_redb_open_enforces_typed_file_byte_budget() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-file-limit";
    write_sealed_run(&tmp, run_id);
    let path = run_path(&tmp, run_id);
    let observed = fs::metadata(&path).expect("metadata").len();
    let limit = observed.checked_sub(1).expect("redb file is non-empty");

    let backend_error = RedbBackend::open_sealed_file_with_max_file_bytes(&path, limit)
        .expect_err("backend must reject oversized sealed input");
    assert!(matches!(
        backend_error,
        nautilus_event_store::EventStoreError::FileSizeLimitExceeded {
            observed: actual,
            limit: actual_limit,
        } if actual == observed && actual_limit == limit
    ));

    let limits = VerifyLimits {
        max_file_bytes: limit,
        ..VerifyLimits::default()
    };
    let verifier_error = Verifier::open_redb_file_with_limits(&path, limits)
        .expect_err("verifier must reject oversized sealed input");
    assert!(matches!(
        verifier_error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::FileBytes,
            observed: actual,
            limit: actual_limit,
        } if actual == observed && actual_limit == limit
    ));
}

#[rstest]
fn sealed_open_does_not_decode_over_budget_entry_rows_before_preflight() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-entry-limit";
    write_sealed_run(&tmp, run_id);
    let path = run_path(&tmp, run_id);
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open entries");
            table
                .insert(4_u64, &[0xff_u8][..])
                .expect("insert deliberately undecodable row");
        }
        txn.commit().expect("commit corrupt row");
    }

    let limits = VerifyLimits {
        max_high_watermark: 10,
        max_entries: 3,
        ..VerifyLimits::default()
    };
    let verifier = Verifier::open_redb_file_with_limits(&path, limits)
        .expect("sealed open must not decode the entries table");
    let error = verifier
        .verify()
        .expect_err("entry count must be rejected before row decode");

    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::Entries,
            observed: 4,
            limit: 3,
        }
    ));
}

#[rstest]
fn verifier_rejects_oversized_first_row_before_decode() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-first-row-limit";
    write_sealed_run(&tmp, run_id);
    let limits = VerifyLimits {
        max_entry_bytes: 1,
        ..VerifyLimits::default()
    };

    let error = Verifier::open_redb_file_with_limits(run_path(&tmp, run_id), limits)
        .expect("open")
        .verify()
        .expect_err("first row must exceed byte budget");
    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::EntryBytes,
            observed,
            limit: 1,
        } if observed > 1
    ));
}

#[rstest]
fn verifier_rejects_cumulative_decoded_entry_bytes() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-total-row-limit";
    write_sealed_run(&tmp, run_id);
    let limits = VerifyLimits {
        max_total_entry_bytes: 1,
        ..VerifyLimits::default()
    };

    let error = Verifier::open_redb_file_with_limits(run_path(&tmp, run_id), limits)
        .expect("open")
        .verify()
        .expect_err("cumulative entry bytes must fail closed");
    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::TotalEntryBytes,
            observed,
            limit: 1,
        } if observed > 1
    ));
}

#[rstest]
fn verifier_rejects_cumulative_index_key_bytes() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-index-key-limit";
    write_sealed_run(&tmp, run_id);
    let limits = VerifyLimits {
        max_total_index_key_bytes: 1,
        ..VerifyLimits::default()
    };

    let error = Verifier::open_redb_file_with_limits(run_path(&tmp, run_id), limits)
        .expect("open")
        .verify()
        .expect_err("index key bytes must fail closed");
    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::TotalIndexKeyBytes,
            observed,
            limit: 1,
        } if observed > 1
    ));
}

#[rstest]
fn verifier_rejects_findings_flood_at_configured_cap() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-findings-limit";
    write_sealed_run(&tmp, run_id);
    let path = run_path(&tmp, run_id);
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    let substitute = build_entry(99, 11);
    let bytes = codec::encode_to_vec(&substitute).expect("encode");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open entries");
            table.insert(2_u64, bytes.as_slice()).expect("replace row");
        }
        txn.commit().expect("commit");
    }
    let limits = VerifyLimits {
        max_findings: 0,
        ..VerifyLimits::default()
    };

    let error = Verifier::open_redb_file_with_limits(path, limits)
        .expect("open")
        .verify()
        .expect_err("findings cap must fail closed");
    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::Findings,
            observed: 1,
            limit: 0,
        }
    ));
}

#[rstest]
fn verifier_rejects_manifest_row_before_decode() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-manifest-limit";
    write_sealed_run(&tmp, run_id);
    let limits = VerifyLimits {
        max_manifest_bytes: 1,
        ..VerifyLimits::default()
    };

    let error = Verifier::open_redb_file_with_limits(run_path(&tmp, run_id), limits)
        .expect_err("manifest row must be bounded before decode");
    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::ManifestBytes,
            observed,
            limit: 1,
        } if observed > 1
    ));
}

#[rstest]
fn verifier_rejects_snapshot_anchor_row_before_decode() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe-anchor-limit";
    write_sealed_run(&tmp, run_id);
    let path = run_path(&tmp, run_id);
    let anchors: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("snapshot_anchor");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(anchors).expect("open anchors");
            table
                .insert("latest", &[0xff_u8, 0xff_u8][..])
                .expect("insert oversized undecodable anchor");
        }
        txn.commit().expect("commit");
    }
    let limits = VerifyLimits {
        max_snapshot_anchor_bytes: 1,
        ..VerifyLimits::default()
    };

    let error = Verifier::open_redb_file_with_limits(path, limits)
        .expect("open")
        .verify()
        .expect_err("snapshot anchor must be bounded before decode");
    assert!(matches!(
        error,
        VerifyError::LimitExceeded {
            resource: VerifyLimitResource::SnapshotAnchorBytes,
            observed: 2,
            limit: 1,
        }
    ));
}

#[rstest]
fn manufactured_seq_swap_surfaces_as_finding() {
    // Build a sealed run, then overwrite the bytes at table key=2 with the
    // codec-encoded entry whose embedded seq is 99. The hash recomputes
    // correctly because the hash hashes entry.seq=99, so scan_seq returns
    // Ok(Some(entry)) without raising HashMismatch. The verifier must catch
    // the key/embedded-seq divergence.
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0003";
    write_sealed_run(&tmp, run_id);

    let path = tmp.path().join(INSTANCE_ID).join(format!("{run_id}.redb"));
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    let substitute = build_entry(99, 11);
    let bytes = codec::encode_to_vec(&substitute).expect("encode");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open table");
            table
                .insert(2_u64, bytes.as_slice())
                .expect("overwrite seq 2");
        }
        txn.commit().expect("commit overwrite");
    }

    let verifier = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id).expect("open verifier");
    let report = verifier.verify().expect("verify");

    assert!(
        report.findings.iter().any(|f| matches!(
            f,
            VerifyFinding::SeqMismatch {
                table_key: 2,
                embedded_seq: 99,
            }
        )),
        "findings was: {:?}",
        report.findings,
    );
}

#[rstest]
fn corrupt_snapshot_anchor_is_a_finding_not_clean() {
    // The restore path reads the anchor before tail replay; a run whose anchor fails
    // to decode must not verify clean and then fail at restore time.
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0110";
    write_sealed_run(&tmp, run_id);

    let path = run_path(&tmp, run_id);
    let snapshot_anchor: redb::TableDefinition<&str, &[u8]> =
        redb::TableDefinition::new("snapshot_anchor");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(snapshot_anchor).expect("open table");
            table
                .insert("latest", b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".as_slice())
                .expect("overwrite latest snapshot anchor");
        }
        txn.commit().expect("commit overwrite");
    }

    let verifier = Verifier::open_redb_file(&path).expect("open verifier");
    let report = verifier.verify().expect("verify");

    assert!(!report.is_clean(), "findings was: {:?}", report.findings);
    assert!(
        report.findings.iter().any(|f| matches!(
            f,
            VerifyFinding::SnapshotAnchorInvalid { reason } if reason.contains("unreadable")
        )),
        "findings was: {:?}",
        report.findings,
    );
}

#[rstest]
fn snapshot_anchor_past_durable_watermark_is_a_finding() {
    // A tail-trimmed run whose anchor points past the durable watermark cannot
    // restore (the reader rejects the plan); the verifier must surface it.
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0111";
    {
        let mut backend = RedbBackend::new(tmp.path());
        backend.open_run(manifest(run_id)).expect("open run");
        backend
            .append_batch(&[
                append_with(1, 10, Vec::new()),
                append_with(2, 11, Vec::new()),
            ])
            .expect("append");
        backend
            .record_snapshot_anchor(SnapshotAnchor::new(2, "cache://snapshots/2", "blake3:abc"))
            .expect("record anchor");
        backend.seal(RunStatus::Ended).expect("seal");
    }

    let path = run_path(&tmp, run_id);
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open table");
            table.remove(2_u64).expect("remove seq 2");
        }
        txn.commit().expect("commit removal");
    }

    let verifier = Verifier::open_redb_file(&path).expect("open verifier");
    let report = verifier.verify().expect("verify");

    assert!(
        report.findings.iter().any(|f| matches!(
            f,
            VerifyFinding::SnapshotAnchorInvalid { reason } if reason.contains("exceeds durable")
        )),
        "findings was: {:?}",
        report.findings,
    );
}

#[rstest]
fn manufactured_multi_gap_surfaces_two_findings() {
    // Build a 5-entry sealed run, then drop seqs 2 AND 4 from the entries table.
    // The verifier must walk to hwm=5 (last() still returns 5) and emit two
    // distinct GapRange findings, one per contiguous hole. Catches a regression
    // that lets a single gap_cursor span across the surviving seq=3.
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0004";
    write_sealed_run_of(&tmp, run_id, 5);

    let path = tmp.path().join(INSTANCE_ID).join(format!("{run_id}.redb"));
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open table");
            table.remove(2_u64).expect("remove seq 2");
            table.remove(4_u64).expect("remove seq 4");
        }
        txn.commit().expect("commit removal");
    }

    let verifier = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id).expect("open verifier");
    let report = verifier.verify().expect("verify");

    let gaps: Vec<GapRange> = report
        .findings
        .iter()
        .filter_map(|f| match f {
            VerifyFinding::Gap { range } => Some(*range),
            _ => None,
        })
        .collect();
    assert_eq!(
        gaps,
        vec![GapRange { from: 2, to: 2 }, GapRange { from: 4, to: 4 },],
    );
    assert_eq!(report.entries_scanned, 3);
    assert_eq!(report.high_watermark, 5);
}

#[rstest]
fn manufactured_gap_surfaces_as_finding() {
    // Build a sealed run, then drop seq=2 directly out of the entries table to
    // simulate the corruption class the SPEC describes: a run that opens cleanly
    // but is missing a row inside the high-watermark. The verifier must record a
    // Gap finding and continue past it instead of aborting on the first hit.
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-cafe0002";
    write_sealed_run(&tmp, run_id);

    let path = tmp.path().join(INSTANCE_ID).join(format!("{run_id}.redb"));
    let entries: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("entries");
    {
        let db = redb::Database::create(&path).expect("open redb");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(entries).expect("open table");
            table.remove(2_u64).expect("remove seq 2");
        }
        txn.commit().expect("commit removal");
    }

    let verifier = Verifier::open_redb(tmp.path(), INSTANCE_ID, run_id).expect("open verifier");
    let report = verifier.verify().expect("verify");

    let gaps: Vec<GapRange> = report
        .findings
        .iter()
        .filter_map(|f| match f {
            VerifyFinding::Gap { range } => Some(*range),
            _ => None,
        })
        .collect();
    assert_eq!(gaps, vec![GapRange { from: 2, to: 2 }]);
    // Verifier still scanned the surviving rows and surfaced the dangling index
    // pointing at the removed seq.
    assert_eq!(report.entries_scanned, 2);
    assert!(
        report.findings.iter().any(|f| matches!(
            f,
            VerifyFinding::IndexDrift {
                kind: IndexKind::ClientOrderId,
                drift: nautilus_event_store::IndexDrift::DanglingTarget { stored_seq: 2 },
                ..
            }
        )),
        "findings was: {:?}",
        report.findings,
    );
}

#[rstest]
fn isolated_helper_returns_digest_bound_clean_receipt() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-isolated-clean";
    write_sealed_run(&tmp, run_id);
    let source = run_path(&tmp, run_id);
    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");

    let receipt = verifier
        .verify_file(&source, VerifyLimits::default())
        .expect("isolated clean receipt");
    let in_process = Verifier::open_redb_file(&source)
        .expect("open")
        .verify()
        .expect("verify");

    assert_eq!(receipt.run_id, run_id);
    assert_eq!(receipt.status, RunStatus::Ended);
    assert_eq!(receipt.high_watermark, in_process.high_watermark);
    assert_eq!(receipt.store_digest, in_process.store_digest.unwrap());
    assert_eq!(receipt.helper_measurement, helper_measurement);
}

#[rstest]
fn isolated_helper_constructor_rejects_unexpected_tcb_digest() {
    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let wrong = nautilus_provenance::ContentDigest::digest_bytes(b"different helper");

    assert!(matches!(
        IsolatedVerifier::new(helper, wrong),
        Err(IsolatedVerifyError::HelperMeasurementMismatch)
    ));
}

#[cfg(unix)]
#[rstest]
fn isolated_helper_constructor_rejects_symlink() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let tmp = TempDir::new().expect("tempdir");
    let helper = tmp.path().join("helper");
    fs::write(&helper, b"#!/bin/sh\nexit 1\n").expect("helper script");
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).expect("chmod helper");
    let helper_measurement = measure_regular_file(&helper, u64::MAX).expect("measure helper");
    let link = tmp.path().join("helper-link");
    symlink(&helper, &link).expect("helper symlink");

    assert!(matches!(
        IsolatedVerifier::new(&link, helper_measurement.digest),
        Err(IsolatedVerifyError::InvalidPath { .. })
    ));
}

#[cfg(unix)]
#[rstest]
fn isolated_helper_constructor_rejects_group_or_world_writable_helper() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().expect("tempdir");
    let helper = tmp.path().join("writable-helper");
    fs::write(&helper, b"#!/bin/sh\nexit 1\n").expect("helper script");
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o722)).expect("chmod helper");
    let helper_measurement = measure_regular_file(&helper, u64::MAX).expect("measure helper");

    assert!(matches!(
        IsolatedVerifier::new(&helper, helper_measurement.digest),
        Err(IsolatedVerifyError::InvalidPath { .. })
    ));
}

#[rstest]
fn isolated_helper_rejects_empty_host_commitments() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-empty-commitments";
    let mut uncommitted = manifest(run_id);
    uncommitted.binary_hash.clear();
    uncommitted.crate_versions.clear();
    uncommitted.config_hash.clear();
    write_sealed_run_with_manifest(&tmp, &uncommitted);
    let source = run_path(&tmp, run_id);
    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");

    assert!(matches!(
        verifier.verify_file(&source, VerifyLimits::default()),
        Err(IsolatedVerifyError::Rejected)
    ));
}

#[rstest]
fn isolated_recovery_allows_missing_base_directory_and_returns_bounded_empty_outcome() {
    let tmp = TempDir::new().expect("tempdir");
    let missing_base = tmp.path().join("not-created-yet");
    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");

    let outcome = verifier
        .recover_predecessors(&missing_base, INSTANCE_ID, VerifyLimits::default())
        .expect("isolated empty recovery");
    assert!(missing_base.is_dir());
    assert!(outcome.recovered.is_empty());
    assert!(outcome.parent_run_id.is_none());
    assert!(outcome.risk_source_run_id.is_none());
    assert!(!outcome.force_halted);
}

#[rstest]
fn isolated_recovery_seals_running_predecessor_and_forces_halted() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-isolated-recovery";
    let mut backend = RedbBackend::new(tmp.path());
    let run_manifest = manifest(run_id);
    backend
        .open_run(run_manifest.clone())
        .expect("open running run");
    backend
        .append_batch(&[AppendEntry::without_indices(build_run_started_entry(
            &run_manifest,
        ))])
        .expect("append RunStarted");
    drop(backend);

    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");
    let outcome = verifier
        .recover_predecessors(tmp.path(), INSTANCE_ID, VerifyLimits::default())
        .expect("isolated recovery");

    assert_eq!(outcome.recovered.len(), 1);
    assert_eq!(outcome.recovered[0].run_id, run_id);
    assert_eq!(outcome.recovered[0].status, RunStatus::CrashedRecovered);
    assert_eq!(outcome.parent_run_id.as_deref(), Some(run_id));
    assert_eq!(outcome.risk_source_run_id.as_deref(), Some(run_id));
    assert!(outcome.force_halted);
}

#[rstest]
fn isolated_recovery_rejects_entry_budget_before_mutating_predecessor() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-isolated-recovery-budget";
    let mut backend = RedbBackend::new(tmp.path());
    let run_manifest = manifest(run_id);
    backend
        .open_run(run_manifest.clone())
        .expect("open running run");
    backend
        .append_batch(&[
            AppendEntry::without_indices(build_run_started_entry(&run_manifest)),
            append_with(2, 11, Vec::new()),
        ])
        .expect("append rows");
    drop(backend);

    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");
    let limits = VerifyLimits {
        max_entries: 1,
        ..VerifyLimits::default()
    };

    let outcome = verifier
        .recover_predecessors(tmp.path(), INSTANCE_ID, limits)
        .expect("over-budget predecessor must return a conservative bounded outcome");
    assert!(outcome.recovered.is_empty());
    assert!(outcome.parent_run_id.is_none());
    assert!(outcome.force_halted);
    let manifests = RedbBackend::list_runs(tmp.path(), INSTANCE_ID).expect("inspect fixture");
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].status, RunStatus::Running);
}

#[rstest]
fn isolated_recovery_enforces_one_cumulative_budget_across_both_entry_scans() {
    let tmp = TempDir::new().expect("tempdir");
    let run_id = "1700000000-isolated-recovery-total-entry-budget";
    let mut backend = RedbBackend::new(tmp.path());
    let run_manifest = manifest(run_id);
    backend
        .open_run(run_manifest.clone())
        .expect("open running run");
    let started = build_run_started_entry(&run_manifest);
    let row_bytes = u64::try_from(
        codec::encode_to_vec(&started)
            .expect("encode RunStarted")
            .len(),
    )
    .expect("encoded row length");
    backend
        .append_batch(&[AppendEntry::without_indices(started)])
        .expect("append RunStarted");
    drop(backend);

    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");
    let limits = VerifyLimits {
        // Each individual pass could decode the row under this limit, but the two recovery
        // passes together cannot. The helper must budget the complete recovery operation.
        max_total_entry_bytes: row_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_sub(1))
            .expect("test budget"),
        ..VerifyLimits::default()
    };

    let outcome = verifier
        .recover_predecessors(tmp.path(), INSTANCE_ID, limits)
        .expect("over-budget predecessor must return a conservative bounded outcome");
    assert!(outcome.recovered.is_empty());
    assert!(outcome.parent_run_id.is_none());
    assert!(outcome.force_halted);
    let manifests = RedbBackend::list_runs(tmp.path(), INSTANCE_ID).expect("inspect fixture");
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].status, RunStatus::Running);
}

#[cfg(unix)]
#[rstest]
fn isolated_recovery_rejects_symlink_instance_directory() {
    use std::os::unix::fs::symlink;

    let base = TempDir::new().expect("base");
    let outside = TempDir::new().expect("outside");
    symlink(outside.path(), base.path().join(INSTANCE_ID)).expect("symlink instance dir");
    let helper = std::path::Path::new(env!("CARGO_BIN_EXE_verify"));
    let helper_measurement = measure_regular_file(helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(helper, helper_measurement.digest).expect("trusted helper");

    assert!(matches!(
        verifier.recover_predecessors(base.path(), INSTANCE_ID, VerifyLimits::default()),
        Err(IsolatedVerifyError::InvalidPath { .. })
    ));
}

#[cfg(unix)]
#[rstest]
fn isolated_parent_bounds_untrusted_helper_output_without_pipe_deadlock() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().expect("tempdir");
    let source = tmp.path().join("source.redb");
    fs::write(&source, b"not opened by hostile helper").expect("source");
    let helper = tmp.path().join("hostile-helper");
    fs::write(&helper, b"#!/bin/sh\nhead -c 1048576 /dev/zero\nexit 0\n").expect("helper script");
    let mut permissions = fs::metadata(&helper).expect("metadata").permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&helper, permissions).expect("chmod");
    let helper_measurement = measure_regular_file(&helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(&helper, helper_measurement.digest).expect("trusted fixture");

    assert!(matches!(
        verifier.verify_file(&source, VerifyLimits::default()),
        Err(IsolatedVerifyError::OutputLimitExceeded)
    ));
}

#[cfg(unix)]
#[rstest]
fn isolated_parent_rejects_helper_replacement_after_construction() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().expect("tempdir");
    let source = tmp.path().join("source.redb");
    fs::write(&source, b"bounded source").expect("source");
    let helper = tmp.path().join("replaceable-helper");
    fs::write(&helper, b"#!/bin/sh\nexit 1\n").expect("helper script");
    let mut permissions = fs::metadata(&helper).expect("metadata").permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&helper, permissions).expect("chmod");
    let helper_measurement = measure_regular_file(&helper, u64::MAX).expect("measure helper");
    let verifier =
        IsolatedVerifier::new(&helper, helper_measurement.digest).expect("trusted fixture");
    fs::write(&helper, b"#!/bin/sh\nexit 0\n").expect("replace helper");

    assert!(matches!(
        verifier.verify_file(&source, VerifyLimits::default()),
        Err(IsolatedVerifyError::HelperMeasurementMismatch)
    ));
}
