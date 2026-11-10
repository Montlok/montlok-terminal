# nautilus-event-store

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

Embedded event store and authoritative log of state-affecting messages for
[NautilusTrader](https://nautilustrader.io).

> [!WARNING]
> **Early alpha**. The API is not stable and may change between versions. Event-store capture,
> replay, and verification workflows are the current focus.

The `nautilus-event-store` crate captures commands, generated events, raw venue reports,
reconciliation outputs, and request/response traffic at the message bus boundary. It persists
those messages as a durable per-run log and exposes read-only verification and replay surfaces.

For the design model, capture boundaries, replay modes, and operational examples, see the
[Event Sourcing concept guide](../../docs/concepts/event_sourcing.md).

## Design contract

The event store is the durable boundary for deterministic engine history.

- The synchronous core is deterministic.
- The cache is a write-through projection, not the source of truth.
- Replay derives state from captured history and named deterministic replay rules.
- The event store records ordered inputs and generated state-affecting messages.
- Anything not recorded is non-state-affecting or named as a deterministic replay rule.
- External I/O becomes replayable when captured as raw reports or commands.

## What this crate provides

`nautilus-event-store` is a single-node, embedded event store for one trading instance. It defines:

- `BusCaptureAdapter`: the seam between message bus dispatch and the writer.
- `EventStoreLifecycleOptions`: runtime-only lifecycle policy for custom encoder registries,
  backend openers, and an explicitly pinned isolated verifier.
- `EventStoreWriter`: the append path, batching, high-watermark advancement, and fail-stop signaling.
- `EventStoreReader`: the read-only range scan, point lookup, and replay-facing surface.
- `RedbBackend`: the default on-disk backend, with one `redb` file per run.
- `MemoryBackend`: the in-process backend used by focused tests and simulation-style capture.
- `Verifier`: the library surface for integrity checks over a single run.
- `verify`: the standalone binary for process-isolated verification of sealed run files.
- `plan_redb_retention`: a non-destructive planner for sealed run-file reclaim candidates.
- `ReplayInputPlan`: planned event-store entries in `seq` order, optionally with typed catalog
  slice plans.
- `ReplayInputs`: loaded replay entries in `seq` order, optionally with catalog records grouped
  under selected slices.

The crate does not replace the data catalog, provide OLAP queries, or aggregate multiple trader
instances into a consensus log.

## Operational use today

Verify a sealed run file:

```bash
cargo run -p nautilus-event-store --bin verify -- /path/to/run.redb
```

Verifier exit codes:

- `0`: the run is clean.
- `1`: the run has corrupt findings, or the verifier worker aborted or timed out.
- `2`: the verifier could not open or run against the requested file.

The verifier opens the run through a read-only `redb` handle and reports
`quarantine=not-performed`. A supervisor or operator process owns quarantine policy. If a sibling
`.markers.redb` exists, this diagnostic command returns a corrupt/unsupported verdict without
opening it; marker tables are not accepted until they have equivalent streaming budgets.
The diagnostic parent drains worker stdout and stderr concurrently, retains at most 1 MiB from
each stream, and returns a corrupt verdict if either stream exceeds that cap.

Verification fails closed under default hard budgets: a 1,000,000 sequence high-watermark,
1,000,000 stored entries, 2,000,000 combined order-index rows, and an 8 GiB sealed file. It also
limits one encoded entry to 16 MiB, cumulative encoded entries to 4 GiB, cumulative index-key bytes
to 256 MiB, retained findings to 10,000, and each manifest or snapshot-anchor row to 4 MiB. Counts
and encoded lengths are checked before their corresponding full decode or scan, and digests are
updated incrementally. Trusted research tooling can opt into larger limits only through
`Verifier::open_redb_with_limits`, `Verifier::open_redb_file_with_limits`, or
`Verifier::with_limits` and an explicit `VerifyLimits` value.

Read a sealed run from Rust in trusted offline tooling (not in a production trader process):

```rust
use nautilus_event_store::{EventStoreReader, RedbBackend, ScanDirection};

fn inspect_run() -> Result<(), Box<dyn std::error::Error>> {
    let backend =
        RedbBackend::open_sealed_file("./event_store/trader-001/1700000000-cafe0001.redb")?;
    let reader = EventStoreReader::new(backend);
    let high_watermark = reader.high_watermark()?;

    for entry in reader.scan_range(1, high_watermark, ScanDirection::Forward) {
        let entry = entry?;
        println!("{} {}", entry.seq, entry.topic);
    }

    Ok(())
}
```

## Lifecycle options

`EventStoreLifecycle::boot(...)` retains the default `RedbBackend` and `default_registry()`
selection, but canonical production redb lifecycle work also requires an explicitly trusted
`IsolatedVerifier`. Because that process-local trust handle is not serialized, production callers
use `EventStoreLifecycle::boot_with_options(...)`; calling the shorter constructor with canonical
redb configuration fails closed for a missing helper. The helper is never resolved from `PATH` or
an environment variable.

Register a custom bus payload before the lifecycle opens a run:

```rust
use bytes::Bytes;
use nautilus_event_store::{
    EncodedPayload, EncoderRegistry, EventStoreLifecycleOptions,
};
use ustr::Ustr;

#[derive(Debug)]
struct AuditRecord {
    payload: Bytes,
}

let mut registry = EncoderRegistry::new();
registry.register::<AuditRecord, _>(Ustr::from("AuditRecord"), |record| {
    Ok(EncodedPayload::without_indices(record.payload.clone()))
});

let options = EventStoreLifecycleOptions::new().with_encoder_registry(registry);
// Pass `options` to EventStoreLifecycle::boot_with_options(...).
```

The same options type accepts a backend opener. The default opener remains `RedbBackend`; tests or
simulation harnesses can supply an opener that returns `MemoryBackend` or another `EventStore`
implementation.

For canonical redb production, deploy the `verify` executable at an absolute, non-symlink path
owned by root or the trader account, without group/other write or set-id permissions. Pin the exact
`ContentDigest` of its bytes in deployment policy and inject it explicitly:

```rust,ignore
use nautilus_event_store::{EventStoreLifecycleOptions, IsolatedVerifier};
use nautilus_provenance::ContentDigest;

let expected_digest: ContentDigest = load_pinned_deployment_digest()?;
let isolated = IsolatedVerifier::new("/opt/nautilus/bin/verify", expected_digest)?;
let options = EventStoreLifecycleOptions::new().with_isolated_verifier(isolated);
// Pass `options` to EventStoreLifecycle::boot_with_options(...).
```

Boot inventory, crashed-run repair, and post-seal verification then execute only in the helper.
The parent receives a bounded `NEV1` result and never opens predecessor redb pages. The current
protocol does not return bounded cache or risk-checkpoint projection DTOs: configured production
replay returns `CacheReplayError::IsolatedProjectionRequired` (or the corresponding kernel error),
and recovery conservatively forces risk state to `Halted`. The kernel latches incomplete recovery,
so ordinary state changes, reset, risk bypass, and a historical checkpoint cannot switch it back to
`Active`; an authenticated durable re-arm protocol is not implemented. There is no in-process
production fallback. Data-marker sidecars are likewise rejected at boot until their tables have an
equivalent bounded helper protocol.

## Storage model

The default backend is [`redb`](https://docs.rs/redb/latest/redb/), a pure-Rust ACID key-value
store. The backend uses one file per run:

```text
<base>/<instance_id>/<run_id>.redb
```

Each file stores:

- Entries keyed by monotonic `seq`.
- Secondary indices for `client_order_id` and `venue_order_id`.
- A manifest written at run start and sealed at run end.
- An optional snapshot anchor for cache restore.

Every commit uses `Durability::Immediate`. The high-watermark advances only after the backend
acknowledges durability.

## Failure and verification model

The verifier is intentionally process-isolated. Some corrupted `redb` files can panic on open or
first read, and the release profile builds with `panic = "abort"`. The `verify` binary delegates
the scan to a worker subprocess so a bad run file aborts the worker, not the caller. Receipt mode
copies the source from one held file handle into a private read-only snapshot while incrementally
hashing it; only that snapshot is opened by `redb`. The parent reads stdout and stderr concurrently,
caps each protocol stream at 4 KiB, enforces a 60-second wall deadline, and kills abnormal workers.
On Unix the worker also applies CPU, file-size, and open-file limits; Linux/Hurd additionally get a
2 GiB address-space limit (RSS is best-effort). Logical byte/row budgets remain authoritative on
platforms where a hard RSS/address-space limit is unavailable.

The helper is part of the deployment trusted computing base. For every verification and recovery
operation, the parent opens the deployment path once with `O_NOFOLLOW`, validates the held regular
file's owner and mode, and streams that same descriptor into a fresh parent-owned `0700` directory
while hashing it. The `0500` destination is flushed, synced, re-hashed, and checked against the
deployment digest before the parent executes that private path; the directory guard outlives the
worker and the directory itself is synced. Replacing the deployment pathname after its descriptor
was opened therefore cannot change the executable used by that operation, including a
trusted-to-malicious-to-trusted ABA replacement. Replacement before pinning either supplies the
pinned bytes or fails closed on digest/metadata validation.

These Unix permissions isolate the private copy from processes running under a different UID. A
malicious process with the same UID/credentials as the trader is inside this deployment TCB: Unix
owner permissions alone cannot prevent that process from changing permissions, inspecting the
process, or interfering with its private files. Run the trader under a dedicated OS account and do
not co-locate untrusted same-UID processes. The isolated helper path is currently fail-closed as
unsupported on non-Unix platforms rather than claiming an equivalent pinning guarantee.

Integrity checks include:

- Recomputing every entry hash.
- Detecting gaps inside the high-watermark.
- Checking table-key and embedded-`seq` agreement.
- Cross-checking secondary indices against entries.
- Validating manifest status and high-watermark fields.

Resource-limit refusal is an operational error, not a clean or corrupt verdict: the verifier did
not inspect the full run. Raising a budget is an explicit trust/deployment decision and does not
change the standalone worker timeout.

Receipt mode also refuses empty or non-canonical `binary_hash`, `crate_versions`, and `config_hash`
manifest commitments. These fields are caller-supplied commitments in the generic lifecycle API;
only a concrete host integration that measures and pins them may describe them as measured.
`EventStoreSealReceipt` currently binds the verified store digest but does not serialize the helper
digest, source-file fingerprint, or `NEV1` protocol version. It is therefore not yet sufficient as
standalone production promotion evidence; production evidence publication remains disabled until
that complete trust binding and an authenticated evaluator path are in place.

## Documentation

- [Event Sourcing concept guide](../../docs/concepts/event_sourcing.md): design, replay, recovery,
  and operational examples.
- [docs.rs](https://docs.rs/nautilus-event-store): generated Rust API reference.

## NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## License

The source code for NautilusTrader is available on GitHub under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

<img src="https://github.com/nautechsystems/nautilus_trader/raw/develop/assets/nautilus-logo-white.png" alt="logo" width="300" height="auto"/>

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
