# Model release boundary

`ModelReleaseStore` registers a model version and selects the published version
for a fixed runner. It never starts a process, imports uploaded Python, loads a
pickle, or submits a trade. Existing runs retain their frozen version.

The only runner IDs are `gru_v1` (`recent_btc_gru`) and `rdt4quant_v1`
(`rdt4quant_multiasset`). Runtime code is preinstalled by the host administrator;
bundle `sources` are provenance only. RDT requires CUDA and does not fall back to
CPU. A successful schema/hash check means `validation: artifact_valid`, not
deployment readiness. The release record remains `deployReady: false` until a
separate target-node preflight verifies installed runner, device and resources.

## Artifact format

Upload a `kind=model` ZIP with root `manifest.json`. Every `model`, `sources` and
file-shaped provenance record has a relative `path` and lowercase SHA256.
ZIP traversal, duplicate/casefold paths, symlinks, encrypted entries, unsupported
compression, oversized input and excessive decompression are rejected. Published
files are copied into a new content-addressed directory and never overwritten.

Alternatively register a model JSON manifest. Each file record must additionally
contain `artifactId`, referencing a previously registered artifact with the exact
same SHA256. Filesystem paths, URLs, executables and arbitrary environment
overrides are not accepted as artifact references.

Schema version 1 binds:

- Release ID, runner ID, model family/version, model weights hash and source hashes.
- Ordered feature names, sequence length, completed-bar interval, required markets,
  normalization mean/scale and clipping bounds. RDT has per-domain/asset contracts.
- Output meaning, scale, horizon units and explicit selected output head. RDT
  requires `selectedHeadByDomain`; it cannot silently use the first head.
  GRU requires matching `selectedHorizon` / `selectedHorizonUnit`; RDT horizons
  and units must both be domain-keyed objects.
- Device, maximum batch and queue sizes, timeout and maximum feature age.
- Explicit `shadow` / `sandbox` allowed modes, signal threshold and allocation cap.
- Research provenance includes `independentTestMetrics`, explicitly `null` when
  no independent evaluation exists. Missing evaluation is not manufactured.

Opaque weights are hashed, not executed during validation. `modelQuality` remains
`not_evaluated`; target-device availability remains `not_checked`.

## Confirmed publication

1. `validate(artifact_id)` reads and validates without creating a release.
2. `prepare("publish", {artifactId})` freezes `manifestSha256` and
   `expectedActiveReleaseId` in the existing authenticated confirmation flow.
3. `execute("publish", normalized_request, operation_id, authenticated_actor)`
   commits the immutable release, active pointer and operation receipt in one
   SQLite transaction. A repeat ID returns the original receipt; conflicting
   payloads or a changed active version are rejected.
4. `prepare("rollback", {releaseId})` selects an explicit previously published
   version. Execution rechecks its manifest and file hashes and atomically changes
   only the published-version pointer. It never restarts or changes a running job.

The store preserves `releases.sqlite` and the `releases/` directories. Publication
uses SQLite DELETE journal mode so the supervisor can read the catalog through a
read-only filesystem mount without creating WAL/SHM sidecars. Existing WAL
catalogs migrate under SQLite's own lock, without interrupting active readers.
Temporary `incoming/` directories are not published evidence.
`deploy/backup.py` takes a consistent database snapshot and copies every immutable
release file, and `check_backup.py` validates the index, active pointer, manifest,
weights and metadata. Root-registered model groups additionally preserve their
host runner, guard, settings and worker code. Live WAL/SHM and process identities
are never restored; in-flight worker operations are marked unknown in the backup
copy and are never replayed automatically.

## Control receipts

Group control uses one `operationId` from the BFF through the supervisor to the
worker. The worker commits intent before calling a handler and commits its result
before writing the socket reply. An incomplete reply, disconnect or timeout is
`unknown`, never proof of failure and never a trigger for automatic resubmission.

`GET /api/operations/{id}` is read-only and can reconcile an unknown group receipt
from the worker's `receipt` command or persisted journal. A later control state is
not used as proof that this particular operation completed. Authenticated users
can read only receipts belonging to their stable operator identity; legacy rows
without an owner retain their original session restriction. Replaying the execute
endpoint remains bound to the original session.

`receiptStatus=completed` describes completion of the requested control step.
For a stop operation it can accompany `status=stopping`; the run becomes `stopped`
only after process exit, final evidence and report checks. Shutdown rejects new
controls and finishes accepted controls before the engine is disposed.
