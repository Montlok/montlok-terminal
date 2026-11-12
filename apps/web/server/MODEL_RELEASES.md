# Model releases

`ModelReleaseStore` registers model versions and selects the published version of each
runner. The supervisor and the workers start runs and load models; existing runs keep the
version they started with.

| Runner | Family | Device | Runs |
|---|---|---|---|
| `gru_v1` | `recent_btc_gru` | cpu or cuda | `shadow`, `sandbox` model groups |
| `rdt4quant_v1` | `rdt4quant_multiasset` | cuda | `shadow` model groups |
| `rdt4quant_cpu_v2` | `rdt4quant_multiasset` | cpu | `shadow` model groups and `live` groups |

A manifest's `allowedModes` lists the modes a release may run in; `live` is reserved for
`rdt4quant_cpu_v2`.

Runtime code is preinstalled by the host administrator; bundle `sources` are provenance
records. A passing schema and hash check gives `validation: artifact_valid`; the release
stays `deployReady: false` until a target-node preflight verifies the installed runner,
device and resources.

## Artifact format

Upload a `kind=model` ZIP with `manifest.json` at its root. Every `model`, `sources` and
file-shaped provenance record has a relative `path` and a lowercase SHA256. An archive
holds 1–256 regular, unencrypted entries, stored or deflated, with unique paths after
NFC case folding, at most 128 MiB expanded and a 100:1 decompression ratio per entry.
Published files are copied into a new content-addressed directory.

A model can also be registered as a JSON manifest whose file records carry an
`artifactId`: a registered artifact with the same SHA256.

Schema version 1 binds:

- Release ID, runner ID, model family/version, model weights hash and source hashes.
- Ordered feature names, sequence length, completed-bar interval, required markets,
  normalization mean/scale and clipping bounds; RDT has per-domain/asset contracts.
- Output meaning, scale, horizon units and the selected output head: RDT names it per
  domain in `selectedHeadByDomain`, with domain-keyed horizons and units; GRU has
  matching `selectedHorizon` / `selectedHorizonUnit`.
- Device, maximum batch and queue sizes, timeout and maximum feature age.
- Allowed modes, signal threshold and allocation cap.
- Research provenance; `independentTestMetrics` is `null` until an independent
  evaluation exists.

Validation hashes the weights; the inference worker loads them. `modelQuality` reads
`not_evaluated` and target-device availability `not_checked` until those checks run.

## Confirmed publication

1. `validate(artifact_id)` reads and validates the artifact.
2. `prepare("publish", {artifactId})` freezes `manifestSha256` and
   `expectedActiveReleaseId` in the authenticated confirmation flow.
3. `execute("publish", normalized_request, operation_id, authenticated_actor)` commits the
   immutable release, the active pointer and the operation receipt in one SQLite
   transaction. A repeated ID returns the original receipt; a conflicting payload or a
   changed active version is rejected.
4. `prepare("rollback", {releaseId})` selects an earlier published version. Execution
   rechecks its manifest and file hashes and atomically moves the published-version
   pointer; running jobs keep their version.

The store keeps `releases.sqlite` and the `releases/` directories. Publication uses SQLite
DELETE journal mode so the supervisor reads the catalog through a read-only mount; WAL
catalogs migrate under SQLite's own lock while readers continue. `incoming/` holds uploads
in progress. `deploy/backup.py` takes a consistent database snapshot and copies every
immutable release file, and `check_backup.py` validates the index, active pointer,
manifest, weights and metadata. Model groups also preserve their host runner, guard,
settings and worker code. A restore recreates WAL/SHM files and process identities;
in-flight worker operations are marked unknown in the backup copy and wait for an
operator.

## Control receipts

Group control carries one `operationId` from the BFF through the supervisor to the
worker. The worker commits the intent before calling a handler and the result before
writing the socket reply. An incomplete reply, disconnect or timeout is `unknown`: the
outcome is queried, and only an operator resubmits.

`GET /api/operations/{id}` is read-only and reconciles an unknown group receipt from the
worker's `receipt` command or persisted journal; each operation's own receipt decides its
completion. Authenticated users read the receipts of their stable operator identity;
legacy rows without an owner keep their original session restriction. Replaying the
execute endpoint stays bound to the original session.

`receiptStatus=completed` describes completion of the requested control step. For a stop
operation it can accompany `status=stopping`; the run becomes `stopped` after process
exit, final evidence and report checks. Shutdown turns away new controls and finishes
accepted controls before the engine is disposed.
