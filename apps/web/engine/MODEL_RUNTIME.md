# Local model releases and inference

This runner loads existing trained models; it does not retrain or claim alpha.
Production inference is a local subprocess on the Tokyo host. There is no Spark
RPC or other remote prediction dependency. Spark is only for training/offline
comparison. Modes are `shadow` (targets only) and `sandbox` (Nautilus local
simulated execution with actual client-class inspection). Real execution is not
implemented by this path.

## Components

- `model_release.py`: immutable manifest + weights/scalers/provenance packaging,
  host-installed reviewed runner loading, safe `torch.load(weights_only=True)`.
- `model_inference_worker.py`: one persistent model instance, warmup, bounded
  queue, compatible-contract batching, per-request deadline checks before and
  after inference, hash-correlated outputs and latency counters.
- `model_client.py`: mandatory local Rust guard and local PyTorch subprocesses.
  A hard prediction timeout stops the dedicated processes. There is no Python
  fallback for the Rust guard and no CPU fallback for the RDT GPU runner.
- `model_strategy.py`: Nautilus Strategy receives raw completed-bar feature
  windows on `model.features.<releaseId>`, schedules async inference, publishes
  validated targets on `model.targets.<releaseId>`, and optionally applies spot
  long/cash targets only to a verified SandboxExecutionClient.
- `model_market_actor.py`: local async public-OKX confirmed-candle producer;
  exact quote-currency volume and original reviewed feature code, off the event
  callback thread; three aligned markets, stale and gap checks, no fake candles.
  RDT supports explicitly selected `crypto:BTC-USDT` (57 features, 480 x 1m
  input sequence) and `token_hour:XNVDA` (23 features, 64 x 1H sequence).
- `model_group_worker.py`: fixed root-registered worker assembling the hardened
  node/settings, producer, model Strategy, durable control endpoint and isolated
  manifest/status/view/equity/final artifacts. The root registry's `modelRuntime`
  selects installed paths and the explicit RDT `contractKey` (string or
  `{ "rdt4quant_v1": "crypto:BTC-USDT" }` mapping). This selection is not a
  browser argument and is not added to immutable manifest.runtime.
  Published releases are dynamic groups named
  `model-<manifestSha256 first 16 characters>`. Production mode starts as shadow.
- `model_probe.py`: existing GRU bars → original feature implementation →
  identical scaler → actual prediction. Historical direct reference mode is
  explicitly distinct from a current guarded runtime probe.
- `model_rdt_probe.py`: existing NPZ eligible windows → real CUDA BF16 weights,
  selected horizon, all quantiles, batch latency and repeat-error measurement.

The model strategy does not manufacture candles from quote ticks. Its upstream
market actor must supply all three GRU markets, aligned completed bars and true
`volume_quote`. The included actor reads OKX's confirmed `volCcyQuote` directly;
base volume times close is not substituted. The original feature code in `recent_btc/prepare.py` is the
reference, and `model_probe.py` validates its installed hash. Source timestamps
label bar opens; runtime `asOfNs` is the completed bar's UTC close timestamp.

## Release trust and schema

ZIP root is `manifest.json`. All model/source paths are relative, accompanied by
SHA256. The root-admin launch registry pins the exact manifest hash. A digest
alone is integrity evidence, not authority to execute unreviewed code.

Uploaded/bundled Python source is provenance only and is **never imported**.
`--runner-root` is a host-admin process option, not a manifest field. It names
preinstalled reviewed research pipeline implementations. GRU model and feature
implementation hashes are additionally fixed in `model_release.py`; RDT compares
the installed full source tree against reviewed provenance before import.

Key fields are `schemaVersion:1`, `releaseId`, `runnerId` (`gru_v1` or
`rdt4quant_v1`), `family`, `modelVersion`, `model`, `sources`, `featureContract`
(GRU) or `domainContracts` (RDT), `outputContract`, `runtime`, `policy`, and
`provenance`. Each domain contract binds ordered features, scaler, clip, sequence
length, instrument and asset embedding ID. RDT `selectedHeadByDomain` is required
and selected explicitly when packaging; no first-head assumption is allowed.

Runtime requests contain `requestId`, `releaseId`, `modelHash`, `instrument`,
optional RDT `domain`, `mode`, `asOfNs`, `deadlineNs`, ordered `featureNames`,
`inputs[sequence][features]`, and `normalized`. The guard takes unnormalized
inputs and returns `workerRequest`. The worker requires normalized float32 data.
Responses carry identity, input timestamp, completion timestamp, selected
`horizon` / `horizonUnit`, scalar `prediction` as simple return, and RDT's full
`quantilesBps` matrix (log-return basis points), with queue/inference/total time.

## Verified GRU artifact

The bundled `releases/gru-latest-20260906-v1` is the existing
`btc_gru_recent_20260905/latest` export, not a new fit:

- Model SHA256: `e14b9e6327ca24bfe0d8d4ca969f0b5c76c912c068bb0ce84f091172d2cc1b9c`.
- 40 features, sequence 32, 15-minute BTC spot / BTC perpetual / ETH spot inputs.
- Full raw warmup needs 128 aligned bars (96-feature lookback + 32 sequence).
- Existing 12 bps validation-selected threshold, long/cash maximum fraction 1.
- Output scale 1000, forecast open t+1 to open t+5 (four 15-minute bars).
- Latest refit has no independent holdout performance attached.

Package a new immutable GRU release:

```sh
python model_release.py gru --model-dir /reviewed/models/trained/btc_gru_recent_20260905/latest \
  --source-dir /reviewed/pipelines/recent_btc --output /releases/new-version --release-id new-version
```

The CLI prints the exact manifest SHA. Run a current guarded probe with
`model_probe.py --manifest ... --manifest-sha256 ... --runner-root ...
--data-dir ... --guard-binary /installed/model-guard`. For historical data use
`--direct-offline` explicitly instead; that is not runtime readiness evidence.

Deployment requirements tested locally are recorded in
`requirements-model-cpu.txt`. The model requires `torch==2.11.0`,
`numpy==2.2.6`, `pandas==3.0.2`, `certifi==2026.4.22`, and `psutil==7.2.2`.
Use the same Python interpreter family as the existing compiled Nautilus node
(this checkout requires Python >=3.12,<3.15 and msgspec >=0.21.1,<1.0.0).
A separate venv using `--system-site-packages` can reuse the existing native
Nautilus build; do not install a different Nautilus release merely for PyTorch.
Use the official CPU PyTorch wheel index on CPU-only Tokyo.

The deployed worker directory needs `model_group_worker.py`,
`model_market_actor.py`, `model_strategy.py`, `model_client.py`,
`model_inference_worker.py`, `model_release.py`, `model_probe.py`, `commands.py`,
and `control.py`. Its sibling `server` directory needs the current registered
`group_runtime.py` and model release-store modules. Hardened `runtimePath` needs
`settings.py`, `node_config.py`, `health.py`, `alerts.py` and their existing
runtime dependencies. GRU `runnerRoot` needs only reviewed
`recent_btc/train_gru.py` and `recent_btc/prepare.py`.

## RDT packaging and offline GPU validation

Required artifacts are a stable `results/latest/seed_N/best.pt`, its full-pass
`prepared/manifest.json`, parent crypto `prepared/metadata.json`, and the reviewed
pipeline source root. Weights alone omit the per-asset input and output scalers.
The checkpoint must have completed a latest full pass and its asset order must
match the prepared metadata. Do not copy metrics from walk-forward fold models
onto this joint latest checkpoint.

Compute the reviewed source-tree digest over `rdt_source_files(root)` sorted
relative `path:sha256` lines joined with `\n` (no trailing newline). The set is
`rdt4quant/model.py`, `rdt4quant/prepare.py`, `rdt4quant/download.py`, all runtime `.py`
under `rdt4quant/native` excluding `tests/` and bytecode, `rdt4quant_fullpass/multiasset_model.py`, and
`rdt4quant_fullpass/prepare.py`. Pass that digest explicitly with
`--source-tree-sha256`. Pass an administrator-selected JSON file mapping each
domain to a zero-based output head through `--selected-head-by-domain`; horizons
are taken from the checkpoint's actual config.

```sh
python model_release.py rdt --checkpoint /stable/results/latest/seed_7/best.pt \
  --prepared-manifest /stable/prepared/manifest.json \
  --crypto-metadata /parent/prepared/metadata.json --source-root /reviewed/pipelines \
  --output /releases/rdt-version --release-id rdt-version \
  --source-tree-sha256 REVIEWED_DIGEST --threshold-bps PUBLISHED_THRESHOLD \
  --selected-head-by-domain /reviewed/selected-heads.json
python model_rdt_probe.py --manifest /releases/rdt-version/manifest.json \
  --manifest-sha256 MANIFEST_DIGEST --runner-root /reviewed/pipelines \
  --contract token_hour:XNVDA --data-npz /stable/prepared/token_hour.npz \
  --batch-size 1 --iterations 20
```

Repeat for batch 8 and the crypto/equity domains against their matching prepared
NPZ. The reported p50/p95/p99 must fit the release deadline on the actual
production host; Spark offline results alone do not establish Tokyo capacity.

The existing runner requires CUDA BF16 and official Mamba3. A CPU-only host must
report a missing CUDA requirement and must not start an RDT model run, even if
the ZIP and manifest are technically valid. Source review found that official
Mamba3 `forward` uses Triton / TileLang, while `step` uses CuteDSL and Triton
rotary. Official tests contain pure PyTorch SISO step/forward references, but
these are not an already integrated full-model CPU backend. A future CPU port
must preserve weights/architecture, replace all fused dependencies, and compare
full RDT outputs and target decisions against GPU golden results on all domains
before it can be offered as deployment-ready.

Continuous RDT production is single-contract, shadow-only. The root registry
selects the contract; the manifest selects its explicit output head. The worker
requires this contract at startup, warms that contract only and rejects another
domain/instrument. For crypto, initial pagination loads 963 aligned 1m bars
(480-feature lookback + 480 sequence + 3 overlap bars), then cached history is
updated with three latest bars. Token-hour uses 131 bars (64 + 64 + 3). Missing
intervals force a bounded rebootstrap, never synthetic bars. Crypto uses true
quote volume; token-hour intentionally uses original base `volume`, matching
fullpass.prepare and its historical CSV. Both feature implementations compute
float64 inputs. Cadence-specific freshness is enforced before and after model
inference. No equity-daily feed is silently substituted for either path.

Primary references:

- https://github.com/state-spaces/mamba/blob/main/mamba_ssm/modules/mamba3.py
- https://github.com/state-spaces/mamba/blob/main/tests/ops/triton/test_mamba3_siso.py

## Checks

`python -m unittest -v test_model_inference` exercises malformed targets, real
GRU packaging/reload, ordered feature and hash checks, invalid live mode,
nonfinite/shape errors, bounded queue, expired requests and late-output discard,
and eight concurrent requests to a real persistent worker. Synthetic timestamps
used by the protocol fixture are test data only. No test sends exchange orders.

`python -m unittest -v test_model_rdt_features test_model_inference` additionally
compares original RDT crypto 480x57 and token 64x23 feature arrays exactly,
checks token base-volume semantics and rejects missing bars, feature reordering
and a contract other than the host-selected one. It also kills a real inference
subprocess and verifies readiness becomes false. The group supervisor checks
both local child processes every cycle, and control.status carries the actual
`warmupComplete`, exact `modelHash` and `manifestSha256`; a dead child is an error,
not a permanently remembered successful warmup.

`model_normalization_audit.py` compared 32 real historical GRU windows, 40,960
values total. Original raw/scaler arrays were float64 and network inputs
float32. Rust f64 normalization/clipping then f32 matched both NumPy and the
saved training NPZ bit-for-bit (zero differences for all 40 features). An
incorrect f32-normalization variant differed on 20,328 values. The exact
per-feature evidence is `/private/tmp/gru-normalization-bit-audit-20260906.json`.

`model_live_probe.py` takes the same manifest/hash/runner-root/guard-binary
arguments and performs one real current public-data → Rust guard → persistent
worker → Rust target check with direct reference comparison. It does not create
a node or submit orders. On 2026-09-06, a local probe read 199 completed candles
for each of the three markets through the 05:45 UTC close; reference and guarded
forecasts both equaled `-0.0003701332211494446`, difference zero, target fraction
zero, worker latency 0.423 ms. This is actual inference evidence, not PnL evidence
or proof of Tokyo deployment. No source/training artifact was modified.

The Strategy and assembled model group are source/API checked against this
Nautilus checkout. Actual deployed-node lifecycle and Sandbox fill/equity history
need separate integration evidence in the compiled native runtime. Publication
does not automatically start a model strategy group. A technically valid RDT
manifest remains unavailable for running on CPU-only Tokyo.
