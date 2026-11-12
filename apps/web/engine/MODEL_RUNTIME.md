# Model inference

The engine runs published model releases in a local inference subprocess next to
the Nautilus node. Each release names one runner:

| `runnerId` | Model | Device | Modes | Runtime |
|---|---|---|---|---|
| `gru_v1` | GRU on 15-minute BTC spot, BTC perpetual and ETH spot bars | CPU | `shadow`, `sandbox` | PyTorch or montlok (`modelRuntime.inferenceBackend`) |
| `rdt4quant_v1` | RDT4quant multi-asset | CUDA BF16 | `shadow` | PyTorch with official Mamba3 |
| `rdt4quant_cpu_v2` | RDT4quant multi-asset, crypto BTC-USDT on 1-minute bars | CPU | `live` | `montlok.rdt` kernels |

Training and offline comparison happen on the research hosts; production
inference is the local subprocess on the Tokyo host.

## Components

| File | Role |
|---|---|
| `model_release.py` | Packages GRU and RDT releases, validates manifests, and loads the host-installed reviewed runner code by hash (`torch.load(weights_only=True)` for weights) |
| `model_inference_worker.py` | One persistent model instance: warmup, bounded queue, batches of one contract, deadline checks before and after inference, hash-tagged outputs and latency counters |
| `model_client.py` | Starts the Rust guard and the inference worker. The guard normalizes inputs, validates predictions and turns them into targets; a timed-out prediction stops both processes |
| `model_strategy.py` | Nautilus strategy of a model group: receives feature windows on `model.features.<releaseId>`, publishes targets on `model.targets.<releaseId>`, and in `sandbox` mode applies spot long/cash targets through a verified `SandboxExecutionClient` |
| `model_market_actor.py` | OKX public closed-candle producer; runs the reviewed feature code off the event thread |
| `model_group_worker.py` | Model group process: hardened node and settings, producer, strategy, control socket and run artifacts |
| `live_model_worker.py` | Live `rdt4quant_cpu_v2` execution: one-minute feature windows through guard and worker, then post-only limit orders toward the target fraction on BTC-USDT |
| `model_probe.py` | GRU bars → original features → scaler → prediction, through the guard or with `--direct-offline` on historical data |
| `model_live_probe.py` | One current public-data GRU prediction through guard and worker, compared with the direct reference |
| `model_rdt_probe.py` | CUDA BF16 RDT reload and timed inference on prepared NPZ windows |
| `model_normalization_audit.py` | Bitwise comparison of NumPy and Rust-guard normalization on historical GRU windows |
| `model_capsule_probe.py`, `model_ingress_ws_probe.py` | Native market-data ABI and OKX subscription-ACK regressions |
| `model_preopen_validation.py` | Administrator smoke run of shadow groups in an isolated directory on the Tokyo host |

## Data flow

1. The market actor publishes a raw completed-bar window on `model.features.<releaseId>`;
   `asOfNs` is the UTC close of the last completed bar.
2. The guard's `prepare` normalizes it and returns the `workerRequest`.
3. The inference worker predicts on normalized float32 inputs.
4. The guard's `validate_prediction` checks the prediction and derives the target fraction,
   published on `model.targets.<releaseId>`.

GRU windows come from three aligned 15-minute markets with OKX's confirmed
`volCcyQuote` as quote volume and the original `recent_btc/prepare.py` feature code
(hash-checked by `model_probe.py`). A full GRU warmup takes 128 aligned bars (96-bar
feature lookback + 32-bar sequence).

RDT runs one host-selected contract. Crypto loads 963 aligned 1-minute bars (480-bar
feature lookback + 480-bar sequence + 3 overlap bars) and then refreshes the three latest
bars; token-hour loads 131 hourly bars (64 + 64 + 3) and uses base `volume`, as
`fullpass.prepare` and its historical CSV do. A gap triggers a bounded re-bootstrap.
Both feature implementations compute float64 inputs, and the contract's cadence bounds
input age before and after inference.

## Release manifest

A release ZIP has `manifest.json` at its root; every model and source path is relative
and carries a SHA256. The root registry pins the exact manifest hash. Bundled sources are
provenance records: the runner code comes from `--runner-root`, a host-installed directory
of reviewed pipelines. GRU model and feature hashes are fixed in `model_release.py`; RDT
compares the installed source tree with the release provenance before import.

Key fields: `schemaVersion: 1`, `releaseId`, `runnerId`, `family`, `modelVersion`,
`model`, `sources`, `featureContract` (GRU) or `domainContracts` (RDT), `outputContract`,
`runtime`, `policy` and `provenance`. A domain contract binds ordered features, scaler,
clip, sequence length, instrument and asset embedding ID. RDT releases name their output
head per domain in `selectedHeadByDomain`.

Requests carry `requestId`, `releaseId`, `modelHash`, `instrument`, optional RDT `domain`,
`mode`, `asOfNs`, `deadlineNs`, ordered `featureNames`, `inputs[sequence][features]` and
`normalized`. Responses carry identity, input and completion timestamps, the selected
`horizon` / `horizonUnit`, the scalar `prediction` as a simple return, RDT's full
`quantilesBps` matrix (log-return basis points), and queue, inference and total time.

## GRU releases

The bundled `releases/gru-latest-20260906-v1` is the `btc_gru_recent_20260905/latest`
export:

- Model SHA256 `e14b9e6327ca24bfe0d8d4ca969f0b5c76c912c068bb0ce84f091172d2cc1b9c`.
- 40 features, sequence 32.
- Validation-selected threshold 12 bps, long/cash maximum fraction 1.
- Output scale 1000; forecast from the open of t+1 to the open of t+5 (four 15-minute bars).
- `provenance.independentTestMetrics` is `null` for the latest refit.

```sh
python model_release.py gru --model-dir /reviewed/models/trained/btc_gru_recent_20260905/latest \
  --source-dir /reviewed/pipelines/recent_btc --output /releases/new-version --release-id new-version
python model_probe.py --manifest /releases/new-version/manifest.json --manifest-sha256 MANIFEST_DIGEST \
  --runner-root /reviewed/pipelines --data-dir DATA_DIR --guard-binary /installed/model-guard
```

The first command prints the manifest SHA256. `model_probe.py --direct-offline` replays
historical data for comparison; readiness comes from the guarded probe.

## RDT releases

GPU releases (`rdt4quant_v1`) need a stable `results/latest/seed_N/best.pt` from a
completed full pass, its `prepared/manifest.json`, the parent crypto
`prepared/metadata.json` and the reviewed pipeline source root; the checkpoint's asset
order matches the prepared metadata, and metrics come from this joint checkpoint. The
source-tree digest is the SHA256 of the sorted relative `path:sha256` lines of
`rdt_source_files(root)`, joined with `\n`: `rdt4quant/model.py`, `rdt4quant/prepare.py`,
`rdt4quant/download.py`, every runtime `.py` under `rdt4quant/native` outside `tests/`,
`rdt4quant_fullpass/multiasset_model.py` and `rdt4quant_fullpass/prepare.py`. A JSON file
maps each domain to its zero-based output head; horizons come from the checkpoint config.

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

Repeat the probe for batch 8 and each domain against its prepared NPZ; the p50/p95/p99
on the production host must fit the release deadline. These releases run on nodes with
CUDA BF16 and official Mamba3, whose `forward` uses Triton/TileLang and `step` CuteDSL
and Triton rotary
([mamba3.py](https://github.com/state-spaces/mamba/blob/main/mamba_ssm/modules/mamba3.py),
[reference tests](https://github.com/state-spaces/mamba/blob/main/tests/ops/triton/test_mamba3_siso.py)).

CPU releases (`rdt4quant_cpu_v2`) come from an installed montlok.cpp checkpoint:
`deploy/publish_cpu_model.py` publishes it through the model catalog, and
`deploy/enable_cpu_model_live.py` binds it to `live_model_worker.py`
(`modelRuntime.liveExecution`, `runnerRoots`, `contractKey`).

## montlok

`model_release.py` uses montlok (`import montlok`, from
[montlok.cpp](https://github.com/Montlok/montlok.cpp)) in two places:

- `rdt4quant_cpu_v2` releases run `montlok.rdt` over the host-reviewed
  `runnerRoot/rdt4quant/native` Model tree once its hashes are verified, with the
  installed `montlok._C` kernels (`MONTLOK_REQUIRE_PREBUILT=1`).
- GRU releases run on montlok when the root registry sets
  `modelRuntime.inferenceBackend` to `montlok` (GROUP_RUNTIME.md). The worker exports
  the reviewed `ReturnGRU` to montlok at load time and starts once the export matches
  PyTorch within 1e-5 of the output scale on 16 probe windows across the published clip
  range. On an M4 Max the bundled release answers a single request in 54 µs (PyTorch:
  216 µs); predictions agree to 1e-8.

The worker's `ready` event carries `backend`: the montlok version, the number of
installed montlok files that match the wheel RECORD, and the GRU parity result. Install
montlok into the model runtime environment after torch, so its kernels are built against
that torch:

```sh
pip install --no-build-isolation "montlok @ git+https://github.com/Montlok/montlok.cpp@v0.2.0"
```

## Deployment

`requirements-model-cpu.txt` lists the tested versions: `torch==2.11.0`, `numpy==2.2.6`,
`pandas==3.0.2`, `certifi==2026.4.22`, `psutil==7.2.2` and `montlok==0.2.0`. Use the
Python interpreter family of the compiled Nautilus node (Python >=3.12,<3.15 and msgspec
>=0.21.1,<1.0.0); a venv with `--system-site-packages` reuses the installed native
Nautilus build. CPU-only hosts use the official CPU PyTorch wheel index.

The worker directory holds `model_group_worker.py`, `model_market_actor.py`,
`model_strategy.py`, `model_client.py`, `model_inference_worker.py`, `model_release.py`,
`model_probe.py`, `commands.py` and `control.py`; its sibling `server` directory holds
the registered `group_runtime.py` and the model release store. The hardened
`runtimePath` holds `settings.py`, `node_config.py`, `health.py` and `alerts.py`. A GRU
`runnerRoot` needs the reviewed `recent_btc/train_gru.py` and `recent_btc/prepare.py`.

## Checks

```sh
python -m unittest -v test_model_inference test_model_rdt_features
```

`test_model_inference` covers target validation, real GRU packaging and reload, feature
order and hash checks, mode checks, nonfinite and shape errors, the bounded queue, expired
requests, late-output discard, eight concurrent requests to a persistent worker, and the
montlok GRU backend against PyTorch on the bundled release. `test_model_rdt_features`
compares the original RDT crypto 480x57 and token 64x23 feature arrays exactly, checks
token base-volume semantics, missing bars, feature order and the host-selected contract,
and kills a real inference subprocess to confirm readiness turns false; the group
supervisor checks both child processes every cycle, and `control.status` reports the
actual `warmupComplete`, `modelHash` and `manifestSha256`. Test fixtures use synthetic
timestamps and local processes.

`model_live_probe.py` takes the manifest, hash, runner-root and guard-binary arguments
and runs one current public-data prediction through the guard and the persistent worker,
compared with the direct reference. Node lifecycle, Sandbox fills and equity history are
verified on the target host's compiled Nautilus runtime. Publishing a release makes it
available to start; a run starts from a confirmed operation.
