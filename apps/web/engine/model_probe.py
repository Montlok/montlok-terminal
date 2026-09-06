"""Read-only model probe using existing completed bars, no orders or retraining."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import time
import uuid

from model_release import GRU_FEATURE_HASH, ModelAdapter, import_file, load_manifest, sha


def gru_feature_window(manifest, data_dir, runner_root):
    source = Path(runner_root) / "recent_btc/prepare.py"
    if sha(source) != GRU_FEATURE_HASH:
        raise ValueError("Host-installed feature implementation changed")
    module = import_file("montlok_release_features", source)
    markets = {name: module.read_market(Path(data_dir) / f"{name}-15m.csv") for name in module.INSTRUMENTS}
    return gru_window_from_markets(manifest, markets, runner_root)


def gru_window_from_markets(manifest, markets, runner_root):
    import numpy as np
    import pandas as pd
    source = Path(runner_root) / "recent_btc/prepare.py"
    if sha(source) != GRU_FEATURE_HASH:
        raise ValueError("Host-installed feature implementation changed")
    module = import_file("montlok_release_features", source)
    if set(markets) != set(module.INSTRUMENTS):
        raise ValueError("All three trained markets are required")
    for name, frame in markets.items():
        if frame.index.has_duplicates or not frame.index.to_series().diff().iloc[1:].eq(pd.Timedelta(minutes=15)).all():
            raise ValueError(f"Duplicate, unordered or missing completed bars: {name}")
    common = pd.date_range(max(f.index[0] for f in markets.values()), min(f.index[-1] for f in markets.values()), freq="15min")
    if not len(common):
        raise ValueError("Market inputs have no overlapping history")
    markets = {name: frame.reindex(common) for name, frame in markets.items()}
    if any(frame.isna().any().any() for frame in markets.values()):
        raise ValueError("Cross-market input has gaps")
    features = module.features(markets)
    contract = manifest["featureContract"]
    if list(features.columns) != contract["names"]:
        raise ValueError("Feature implementation order differs from model metadata")
    features = features.iloc[-contract["sequenceBars"]:]
    raw = features.to_numpy(dtype="float64")
    if raw.shape != (contract["sequenceBars"], len(contract["names"])) or not np.isfinite(raw).all():
        raise ValueError("Insufficient finite feature history")
    # Source timestamps label bar OPEN. Inputs are available only after close.
    asof = int(features.index[-1].value) + contract["barSeconds"] * 1_000_000_000
    if asof > time.time_ns():
        raise ValueError("Latest input bar has not closed")
    return dict(instrument="BTC-USDT", featureNames=contract["names"], inputs=raw.tolist(), asOfNs=asof)


async def probe(args):
    import numpy as np
    manifest = load_manifest(args.manifest, args.manifest_sha256)
    window = gru_feature_window(manifest, args.data_dir, args.runner_root)
    if args.fixture_output:
        Path(args.fixture_output).write_text(json.dumps(window, allow_nan=False) + "\n")
    if args.guard_binary:
        from model_client import ModelGuardClient
        client = ModelGuardClient(args.manifest, args.manifest_sha256, args.guard_binary, args.runner_root)
        ready = await client.start()
        try:
            now = time.time_ns()
            result = await client.predict({**window, "requestId": uuid.uuid4().hex,
                "releaseId": manifest["releaseId"], "modelHash": manifest["model"]["sha256"],
                "normalized": False, "mode": "shadow", "deadlineNs": now + manifest["runtime"]["timeoutMs"] * 1_000_000})
            return {"ready": ready, "result": result, "ordersSent": False}
        finally:
            await client.close()
    if not args.direct_offline:
        raise ValueError("Supply --guard-binary for runtime probe, or explicitly --direct-offline for historical reference only")
    adapter = ModelAdapter(args.manifest, args.manifest_sha256, args.runner_root)
    before = time.perf_counter()
    adapter.warmup()
    warmup = (time.perf_counter() - before) * 1000
    c = manifest["featureContract"]
    normalized = np.clip((np.asarray(window["inputs"]) - c["mean"]) / c["scale"], *c["clip"]).astype("float32")
    request = {**window, "releaseId": manifest["releaseId"], "modelHash": manifest["model"]["sha256"],
        "normalized": True, "mode": "shadow", "inputs": normalized}
    timings, result = [], None
    for _ in range(args.iterations):
        before = time.perf_counter()
        result = adapter.predict_batch([request])[0]
        timings.append((time.perf_counter() - before) * 1000)
    return {"kind": "historical_direct_reference_not_live_readiness", "modelHash": manifest["model"]["sha256"],
        "asOfNs": window["asOfNs"], "prediction": result, "warmupMs": warmup, "iterations": args.iterations,
        "p50Ms": float(np.quantile(timings, .5)), "p95Ms": float(np.quantile(timings, .95)),
        "p99Ms": float(np.quantile(timings, .99)), "ordersSent": False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True)
    p.add_argument("--manifest-sha256", required=True)
    p.add_argument("--runner-root", required=True)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--guard-binary")
    p.add_argument("--direct-offline", action="store_true")
    p.add_argument("--fixture-output")
    p.add_argument("--iterations", type=int, default=100)
    args = p.parse_args()
    if not 1 <= args.iterations <= 10000:
        p.error("iterations must be in [1,10000]")
    print(json.dumps(asyncio.run(probe(args)), allow_nan=False, indent=2))


if __name__ == "__main__":
    main()
