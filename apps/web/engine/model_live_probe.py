"""One current public-data GRU shadow prediction, no node/order submission."""
import argparse
import asyncio
import json
from pathlib import Path
import time
import uuid

from model_client import ModelGuardClient
from model_market_actor import MARKETS, fetch_completed_candles
from model_probe import gru_window_from_markets
from model_release import ModelAdapter, load_manifest


async def probe(args):
    import numpy as np
    manifest = load_manifest(args.manifest, args.manifest_sha256)
    frames = await asyncio.gather(*[asyncio.to_thread(fetch_completed_candles, name) for name in MARKETS])
    window = await asyncio.to_thread(gru_window_from_markets, manifest, dict(zip(MARKETS, frames)), args.runner_root)
    if time.time_ns() - window["asOfNs"] > manifest["runtime"]["maxInputAgeMs"] * 1_000_000:
        raise ValueError("Public inputs are stale")
    if args.fixture_output:
        Path(args.fixture_output).write_text(json.dumps(window, allow_nan=False) + "\n")
    c = manifest["featureContract"]
    adapter = ModelAdapter(args.manifest, args.manifest_sha256, args.runner_root)
    reference = adapter.predict_batch([{**window, "releaseId": manifest["releaseId"], "modelHash": manifest["model"]["sha256"],
        "normalized": True, "mode": "shadow", "inputs": np.clip((np.asarray(window["inputs"]) - c["mean"]) / c["scale"], *c["clip"]).astype("float32")}])[0]
    result = {"kind": "current_public_closed_bars_shadow_probe", "ordersSent": False,
        "asOfNs": window["asOfNs"], "marketRows": {name: len(frame) for name, frame in zip(MARKETS, frames)},
        "modelHash": manifest["model"]["sha256"], "reference": reference}
    client = ModelGuardClient(args.manifest, args.manifest_sha256, args.guard_binary, args.runner_root)
    try:
        result["ready"] = await client.start()
        now = time.time_ns()
        result["guarded"] = await client.predict({**window, "requestId": uuid.uuid4().hex,
            "releaseId": manifest["releaseId"], "modelHash": manifest["model"]["sha256"], "normalized": False,
            "mode": "shadow", "deadlineNs": now + manifest["runtime"]["timeoutMs"] * 1_000_000})
        result["maxAbsPredictionDifference"] = abs(result["guarded"]["prediction"]["prediction"] - reference["prediction"])
    finally:
        await client.close()
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "manifest-sha256", "runner-root", "guard-binary"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--fixture-output")
    args = p.parse_args()
    print(json.dumps(asyncio.run(probe(args)), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
