"""Multi-window float-bit comparison: original NumPy normalization vs Rust.

This is an offline numeric audit. Only protocol timestamps are synthesized;
historical feature values are never represented as current market observations.
"""
import argparse
import asyncio
import json
from pathlib import Path
import time

from model_client import JsonProcess
from model_release import load_manifest


async def audit(args):
    import numpy as np
    import pandas as pd
    manifest = load_manifest(args.manifest, args.manifest_sha256)
    c = manifest["featureContract"]
    raw = pd.read_csv(args.feature_csv, index_col="timestamp")[c["names"]].to_numpy()
    if raw.dtype != np.float64:
        raise ValueError("Expected original float64 raw feature table")
    with np.load(args.prepared_npz, allow_pickle=False) as data:
        stored = data["x"]
        mean, scale = data["mean"], data["scale"]
        if mean.dtype != np.float64 or scale.dtype != np.float64 or stored.dtype != np.float32:
            raise ValueError("Original prepared array dtypes do not match float64-to-float32 contract")
        if not np.array_equal(mean, c["mean"]) or not np.array_equal(scale, c["scale"]):
            raise ValueError("Prepared scaler differs from released checkpoint")
    offsets = np.linspace(c["sequenceBars"] - 1, len(raw) - 1, args.windows, dtype=int)
    mismatches = np.zeros(len(c["names"]), dtype="int64")
    stored_mismatches = np.zeros(len(c["names"]), dtype="int64")
    delta = np.zeros(len(c["names"]), dtype="float64")
    wrong_f32_differences = 0
    guard = JsonProcess([str(Path(args.guard_binary).resolve())])
    await guard.start()
    try:
        await guard.exchange({"op": "init", "release": manifest})
        for i, end in enumerate(offsets):
            if i and i % manifest["runtime"]["maxQueueSize"] == 0:
                await guard.close()
                guard = JsonProcess([str(Path(args.guard_binary).resolve())])
                await guard.start()
                await guard.exchange({"op": "init", "release": manifest})
            start = end + 1 - c["sequenceBars"]
            window = raw[start:end+1]
            expected = np.clip((window - mean) / scale, *c["clip"]).astype("float32")
            wrong_f32 = np.clip((window.astype("float32") - mean.astype("float32")) / scale.astype("float32"), *c["clip"]).astype("float32")
            wrong_f32_differences += int(np.sum(expected.view("uint32") != wrong_f32.view("uint32")))
            now = time.time_ns()
            request = {"op": "prepare", "requestId": f"normalization-window-{i}", "releaseId": manifest["releaseId"],
                "modelHash": manifest["model"]["sha256"], "instrument": "BTC-USDT", "mode": "shadow", "normalized": False,
                "featureNames": c["names"], "inputs": window.tolist(), "asOfNs": now - 1_000_000,
                "deadlineNs": now + manifest["runtime"]["timeoutMs"] * 1_000_000}
            result = await guard.exchange(request)
            actual = np.asarray(result["workerRequest"]["inputs"], dtype="float32")
            mismatches += np.sum(expected.view("uint32") != actual.view("uint32"), axis=0)
            stored_mismatches += np.sum(expected.view("uint32") != stored[start:end+1].view("uint32"), axis=0)
            delta = np.maximum(delta, np.max(abs(expected.astype("float64") - actual.astype("float64")), axis=0))
            # No artificial prediction is injected to consume the sample. New
            # bounded offline processes are used if more audit windows remain.
    finally:
        await guard.close()
    result = {"kind": "offline_normalization_bit_audit", "modelHash": manifest["model"]["sha256"],
        "windows": len(offsets), "rowsPerWindow": c["sequenceBars"], "featureCount": len(c["names"]),
        "valuesCompared": len(offsets) * c["sequenceBars"] * len(c["names"]),
        "rawDtype": str(raw.dtype), "meanDtype": str(mean.dtype), "scaleDtype": str(scale.dtype), "networkInputDtype": str(stored.dtype),
        "rustBitMismatches": int(mismatches.sum()), "storedTrainingBitMismatches": int(stored_mismatches.sum()),
        "incorrectFloat32NormalizationBitDifferences": wrong_f32_differences,
        "perFeature": [{"name": name, "rustBitMismatches": int(mismatches[i]),
            "storedTrainingBitMismatches": int(stored_mismatches[i]), "maxAbsDifference": float(delta[i])} for i, name in enumerate(c["names"])],
        "ordersSent": False}
    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "manifest-sha256", "guard-binary", "feature-csv", "prepared-npz"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--windows", type=int, default=16)
    p.add_argument("--output")
    args = p.parse_args()
    if not 2 <= args.windows <= 1000:
        p.error("windows must be 2..1000")
    result = asyncio.run(audit(args))
    print(json.dumps({k: v for k, v in result.items() if k != "perFeature"}, indent=2))
    if result["rustBitMismatches"] or result["storedTrainingBitMismatches"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
