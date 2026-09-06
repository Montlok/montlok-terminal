"""Real CUDA/BF16 RDT weight reload and timed inference on existing NPZ windows.

This reads fixed training/prepared artifacts, never creates orders or a new fit.
Historical inputs are explicitly an offline replay, not current market evidence.
"""
from __future__ import annotations

import argparse
import json
import time

from model_release import ModelAdapter


def probe(args):
    import numpy as np
    import torch
    adapter = ModelAdapter(args.manifest, args.manifest_sha256, args.runner_root, args.contract)
    if adapter.manifest["family"] != "rdt4quant_multiasset":
        raise ValueError("RDT release required")
    c = adapter.manifest["domainContracts"][args.contract]
    with np.load(args.data_npz, allow_pickle=False) as data:
        x = data["x"]
        asset = data["asset_id"] if "asset_id" in data else np.zeros(len(x), dtype="int64")
        eligible = data["all_indices"] if "all_indices" in data else data["test_indices"]
        eligible = eligible[asset[eligible] == c["assetId"]]
        if len(eligible) < args.batch_size:
            raise ValueError("Not enough existing eligible samples for benchmark")
        batch = []
        for index in eligible[-args.batch_size:]:
            start = index + 1 - c["sequenceBars"]
            if start < 0 or not np.all(asset[start:index+1] == c["assetId"]):
                raise ValueError("Prepared window crossed asset boundary")
            batch.append(dict(releaseId=adapter.manifest["releaseId"], modelHash=adapter.manifest["model"]["sha256"],
                mode="shadow", normalized=True, instrument=c["instrument"], domain=c["domain"],
                featureNames=c["names"], inputs=x[start:index+1].copy()))
    torch.cuda.reset_peak_memory_stats()
    before = time.perf_counter()
    adapter.warmup()
    torch.cuda.synchronize()
    warmup_ms = (time.perf_counter() - before) * 1000
    timings, reference, max_delta = [], None, 0.0
    for _ in range(args.iterations):
        torch.cuda.synchronize()
        before = time.perf_counter()
        result = adapter.predict_batch(batch)
        torch.cuda.synchronize()
        timings.append((time.perf_counter() - before) * 1000)
        current = np.asarray([r["quantilesBps"] for r in result])
        if reference is None:
            reference = current
        max_delta = max(max_delta, float(np.max(abs(current - reference))))
    return dict(kind="historical_existing_weights_gpu_probe_no_orders", modelHash=adapter.manifest["model"]["sha256"],
        contract=args.contract, device=torch.cuda.get_device_name(), torch=torch.__version__, cuda=torch.version.cuda,
        bf16=torch.cuda.is_bf16_supported(), parameters=sum(p.numel() for p in adapter.model.parameters()),
        batchSize=len(batch), sequenceBars=c["sequenceBars"], iterations=args.iterations, warmupMs=warmup_ms,
        p50Ms=float(np.quantile(timings, .5)), p95Ms=float(np.quantile(timings, .95)), p99Ms=float(np.quantile(timings, .99)),
        repeatMaxAbsDeltaBps=max_delta, peakAllocatedBytes=torch.cuda.max_memory_allocated(),
        lastPrediction=result[-1], independentTestMetrics=None, ordersSent=False)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "manifest-sha256", "runner-root", "contract", "data-npz"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--batch-size", type=int, default=1)
    p.add_argument("--iterations", type=int, default=20)
    args = p.parse_args()
    if not 1 <= args.batch_size <= 8 or not 1 <= args.iterations <= 1000:
        p.error("batch size must be 1..8 and iterations 1..1000")
    print(json.dumps(probe(args), allow_nan=False, indent=2))


if __name__ == "__main__":
    main()
