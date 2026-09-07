"""Immutable, locally reviewed model releases. Never loads an uploaded pickle.

The administrator pins the resulting manifest SHA256 in the launch registry.
That digest is required again at runtime; hashes are integrity, not authorship.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import shutil
import sys
from pathlib import Path

GRU_SOURCE_HASH = "a88577d4056864e486aca926402b0cdb4d92fc9945f3bec40256b7f246e0ad47"
GRU_FEATURE_HASH = "186cea5c66fd1b58c7c5862455d245918333f17b3a0b6b98bd66e9afc4bddd92"


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def checked_path(root, record):
    name = record["path"]
    path = (root / name).resolve()
    if Path(name).is_absolute() or not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("Release file escapes its directory or is missing")
    if sha(path) != record["sha256"]:
        raise ValueError(f"Release checksum mismatch: {name}")
    return path


def import_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_manifest(path, expected_hash):
    path = Path(path).resolve()
    if not expected_hash or sha(path) != expected_hash:
        raise ValueError("Manifest differs from administrator-pinned release")
    manifest = read_json(path)
    if manifest["schemaVersion"] != 1 or manifest["family"] not in ("recent_btc_gru", "rdt4quant_multiasset"):
        raise ValueError("Unsupported model release")
    if set(manifest["policy"]["allowedModes"]) - {"shadow", "sandbox", "live"}:
        raise ValueError("Unsupported model execution mode")
    if manifest["family"] == "rdt4quant_multiasset" and manifest["outputContract"].get("quantiles") != [0.1, 0.5, 0.9]:
        raise ValueError("Reviewed RDT heads are fixed q10/q50/q90; other quantile metadata is unsupported")
    runtime = manifest["runtime"]
    for key, lo, hi in (("maxBatchSize", 1, 64), ("maxQueueSize", 1, 256),
                        ("timeoutMs", 1, 300000), ("maxInputAgeMs", 1, 172800000)):
        if type(runtime[key]) is not int or not lo <= runtime[key] <= hi:
            raise ValueError(f"Invalid runtime limit: {key}")
    checked_path(path.parent, manifest["model"])
    for source in manifest["sources"]:
        checked_path(path.parent, source)
    contracts = manifest.get("domainContracts", {"default": manifest.get("featureContract")})
    for contract in contracts.values():
        names, mean, scale = contract["names"], contract["mean"], contract["scale"]
        if not names or len(names) != len(set(names)) or len(names) != len(mean) or len(names) != len(scale):
            raise ValueError("Inconsistent feature contract")
        if any(not math.isfinite(v) for v in mean + scale) or any(v <= 0 for v in scale):
            raise ValueError("Nonfinite/invalid scaler")
        if type(contract["sequenceBars"]) is not int or not 1 <= contract["sequenceBars"] <= 4096:
            raise ValueError("Invalid sequence length")
        if len(names) > 1024 or len(names) * contract["sequenceBars"] > 262144:
            raise ValueError("Feature matrix exceeds the installed Rust guard limits")
    return manifest


def file_record(root, path):
    return {"path": str(path.relative_to(root)), "sha256": sha(path)}


def package_gru(model_dir, source_dir, output, release_id):
    import torch
    model_dir, source_dir, output = map(Path, (model_dir, source_dir, output))
    if output.exists():
        raise FileExistsError("Choose a new immutable release directory")
    artifact = read_json(model_dir / "artifact.json")
    metadata = read_json(model_dir / "metadata.json")
    if sha(model_dir / "model.pt") != artifact["model_sha256"] or sha(model_dir / "metadata.json") != artifact["metadata_sha256"]:
        raise ValueError("Research export hashes differ")
    for filename, expected in (("train_gru.py", GRU_SOURCE_HASH), ("prepare.py", GRU_FEATURE_HASH)):
        if sha(source_dir / filename) != expected:
            raise ValueError(f"Review changed research implementation before release: {filename}")
    checkpoint = torch.load(model_dir / "model.pt", map_location="cpu", weights_only=True)
    if checkpoint["input_size"] != len(metadata["feature_names"]) or checkpoint["sequence_bars"] != metadata["sequence_bars"]:
        raise ValueError("Checkpoint and feature metadata disagree")
    output.mkdir(parents=True)
    (output / "source").mkdir()
    for filename in ("model.pt", "metadata.json", "artifact.json", "training.json"):
        shutil.copy2(model_dir / filename, output / filename)
    for filename in ("train_gru.py", "prepare.py"):
        shutil.copy2(source_dir / filename, output / "source" / filename)
    manifest = dict(schemaVersion=1, releaseId=release_id, runnerId="gru_v1", family="recent_btc_gru",
        modelVersion=model_dir.parent.name + "/" + model_dir.name,
        model=file_record(output, output / "model.pt"),
        sources=[file_record(output, output / "source" / n) for n in ("train_gru.py", "prepare.py")],
        featureContract=dict(names=metadata["feature_names"], sequenceBars=metadata["sequence_bars"],
            barSeconds=900, requiredMarkets=["BTC-USDT", "BTC-USDT-SWAP", "ETH-USDT"],
            mean=checkpoint["feature_mean"].tolist(), scale=checkpoint["feature_scale"].tolist(),
            clip=[-8, 8], rawWarmupBars=128, timestampSemantics="completed_bar_close_utc"),
        outputContract=dict(kind="simple_return", targetScale=checkpoint["target_scale"], horizons=[4], horizonUnit="15m_bars", selectedHorizon=4, selectedHorizonUnit="15m_bars"),
        runtime=dict(device="cpu", maxBatchSize=16, maxQueueSize=32, timeoutMs=2000, maxInputAgeMs=1200000),
        policy=dict(allowedModes=["shadow", "sandbox"], thresholdBps=artifact["threshold_from_research_validation_bps"], maxTargetFraction=1),
        provenance=dict(role=artifact["role"], independentTestMetrics=artifact["independent_test_metrics"],
            evidence="Existing trained artifact; latest refit has no independent holdout score",
            lastTrainingBar=metadata["last_confirmed_bar"], researchArtifact=file_record(output, output / "artifact.json")))
    write_json(output / "manifest.json", manifest)
    load_manifest(output / "manifest.json", sha(output / "manifest.json"))
    return {"manifest": str((output / "manifest.json").resolve()), "sha256": sha(output / "manifest.json"), "modelHash": manifest["model"]["sha256"]}


def package_rdt(checkpoint_path, prepared_manifest, crypto_metadata, source_root, output, release_id,
                source_tree_sha256, threshold_bps, selected_head_by_domain):
    """Package reviewed MultiAssetRDT. Metadata/scalers are mandatory, not inferred.

    source_tree_sha256 is an administrator-reviewed digest over sorted relative
    paths and individual file hashes. It cannot be supplied by a web upload.
    """
    import torch
    source_root, output = Path(source_root), Path(output)
    source_files = rdt_source_files(source_root)
    tree = "\n".join(f"{p.relative_to(source_root)}:{sha(p)}" for p in source_files)
    if hashlib.sha256(tree.encode()).hexdigest() != source_tree_sha256:
        raise ValueError("RDT source tree differs from reviewed digest")
    if output.exists():
        raise FileExistsError("Choose a new immutable release directory")
    c = torch.load(checkpoint_path, map_location="cpu", weights_only=True)
    if c.get("epoch_complete") is not True or c.get("signature", {}).get("phase") != "latest":
        raise ValueError("Expected a completed latest full-pass checkpoint")
    metadata, crypto = read_json(prepared_manifest), read_json(crypto_metadata)
    if c["asset_order"] != metadata["asset_order"]:
        raise ValueError("Asset embedding order differs from prepared metadata")
    config = c["config"]
    heads = read_json(selected_head_by_domain)
    if set(heads) != set(config["horizons"]) or any(type(index) is not int or not 0 <= index < len(config["horizons"][domain]) for domain, index in heads.items()):
        raise ValueError("Explicit zero-based selected head required for every RDT domain")
    contracts = {}
    contracts["crypto:BTC-USDT"] = dict(names=crypto["features"], mean=crypto["mean"], scale=crypto["std"],
        sequenceBars=config["sequence"]["crypto"], barSeconds=60, clip=[-12, 12], assetId=0,
        yScale=crypto["y_scale"], domain="crypto", instrument="BTC-USDT")
    for domain, values in metadata["domains"].items():
        for item in values["assets"]:
            contracts[f"{domain}:{item['asset']}"] = dict(names=item["features"], mean=item["feature_mean"],
                scale=item["feature_scale"], sequenceBars=config["sequence"][domain],
                barSeconds=3600 if domain == "token_hour" else 86400, clip=[-12, 12],
                assetId=item["asset_id"], yScale=item["y_scale"], domain=domain, instrument=item["asset"])
    output.mkdir(parents=True)
    shutil.copy2(checkpoint_path, output / "model.pt")
    for path in source_files:
        target = output / "source" / path.relative_to(source_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    manifest = dict(schemaVersion=1, releaseId=release_id, runnerId="rdt4quant_v1", family="rdt4quant_multiasset",
        modelVersion=f"{config['training_revision']}/latest/seed_{c['signature'].get('seed', 'unknown')}/epoch_{c['epoch']}",
        model=file_record(output, output / "model.pt"),
        sources=[file_record(output, output / "source" / p.relative_to(source_root)) for p in source_files],
        domainContracts=contracts, outputContract=dict(kind="log_return_bps_quantiles", quantiles=[0.1, 0.5, 0.9],
            horizons=config["horizons"], horizonUnit=config["horizon_units"], selectedHeadByDomain=heads, depth=4),
        runtime=dict(device="cuda", maxBatchSize=8, maxQueueSize=16, timeoutMs=30000, maxInputAgeMs=172800000),
        policy=dict(allowedModes=["shadow"], thresholdBps=threshold_bps, maxTargetFraction=1),
        provenance=dict(independentTestMetrics=None, sourceTreeSha256=source_tree_sha256,
            evidence="Latest joint refit. Walk-forward fold performance is not performance of these weights.",
            coverage=c["coverage"], fullEpoch=c["epoch"], assetOrder=c["asset_order"]))
    write_json(output / "manifest.json", manifest)
    load_manifest(output / "manifest.json", sha(output / "manifest.json"))
    return {"manifest": str((output / "manifest.json").resolve()), "sha256": sha(output / "manifest.json")}


def rdt_source_files(root):
    root = Path(root)
    return sorted([root / "rdt4quant/model.py", root / "rdt4quant/prepare.py", root / "rdt4quant/download.py",
        root / "rdt4quant_fullpass/multiasset_model.py", root / "rdt4quant_fullpass/prepare.py"] +
        # Test-only source is not imported by the runner. Including all 300
        # research/test files exceeds the artifact store's 256-entry limit.
        [p for p in (root / "rdt4quant/native").rglob("*.py") if "__pycache__" not in p.parts
            and "tests" not in p.relative_to(root / "rdt4quant/native").parts])


class ModelAdapter:
    def __init__(self, manifest_path, expected_hash, runner_root=None, contract_key=None):
        import numpy as np
        import torch
        self.np, self.torch = np, torch
        self.manifest = load_manifest(manifest_path, expected_hash)
        self.contract_key = contract_key
        root = Path(manifest_path).resolve().parent
        # runner_root is a host-admin deployment option, NEVER a release field.
        # Uploaded/packaged .py files are provenance only and never imported.
        runner_root = Path(runner_root or Path(__file__).resolve().parents[3] / "model_training/pipelines").resolve()
        self.device = self.manifest["runtime"]["device"]
        torch.set_num_threads(self.manifest["runtime"].get("threads", 1))
        c = torch.load(checked_path(root, self.manifest["model"]), map_location="cpu", weights_only=True)
        if self.manifest["family"] == "recent_btc_gru":
            contract = self.manifest["featureContract"]
            if not np.array_equal(c["feature_mean"].numpy(), contract["mean"]) or not np.array_equal(c["feature_scale"].numpy(), contract["scale"]):
                raise ValueError("Released scaler differs from weights")
            if c["target_scale"] != self.manifest["outputContract"]["targetScale"] or c["sequence_bars"] != contract["sequenceBars"]:
                raise ValueError("Released output/sequence differs from weights")
            source = runner_root / "recent_btc/train_gru.py"
            if sha(source) != GRU_SOURCE_HASH:
                raise ValueError("Unreviewed GRU implementation")
            self.model = import_file("montlok_release_gru", source).ReturnGRU(c["input_size"], c["hidden_size"])
            self.model.load_state_dict(c["state_dict"], strict=True)
        else:
            if contract_key not in self.manifest["domainContracts"]:
                raise ValueError("RDT needs an explicit administrator-selected published contract key")
            cpu = self.manifest["runnerId"] == "rdt4quant_cpu_v2"
            if not cpu and (self.device != "cuda" or not torch.cuda.is_available() or not torch.cuda.is_bf16_supported()):
                raise RuntimeError("RDT requires CUDA BF16 and official Mamba3; CPU fallback is prohibited")
            for record in self.manifest["sources"]:
                relative = Path(record["path"]).relative_to("source")
                installed = (runner_root / relative).resolve()
                if not installed.is_relative_to(runner_root) or sha(installed) != record["sha256"]:
                    raise ValueError("Host-installed RDT runner differs from reviewed release provenance")
            base = runner_root / "rdt4quant"
            os.environ["RDT_BASE_CODE"] = str(base)
            # Full source tree was verified before any research module import.
            if cpu:
                if self.device != "cpu":
                    raise ValueError("montlok.cpp requires an explicit CPU release")
                os.environ["MONTLOK_CPP"] = "1"
                sys.path.insert(0, str(runner_root / "rdt4quant_cpu"))
                # Production uses the prebuilt extension; never JIT during a run.
                import montlok_cpp_v2
                self.backend_info = montlok_cpp_v2.build_info()
                self.model = import_file("montlok_release_cpu", runner_root / "rdt4quant_cpu/model_cpu.py").MultiAssetRDTCPU(c["base_config"], len(c["asset_order"]))
            else:
                self.model = import_file("montlok_release_multiasset", runner_root / "rdt4quant_fullpass/multiasset_model.py").MultiAssetRDT(c["base_config"], len(c["asset_order"]))
            self.model.load_state_dict(c["model"], strict=True)
        self.model.to(self.device).eval()

    def contract(self, request):
        if self.manifest["family"] == "recent_btc_gru":
            if request.get("instrument") != "BTC-USDT":
                raise ValueError("Unreleased instrument")
            return self.manifest["featureContract"]
        key = f"{request['domain']}:{request['instrument']}"
        if key != self.contract_key:
            raise ValueError("Request differs from the host-selected RDT contract")
        return self.manifest["domainContracts"][key]

    def validate(self, request):
        m = self.manifest
        if request.get("releaseId") != m["releaseId"] or request.get("modelHash") != m["model"]["sha256"]:
            raise ValueError("Request targets a different release/model hash")
        if request.get("mode") not in m["policy"]["allowedModes"] or request.get("normalized") is not True:
            raise ValueError("Unsupported mode or unnormalized inputs")
        contract = self.contract(request)
        if request.get("featureNames") != contract["names"]:
            raise ValueError("Feature names/order differ from trained contract")
        x = self.np.asarray(request["inputs"], dtype=self.np.float32)
        if x.shape != (contract["sequenceBars"], len(contract["names"])) or not self.np.isfinite(x).all():
            raise ValueError("Invalid feature shape or nonfinite features")
        if x.min() < contract["clip"][0] - 1e-5 or x.max() > contract["clip"][1] + 1e-5:
            raise ValueError("Normalized feature exceeds published clipping bounds")
        return x

    def predict_batch(self, requests):
        np, torch = self.np, self.torch
        arrays = [self.validate(r) for r in requests]
        contract = self.contract(requests[0])
        if any(self.contract(r) != contract for r in requests):
            raise ValueError("Batch crosses feature/scaler/domain contracts")
        with torch.inference_mode():
            x = torch.as_tensor(np.stack(arrays), device=self.device)
            if self.manifest["family"] == "recent_btc_gru":
                result = self.model(x).cpu().numpy()
                if not np.isfinite(result).all():
                    raise ValueError("Nonfinite model output")
                return [{"prediction": float(value) / self.manifest["outputContract"]["targetScale"], "horizon": self.manifest["outputContract"]["selectedHorizon"],
                    "horizonUnit": self.manifest["outputContract"]["selectedHorizonUnit"]} for value in result]
            asset = torch.tensor([contract["assetId"]] * len(requests), device=self.device)
            q, _ = self.model(x, self.manifest["outputContract"]["depth"], contract["domain"], asset)
            q = q.float().cpu().numpy() * np.asarray(contract["yScale"])[None, :, None]
            if not np.isfinite(q).all() or (np.diff(q, axis=2) < 0).any():
                raise ValueError("Nonfinite or unordered model quantiles")
            output = self.manifest["outputContract"]
            index = output["selectedHeadByDomain"][contract["domain"]]
            return [{"prediction": float(np.expm1(row[index, 1] / 10000)), "quantilesBps": row.tolist(),
                "horizon": output["horizons"][contract["domain"]][index],
                "horizonUnit": output["horizonUnit"][contract["domain"]]} for row in q]

    def warmup(self):
        m = self.manifest
        c = m["featureContract"] if m["family"] == "recent_btc_gru" else m["domainContracts"][self.contract_key]
        contracts = [("BTC-USDT", None, c)] if m["family"] == "recent_btc_gru" else [(c["instrument"], c["domain"], c)]
        # Warm each domain shape once, not all asset embeddings.
        seen = set()
        for instrument, domain, c in contracts:
            if domain in seen:
                continue
            seen.add(domain)
            self.predict_batch([dict(releaseId=m["releaseId"], modelHash=m["model"]["sha256"], mode=m["policy"]["allowedModes"][0],
                instrument=instrument, domain=domain, normalized=True, featureNames=c["names"],
                inputs=self.np.zeros((c["sequenceBars"], len(c["names"])), dtype="float32"))])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    gru = sub.add_parser("gru")
    gru.add_argument("--model-dir", required=True)
    gru.add_argument("--source-dir", required=True)
    gru.add_argument("--output", required=True)
    gru.add_argument("--release-id", required=True)
    rdt = sub.add_parser("rdt")
    for name in ("checkpoint", "prepared-manifest", "crypto-metadata", "source-root", "output", "release-id", "source-tree-sha256", "selected-head-by-domain"):
        rdt.add_argument("--" + name, required=True)
    rdt.add_argument("--threshold-bps", type=float, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "rdt":
        args["checkpoint_path"] = args.pop("checkpoint")
    print(json.dumps((package_gru if command == "gru" else package_rdt)(**args)))


if __name__ == "__main__":
    main()
