#!/usr/bin/env python3
"""Install immutable live workers/configs and merge their specs into the group registry."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time


SOURCE = Path("/tmp/montlok-live-hft-stage")
BASE = Path("/usr/local/lib/montlok-live")
ENGINE = BASE / "engine"
OPERATOR_API = BASE / "operator-api"
CONFIG_ROOT = Path("/etc/montlok-live")
REGISTRY = Path("/etc/montlok-groups/registry.json")


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def spec(group_id: str, name: str, description: str, config_name: str, maximum: int, **extra):
    config_path = CONFIG_ROOT / config_name
    return {
        "id": group_id,
        "kind": "live",
        "name": name,
        "description": description,
        "enabled": True,
        "environment": "live",
        "workerPath": str(ENGINE / "live_hft_worker.py"),
        "workerSha256": digest(ENGINE / "live_hft_worker.py"),
        "strategySha256": digest(ENGINE / "live_hft_worker.py"),
        "pythonPath": "/opt/montlok-model-runtime/bin/python",
        "configPath": str(config_path),
        "configSha256": digest(config_path),
        "profileStatePath": "/www/nautilus/operator/state",
        "profileId": "tokyoreal",
        "operatorServerPath": str(OPERATOR_API),
        "marketTransport": "native_ws",
        "maxDurationSeconds": maximum,
        "dashboardVisible": True,
        **extra,
    }


for root in (BASE, ENGINE, OPERATOR_API, CONFIG_ROOT):
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o755)
for name in ("live_hft_worker.py", "control.py"):
    shutil.copy2(SOURCE / name, ENGINE / name)
for name in ("exchange.py", "profiles.py"):
    shutil.copy2(SOURCE / name, OPERATOR_API / name)
for source_name, target_name in (("live-hft.json", "hft.json"),
                                 ("live-hft-test-1u.json", "hft-test-1u.json"),
                                 ("live-yesterday-alpha-test-1u.json", "yesterday-alpha-test-1u.json")):
    shutil.copy2(SOURCE / source_name, CONFIG_ROOT / target_name)
for path in (*ENGINE.iterdir(), *OPERATOR_API.iterdir(), *CONFIG_ROOT.iterdir()):
    if path.is_file() and not path.is_symlink():
        path.chmod(0o444)

published = [
    spec(
        "live-yesterday-alpha-test-1u",
        "四板块 60/40 · 1 USDT 实盘",
        "2026-09-03 因子信号·最高权重 XRKLB 执行袖·新增净敞口上限 1 USDT",
        "yesterday-alpha-test-1u.json",
        3600,
        capitalMode="incremental_quote_cap",
        exposureCapUsdt="1",
        signalsSha256="9dc471f42a3b9af5ee5ce191d080de45cdc5367dd61342b623f7308f5e28dcff",
        signalAsOf="2026-09-03",
    ),
    spec(
        "live-hft-inventory",
        "BTC + xStock 库存做市",
        "BTC/USDT 使用可用 USDT；xStock/USDT 使用已有库存",
        "hft.json",
        86400,
        capitalMode="account_inventory",
    ),
]

document = json.loads(REGISTRY.read_text())
ids = {row["id"] for row in published} | {"live-hft-test-1u"}
document["groups"] = [row for row in document.get("groups", []) if row.get("id") not in ids] + published
temporary = REGISTRY.with_suffix(".json.new")
temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
temporary.chmod(0o644)
sys.path.insert(0, "/usr/local/lib/montlok-groups/server")
from group_runtime import LaunchRegistry

LaunchRegistry(temporary, strict=True)
backup = REGISTRY.with_name(f"registry.json.before-live-{int(time.time())}")
shutil.copy2(REGISTRY, backup)
os.replace(temporary, REGISTRY)
print(json.dumps({"published": published, "registrySha256": digest(REGISTRY),
                  "backup": str(backup)}, ensure_ascii=False))
