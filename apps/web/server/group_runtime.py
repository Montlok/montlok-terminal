"""Bounded strategy-group lifecycle over a private UNIX socket.

The BFF authenticates the operator and confirms the exact request before calling
``execute``. This separate service owns new sandbox processes; it never adopts an
existing paper/live PID. Root-owned launch registrations, not HTTP payloads, select
the executable, frozen signals and reviewed strategy source. A successful start
means a child was launched, not that its market connection is ready.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
import re
import signal
import sqlite3
import stat
import subprocess
import sys
import time
import uuid
from contextlib import closing
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psutil
from model_releases import file_records, released_file, validate_manifest

MAX_MESSAGE = 128 * 1024
MAX_RESPONSE = 2 * 1024 * 1024
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,47}$")
OPERATION_ID = re.compile(r"^[A-Za-z0-9_-]{8,96}$")
MARKET_DATA = {"native_ws": "okx_public_live_websocket_l2", "public_rest_l2": "okx_public_live_rest_l2_snapshots"}
ACTIONS = frozenset({"start", "stop", "halt", "reduce", "resume", "cancel", "flatten"})
ACTIVE = frozenset({"starting", "running", "halted", "reducing", "stopping", "unresponsive", "error", "recovering", "engine_stopped"})


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def object_file(path: Path):
    with path.open("rb") as stream:
        data = stream.read(MAX_MESSAGE + 1)
    if len(data) > MAX_MESSAGE:
        raise ValueError("配置文件过大")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("配置必须为 JSON 对象")
    return value


def money(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("预算必须是 USDT 金额")
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result <= 0 or result > Decimal("1000000000"):
            raise ValueError("预算超出范围")
        if result != result.quantize(Decimal("0.01")):
            raise ValueError("预算最多两位小数")
        return format(result, ".2f")
    except InvalidOperation:
        raise ValueError("预算格式不正确") from None


def duration_policy(spec):
    """Versioned so an old worker is never sent an unlimited-run request."""
    continuous = spec.get("kind") == "live" and spec.get("durationPolicyVersion") == 1
    return {"continuous": continuous and spec.get("maxDurationSeconds") is None,
            "maxSeconds": spec.get("maxDurationSeconds", 86400),
            "defaultSeconds": 0 if continuous and spec.get("maxDurationSeconds") is None else 3600}


from execution_options import contract


def execution_policy(spec):
    from execution_options import settings, describe
    configured=settings(spec)
    if configured:return describe(configured)
    policies = {
        "initial_allocation": ("初始调仓", "按已发布信号配置目标仓位，完成后跟踪持仓与收益。"),
        "inventory_quotes": ("连续报价", "根据行情和库存更新委托。"),
        "model_signal": ("模型信号执行", "根据模型输出更新目标仓位和委托。"),
    }
    kind = spec.get("executionPolicy")
    if kind not in policies:
        return None
    label, description = policies[kind]
    return {"kind": kind, "label": label, "description": description}


def trusted_file(path: Path, strict=True):
    """Reject executable/configuration inputs writable by the service account."""
    if not path.is_absolute() or not path.is_file():
        raise ValueError("运行配置必须引用已部署的绝对文件路径")
    if strict:
        for item in {*path.parents, path, path.resolve(), *path.resolve().parents}:
            info = item.lstat()
            writable = not stat.S_ISLNK(info.st_mode) and info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            if info.st_uid != 0 or writable:
                raise ValueError("运行程序和配置必须由 root 管理且不可被服务账户修改")
    return path


def trusted_directory(path: Path, strict=True):
    if not path.is_absolute() or not path.is_dir():
        raise ValueError("运行目录必须为已部署的绝对目录")
    if strict:
        for item in {*path.parents, path, path.resolve(), *path.resolve().parents}:
            info = item.lstat()
            if info.st_uid != 0 or not stat.S_ISLNK(info.st_mode) and info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                raise ValueError("运行代码目录必须由 root 管理且不可被服务账户修改")
    return path


def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class LaunchRegistry:
    """The only place permitted to select executable code. Loaded once at boot."""

    def __init__(self, path: Path, *, strict=True, capability_probe=None):
        self.path = trusted_file(path, strict)
        self.strict = strict
        self.document = object_file(path)
        self.digest = hashlib.sha256(encode(self.document).encode()).hexdigest()
        if self.document.get("version") != 1:
            raise ValueError("不支持的运行注册版本")
        sandbox_registered = any(spec.get("kind") not in {"live", "model"} for spec in self.document.get("groups", []))
        self.python = self.worker = None
        self.worker_hash = None
        if sandbox_registered:
            self.python = trusted_file(Path(self.document["pythonPath"]), strict)
            self.worker = trusted_file(Path(self.document["workerPath"]), strict)
            self.worker_hash = hashlib.sha256(self.worker.read_bytes()).hexdigest()
        self.groups = {}
        self.model_runtime = None
        self.model_groups = set()
        self.model_program_hashes = {}
        self.model_catalog_signature = None
        self.model_catalog_reason = None
        self.capability_probe = capability_probe or self.probe_model_node
        self.node_capabilities = None
        for spec in self.document.get("groups", []):
            group_id = spec.get("id", "")
            if not IDENTIFIER.fullmatch(group_id) or group_id in self.groups:
                raise ValueError("策略组标识不正确或重复")
            if spec.get("kind") == "live":
                if spec.get("environment") != "live" or spec.get("enabled") is not True:
                    raise ValueError("实盘策略组必须明确启用 live 环境")
                fields = {"workerPath", "workerSha256", "pythonPath", "configPath", "configSha256",
                          "profileStatePath", "profileId", "operatorServerPath"}
                if not fields.issubset(spec):
                    raise ValueError("实盘策略组缺少固定运行路径")
                for key in ("workerPath", "pythonPath", "configPath"):
                    trusted_file(Path(spec[key]), strict)
                for key in ("operatorServerPath",):
                    trusted_directory(Path(spec[key]), strict)
                state = Path(spec["profileStatePath"])
                if not state.is_absolute() or not state.is_dir() or state.is_symlink():
                    raise ValueError("实盘账户状态目录不可用")
                for key, path_key in (("workerSha256", "workerPath"), ("configSha256", "configPath")):
                    if spec.get(key) != file_hash(Path(spec[path_key])):
                        raise ValueError("实盘运行程序或配置 hash 不一致")
                if "signalsPath" in spec:
                    signal_path = trusted_file(Path(spec.get("signalsPath", "")), strict)
                    if file_hash(signal_path) != spec.get("signalsSha256"):
                        raise ValueError("策略信号版本不一致")
                version = spec.get("durationPolicyVersion", 0)
                if type(version) is not int or version not in (0, 1):
                    raise ValueError("运行时长协议版本不正确")
                duration = spec.get("maxDurationSeconds", None if version == 1 else 86400)
                if duration is None:
                    if version != 1:
                        raise ValueError("运行程序尚未发布持续运行能力")
                elif type(duration) is not int or not 1 <= duration <= 9007199254740991:
                    raise ValueError("运行时长上限必须为正整数秒数")
                self.groups[group_id] = {**spec, "marketTransport": "native_ws",
                    "defaultBudgetUsdt": None, "maxBudgetUsdt": None, "maxDurationSeconds": duration}
                continue
            if spec.get("environment") != "sandbox":
                raise ValueError("此运行服务仅支持 Nautilus Sandbox")
            if "verifyOnly" in spec and type(spec["verifyOnly"]) is not bool:
                raise ValueError("verifyOnly 必须为布尔值")
            transport = spec.get("marketTransport", "native_ws")
            if not isinstance(transport, str) or transport not in MARKET_DATA:
                raise ValueError("marketTransport 必须为 native_ws 或 public_rest_l2")
            maximum = money(spec["maxBudgetUsdt"])
            default = money(spec.get("defaultBudgetUsdt", maximum))
            if Decimal(default) > Decimal(maximum):
                raise ValueError("默认预算超过策略组上限")
            duration = spec.get("maxDurationSeconds", 86400)
            if type(duration) is not int or not 60 <= duration <= 86400:
                raise ValueError("运行时长上限必须在 60 至 86400 秒之间")
            self.groups[group_id] = {**spec, "maxBudgetUsdt": maximum,
                                     "defaultBudgetUsdt": default, "maxDurationSeconds": duration,
                                     "marketTransport": transport}
        if self.document.get("modelRuntime") is not None:
            self.configure_models(self.document["modelRuntime"])
            self.refresh_models()

    def configure_models(self, values):
        fields = {"storePath", "workerPath", "pythonPath", "guardBinary", "runnerRoot", "settingsPath",
                  "settingsSha256", "runtimePath", "mode", "maxBudgetUsdt", "maxDurationSeconds"}
        if not isinstance(values, dict) or not fields.issubset(values) or set(values) - fields - {"contractKey", "maxConcurrentRuns", "liveExecution", "runnerRoots"} or values.get("mode") != "shadow":
            raise ValueError("modelRuntime 仅允许固定 root 配置和 shadow 模式")
        config = copy.deepcopy(values)
        slots = config.get("maxConcurrentRuns", 2)
        if type(slots) is not int or not 1 <= slots <= 16:
            raise ValueError("modelRuntime.maxConcurrentRuns 必须为 1 至 16 的整数")
        config["maxConcurrentRuns"] = slots
        contracts = config.get("contractKey")
        if contracts is not None:
            if isinstance(contracts, str):
                contracts = {"rdt4quant_v1": contracts}
            if (not isinstance(contracts, dict) or set(contracts) - {"gru_v1", "rdt4quant_v1", "rdt4quant_cpu_v2"}
                    or any(not isinstance(v, str) or len(v) > 128 or ":" not in v for v in contracts.values())):
                raise ValueError("modelRuntime.contractKey 必须按 runnerId 明确选择 domain/品种")
            if contracts.get("gru_v1", "crypto:BTC-USDT") != "crypto:BTC-USDT":
                raise ValueError("GRU 固定选择 BTC-USDT")
            config["contractKey"] = contracts
        for root in config.get("runnerRoots", {}).values():
            trusted_directory(Path(root), self.strict)
        if config.get("liveExecution"):
            live = config["liveExecution"]
            for key in ("workerPath", "configPath"):
                trusted_file(Path(live[key]), self.strict)
            trusted_directory(Path(live["operatorServerPath"]), self.strict)
            for checksum, filename in (("workerSha256", "workerPath"), ("configSha256", "configPath")):
                if live[checksum] != file_hash(Path(live[filename])):
                    raise ValueError("模型实盘执行版本不一致")
        for key in ("workerPath", "pythonPath", "guardBinary", "settingsPath"):
            path = trusted_file(Path(config[key]), self.strict)
            self.model_program_hashes[str(path)] = file_hash(path)
        for key in ("runnerRoot", "runtimePath"):
            trusted_directory(Path(config[key]), self.strict)
        store = Path(config["storePath"])
        if not store.is_absolute() or store.is_symlink() or any(p.is_symlink() for p in store.parents):
            raise ValueError("模型发布库必须为固定非链接绝对路径")
        if not isinstance(config["settingsSha256"], str) or not re.fullmatch(r"[a-f0-9]{64}", config["settingsSha256"]):
            raise ValueError("模型 root settings 缺少有效 hash")
        config["maxBudgetUsdt"] = money(config["maxBudgetUsdt"])
        if type(config["maxDurationSeconds"]) is not int or not 60 <= config["maxDurationSeconds"] <= 86400:
            raise ValueError("模型运行时长上限必须在 60 至 86400 秒之间")
        # Every imported runtime module is host-installed. The release tree is
        # only data and is never placed on sys.path or used as a working dir.
        worker_root = Path(config["workerPath"]).parent
        for name in ("model_release.py", "model_client.py", "model_inference_worker.py", "model_market_actor.py",
                     "model_strategy.py", "model_probe.py", "commands.py", "control.py"):
            path = trusted_file(worker_root / name, self.strict)
            self.model_program_hashes[str(path)] = file_hash(path)
        for name in ("settings.py", "node_config.py", "health.py", "alerts.py"):
            path = trusted_file(Path(config["runtimePath"]) / name, self.strict)
            self.model_program_hashes[str(path)] = file_hash(path)
        self.model_runtime = config
        self.node_capabilities = self.capability_probe(config)
        self.node_probe_at = time.monotonic()

    @staticmethod
    def probe_model_node(config):
        script = """import importlib.util,json
result={"torch":False,"numpy":False,"nautilus":False,"nativeBridge":False,"dataAbi":False,"cuda":False,"cudaBf16":False,"mamba3":False}
try:
 import torch
 import numpy
 import nautilus_trader
 from nautilus_trader.core import nautilus_pyo3
 from nautilus_trader.live.node import TradingNode
 from nautilus_trader.live.data_engine import LiveDataEngine
 from nautilus_trader.adapters.okx.data import OKXDataClient
 result.update(torch=True,numpy=True,nautilus=True,cuda=bool(torch.cuda.is_available()))
 result["nativeBridge"]=(hasattr(nautilus_pyo3,"PythonDataIngress") and hasattr(nautilus_pyo3.PythonDataIngress,"validate_context") and hasattr(TradingNode,"enable_python_data_ingress") and hasattr(LiveDataEngine,"bind_python_data_ingress") and hasattr(OKXDataClient,"set_python_data_ingress"))
 from nautilus_trader.model.data import capsule_to_data, QuoteTick
 quote=nautilus_pyo3.QuoteTick(nautilus_pyo3.InstrumentId.from_str("BTC-USDT.OKX"),nautilus_pyo3.Price.from_str("80000.1"),nautilus_pyo3.Price.from_str("80000.2"),nautilus_pyo3.Quantity.from_str("0.125"),nautilus_pyo3.Quantity.from_str("0.250"),1,2)
 decoded=capsule_to_data(quote.as_pycapsule())
 result["dataAbi"]=(type(decoded) is QuoteTick and str(decoded.instrument_id)=="BTC-USDT.OKX" and str(decoded.bid_price)=="80000.1" and str(decoded.ask_price)=="80000.2" and str(decoded.bid_size)=="0.125" and str(decoded.ask_size)=="0.250" and decoded.ts_event==1 and decoded.ts_init==2)
 result["cudaBf16"]=bool(result["cuda"] and torch.cuda.is_bf16_supported())
 if result["cudaBf16"]:
  try:
   from mamba_ssm.modules.mamba3 import Mamba3
   result["mamba3"]=True
  except Exception:
   pass
except Exception:
 pass
print(json.dumps(result))
"""
        try:
            result = subprocess.run([config["pythonPath"], "-I", "-c", script], capture_output=True,
                text=True, timeout=12, check=False, env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
                    "PYTHONDONTWRITEBYTECODE": "1", "OKX_API_KEY": "", "OKX_API_SECRET": "", "OKX_API_PASSPHRASE": ""})
            value = json.loads(result.stdout.strip().splitlines()[-1])
            if result.returncode != 0 or not isinstance(value, dict):
                raise ValueError("probe failed")
            return {key: value.get(key) is True for key in ("torch", "numpy", "nautilus", "nativeBridge", "dataAbi", "cuda", "cudaBf16", "mamba3")}
        except (OSError, ValueError, IndexError, subprocess.TimeoutExpired):
            return {"torch": False, "numpy": False, "nautilus": False, "nativeBridge": False, "dataAbi": False, "cuda": False, "cudaBf16": False, "mamba3": False}

    def refresh_models(self):
        if self.model_runtime is None:
            return
        catalog = Path(self.model_runtime["storePath"]) / "releases.sqlite"
        if catalog.is_symlink() or not catalog.is_file():
            self.model_catalog_reason = "模型发布库尚未配置或不可读取"
            return
        def stamp(path):
            try:
                info = path.stat()
                return info.st_mtime_ns, info.st_size, info.st_ino
            except FileNotFoundError:
                return None
        signature = (stamp(catalog), stamp(Path(str(catalog) + "-wal")))
        if signature == self.model_catalog_signature:
            return
        discovered = {}
        try:
            with closing(sqlite3.connect(catalog.as_uri() + "?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                rows = db.execute("SELECT r.*,a.release_id AS active_id FROM releases r LEFT JOIN active a ON a.runner=r.runner ORDER BY r.created LIMIT 256").fetchall()
            for row in rows:
                checksum = row["manifest_hash"]
                if not isinstance(checksum, str) or not re.fullmatch(r"[a-f0-9]{64}", checksum):
                    raise ValueError("模型发布记录 hash 无效")
                manifest = validate_manifest(json.loads(row["manifest"]))
                if row["id"] != manifest["releaseId"] or row["runner"] != manifest["runnerId"] or row["model_version"] != manifest["modelVersion"]:
                    raise ValueError("模型发布索引与清单不一致")
                group_id = "model-" + checksum[:16]
                if group_id in discovered or group_id in self.groups and group_id not in self.model_groups:
                    raise ValueError("模型派生策略组 ID 冲突")
                config = self.model_runtime
                contract_key = "crypto:BTC-USDT" if manifest["runnerId"] == "gru_v1" else (config.get("contractKey") or {}).get(manifest["runnerId"])
                domain, instrument = contract_key.split(":", 1) if contract_key else (None, None)
                discovered[group_id] = {**config, "id": group_id, "kind": "model", "name": f"{manifest['runnerId']} · {manifest['modelVersion']}",
                    "enabled": row["id"] == row["active_id"], "reason": "该版本不是当前发布版本；已有运行不受影响",
                    "environment": "sandbox", "mode": "shadow", "modelMode": "shadow", "marketTransport": "native_ws",
                    "defaultBudgetUsdt": config["maxBudgetUsdt"], "runnerId": manifest["runnerId"], "releaseId": row["id"],
                    "manifestSha256": checksum, "modelHash": manifest["model"]["sha256"], "modelManifest": manifest,
                    "contractKey": contract_key, "domain": domain, "modelInstrument": instrument,
                    "executionInstrumentId": "BTC-USDT.OKX" if contract_key == "crypto:BTC-USDT" else "XNVDA-USDT.OKX" if contract_key == "token_hour:XNVDA" else None,
                    "releaseManifest": str(Path(config["storePath"]) / "releases" / checksum / "manifest.json")}
                if (config.get("liveExecution") and "live" in manifest["policy"]["allowedModes"]
                        and manifest["runnerId"] == "rdt4quant_cpu_v2"):
                    discovered[group_id].update(config["liveExecution"])
                    discovered[group_id].update(kind="live", environment="live", mode="live", modelMode="live",
                        runnerRoot=config.get("runnerRoots", {}).get(manifest["runnerId"], config["runnerRoot"]),
                        defaultBudgetUsdt=None, maxBudgetUsdt=None,
                        name=f"RDT4quant · {manifest['modelVersion']}")
        except (sqlite3.Error, ValueError, KeyError, TypeError):
            self.model_catalog_reason = "模型发布索引校验失败"
            return
        for group_id in self.model_groups:
            self.groups.pop(group_id, None)
        self.groups.update(discovered)
        self.model_groups = set(discovered)
        self.model_catalog_signature = signature
        self.model_catalog_reason = None

    def model_reason(self, spec):
        if self.model_catalog_reason:
            return self.model_catalog_reason
        if not spec["enabled"]:
            return spec["reason"]
        if spec["runnerId"] == "rdt4quant_v1" and (spec["contractKey"] not in {"crypto:BTC-USDT", "token_hour:XNVDA"}
                or spec["contractKey"] not in spec["modelManifest"]["domainContracts"]):
            return "root 配置未明确选择已接通的 RDT contract；须选择已发布 domain/品种"
        if not os.access(spec["guardBinary"], os.X_OK) or not os.access(spec["pythonPath"], os.X_OK):
            return "目标节点 guard 或 Python 不可执行"
        sources = ([Path(spec["runnerRoot"]) / "recent_btc" / name for name in ("train_gru.py", "prepare.py")]
                   if spec["runnerId"] == "gru_v1" else
                   [Path(spec["runnerRoot"]) / row["path"][7:] for row in spec["modelManifest"]["sources"] if row["path"].startswith("source/")])
        if not sources or any(not path.is_file() for path in sources):
            return "目标节点缺少已审核的预装模型 runner"
        node = self.node_capabilities or {}
        if not all(node.get(name) is True for name in ("torch", "numpy", "nautilus")):
            return "节点依赖检查未通过（PyTorch / NumPy / Nautilus），请更新运行环境"
        if node.get("nativeBridge") is not True:
            return "节点尚未安装原生 Python 数据桥，请更新运行环境"
        if node.get("dataAbi") is not True:
            return "行情数据兼容检查未通过，请更新节点运行环境"
        if spec["modelManifest"]["runtime"]["device"] == "cuda" and not (node.get("cuda") and node.get("cudaBf16")):
            return "此模型需要 CUDA BF16，当前节点未检测到兼容 GPU"
        if spec["runnerId"] == "rdt4quant_v1" and node.get("mamba3") is not True:
            return "目标节点缺少已安装的官方 Mamba3 运行依赖"
        if "shadow" not in spec["modelManifest"]["policy"]["allowedModes"]:
            return "该模型清单未允许 shadow 运行"
        return None

    def command(self, group_id, request_path):
        spec = self.spec(group_id) if isinstance(group_id, str) else group_id
        if spec.get("kind") == "live":
            return [spec["pythonPath"], spec["workerPath"], "--registry", str(self.path),
                    "--group-id", spec["id"], "--request", str(request_path)]
        if spec.get("kind") == "model":
            return [spec["pythonPath"], spec["workerPath"], "--registry", str(self.path), "--request", str(request_path),
                    "--release-manifest", spec["releaseManifest"], "--manifest-sha256", spec["manifestSha256"],
                    "--guard-binary", spec["guardBinary"], "--runner-root", spec["runnerRoot"]]
        if self.python is None or self.worker is None:
            raise ValueError("生产运行服务未配置模拟运行器")
        return [str(self.python), str(self.worker), "--registry", str(self.path), "--request", str(request_path)]

    def spec(self, group_id):
        self.refresh_models()
        if group_id not in self.groups:
            raise ValueError("策略组尚未发布运行配置")
        return copy.deepcopy(self.groups[group_id])

    def validate_inputs(self, group_id):
        spec = self.spec(group_id)
        if spec.get("kind") == "live":
            if spec.get("enabled") is not True:
                raise ValueError(spec.get("reason") or "该模型版本未激活")
            if hashlib.sha256(encode(object_file(self.path)).encode()).hexdigest() != self.digest:
                raise ValueError("运行注册已变化，请重新加载运行服务")
            for checksum, filename in (("workerSha256", "workerPath"), ("configSha256", "configPath")):
                path = trusted_file(Path(spec[filename]), self.strict)
                if file_hash(path) != spec[checksum]:
                    raise ValueError("实盘运行程序或配置已变化")
            if spec.get("signalsPath") and file_hash(trusted_file(Path(spec["signalsPath"]), self.strict)) != spec.get("signalsSha256"):
                raise ValueError("策略信号版本已变化")
            if spec.get("modelHash"):
                for filename, checksum in self.model_program_hashes.items():
                    if file_hash(trusted_file(Path(filename), self.strict)) != checksum:
                        raise ValueError("模型运行程序已变化，请重新加载运行服务")
                if file_hash(released_file(Path(spec["releaseManifest"]).parent, "manifest.json")) != spec["manifestSha256"]:
                    raise ValueError("模型发布文件已变化")
            return spec
        if spec.get("kind") == "model":
            return self.validate_model_inputs(spec)
        if spec.get("enabled") is not True:
            raise ValueError(spec.get("reason") or "策略组运行配置未启用")
        if self.worker is None or hashlib.sha256(self.worker.read_bytes()).hexdigest() != self.worker_hash:
            raise ValueError("运行程序已变化，请重新加载运行服务")
        if hashlib.sha256(encode(object_file(self.path)).encode()).hexdigest() != self.digest:
            raise ValueError("运行注册已变化，请重新加载运行服务")
        for label in ("settings", "signals", "strategy"):
            path = trusted_file(Path(spec[f"{label}Path"]), self.strict)
            if hashlib.sha256(path.read_bytes()).hexdigest() != spec[f"{label}Sha256"]:
                raise ValueError(f"{label} 内容与发布版本不一致")
        runtime = Path(spec["runtimePath"])
        for name in ("settings.py", "run.py", "node_config.py", "health.py", "commands.py", "control.py", "alerts.py"):
            trusted_file(runtime / name, self.strict)
        settings = object_file(Path(spec["settingsPath"]))
        if (settings.get("environment") != "sandbox" or settings.get("venue", "OKX") != "OKX"
                or settings.get("instrument_types", ["SPOT"]) != ["SPOT"] or settings.get("strategies")):
            raise ValueError("运行配置必须为固定现货 Sandbox，不加载清单外策略")
        signals = object_file(Path(spec["signalsPath"]))
        if (signals.get("execution") != "nautilus_sandbox" or signals.get("account_type") != "CASH"
                or signals.get("leverage") != 1 or signals.get("instrument_type", "SPOT") != "SPOT"):
            raise ValueError("信号仅允许现货 CASH 1x 本地模拟")
        instruments = signals.get("instruments", {})
        if not isinstance(instruments, dict) or not 1 <= len(instruments) <= 100:
            raise ValueError("信号品种数量不正确")
        weights = []
        for instrument, row in instruments.items():
            if not re.fullmatch(r"[A-Z0-9]+-USDT\.OKX", instrument) or not isinstance(row, dict):
                raise ValueError("信号必须为 OKX USDT 现货")
            weight = row.get("weight")
            if isinstance(weight, bool) or not isinstance(weight, (float, int)) or not 0 <= weight <= 0.10:
                raise ValueError("单品种信号权重必须介于 0 和 10%")
            weights.append(Decimal(str(weight)))
        if sum(weights) > 1 or set(settings.get("instruments", [])) != set(instruments):
            raise ValueError("品种集合或总权重与运行清单不一致")
        caps = settings.get("risk", {}).get("max_notional_per_order", {})
        if any(type(caps.get(instrument)) is not int or caps[instrument] <= 0 for instrument in instruments):
            raise ValueError("每个品种需要已发布的正整数单笔名义上限")
        return spec

    def live_preview(self, spec, execution_settings=None):
        command = [spec["pythonPath"], spec["workerPath"], "--registry", str(self.path),
                   "--group-id", spec["id"], "--preview"]
        if execution_settings is not None:
            command.extend(['--execution-settings',encode(execution_settings)])
        result = subprocess.run(command, capture_output=True, text=True, timeout=45, check=False,
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "TZ": "UTC",
                 "PYTHONDONTWRITEBYTECODE": "1", "OKX_API_KEY": "", "OKX_API_SECRET": "",
                 "OKX_API_PASSPHRASE": ""})
        try:
            value = json.loads(result.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise ValueError("实盘账户检查未返回有效结果") from None
        if result.returncode != 0 or not isinstance(value, dict) or not re.fullmatch(r"[a-f0-9]{64}", value.get("inventoryHash", "")):
            raise ValueError(value.get("error") if isinstance(value, dict) else "实盘账户检查失败")
        return value

    def validate_model_inputs(self, spec):
        if time.monotonic() - self.node_probe_at > 30:
            self.node_capabilities = self.capability_probe(self.model_runtime)
            self.node_probe_at = time.monotonic()
        reason = self.model_reason(spec)
        if reason:
            raise ValueError(reason)
        if hashlib.sha256(encode(object_file(self.path)).encode()).hexdigest() != self.digest:
            raise ValueError("运行注册已变化，请重新加载运行服务")
        for filename, checksum in self.model_program_hashes.items():
            path = trusted_file(Path(filename), self.strict)
            if file_hash(path) != checksum:
                raise ValueError("模型运行程序已变化，请重新加载运行服务")
        settings_path = trusted_file(Path(spec["settingsPath"]), self.strict)
        if file_hash(settings_path) != spec["settingsSha256"]:
            raise ValueError("模型 settings 内容与 root 发布配置不一致")
        settings = object_file(settings_path)
        instrument_id = spec["executionInstrumentId"]
        if (settings.get("environment") != "sandbox" or settings.get("venue", "OKX") != "OKX"
                or settings.get("instrument_types", ["SPOT"]) != ["SPOT"] or settings.get("strategies")
                or type(settings.get("risk", {}).get("max_notional_per_order", {}).get(instrument_id)) is not int
                or settings["risk"]["max_notional_per_order"][instrument_id] <= 0):
            raise ValueError("模型需要已限制目标品种单笔金额的固定 SPOT sandbox settings")
        manifest_path = released_file(Path(spec["releaseManifest"]).parent, "manifest.json")
        if manifest_path.stat().st_size > 4 * 1024 * 1024 or file_hash(manifest_path) != spec["manifestSha256"]:
            raise ValueError("模型不可变 manifest hash 不匹配")
        manifest = validate_manifest(json.loads(manifest_path.read_bytes()))
        if manifest != spec["modelManifest"] or manifest["model"]["sha256"] != spec["modelHash"]:
            raise ValueError("模型发布索引与文件不一致")
        for record in file_records(manifest):
            path = released_file(manifest_path.parent, record["path"])
            if not path.is_file() or file_hash(path) != record["sha256"]:
                raise ValueError("模型发布文件 hash 不匹配")
        runner_root = trusted_directory(Path(spec["runnerRoot"]), self.strict)
        if manifest["runnerId"] == "gru_v1":
            mappings = {"source/train_gru.py": "recent_btc/train_gru.py", "source/prepare.py": "recent_btc/prepare.py"}
            source_records = {record["path"]: record for record in manifest["sources"]}
            if not set(mappings).issubset(source_records):
                raise ValueError("GRU 缺少受审核的训练和特征源码 hash")
            for source, installed in mappings.items():
                host_path = trusted_file(runner_root / installed, self.strict)
                if file_hash(host_path) != source_records[source]["sha256"]:
                    raise ValueError("节点预装 GRU runner 与发布来源不一致")
        else:
            if not manifest["sources"]:
                raise ValueError("RDT 缺少受审核的 runner 源码 hash")
            for record in manifest["sources"]:
                if not record["path"].startswith("source/"):
                    raise ValueError("RDT 来源路径必须位于 source/")
                host_path = trusted_file(runner_root / record["path"][7:], self.strict)
                if file_hash(host_path) != record["sha256"]:
                    raise ValueError("节点预装 RDT runner 与发布来源不一致")
        return spec


async def socket_call(path: Path, request: dict, timeout=5.0):
    encoded = (encode(request) + "\n").encode()
    if len(encoded) > MAX_MESSAGE:
        raise ValueError("请求过大")
    writer = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(path), limit=MAX_RESPONSE), timeout)
        writer.write(encoded)
        await asyncio.wait_for(writer.drain(), timeout)
        raw = await asyncio.wait_for(reader.readline(), timeout)
        if len(raw) > MAX_RESPONSE:
            raise ValueError("运行状态响应过大")
        if not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE:
            raise ValueError("运行服务响应不完整")
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError("运行服务响应格式不正确")
        return result
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass


class GroupRuntimeClient:
    def __init__(self, socket_path: Path | None, timeout=5.0):
        self.path = socket_path
        self.timeout = timeout
        self.status_cache = {}
        self.status_pending = {}
        self.status_epoch = 0

    def invalidate_status(self):
        self.status_cache.clear()
        self.status_pending.clear()
        self.status_epoch += 1

    async def request(self, command, **fields):
        if self.path is None:
            raise ValueError("策略组运行服务尚未配置")
        try:
            timeout = max(self.timeout, 60.0) if command in {"prepare", "execute"} else self.timeout
            result = await socket_call(self.path, {"command": command, **fields}, timeout)
        except (OSError, TimeoutError, ValueError):
            if command == "execute":
                raise TimeoutError("运行操作结果未知，请核对实例状态") from None
            raise ValueError("策略组运行服务未连接") from None
        if not result.get("ok"):
            raise ValueError(result.get("error") or "策略组运行请求未完成")
        return result["result"]

    async def status(self, group_id=None):
        from execution_options import contract
        epoch=self.status_epoch
        cached=self.status_cache.get(group_id)
        if cached and time.monotonic()-cached[0]<.5:
            return copy.deepcopy(cached[1])
        task=self.status_pending.get(group_id)
        if task is None:
            task=asyncio.create_task(self.request('status',**({'groupId':group_id} if group_id else {})))
            self.status_pending[group_id]=task
        try:
            result=await asyncio.shield(task)
            if epoch==self.status_epoch:self.status_cache[group_id]=(time.monotonic(),copy.deepcopy(result))
            return copy.deepcopy(result)
        finally:
            if task.done() and self.status_pending.get(group_id) is task:
                self.status_pending.pop(group_id,None)

    async def prepare(self, request):
        return await self.request("prepare", request=request)

    async def execute(self, request, operation_id):
        self.invalidate_status()
        try:
            return await self.request("execute", request=request, operationId=operation_id)
        finally:
            self.invalidate_status()

    async def receipt(self, operation_id):
        return await self.request("receipt", operationId=operation_id)


class GroupSupervisor:
    def __init__(self, registry: LaunchRegistry, state_dir: Path, *, process_factory=asyncio.create_subprocess_exec):
        self.registry = registry
        self.path = state_dir
        self.path.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path / "runs.sqlite")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA busy_timeout=15000")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY, group_id TEXT NOT NULL, state TEXT NOT NULL,
                budget TEXT NOT NULL, duration INTEGER NOT NULL, created REAL NOT NULL,
                pid INTEGER, process_created REAL, result TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS operations (
                id TEXT PRIMARY KEY, request TEXT NOT NULL, result TEXT);
        """)
        # A crash between intent and completion is not proof of failure. The
        # durable operation ID is queried at the worker; never sent as a retry.
        with self.db:
            self.db.execute("UPDATE operations SET result=? WHERE result IS NULL",
                            (encode({"status": "unknown", "receiptStatus": "unknown"}),))
        self.lock = asyncio.Lock()
        self.process_factory = process_factory
        self.processes = {}
        self.tasks = set()
        self.server = None

    async def reload_registry(self):
        """Replace reviewed launch metadata without restarting the supervisor process."""
        async with self.lock:
            active = []
            for group_id in self.registry.groups:
                for row in self.rows(group_id):
                    if row["state"] in ACTIVE and self.owned_process(row) is not None:
                        active.append(row["id"])
            if active:
                raise ValueError(f"存在正在运行的策略实例：{','.join(active)}")

            candidate = await asyncio.to_thread(
                LaunchRegistry,
                self.registry.path,
                strict=self.registry.strict,
                capability_probe=self.registry.capability_probe,
            )
            previous = self.registry.digest
            self.registry = candidate
            return {"previousRegistryVersion": previous, "registryVersion": candidate.digest,
                    "groups": list(candidate.groups)}

    def owned_process(self, row):
        if not row["pid"] or not row["process_created"]:
            return None
        try:
            process = psutil.Process(row["pid"])
            if (process.create_time() != row["process_created"] or process.status() == psutil.STATUS_ZOMBIE
                    or process.uids().real != os.getuid()):
                return None
            expected = self.registry.command(row["group_id"], self.path / row["id"] / "request.json")
            if process.cmdline() != expected:
                return None
            return process
        except (psutil.Error, OSError, ValueError):
            return None

    def rows(self, group_id):
        return self.db.execute("SELECT * FROM runs WHERE group_id=? ORDER BY created DESC LIMIT 20", (group_id,)).fetchall()

    def row(self, run_id, group_id):
        row = self.db.execute("SELECT * FROM runs WHERE id=? AND group_id=?", (run_id, group_id)).fetchone()
        if row is None:
            raise ValueError("运行实例不属于该策略组")
        return row

    def update(self, run_id, state, result=None):
        with self.db:
            self.db.execute("UPDATE runs SET state=?, result=COALESCE(?,result) WHERE id=?",
                            (state, encode(result) if result is not None else None, run_id))

    async def run_status(self, row):
        state = row["state"]
        result = json.loads(row["result"])
        spec = self.registry.spec(row["group_id"])
        expected_environment = spec.get("environment", "sandbox")
        if state in ACTIVE:
            process = self.owned_process(row)
            spawning = state == "starting" and row["pid"] is None and time.time() - row["created"] < 30
            if process is None and not spawning:
                state = "interrupted"
                self.update(row["id"], state)
            elif process is not None and state != "stopping":
                try:
                    engine = await socket_call(self.path / row["id"] / "control.sock", {"command": "status"}, 1.0)
                    if (engine.get("ok") and engine.get("pid") == row["pid"]
                            and engine.get("environment") == expected_environment):
                        connected = engine.get("connected", {})
                        state = ({"STARTING": "starting", "ACTIVE": "running", "HALTED": "halted", "REDUCING": "reducing",
                                  "ERROR": "error", "FAULTED": "error", "FAILED": "error",
                                  "STOPPED": "engine_stopped", "RECOVERING": "recovering"}
                                 .get(engine.get("tradingState"), "unresponsive"))
                        if state == "running" and spec.get("kind") == "live" and not (
                            engine.get("marketReady") is True and engine.get("executionReady") is True
                        ):
                            state = "starting" if row["state"] == "starting" else "recovering"
                        elif state == "running" and spec.get("kind") != "live" and not all(
                            connected.get(name) is True for name in ("data", "exec")
                        ):
                            state = "starting"
                        if result.get("modelHash"):
                            if engine.get("modelHash") != result["modelHash"] or engine.get("manifestSha256") != result["manifestSha256"]:
                                state = "unresponsive"
                            elif state in {"running", "halted", "reducing"} and engine.get("warmupComplete") is not True:
                                state = "starting"
                            elif state == "running" and engine.get("marketReady") is not True:
                                state = "starting" if row["state"] == "starting" else "recovering"
                        result = {**result, "engine": engine,
                                  "errorObserved": result.get("errorObserved") is True or state == "error"}
                    else:
                        state = "unresponsive"
                except (OSError, TimeoutError, ValueError):
                    if result.get("modelHash") and row["duration"] > 0 and time.time() >= row["created"] + row["duration"]:
                        state = "stopping"
                        result["automaticShutdown"] = True
                    else:
                        state = "starting" if row["state"] == "starting" and time.time() - row["created"] < 90 else "unresponsive"
                self.update(row["id"], state, result)
        return {"runId": row["id"], "groupId": row["group_id"],
                "mode": "live" if spec.get("kind") == "live" else "nautilus_sandbox", "environment": expected_environment,
                "runDir": str(self.path / row["id"]), "ready": state in {"running", "halted", "reducing"},
                "status": state, "budgetUsdt": None if spec.get("kind") == "live" else row["budget"], "durationSeconds": row["duration"],
                "startedAt": row["created"], "pid": row["pid"], **result}

    def model_capacity(self):
        if self.registry.model_runtime is None:
            return None
        used = 0
        for row in self.db.execute("SELECT * FROM runs"):
            if row["state"] not in ACTIVE or not json.loads(row["result"]).get("modelHash"):
                continue
            spawning = row["state"] == "starting" and row["pid"] is None and time.time() - row["created"] < 30
            if spawning or self.owned_process(row) is not None:
                used += 1
        limit = self.registry.model_runtime["maxConcurrentRuns"]
        return {"used": used, "limit": limit, "available": max(0, limit - used)}

    def require_model_capacity(self):
        capacity = self.model_capacity()
        if capacity and not capacity["available"]:
            raise ValueError(f"模型运行槽位已满（{capacity['used']}/{capacity['limit']}），请先停止一个模型实例")

    async def status(self, group_id=None):
        self.registry.refresh_models()
        model_capacity = self.model_capacity()
        groups = [group_id] if group_id else list(self.registry.groups)
        output = []
        for name in groups:
            spec = self.registry.spec(name)
            reason = None
            try:
                if spec.get("kind") == "model":
                    reason = self.registry.model_reason(spec)
                else:
                    self.registry.validate_inputs(name)
            except (KeyError, ValueError, OSError) as error:
                reason = str(error)
            runs = [await self.run_status(row) for row in self.rows(name)]
            active = next((run for run in runs if run["status"] in ACTIVE), None)
            control_ready = active and active["status"] in {"running", "halted", "reducing"}
            capabilities = {"start": reason is None and active is None, "stop": active is not None,
                            "halt": bool(control_ready and active["status"] != "halted"),
                            "reduce": bool(control_ready and active["status"] != "reducing"),
                            "resume": bool(control_ready and active["status"] in {"halted", "reducing"}),
                            "cancel": False, "flatten": False,
                            "reason": reason or ("该策略组已有运行实例" if active else None)}
            if spec.get("kind") == "live":
                engine = (active or {}).get('engine') or {}
                native_control = engine.get('controlVersion') == 1
                capabilities.update(cancel=bool(control_ready and native_control),
                    flatten=bool(control_ready and native_control and (engine.get('flatten') or {}).get('phase') not in {'cancelling','reducing'}))
                for action in ('halt','reduce','resume'):
                    capabilities[action] = bool(capabilities[action] and native_control)
                if engine.get('resumeAllowed') is False:
                    capabilities['resume'] = False
                    capabilities['resumeReason'] = '库存已由平仓操作更新，请停止后重新启动策略'
            if spec.get("kind") == "model" and model_capacity and not model_capacity["available"]:
                capabilities["start"] = False
                capabilities["reason"] = capabilities["reason"] or f"模型运行槽位已满（{model_capacity['used']}/{model_capacity['limit']}）"
            actions = [action for action in ("start", "halt", "reduce", "resume", "cancel", "flatten", "stop") if capabilities[action]]
            summary_keys = ("runId", "groupId", "mode", "environment", "ready", "status", "durationSeconds",
                            "startedAt", "pid", "runDir", "profileId", "releaseId", "runnerId", "modelHash",
                            "error", "deadlineReached", "stopRequested")
            reported_runs = [run if group_id and active and run.get("runId") == active.get("runId") else
                             {key: run.get(key) for key in summary_keys} for run in runs]
            output.append({"groupId": name, "name": spec.get("name", name),
                           "mode": "live" if spec.get("kind") == "live" else spec.get("modelMode", "nautilus_sandbox"),
                           "dashboardVisible": spec.get("dashboardVisible", True),
                           "environment": spec.get("environment", "sandbox"), "ready": reason is None,
                           "marketTransport": spec["marketTransport"], "marketData": MARKET_DATA[spec["marketTransport"]],
                           "runId": active["runId"] if active else None, "supportedActions": actions,
                           "defaultBudgetUsdt": spec["defaultBudgetUsdt"], "maxBudgetUsdt": spec["maxBudgetUsdt"],
                           "maxDurationSeconds": spec["maxDurationSeconds"], "runs": reported_runs,
                           "durationPolicy": duration_policy(spec), "executionPolicy": execution_policy(spec),
                           "executionOptions": contract(spec),
                           "capabilities": capabilities,
                           **({"kind": "live", "profileId": spec["profileId"], "ordersEnabled": True,
                               "capitalMode": spec.get("capitalMode", "account_inventory"),
                               "exposureCapUsdt": spec.get("exposureCapUsdt")} if spec.get("kind") == "live" else {}),
                           **({"releaseId": spec["releaseId"], "runnerId": spec["runnerId"],
                               "modelHash": spec["modelHash"], "manifestSha256": spec["manifestSha256"],
                               "device": spec["modelManifest"]["runtime"]["device"], "warmupRequired": True}
                              if spec.get("kind") == "live" and spec.get("modelHash") else {}),
                           **({"kind": "model", "releaseId": spec["releaseId"], "runnerId": spec["runnerId"],
                               "manifestSha256": spec["manifestSha256"], "modelHash": spec["modelHash"],
                               "device": spec["modelManifest"]["runtime"]["device"], "nodeCapabilities": self.registry.node_capabilities,
                               "contractKey": spec["contractKey"], "executionInstrumentId": spec["executionInstrumentId"],
                               "capacity": model_capacity,
                               "warmupRequired": True, "ordersEnabled": False} if spec.get("kind") == "model" else {})})
        return {"available": True, "groups": output,
                **({"modelRuntimeReason": self.registry.model_catalog_reason} if self.registry.model_runtime else {})}

    async def prepare(self, request):
        if not isinstance(request, dict) or set(request) - {"groupId", "action", "runId", "budgetUsdt", "durationSeconds", "registryVersion", "inventoryHash", "instruments", "executionSettings"}:
            raise ValueError("策略组请求字段不正确")
        group_id, action = request.get("groupId"), request.get("action")
        spec = self.registry.spec(group_id)
        if action not in ACTIONS:
            raise ValueError("不支持的策略组操作")
        if request.get("registryVersion", self.registry.digest) != self.registry.digest:
            raise ValueError("策略版本已更新，请重新检查配置")
        normalized = {"groupId": group_id, "action": action, "registryVersion": self.registry.digest}
        live = None
        run_settings = None
        if spec.get("kind") == "live" and action not in {"start", "stop"} and spec.get('controlVersion') != 1:
            raise ValueError("此版本支持启动和停止；其他控制需更新运行程序")
        if action in {'cancel','flatten'} and spec.get('kind') != 'live':
            raise ValueError('此控制用于实盘运行')
        if 'instruments' in request and action != 'flatten':
            raise ValueError('只有平仓操作可指定品种范围')
        if action == "start":
            from execution_options import settings
            run_settings=settings(spec,request.get('executionSettings'))
            if run_settings is not None:normalized['executionSettings']=run_settings
            if spec.get("kind") == "model":
                await asyncio.to_thread(self.registry.validate_model_inputs, spec)
                self.require_model_capacity()
            else:
                self.registry.validate_inputs(group_id)
                if spec.get("modelHash"):
                    self.require_model_capacity()
            if request.get("runId"):
                raise ValueError("新实例不能指定旧运行编号")
            for row in self.rows(group_id):
                if (await self.run_status(row))["status"] in ACTIVE:
                    raise ValueError("该策略组已有运行实例")
            if spec.get('kind') == 'live':
                for other_id,other in self.registry.groups.items():
                    if other_id == group_id or other.get('kind') != 'live' or other.get('profileId') != spec['profileId']:
                        continue
                    for row in self.rows(other_id):
                        if (await self.run_status(row))['status'] in ACTIVE:
                            raise ValueError(f"账户资金正在由 {other.get('name',other_id)} 使用；请使用独立账户，或先结束该运行")
            policy = duration_policy(spec)
            duration = request.get("durationSeconds", policy["defaultSeconds"] if policy["continuous"] else spec["maxDurationSeconds"])
            if (type(duration) is not int or not 0 <= duration <= 9007199254740991
                    or (duration == 0 and not policy["continuous"])
                    or (duration > 0 and policy["maxSeconds"] is not None and duration > policy["maxSeconds"])
                    or (spec.get("kind") != "live" and duration < 60)):
                raise ValueError("运行时长与已发布的运行方式不一致")
            if spec.get("kind") == "live":
                for other in self.db.execute("SELECT * FROM runs"):
                    other_spec = self.registry.groups.get(other["group_id"], {})
                    if (other["state"] in ACTIVE and other_spec.get("kind") == "live"
                            and other_spec.get("profileId") == spec.get("profileId")
                            and self.owned_process(other) is not None):
                        raise ValueError(f"执行账户正在运行 {other_spec.get('name', other['group_id'])}")
            live = (await asyncio.to_thread(self.registry.live_preview, spec, run_settings) if run_settings is not None else
                    await asyncio.to_thread(self.registry.live_preview, spec)) if spec.get("kind") == "live" else None
            if live:
                if request.get("inventoryHash", live["inventoryHash"]) != live["inventoryHash"]:
                    raise ValueError("账户余额或委托已变化，请重新检查配置")
                normalized.update(durationSeconds=duration, inventoryHash=live["inventoryHash"])
            else:
                budget = money(request.get("budgetUsdt", spec["defaultBudgetUsdt"]))
                if Decimal(budget) > Decimal(spec["maxBudgetUsdt"]):
                    raise ValueError("预算超过策略组已发布上限")
                normalized.update(budgetUsdt=budget, durationSeconds=duration)
        else:
            if "budgetUsdt" in request or "durationSeconds" in request or 'executionSettings' in request:
                raise ValueError("运行中不能改变实例预算或时长")
            row = self.row(request.get("runId"), group_id)
            run = await self.run_status(row)
            if run["status"] not in ACTIVE or self.owned_process(row) is None:
                raise ValueError("实例已结束或进程身份不匹配")
            if action == "resume" and run["status"] not in {"halted", "reducing"}:
                raise ValueError("仅可恢复已暂停或减仓中的实例")
            engine = run.get('engine') or {}
            if spec.get('kind') == 'live' and action != 'stop' and engine.get('controlVersion') != 1:
                raise ValueError('当前运行尚未提供该控制能力')
            if action == 'resume' and engine.get('resumeAllowed') is False:
                raise ValueError('平仓后的库存已变化，请停止后重新启动策略')
            if action == 'flatten':
                known = {item['instrument'] for item in (engine.get('inventory') or {}).get('pairs',[])}
                symbols = request.get('instruments')
                if not isinstance(symbols,list) or not symbols or len(symbols)>128 or any(not isinstance(s,str) or s not in known for s in symbols):
                    raise ValueError('请选择本次运行中的交易品种')
                if (engine.get('flatten') or {}).get('phase') in {'cancelling','reducing'}:
                    raise ValueError('当前平仓请求尚未结束')
                normalized['instruments'] = sorted(set(symbols))
            normalized["runId"] = row["id"]
        return {"request": normalized, "mode": "live" if spec.get("kind") == "live" else spec.get("modelMode", "nautilus_sandbox"), "groupName": spec.get("name", group_id),
                "executionPolicy": __import__('execution_options').describe(run_settings) or execution_policy(spec), "durationPolicy": duration_policy(spec),
                "executionSettings": run_settings,
                "marketTransport": spec["marketTransport"], "marketData": MARKET_DATA[spec["marketTransport"]],
                "effect": "使用账户库存启动实盘策略" if action == "start" and spec.get("kind") == "live" else
                          "新建独立虚拟资金实例" if action == "start" else
                          "暂停本策略组并撤销本组全部活动委托，按 10 bps 限价偏移卖出所选品种的本组分配库存；保留未成交余额" if action == 'flatten' else
                          "暂停本策略组并撤销本组全部活动委托，保留持仓" if action in {'halt','cancel'} else
                          "只接受减少库存的委托；双边做市报价暂停" if action == 'reduce' else
                          "停止策略进程并撤销本组活动委托，保留持仓" if action == 'stop' else "恢复所选策略的信号执行",
                "signalVersion": spec.get("signalsSha256"), "strategyVersion": spec.get("strategySha256"),
                **({"liveInventory": live, "ordersEnabled": True, "profileId": spec["profileId"],
                    "exposureCapUsdt": spec.get("exposureCapUsdt"),
                    "capitalMode": spec.get("capitalMode", "account_inventory")}
                   if action == "start" and spec.get("kind") == "live" else {}),
                **({"releaseId": spec["releaseId"], "manifestSha256": spec["manifestSha256"], "modelHash": spec["modelHash"],
                    "ordersEnabled": False, "warmupRequired": True} if spec.get("kind") == "model" else {})}

    async def execute(self, request, operation_id):
        if not isinstance(operation_id, str) or not OPERATION_ID.fullmatch(operation_id):
            raise ValueError("操作编号格式不正确")
        serialized = encode(request)
        async with self.lock:
            prior = self.db.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
            if prior:
                if prior["request"] != serialized:
                    raise ValueError("操作编号已用于其他请求")
                return await self.receipt(operation_id)
            prepared = await self.prepare(request)
            normalized = prepared["request"]
            if normalized != request:
                raise ValueError("请先检查并确认规范化运行请求")
            with self.db:
                self.db.execute("INSERT INTO operations(id,request) VALUES(?,?)", (operation_id, serialized))
            try:
                if normalized["action"] == "start":
                    result = await self.launch(normalized)
                else:
                    result = await self.control(normalized, operation_id)
                result["operationId"] = operation_id
                result.setdefault("receiptStatus", "completed")
            except (OSError, TimeoutError, ConnectionError) as error:
                uncertain = normalized["action"] != "start"
                result = {"operationId": operation_id, "status": "unknown" if uncertain else "failed",
                          "receiptStatus": "unknown" if uncertain else "failed",
                          "groupId": normalized["groupId"], "runId": normalized.get("runId"),
                          "reason": "控制结果尚未确认；只查询回执，不重复执行", "error": str(error)}
            except Exception as error:
                result = {"operationId": operation_id, "status": "failed", "receiptStatus": "failed", "error": str(error)}
            with self.db:
                self.db.execute("UPDATE operations SET result=? WHERE id=?", (encode(result), operation_id))
            return result

    async def receipt(self, operation_id):
        if not isinstance(operation_id, str) or not OPERATION_ID.fullmatch(operation_id):
            raise ValueError("操作编号格式不正确")
        operation = self.db.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
        if operation is None:
            raise ValueError("操作回执不存在")
        request = json.loads(operation["request"])
        result = json.loads(operation["result"]) if operation["result"] else {"status": "unknown", "receiptStatus": "unknown"}
        result["operationId"] = operation_id
        result.setdefault("receiptStatus", "unknown" if result.get("status") == "unknown" else
                          "failed" if result.get("status") == "failed" else "completed")
        if result["receiptStatus"] in {"processing", "unknown"} and request["action"] in {"halt", "reduce", "resume"}:
            # Query only; an observed trading state is not a receipt for this
            # particular action (another operator may have changed it since).
            try:
                row = self.row(request["runId"], request["groupId"])
                if self.owned_process(row):
                    engine = await socket_call(self.path / row["id"] / "control.sock",
                        {"command": "receipt", "operationId": operation_id}, 1.0)
                else:
                    engine = self.stored_worker_receipt(row["id"], operation_id)
                if engine.get("operationId") == operation_id and engine.get("receiptStatus") in {"completed", "failed"}:
                    result = self.control_result(request, engine)
                    result["operationId"] = operation_id
                    with self.db:
                        self.db.execute("UPDATE operations SET result=? WHERE id=?", (encode(result), operation_id))
            except (OSError, TimeoutError, ValueError):
                pass
        return result

    def stored_worker_receipt(self, run_id, operation_id):
        journal = self.path / run_id / "control-operations.sqlite"
        if journal.is_symlink() or not journal.is_file():
            return {}
        try:
            with closing(sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True)) as db:
                row = db.execute("SELECT status,result FROM operations WHERE id=?", (operation_id,)).fetchone()
                if row and row[1]:
                    return {**json.loads(row[1]), "operationId": operation_id, "receiptStatus": row[0]}
        except (sqlite3.Error, ValueError):
            pass
        return {}

    def control_result(self, request, engine):
        receipt_status = engine.get("receiptStatus", "completed" if engine.get("ok") else "unknown")
        result = {"groupId": request["groupId"], "runId": request["runId"],
                  "receiptStatus": receipt_status, "engine": engine}
        if receipt_status != "completed" or not engine.get("ok"):
            return {**result, "status": "failed" if receipt_status == "failed" else "unknown",
                    "error": engine.get("error") or "引擎未确认操作"}
        state = {"HALTED":"halted", "REDUCING":"reducing", "ACTIVE":"running"}.get(engine.get('tradingState')) or {
            "halt": "halted", "reduce": "reducing", "resume": "running", 'cancel':'halted', 'flatten':'reducing'}[request["action"]]
        # Receipt reconciliation must not overwrite a later control/run state.
        return {**result, "status": state}

    async def launch(self, request):
        spec = self.registry.spec(request["groupId"])
        if spec.get("kind") == "model":
            await asyncio.to_thread(self.registry.validate_model_inputs, spec)
            self.require_model_capacity()
        elif spec.get("kind") == "live":
            self.registry.validate_inputs(request["groupId"])
        run_id = f"{request['groupId']}-{uuid.uuid4().hex[:16]}"
        directory = self.path / run_id
        directory.mkdir(mode=0o700)
        frozen = {**request, "runId": run_id, "createdAt": time.time()}
        request_path = directory / "request.json"
        request_path.write_text(encode(frozen) + "\n")
        os.chmod(request_path, 0o600)
        metadata = ({"mode": "live", "profileId": spec["profileId"], "ordersEnabled": True,
                     "inventoryHash": request["inventoryHash"]} if spec.get("kind") == "live" else
                    {"mode": "shadow", "modelHash": spec["modelHash"], "manifestSha256": spec["manifestSha256"],
                     "releaseId": spec["releaseId"], "runnerId": spec["runnerId"], "ordersEnabled": False}
                    if spec.get("kind") == "model" else {})
        if spec.get("kind") == "live" and spec.get("modelHash"):
            metadata.update(modelHash=spec["modelHash"], manifestSha256=spec["manifestSha256"],
                            releaseId=spec["releaseId"], runnerId=spec["runnerId"], ordersEnabled=False)
        if request.get('executionSettings'):metadata['executionSettings']=request['executionSettings']
        with self.db:
            self.db.execute("INSERT INTO runs(id,group_id,state,budget,duration,created,result) VALUES(?,?,?,?,?,?,?)",
                            (run_id, request["groupId"], "starting", request.get("budgetUsdt", "0.00"),
                             request["durationSeconds"], frozen["createdAt"], encode(metadata)))
        # No OKX credentials, shell, proxy env, PYTHONPATH, or user-supplied imports
        # cross into this sandbox worker. The service has its own cgroup.
        environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "TZ": "UTC",
                       "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1",
                       "OKX_API_KEY": "", "OKX_API_SECRET": "", "OKX_API_PASSPHRASE": ""}
        try:
            with (directory / "engine.log").open("ab", buffering=0) as stream:
                command = self.registry.command(spec, request_path)
                process = await self.process_factory(*command,
                    cwd=str(Path(command[1]).parent), env=environment, stdout=stream,
                    stderr=asyncio.subprocess.STDOUT, start_new_session=True)
            self.processes[run_id] = process
            created = psutil.Process(process.pid).create_time()
            with self.db:
                self.db.execute("UPDATE runs SET pid=?,process_created=? WHERE id=?", (process.pid, created, run_id))
            task = asyncio.create_task(self.watch(run_id, process))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
            return {"runId": run_id, "groupId": request["groupId"], "status": "starting",
                    "mode": "live" if spec.get("kind") == "live" else "nautilus_sandbox", **metadata}
        except Exception as error:
            self.update(run_id, "failed", {"error": str(error)})
            raise

    async def watch(self, run_id, process):
        code = await process.wait()
        row = self.db.execute("SELECT state,result,group_id FROM runs WHERE id=?", (run_id,)).fetchone()
        previous = json.loads(row["result"]) if row else {}
        directory = self.path / run_id
        try:
            final = object_file(directory / "final.json")
        except (ValueError, OSError):
            final = {}
        valid_final = final.get("run_id") == run_id and row and final.get("group_id") == row["group_id"]
        try:
            reports = all((directory / name).is_file() and not (directory / name).is_symlink()
                          and (directory / name).stat().st_size > 0
                          for name in ("orders.csv", "fills.csv", "positions.csv", "account.csv"))
        except OSError:
            reports = False
        engine = previous.get("engine") if isinstance(previous.get("engine"), dict) else {}
        observed_error = (row and row["state"] == "error"
                          or engine.get("tradingState") in {"ERROR", "FAILED", "FAULTED"}
                          or previous.get("error") or final.get("error")
                          or previous.get("errorObserved") is True
                          or final.get("trading_state") in {"ERROR", "FAILED", "FAULTED"})
        stopped = row and (row["state"] == "stopping" and not previous.get("automaticShutdown") or previous.get("stopRequested") is True)
        stopped_before_ready = bool(valid_final and stopped and previous.get("modelHash")
                                    and previous.get("ordersEnabled") is False
                                    and final.get("stop_reason") == "stopped_before_model_ready"
                                    and final.get("node_started") is False and final.get("orders_enabled") is False)
        if code != 0 or observed_error:
            state = "failed"
        elif valid_final and stopped and (reports or stopped_before_ready):
            state = "stopped"
        elif valid_final and reports and (final.get("deadline_reached") is True or final.get("validation_complete") is True):
            state = "completed"
        else:
            state = "interrupted"
        result = {**previous, "exitCode": code, "finalAvailable": bool(valid_final), "reportsAvailable": reports,
                  "deadlineReached": final.get("deadline_reached") is True}
        if previous.get("modelHash"):
            saved_model = final.get("model") if valid_final else None
            if (not isinstance(saved_model, dict) or saved_model.get("modelHash") != previous["modelHash"]
                    or saved_model.get("releaseId") != previous.get("releaseId")):
                saved_model = engine.get("model", {})
            # A completed process must not retain the last live readiness flag.
            result["engine"] = {**engine, "tradingState": "ERROR" if state in {"failed", "interrupted"} else "STOPPED",
                "warmupComplete": False, "marketReady": False, "connected": {"data": False, "exec": False},
                "model": {**saved_model, "warmupComplete": False,
                    "status": "error" if state == "failed" else "unavailable" if state == "interrupted" else "stopped"}}
        if stopped_before_ready and state == "stopped":
            result["stoppedBeforeModelReady"] = True
        if observed_error:
            result["error"] = str(final.get("error") or previous.get("error") or "引擎曾报告异常状态")
        elif state == "interrupted":
            result["reason"] = "结束证据不完整或运行未达到计划结束时间"
        self.update(run_id, state, result)
        self.processes.pop(run_id, None)

    async def control(self, request, operation_id=None):
        row = self.row(request["runId"], request["groupId"])
        process = self.owned_process(row)
        if process is None:
            raise ValueError("实例进程身份不匹配")
        action = request["action"]
        if action == "stop":
            # psutil rechecks PID reuse immediately before signalling.
            # Persist intent first: SIGTERM can cause watch() to run immediately.
            self.update(row["id"], "stopping", {**json.loads(row["result"]), "stopRequested": True})
            process.send_signal(signal.SIGTERM)
            return {"runId": row["id"], "groupId": row["group_id"], "status": "stopping"}
        try:
            engine = await socket_call(self.path / row["id"] / "control.sock",
                {"command": action, "operationId": operation_id or uuid.uuid4().hex,
                 'runId':row['id'], **({'instruments':request['instruments']} if action=='flatten' else {}),
                 "operator": "montlok", "reason": "confirmed group action"}, 3)
        except (ValueError, OSError, TimeoutError) as error:
            raise TimeoutError("控制回复未确认，操作可能已生效") from error
        if engine.get("operationId") != operation_id or engine.get("receiptStatus") not in {"completed", "failed", "unknown", "processing"}:
            return {"runId": row["id"], "groupId": row["group_id"], "status": "unknown", "receiptStatus": "unknown",
                    "reason": "正在查询引擎的持久化操作回执"}
        result = self.control_result(request, engine)
        if result["receiptStatus"] == "completed":
            current = self.row(row["id"], row["group_id"])
            if current["state"] in ACTIVE and current["state"] != "stopping":
                self.update(row["id"], result["status"])
        return result

    async def serve(self, reader, writer):
        try:
            raw = await asyncio.wait_for(reader.readline(), 5)
            if not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE:
                raise ValueError("请求过大或不完整")
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError("请求必须为 JSON 对象")
            command = request.get("command")
            if command == "status":
                result = await self.status(request.get("groupId"))
            elif command == "prepare":
                result = await self.prepare(request.get("request"))
            elif command == "execute":
                result = await self.execute(request.get("request"), request.get("operationId"))
            elif command == "receipt":
                result = await self.receipt(request.get("operationId"))
            else:
                raise ValueError("未知运行服务操作")
            response = {"ok": True, "result": result}
        except Exception as error:
            response = {"ok": False, "error": str(error)}
        try:
            writer.write((encode(response) + "\n").encode())
            await asyncio.wait_for(writer.drain(), 5)
        except (OSError, TimeoutError):
            pass  # Receipt has already committed, even if the client left.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def start(self, path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if not stat.S_ISSOCK(path.stat().st_mode):
                raise ValueError("控制路径已被非 socket 文件占用")
            try:
                await socket_call(path, {"command": "status"}, 0.5)
            except (ConnectionRefusedError, FileNotFoundError):
                path.unlink(missing_ok=True)
            else:
                raise ValueError("运行服务已存在")
        self.server = await asyncio.start_unix_server(self.serve, path=str(path), limit=MAX_MESSAGE)
        os.chmod(path, 0o600)


async def main(args):
    supervisor = GroupSupervisor(LaunchRegistry(args.registry), args.state_dir)
    await supervisor.start(args.socket)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    reload_tasks = set()

    async def reload_and_report():
        try:
            result = await supervisor.reload_registry()
            print(encode({"event": "registry_reloaded", **result}), flush=True)
        except Exception as error:
            print(encode({"event": "registry_reload_failed", "error": str(error)}),
                  file=sys.stderr, flush=True)

    def schedule_reload():
        task = loop.create_task(reload_and_report())
        reload_tasks.add(task)
        task.add_done_callback(reload_tasks.discard)

    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    loop.add_signal_handler(signal.SIGHUP, schedule_reload)
    await stop.wait()
    supervisor.server.close()
    await supervisor.server.wait_closed()
    # Only exact processes created through this registry can receive SIGTERM.
    # The dedicated service's cgroup is the final cleanup boundary on shutdown.
    for group_id in supervisor.registry.groups:
        for row in supervisor.rows(group_id):
            if row["state"] in ACTIVE and supervisor.owned_process(row):
                await supervisor.control({"groupId": group_id, "runId": row["id"], "action": "stop"})
    if supervisor.tasks:
        await asyncio.wait(supervisor.tasks, timeout=40)
    if reload_tasks:
        await asyncio.wait(reload_tasks, timeout=5)
    args.socket.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--check-registry", action="store_true", help="Validate reviewed sandbox inputs without starting a process")
    arguments = parser.parse_args()
    if arguments.check_registry:
        checked = LaunchRegistry(arguments.registry)
        for group in checked.groups:
            checked.validate_inputs(group)
        print(encode({"ok": True, "groups": list(checked.groups), "registryVersion": checked.digest}))
    elif arguments.state_dir is None or arguments.socket is None:
        parser.error("--state-dir and --socket are required to serve")
    else:
        asyncio.run(main(arguments))
