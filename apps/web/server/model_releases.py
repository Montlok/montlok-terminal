"""Versioned model publication, not execution or model-quality approval.

Only registered artifacts can become releases. A bundle's source files are
provenance; this module never imports code, unpickles weights, spawns a runner,
or accesses trading credentials. A host-installed, allowlisted runner interprets
the frozen manifest later under a separate, explicitly confirmed run action.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
import time
import zipfile
from contextlib import closing
from pathlib import Path

from artifacts import ArtifactError, ArtifactStore

RUNNERS = {"gru_v1": "recent_btc_gru", "rdt4quant_v1": "rdt4quant_multiasset",
           "rdt4quant_cpu_v2": "rdt4quant_multiasset"}
ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
HASH = re.compile(r"^[a-f0-9]{64}$")
OPERATION = re.compile(r"^[A-Za-z0-9_-]{8,96}$")


class ModelReleaseError(ArtifactError):
    pass


class ModelReleaseConflict(ModelReleaseError):
    status = 409


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def number(value, label, low=None, high=None):
    try:
        finite = not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise ModelReleaseError(f"{label} 必须为有限数值")
    if low is not None and value < low or high is not None and value > high:
        raise ModelReleaseError(f"{label} 超出范围")
    return value


def integer(value, label, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ModelReleaseError(f"{label} 必须为 {low} 至 {high} 的整数")


def relative(name):
    if (not isinstance(name, str) or not name or len(name) > 256 or name.startswith(("/", "\\"))
            or any(c in name for c in ("\\", ":")) or re.search(r"[\x00-\x1f\x7f]", name)
            or any(p in {"", ".", ".."} for p in name.split("/"))):
        raise ModelReleaseError("发布文件路径必须为包内相对路径")
    return name


def released_file(root, name):
    root = Path(root)
    path = root / relative(name)
    if root.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent.is_relative_to(root)) or path.is_symlink():
        raise ModelReleaseError("不可变发布路径包含符号链接")
    return path


def json_manifest(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ModelReleaseError("manifest 包含重复 JSON 字段")
            result[key] = value
        return result
    try:
        return validate_manifest(json.loads(raw, object_pairs_hook=pairs))
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ModelReleaseError(str(error) if isinstance(error, ModelReleaseError) else "manifest JSON 无效或包含非有限数值") from error


def file_records(manifest):
    records = [manifest["model"], *manifest.get("sources", [])]
    # Research metadata may also carry a content-addressed provenance record.
    def collect(value):
        if isinstance(value, dict):
            if "path" in value and "sha256" in value:
                records.append(value)
            else:
                for item in value.values():
                    collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)
    collect(manifest.get("provenance", {}))
    return records


def feature_contract(contract, label, *, domain=False):
    if not isinstance(contract, dict):
        raise ModelReleaseError(f"{label} 必须为对象")
    names = contract.get("names")
    if (not isinstance(names, list) or not 1 <= len(names) <= 1024
            or any(not isinstance(n, str) or not re.fullmatch(r"[A-Za-z0-9_:\-./]{1,128}", n) for n in names)
            or len(set(names)) != len(names)):
        raise ModelReleaseError(f"{label} 特征名称或顺序不正确")
    for key in ("mean", "scale"):
        values = contract.get(key)
        if not isinstance(values, list) or len(values) != len(names):
            raise ModelReleaseError(f"{label}.{key} 与特征维度不一致")
        for value in values:
            number(value, f"{label}.{key}")
            if key == "scale" and value <= 0:
                raise ModelReleaseError("归一化 scale 必须为正数")
    integer(contract.get("sequenceBars"), f"{label}.sequenceBars", 1, 4096)
    integer(contract.get("barSeconds"), f"{label}.barSeconds", 1, 86400)
    if len(names) * contract["sequenceBars"] > 262144:
        raise ModelReleaseError("输入特征矩阵过大")
    clip = contract.get("clip")
    if not isinstance(clip, list) or len(clip) != 2:
        raise ModelReleaseError("clip 必须有上下界")
    if number(clip[0], "clip", -1000000, 1000000) >= number(clip[1], "clip", -1000000, 1000000):
        raise ModelReleaseError("clip 上下界不正确")
    markets = contract.get("requiredMarkets", [contract.get("instrument")] if domain else None)
    if not isinstance(markets, list) or not 1 <= len(markets) <= 256 or any(not isinstance(m, str) or not m or len(m) > 96 for m in markets):
        raise ModelReleaseError("requiredMarkets 必须绑定行情来源")
    if domain:
        integer(contract.get("assetId"), f"{label}.assetId", 0, 100000)
        if not isinstance(contract.get("domain"), str) or not contract["domain"]:
            raise ModelReleaseError("domainContract 缺少 domain")
        if not isinstance(contract.get("instrument"), str) or not contract["instrument"]:
            raise ModelReleaseError("domainContract 缺少 instrument")
        scales = contract.get("yScale")
        if not isinstance(scales, list) or not 1 <= len(scales) <= 64:
            raise ModelReleaseError("domainContract 缺少输出 yScale")
        for value in scales:
            if number(value, "yScale") <= 0:
                raise ModelReleaseError("yScale 必须为正数")


def validate_manifest(manifest):
    if not isinstance(manifest, dict):
        raise ModelReleaseError("manifest 必须为对象")
    try:
        encode(manifest)
    except (TypeError, ValueError, RecursionError):
        raise ModelReleaseError("manifest 必须为有限 JSON 值") from None
    allowed = {"schemaVersion", "releaseId", "runnerId", "family", "modelVersion", "model", "sources",
               "featureContract", "domainContracts", "outputContract", "runtime", "policy", "provenance"}
    if set(manifest) - allowed or type(manifest.get("schemaVersion")) is not int or manifest["schemaVersion"] != 1:
        raise ModelReleaseError("不支持的发布清单字段或 schemaVersion")
    runner = manifest.get("runnerId")
    if not isinstance(runner, str) or runner not in RUNNERS or manifest.get("family") != RUNNERS[runner]:
        raise ModelReleaseError("runnerId 仅允许预装 gru_v1 / rdt4quant_v1，且须与 family 一致")
    if not isinstance(manifest.get("releaseId"), str) or not ID.fullmatch(manifest["releaseId"]):
        raise ModelReleaseError("releaseId 格式不正确")
    version = manifest.get("modelVersion")
    if not isinstance(version, str) or not 1 <= len(version) <= 160 or re.search(r"[\x00-\x1f\x7f]", version):
        raise ModelReleaseError("modelVersion 必须是明确版本")
    if not isinstance(manifest.get("model"), dict) or not isinstance(manifest.get("sources"), list) or len(manifest["sources"]) > 256:
        raise ModelReleaseError("model / sources 文件清单不正确")
    if not isinstance(manifest.get("provenance"), dict) or "independentTestMetrics" not in manifest["provenance"]:
        raise ModelReleaseError("provenance 必须显式记录 independentTestMetrics；无独立评估时填写 null")
    if manifest["provenance"]["independentTestMetrics"] is not None and not isinstance(manifest["provenance"]["independentTestMetrics"], dict):
        raise ModelReleaseError("independentTestMetrics 必须为对象或 null")
    paths = {}
    for record in file_records(manifest):
        if not isinstance(record, dict) or set(record) - {"path", "sha256", "artifactId"}:
            raise ModelReleaseError("发布文件记录不正确")
        name = relative(record.get("path"))
        checksum = record.get("sha256")
        if not isinstance(checksum, str) or not HASH.fullmatch(checksum) or name == "manifest.json":
            raise ModelReleaseError("文件 sha256 不正确或与 manifest 路径冲突")
        if name.casefold() in paths and paths[name.casefold()] != record:
            raise ModelReleaseError("发布文件路径重复或大小写冲突")
        paths[name.casefold()] = record
    for name in paths:
        if any(str(parent) in paths for parent in Path(name).parents if str(parent) != "."):
            raise ModelReleaseError("发布文件与目录路径冲突")
    if runner == "gru_v1":
        if "domainContracts" in manifest:
            raise ModelReleaseError("GRU 必须使用单一 featureContract")
        feature_contract(manifest.get("featureContract"), "featureContract")
    else:
        contracts = manifest.get("domainContracts")
        if not isinstance(contracts, dict) or not 1 <= len(contracts) <= 256:
            raise ModelReleaseError("RDT 缺少 domainContracts")
        for key, contract in contracts.items():
            feature_contract(contract, key, domain=True)
            if key != f"{contract['domain']}:{contract['instrument']}":
                raise ModelReleaseError("domainContract 品种键不一致")
    output = manifest.get("outputContract")
    if not isinstance(output, dict) or not isinstance(output.get("kind"), str) or output["kind"] not in {"return", "simple_return", "log_return_bps_quantiles"}:
        raise ModelReleaseError("输出类型必须明确为收益预测")
    if runner == "gru_v1":
        if output["kind"] not in {"return", "simple_return"} or number(output.get("targetScale"), "targetScale") <= 0:
            raise ModelReleaseError("GRU 输出 targetScale 不正确")
    else:
        if output["kind"] != "log_return_bps_quantiles":
            raise ModelReleaseError("RDT 输出须为 log_return_bps_quantiles")
        quantiles = output.get("quantiles")
        if not isinstance(quantiles, list) or quantiles != [0.1, 0.5, 0.9]:
            raise ModelReleaseError("RDT quantiles 必须与现有训练输出一致：[0.1, 0.5, 0.9]")
        for value in quantiles:
            number(value, "quantiles", 0, 1)
        if sorted(set(quantiles)) != quantiles or quantiles[0] <= 0 or quantiles[-1] >= 1:
            raise ModelReleaseError("quantiles 须严格递增且位于 (0,1)")
        integer(output.get("depth"), "depth", 1, 256)
    horizons = output.get("horizons")
    if not isinstance(horizons, (dict, list)) or not horizons:
        raise ModelReleaseError("输出 horizons 不正确")
    horizon_sets = horizons.values() if isinstance(horizons, dict) else [horizons]
    for values in horizon_sets:
        if not isinstance(values, list) or not 1 <= len(values) <= 64:
            raise ModelReleaseError("输出 horizons 不正确")
        for value in values:
            integer(value, "horizon", 1, 100000)
        if sorted(set(values)) != values:
            raise ModelReleaseError("horizons 须严格递增")
    unit = output.get("horizonUnit")
    if not isinstance(unit, (str, dict)) or not unit:
        raise ModelReleaseError("输出 horizonUnit 必须明确")
    if isinstance(unit, dict) and (not isinstance(horizons, dict) or set(unit) != set(horizons)
                                  or any(not isinstance(v, str) or not v for v in unit.values())):
        raise ModelReleaseError("horizonUnit 与 horizons domain 不一致")
    if runner in {"rdt4quant_v1", "rdt4quant_cpu_v2"}:
        if not isinstance(horizons, dict) or not isinstance(unit, dict):
            raise ModelReleaseError("RDT horizons / horizonUnit 必须是按 domain 绑定的对象")
        for contract in manifest["domainContracts"].values():
            domain_horizons = horizons.get(contract["domain"]) if isinstance(horizons, dict) else horizons
            if not isinstance(domain_horizons, list) or len(contract["yScale"]) != len(domain_horizons):
                raise ModelReleaseError("RDT yScale 与 domain 输出 horizons 维度不一致")
        selection = output.get("selectedHeadByDomain")
        if not isinstance(selection, dict):
            raise ModelReleaseError("RDT 必须显式选择 selectedHeadByDomain，不能默认第一个头")
        domains = {contract["domain"] for contract in manifest["domainContracts"].values()}
        if set(selection) != domains:
            raise ModelReleaseError("selectedHeadByDomain 与发布 domains 不一致")
        for domain, selected in selection.items():
            domain_horizons = horizons[domain] if isinstance(horizons, dict) else horizons
            integer(selected, f"selectedHeadByDomain.{domain}", 0, len(domain_horizons) - 1)
    else:
        if (not isinstance(horizons, list) or type(output.get("selectedHorizon")) is not int
                or output["selectedHorizon"] not in horizons or not isinstance(unit, str)
                or output.get("selectedHorizonUnit") != unit):
            raise ModelReleaseError("GRU selectedHorizon 与输出 contract 不一致")
    runtime = manifest.get("runtime")
    if not isinstance(runtime, dict) or set(runtime) - {"device", "threads", "maxBatchSize", "maxQueueSize", "timeoutMs", "maxInputAgeMs"}:
        raise ModelReleaseError("runtime 不允许 executable / import / 环境变量覆盖")
    if not isinstance(runtime.get("device"), str) or runtime["device"] not in {"cpu", "cuda"} or runner == "rdt4quant_v1" and runtime["device"] != "cuda":
        raise ModelReleaseError("运行设备不受支持；RDT 必须使用 CUDA")
    if runner == "rdt4quant_cpu_v2" and runtime["device"] != "cpu":
        raise ModelReleaseError("montlok.cpp 运行器使用 CPU")
    if "threads" in runtime:
        integer(runtime["threads"], "threads", 1, 64)
    for name, lo, hi in (("maxBatchSize", 1, 64), ("maxQueueSize", 1, 256), ("timeoutMs", 1, 300000), ("maxInputAgeMs", 1, 172800000)):
        integer(runtime.get(name), name, lo, hi)
    policy = manifest.get("policy")
    if not isinstance(policy, dict) or set(policy) - {"allowedModes", "thresholdBps", "maxTargetFraction"}:
        raise ModelReleaseError("运行 policy 不正确")
    modes = policy.get("allowedModes")
    if not isinstance(modes, list) or not modes or any(not isinstance(m, str) or m not in {"shadow", "sandbox", "live"} for m in modes) or len(modes) != len(set(modes)):
        raise ModelReleaseError("模型发布须明确运行模式")
    if "live" in modes and runner != "rdt4quant_cpu_v2":
        raise ModelReleaseError("该运行器尚未配置原生实盘执行")
    number(policy.get("thresholdBps"), "thresholdBps", 0, 10000)
    number(policy.get("maxTargetFraction"), "maxTargetFraction", 0, 1)
    return manifest


class ModelReleaseStore:
    def __init__(self, root: Path, artifacts: ArtifactStore, *, max_releases=256, max_total_bytes=1024 * 1024 * 1024):
        self.root, self.artifacts = Path(root), artifacts
        self.max_releases, self.max_total_bytes = max_releases, max_total_bytes
        for path in (self.root, self.root / "releases", self.root / "incoming"):
            if path.is_symlink():
                raise ModelReleaseError("发布目录不能是符号链接")
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.database = self.root / "releases.sqlite"
        if self.database.is_symlink():
            raise ModelReleaseError("发布索引不能是符号链接")
        with self.connect() as db:
            # Publication is low-frequency and already serialized by SQLite.
            # The supervisor reads this directory through a read-only systemd
            # mount; DELETE mode needs no writable -wal/-shm sidecars there.
            # Switching an old WAL catalog checkpoints it under SQLite's own
            # lock. A concurrent holder must release its lock before migration.
            try:
                if db.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() != "delete":
                    raise ModelReleaseError("模型发布索引日志模式迁移尚未完成")
            except sqlite3.OperationalError as error:
                raise ModelReleaseError("模型发布索引正被使用，无法安全切换只读兼容日志模式") from error
            db.executescript("""
                CREATE TABLE IF NOT EXISTS releases (
                    id TEXT PRIMARY KEY, runner TEXT NOT NULL, model_version TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL UNIQUE, artifact_id TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL, manifest TEXT NOT NULL, bytes INTEGER NOT NULL,
                    created REAL NOT NULL, actor TEXT NOT NULL, UNIQUE(runner, model_version));
                CREATE TABLE IF NOT EXISTS active (runner TEXT PRIMARY KEY, release_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, request TEXT NOT NULL, result TEXT NOT NULL);
            """)
        self.database.chmod(0o600)

    def connect(self):
        db = sqlite3.connect(self.database, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=15000")
        db.execute("PRAGMA synchronous=FULL")
        return closing(db)

    def artifact(self, identifier):
        try:
            artifact = self.artifacts.get(identifier)
        except KeyError:
            raise ModelReleaseError("上传 artifact 不存在") from None
        path = self.artifacts.root / "objects" / artifact["sha256"]
        if path.is_symlink() or not path.is_file() or path.stat().st_size != artifact["bytes"]:
            raise ModelReleaseError("已登记 artifact 文件缺失或已改变")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != artifact["sha256"]:
                raise ModelReleaseError("已登记 artifact hash 不匹配")
        return artifact, path

    def inspect(self, artifact_id):
        artifact, path = self.artifact(artifact_id)
        if artifact["kind"] != "model" or artifact["format"] not in {"zip", "json"}:
            raise ModelReleaseError("发布需要 model 类型 ZIP / JSON manifest")
        files = {}
        if artifact["format"] == "zip":
            self.artifacts._archive(path)  # Bounded paths, symlinks, ratio, CRC, size, duplicates.
            with zipfile.ZipFile(path) as archive:
                try:
                    info = archive.getinfo("manifest.json")
                except KeyError:
                    raise ModelReleaseError("ZIP 根目录缺少 manifest.json") from None
                if info.file_size > ArtifactStore.MAX_JSON_BYTES:
                    raise ModelReleaseError("manifest.json 过大")
                raw = archive.read(info)
                manifest = json_manifest(raw)
                for item in archive.infolist():
                    if not item.is_dir():
                        data = archive.read(item)
                        files[item.filename] = data
                for record in file_records(manifest):
                    if record["path"] not in files or digest(files[record["path"]]) != record["sha256"]:
                        raise ModelReleaseError(f"发布文件 hash 不匹配或缺失: {record['path']}")
        else:
            if path.stat().st_size > ArtifactStore.MAX_JSON_BYTES:
                raise ModelReleaseError("manifest JSON 过大")
            raw = path.read_bytes()
            manifest = json_manifest(raw)
            files["manifest.json"] = raw
            total = len(raw)
            for record in file_records(manifest):
                referenced, target = self.artifact(record.get("artifactId"))
                if referenced["sha256"] != record["sha256"]:
                    raise ModelReleaseError("JSON manifest 引用 artifact hash 不匹配")
                total += referenced["bytes"]
                if total > ArtifactStore.MAX_ARCHIVE_BYTES:
                    raise ModelReleaseError("发布展开内容过大")
                files[record["path"]] = target.read_bytes()
        size = sum(len(data) for data in files.values())
        if size > ArtifactStore.MAX_ARCHIVE_BYTES:
            raise ModelReleaseError("发布展开内容过大")
        return {"artifact": artifact, "manifest": manifest, "manifestSha256": digest(raw), "files": files, "bytes": size}

    @staticmethod
    def preview(bundle):
        manifest, artifact = bundle["manifest"], bundle["artifact"]
        return {"releaseId": manifest["releaseId"], "runnerId": manifest["runnerId"], "modelVersion": manifest["modelVersion"],
                "artifactId": artifact["id"], "artifactSha256": artifact["sha256"], "manifestSha256": bundle["manifestSha256"],
                "modelHash": manifest["model"]["sha256"], "manifest": manifest, "bytes": bundle["bytes"],
                "status": "validated", "validation": "artifact_valid", "deployReady": False,
                "deployReadiness": "requires_target_node_preflight", "started": False, "checks": {"hashes": "verified", "schema": "verified",
                    "runner": "allowlisted_host_runner", "sourceExecution": "never", "modelQuality": "not_evaluated",
                    "deviceAvailability": "not_checked", "environments": manifest["policy"]["allowedModes"]}}

    def validate(self, artifact_id):
        return self.preview(self.inspect(artifact_id))

    def active_id(self, runner):
        with self.connect() as db:
            row = db.execute("SELECT release_id FROM active WHERE runner=?", (runner,)).fetchone()
            return row[0] if row else None

    def get(self, release_id):
        if not isinstance(release_id, str) or not ID.fullmatch(release_id):
            raise ModelReleaseError("发布版本不存在")
        with self.connect() as db:
            row = db.execute("SELECT * FROM releases WHERE id=?", (release_id,)).fetchone()
            if row is None:
                raise ModelReleaseError("发布版本不存在")
            manifest = json.loads(row["manifest"])
            active = db.execute("SELECT release_id FROM active WHERE runner=?", (row["runner"],)).fetchone()
        return {"releaseId": row["id"], "runnerId": row["runner"], "modelVersion": row["model_version"],
                "manifestSha256": row["manifest_hash"], "artifactId": row["artifact_id"], "artifactSha256": row["artifact_hash"],
                "modelHash": manifest["model"]["sha256"], "manifest": manifest, "bytes": row["bytes"],
                "publishedAt": row["created"], "publishedBy": row["actor"], "active": bool(active and active[0] == row["id"]),
                "status": "published", "validation": "artifact_valid", "deployReady": False,
                "deployReadiness": "requires_target_node_preflight", "started": False}

    def list(self):
        with self.connect() as db:
            identifiers = [row[0] for row in db.execute("SELECT id FROM releases ORDER BY created DESC LIMIT 256")]
        return [self.get(identifier) for identifier in identifiers]

    def prepare(self, action, arguments):
        if action not in {"publish", "rollback"} or not isinstance(arguments, dict):
            raise ModelReleaseError("不支持的模型发布操作")
        allowed = {"artifactId", "manifestSha256", "expectedActiveReleaseId"} if action == "publish" else {"releaseId", "manifestSha256", "expectedActiveReleaseId"}
        if set(arguments) - allowed:
            raise ModelReleaseError("模型发布请求字段不正确")
        preview = self.validate(arguments.get("artifactId")) if action == "publish" else self.get(arguments.get("releaseId"))
        if arguments.get("manifestSha256", preview["manifestSha256"]) != preview["manifestSha256"]:
            raise ModelReleaseConflict("发布清单已改变，请重新确认")
        active = self.active_id(preview["runnerId"])
        if arguments.get("expectedActiveReleaseId", active) != active:
            raise ModelReleaseConflict("当前发布版本已改变，请重新确认")
        key = "artifactId" if action == "publish" else "releaseId"
        request = {key: preview[key], "manifestSha256": preview["manifestSha256"], "expectedActiveReleaseId": active}
        return {"request": request, "release": preview, "effect": "将此版本设为当前发布版本",
                "previousReleaseId": active, "targetReleaseId": preview["releaseId"], "started": False}

    def materialize(self, bundle):
        target = self.root / "releases" / bundle["manifestSha256"]
        if target.is_symlink():
            raise ModelReleaseError("发布目录不能是符号链接")
        if target.exists():
            for name, data in bundle["files"].items():
                path = released_file(target, name)
                if not path.is_file() or path.stat().st_size != len(data) or digest(path.read_bytes()) != digest(data):
                    raise ModelReleaseError("不可变发布内容已改变")
            return
        with tempfile.TemporaryDirectory(prefix="release-", dir=self.root / "incoming") as temporary:
            staging = Path(temporary) / "content"
            staging.mkdir(mode=0o700)
            for name, data in bundle["files"].items():
                path = staging / relative(name)
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                with path.open("xb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                path.chmod(0o400)
            os.rename(staging, target)
            descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def execute(self, action, arguments, operation_id, actor):
        if not isinstance(operation_id, str) or not OPERATION.fullmatch(operation_id):
            raise ModelReleaseError("操作编号格式不正确")
        if not isinstance(actor, str) or not actor or len(actor) > 160:
            raise ModelReleaseError("发布必须关联已认证管理员")
        serialized = encode({"action": action, "arguments": arguments, "actor": actor})
        with self.connect() as db:
            prior = db.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
            if prior:
                if prior["request"] != serialized:
                    raise ModelReleaseConflict("操作编号已用于其他请求")
                return json.loads(prior["result"])
        try:
            prepared = self.prepare(action, arguments)
        except ModelReleaseConflict:
            # A concurrent copy may commit between the initial lookup and the
            # read-only preflight. Its exact receipt wins over stale active-ID
            # rejection, without creating another publication.
            with self.connect() as db:
                prior = db.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
                if prior and prior["request"] == serialized:
                    return json.loads(prior["result"])
            raise
        if prepared["request"] != arguments:
            raise ModelReleaseError("请先确认规范化模型发布请求")
        release = prepared["release"]
        bundle = self.inspect(release["artifactId"]) if action == "publish" else None
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
            if prior:
                if prior["request"] != serialized:
                    raise ModelReleaseConflict("操作编号已用于其他请求")
                return json.loads(prior["result"])
            current = db.execute("SELECT release_id FROM active WHERE runner=?", (release["runnerId"],)).fetchone()
            if (current[0] if current else None) != arguments["expectedActiveReleaseId"]:
                raise ModelReleaseConflict("当前发布版本已改变，请重新确认")
            if action == "publish":
                if bundle["manifestSha256"] != arguments["manifestSha256"]:
                    raise ModelReleaseConflict("确认后的发布内容已改变")
                existing = db.execute("SELECT * FROM releases WHERE id=? OR (runner=? AND model_version=?)",
                    (release["releaseId"], release["runnerId"], release["modelVersion"])).fetchone()
                if existing and (existing["manifest_hash"] != release["manifestSha256"] or existing["artifact_hash"] != release["artifactSha256"]):
                    raise ModelReleaseConflict("同一模型版本已发布其他内容，请使用新版本")
                if not existing:
                    count, size = db.execute("SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM releases").fetchone()
                    if count >= self.max_releases or size + bundle["bytes"] > self.max_total_bytes:
                        raise ModelReleaseError("模型发布版本数量或容量已达到上限")
                self.materialize(bundle)
                if not existing:
                    db.execute("INSERT INTO releases VALUES (?,?,?,?,?,?,?,?,?,?)", (release["releaseId"], release["runnerId"],
                        release["modelVersion"], release["manifestSha256"], release["artifactId"], release["artifactSha256"],
                        encode(bundle["manifest"]), bundle["bytes"], time.time(), actor))
            else:
                # Explicit old version is checked again; rollback never means
                # 'previous' by position and never mutates an active run.
                manifest_path = released_file(self.root / "releases" / release["manifestSha256"], "manifest.json")
                if not manifest_path.is_file() or manifest_path.stat().st_size > ArtifactStore.MAX_JSON_BYTES or digest(manifest_path.read_bytes()) != release["manifestSha256"]:
                    raise ModelReleaseError("回滚版本的不可变清单已改变或丢失")
                for record in file_records(release["manifest"]):
                    path = released_file(manifest_path.parent, record["path"])
                    if not path.is_file() or path.stat().st_size > ArtifactStore.MAX_ARCHIVE_BYTES or digest(path.read_bytes()) != record["sha256"]:
                        raise ModelReleaseError("回滚版本文件 hash 不匹配")
            db.execute("INSERT INTO active VALUES (?,?) ON CONFLICT(runner) DO UPDATE SET release_id=excluded.release_id",
                       (release["runnerId"], release["releaseId"]))
            result = {"operationId": operation_id, "receiptStatus": "completed", "status": "published",
                      "action": action, "releaseId": release["releaseId"], "runnerId": release["runnerId"],
                      "manifestSha256": release["manifestSha256"], "modelHash": release["modelHash"],
                      "previousReleaseId": arguments["expectedActiveReleaseId"], "started": False}
            db.execute("INSERT INTO operations VALUES (?,?,?)", (operation_id, serialized, encode(result)))
            db.commit()
            return result

    def receipt(self, operation_id):
        with self.connect() as db:
            row = db.execute("SELECT result FROM operations WHERE id=?", (operation_id,)).fetchone()
            if row is None:
                raise ModelReleaseError("模型发布回执不存在")
            return json.loads(row[0])

    def manifest_path(self, release_id):
        release = self.get(release_id)
        return self.root / "releases" / release["manifestSha256"] / "manifest.json"
