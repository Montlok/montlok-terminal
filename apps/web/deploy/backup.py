"""Create an age-encrypted operator recovery archive; private identity stays off-server."""
import datetime
import hashlib
import io
import json
import os
import re
import sqlite3
import stat
import subprocess
import tarfile
from pathlib import Path

os.umask(0o077)
root = Path("/www/nautilus/operator")
destination = root / "backups"
destination.mkdir(mode=0o700, exist_ok=True)
stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
output = destination / f"operator-{stamp}.tar.gz.age"
pending = output.with_suffix(".pending")
recipient = (root / "identity/backup-recipient.txt").read_text().strip()

def add_artifacts(archive, root):
    artifact_root = root / "state/artifacts"
    catalog = artifact_root / "catalog.sqlite"
    if artifact_root.is_symlink() or catalog.is_symlink() or (artifact_root / "objects").is_symlink():
        raise RuntimeError("Invalid artifact recovery path")
    if not artifact_root.exists():
        return 0
    if not catalog.is_file():
        raise RuntimeError("Missing artifact recovery index")
    # Snapshot the index first; only its referenced, immutable blobs are included.
    source = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
    database = sqlite3.connect(":memory:")
    try:
        source.backup(database)
        if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("Invalid artifact recovery database")
        content = database.serialize()
        objects = database.execute("SELECT sha256, MAX(bytes) FROM artifacts GROUP BY sha256").fetchall()
    finally:
        source.close()
        database.close()
    item = tarfile.TarInfo("state/artifacts/catalog.sqlite")
    item.size, item.mode = len(content), 0o600
    archive.addfile(item, io.BytesIO(content))
    for digest, size in objects:
        if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest) or not isinstance(size, int) or size <= 0:
            raise RuntimeError("Invalid artifact recovery metadata")
        descriptor = os.open(artifact_root / "objects" / digest, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size != size or hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                raise RuntimeError("Artifact recovery content mismatch")
            stream.seek(0)
            item = tarfile.TarInfo("state/artifacts/objects/" + digest)
            item.size, item.mode = size, 0o400
            archive.addfile(item, stream)
    return len(objects)

def database_snapshot(name):
    source = sqlite3.connect(f"file:{root}/state/{name}?mode=ro", uri=True)
    snapshot = sqlite3.connect(":memory:")
    try:
        source.backup(snapshot)
        if snapshot.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError(f"Invalid recovery database: {name}")
        return snapshot.serialize()
    finally:
        source.close()
        snapshot.close()


def add_models(archive, model_root):
    """Consistent published index plus every immutable weight/metadata file."""
    model_root = Path(model_root)
    database_path = model_root / "releases.sqlite"
    if not model_root.exists():
        return {"modelReleases": 0, "modelBytes": 0}
    if model_root.is_symlink() or database_path.is_symlink() or not database_path.is_file() or (model_root / "releases").is_symlink():
        raise RuntimeError("Invalid model release recovery path")
    source = sqlite3.connect(database_path.as_uri() + "?mode=ro", uri=True)
    database = sqlite3.connect(":memory:")
    database.row_factory = sqlite3.Row
    try:
        source.backup(database)
        if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("Invalid model release recovery index")
        rows = database.execute("SELECT * FROM releases ORDER BY created").fetchall()
        active = database.execute("SELECT runner,release_id FROM active").fetchall()
        content = database.serialize()
        content = content[:18] + b"\x01\x01" + content[20:]
    finally:
        source.close()
        database.close()
    known = {row["id"]: row["runner"] for row in rows}
    if len(rows) > 256 or any(known.get(row["release_id"]) != row["runner"] for row in active):
        raise RuntimeError("Invalid active model release recovery record")
    prefix = "state/models/"
    inventory = {}
    def add_bytes(name, data):
        info = tarfile.TarInfo(prefix + name)
        info.size, info.mode = len(data), 0o600
        archive.addfile(info, io.BytesIO(data))
        inventory[name] = {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    add_bytes("releases.sqlite", content)
    total = 0
    for row in rows:
        checksum = row["manifest_hash"]
        if not isinstance(checksum, str) or not re.fullmatch(r"[a-f0-9]{64}", checksum):
            raise RuntimeError("Invalid model release hash")
        directory = model_root / "releases" / checksum
        if directory.is_symlink() or not directory.is_dir():
            raise RuntimeError("Missing immutable model release directory")
        manifest_path = directory / "manifest.json"
        if manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 4 * 1024 * 1024:
            raise RuntimeError("Invalid model release manifest")
        manifest_bytes = manifest_path.read_bytes()
        if hashlib.sha256(manifest_bytes).hexdigest() != checksum or json.loads(manifest_bytes) != json.loads(row["manifest"]):
            raise RuntimeError("Model release manifest hash mismatch")
        manifest = json.loads(manifest_bytes)
        if manifest.get("releaseId") != row["id"] or manifest.get("runnerId") != row["runner"]:
            raise RuntimeError("Model release identity differs from recovery index")
        expected = {}
        def records(value):
            if isinstance(value, dict):
                if "path" in value and "sha256" in value:
                    expected[value["path"]] = value["sha256"]
                else:
                    for child in value.values():
                        records(child)
            elif isinstance(value, list):
                for child in value:
                    records(child)
        records(manifest)
        found, release_bytes = set(), 0
        for path in sorted(directory.rglob("*")):
            if path.is_symlink():
                raise RuntimeError("Model release recovery cannot include symlinks")
            if path.is_dir():
                continue
            name = str(path.relative_to(directory))
            if not path.is_file() or name.startswith("/") or any(part in {"", ".", ".."} for part in name.split("/")):
                raise RuntimeError("Invalid model release recovery file")
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode):
                    raise RuntimeError("Model release recovery requires regular files")
                checksum_file = hashlib.file_digest(stream, "sha256").hexdigest()
                if name in expected and checksum_file != expected[name]:
                    raise RuntimeError("Model release weight/source hash mismatch")
                stream.seek(0)
                item_name = "releases/" + checksum + "/" + name
                item = tarfile.TarInfo(prefix + item_name)
                item.size, item.mode = info.st_size, 0o400
                archive.addfile(item, stream)
                inventory[item_name] = {"sha256": checksum_file, "bytes": info.st_size}
                found.add(name)
                release_bytes += info.st_size
        if not set(expected).issubset(found) or release_bytes != row["bytes"]:
            raise RuntimeError("Model release recovery missing weights or metadata")
        total += release_bytes
    metadata = {"schemaVersion": 1, "sourceRoot": str(model_root), "restoreStartsProcesses": False,
                "modelReleases": len(rows), "modelBytes": total, "files": inventory}
    item = tarfile.TarInfo(prefix + "recovery.json")
    content = json.dumps(metadata, sort_keys=True).encode()
    item.size, item.mode = len(content), 0o600
    archive.addfile(item, io.BytesIO(content))
    return {"modelReleases": len(rows), "modelBytes": total}

def add_groups(archive, registry_path=Path("/etc/montlok-groups/registry.json"), runs_root=Path("/www/nautilus/group-runs")):
    """Archive reviewed group code and run evidence, not runnable process state."""
    if not registry_path.exists():
        if runs_root.exists():
            raise RuntimeError("Missing group recovery registry")
        return {"groups": 0, "groupRuns": 0}
    prefix = "group-runtime/"
    inventory = {}

    def add_bytes(name, content, source=None):
        if name in inventory:
            return
        item = tarfile.TarInfo(prefix + name)
        item.size, item.mode = len(content), 0o600
        archive.addfile(item, io.BytesIO(content))
        inventory[name] = {"sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content), "source": source}

    def add_regular(path, name, expected=None, return_content=False):
        if not path.is_absolute() or path.is_symlink():
            raise RuntimeError("Invalid group recovery reference")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise RuntimeError("Non-regular group recovery source")
            # Freeze append-only file length and stream it into age; no whole
            # log/curve is retained in the backup process's memory.
            if return_content:
                if info.st_size > 2 * 1024 * 1024:
                    raise RuntimeError("Group recovery registry is too large")
                content = stream.read(info.st_size)
                if len(content) != info.st_size:
                    raise RuntimeError("Group recovery source was truncated")
                add_bytes(name, content, str(path))
                return content
            if name in inventory:
                return
            hasher = hashlib.sha256()

            class CheckedReader:
                def read(self, size):
                    value = stream.read(size)
                    hasher.update(value)
                    return value

            item = tarfile.TarInfo(prefix + name)
            item.size, item.mode = info.st_size, 0o600
            archive.addfile(item, CheckedReader())
            digest = hasher.hexdigest()
            if expected is not None and digest != expected:
                raise RuntimeError("Group recovery release hash mismatch")
            inventory[name] = {"sha256": digest, "bytes": info.st_size, "source": str(path)}

    registry = json.loads(add_regular(registry_path, "deploy/registry.json", return_content=True))
    if not isinstance(registry, dict) or registry.get("version") != 1:
        raise RuntimeError("Invalid group recovery registry")
    worker = Path(registry["workerPath"])
    add_regular(worker, "program/engine/group_worker.py")
    for filename in ("group_runtime.py", "managed_run_view.py"):
        add_regular(worker.parent.parent / "server" / filename, "program/server/" + filename)
    for filename in ("model_releases.py", "artifacts.py"):
        dependency = worker.parent.parent / "server" / filename
        if dependency.is_file():
            add_regular(dependency, "program/server/" + filename)
    model_summary = None
    if registry.get("modelRuntime"):
        model = registry["modelRuntime"]
        model_worker = Path(model["workerPath"])
        for filename in (model_worker.name, "model_release.py", "model_client.py", "model_inference_worker.py",
                         "model_strategy.py", "model_market_actor.py", "model_probe.py", "commands.py", "control.py"):
            add_regular(model_worker.parent / filename, "program/model/" + filename)
        for filename in ("model_releases.py", "artifacts.py"):
            add_regular(model_worker.parent.parent / "server" / filename, "program/server/" + filename)
        for filename in ("settings.py", "node_config.py", "health.py", "alerts.py"):
            add_regular(Path(model["runtimePath"]) / filename, "program/model/runtime/" + filename)
        add_regular(Path(model["guardBinary"]), "program/model/guard-binary")
        add_regular(Path(model["settingsPath"]), "deploy/model-settings.json", model["settingsSha256"])
        model_summary = add_models(archive, Path(model["storePath"]))
        catalog = Path(model["storePath"]) / "releases.sqlite"
        source = sqlite3.connect(catalog.as_uri() + "?mode=ro", uri=True)
        try:
            manifests = [json.loads(row[0]) for row in source.execute("SELECT manifest FROM releases")]
        finally:
            source.close()
        for manifest in manifests:
            for record in manifest.get("sources", []):
                relative = record["path"]
                if not relative.startswith("source/") or ".." in relative.split("/"):
                    raise RuntimeError("Unsafe reviewed model runner source")
                relative = "recent_btc/" + relative[7:] if manifest["runnerId"] == "gru_v1" else relative[7:]
                add_regular(Path(model["runnerRoot"]) / relative, "program/model/runner/" + relative, record["sha256"])
    python_path = Path(registry["pythonPath"])
    dependency_notes = {"pythonPath": str(python_path), "environmentRebuildRequired": True,
                        "requiredPackages": ["nautilus_trader (matching compiled release)", "psutil", "msgspec", "pandas"],
                        "restoreStartsProcesses": False, "installedDistributions": []}
    if registry.get("modelRuntime"):
        dependency_notes["modelRuntime"] = {"pythonPath": registry["modelRuntime"]["pythonPath"],
            "requiredPackages": ["matching nautilus_trader", "torch", "numpy", "pandas", "aiohttp", "msgspec", "psutil"],
            "cudaRdtRequires": ["CUDA BF16", "reviewed official Mamba3 implementation"],
            "nodeCapabilityProbeRequiredAfterRestore": True, "guardExecutablePermissionsRequired": True}
        model_venv_config = Path(registry["modelRuntime"]["pythonPath"]).parent.parent / "pyvenv.cfg"
        if model_venv_config.is_file() and not model_venv_config.is_symlink():
            add_regular(model_venv_config, "deploy/model-python-environment.cfg")
    venv_config = python_path.parent.parent / "pyvenv.cfg"
    if venv_config.is_file() and not venv_config.is_symlink():
        add_regular(venv_config, "deploy/python-environment.cfg")
    for site_packages in (python_path.parent.parent / "lib").glob("python*/site-packages"):
        for package in ("nautilus_trader", "psutil", "msgspec", "pandas"):
            for path in site_packages.glob(package + "-*.dist-info/METADATA"):
                if path.is_symlink():
                    continue
                with path.open("rb") as stream:
                    header = stream.read(64 * 1024).decode("utf-8", errors="replace")
                match = re.search(r"^Version: (.+)$", header, re.MULTILINE)
                if match:
                    dependency_notes["installedDistributions"].append({"name": package, "version": match[1].strip()})
    groups = registry.get("groups", [])
    for spec in groups:
        group_id = spec.get("id")
        if not isinstance(group_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,47}", group_id):
            raise RuntimeError("Invalid group recovery identifier")
        for label in ("settings", "signals", "strategy"):
            expected = spec.get(label + "Sha256")
            if not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}", expected):
                raise RuntimeError("Invalid group release checksum")
            add_regular(Path(spec[label + "Path"]), f"releases/{group_id}/{label}", expected)
        runtime = Path(spec["runtimePath"])
        for filename in ("settings.py", "run.py", "node_config.py", "health.py", "commands.py", "control.py", "alerts.py"):
            add_regular(runtime / filename, f"releases/{group_id}/hardened/{filename}")
    unit = Path("/etc/systemd/system/nautilus-group-supervisor.service")
    if unit.is_file() and not unit.is_symlink():
        add_regular(unit, "deploy/nautilus-group-supervisor.service")
    database_path = runs_root / "runs.sqlite"
    if runs_root.is_symlink() or database_path.is_symlink() or not database_path.is_file():
        raise RuntimeError("Missing or invalid group recovery database")
    source = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    snapshot = sqlite3.connect(":memory:")
    try:
        source.backup(snapshot)
        if snapshot.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("Invalid group recovery database")
        runs = snapshot.execute("SELECT id,group_id,state FROM runs").fetchall()
        # The recovery copy is deliberately non-runnable. Manifest PID values
        # remain historical evidence, never restored ownership instructions.
        snapshot.execute("UPDATE runs SET pid=NULL,process_created=NULL,state=CASE WHEN state IN "
                         "('completed','stopped','failed','interrupted') THEN state ELSE 'interrupted' END")
        snapshot.commit()
        content = snapshot.serialize()
        # SQLite's supported in-memory deserialize workaround for a WAL source:
        # the backup API already merged its pages; only rollback format markers
        # are required in this independent, non-running recovery image.
        # https://www.sqlite.org/c3ref/deserialize.html
        if content[:16] != b"SQLite format 3\0":
            raise RuntimeError("Invalid serialized group recovery database")
        content = content[:18] + b"\x01\x01" + content[20:]
        add_bytes("state/runs.sqlite", content, str(database_path))
    finally:
        source.close()
        snapshot.close()
    run_metadata = []
    filenames = ("request.json", "manifest.json", "status.json", "final.json", "view.json", "equity.jsonl", "events.jsonl",
                 "orders.csv", "fills.csv", "positions.csv", "account.csv", "engine.log", "model-ready.json")
    for run_id, group_id, previous_state in runs:
        if (not isinstance(run_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,80}", run_id)
                or not isinstance(group_id, str) or not run_id.startswith(group_id + "-")):
            raise RuntimeError("Invalid group recovery run id")
        directory = runs_root / run_id
        if directory.is_symlink() or not directory.is_dir():
            raise RuntimeError("Missing group recovery run directory")
        present = []
        for filename in filenames:
            path = directory / filename
            if path.exists():
                add_regular(path, f"runs/{run_id}/{filename}")
                present.append(filename)
        journal = directory / "control-operations.sqlite"
        if journal.exists():
            if journal.is_symlink() or not journal.is_file():
                raise RuntimeError("Invalid worker control journal")
            source = sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True)
            copied = sqlite3.connect(":memory:")
            try:
                source.backup(copied)
                if copied.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("Invalid worker control journal")
                copied.execute("UPDATE operations SET status='unknown' WHERE status='processing'")
                copied.commit()
                content = copied.serialize()
                content = content[:18] + b"\x01\x01" + content[20:]
                add_bytes(f"runs/{run_id}/control-operations.sqlite", content, str(journal))
                present.append("control-operations.sqlite")
            finally:
                source.close()
                copied.close()
        if "request.json" not in present:
            raise RuntimeError("Missing group run request evidence")
        run_metadata.append({"runId": run_id, "groupId": group_id, "stateAtSnapshot": previous_state,
                             "files": present, "liveFilesMayHavePartialTail": previous_state not in {"completed", "stopped", "failed", "interrupted"}})
    metadata = {"schemaVersion": 1, "files": inventory, "runs": run_metadata, "groups": len(groups),
                "dependencies": dependency_notes, "restorePolicy": "evidence_only_no_process_restore",
                **({"models": model_summary} if model_summary is not None else {})}
    content = json.dumps(metadata, sort_keys=True, allow_nan=False).encode()
    item = tarfile.TarInfo(prefix + "recovery.json")
    item.size, item.mode = len(content), 0o600
    archive.addfile(item, io.BytesIO(content))
    return {"groups": len(groups), "groupRuns": len(runs), **(model_summary or {})}

databases = {name: database_snapshot(name) for name in ("operations.sqlite", "passkeys.sqlite")}
process = subprocess.Popen(["/usr/bin/age", "--encrypt", "--recipient", recipient, "--output", str(pending)], stdin=subprocess.PIPE)
try:
    with tarfile.open(fileobj=process.stdin, mode="w|gz") as archive:
        for relative in ("state/master.key", "state/profiles.enc", "identity/login.json"):
            archive.add(root / relative, arcname=relative, recursive=False)
        for name, content in databases.items():
            item = tarfile.TarInfo("state/" + name)
            item.size, item.mode = len(content), 0o600
            archive.addfile(item, io.BytesIO(content))
        add_artifacts(archive, root)
        group_summary = add_groups(archive)
        if "modelReleases" not in group_summary:
            add_models(archive, root / "state/models")
        archive.add("/www/server/panel/vhost/nginx/tokyo.montlok.com.conf", arcname="deploy/nginx.conf", recursive=False)
        archive.add("/etc/systemd/system/nautilus-operator-terminal.service.d/https.conf", arcname="deploy/https.conf", recursive=False)
        for source_path, archive_path in (
            ("/www/nautilus/operator/montlok-origin-ca.crt", "deploy/origin-ca.crt"),
            ("/etc/nginx/conf.d/cloudflare-realip.conf", "deploy/cloudflare-realip.conf"),
            ("/etc/nginx/nginx.conf", "deploy/nginx-main.conf"),
            ("/etc/nginx/conf.d/00-montlok-common.conf", "deploy/nginx-common.conf"),
            ("/etc/init.d/nginx", "deploy/nginx-init"),
            ("/etc/ssh/sshd_config.d/00-montlok-hardening.conf", "deploy/ssh-hardening.conf"),
            ("/etc/ufw/user.rules", "deploy/ufw-user.rules"),
            ("/etc/ufw/user6.rules", "deploy/ufw-user6.rules"),
        ):
            archive.add(source_path, arcname=archive_path, recursive=False)
    process.stdin.close()
    if process.wait() != 0:
        raise RuntimeError("age encryption failed")
    pending.replace(output)
    print(f"Encrypted recovery archive: {output.name} ({output.stat().st_size} bytes)")
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
    pending.unlink(missing_ok=True)
