"""Verify a decrypted archive from stdin in memory, without writing or printing secrets."""
import hashlib
import io
import json
import re
import sqlite3
import sys
import tarfile
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography import x509

def verify_groups(archive):
    prefix = "group-runtime/"
    entries = [item for item in archive.getmembers() if item.name.startswith(prefix)]
    if not entries:
        return {"groups": 0, "groupRuns": 0}
    names = [item.name for item in entries]
    if len(set(names)) != len(names) or any(not item.isfile() for item in entries):
        raise ValueError("Invalid group recovery entries")
    metadata = json.loads(archive.extractfile(prefix + "recovery.json").read())
    if metadata.get("schemaVersion") != 1 or metadata.get("restorePolicy") != "evidence_only_no_process_restore":
        raise ValueError("Invalid group recovery policy")
    inventory = metadata.get("files", {})
    if set(names) != {prefix + name for name in inventory} | {prefix + "recovery.json"}:
        raise ValueError("Incomplete group recovery archive")
    for name, value in inventory.items():
        if name.startswith("/") or ".." in name.split("/") or name.endswith((".sock", ".pid", "-wal", "-shm")):
            raise ValueError("Unsafe group recovery entry")
        item = archive.getmember(prefix + name)
        if item.size != value.get("bytes") or not re.fullmatch(r"[a-f0-9]{64}", value.get("sha256", "")):
            raise ValueError("Invalid group recovery file metadata")
        with archive.extractfile(item) as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != value["sha256"]:
                raise ValueError("Group recovery content checksum mismatch")
    registry = json.loads(archive.extractfile(prefix + "deploy/registry.json").read())
    groups = registry.get("groups", [])
    if registry.get("version") != 1 or len(groups) != metadata.get("groups"):
        raise ValueError("Invalid group recovery registry")
    required = {"program/engine/group_worker.py", "program/server/group_runtime.py", "program/server/managed_run_view.py",
                "deploy/registry.json", "state/runs.sqlite"}
    for group in groups:
        group_id = group.get("id", "")
        if not isinstance(group_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,47}", group_id):
            raise ValueError("Invalid group recovery identifier")
        for label in ("settings", "signals", "strategy"):
            key = f"releases/{group_id}/{label}"
            entry = inventory.get(key, {})
            if entry.get("sha256") != group.get(label + "Sha256") or entry.get("source") != group.get(label + "Path"):
                raise ValueError("Group recovery release differs from registry")
            required.add(key)
        required.update(f"releases/{group_id}/hardened/{name}" for name in
                        ("settings.py", "run.py", "node_config.py", "health.py", "commands.py", "control.py", "alerts.py"))
    if not required.issubset(inventory):
        raise ValueError("Group recovery program dependency is missing")
    database = sqlite3.connect(":memory:")
    try:
        database.deserialize(archive.extractfile(prefix + "state/runs.sqlite").read())
        if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Invalid group recovery database")
        runs = database.execute("SELECT id,group_id,state,pid,process_created FROM runs").fetchall()
    finally:
        database.close()
    index = {run["runId"]: run for run in metadata.get("runs", [])}
    if len(index) != len(runs) or len(metadata.get("runs", [])) != len(runs):
        raise ValueError("Group recovery run index mismatch")
    for run_id, group_id, state, pid, created in runs:
        evidence = index.get(run_id, {})
        if (pid is not None or created is not None or state not in {"completed", "stopped", "failed", "interrupted"}
                or evidence.get("groupId") != group_id):
            raise ValueError("Group recovery contains runnable process state")
        files = evidence.get("files", [])
        if "request.json" not in files or any(f"runs/{run_id}/{name}" not in inventory for name in files):
            raise ValueError("Group run evidence is incomplete")
        if "control-operations.sqlite" in files:
            journal = sqlite3.connect(":memory:")
            try:
                journal.deserialize(archive.extractfile(prefix + f"runs/{run_id}/control-operations.sqlite").read())
                if journal.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or journal.execute("SELECT 1 FROM operations WHERE status='processing'").fetchone():
                    raise ValueError("Worker journal contains invalid or in-flight recovery operations")
            finally:
                journal.close()
    if metadata.get("dependencies", {}).get("restoreStartsProcesses") is not False:
        raise ValueError("Group recovery cannot start processes")
    model_summary = {}
    if registry.get("modelRuntime"):
        model = registry["modelRuntime"]
        required = {"program/model/" + name for name in (Path(model["workerPath"]).name, "model_release.py", "model_client.py",
            "model_inference_worker.py", "model_strategy.py", "model_market_actor.py", "model_probe.py", "commands.py", "control.py", "guard-binary")}
        required.update({"program/server/model_releases.py", "program/server/artifacts.py", "deploy/model-settings.json"})
        required.update("program/model/runtime/" + name for name in ("settings.py", "node_config.py", "health.py", "alerts.py"))
        if not required.issubset(inventory) or inventory["deploy/model-settings.json"]["sha256"] != model["settingsSha256"]:
            raise ValueError("Model group recovery dependencies are incomplete")
        for key, field in (("program/model/guard-binary", "guardBinary"), ("deploy/model-settings.json", "settingsPath"),
                           ("program/model/" + Path(model["workerPath"]).name, "workerPath")):
            if inventory[key].get("source") != model[field]:
                raise ValueError("Model group recovery source differs from root registry")
        model_summary = verify_models(archive)
        recovery = json.loads(archive.extractfile("state/models/recovery.json").read())
        if recovery["sourceRoot"] != model["storePath"] or metadata.get("models") != model_summary:
            raise ValueError("Model release recovery differs from root group registry")
        model_db = sqlite3.connect(":memory:")
        try:
            model_db.deserialize(archive.extractfile("state/models/releases.sqlite").read())
            manifests = [json.loads(row[0]) for row in model_db.execute("SELECT manifest FROM releases")]
        finally:
            model_db.close()
        for manifest in manifests:
            for record in manifest.get("sources", []):
                relative = record["path"]
                if not relative.startswith("source/") or ".." in relative.split("/"):
                    raise ValueError("Invalid reviewed host runner reference")
                relative = "recent_btc/" + relative[7:] if manifest["runnerId"] == "gru_v1" else relative[7:]
                entry = inventory.get("program/model/runner/" + relative, {})
                if entry.get("sha256") != record["sha256"] or entry.get("source") != str(Path(model["runnerRoot"]) / relative):
                    raise ValueError("Model recovery host runner differs from reviewed release")
    return {"groups": len(groups), "groupRuns": len(runs), **model_summary}


def verify_models(archive):
    prefix = "state/models/"
    entries = [item for item in archive.getmembers() if item.name.startswith(prefix)]
    if not entries:
        return {"modelReleases": 0, "modelBytes": 0}
    names = [item.name for item in entries]
    if len(set(names)) != len(names) or any(not item.isfile() for item in entries):
        raise ValueError("Invalid model recovery archive entries")
    metadata = json.loads(archive.extractfile(prefix + "recovery.json").read())
    inventory = metadata.get("files", {})
    if metadata.get("schemaVersion") != 1 or metadata.get("restoreStartsProcesses") is not False:
        raise ValueError("Model recovery must not start processes")
    if set(names) != {prefix + name for name in inventory} | {prefix + "recovery.json"}:
        raise ValueError("Incomplete model recovery archive")
    for name, record in inventory.items():
        if name.startswith("/") or ".." in name.split("/") or name.endswith(("-wal", "-shm", ".sock", ".pid")):
            raise ValueError("Unsafe model recovery path")
        info = archive.getmember(prefix + name)
        if info.size != record.get("bytes"):
            raise ValueError("Model recovery file size mismatch")
        with archive.extractfile(info) as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != record.get("sha256"):
                raise ValueError("Model recovery file hash mismatch")
    database = sqlite3.connect(":memory:")
    database.row_factory = sqlite3.Row
    try:
        database.deserialize(archive.extractfile(prefix + "releases.sqlite").read())
        if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Invalid model recovery database")
        releases = database.execute("SELECT * FROM releases").fetchall()
        active = database.execute("SELECT runner,release_id FROM active").fetchall()
    finally:
        database.close()
    known = {row["id"]: row["runner"] for row in releases}
    if any(known.get(row["release_id"]) != row["runner"] for row in active):
        raise ValueError("Model recovery active version is missing")
    required = {"releases.sqlite"}
    total = 0
    for row in releases:
        checksum = row["manifest_hash"]
        if not re.fullmatch(r"[a-f0-9]{64}", checksum):
            raise ValueError("Invalid model recovery manifest hash")
        release_prefix = "releases/" + checksum + "/"
        name = release_prefix + "manifest.json"
        raw = archive.extractfile(prefix + name).read()
        manifest = json.loads(raw)
        if hashlib.sha256(raw).hexdigest() != checksum or manifest != json.loads(row["manifest"]) or manifest["releaseId"] != row["id"]:
            raise ValueError("Model recovery manifest disagrees with publication index")
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
        for relative, digest in expected.items():
            if inventory.get(release_prefix + relative, {}).get("sha256") != digest:
                raise ValueError("Model recovery weight/source hash differs from manifest")
        version_names = {name for name in inventory if name.startswith(release_prefix)}
        size = sum(inventory[name]["bytes"] for name in version_names)
        if size != row["bytes"]:
            raise ValueError("Model recovery is missing immutable metadata")
        total += size
        required.update(version_names)
    if required != set(inventory) or len(releases) != metadata.get("modelReleases") or total != metadata.get("modelBytes"):
        raise ValueError("Model recovery inventory mismatch")
    return {"modelReleases": len(releases), "modelBytes": total}

def verify_artifacts(archive):
    prefix = "state/artifacts/"
    entries = [item for item in archive.getmembers() if item.name.startswith(prefix)]
    if not entries:
        return {"artifacts": 0, "artifactBytes": 0}
    names = [item.name for item in entries]
    if len(names) != len(set(names)) or any(not item.isfile() for item in entries):
        raise ValueError("Invalid artifact recovery entries")
    database = sqlite3.connect(":memory:")
    try:
        database.deserialize(archive.extractfile(prefix + "catalog.sqlite").read())
        if database.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Invalid artifact recovery database")
        rows = database.execute("SELECT sha256, bytes FROM artifacts").fetchall()
    finally:
        database.close()
    objects = {}
    for digest, size in rows:
        if (not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)
                or not isinstance(size, int) or size <= 0 or digest in objects and objects[digest] != size):
            raise ValueError("Invalid artifact recovery metadata")
        objects[digest] = size
    expected = {prefix + "catalog.sqlite", *(prefix + "objects/" + digest for digest in objects)}
    if set(names) != expected:
        raise ValueError("Incomplete artifact recovery archive")
    for digest, size in objects.items():
        item = archive.getmember(prefix + "objects/" + digest)
        if item.size != size:
            raise ValueError("Artifact recovery size mismatch")
        with archive.extractfile(item) as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                raise ValueError("Artifact recovery hash mismatch")
    return {"artifacts": len(rows), "artifactBytes": sum(objects.values())}

with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()), mode="r:gz") as archive:
    def content(name):
        return archive.extractfile(name).read()

    state = json.loads(Fernet(content("state/master.key")).decrypt(content("state/profiles.enc")))
    assert state["active"] in state["profiles"]
    assert state["profiles"][state["active"]]["mode"] in ("demo", "live_readonly")
    identity = json.loads(content("identity/login.json"))
    assert all(user["algorithm"] == "scrypt-n131072-r8-p1" and len(user["hash"]) == 64 for user in identity.get("users", [identity]))
    database = sqlite3.connect(":memory:")
    database.deserialize(content("state/operations.sqlite"))
    assert database.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert b"ssl_certificate" in content("deploy/nginx.conf")
    assert b"ssl_verify_client on" in content("deploy/nginx.conf")
    x509.load_pem_x509_certificate(content("deploy/origin-ca.crt"))
    database.deserialize(content("state/passkeys.sqlite"))
    assert database.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    settings = dict(database.execute("SELECT key,value FROM settings"))
    assert settings["origin"] == "https://tokyo.montlok.com"
    assert settings["password_login_disabled"] in ("true", "false")
    artifact_summary = verify_artifacts(archive)
    group_summary = verify_groups(archive)
    model_summary = verify_models(archive) if "modelReleases" not in group_summary else {}
    print("Recovery verified: vault, identity, operations/passkeys SQLite integrity, password migration state, Nginx mTLS, public origin CA and artifact content hashes.")
    print(json.dumps({"credentials": database.execute("SELECT count(*) FROM credentials").fetchone()[0],
                      "passwordLoginDisabled": settings["password_login_disabled"] == "true", **artifact_summary, **group_summary, **model_summary}))
    database.close()
