"""Server-owned, encrypted connection profiles. No credential leaves this module unmasked."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from pathlib import Path

from cryptography.fernet import Fernet

SITES = {"global": "https://www.okx.com", "eea": "https://eea.okx.com", "us": "https://us.okx.com", "tr": "https://tr.okx.com"}


def atomic_private(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class Profiles:
    def __init__(self, root: Path, bootstrap: Path | None) -> None:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root, 0o700)
        self.root = root
        key_path = root / "master.key"
        if not key_path.exists():
            atomic_private(key_path, Fernet.generate_key())
        self.cipher = Fernet(key_path.read_bytes())
        self.path = root / "profiles.enc"
        if self.path.exists():
            self.state = json.loads(self.cipher.decrypt(self.path.read_bytes()))
        else:
            self.state = {"active": None, "profiles": {}, "epoch": 0}
            if bootstrap and bootstrap.exists():
                values = dict(line.split("=", 1) for line in bootstrap.read_text().splitlines() if line)
                self.save({"id": "tokyo-demo", "name": "东京 · OKX Demo", "mode": "demo", "site": "global",
                           "apiKey": values["OKX_DEMO_API_KEY"], "secret": values["OKX_DEMO_API_SECRET"],
                           "passphrase": values["OKX_DEMO_API_PASSPHRASE"]})
                self.select("tokyo-demo")

    def persist(self) -> None:
        self.state["epoch"] += 1
        atomic_private(self.path, self.cipher.encrypt(json.dumps(self.state).encode()))

    @property
    def epoch(self) -> int:
        return self.state["epoch"]

    def get(self, identifier: str | None = None) -> dict:
        identifier = identifier or self.state["active"]
        if identifier not in self.state["profiles"]:
            raise ValueError("尚未选择可用的 API 连接")
        return dict(self.state["profiles"][identifier])

    def public(self) -> list[dict]:
        return [{"id": p["id"], "name": p["name"], "mode": p["mode"], "site": p["site"],
                 "keyMask": p["apiKey"][:4] + "…" + p["apiKey"][-4:],
                 "active": p["id"] == self.state["active"], "updatedAt": p["updatedAt"],
                 "verification": p.get("verification")}
                for p in self.state["profiles"].values()]

    def validate(self, draft: dict) -> dict:
        identifier = str(draft.get("id", ""))
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", identifier):
            raise ValueError("连接 ID 只允许 1–40 个字母、数字、下划线或短横线")
        previous = self.state["profiles"].get(identifier, {})
        value = {key: draft.get(key) or previous.get(key) for key in ("id", "name", "mode", "site", "apiKey", "secret", "passphrase")}
        if value["mode"] not in ("demo", "live_readonly") or value["site"] not in SITES:
            raise ValueError("仅支持 OKX Demo 或实盘只读，以及已知 OKX 站点")
        if any(not isinstance(value[key], str) or not value[key].strip() for key in value):
            raise ValueError("名称、Key、Secret 和 Passphrase 均必填")
        if any(len(value[key]) > 512 for key in value):
            raise ValueError("连接字段过长")
        return value

    def save(self, draft: dict, verification: dict | None = None) -> dict:
        value = self.validate(draft)
        value.update(updatedAt=time.time(), verification=verification)
        self.state["profiles"][value["id"]] = value
        self.persist()
        return {"saved": value["id"], "activated": self.state["active"] == value["id"]}

    def select(self, identifier: str) -> dict:
        self.get(identifier)
        self.state["active"] = identifier
        self.persist()
        return {"active": identifier}

    def delete(self, identifier: str) -> dict:
        if identifier == self.state["active"]:
            raise ValueError("请先切换到另一连接，再删除当前连接")
        self.state["profiles"].pop(identifier)
        self.persist()
        return {"removedLocalProfile": identifier, "exchangeKeyRevoked": False}
