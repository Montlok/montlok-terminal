"""Password verification for the operator, independent of exchange credentials."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import secrets
import time
from pathlib import Path

from profiles import atomic_private


def password_hash(password: str, salt: bytes) -> str:
    return hashlib.scrypt(password.encode(), salt=salt, n=131072, r=8, p=1,
                          maxmem=256 * 1024 * 1024, dklen=32).hex()


class Authentication:
    def __init__(self, path: Path):
        self.path = path
        self.identity = json.loads(path.read_text())
        self.attempts: dict[str, list[float]] = {}
        self.total: list[float] = []
        self.workers = asyncio.Semaphore(2)

    async def verify(self, peer: str, username: str, password: str) -> dict | None:
        now = time.monotonic()
        self.attempts = {key: [at for at in values if now - at < 300]
                         for key, values in self.attempts.items() if values and now - values[-1] < 300}
        self.total = [at for at in self.total if now - at < 300]
        attempts = self.attempts.setdefault(peer, [])
        if len(attempts) >= 5 or len(self.total) >= 30:
            raise OverflowError("登录尝试过多，请 5 分钟后重试")
        attempts.append(now)
        self.total.append(now)
        if not isinstance(username, str) or not isinstance(password, str) or len(password) > 512:
            return None
        document = json.loads(self.path.read_text())
        users = document.get("users", [document])
        identity = next((user for user in users if user["username"] == username), users[0])
        async with self.workers:
            candidate = await asyncio.to_thread(password_hash, password, bytes.fromhex(identity["salt"]))
        accepted = secrets.compare_digest(candidate, identity["hash"])
        accepted &= secrets.compare_digest(username.encode(), identity["username"].encode())
        if accepted:
            self.attempts.pop(peer, None)
        return {"username": identity["username"], "role": identity.get("role", "admin")} if accepted else None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--username", default="gabira")
    parser.add_argument("--role", choices=("admin", "viewer"), default="admin")
    parser.add_argument("--add-user", action="store_true")
    args = parser.parse_args()
    if (args.output.exists() and not args.add_user) or args.handoff.exists():
        raise SystemExit("Refusing to overwrite existing credentials")
    users = []
    if args.output.exists():
        existing = json.loads(args.output.read_text())
        users = existing.get("users", [existing])
    if any(user["username"] == args.username for user in users):
        raise SystemExit("User already exists; no credentials changed")
    password, salt = secrets.token_urlsafe(24), secrets.token_bytes(16)
    users.append({"username": args.username, "salt": salt.hex(), "hash": password_hash(password, salt),
                  "algorithm": "scrypt-n131072-r8-p1", "role": args.role})
    atomic_private(args.output, json.dumps({"users": users}).encode())
    atomic_private(args.handoff, ("https://tokyo.montlok.com\nUsername: " + args.username + "\nPassword: " + password + "\n").encode())
    print("Operator password hash and private handoff file created; password not logged.")
