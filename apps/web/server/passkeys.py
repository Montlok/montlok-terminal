"""Discoverable WebAuthn credentials and one-use, operator-issued enrollment links."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlsplit

from webauthn import (
    generate_authentication_options,
    generate_registration_options,
    options_to_json,
    verify_authentication_response,
    verify_registration_response,
)
from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.exceptions import WebAuthnException
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    PublicKeyCredentialDescriptor,
    PublicKeyCredentialHint,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)

from profiles import atomic_private


class Passkeys:
    def __init__(self, path: Path, identity: Path, origin: str, *, initialize=False):
        parsed = urlsplit(origin)
        if parsed.scheme != "https" or not parsed.hostname or parsed.netloc != parsed.hostname or parsed.path or parsed.query or parsed.fragment:
            raise ValueError("Passkey origin must be an exact HTTPS origin without a port or path")
        self.origin, self.rp_id, self.identity = origin, parsed.hostname, identity
        if initialize:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        # A missing/replaced database must never silently restore password access.
        self.database = sqlite3.connect(f"file:{path}?mode=rw", uri=True)
        self.database.row_factory = sqlite3.Row
        if initialize:
            self.database.executescript("""
                CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE users (username TEXT PRIMARY KEY, handle BLOB UNIQUE NOT NULL);
                CREATE TABLE credentials (id BLOB PRIMARY KEY, username TEXT NOT NULL,
                    public_key BLOB NOT NULL, sign_count INTEGER NOT NULL, device_type TEXT NOT NULL,
                    backed_up INTEGER NOT NULL, created REAL NOT NULL, last_used REAL);
                CREATE TABLE enrollments (digest TEXT PRIMARY KEY, username TEXT NOT NULL,
                    expires REAL NOT NULL);
            """)
            with self.database:
                self.database.executemany("INSERT INTO settings VALUES (?,?)", [
                    ("origin", origin), ("password_login_disabled", "false"),
                ])
        settings = dict(self.database.execute("SELECT key,value FROM settings"))
        if settings.get("origin") != origin or settings.get("password_login_disabled") not in ("true", "false"):
            raise ValueError("Passkey configuration is incomplete or belongs to a different origin")
        self.pending = {}
        self.attempts = {}
        self.total = []

    @property
    def password_disabled(self):
        row = self.database.execute("SELECT value FROM settings WHERE key='password_login_disabled'").fetchone()
        if row is None or row[0] not in ("true", "false"):
            raise ValueError("Passkey authentication state is invalid")
        return row[0] == "true"

    def user(self, username):
        document = json.loads(self.identity.read_text())
        user = next((item for item in document.get("users", [document]) if item["username"] == username), None)
        if not user or user.get("role", "admin") not in ("admin", "viewer") or user.get("disabled"):
            raise ValueError("账户不可用")
        return {"username": username, "role": user.get("role", "admin")}

    def throttle(self, peer):
        now = time.monotonic()
        self.attempts = {key: [stamp for stamp in stamps if now - stamp < 300]
                         for key, stamps in self.attempts.items() if stamps and now - stamps[-1] < 300}
        self.total = [stamp for stamp in self.total if now - stamp < 300]
        stamps = self.attempts.setdefault(peer, [])
        if len(stamps) >= 20 or len(self.total) >= 100:
            raise OverflowError("请求过多，请 5 分钟后重试")
        stamps.append(now)
        self.total.append(now)

    def issue_enrollment(self, username):
        self.user(username)
        token = secrets.token_urlsafe(32)
        with self.database:
            self.database.execute("DELETE FROM enrollments WHERE username=? OR expires<?", (username, time.time()))
            self.database.execute("INSERT INTO enrollments VALUES (?,?,?)",
                                  (hashlib.sha256(token.encode()).hexdigest(), username, time.time() + 900))
            self.database.execute("INSERT OR IGNORE INTO users VALUES (?,?)", (username, secrets.token_bytes(32)))
        return token

    def enrollment(self, digest):
        row = self.database.execute("SELECT * FROM enrollments WHERE digest=? AND expires>?", (digest, time.time())).fetchone()
        if not row:
            raise ValueError("设置链接已失效，请重新获取")
        self.user(row["username"])
        return row["username"]

    def challenge(self, kind, **details):
        now = time.monotonic()
        self.pending = {key: item for key, item in self.pending.items() if item["expires"] > now}
        if len(self.pending) >= 256:
            raise OverflowError("登录请求繁忙，请稍后重试")
        flow = secrets.token_urlsafe(32)
        record = {"kind": kind, "challenge": secrets.token_bytes(32), "expires": now + 300, **details}
        self.pending[flow] = record
        return flow, record

    def consume(self, flow, kind):
        record = self.pending.pop(flow, None)
        if not record or record["kind"] != kind or record["expires"] <= time.monotonic():
            raise ValueError("验证已过期，请重试")
        return record

    def registration_options(self, token):
        if not isinstance(token, str) or not 32 <= len(token) <= 128:
            raise ValueError("设置链接已失效，请重新获取")
        digest = hashlib.sha256(token.encode()).hexdigest()
        username = self.enrollment(digest)
        handle = self.database.execute("SELECT handle FROM users WHERE username=?", (username,)).fetchone()[0]
        credentials = self.database.execute("SELECT id FROM credentials WHERE username=?", (username,)).fetchall()
        flow, record = self.challenge("register", username=username, digest=digest)
        options = generate_registration_options(
            rp_id=self.rp_id, rp_name="Montlok", user_name=username, user_id=handle,
            challenge=record["challenge"], timeout=120000,
            authenticator_selection=AuthenticatorSelectionCriteria(
                resident_key=ResidentKeyRequirement.REQUIRED, require_resident_key=True,
                user_verification=UserVerificationRequirement.REQUIRED),
            exclude_credentials=[PublicKeyCredentialDescriptor(id=row[0]) for row in credentials],
            hints=[PublicKeyCredentialHint.CLIENT_DEVICE],
        )
        return flow, json.loads(options_to_json(options))

    def register(self, flow, credential):
        record = self.consume(flow, "register")
        self.enrollment(record["digest"])
        try:
            verified = verify_registration_response(
                credential=credential, expected_challenge=record["challenge"], expected_rp_id=self.rp_id,
                expected_origin=self.origin, require_user_presence=True, require_user_verification=True,
            )
        except (WebAuthnException, ValueError, TypeError) as error:
            raise ValueError("通行密钥验证失败，请重试") from error
        with self.database:
            # Delete and insert in the same transaction: concurrent completions cannot reuse a link.
            count = self.database.execute("DELETE FROM enrollments WHERE digest=? AND expires>?",
                                          (record["digest"], time.time())).rowcount
            if count != 1:
                raise ValueError("设置链接已失效，请重新获取")
            try:
                self.database.execute("INSERT INTO credentials VALUES (?,?,?,?,?,?,?,NULL)", (
                    verified.credential_id, record["username"], verified.credential_public_key,
                    verified.sign_count, verified.credential_device_type.value,
                    int(verified.credential_backed_up), time.time(),
                ))
            except sqlite3.IntegrityError as error:
                raise ValueError("此通行密钥已经存在") from error

    def authentication_options(self):
        flow, record = self.challenge("authenticate")
        options = generate_authentication_options(rp_id=self.rp_id, challenge=record["challenge"],
                                                 timeout=120000, user_verification=UserVerificationRequirement.REQUIRED)
        return flow, json.loads(options_to_json(options))

    def authenticate(self, flow, credential):
        record = self.consume(flow, "authenticate")
        try:
            credential_id = base64url_to_bytes(credential["id"])
            row = self.database.execute("SELECT c.*,u.handle FROM credentials c JOIN users u USING(username) WHERE c.id=?",
                                        (credential_id,)).fetchone()
            if not row or not secrets.compare_digest(base64url_to_bytes(credential["response"]["userHandle"]), row["handle"]):
                raise ValueError("Unknown credential or user handle")
            user = self.user(row["username"])
            verified = verify_authentication_response(
                credential=credential, expected_challenge=record["challenge"], expected_rp_id=self.rp_id,
                expected_origin=self.origin, credential_public_key=row["public_key"],
                credential_current_sign_count=row["sign_count"], require_user_verification=True,
            )
            if verified.credential_device_type.value != row["device_type"]:
                raise ValueError("Credential backup eligibility changed")
        except (WebAuthnException, ValueError, TypeError, KeyError) as error:
            raise ValueError("通行密钥验证失败，请重试") from error
        activated = user["role"] == "admin" and not self.password_disabled
        with self.database:
            self.database.execute("UPDATE credentials SET sign_count=?,backed_up=?,last_used=? WHERE id=?",
                                  (verified.new_sign_count, int(verified.credential_backed_up), time.time(), credential_id))
            if activated:
                self.database.execute("UPDATE settings SET value='true' WHERE key='password_login_disabled'")
        return user, activated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--origin", default="https://tokyo.montlok.com")
    parser.add_argument("--initialize", action="store_true")
    parser.add_argument("--username")
    parser.add_argument("--handoff", type=Path)
    args = parser.parse_args()
    if bool(args.username) != bool(args.handoff) or (args.handoff and args.handoff.exists()):
        parser.error("Enrollment requires --username and a new --handoff path")
    store = Passkeys(args.database, args.identity, args.origin, initialize=args.initialize)
    if args.username:
        token = store.issue_enrollment(args.username)
        atomic_private(args.handoff, (args.origin + "/login#enroll=" + token + "\n").encode())
        print("One-use enrollment link written to the private handoff file; expires in 15 minutes.")
    print(json.dumps({"passwordLoginDisabled": store.password_disabled,
                      "credentials": store.database.execute("SELECT count(*) FROM credentials").fetchone()[0]}))
    store.database.close()
