"""Verify a decrypted archive from stdin in memory, without writing or printing secrets."""
import io
import json
import sqlite3
import sys
import tarfile

from cryptography.fernet import Fernet

with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()), mode="r:gz") as archive:
    def content(name):
        return archive.extractfile(name).read()

    state = json.loads(Fernet(content("state/master.key")).decrypt(content("state/profiles.enc")))
    assert state["active"] in state["profiles"]
    assert state["profiles"][state["active"]]["mode"] == "demo"
    identity = json.loads(content("identity/login.json"))
    assert all(user["algorithm"] == "scrypt-n131072-r8-p1" and len(user["hash"]) == 64 for user in identity.get("users", [identity]))
    database = sqlite3.connect(":memory:")
    database.deserialize(content("state/operations.sqlite"))
    assert database.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert b"ssl_certificate" in content("deploy/nginx.conf")
    print("Recovery verified: age archive, vault decryption, active Demo profile, login hash, SQLite integrity, Nginx configuration.")
    database.close()
