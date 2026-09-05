"""Create an age-encrypted operator recovery archive; private identity stays off-server."""
import datetime
import io
import os
import sqlite3
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
source = sqlite3.connect(f"file:{root}/state/operations.sqlite?mode=ro", uri=True)
snapshot = sqlite3.connect(":memory:")
source.backup(snapshot)
source.close()
audit = snapshot.serialize()
snapshot.close()
process = subprocess.Popen(["/usr/bin/age", "--encrypt", "--recipient", recipient, "--output", str(pending)], stdin=subprocess.PIPE)
try:
    with tarfile.open(fileobj=process.stdin, mode="w|gz") as archive:
        for relative in ("state/master.key", "state/profiles.enc", "identity/login.json"):
            archive.add(root / relative, arcname=relative, recursive=False)
        item = tarfile.TarInfo("state/operations.sqlite")
        item.size, item.mode = len(audit), 0o600
        archive.addfile(item, io.BytesIO(audit))
        archive.add("/www/server/panel/vhost/nginx/tokyo.montlok.com.conf", arcname="deploy/nginx.conf", recursive=False)
        archive.add("/etc/systemd/system/nautilus-operator-terminal.service.d/https.conf", arcname="deploy/https.conf", recursive=False)
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
