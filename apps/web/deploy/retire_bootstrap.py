"""Retire the duplicate plaintext Demo bootstrap only after checking the encrypted vault."""
import hmac
import json
from pathlib import Path

from cryptography.fernet import Fernet

root = Path("/www/nautilus/operator")
bootstrap = Path("/www/nautilus/credentials/tokyo-demo.env")
if bootstrap.exists():
    state = json.loads(Fernet((root / "state/master.key").read_bytes()).decrypt((root / "state/profiles.enc").read_bytes()))
    values = dict(line.split("=", 1) for line in bootstrap.read_text().splitlines() if line)
    profile = state["profiles"]["tokyo-demo"]
    for field, variable in (("apiKey", "OKX_DEMO_API_KEY"), ("secret", "OKX_DEMO_API_SECRET"), ("passphrase", "OKX_DEMO_API_PASSPHRASE")):
        assert hmac.compare_digest(profile[field], values[variable]), "Vault/bootstrap mismatch; original retained"
    assert list((root / "backups").glob("operator-*.tar.gz.age")), "Encrypted recovery archive missing"
    bootstrap.unlink()
    print("Retired duplicate plaintext bootstrap; identical credentials remain in the tested encrypted vault/backup.")
else:
    print("No plaintext bootstrap remains.")
