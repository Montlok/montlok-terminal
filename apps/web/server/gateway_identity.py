"""Verify the authenticated Rust gateway on the private loopback BFF link."""
from __future__ import annotations
import base64
import hashlib
import hmac
import json
import time
from pathlib import Path


class GatewayIdentity:
    def __init__(self, secret_path: Path, authority_path: Path):
        self.secret = secret_path.read_bytes().strip()
        if len(self.secret) < 32:
            raise ValueError('Gateway signing key requires at least 32 bytes')
        self.authority_path = authority_path
        self.nonces: dict[str, float] = {}

    def verify(self, method: str, path: str, headers, body: bytes, peer: str) -> dict:
        if peer not in {'127.0.0.1', '::1'}:
            raise ValueError('Gateway requests use the local connection')
        timestamp = headers.get('X-Montlok-Bridge-Time', '')
        nonce = headers.get('X-Montlok-Bridge-Nonce', '')
        principal = headers.get('X-Montlok-Bridge-Principal', '')
        signature = headers.get('X-Montlok-Bridge-Signature', '')
        if len(timestamp) > 16 or not 16 <= len(nonce) <= 96 or len(principal) > 1024:
            raise ValueError('Invalid gateway signature context')
        now = time.time()
        if abs(now - int(timestamp)) > 30:
            raise ValueError('Gateway signature expired')
        material = '\n'.join((method, path, timestamp, nonce, principal, hashlib.sha256(body).hexdigest()))
        expected = hmac.new(self.secret, material.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError('Gateway signature is invalid')
        self.nonces = {key: expires for key, expires in self.nonces.items() if expires > now}
        if nonce in self.nonces or len(self.nonces) > 10000:
            raise ValueError('Gateway request nonce was already used')
        user = json.loads(base64.b64decode(principal, validate=True))
        authority = json.loads(self.authority_path.read_text())
        current = next((item for item in authority.get('users', [authority]) if item.get('username') == user.get('id')), None)
        if current is None or user.get('role') not in {'admin', 'viewer', 'operator'}:
            raise ValueError('Gateway principal is unavailable')
        role = 'viewer' if 'viewer' in {current.get('role', 'admin'), user['role']} else current.get('role', 'admin')
        self.nonces[nonce] = now + 60
        identifier = hmac.new(self.secret, ('gateway-session:' + user['id']).encode(), hashlib.sha256).hexdigest()
        return {'id': identifier, 'operator': user['id'], 'role': role,
                'csrf': identifier, 'expires': now + 60, 'lastSeen': now, 'public': False}
