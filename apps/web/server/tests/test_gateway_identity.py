import base64
import hashlib
import hmac
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from gateway_identity import GatewayIdentity


class GatewayIdentityTests(unittest.TestCase):
    def test_signed_requests_preserve_current_role_and_reject_replay(self):
        with TemporaryDirectory() as directory:
            secret = Path(directory) / 'key'; secret.write_bytes(b'k' * 64)
            authority = Path(directory) / 'users.json'
            authority.write_text(json.dumps({'users': [{'username': 'operator', 'role': 'viewer'}]}))
            verifier = GatewayIdentity(secret, authority)
            timestamp = str(int(time.time())); nonce = 'unique-request-nonce-001'
            principal = base64.b64encode(json.dumps({'id': 'operator', 'role': 'admin'}).encode()).decode()
            body = b'{}'; path = '/api/prepare'
            material = '\n'.join(('POST', path, timestamp, nonce, principal, hashlib.sha256(body).hexdigest()))
            headers = {'X-Montlok-Bridge-Time': timestamp, 'X-Montlok-Bridge-Nonce': nonce,
                       'X-Montlok-Bridge-Principal': principal,
                       'X-Montlok-Bridge-Signature': hmac.new(b'k' * 64, material.encode(), hashlib.sha256).hexdigest()}
            with self.assertRaises(ValueError): verifier.verify('POST', path, headers, b'{"changed":true}', '127.0.0.1')
            identity = verifier.verify('POST', path, headers, body, '127.0.0.1')
            self.assertEqual(identity['role'], 'viewer')
            with self.assertRaises(ValueError): verifier.verify('POST', path, headers, body, '127.0.0.1')
            with self.assertRaises(ValueError): verifier.verify('POST', path, headers, body, '203.0.113.7')
