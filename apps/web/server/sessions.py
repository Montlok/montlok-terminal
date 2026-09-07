"""Encrypted web-session continuity across a routine application restart."""
import hashlib
import json
import time

from profiles import atomic_private


class SessionStore:
    def __init__(self, root, cipher, auth_file, origin):
        self.path = root/'sessions.enc'
        self.cipher = cipher
        self.auth_file = auth_file
        self.origin = origin
        self.last_save = 0

    def authority(self):
        if not self.auth_file:
            return None, {}
        raw = self.auth_file.read_bytes()
        document = json.loads(raw)
        return hashlib.sha256(raw).hexdigest(), {row['username']:row.get('role','admin') for row in document.get('users',[document])}

    def valid(self, sessions, roles):
        now = time.time()
        return {token:row for token,row in sessions.items() if isinstance(token,str) and 32<=len(token)<=128
            and isinstance(row,dict) and isinstance(row.get('csrf'),str) and len(row['csrf'])>=32
            and row.get('public') is True and row.get('role') in {'admin','viewer'}
            and roles.get(row.get('operator'))==row.get('role')
            and type(row.get('expires')) in (int,float) and now<row['expires']<=now+8*3600
            and type(row.get('lastSeen')) in (int,float) and 0<=now-row['lastSeen']<=1800}

    def load(self):
        if not self.auth_file or not self.path.exists():
            return {}
        try:
            if self.path.is_symlink() or self.path.stat().st_size>2*1024*1024:
                return {}
            data=json.loads(self.cipher.decrypt(self.path.read_bytes()))
            authority,roles=self.authority()
            if data.get('version')!=1 or data.get('authority')!=authority or data.get('origin')!=self.origin:
                return {}
            return self.valid(data['sessions'],roles)
        except Exception:
            return {}

    def save(self, sessions, force=False):
        if not self.auth_file or not force and time.monotonic()-self.last_save<30:
            return
        authority,roles=self.authority()
        valid=self.valid(sessions,roles)
        atomic_private(self.path,self.cipher.encrypt(json.dumps({'version':1,'authority':authority,
            'origin':self.origin,'sessions':valid}).encode()))
        self.last_save=time.monotonic()
