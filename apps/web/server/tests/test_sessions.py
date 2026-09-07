import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from cryptography.fernet import Fernet
from sessions import SessionStore


class SessionContinuityTests(unittest.TestCase):
    def test_encrypted_session_survives_restart_but_not_logout_or_role_change(self):
        with TemporaryDirectory() as directory:
            root=Path(directory);auth=root/'auth.json'
            auth.write_text(json.dumps({'users':[{'username':'owner','role':'admin'}]}))
            cipher=Fernet(Fernet.generate_key())
            store=SessionStore(root,cipher,auth,'https://example.test')
            token='t'*43
            session={'csrf':'c'*43,'public':True,'operator':'owner','role':'admin','expires':time.time()+3600,'lastSeen':time.time()}
            store.save({token:session},True)
            self.assertNotIn(token.encode(),store.path.read_bytes())
            self.assertEqual(store.path.stat().st_mode & 0o777,0o600)
            self.assertEqual(store.load(),{token:session})
            self.assertEqual(SessionStore(root,cipher,auth,'https://other.test').load(),{})
            auth.write_text(json.dumps({'users':[{'username':'owner','role':'viewer'}]}))
            self.assertEqual(store.load(),{})
            auth.write_text(json.dumps({'users':[{'username':'owner','role':'admin'}]}))
            store.save({},True)
            self.assertEqual(store.load(),{})

    def test_idle_expired_and_loopback_sessions_are_not_restored(self):
        with TemporaryDirectory() as directory:
            root=Path(directory);auth=root/'auth.json';auth.write_text('{"username":"owner","role":"admin"}')
            store=SessionStore(root,Fernet(Fernet.generate_key()),auth,'https://example.test')
            row={'csrf':'c'*43,'public':True,'operator':'owner','role':'admin','expires':time.time()+10,'lastSeen':time.time()}
            for value in ({**row,'expires':time.time()-1},{**row,'lastSeen':time.time()-1801},{**row,'public':False}):
                store.save({'t'*43:value},True)
                self.assertEqual(store.load(),{})
