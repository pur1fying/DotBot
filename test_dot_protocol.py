import asyncio
import base64
import json
import tempfile
import time
import unittest
from pathlib import Path

from dot_auth import CodexAuth, DotError, Identity, decode_authorization
from dot_gateway import preflight, desktop_candidate
from dot_transport import DotClient


def login(account='account', user='user'):
    data = {'exp': time.time()+3600, 'https://api.openai.com/auth':
            {'chatgpt_account_id': account, 'user_id': user}}
    token = 'SYNTHETIC.' + base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip('=') + '.SYNTHETIC'
    return {'authMethod': 'chatgpt', 'authToken': token}


def primary():
    return {'selection': {'available': True, 'thread_id': 'thread', 'aeon_id': 'alice'},
            'profile': {'id': 'alice', 'active_root_thread_id': 'thread', 'display_name': 'Alice',
                        'aeon_kind': 'orbit', 'status': 'active', 'is_paused': False,
                        'messaging_room_id': 'room', 'avatar_url': 'NEVER_INCLUDE'}}


class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_change_invalidates_cached_authorization(self):
        class FakeAuth(CodexAuth):
            account = 'account'
            async def _start(self): pass
            async def _rpc(self, method, params): return login(self.account)
        auth = FakeAuth()
        token = await auth.get()
        self.assertNotIn(token.token, repr(token))
        auth.account = 'other'
        with self.assertRaisesRegex(DotError, 'auth_account_changed'): await auth.get()
        self.assertIsNone(auth.cached)

    def test_rejects_api_keys_and_malformed_tokens(self):
        for result in ({'authMethod': 'apikey', 'authToken': 'NEVER_INCLUDE'},
                       {'authMethod': 'chatgpt', 'authToken': 'bad'},
                       {'authMethod': 'agentIdentity', 'authToken': 'NEVER_INCLUDE'}):
            with self.assertRaises(DotError): decode_authorization(result)


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_direct_input_denial_never_resumes_or_submits(self):
        client = DotClient(decode_authorization(login()))
        calls = []
        async def request(method, params, **kw):
            calls.append(method)
            return {'thread': {'id': 'thread', 'threadSource': 'aeon', 'canAcceptDirectInput': False}}
        client.request = request
        with self.assertRaisesRegex(DotError, 'dot_direct_input_unavailable'):
            await client.bind_thread('thread')
        self.assertEqual(calls, ['thread/read'])
        self.assertIsNone(client.input_thread)

    async def test_raw_submit_cannot_bypass_binding_check(self):
        client = DotClient(decode_authorization(login()))
        class Socket:
            async def send(self, *_): raise AssertionError('must not send')
        client.socket = Socket()
        with self.assertRaisesRegex(DotError, 'dot_direct_input_unavailable'):
            await client.request('turn/addUserMessage', {'threadId': 'thread'})

    async def test_preflight_reports_room_requirement_without_sending_a_message(self):
        calls = []
        class Auth:
            async def get(self): return decode_authorization(login())
        class HTTP:
            def get(self, path, auth): return primary()
        class Client:
            def __init__(self, auth): pass
            async def open(self): calls.append('initialize')
            async def request(self, method, params):
                calls.append(method)
                return {'thread': {'id': 'thread', 'threadSource': 'aeon',
                                   'canAcceptDirectInput': False, 'status': {'type': 'idle'}}}
            async def close(self): calls.append('close')
        result = await preflight(Auth(), http=HTTP(), client_factory=Client)
        self.assertFalse(result['ready_to_activate'])
        self.assertFalse(result['application_message_sent'])
        self.assertEqual(result['required_transport'], 'messaging_room')
        self.assertEqual(calls, ['initialize', 'thread/read', 'close'])
        self.assertNotIn('NEVER_INCLUDE', json.dumps(result))

    def test_desktop_metadata_is_scoped_to_current_account_and_minimized(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'state.json'
            path.write_text(json.dumps({'electron-persisted-atom-state':
                {'primary-aeon-selection-v1': {'accountId': 'account', 'userId': 'user', 'response': primary()}},
                'other': 'NEVER_INCLUDE'}))
            result = desktop_candidate(path, Identity('account', 'user'))
            self.assertEqual(result['dot_id'], 'alice')
            self.assertNotIn('NEVER_INCLUDE', json.dumps(result))
            with self.assertRaisesRegex(DotError, 'auth_account_changed'):
                desktop_candidate(path, Identity('other', 'user'))


if __name__ == '__main__': unittest.main()
