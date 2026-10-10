"""Codex login adapter. Tokens stay in memory and never enter diagnostics."""
import asyncio
import base64
from dataclasses import dataclass, field
import json
import math
import os
import time


class DotError(Exception):
    """Only fixed categories may be shown outside the private runtime."""
    def __init__(self, category, *, status=None, code=None):
        self.category, self.status, self.code = category, status, code
        super().__init__(category)


@dataclass(frozen=True)
class Identity:
    account_id: str
    user_id: str


@dataclass(frozen=True)
class Authorization:
    identity: Identity
    expires_at: float
    token: str = field(repr=False)

    def headers(self):
        return {'Authorization': 'Bearer ' + self.token,
                'ChatGPT-Account-Id': self.identity.account_id,
                'X-OpenAI-Product-Sku': 'codex'}


def decode_authorization(result):
    if not isinstance(result, dict) or result.get('authMethod') not in ('chatgpt', 'chatgptAuthTokens'):
        raise DotError('chatgpt_login_required')
    token = result.get('authToken')
    if not isinstance(token, str) or not token or len(token) > 32768:
        raise DotError('auth_token_unavailable')
    try:
        encoded = token.split('.')[1]
        claims = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
        auth = claims['https://api.openai.com/auth']
        account = auth.get('chatgpt_account_id') or auth.get('account_id')
        user = auth.get('user_id') or auth.get('chatgpt_user_id')
        expiry = claims['exp']
        if not all(isinstance(v, str) and 0 < len(v) <= 256 for v in (account, user)):
            raise ValueError()
        if type(expiry) not in (int, float) or not math.isfinite(expiry) or expiry <= 0:
            raise ValueError()
    except (ValueError, KeyError, IndexError, TypeError):
        raise DotError('auth_identity_unavailable') from None
    # Claim parsing selects the local owner; the server still authenticates JWT.
    return Authorization(Identity(account, user), expiry, token)


class CodexAuth:
    def __init__(self, executable='/usr/lib/chatgpt/resources/codex', *, expected=None,
                 now=time.time, request_timeout=30):
        self.executable, self.expected = executable, expected
        self.now, self.request_timeout = now, request_timeout
        self.process = self.reader = None
        self.pending = {}
        self.sequence = 0
        self.cached = None
        self.lock = asyncio.Lock()

    async def _start(self):
        if self.process is not None and self.process.returncode is None:
            return
        await self._stop()
        # Preserve login/keyring context, not model-provider API keys or tunnel credentials.
        names = ('PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'DISPLAY',
                 'DBUS_SESSION_BUS_ADDRESS', 'XDG_RUNTIME_DIR', 'CODEX_HOME',
                 'SSL_CERT_FILE', 'SSL_CERT_DIR')
        env = {name: os.environ[name] for name in names if name in os.environ}
        try:
            self.process = await asyncio.create_subprocess_exec(
                self.executable, 'app-server', '--listen', 'stdio://',
                '-c', 'model_provider="openai"', env=env,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, limit=1048576)
        except OSError:
            raise DotError('auth_helper_start_failed') from None
        self.reader = asyncio.create_task(self._read())
        await self._rpc('initialize', {
            'clientInfo': {'name': 'dotbot_bridge', 'title': 'DotBot login helper', 'version': '0.5.0'},
            'capabilities': {'experimentalApi': True}})
        self.process.stdin.write(b'{"method":"initialized","params":{}}\n')
        await self.process.stdin.drain()

    async def _read(self):
        try:
            while raw := await self.process.stdout.readline():
                data = json.loads(raw)
                if not isinstance(data, dict):
                    continue
                future = self.pending.get(data.get('id'))
                if future is not None and not future.done() and ('result' in data or 'error' in data):
                    future.set_result(data)
        except (ValueError, OSError, asyncio.CancelledError):
            pass
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(DotError('auth_helper_disconnected'))

    async def _rpc(self, method, params):
        self.sequence += 1
        rid = 'dot-auth-' + str(self.sequence)
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        try:
            raw = json.dumps({'id': rid, 'method': method, 'params': params}).encode() + b'\n'
            self.process.stdin.write(raw)
            await self.process.stdin.drain()
            response = await asyncio.wait_for(future, self.request_timeout)
            if 'error' in response:
                raise DotError('auth_helper_rpc_failed')
            return response.get('result')
        except TimeoutError:
            raise DotError('auth_helper_timeout') from None
        except (OSError, ConnectionError):
            raise DotError('auth_helper_disconnected') from None
        finally:
            self.pending.pop(rid, None)

    async def get(self, *, refresh=False):
        async with self.lock:
            # The native helper already caches credentials. Ask it on each use
            # so an account change cannot be hidden by our own long-lived cache.
            await self._start()
            result = await self._rpc('getAuthStatus', {'includeToken': True, 'refreshToken': refresh})
            candidate = decode_authorization(result)
            if candidate.expires_at - self.now() <= 300 and not refresh:
                candidate = decode_authorization(await self._rpc(
                    'getAuthStatus', {'includeToken': True, 'refreshToken': True}))
            if candidate.expires_at <= self.now():
                self.cached = None
                raise DotError('auth_expired')
            owner = self.expected or (self.cached.identity if self.cached is not None else None)
            if owner is not None and candidate.identity != owner:
                self.cached = None
                raise DotError('auth_account_changed')
            self.expected = owner or candidate.identity
            self.cached = candidate
            return candidate

    async def _stop(self):
        if self.process is not None and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        self.process = self.reader = None

    async def close(self):
        async with self.lock:
            self.cached = None
            await self._stop()
