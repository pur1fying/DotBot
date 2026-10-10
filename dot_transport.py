"""Account-bound desktop dot protocol; no model Responses API or new dot creation."""
import asyncio
import http.client
import json
import re
import socket
import ssl
import time

from dot_auth import DotError
from proxy_outbound import CloudflareResolver, ProxyPinnedHTTPSConnection, PROXY

CLOUD_HOST = 'codex-cloud-backend.chatgpt.com'
IDENTITY_HOST = 'chatgpt.com'
MAX_RESPONSE = 16 * 1024 * 1024


class DotHTTP:
    """Only read identity metadata; an API token never reaches another host."""
    def __init__(self, factory=ProxyPinnedHTTPSConnection, resolver=None):
        self.factory = factory
        self.resolver = resolver or CloudflareResolver(query_host=IDENTITY_HOST)

    def get(self, path, authorization):
        if not re.fullmatch(r'/tbo(?:/primary|/by-thread/[A-Za-z0-9_-]+|/[A-Za-z0-9_-]+/threads)?(?:\?[A-Za-z0-9_%=&.-]*)?', path):
            raise DotError('dot_identity_path_forbidden')
        addresses = self.resolver(IDENTITY_HOST, 443, type=socket.SOCK_STREAM)
        conn = self.factory(IDENTITY_HOST, addresses[0][4][0], 15, allowed_hosts=(IDENTITY_HOST,))
        try:
            headers = authorization.headers()
            headers.update({'Accept': 'application/json', 'User-Agent': 'DotBot/0.5.0',
                            'originator': 'DotBot', 'x-openai-web-frontend': 'codex_webview'})
            conn.request('GET', '/backend-api' + path, headers=headers)
            response = conn.getresponse()
            raw = response.read(MAX_RESPONSE + 1)
            if response.status != 200:
                if response.getheader('cf-mitigated') == 'challenge':
                    raise DotError('dot_browser_verification_required', status=response.status)
                raise DotError('dot_identity_http_failed', status=response.status)
            if len(raw) > MAX_RESPONSE:
                raise DotError('dot_response_too_large')
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise DotError('dot_identity_invalid_response')
            return value
        except DotError:
            raise
        except Exception:
            raise DotError('dot_identity_network_failed') from None
        finally:
            conn.close()


def connect_tunnel(address):
    # Return an unencrypted numeric CONNECT socket. websockets performs TLS
    # with CLOUD_HOST as SNI and certificate name; no second DNS lookup.
    helper = http.client.HTTPConnection(*PROXY, timeout=15)
    helper.set_tunnel(address, 443)
    try:
        helper.connect()
        sock = helper.sock
        helper.sock = None
        sock.setblocking(False)
        return sock
    except Exception:
        raise DotError('dot_proxy_connect_failed') from None
    finally:
        helper.close()


class DotClient:
    def __init__(self, authorization, *, resolver=None, connect=None, rpc_timeout=30):
        self.authorization = authorization
        self.resolver = resolver or CloudflareResolver(query_host=CLOUD_HOST)
        self.connect_factory = connect
        self.rpc_timeout = rpc_timeout
        self.socket = self.reader = None
        self.pending = {}
        self.events = asyncio.Queue(maxsize=2048)
        self.sequence = 0
        self.connected_at = None
        self.input_thread = None

    async def open(self):
        from websockets.asyncio.client import connect
        from websockets.exceptions import InvalidStatus
        sock = None
        try:
            addresses = await asyncio.to_thread(self.resolver, CLOUD_HOST, 443, type=socket.SOCK_STREAM)
            sock = await asyncio.to_thread(connect_tunnel, addresses[0][4][0])
            self.socket = await (self.connect_factory or connect)(
                'wss://' + CLOUD_HOST + '/', sock=sock, proxy=None,
                ssl=ssl.create_default_context(), server_hostname=CLOUD_HOST,
                subprotocols=['codex-app-server', 'codex-client.desktop',
                              'openai-bearer.' + self.authorization.token],
                additional_headers={'ChatGPT-Account-Id': self.authorization.identity.account_id,
                                    'X-OpenAI-Product-Sku': 'codex'},
                user_agent_header='DotBot/0.5.0', compression=None,
                open_timeout=15, ping_interval=20, ping_timeout=30,
                close_timeout=5, max_size=MAX_RESPONSE)
            sock = None
            self.connected_at = time.monotonic()
            self.reader = asyncio.create_task(self._read())
            result = await self.request('initialize', {
                'clientInfo': {'name': 'dotbot_bridge', 'title': 'DotBot', 'version': '0.5.0'},
                'capabilities': {'experimentalApi': True}})
            await self.socket.send(json.dumps({'method': 'initialized', 'params': {}}))
            return result
        except InvalidStatus as error:
            await self.close()
            raise DotError('dot_handshake_rejected', status=error.response.status_code) from None
        except DotError:
            await self.close()
            raise
        except Exception:
            await self.close()
            raise DotError('dot_connect_failed') from None
        finally:
            if sock is not None:
                sock.close()

    async def _read(self):
        category = 'dot_connection_closed'
        try:
            async for raw in self.socket:
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise DotError('dot_invalid_frame')
                future = self.pending.get(message.get('id'))
                if future is not None and ('result' in message or 'error' in message):
                    if not future.done():
                        future.set_result(message)
                elif 'method' in message:
                    self.events.put_nowait(message)
        except DotError as error:
            category = error.category
        except asyncio.QueueFull:
            category = 'dot_notification_backlog'
        except asyncio.CancelledError:
            pass
        except Exception:
            category = 'dot_connection_closed'
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(DotError(category))
            if not self.events.full():
                self.events.put_nowait({'method': '_connection_closed', 'params': {'category': category}})

    async def request(self, method, params, *, timeout=None, on_sent=None):
        if self.socket is None:
            raise DotError('dot_not_connected')
        if method in ('turn/start', 'turn/steer', 'turn/addUserMessage'):
            if self.input_thread is None or params.get('threadId') != self.input_thread:
                raise DotError('dot_direct_input_unavailable')
        self.sequence += 1
        rid = 'dotbot-' + str(self.sequence)
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        try:
            await self.socket.send(json.dumps({'id': rid, 'method': method, 'params': params},
                                              ensure_ascii=False, separators=(',', ':')))
            if on_sent is not None:
                on_sent()
            message = await asyncio.wait_for(future, timeout or self.rpc_timeout)
            if 'error' in message:
                code = message['error'].get('code') if isinstance(message['error'], dict) else None
                raise DotError('dot_rpc_rejected', code=code if type(code) is int else None)
            return message.get('result')
        except DotError:
            raise
        except TimeoutError:
            raise DotError('dot_rpc_timeout') from None
        except Exception:
            raise DotError('dot_connection_closed') from None
        finally:
            self.pending.pop(rid, None)

    async def bind_thread(self, thread_id):
        self.input_thread = None
        result = await self.request('thread/read', {'threadId': thread_id, 'includeTurns': False})
        thread = result.get('thread', {}) if isinstance(result, dict) else {}
        if thread.get('id') != thread_id or thread.get('threadSource') != 'aeon':
            raise DotError('dot_thread_identity_mismatch')
        if thread.get('canAcceptDirectInput') is not True:
            raise DotError('dot_direct_input_unavailable')
        await self.request('thread/resume', {'threadId': thread_id, 'excludeTurns': True})
        self.input_thread = thread_id
        return thread

    async def close(self):
        if self.socket is not None:
            await self.socket.close()
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        self.socket = self.reader = None
        self.input_thread = None
