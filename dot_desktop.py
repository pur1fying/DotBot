"""Call the installed desktop's room client over a loopback debugging session.

No independent HTTP sender, exported login token, app archive patch, automatic
restart, debugger enabling, or fallback to legacy thread input. The operator
must start the desktop with its debugging endpoint enabled before connecting.
"""
import asyncio
import contextlib
import http.client
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from dot_auth import DotError

SHARED_BUNDLE = 'app-shared-9422db8faa3a.js'
ROOM_BUNDLE = 'app-initial-47b07e923fcc.js'


def desktop_targets(port):
    if type(port) is not int or not 1024 <= port <= 65535:
        raise DotError('desktop_invalid_debug_port')
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    try:
        connection.request('GET', '/json/list')
        response = connection.getresponse()
        body = response.read(1048577)
        if response.status != 200 or len(body) > 1048576:
            raise DotError('desktop_debug_unavailable')
        targets = json.loads(body)
        if not isinstance(targets, list):
            raise DotError('desktop_debug_unavailable')
        return targets
    except (OSError, ValueError, http.client.HTTPException):
        raise DotError('desktop_debug_unavailable') from None
    finally:
        connection.close()


def select_target(targets, port, target_id=None):
    result = []
    for target in targets:
        if not isinstance(target, dict) or target.get('type') != 'page':
            continue
        url = urlsplit(target.get('url', ''))
        # Only the packaged app renderer, never an embedded site or MCP widget.
        route = parse_qs(url.query).get('initialRoute', [''])[0]
        if (url.scheme != 'app' or url.hostname not in ('-', '.') or url.path != '/index.html' or
                route in ('/avatar-overlay', '/detached-window')):
            continue
        endpoint = urlsplit(target.get('webSocketDebuggerUrl', ''))
        if (endpoint.scheme != 'ws' or endpoint.hostname != '127.0.0.1' or
                endpoint.port != port or endpoint.username or endpoint.password or
                not endpoint.path.startswith('/devtools/page/') or endpoint.query or endpoint.fragment):
            continue
        if target_id is None or target.get('id') == target_id:
            result.append(target)
    if len(result) != 1:
        raise DotError('desktop_target_ambiguous' if result else 'desktop_window_unavailable')
    return result[0]


class DesktopRoomClient:
    def __init__(self, *, port=9229, target_id=None):
        self.port, self.target_id = port, target_id
        self.socket = self.reader = self.object_id = None
        self.pending = {}
        self.sequence = 0

    async def open(self, binding):
        from websockets.asyncio.client import connect
        target = select_target(await asyncio.to_thread(desktop_targets, self.port),
                               self.port, self.target_id)
        try:
            self.socket = await connect(target['webSocketDebuggerUrl'], proxy=None,
                                        open_timeout=3, max_size=8 * 1024 * 1024)
            self.reader = asyncio.create_task(self._read())
            source = Path(__file__).with_name('desktop_room_bridge.mjs').read_text()
            source = source.replace('export async function ', 'async function ', 1)
            # Fixed local code, no arbitrary function/string supplied by a queued message.
            expression = '''(async () => {
              if (window.codexWindowType !== 'electron' || !window.electronBridge)
                throw new Error('desktop_window_unavailable');
              const entry = [...document.scripts].map(s => s.src).find(src => {
                const u = new URL(src, location.href);
                return u.origin === location.origin && u.pathname.includes('/assets/');
              });
              if (!entry) throw new Error('desktop_bundle_changed');
              const base = new URL('.', entry);
              const shared = await import(new URL(SHARED_NAME, base).href);
              const native = await import(new URL(ROOM_NAME, base).href);
              BRIDGE_SOURCE
              return createDesktopRoomBridge(shared, native, BINDING);
            })()'''.replace('SHARED_NAME', json.dumps(SHARED_BUNDLE)).replace(
                'ROOM_NAME', json.dumps(ROOM_BUNDLE)).replace('BRIDGE_SOURCE', source).replace(
                'BINDING', json.dumps(binding, ensure_ascii=True))
            result = await self._request('Runtime.evaluate', {
                'expression': expression, 'awaitPromise': True,
                'returnByValue': False, 'objectGroup': 'dotbot-room-bridge'}, timeout=60)
            if result.get('exceptionDetails') or not result.get('result', {}).get('objectId'):
                raise DotError('desktop_bridge_initialization_failed')
            self.object_id = result['result']['objectId']
            return await self.invoke('status')
        except BaseException:
            await self.close()
            raise

    async def _read(self):
        try:
            async for raw in self.socket:
                result = json.loads(raw)
                future = self.pending.get(result.get('id'))
                if future is not None and not future.done():
                    if 'error' in result:
                        future.set_exception(DotError('desktop_rpc_failed'))
                    else:
                        future.set_result(result.get('result', {}))
        except Exception:
            pass
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(DotError('desktop_disconnected'))

    async def _request(self, method, params, *, timeout=35):
        self.sequence += 1
        request_id = self.sequence
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            await self.socket.send(json.dumps({'id': request_id, 'method': method, 'params': params}))
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            raise DotError('desktop_request_timeout') from None
        finally:
            self.pending.pop(request_id, None)

    async def invoke(self, operation, arguments=None):
        if operation not in ('status', 'messages', 'send', 'wait', 'upload_image', 'download_image', 'check_image_file'):
            raise DotError('desktop_operation_not_allowed')
        if self.object_id is None:
            raise DotError('desktop_bridge_closed')
        try:
            result = await self._request('Runtime.callFunctionOn', {
                'objectId': self.object_id,
                'functionDeclaration': 'function(op, args) { return this.invoke(op, args); }',
                'arguments': [{'value': operation}, {'value': arguments or {}}],
                'awaitPromise': True, 'returnByValue': True}, timeout=60 if operation in ('send', 'upload_image', 'download_image', 'check_image_file') else 35)
            if result.get('exceptionDetails'):
                raise DotError('desktop_room_request_failed')
            response = result.get('result', {}).get('value', {})
            if not response.get('ok'):
                category = response.get('error', 'desktop_room_request_failed')
                if not isinstance(category, str) or len(category) > 80:
                    category = 'desktop_room_request_failed'
                error = DotError(category, status=response.get('http_status'))
                error.submission_started = response.get('submission_started')
                raise error
            return response['value']
        except BaseException as error:
            # Once a renderer send is dispatched, a timeout/disconnection cannot
            # establish whether the upstream request was accepted. Never retry.
            if operation == 'send' and (not isinstance(error, DotError) or error.category in (
                    'desktop_request_timeout', 'desktop_disconnected', 'desktop_rpc_failed')):
                raise DotError('dot_submission_unknown') from None
            if operation == 'upload_image' and (not isinstance(error, DotError) or error.category in (
                    'desktop_request_timeout', 'desktop_disconnected', 'desktop_rpc_failed')):
                raise DotError('dot_image_upload_unknown') from None
            raise

    async def close(self):
        if self.object_id and self.socket:
            with contextlib.suppress(Exception):
                await self._request('Runtime.callFunctionOn', {
                    'objectId': self.object_id, 'functionDeclaration': 'function() { this.dispose(); }',
                    'returnByValue': True}, timeout=2)
                await self._request('Runtime.releaseObjectGroup', {
                    'objectGroup': 'dotbot-room-bridge'}, timeout=2)
            self.object_id = None
        if self.socket:
            await self.socket.close()
            self.socket = None
        if self.reader:
            self.reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reader
            self.reader = None
