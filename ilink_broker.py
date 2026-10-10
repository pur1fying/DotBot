"""Owner-only Unix socket broker. Only this user-started process loads bot token.

No stdout logging or credential/context in IPC responses. Optional TTS uses a
fixed local worker with text on stdin and no inherited credential environment.
The socket is a same-OS-user trust boundary, not a multi-user authorization API.
"""
import json
import os
from pathlib import Path
import socket
import socketserver
import stat
import struct
import threading
import time
import argparse
from dot_auth import DotError
from ilink_adapter import Binding, TextAdapter, AdapterError, API_HOST, validate_read_arguments
from ilink_transport import ILinkHTTPS
from outbound import CallbackError
from wechat_media import MAX_FRAME, ERROR_CATEGORIES, MediaError, CDNHTTPS

PRIVATE = Path(__file__).parent / 'runtime' / 'wechat-private'
SOCKET = PRIVATE / 'broker.sock'
CHANNEL = 'wechat-owner'


def check_private(directory):
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise AdapterError('unsafe_private_directory')


def load_binding(directory):
    check_private(directory)
    fd = os.open(directory / 'binding.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise AdapterError('unsafe_binding_file')
        raw = stream.read(16385)
    if len(raw) > 16384:
        raise AdapterError('invalid_binding_file')
    data = json.loads(raw)
    if set(data) != {'bot_id', 'user_id', 'token', 'api_host'} or data['api_host'] != API_HOST:
        raise AdapterError('invalid_binding_file')
    values = [data[k] for k in ('bot_id', 'user_id', 'token')]
    if not all(isinstance(v, str) and 0 < len(v) <= 4096 and not any(ord(c) < 32 or ord(c) == 127 for c in v) for v in values):
        raise AdapterError('invalid_binding_file')
    return Binding(*values)


class Broker:
    def __init__(self, directory, binding, exchange, clock=time.time, *, media_enabled=False, media_transport=None, silk_decoder=None, tts=None):
        self.directory, self.binding, self.clock = directory, binding, clock
        self.lock = threading.Lock()
        self.changed = threading.Condition()
        self.status = {'bound': True, 'owner_bound': True, 'poll_state': 'starting'}
        self.media_enabled, self.media_transport, self.silk_decoder = media_enabled, media_transport, silk_decoder
        self.tts = tts
        if media_enabled:
            from local_tts import LocalTTS
            self.tts = tts if tts is not None else LocalTTS()
            self.status.update({'media_enabled': True, 'native_voice': False, 'silk_decoder_enabled': silk_decoder is not None,
                                'local_tts_configured': True})
        def checked_exchange(path, headers, body):
            try:
                result = exchange(path, headers, body)
                self.status.pop('error_category', None)
                self.status.pop('http_status', None)
                for field in ('ret', 'errcode'):
                    if type(result.get(field)) is int:
                        self.status['api_' + field] = result[field]
                    else:
                        self.status.pop('api_' + field, None)
                return result
            except CallbackError as error:
                categories = {'api_http_failed', 'api_exchange_failed', 'redirect_forbidden', 'doh_http_failed', 'doh_invalid_response', 'doh_invalid_answer', 'doh_question_mismatch', 'doh_query_forbidden', 'doh_bootstrap_forbidden', 'doh_network_failed', 'doh_no_address', 'non_public_address', 'proxy_connect_failed', 'tls_certificate_failed', 'tls_failed', 'timeout', 'response_too_large', 'invalid_response', 'endpoint_forbidden', 'host_not_allowlisted'}
                self.status['error_category'] = str(error) if str(error) in categories else 'transport_failed'
                if type(error.http_status) is int and 100 <= error.http_status <= 599:
                    self.status['http_status'] = error.http_status
                raise
        self.exchange = checked_exchange
        # Initialize private DB once before worker threads; reopen per operation.
        self.adapter(recover=True).close()

    def adapter(self, recover=False):
        klass, options = TextAdapter, {}
        if self.media_enabled:
            from media_adapter import MediaAdapter
            klass, options = MediaAdapter, {'media_transport': self.media_transport, 'silk_decoder': self.silk_decoder, 'tts': self.tts}
        return klass(self.directory / 'messages.sqlite', self.binding,
                     test_exchange=self.exchange, clock=self.clock, recover_sending=recover, **options)

    def call(self, request):
        if not isinstance(request, dict) or set(request) != {'method', 'arguments'} or not isinstance(request['arguments'], dict):
            raise AdapterError('invalid_broker_request')
        method, args = request['method'], request['arguments']
        if method == 'status' and not args:
            return dict(self.status)
        if method == 'build' and not args:
            return {'broker_build': 'dot-direct-20261010', 'optional_ret_with_valid_id': True,
                    'per_reply_diagnostics': True, 'media_enabled': self.media_enabled, 'native_voice': False}
        if method == 'wait_inbox' and set(args) == {'after_seq', 'timeout_ms'}:
            if type(args['after_seq']) is not int or args['after_seq'] < 0 or type(args['timeout_ms']) is not int or not 0 <= args['timeout_ms'] <= 25000:
                raise AdapterError('invalid_wait_arguments')
            return self.wait_inbox(args['after_seq'], args['timeout_ms'])
        if method.startswith('dot_'):
            with self.lock:
                adapter = self.adapter()
                try:
                    return self.dot_call(adapter, method, args)
                finally:
                    adapter.close()
                    with self.changed:
                        self.changed.notify_all()
        if method in ('read', 'read_pending', 'read_relay'):
            validate_read_arguments(args, pending_only=method == 'read_pending')
            with self.lock:
                adapter = self.adapter()
                try:
                    result = adapter.read_page(**args, pending_only=method == 'read_pending')
                    if method == 'read_relay':
                        result['messages'] = [m for m in result['messages']
                                              if adapter.dot.route(m['message_id']) == 'webhook']
                    return {'channel_id': CHANNEL, **result}
                finally:
                    adapter.close()
        if method == 'reply' and set(args) == {'message_id', 'text', 'idempotency_key'}:
            # Serialize send operations, preserving durable ambiguity semantics.
            with self.lock:
                adapter = self.adapter()
                try:
                    if self.status['poll_state'] == 'session_paused':
                        raise AdapterError('session_paused')
                    return adapter.reply(**args)
                finally:
                    adapter.close()
        fields = {'read_media': ({'message_id', 'media_id'},),
                  'reply_media': ({'message_id', 'data_base64', 'mime_type', 'idempotency_key', 'delivery_mode'}, {'message_id', 'media_id', 'mime_type', 'idempotency_key', 'delivery_mode'}),
                  'forget_media': ({'message_id'},), 'stage_media': ({'message_id', 'data_base64', 'mime_type'},),
                  'generate_audio': ({'message_id', 'text', 'idempotency_key'},)}
        if self.media_enabled and method in fields and set(args) in fields[method]:
            if any(not isinstance(v, str) or not v for v in args.values()):
                raise MediaError('media_too_large_or_invalid')
            with self.lock:
                adapter = self.adapter()
                try:
                    if method == 'reply_media' and self.status['poll_state'] == 'session_paused':
                        raise AdapterError('session_paused')
                    return getattr(adapter, method)(**args)
                finally:
                    adapter.close()
        raise AdapterError('broker_method_forbidden')

    def wait_inbox(self, after_seq, timeout_ms):
        deadline = time.monotonic() + timeout_ms / 1000
        with self.changed:
            while True:
                adapter = self.adapter()
                try:
                    seq = adapter.db.execute('SELECT COALESCE(MAX(rowid),0) FROM messages').fetchone()[0]
                finally:
                    adapter.close()
                remaining = deadline - time.monotonic()
                if seq > after_seq or remaining <= 0:
                    return {'local_seq': seq}
                # No send lock or SQLite transaction is held while waiting.
                self.changed.wait(remaining)

    def dot_call(self, adapter, method, args):
        store = adapter.dot
        operations = {
            'dot_status': (set(), store.status),
            'dot_binding': (set(), lambda: store.binding),
            'dot_bind': ({'binding'}, store.bind),
            'dot_activate': (set(), store.activate),
            'dot_rollback': (set(), store.rollback),
            'dot_next': (set(), store.next_job),
            'dot_submitting': ({'message_id'}, store.submitting),
            'dot_submitted': ({'message_id', 'turn_id', 'written_at', 'error'}, store.submitted),
            'dot_mark': ({'message_id', 'state', 'category'}, store.mark),
            'dot_item': ({'thread_id', 'item', 'completed'}, store.accept_item),
        }
        if method in operations:
            fields, operation = operations[method]
            if set(args) != fields:
                raise DotError('dot_invalid_arguments')
            return operation(**args)
        if method == 'dot_drain_one' and not args:
            if self.status['poll_state'] == 'session_paused':
                raise DotError('wechat_session_paused')
            part = store.next_part()
            if part is None:
                return None
            store.begin_part(part['item_id'], part['part'])
            try:
                result = adapter.reply(part['message_id'], part['text'], part['idempotency_key'], _route='direct')
                state = result['state']
            except Exception:
                state = 'unknown'
            store.finish_part(part['item_id'], part['part'], state)
            return {key: part[key] for key in ('message_id', 'item_id', 'part')} | {'state': state}
        if method == 'dot_reconcile_jobs' and not args:
            store.expire()
            rows = adapter.db.execute("SELECT message_id FROM dot_jobs WHERE state NOT IN ('expired','rolled_back','failed') ORDER BY received_at").fetchall()
            return {'jobs': [store.job(row[0]) for row in rows]}
        raise DotError('dot_method_forbidden')

    def poll_once(self):
        # Do not hold send lock during 35-second long poll; adapter has its own
        # SQLite connection. Writes are SQLite transactions, never shared cursor.
        adapter = self.adapter()
        try:
            result = adapter.poll()
            self.status['poll_state'] = result.get('state', 'ready')
            if result.get('accepted', 0):
                with self.changed:
                    self.changed.notify_all()
            return result
        finally:
            adapter.close()


def write_status(directory, status):
    # Only fixed non-sensitive values reach this optional local health file.
    temp = directory / 'broker-status.tmp'
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(status, stream)
    os.replace(temp, directory / 'broker-status.json')


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(50)
        _, uid, _ = struct.unpack('3i', self.request.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if uid != os.getuid():
            return
        try:
            line = self.rfile.readline(MAX_FRAME + 1)
            if len(line) > MAX_FRAME or not line.endswith(b'\n'):
                raise AdapterError('request_too_large')
            result = self.server.broker.call(json.loads(line))
            output = {'ok': True, 'result': result}
        except Exception as e:
            # Never serialize request, server response, exception or traceback.
            category = e.category if isinstance(e, DotError) else (str(e) if isinstance(e, MediaError) and str(e) in ERROR_CATEGORIES else 'broker_operation_rejected')
            output = {'ok': False, 'error': category}
        self.wfile.write(json.dumps(output, ensure_ascii=False).encode() + b'\n')


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    def handle_error(self, request, client_address):
        pass  # socketserver default prints tracebacks; prohibited here.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--media', action='store_true', help='Requires approval to restart broker and enable Tencent CDN media I/O')
    parser.add_argument('--silk-node', help='Absolute Node executable for optional local silk-wasm decoder; no STT/TTS')
    args = parser.parse_args()
    if args.silk_node and not args.media:
        parser.error('--silk-node requires --media')
    os.umask(0o077)
    # Atomic single instance via flock; no inspection of another process/env.
    import fcntl
    check_private(PRIVATE)
    lockfd = os.open(PRIVATE / 'broker.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    binding = load_binding(PRIVATE)
    decoder = None
    if args.silk_node:
        from silk_decoder import SilkDecoder
        decoder = SilkDecoder(args.silk_node)
    broker = Broker(PRIVATE, binding, ILinkHTTPS().exchange,
                    media_enabled=args.media, media_transport=CDNHTTPS(enabled=True) if args.media else None, silk_decoder=decoder)
    SOCKET.unlink(missing_ok=True)
    server = Server(str(SOCKET), Handler)
    os.chmod(SOCKET, 0o600)
    server.broker = broker
    stop = threading.Event()
    def poller():
        empty_delay = 0.1
        while not stop.is_set():
            try:
                started = time.monotonic()
                result = broker.poll_once()
                if result.get('state') == 'session_paused':
                    delay = 60
                elif result.get('accepted', 0) or time.monotonic() - started >= 1:
                    delay, empty_delay = 0, 0.1
                else:
                    delay, empty_delay = empty_delay, min(1, empty_delay * 2)
            except Exception as error:
                broker.status['poll_state'] = 'poll_failed'
                if 'error_category' not in broker.status:
                    safe = {'poll_rejected', 'invalid_batch', 'message_id_conflict', 'exchange_failed', 'invalid_response'}
                    broker.status['error_category'] = str(error) if isinstance(error, AdapterError) and str(error) in safe else 'local_poll_failed'
                delay = 10
            try:
                write_status(PRIVATE, broker.status)
            except Exception:
                pass
            stop.wait(delay)
    threading.Thread(target=poller, daemon=True).start()
    write_status(PRIVATE, broker.status)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        stop.set()
        server.server_close()
        SOCKET.unlink(missing_ok=True)
        os.close(lockfd)


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        # Constant output only; startup failures cannot reveal credentials.
        print('Broker stopped. Check private permissions / binding / existing instance.')
