"""Owner-only Unix socket broker. Only this user-started process loads bot token.

No stdout logging, no subprocesses, no credential/context in IPC responses.
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
from ilink_adapter import Binding, TextAdapter, AdapterError, API_HOST
from ilink_transport import ILinkHTTPS
from outbound import CallbackError

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
    def __init__(self, directory, binding, exchange, clock=time.time):
        self.directory, self.binding, self.clock = directory, binding, clock
        self.lock = threading.Lock()
        self.status = {'bound': True, 'owner_bound': True, 'poll_state': 'starting'}
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
        return TextAdapter(self.directory / 'messages.sqlite', self.binding,
                           test_exchange=self.exchange, clock=self.clock, recover_sending=recover)

    def call(self, request):
        if not isinstance(request, dict) or set(request) != {'method', 'arguments'} or not isinstance(request['arguments'], dict):
            raise AdapterError('invalid_broker_request')
        method, args = request['method'], request['arguments']
        if method == 'status' and not args:
            return dict(self.status)
        if method == 'build' and not args:
            return {'broker_build': 'ack-compat-20260930', 'optional_ret_with_valid_id': True,
                    'per_reply_diagnostics': True}
        if method == 'read' and set(args) in (set(), {'after_seq'}):
            after_seq = args.get('after_seq', 0)
            if type(after_seq) is not int or after_seq < 0:
                raise AdapterError('invalid_read_cursor')
            with self.lock:
                adapter = self.adapter()
                try:
                    return {'channel_id': CHANNEL, 'messages': adapter.read_messages(after_seq)}
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
        raise AdapterError('broker_method_forbidden')

    def poll_once(self):
        # Do not hold send lock during 35-second long poll; adapter has its own
        # SQLite connection. Writes are SQLite transactions, never shared cursor.
        adapter = self.adapter()
        try:
            result = adapter.poll()
            self.status['poll_state'] = result.get('state', 'ready')
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
            line = self.rfile.readline(32769)
            if len(line) > 32768 or not line.endswith(b'\n'):
                raise AdapterError('request_too_large')
            result = self.server.broker.call(json.loads(line))
            output = {'ok': True, 'result': result}
        except Exception:
            # Never serialize request, server response, exception or traceback.
            output = {'ok': False, 'error': 'broker_operation_rejected'}
        self.wfile.write(json.dumps(output, ensure_ascii=False).encode() + b'\n')


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    def handle_error(self, request, client_address):
        pass  # socketserver default prints tracebacks; prohibited here.


def main():
    os.umask(0o077)
    # Atomic single instance via flock; no inspection of another process/env.
    import fcntl
    check_private(PRIVATE)
    lockfd = os.open(PRIVATE / 'broker.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    binding = load_binding(PRIVATE)
    broker = Broker(PRIVATE, binding, ILinkHTTPS().exchange)
    SOCKET.unlink(missing_ok=True)
    server = Server(str(SOCKET), Handler)
    os.chmod(SOCKET, 0o600)
    server.broker = broker
    stop = threading.Event()
    def poller():
        while not stop.is_set():
            try:
                result = broker.poll_once()
                delay = 60 if result.get('state') == 'session_paused' else 1
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
