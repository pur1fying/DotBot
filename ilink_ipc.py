"""Non-secret MCP-side Unix socket client; never opens binding or message DB."""
import json
import os
import socket
import stat
from pathlib import Path
from wechat_media import MAX_FRAME, ERROR_CATEGORIES

SOCKET = Path(__file__).parent / 'runtime' / 'wechat-private' / 'broker.sock'


class BrokerError(Exception):
    def __init__(self, category):
        self.category = category
        super().__init__(category)


def call(method, arguments=None, path=SOCKET):
    try:
        return _call(method, arguments, path)
    except BrokerError:
        raise
    except (OSError, ValueError, KeyError, TypeError):
        raise BrokerError('broker_unavailable_or_invalid_response') from None


def _call(method, arguments, path):
    parent = path.parent.lstat()
    info = path.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077
            or parent.st_uid != os.getuid() or not stat.S_ISSOCK(info.st_mode)
            or info.st_mode & 0o077 or info.st_uid != os.getuid()):
        raise ValueError('unsafe_broker_socket')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(50)
        conn.connect(str(path))
        body = json.dumps({'method': method, 'arguments': arguments or {}}).encode() + b'\n'
        if len(body) > MAX_FRAME:
            raise BrokerError('broker_request_too_large')
        conn.sendall(body)
        with conn.makefile('rb') as stream:
            raw = stream.readline(MAX_FRAME + 1)
        if len(raw) > MAX_FRAME or not raw.endswith(b'\n'):
            raise ValueError('invalid_broker_response')
        response = json.loads(raw)
        if response.get('ok') is not True:
            category = response.get('error')
            safe_dot = (isinstance(category, str) and category.startswith(('dot_', 'auth_', 'wechat_', 'webhook_', 'reply_window_'))
                        and len(category) <= 100 and category.replace('_', '').isalnum())
            raise BrokerError(category if isinstance(category, str) and (category in ERROR_CATEGORIES or safe_dot) else 'broker_operation_rejected')
        return response['result']
