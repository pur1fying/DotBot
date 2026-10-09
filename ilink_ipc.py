"""Non-secret MCP-side Unix socket client; never opens binding or message DB."""
import json
import os
import socket
import stat
from pathlib import Path

SOCKET = Path(__file__).parent / 'runtime' / 'wechat-private' / 'broker.sock'


def call(method, arguments=None, path=SOCKET):
    parent = path.parent.lstat()
    info = path.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077
            or parent.st_uid != os.getuid() or not stat.S_ISSOCK(info.st_mode)
            or info.st_mode & 0o077 or info.st_uid != os.getuid()):
        raise ValueError('unsafe_broker_socket')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(50)
        conn.connect(str(path))
        conn.sendall(json.dumps({'method': method, 'arguments': arguments or {}}).encode() + b'\n')
        with conn.makefile('rb') as stream:
            raw = stream.readline(2097153)
        if len(raw) > 2097152 or not raw.endswith(b'\n'):
            raise ValueError('invalid_broker_response')
        response = json.loads(raw)
        if response.get('ok') is not True:
            raise ValueError('broker_operation_rejected')
        return response['result']
