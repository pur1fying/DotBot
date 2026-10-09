"""Start the already-authorized broker independently of a temporary terminal.

This launcher never opens binding.json, accepts credentials, or reads messages.
"""
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import time
import argparse
from ilink_broker import check_private
from ilink_ipc import call


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--media', action='store_true', help='Enable media on new broker; approval required')
    parser.add_argument('--silk-node', help='Optional absolute Node path for local silk-wasm decoding')
    args = parser.parse_args()
    if args.silk_node and not args.media:
        parser.error('--silk-node requires --media')
    base = Path(__file__).resolve().parent
    private = base / 'runtime' / 'wechat-private'
    check_private(private)
    fd = os.open(private / 'broker.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    active = False
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError()
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            active = True
    finally:
        os.close(fd)
    if not active:
        if args.media:
            subprocess.run(['/usr/bin/python3', '-I', '-c', 'import cryptography, PIL'],
                           env={'PATH': '/usr/bin:/bin'}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        command = ['/usr/bin/python3', '-B', str(base / 'ilink_broker.py')]
        if args.media: command.append('--media')
        if args.silk_node: command.extend(['--silk-node', args.silk_node])
        subprocess.Popen(command,
                         cwd=base, env={'PATH': '/usr/bin:/bin'},
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
    for _ in range(5):
        try:
            status = call('status', {})
            if args.media and status.get('media_enabled') is not True:
                print('{"broker_status":"existing_broker_requires_approved_restart"}')
                return
            print(json.dumps({k: status[k] for k in ('bound', 'owner_bound', 'poll_state')}))
            return
        except Exception:
            time.sleep(0.5)
    print('{"broker_status":"unavailable"}')


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        print('{"broker_status":"startup_failed"}')
