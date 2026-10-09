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
from ilink_broker import check_private
from ilink_ipc import call


def main():
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
        subprocess.Popen(['/usr/bin/python3', '-B', str(base / 'ilink_broker.py')],
                         cwd=base, env={'PATH': '/usr/bin:/bin'},
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
    for _ in range(5):
        try:
            status = call('status', {})
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
