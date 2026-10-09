"""Quiescent project-only restart. No credentials, messages or network mutation.

SQLite write gate ensures no send can commit its 'sending' marker between the
idle check and process stop. Broker always commits that marker BEFORE HTTPS.
"""
import json
import os
from pathlib import Path
import select
import signal
import sqlite3
import stat
import subprocess
import time
from ilink_broker import check_private
from ilink_ipc import call


def main():
    base = Path(__file__).resolve().parent
    private = base / 'runtime' / 'wechat-private'
    check_private(private)
    info = (private / 'broker.lock').stat()
    identity = f'{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}:{info.st_ino}'
    pids = []
    for line in Path('/proc/locks').read_text().splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[1:4] == ['FLOCK', 'ADVISORY', 'WRITE'] and fields[5] == identity:
            pids.append(int(fields[4]))
    if len(pids) != 1:
        print('{"restart":"refused","reason":"broker_identity_unconfirmed"}')
        return
    pid = pids[0]
    proc = Path('/proc') / str(pid)
    if proc.stat().st_uid != os.getuid() or not os.readlink(proc / 'exe').split('/')[-1].startswith('python3'):
        print('{"restart":"refused","reason":"unexpected_lock_holder"}')
        return
    pidfd = os.pidfd_open(pid)
    db = sqlite3.connect(private / 'messages.sqlite', timeout=2)
    try:
        db.execute('BEGIN IMMEDIATE')
        if db.execute("SELECT count(*) FROM replies WHERE state='sending'").fetchone()[0]:
            print('{"restart":"refused","reason":"send_in_progress"}')
            return
        # No forced kill: TERM only, hold gate until the exact process exits.
        signal.pidfd_send_signal(pidfd, signal.SIGTERM)
        if not select.select([pidfd], [], [], 5)[0]:
            print('{"restart":"stopped_waiting","reason":"broker_did_not_exit"}')
            return
    finally:
        db.rollback()
        db.close()
        os.close(pidfd)
    subprocess.Popen(['/usr/bin/python3', '-B', str(base / 'ilink_broker.py')], cwd=base,
                     env={'PATH': '/usr/bin:/bin'}, stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                     start_new_session=True, close_fds=True)
    for _ in range(10):
        try:
            build = call('build', {})
            status = call('status', {})
            print(json.dumps({'restart': 'loaded', 'build': build, 'status': status}))
            return
        except Exception:
            time.sleep(0.5)
    print('{"restart":"started","status":"not_yet_observed"}')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('{"restart":"refused","reason":"safe_check_failed"}')
