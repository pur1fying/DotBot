"""Stage an explicitly selected local output asset for one owner reply. Never sends.

This is an operator action, not a remote arbitrary-file-reading MCP tool.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import stat
from ilink_ipc import call
from wechat_media import MAX_MEDIA


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--message-id', required=True, help='Actual existing owner message_id; never guess')
    parser.add_argument('--file', required=True, help='Explicitly selected image/WAV output asset')
    parser.add_argument('--mime-type', required=True, choices=['image/png', 'image/jpeg', 'image/webp', 'audio/wav'])
    args = parser.parse_args()
    fd = os.open(Path(args.file), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError()
        raw = stream.read(MAX_MEDIA + 1)
    if not 0 < len(raw) <= MAX_MEDIA:
        raise ValueError()
    result = call('stage_media', {'message_id': args.message_id,
                  'data_base64': base64.b64encode(raw).decode(), 'mime_type': args.mime_type})
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('{"staged":false,"sent":false,"error_category":"local_staging_failed"}')
        raise SystemExit(1)
