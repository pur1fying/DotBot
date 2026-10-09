"""Optional fixed local SILK decoder. Tool arguments cannot select commands."""
from pathlib import Path
import subprocess
from wechat_media import MAX_MEDIA, MediaError


class SilkDecoder:
    def __init__(self, node):
        path = Path(node)
        if not path.is_absolute() or not path.is_file():
            raise MediaError('silk_decode_failed')
        self.command = [str(path), str(Path(__file__).with_name('silk_decode.mjs'))]

    def __call__(self, raw):
        try:
            result = subprocess.run(self.command, input=raw, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, timeout=20,
                                    env={'PATH': '/usr/bin:/bin'}, check=True)
            if not 0 < len(result.stdout) <= MAX_MEDIA:
                raise ValueError()
            return result.stdout
        except Exception:
            raise MediaError('silk_decode_failed') from None
