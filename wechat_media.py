"""Bounded media validation and pinned Tencent CDN transport. No credential I/O.

Only the broker uses this transport. Importing it never enables networking.
"""
import base64
import hashlib
import io
import socket
import ssl
import warnings
import wave
from urllib.parse import urlencode, urlsplit

from outbound import CallbackError, destination
from proxy_outbound import CloudflareResolver, ProxyPinnedHTTPSConnection, DOH_HOST, public_ip

CDN_HOST = 'novac2c.cdn.weixin.qq.com'
CDN_BASE = 'https://' + CDN_HOST + '/c2c'
MAX_MEDIA = 4 * 1024 * 1024
MAX_FRAME = 6 * 1024 * 1024
MAX_DURATION = 60
MAX_CACHE = 64 * 1024 * 1024
RETENTION = 24 * 3600
ERROR_CATEGORIES = frozenset({
    'media_too_large_or_invalid', 'invalid_base64', 'media_too_large_or_empty',
    'invalid_media_key', 'cryptography_dependency_missing', 'media_decryption_failed',
    'pillow_dependency_missing', 'invalid_or_unsupported_image', 'invalid_or_unsupported_audio',
    'unsupported_media_kind', 'mime_content_mismatch', 'invalid_cdn_url',
    'cdn_destination_forbidden', 'invalid_media_reference', 'missing_media_reference',
    'missing_media_key', 'media_network_disabled', 'cdn_method_forbidden',
    'cdn_no_public_address', 'cdn_redirect_forbidden', 'cdn_http_failed',
    'cdn_response_too_large', 'cdn_upload_ack_missing', 'cdn_tls_certificate_failed',
    'cdn_timeout', 'cdn_exchange_failed', 'unknown_message_media', 'media_unavailable',
    'silk_decode_failed', 'media_cache_quota', 'unknown_message',
    'native_voice_not_verified', 'outgoing_audio_requires_wav',
    'invalid_idempotency_key', 'reply_idempotency_conflict', 'local_context_age_limit',
    'media_source_required', 'media_not_ready_or_unknown',
    'invalid_tts_text', 'tts_library_missing', 'tts_initialization_failed',
    'tts_voice_missing', 'tts_synthesis_failed', 'tts_output_invalid', 'tts_timeout',
    'tts_idempotency_conflict', 'tts_result_unavailable', 'tts_previous_generation_failed',
    'file_size_mismatch', 'invalid_file_size', 'invalid_file_name',
})


class MediaError(ValueError):
    """Only fixed categories may cross the IPC boundary."""


def decode_base64(value):
    if not isinstance(value, str) or len(value) > (MAX_MEDIA + 2) // 3 * 4:
        raise MediaError('media_too_large_or_invalid')
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, UnicodeError):
        raise MediaError('invalid_base64') from None
    if not 0 < len(raw) <= MAX_MEDIA:
        raise MediaError('media_too_large_or_empty')
    return raw


def aes_key(value, hex_key=None):
    try:
        if hex_key is not None:
            if not isinstance(hex_key, str) or len(hex_key) != 32:
                raise ValueError()
            raw = bytes.fromhex(hex_key)
        else:
            if not isinstance(value, str) or len(value) > 64:
                raise ValueError()
            raw = base64.b64decode(value, validate=True)
            if len(raw) == 32:
                raw = bytes.fromhex(raw.decode('ascii'))
        if len(raw) != 16:
            raise ValueError()
        return raw
    except (ValueError, UnicodeError, TypeError):
        raise MediaError('invalid_media_key') from None


def crypt(raw, key, encrypt=False):
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives.padding import PKCS7
    except ImportError:
        raise MediaError('cryptography_dependency_missing') from None
    try:
        cipher = Cipher(algorithms.AES(key), modes.ECB())
        if encrypt:
            pad = PKCS7(128).padder()
            raw = pad.update(raw) + pad.finalize()
            op = cipher.encryptor()
            return op.update(raw) + op.finalize()
        if not raw or len(raw) % 16 or len(raw) > MAX_MEDIA + 16:
            raise ValueError()
        op = cipher.decryptor()
        decoded = op.update(raw) + op.finalize()
        pad = PKCS7(128).unpadder()
        return pad.update(decoded) + pad.finalize()
    except (ValueError, TypeError):
        raise MediaError('media_decryption_failed') from None


def validate_media(raw, kind, declared=None):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_MEDIA:
        raise MediaError('media_too_large_or_empty')
    duration = None
    if kind == 'image':
        try:
            from PIL import Image
        except ImportError:
            raise MediaError('pillow_dependency_missing') from None
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('error')
                with Image.open(io.BytesIO(raw)) as img:
                    mime = {'PNG': 'image/png', 'JPEG': 'image/jpeg', 'WEBP': 'image/webp'}[img.format]
                    if img.width <= 0 or img.height <= 0 or img.width * img.height > 20_000_000 or getattr(img, 'n_frames', 1) != 1:
                        raise ValueError()
                    img.verify()
                # Decode too: verify() alone does not validate JPEG pixel data.
                with Image.open(io.BytesIO(raw)) as img:
                    img.load()
        except Exception:
            raise MediaError('invalid_or_unsupported_image') from None
    elif kind == 'audio':
        if raw.startswith((b'#!SILK_V3', b'\x02#!SILK_V3')):
            mime = 'audio/silk'  # Stored only, never claimed to be model-readable.
        else:
            try:
                with wave.open(io.BytesIO(raw)) as wav:
                    if wav.getcomptype() != 'NONE' or wav.getnchannels() not in (1, 2) or wav.getsampwidth() != 2 or not 8000 <= wav.getframerate() <= 48000:
                        raise ValueError()
                    frames = wav.getnframes()
                    duration = frames / wav.getframerate()
                    if not 0 < duration <= MAX_DURATION or len(wav.readframes(frames)) != frames * wav.getnchannels() * 2:
                        raise ValueError()
                mime = 'audio/wav'
            except Exception:
                raise MediaError('invalid_or_unsupported_audio') from None
    else:
        raise MediaError('unsupported_media_kind')
    if declared is not None and mime != declared:
        raise MediaError('mime_content_mismatch')
    return {'kind': kind, 'mime_type': mime, 'size_bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest(),
            'model_input': 'unverified' if mime != 'audio/silk' else 'decoder_required',
            **({'duration_ms': round(duration * 1000)} if duration else {})}


def cdn_url(url, operation):
    if not isinstance(url, str) or len(url) > 16384:
        raise MediaError('invalid_cdn_url')
    try:
        host, path = destination(url, {CDN_HOST})
        if urlsplit(url).path != '/c2c/' + operation:
            raise ValueError()
    except (ValueError, TypeError):
        raise MediaError('cdn_destination_forbidden') from None
    return host, path


def reference(item, kind):
    media = item.get('media')
    if not isinstance(media, dict):
        raise MediaError('invalid_media_reference')
    # Tencent defines modes 0 (file ID metadata) and 1 (packed image metadata).
    # Both use the supplied CDN reference and the same payload AES key. The
    # official downloader does not select image quality using this field.
    mode = media.get('encrypt_type', 1)
    if type(mode) is not int or mode not in (0, 1):
        raise MediaError('invalid_media_reference')
    full = media.get('full_url')
    param = media.get('encrypt_query_param')
    if full:
        url = full
    elif isinstance(param, str) and 0 < len(param) <= 8192:
        url = CDN_BASE + '/download?' + urlencode({'encrypted_query_param': param})
    else:
        raise MediaError('missing_media_reference')
    cdn_url(url, 'download')
    # Official image hex key takes precedence. Plain images are permitted.
    key = None
    if kind == 'image' and item.get('aeskey'):
        key = aes_key(None, item['aeskey'])
    elif media.get('aes_key'):
        key = aes_key(media['aes_key'])
    elif kind == 'audio':
        raise MediaError('missing_media_key')
    return {'url': url, 'key': key.hex() if key else None}


class CDNHTTPS:
    def __init__(self, *, enabled=False, resolver=None, factory=None):
        self.enabled = enabled
        self.resolver = resolver or CloudflareResolver(query_host=CDN_HOST)
        self.factory = factory or (lambda h, ip, t: ProxyPinnedHTTPSConnection(h, ip, t, allowed_hosts=(CDN_HOST, DOH_HOST)))

    def request(self, method, url, body=None):
        if not self.enabled:
            raise MediaError('media_network_disabled')
        if method not in ('GET', 'POST'):
            raise MediaError('cdn_method_forbidden')
        host, target = cdn_url(url, 'download' if method == 'GET' else 'upload')
        if method == 'POST' and (not isinstance(body, bytes) or len(body) > MAX_MEDIA + 16):
            raise MediaError('media_too_large_or_invalid')
        conn = None
        try:
            addresses = self.resolver(host, 443, type=socket.SOCK_STREAM)
            ips = [str(public_ip(entry[4][0])) for entry in addresses]
            if not ips:
                raise MediaError('cdn_no_public_address')
            conn = self.factory(host, ips[0], 40)
            # No bot Authorization, context, cookies or environment proxy sent to CDN.
            conn.request(method, target, body=body, headers={'Host': host, 'Content-Type': 'application/octet-stream'})
            response = conn.getresponse()
            if 300 <= response.status < 400:
                raise MediaError('cdn_redirect_forbidden')
            if response.status != 200:
                raise MediaError('cdn_http_failed')
            length = response.getheader('Content-Length')
            cap = MAX_MEDIA + 16 if method == 'GET' else 65536
            if length is not None and (not length.isascii() or not length.isdecimal() or int(length) > cap):
                raise MediaError('cdn_response_too_large')
            raw = response.read(cap + 1)
            if len(raw) > cap:
                raise MediaError('cdn_response_too_large')
            param = response.getheader('x-encrypted-param')
            if method == 'POST' and (not isinstance(param, str) or not 0 < len(param) <= 8192 or any(ord(c) < 32 for c in param)):
                raise MediaError('cdn_upload_ack_missing')
            return raw if method == 'GET' else param
        except MediaError:
            raise
        except ssl.SSLCertVerificationError:
            raise MediaError('cdn_tls_certificate_failed') from None
        except TimeoutError:
            raise MediaError('cdn_timeout') from None
        except Exception:
            raise MediaError('cdn_exchange_failed') from None
        finally:
            if conn is not None:
                conn.close()


def validate_file(raw, file_name):
    """Opaque output bytes; never interpret, extract or execute their contents."""
    if (not isinstance(file_name, str) or not file_name.strip() or
            len(file_name.encode('utf-8')) > 240 or file_name in ('.', '..') or
            any(c in '/\\' or ord(c) < 32 or ord(c) == 127 for c in file_name)):
        raise MediaError('invalid_file_name')
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_MEDIA:
        raise MediaError('invalid_file_size')
    return {'mime_type': 'application/octet-stream', 'sha256': hashlib.sha256(raw).hexdigest()}
