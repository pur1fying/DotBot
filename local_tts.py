"""Bounded local eSpeak NG TTS. No network, credentials, playback or arbitrary voice.

The broker invokes a fixed subprocess with text only on stdin and an empty
credential environment. A library failure cannot crash the broker process.
"""
import ctypes
import ctypes.util
import io
from pathlib import Path
import subprocess
import sys
import wave

sys.path.insert(0, str(Path(__file__).resolve().parent))
from wechat_media import MAX_MEDIA, MAX_DURATION, MediaError

MAX_TTS_CHARS = 300
MAX_TTS_BYTES = 1200
VOICE = 'cmn'


def validate_text(text):
    try:
        size = len(text.encode('utf-8')) if isinstance(text, str) else 0
    except UnicodeError:
        raise MediaError('invalid_tts_text') from None
    if (not isinstance(text, str) or not text.strip() or len(text) > MAX_TTS_CHARS
            or size > MAX_TTS_BYTES
            or any((ord(c) < 32 and c not in '\n\t') or ord(c) == 127 for c in text)):
        raise MediaError('invalid_tts_text')
    return text


def synthesize(text):
    validate_text(text)
    name = ctypes.util.find_library('espeak-ng')
    if not name:
        raise MediaError('tts_library_missing')
    try:
        lib = ctypes.CDLL(name)
    except OSError:
        raise MediaError('tts_library_missing') from None
    callback_type = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.POINTER(ctypes.c_short), ctypes.c_int, ctypes.c_void_p)
    lib.espeak_Initialize.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
    lib.espeak_Initialize.restype = ctypes.c_int
    lib.espeak_SetSynthCallback.argtypes = [callback_type]; lib.espeak_SetSynthCallback.restype = None
    lib.espeak_SetVoiceByName.argtypes = [ctypes.c_char_p]; lib.espeak_SetVoiceByName.restype = ctypes.c_int
    lib.espeak_Synth.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint, ctypes.c_int,
                                ctypes.c_uint, ctypes.c_uint, ctypes.POINTER(ctypes.c_uint), ctypes.c_void_p]
    lib.espeak_Synth.restype = ctypes.c_int
    lib.espeak_Terminate.argtypes = []; lib.espeak_Terminate.restype = ctypes.c_int
    # Fixed retrieval mode; no device output. Use distro library's data default.
    rate = lib.espeak_Initialize(2, 0, None, 0x8000)
    if not 8000 <= rate <= 48000:
        raise MediaError('tts_initialization_failed')
    chunks, size, exceeded = [], 0, False

    @callback_type
    def receive(samples, count, events):
        nonlocal size, exceeded
        if not samples or count <= 0:
            return 0
        size += count * 2
        if size > min(MAX_MEDIA - 44, rate * 2 * MAX_DURATION):
            exceeded = True
            return 1
        chunks.append(ctypes.string_at(samples, count * 2))
        return 0

    try:
        lib.espeak_SetSynthCallback(receive)
        if lib.espeak_SetVoiceByName(VOICE.encode()) != 0:
            raise MediaError('tts_voice_missing')
        buffer = ctypes.create_string_buffer(text.encode('utf-8'))
        identifier = ctypes.c_uint()
        # UTF8 only: SSML/phoneme flags disabled, no URI/file directives.
        if lib.espeak_Synth(buffer, len(buffer), 0, 1, 0, 1, ctypes.byref(identifier), None) != 0:
            raise MediaError('tts_synthesis_failed')
        pcm = b''.join(chunks)
        if exceeded or not pcm or not any(pcm):
            raise MediaError('tts_output_invalid')
        stream = io.BytesIO()
        with wave.open(stream, 'wb') as output:
            output.setnchannels(1); output.setsampwidth(2); output.setframerate(rate)
            output.writeframes(pcm)
        return stream.getvalue()
    finally:
        lib.espeak_Terminate()


class LocalTTS:
    def __call__(self, text):
        validate_text(text)
        try:
            result = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve())],
                                    input=text.encode('utf-8'), stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, timeout=20, check=True,
                                    env={'PATH': '/usr/bin:/bin'})
            if not 0 < len(result.stdout) <= MAX_MEDIA:
                raise MediaError('tts_output_invalid')
            return result.stdout
        except subprocess.TimeoutExpired:
            raise MediaError('tts_timeout') from None
        except MediaError:
            raise
        except Exception:
            raise MediaError('tts_synthesis_failed') from None


if __name__ == '__main__':
    try:
        raw = sys.stdin.buffer.read(MAX_TTS_BYTES + 1)
        if len(raw) > MAX_TTS_BYTES:
            raise MediaError('invalid_tts_text')
        sys.stdout.buffer.write(synthesize(raw.decode('utf-8')))
    except Exception:
        # No input, exception, library message or traceback in protocol output.
        raise SystemExit(1)
