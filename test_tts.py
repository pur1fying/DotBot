"""Local synthesis and generated-ID tests; synthetic owner, no network/playback."""
import base64
import io
import json
import os
import subprocess
import threading
import unittest
import wave
from unittest.mock import patch

import jsonschema
from ilink_broker import Handler, Server
from ilink_ipc import call
from local_tts import LocalTTS, synthesize, validate_text
from service import process, run_stdio
import test_media as fixtures
from wechat_media import MediaError, MAX_MEDIA, validate_media


def speech_wav(seconds=0.1, channels=1):
    stream = io.BytesIO()
    with wave.open(stream, 'wb') as output:
        output.setnchannels(channels); output.setsampwidth(2); output.setframerate(24000)
        output.writeframes(b'\x01\x00' * int(24000 * seconds) * channels)
    return stream.getvalue()


class TTSTests(unittest.TestCase):
    # Reuse the synthetic broker fixture, not its test methods.
    setUp = fixtures.MediaTests.setUp
    tearDown = fixtures.MediaTests.tearDown
    broker_call = fixtures.MediaTests.broker_call
    message = fixtures.MediaTests.message
    ingest = fixtures.MediaTests.ingest
    read = fixtures.MediaTests.read
    tool = fixtures.MediaTests.tool

    def args(self, text='这是合成测试。'):
        return {'message_id': self.read()['message_id'], 'text': text, 'idempotency_key': 'synthetic-generation-once'}

    def fake(self, output=None):
        self.generations = []
        def generate(text):
            self.generations.append(text)
            return speech_wav() if output is None else output
        self.b.tts = generate

    def generated(self, args=None):
        return self.tool('wechat_generate_audio', args or self.args())['structuredContent']

    def test_generate_stages_only_and_explicit_reply_uses_actual_id(self):
        self.ingest(); self.fake()
        result = self.generated()
        self.assertFalse(result['sent']); self.assertTrue(result['staged'])
        self.assertEqual(self.sent, []); self.assertEqual(self.cdn.calls, [])
        schema = next(t for t in self.s.tools()['tools'] if t['name'] == 'wechat_generate_audio')
        jsonschema.validate(self.args(), schema['inputSchema'])
        jsonschema.validate(result, schema['outputSchema'])
        self.assertFalse(schema['annotations']['openWorldHint'])
        self.assertFalse(schema['annotations']['destructiveHint'])
        response = self.tool('wechat_reply_media', {'message_id': result['message_id'], 'media_id': result['media_id'],
                    'mime_type': 'audio/wav', 'delivery_mode': 'audio_attachment', 'idempotency_key': 'synthetic-send-once'})
        self.assertEqual(response['structuredContent']['state'], 'api_accepted')
        self.assertEqual(self.sent[-1][2]['msg']['item_list'][0]['type'], 4)

    def test_duplicate_generation_survives_adapter_restart(self):
        self.ingest(); self.fake(); first = self.generated()
        self.b.adapter(recover=True).close()
        self.assertEqual(self.generated(), first)
        self.assertEqual(len(self.generations), 1)

    def test_same_key_changed_text_or_message_conflicts(self):
        self.ingest(); self.fake(); args = self.args(); self.generated(args)
        for changed in [dict(args, text='别的文字'), dict(args, message_id='other-message')]:
            self.assertTrue(self.tool('wechat_generate_audio', changed)['isError'])
        self.assertEqual(len(self.generations), 1)

    def test_generation_text_is_hashed_and_not_in_metadata_logs_or_result(self):
        self.ingest(); self.fake(); secret_text = 'PRIVATE_SYNTHETIC_OUTPUT_TEXT_739'
        result = self.generated(self.args(secret_text))
        a = self.b.adapter()
        try:
            rows = a.db.execute('SELECT * FROM tts_generations').fetchall()
            self.assertNotIn(secret_text, json.dumps(rows))
            self.assertEqual(len(rows[0][2]), 64)
        finally: a.close()
        for data in [result, self.s.db.execute('SELECT * FROM diagnostics').fetchall()]:
            self.assertNotIn(secret_text, json.dumps(data))
            self.assertNotIn('SYNTHETIC-TOKEN', json.dumps(data))

    def test_unknown_owner_message_or_old_context_never_synthesizes(self):
        self.ingest(); self.fake(); args = self.args()
        self.assertTrue(self.tool('wechat_generate_audio', dict(args, message_id='unknown'))['isError'])
        self.now += 3601
        self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        self.assertEqual(self.generations, [])

    def test_forget_revokes_generated_id_and_does_not_regenerate_key(self):
        self.ingest(); self.fake(); args = self.args(); result = self.generated(args)
        self.tool('wechat_forget_media', {'message_id': args['message_id']})
        self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        reply = dict(message_id=args['message_id'], media_id=result['media_id'], mime_type='audio/wav',
                     delivery_mode='audio_attachment', idempotency_key='revoked-send')
        self.assertTrue(self.tool('wechat_reply_media', reply)['isError'])
        self.assertEqual(len(self.generations), 1); self.assertEqual(self.sent, [])

    def test_expiry_purges_audio_and_retains_generation_tombstone(self):
        self.ingest(); self.fake(); args = self.args(); self.generated(args)
        self.now += 86401
        self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        a = self.b.adapter()
        try:
            self.assertEqual(a.db.execute('SELECT count(*) FROM staged_media').fetchone()[0], 0)
            self.assertEqual(a.db.execute('SELECT state,media_id FROM tts_generations').fetchone(), ('expired', None))
        finally: a.close()
        self.assertEqual(len(self.generations), 1)

    def test_failed_generation_redacts_error_and_never_retries(self):
        self.ingest(); calls = []
        def fail(text):
            calls.append(text)
            raise RuntimeError('PRIVATE_SYNTHETIC_TOKEN /private/path')
        self.b.tts = fail; args = self.args()
        result = self.tool('wechat_generate_audio', args)
        self.assertTrue(result['isError']); self.assertNotIn('PRIVATE', json.dumps(result))
        self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        a = self.b.adapter()
        try: self.assertEqual(a.db.execute('SELECT state,error_category FROM tts_generations').fetchone(), ('failed', 'tts_synthesis_failed'))
        finally: a.close()
        self.assertEqual(len(calls), 1)

    def test_interrupted_generation_marker_becomes_unknown_without_execution(self):
        self.ingest(); self.fake(); args = self.args()
        a = self.b.adapter()
        try:
            with a.db:
                a.db.execute('INSERT INTO tts_generations VALUES (?,?,?,?,?,?,?)',
                             (args['idempotency_key'], args['message_id'], 'synthetic-digest', None, 'generating', self.now + 86400, None))
        finally: a.close()
        self.b.adapter(recover=True).close()
        a = self.b.adapter()
        try: self.assertEqual(a.db.execute('SELECT state FROM tts_generations').fetchone()[0], 'unknown')
        finally: a.close()
        self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        self.assertEqual(self.generations, [])

    def test_invalid_output_size_duration_stereo_silence_is_never_staged(self):
        self.ingest()
        for index, raw in enumerate([b'bad', b'x' * (MAX_MEDIA + 1), speech_wav(61), speech_wav(channels=2), fixtures.wav()]):
            self.fake(raw)
            result = self.tool('wechat_generate_audio', dict(self.args(), idempotency_key='invalid-' + str(index)))
            self.assertTrue(result['isError'])
        a = self.b.adapter()
        try: self.assertEqual(a.db.execute('SELECT count(*) FROM staged_media').fetchone()[0], 0)
        finally: a.close()
        self.assertEqual(self.sent, []); self.assertEqual(self.cdn.calls, [])

    def test_cache_quota_failure_is_durable_and_stores_no_audio(self):
        self.ingest(); self.fake(); args = self.args()
        with patch('media_adapter.MAX_CACHE', 1):
            self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        self.assertTrue(self.tool('wechat_generate_audio', args)['isError'])
        self.assertEqual(len(self.generations), 1)
        a = self.b.adapter()
        try: self.assertEqual(a.db.execute('SELECT count(*) FROM staged_media').fetchone()[0], 0)
        finally: a.close()

    def test_text_and_idempotency_limits_fail_before_generation(self):
        self.ingest(); self.fake(); args = self.args()
        with self.assertRaises(ValueError): self.tool('wechat_generate_audio', dict(args, text=''))
        for text in ['   ', '中' * 301, 'x\x00y', 'x\x1by', '\ud800']:
            self.assertTrue(self.tool('wechat_generate_audio', dict(args, text=text))['isError'])
        self.assertTrue(self.tool('wechat_generate_audio', dict(args, idempotency_key='x' * 129))['isError'])
        self.assertEqual(self.generations, [])

    def test_extra_voice_or_command_argument_is_invalid_params(self):
        self.ingest(); self.fake()
        params = {'name': 'wechat_generate_audio', 'arguments': dict(self.args(), voice='arbitrary')}
        params['_meta'] = {'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {}}
        result = process(self.s, {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': params})
        self.assertEqual(result['error']['code'], -32602)
        self.assertEqual(self.generations, [])

    def test_generation_operational_error_preserves_broker_stage_without_fake_http_status(self):
        self.ingest(); self.b.tts = None
        result = self.tool('wechat_generate_audio', self.args())
        self.assertTrue(result['isError'])
        stage, status = self.s.db.execute('SELECT stage,http_status FROM diagnostics ORDER BY rowid DESC LIMIT 1').fetchone()
        self.assertEqual(stage, 'broker_operation'); self.assertIsNone(status)

    def test_actual_local_worker_through_unix_ipc_and_stdio_generates_mandarin_wav(self):
        self.ingest(); args = self.args('这是本地离线合成语音测试。数字七三九。')
        path = self.directory / 'broker.sock'
        server = Server(str(path), Handler); server.broker = self.b; os.chmod(path, 0o600)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        self.s.wechat = lambda method, arguments: call(method, arguments, path)
        try:
            params = {'name': 'wechat_generate_audio', 'arguments': args,
                      '_meta': {'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {}}}
            sink = io.BytesIO()
            run_stdio(self.s, io.BytesIO(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': params}).encode() + b'\n'), sink)
            result = json.loads(sink.getvalue())['result']['structuredContent']
            self.assertFalse(result['sent']); self.assertEqual(result['voice'], 'cmn')
            a = self.b.adapter()
            try: raw = a.db.execute('SELECT blob FROM staged_media WHERE id=?', (result['media_id'],)).fetchone()[0]
            finally: a.close()
            self.assertEqual(validate_media(raw, 'audio', 'audio/wav')['sha256'], result['sha256'])
            with wave.open(io.BytesIO(raw)) as audio:
                self.assertEqual(audio.getnchannels(), 1); self.assertEqual(audio.getsampwidth(), 2)
                self.assertTrue(any(audio.readframes(audio.getnframes())))
            self.assertEqual(self.sent, []); self.assertEqual(self.cdn.calls, [])
        finally: server.shutdown(); server.server_close(); thread.join()

    def test_disabled_media_catalog_does_not_expose_synthesis(self):
        self.s.wechat_media = False
        self.assertNotIn('wechat_generate_audio', [t['name'] for t in self.s.tools()['tools']])


class WorkerTests(unittest.TestCase):
    def test_utf8_limit_and_controls(self):
        self.assertEqual(validate_text('\U0001f600' * 300), '\U0001f600' * 300)
        self.assertEqual(validate_text('中文\n测试\t'), '中文\n测试\t')
        for text in [None, 1, '\udfff', '\x7f', '\r']:
            with self.assertRaises(MediaError): validate_text(text)

    def test_worker_receives_text_only_on_stdin_with_no_inherited_environment(self):
        text = 'PRIVATE_SYNTHETIC_TTS_TEXT'
        with patch('local_tts.subprocess.run', return_value=subprocess.CompletedProcess([], 0, speech_wav())) as run:
            self.assertEqual(LocalTTS()(text), speech_wav())
            args, kwargs = run.call_args
            self.assertNotIn(text, ' '.join(args[0]))
            self.assertEqual(kwargs['input'], text.encode()); self.assertEqual(kwargs['env'], {'PATH': '/usr/bin:/bin'})
            self.assertEqual(kwargs['timeout'], 20); self.assertEqual(kwargs['stderr'], subprocess.DEVNULL)
            self.assertIn('-I', args[0]); self.assertNotIn('shell', kwargs)

    def test_worker_timeout_and_process_failure_are_fixed_errors(self):
        for exception, category in [(subprocess.TimeoutExpired('PRIVATE', 20), 'tts_timeout'),
                                    (subprocess.CalledProcessError(1, 'PRIVATE', stderr=b'PRIVATE'), 'tts_synthesis_failed')]:
            with patch('local_tts.subprocess.run', side_effect=exception):
                with self.assertRaisesRegex(MediaError, '^' + category + '$'): LocalTTS()('测试')

    def test_missing_existing_library_fails_closed(self):
        with patch('local_tts.ctypes.util.find_library', return_value=None):
            with self.assertRaisesRegex(MediaError, '^tts_library_missing$'): synthesize('测试')


if __name__ == '__main__': unittest.main()
