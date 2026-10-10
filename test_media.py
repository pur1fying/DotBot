"""Synthetic offline media tests. No Tencent/OpenAI endpoints are contacted."""
import base64
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
import wave
from unittest.mock import patch

from PIL import Image
import jsonschema
from ilink_adapter import Binding, TextAdapter
from ilink_broker import Broker, Handler, Server
from ilink_ipc import call, BrokerError
from media_adapter import MediaAdapter
from service import PersistentService, OfflineDelivery, FIXTURE_SECRET, process, run_stdio
from wechat_media import (CDNHTTPS, CDN_BASE, CDN_HOST, MediaError, MAX_MEDIA,
                          crypt, aes_key, reference, validate_media, decode_base64)


def png():
    stream = io.BytesIO()
    Image.new('RGB', (4, 3), (123, 45, 67)).save(stream, 'PNG')
    return stream.getvalue()


def wav(seconds=0.1):
    stream = io.BytesIO()
    with wave.open(stream, 'wb') as output:
        output.setnchannels(1); output.setsampwidth(2); output.setframerate(24000)
        output.writeframes(b'\0\0' * int(24000 * seconds))
    return stream.getvalue()


KEY = b'0123456789abcdef'


def image_item():
    return {'type': 2, 'image_item': {'media': {
        'encrypt_query_param': 'SYNTHETIC-PRIVATE-DOWNLOAD',
        'aes_key': base64.b64encode(KEY).decode(), 'encrypt_type': 1}}}


def voice_item(transcript=None):
    field = {'media': {'encrypt_query_param': 'SYNTHETIC-VOICE',
                       'aes_key': base64.b64encode(KEY.hex().encode()).decode()}, 'playtime': 1000}
    if transcript is not None: field['text'] = transcript
    return {'type': 3, 'voice_item': field}


class FakeCDN:
    def __init__(self):
        self.calls = []
        self.image = crypt(png(), KEY, True)
        self.voice = crypt(wav(), KEY, True)
        self.fail_upload = False

    def request(self, method, url, body=None):
        self.calls.append((method, url, body))
        if method == 'GET':
            return self.voice if 'VOICE' in url else self.image
        if self.fail_upload: raise MediaError('cdn_timeout')
        return 'SYNTHETIC-PRIVATE-UPLOADED'


class MediaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)
        self.now = 1000
        self.binding = Binding('SYNTHETIC-BOT', 'SYNTHETIC-OWNER', 'SYNTHETIC-TOKEN')
        self.cdn = FakeCDN(); self.sent = []; self.ack = {'ret': 0}
        def exchange(path, headers, body):
            self.sent.append((path, headers, body))
            return {'upload_param': 'SYNTHETIC-PRIVATE-UPLOAD'} if path.endswith('getuploadurl') else self.ack
        self.exchange = exchange
        self.b = Broker(self.directory, self.binding, exchange, clock=lambda: self.now,
                        media_enabled=True, media_transport=self.cdn)
        self.s = PersistentService(self.directory / 'mcp.sqlite', OfflineDelivery(now=lambda: self.now),
                                   now=lambda: self.now, wechat=self.broker_call, wechat_media=True)

    def tearDown(self):
        self.s.close(); self.tmp.cleanup()

    def broker_call(self, method, args):
        return self.b.call({'method': method, 'arguments': args})

    def message(self, items=None, mid=123, **extra):
        return dict({'message_id': mid, 'from_user_id': 'SYNTHETIC-OWNER', 'to_user_id': 'SYNTHETIC-BOT',
                     'message_type': 1, 'message_state': 2, 'context_token': 'SYNTHETIC-CONTEXT',
                     'item_list': items or [image_item()]}, **extra)

    def ingest(self, msg=None):
        a = self.b.adapter()
        try: return a.ingest({'msgs': [msg or self.message()]})
        finally: a.close()

    def read(self):
        return self.broker_call('read', {})['messages'][0]

    def media_args(self):
        msg = self.read()
        return {'message_id': msg['message_id'], 'media_id': msg['attachments'][0]['media_id']}

    def tool(self, name, args):
        return self.s.dispatch('tools/call', {'name': name, 'arguments': args})

    def reply_args(self, mode='image'):
        raw = png() if mode == 'image' else wav()
        return {'message_id': self.read()['message_id'], 'data_base64': base64.b64encode(raw).decode(),
                'mime_type': 'image/png' if mode == 'image' else 'audio/wav',
                'idempotency_key': 'synthetic-once', 'delivery_mode': mode}

    def test_owner_image_event_has_actual_payload_and_id_no_keys(self):
        self.s.dispatch('events/subscribe', {'name': 'wechat.message.created', 'arguments': {'channel_id': 'wechat-owner'},
                       'ttlMs': 60000, 'delivery': {'mode': 'webhook', 'url': 'https://mock.invalid/callback', 'secret': FIXTURE_SECRET}})
        self.assertEqual(self.ingest()['accepted'], 1)
        self.assertEqual(self.cdn.calls, [])
        self.s.sync_wechat()
        event = self.s.delivery.callback.events[0]
        msg = self.read()
        self.assertEqual(event['data']['message_id'], msg['message_id'])
        self.assertEqual(event['data']['attachments'][0]['media_id'], msg['attachments'][0]['media_id'])
        self.assertEqual(self.s.sync_wechat(), 0)
        self.assertEqual(self.ingest()['duplicates'], 1)
        output = json.dumps(event)
        for private in ['SYNTHETIC-PRIVATE', 'SYNTHETIC-TOKEN', 'SYNTHETIC-CONTEXT', KEY.hex(), 'aes_key']:
            self.assertNotIn(private, output)
        schema = self.s.dispatch('events/list', {})['events'][-1]['payloadSchema']
        jsonschema.validate(event['data'], schema)

    def test_image_tool_returns_actual_mcp_pixels_and_private_cache(self):
        self.ingest()
        args = self.media_args()
        result = self.tool('wechat_read_media', args)
        block = result['content'][1]
        self.assertEqual(block['type'], 'image')
        self.assertEqual(base64.b64decode(block['data']), png())
        self.assertEqual(result['structuredContent']['model_input'], 'unverified')
        self.tool('wechat_read_media', args)
        self.assertEqual(len(self.cdn.calls), 1)
        self.assertEqual(os.stat(self.directory / 'messages.sqlite').st_mode & 0o077, 0)

    def test_wav_voice_is_audio_block_and_transcript_is_labeled(self):
        self.ingest(self.message([voice_item('外部转写')]))
        self.assertIn('微信提供的语音转写', self.read()['text'])
        result = self.tool('wechat_read_media', self.media_args())
        self.assertEqual(result['content'][1]['type'], 'audio')
        self.assertEqual(base64.b64decode(result['content'][1]['data']), wav())

    def test_silk_without_decoder_never_claims_model_audio(self):
        self.cdn.voice = crypt(b'\x02#!SILK_V3' + b'SYNTHETIC-CODEC-DATA', KEY, True)
        self.ingest(self.message([voice_item()]))
        result = self.tool('wechat_read_media', self.media_args())
        self.assertEqual(result['structuredContent']['model_input'], 'decoder_required')
        self.assertEqual(len(result['content']), 1)

    def test_injected_decoder_validates_wav_output(self):
        self.cdn.voice = crypt(b'#!SILK_V3' + b'SYNTHETIC-CODEC-DATA', KEY, True)
        self.b.silk_decoder = lambda raw: wav()
        self.ingest(self.message([voice_item()]))
        self.assertEqual(self.tool('wechat_read_media', self.media_args())['content'][1]['mimeType'], 'audio/wav')
        self.b.silk_decoder = lambda raw: b'invalid-wav'
        self.ingest(self.message([voice_item()], mid=124))
        msg = self.broker_call('read', {})['messages'][1]
        result = self.tool('wechat_read_media', {'message_id': msg['message_id'], 'media_id': msg['attachments'][0]['media_id']})
        self.assertTrue(result['isError'])

    def test_image_send_matches_upload_encryption_and_message_fields(self):
        self.ingest()
        args = self.reply_args()
        result = self.tool('wechat_reply_media', args)['structuredContent']
        self.assertEqual(result['state'], 'api_accepted')
        self.assertEqual(result['mobile_receipt'], 'unverified')
        upload = self.sent[0][2]
        self.assertEqual(upload['media_type'], 1)
        self.assertEqual(upload['filesize'], len(self.cdn.calls[0][2]))
        self.assertEqual(crypt(self.cdn.calls[0][2], bytes.fromhex(upload['aeskey'])), png())
        msg = self.sent[1][2]['msg']
        self.assertEqual(msg['to_user_id'], self.binding.user_id)
        self.assertEqual(msg['context_token'], 'SYNTHETIC-CONTEXT')
        item = msg['item_list'][0]
        self.assertEqual(item['type'], 2)
        self.assertEqual(aes_key(item['image_item']['media']['aes_key']), bytes.fromhex(upload['aeskey']))
        self.tool('wechat_reply_media', args)
        self.assertEqual(len(self.sent), 2); self.assertEqual(len(self.cdn.calls), 1)

    def test_wav_outbound_is_file_attachment_never_native_voice(self):
        self.ingest()
        result = self.tool('wechat_reply_media', self.reply_args('audio_attachment'))['structuredContent']
        self.assertFalse(result['native_voice'])
        self.assertEqual(self.sent[0][2]['media_type'], 3)
        self.assertEqual(self.sent[1][2]['msg']['item_list'][0]['type'], 4)
        self.assertEqual(result['delivery_mode'], 'audio_attachment')

    def test_native_voice_request_is_rejected_before_upload(self):
        self.ingest(); args = self.reply_args(); args['delivery_mode'] = 'native_voice'
        self.assertTrue(self.tool('wechat_reply_media', args)['isError'])
        self.assertEqual(self.sent, []); self.assertEqual(self.cdn.calls, [])

    def test_unknown_media_send_result_survives_restart_without_retry(self):
        self.ingest(); args = self.reply_args(); self.ack = {}
        self.assertEqual(self.tool('wechat_reply_media', args)['structuredContent']['state'], 'unknown')
        self.b.adapter(recover=True).close()
        self.assertEqual(self.tool('wechat_reply_media', args)['structuredContent']['state'], 'unknown')
        self.assertEqual(len(self.sent), 2)

    def test_upload_timeout_persists_ambiguity_and_does_not_resend(self):
        self.ingest(); args = self.reply_args(); self.cdn.fail_upload = True
        self.assertEqual(self.tool('wechat_reply_media', args)['structuredContent']['state'], 'unknown')
        self.tool('wechat_reply_media', args)
        self.assertEqual(len(self.cdn.calls), 1); self.assertEqual(len(self.sent), 1)

    def test_text_and_media_share_idempotency_namespace(self):
        self.ingest(); args = self.reply_args()
        self.tool('wechat_reply_media', args)
        result = self.tool('wechat_reply_text', {'message_id': args['message_id'], 'text': 'different', 'idempotency_key': args['idempotency_key']})
        self.assertTrue(result['isError']); self.assertEqual(len(self.sent), 2)

    def test_read_media_id_can_be_replied_without_model_copying_base64(self):
        self.ingest(); media = self.media_args(); self.tool('wechat_read_media', media)
        args = self.reply_args(); args.pop('data_base64'); args['media_id'] = media['media_id']
        result = self.tool('wechat_reply_media', args)
        self.assertEqual(result['structuredContent']['state'], 'api_accepted')
        self.assertEqual(len(self.cdn.calls), 2)
        schema = next(t['inputSchema'] for t in self.s.tools()['tools'] if t['name'] == 'wechat_reply_media')
        jsonschema.validate(args, schema)
        with self.assertRaises(jsonschema.ValidationError): jsonschema.validate(dict(args, data_base64='extra'), schema)

    def test_operator_staging_only_and_cross_message_or_revoked_output_rejected(self):
        self.ingest()
        staged = self.broker_call('stage_media', {'message_id': self.read()['message_id'],
                      'data_base64': base64.b64encode(png()).decode(), 'mime_type': 'image/png'})
        self.assertFalse(staged['sent']); self.assertEqual(self.sent, []); self.assertEqual(self.cdn.calls, [])
        args = self.reply_args(); args.pop('data_base64'); args['media_id'] = staged['media_id']
        other = dict(args, message_id='other-message')
        self.assertTrue(self.tool('wechat_reply_media', other)['isError'])
        self.tool('wechat_forget_media', {'message_id': args['message_id']})
        self.assertTrue(self.tool('wechat_reply_media', args)['isError'])
        self.assertEqual(self.sent, [])

    def test_context_age_mime_spoof_and_non_tencent_upload_fail_closed(self):
        self.ingest(); args = self.reply_args()
        bad = dict(args, mime_type='image/jpeg')
        self.assertTrue(self.tool('wechat_reply_media', bad)['isError']); self.assertEqual(self.sent, [])
        self.now += 3601
        self.assertTrue(self.tool('wechat_reply_media', args)['isError']); self.assertEqual(self.sent, [])
        self.now = 1000
        self.b.exchange = lambda *a: {'upload_full_url': 'https://attacker.example/c2c/upload'}
        self.assertEqual(self.tool('wechat_reply_media', args)['structuredContent']['state'], 'unknown')
        self.assertEqual(self.cdn.calls, [])

    def test_invalid_download_is_tool_error_and_not_cached(self):
        self.ingest(); self.cdn.image = b'invalid encrypted image'
        result = self.tool('wechat_read_media', self.media_args())
        self.assertTrue(result['isError'])
        a = self.b.adapter()
        try: self.assertIsNone(a.db.execute('SELECT blob FROM media').fetchone()[0])
        finally: a.close()

    def test_media_cache_quota_applies_to_downloads_and_staged_output(self):
        self.ingest()
        with patch('media_adapter.MAX_CACHE', 1):
            self.assertTrue(self.tool('wechat_read_media', self.media_args())['isError'])
            with self.assertRaises(MediaError):
                self.broker_call('stage_media', {'message_id': self.read()['message_id'],
                                 'data_base64': base64.b64encode(png()).decode(), 'mime_type': 'image/png'})

    def test_staged_output_can_be_sent_by_opaque_id(self):
        self.ingest(); args = self.reply_args('audio_attachment')
        staged = self.broker_call('stage_media', {k: args[k] for k in ['message_id', 'data_base64', 'mime_type']})
        args.pop('data_base64'); args['media_id'] = staged['media_id']
        self.assertEqual(self.tool('wechat_reply_media', args)['structuredContent']['state'], 'api_accepted')
        self.assertEqual(self.sent[-1][2]['msg']['item_list'][0]['type'], 4)

    def test_forget_and_expiry_prevent_future_downloads(self):
        self.ingest(); args = self.media_args()
        self.tool('wechat_read_media', args)
        self.tool('wechat_forget_media', {'message_id': args['message_id']})
        self.assertTrue(self.tool('wechat_read_media', args)['isError'])
        self.assertEqual(len(self.cdn.calls), 1)
        a = self.b.adapter()
        try: self.assertEqual(a.db.execute('SELECT reference,blob FROM media').fetchone(), (None, None))
        finally: a.close()
        self.ingest(self.message(mid=124)); self.now += 86401
        messages = self.broker_call('read', {})['messages']
        second = messages[1]
        self.assertEqual(second['attachments'][0]['state'], 'expired')

    def test_other_owner_group_and_missing_id_are_rejected_without_download(self):
        for extra in [{'from_user_id': 'OTHER'}, {'to_user_id': 'OTHER'}, {'group_id': 'GROUP'}, {'message_id': None}, {'message_state': 1}]:
            self.assertEqual(self.ingest(self.message(**extra))['accepted'], 0)
        self.assertEqual(self.cdn.calls, [])

    def test_unknown_id_and_cross_message_attachment_never_download(self):
        self.ingest(); args = self.media_args(); args['message_id'] = 'unknown'
        self.assertTrue(self.tool('wechat_read_media', args)['isError'])
        self.assertEqual(self.cdn.calls, [])
        args = self.reply_args(); args['message_id'] = 'unknown'
        self.assertTrue(self.tool('wechat_reply_media', args)['isError'])
        self.assertEqual(self.sent, [])

    def test_plain_text_upgrade_keeps_id_and_digest(self):
        msg = self.message([{'type': 1, 'text_item': {'text': 'original'}}])
        original = TextAdapter(self.directory / 'messages.sqlite', self.binding)
        try: original.ingest({'msgs': [msg]})
        finally: original.close()
        self.assertEqual(self.ingest(msg)['duplicates'], 1)
        self.assertEqual(self.read()['text'], 'original')
        self.assertEqual(self.read()['attachments'], [])

    def test_all_schemas_validate_actual_results_and_exact_lookup_paginates(self):
        self.ingest(); msg = self.read()
        schemas = {t['name']: t for t in self.s.tools()['tools']}
        calls = [('wechat_read_messages', {}), ('wechat_read_messages', {'message_id': msg['message_id']}),
                 ('wechat_read_pending_messages', {'limit': 1}),
                 ('wechat_read_status', {}), ('wechat_read_media', self.media_args()),
                 ('wechat_reply_media', self.reply_args()), ('wechat_forget_media', {'message_id': msg['message_id']})]
        for name, args in calls:
            jsonschema.validate(args, schemas[name]['inputSchema'])
            result = self.tool(name, args)
            jsonschema.validate(result['structuredContent'], schemas[name]['outputSchema'])
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate({'message_id': msg['message_id'], 'after_seq': 0}, schemas['wechat_read_messages']['inputSchema'])
        self.assertEqual(self.tool('wechat_read_messages', {'after_seq': msg['local_seq']})['structuredContent']['messages'], [])
        self.assertEqual(self.tool('wechat_read_pending_messages', {})['structuredContent']['messages'], [])

    def test_missing_payload_completion_failure_has_validation_stage(self):
        meta = {'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {}}
        bad = process(self.s, {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {'_meta': meta,
                      'name': 'wechat_reply_text', 'arguments': {'text': 'completion without message_id'}}})
        self.assertEqual(bad['error']['code'], -32602)
        stage, status = self.s.db.execute('SELECT stage,http_status FROM diagnostics ORDER BY rowid DESC LIMIT 1').fetchone()
        self.assertEqual(stage, 'tool_validation'); self.assertIsNone(status)
        self.assertEqual(self.sent, [])
        self.ingest()
        good = process(self.s, {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'_meta': meta,
                       'name': 'wechat_reply_text', 'arguments': {'text': 'completion', 'message_id': self.read()['message_id'], 'idempotency_key': 'completion-once'}}})
        self.assertEqual(good['result']['structuredContent']['state'], 'api_accepted')

    def test_broker_unavailable_is_tool_error_and_redacts_arbitrary_exception(self):
        def fail(*args): raise OSError('SYNTHETIC-CONTEXT /private/url?credential=secret')
        self.s.wechat = fail
        result = self.tool('wechat_read_status', {})
        self.assertTrue(result['isError']); self.assertNotIn('credential', json.dumps(result))
        self.assertEqual(self.s.db.execute('SELECT stage FROM diagnostics ORDER BY rowid DESC LIMIT 1').fetchone()[0], 'broker_operation')

    def test_real_unix_ipc_transfers_content_and_large_frame(self):
        self.cdn.voice = crypt(wav(30), KEY, True)
        self.ingest(self.message([voice_item()]))
        path = self.directory / 'broker.sock'
        server = Server(str(path), Handler); server.broker = self.b; os.chmod(path, 0o600)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            result = call('read_media', self.media_args(), path)
            self.assertGreater(len(result['content'][0]['data']), 1048576)
            self.assertEqual(base64.b64decode(result['content'][0]['data']), wav(30))
            with self.assertRaises(BrokerError): call('read_media', {'message_id': 'unknown', 'media_id': 'unknown'}, path)
        finally: server.shutdown(); server.server_close(); thread.join()

    def test_large_media_stdio_frame_is_complete(self):
        self.ingest(); raw = wav(30)
        args = self.reply_args('audio_attachment'); args['data_base64'] = base64.b64encode(raw).decode()
        request = {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'name': 'wechat_reply_media', 'arguments': args,
                   '_meta': {'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {}}}}
        sink = io.BytesIO()
        run_stdio(self.s, io.BytesIO(json.dumps(request).encode() + b'\n'), sink)
        self.assertEqual(json.loads(sink.getvalue())['result']['structuredContent']['state'], 'api_accepted')

    def test_disabled_media_catalog_never_exposes_new_tools(self):
        other = PersistentService(self.directory / 'plain.sqlite', wechat=self.broker_call)
        try: self.assertNotIn('wechat_read_media', [t['name'] for t in other.tools()['tools']])
        finally: other.close()


class ValidationTests(unittest.TestCase):
    def test_key_encodings_and_image_precedence(self):
        for encoded in [base64.b64encode(KEY).decode(), base64.b64encode(KEY.hex().encode()).decode()]:
            self.assertEqual(aes_key(encoded), KEY)
        field = image_item()['image_item']; field['aeskey'] = KEY.hex(); field['media']['aes_key'] = 'invalid'
        self.assertEqual(reference(field, 'image')['key'], KEY.hex())
        with self.assertRaises(MediaError): aes_key(base64.b64encode(b'wrong').decode())

    def test_pkcs7_full_block_and_bad_ciphertext(self):
        raw = b'a' * 16
        self.assertEqual(len(crypt(raw, KEY, True)), 32)
        self.assertEqual(crypt(crypt(raw, KEY, True), KEY), raw)
        for bad in [b'', b'broken', b'\0' * 16]:
            with self.assertRaises(MediaError): crypt(bad, KEY)

    def test_mime_spoof_svg_truncated_audio_duration_and_size(self):
        for raw, kind, declared in [(png(), 'image', 'image/jpeg'), (b'<svg/>', 'image', None),
                                    (wav()[:-2], 'audio', None), (wav(61), 'audio', None),
                                    (b'x' * (MAX_MEDIA + 1), 'image', None)]:
            with self.assertRaises(MediaError): validate_media(raw, kind, declared)
        for encoded in ['not base64!', '', 'x' * (6 * 1024 * 1024)]:
            with self.assertRaises(MediaError): decode_base64(encoded)

    def test_cdn_url_forbids_private_other_hosts_paths_credentials_redirects(self):
        for url in ['http://' + CDN_HOST + '/c2c/download', 'https://127.0.0.1/c2c/download',
                    'https://attacker.example/c2c/download', 'https://user@' + CDN_HOST + '/c2c/download',
                    CDN_BASE + '/download#fragment', CDN_BASE + '/../private', CDN_BASE + '/download\n']:
            field = image_item()['image_item']; field['media']['full_url'] = url
            with self.assertRaises(MediaError): reference(field, 'image')

    def test_cdn_checks_all_dns_addresses_and_never_constructs_connection(self):
        created = []
        cdn = CDNHTTPS(enabled=True, resolver=lambda *a, **k: [(0,0,0,'',('1.1.1.1',443)),(0,0,0,'',('127.0.0.1',443))],
                       factory=lambda *a: created.append(a))
        with self.assertRaises(MediaError): cdn.request('GET', CDN_BASE + '/download?encrypted_query_param=fixture')
        self.assertEqual(created, [])

    def test_cdn_response_limits_upload_ack_redirect_and_no_bot_headers(self):
        class Response:
            status = 200
            headers = {'x-encrypted-param': 'fixture'}
            data = b''
            def read(self, n): return self.data[:n]
            def getheader(self, key): return self.headers.get(key)
        class Connection:
            def request(self, method, target, body=None, headers=None): self.headers = headers
            def getresponse(self): return response
            def close(self): pass
        response = Response(); conn = Connection()
        cdn = CDNHTTPS(enabled=True, resolver=lambda *a, **k: [(0,0,0,'',('1.1.1.1',443))], factory=lambda *a: conn)
        self.assertEqual(cdn.request('POST', CDN_BASE + '/upload?fixture=1', b'\0' * 16), 'fixture')
        self.assertEqual(set(conn.headers), {'Host', 'Content-Type'})
        response.headers = {}
        with self.assertRaises(MediaError): cdn.request('POST', CDN_BASE + '/upload', b'\0' * 16)
        response.status = 302
        with self.assertRaises(MediaError): cdn.request('GET', CDN_BASE + '/download')
        response.status = 200; response.headers = {'Content-Length': str(MAX_MEDIA + 17)}
        with self.assertRaises(MediaError): cdn.request('GET', CDN_BASE + '/download')
        response.headers = {}; response.data = b'x' * (MAX_MEDIA + 17)
        with self.assertRaises(MediaError): cdn.request('GET', CDN_BASE + '/download')


if __name__ == '__main__': unittest.main()
