"""Opaque native files: correlation, byte preservation, failure and replay safety."""
import base64
import unittest
from dot_auth import DotError
from dot_runner import Gateway
import test_dot_images as image_tests
from test_dot_room_gateway import BINDING
from wechat_media import crypt, validate_file, MediaError, MAX_MEDIA


class FileGatewayTests(unittest.IsolatedAsyncioTestCase):
    tearDown = image_tests.ImageGatewayTests.tearDown
    call = image_tests.ImageGatewayTests.call
    rpc = image_tests.ImageGatewayTests.rpc
    ingest = image_tests.ImageGatewayTests.ingest
    own = image_tests.ImageGatewayTests.own
    reply = image_tests.ImageGatewayTests.reply
    admitted = image_tests.ImageGatewayTests.admitted
    def setUp(self):
        image_tests.ImageGatewayTests.setUp(self)
        self.b.files_enabled = True

    def file_reply(self, **changes):
        return self.reply(images=[], files=[{'attachment_id': 'document'}], **changes)

    def room(self, name='报告.txt', error=None):
        class Room:
            async def invoke(self, op, args):
                if op != 'download_file' or args['attachment_id'] != 'document':
                    raise AssertionError('wrong native download')
                if error:
                    raise DotError(error)
                return {'data_base64': base64.b64encode(b'opaque\x00\xff\n').decode(), 'file_name': name}
        return Room()

    async def test_file_bytes_name_dedup_and_restart(self):
        self.admitted()
        self.call('dot_room_message', message=self.file_reply())
        self.call('dot_room_message', message=self.file_reply())
        self.assertEqual(self.call('dot_output_next')['kind'], 'file')
        await Gateway(self.rpc, self.room(), BINDING).drain()
        self.call('dot_recover')
        await Gateway(self.rpc, self.room(), BINDING).drain()
        self.assertEqual(len(self.sent), 1)
        item = self.sent[0]['msg']['item_list'][0]
        self.assertEqual(item['type'], 4)
        field = item['file_item']
        self.assertEqual(field['file_name'], '报告.txt')
        key = bytes.fromhex(base64.b64decode(field['media']['aes_key']).decode())
        self.assertEqual(crypt(self.cdn.calls[0][2], key, False), b'opaque\x00\xff\n')

    async def test_filename_is_part_of_idempotency_identity(self):
        job = self.admitted()
        adapter = self.b.adapter()
        try:
            args = (job['message_id'], 'application/octet-stream', 'file-key', 'file_attachment')
            encoded = base64.b64encode(b'content').decode()
            adapter.reply_media(*args, data_base64=encoded, file_name='a.txt', _route='direct')
            with self.assertRaisesRegex(MediaError, 'reply_idempotency_conflict'):
                adapter.reply_media(*args, data_base64=encoded, file_name='b.txt', _route='direct')
            self.assertEqual(len(self.sent), 1)
        finally:
            adapter.close()

    async def test_file_unknown_does_not_retry(self):
        self.admitted(); self.send_failure = True
        self.call('dot_room_message', message=self.file_reply())
        gateway = Gateway(self.rpc, self.room(), BINDING)
        await gateway.drain(); self.call('dot_recover'); await gateway.drain()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(len(self.cdn.calls), 1)

    async def test_bad_file_becomes_notice_and_next_reply_flows(self):
        self.admitted()
        self.call('dot_room_message', message=self.file_reply())
        self.call('dot_room_message', message=self.reply(id='next', images=[], text='后续回复'))
        await Gateway(self.rpc, self.room(error='dot_file_too_large'), BINDING).drain()
        self.assertEqual(len(self.sent), 2)
        self.assertIn('文件未能回传', str(self.sent[0]))
        self.assertIn('后续回复', str(self.sent[1]))

    async def test_invalid_filename_becomes_notice(self):
        self.admitted(); self.call('dot_room_message', message=self.file_reply())
        await Gateway(self.rpc, self.room(name='../bad.txt'), BINDING).drain()
        self.assertEqual(len(self.cdn.calls), 0)
        self.assertIn('文件未能回传', str(self.sent[0]))

    async def test_foreign_parent_never_downloads(self):
        self.admitted()
        self.assertFalse(self.call('dot_room_message', message=self.file_reply(replyToMessageId='foreign'))['accepted'])
        self.assertIsNone(self.call('dot_output_next'))

    async def test_flag_off_does_not_enqueue_file(self):
        self.b.files_enabled = False
        self.admitted(); self.call('dot_room_message', message=self.file_reply())
        self.assertEqual(self.call('dot_output_next')['kind'], 'text')


class FileValidationTests(unittest.TestCase):
    def test_size_and_filename_bounds(self):
        for name in ('../a', 'a\\b', '', '.', 'x\n.txt', '文' * 81):
            with self.assertRaises(MediaError): validate_file(b'bytes', name)
        for raw in (b'', b'x' * (MAX_MEDIA + 1)):
            with self.assertRaises(MediaError): validate_file(raw, 'file.zip')
        validate_file(b'x' * MAX_MEDIA, '原始文件.zip')
