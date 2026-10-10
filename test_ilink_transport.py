import json
import os
from pathlib import Path
import tempfile
import unittest
from ilink_transport import ILinkHTTPS
from ilink_login_ui import save_new_binding
from ilink_adapter import Binding, API_HOST
from outbound import CallbackError
from proxy_outbound import parse_answers


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.status = 200
        self.body = b'{"ret":0}'
        test = self
        class Conn:
            def request(self, *args, **kwargs):
                test.calls.append((args, kwargs))
            def getresponse(self):
                return self
            @property
            def status(self):
                return test.status
            def read(self, size):
                return test.body[:size]
            def close(self):
                pass
        def factory(host, ip, timeout):
            self.target = host, ip, timeout
            return Conn()
        self.t = ILinkHTTPS(resolver=lambda *a, **k: [(2, 1, 6, '', ('1.1.1.2', 443))], factory=factory)

    def test_numeric_pin_original_host_and_text_endpoint(self):
        self.assertEqual(self.t.request('POST', '/ilink/bot/getupdates', {}, {})['ret'], 0)
        self.assertEqual(self.target[:2], (API_HOST, '1.1.1.2'))
        self.assertEqual(self.calls[0][1]['headers']['Host'], API_HOST)

    def test_endpoint_redirect_and_nonpublic_rejected(self):
        with self.assertRaises(CallbackError):
            self.t.request('GET', '/other', {})
        self.status = 302
        with self.assertRaisesRegex(CallbackError, 'redirect_forbidden'):
            self.t.request('GET', '/ilink/bot/get_qrcode_status?qrcode=SYNTHETIC', {})
        self.t.resolver = lambda *a, **k: [(2, 1, 6, '', ('127.0.0.1', 443))]
        with self.assertRaisesRegex(CallbackError, 'non_public_address'):
            self.t.request('POST', '/ilink/bot/sendmessage', {}, {})

    def test_exact_ilink_dns_question(self):
        data = {'Status': 0, 'TC': False, 'Question': [{'name': API_HOST, 'type': 1}],
                'Answer': [{'name': API_HOST, 'type': 1, 'data': '1.1.1.2'}]}
        self.assertEqual(parse_answers(data, 1, API_HOST), ['1.1.1.2'])
        with self.assertRaises(CallbackError):
            parse_answers(data, 1)

    def test_private_exclusive_credential_storage(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / 'isolated'
            save_new_binding(Binding('SYNTHETIC-BOT', 'SYNTHETIC-OWNER', 'SYNTHETIC-TOKEN'), directory)
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((directory / 'binding.json').stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                save_new_binding(Binding('other', 'other', 'SYNTHETIC'), directory)

    def test_reuses_separate_poll_and_send_connections_without_resolving_again(self):
        created, resolved = [], []
        class Conn:
            status = 200
            will_close = False
            def __init__(self): self.closed = False
            def request(self, *args, **kwargs): pass
            def getresponse(self): return self
            def read(self, _): return b'{"ret":0}'
            def close(self): self.closed = True
        def factory(*_):
            value = Conn(); created.append(value); return value
        def resolver(*_, **kw):
            resolved.append(1); return [(2, 1, 6, '', ('1.1.1.2', 443))]
        transport = ILinkHTTPS(resolver=resolver, factory=factory)
        for _ in range(2):
            transport.exchange('/ilink/bot/getupdates', {}, {})
            transport.exchange('/ilink/bot/sendmessage', {}, {})
        self.assertEqual(len(created), 2)
        self.assertEqual(len(resolved), 2)
        self.assertIsNot(transport.connections['poll'], transport.connections['send'])
        transport.close()
        self.assertTrue(all(c.closed for c in created))

    def test_ambiguous_send_is_never_retried_on_a_new_connection(self):
        attempts, created = [], []
        class Conn:
            def request(self, *_, **kw):
                attempts.append(1)
                raise ConnectionResetError('synthetic after write')
            def close(self): pass
        def factory(*_): created.append(1); return Conn()
        transport = ILinkHTTPS(resolver=self.t.resolver, factory=factory)
        with self.assertRaises(CallbackError): transport.exchange('/ilink/bot/sendmessage', {}, {})
        self.assertEqual(len(attempts), 1)
        self.assertEqual(len(created), 1)
        self.assertEqual(transport.connections, {})


if __name__ == '__main__':
    unittest.main()
