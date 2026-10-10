import copy
import json
import socket
import ssl
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, MagicMock
from outbound import CallbackError
from proxy_outbound import (CALLBACK_HOST, DOH_HOST, DOH_BOOTSTRAP, PROXY,
    CloudflareResolver, ProxyPinnedHTTPSConnection, parse_answers, proxy_transport)


def answer(kind=1, addresses=('8.8.8.8',), ttl=300):
    return {'Status': 0, 'TC': False,
        'Question': [{'name': CALLBACK_HOST+'.', 'type': kind}],
        'Answer': [{'name': CALLBACK_HOST+'.', 'type': kind, 'data': ip, 'TTL': ttl} for ip in addresses]}


class DNSFixture:
    def __init__(self):
        self.clock = 1000
        self.calls = []
        self.data = {1: answer(), 28: answer(28, ('2606:4700:4700::1111',))}
        self.age = '0'
        self.duration = 0
        self.on_read = lambda: None
        self.resolver = CloudflareResolver(self.connection, now=lambda: self.clock)

    def connection(self, *_):
        fixture = self
        class Conn:
            def request(self, method, path, headers):
                self.kind = 28 if 'type=28' in path else 1
                fixture.calls.append(self.kind)
            def getresponse(self): return self
            status = 200
            def getheader(self, name, default=None): return fixture.age
            def read(self, _):
                fixture.on_read()
                fixture.clock += fixture.duration
                return json.dumps(fixture.data[self.kind]).encode()
            def close(self): pass
        return Conn()

    def resolve(self):
        return self.resolver(CALLBACK_HOST, 443, type=socket.SOCK_STREAM)


class ProxyTests(unittest.TestCase):
    def test_private_mixed_benchmark_answers_rejected(self):
        for ips in [('10.0.0.1',), ('8.8.8.8', '127.0.0.1'), ('198.18.0.1',), ('::1',)]:
            with self.subTest(ips=ips), self.assertRaises(CallbackError):
                parse_answers(answer(28 if ':' in ips[0] else 1, ips), 28 if ':' in ips[0] else 1)

    def test_malformed_question_truncation_alias_and_unrelated(self):
        data = answer()
        for modified in [{**data, 'TC': True}, {**data, 'Status': 3},
                         {**data, 'Question': [{'name': 'evil.test', 'type': 1}]},
                         {**data, 'Answer': [{'name': 'evil.test', 'type': 1, 'data': '8.8.8.8'}]}]:
            with self.assertRaises(CallbackError): parse_answers(modified, 1)
        data['Answer']=[{'name': CALLBACK_HOST, 'type': 5, 'data':'alias.test.'},
                        {'name':'alias.test.', 'type':1,'data':'8.8.8.8'}]
        self.assertEqual(parse_answers(data,1), ['8.8.8.8'])

    def test_connect_numeric_tls_original_and_fixed_proxy(self):
        c=ProxyPinnedHTTPSConnection(CALLBACK_HOST,'8.8.8.8',10)
        helper=MagicMock(); raw=helper.sock
        with patch('proxy_outbound.http.client.HTTPConnection',return_value=helper) as constructor, patch.object(c._context,'wrap_socket') as wrap:
            c.connect()
            constructor.assert_called_once_with(PROXY[0],PROXY[1],timeout=10)
            helper.set_tunnel.assert_called_once_with('8.8.8.8',443)
            wrap.assert_called_once_with(raw,server_hostname=CALLBACK_HOST)
        self.assertTrue(c._context.check_hostname)
        self.assertEqual(c._context.verify_mode,ssl.CERT_REQUIRED)

    def test_bad_certificate_preserved_and_socket_closed(self):
        c=ProxyPinnedHTTPSConnection(CALLBACK_HOST,'8.8.8.8',10)
        helper=MagicMock();raw=helper.sock
        with patch('proxy_outbound.http.client.HTTPConnection',return_value=helper), patch.object(c._context,'wrap_socket',side_effect=ssl.SSLCertVerificationError('synthetic')):
            with self.assertRaises(ssl.SSLCertVerificationError):c.connect()
            raw.close.assert_called_once()

    def test_exact_query_bootstrap_no_system_dns(self):
        calls=[]
        class Conn:
            def __init__(self,host,ip,timeout):calls.append((host,ip));self.kind=None
            def request(self,method,path,headers):
                self.kind=28 if 'type=28' in path else 1
                self.assertion=(method,path,headers)
                assert method=='GET' and 'name='+CALLBACK_HOST in path
                assert headers=={'Accept':'application/dns-json','Host':DOH_HOST}
            def getresponse(self):
                raw=json.dumps(answer(self.kind,('8.8.8.8',) if self.kind==1 else ())).encode()
                return type('R',(),{'status':200,'read':lambda _,n:raw,
                                  'getheader':lambda _,name,default=None:default})()
            def close(self):pass
        with patch('socket.getaddrinfo',side_effect=AssertionError('system DNS forbidden')):
            result=CloudflareResolver(Conn)(CALLBACK_HOST,443,type=socket.SOCK_STREAM)
        self.assertEqual(result[0][4][0],'8.8.8.8')
        self.assertEqual(calls,[(DOH_HOST,DOH_BOOTSTRAP)]*2)
        with self.assertRaises(CallbackError):CloudflareResolver(Conn)('evil.test',443,type=socket.SOCK_STREAM)

    def test_cache_reuses_both_families_without_system_dns(self):
        fixture = DNSFixture()
        with patch('socket.getaddrinfo', side_effect=AssertionError('system DNS forbidden')):
            first = fixture.resolve()
            self.assertEqual(fixture.resolve(), first)
            first.clear()  # Callers cannot mutate the cached answer.
            self.assertEqual(len(fixture.resolve()), 2)
        self.assertEqual(fixture.calls, [1, 28])
        with self.assertRaises(CallbackError):
            fixture.resolver('evil.test', 443, type=socket.SOCK_STREAM)

    def test_cache_cname_ttl_age_and_lookup_time_bound_expiry_per_family(self):
        fixture = DNSFixture()
        fixture.data[1]['Answer'][0]['name'] = 'alias.test.'
        fixture.data[1]['Answer'].insert(0,
            {'name': CALLBACK_HOST, 'type': 5, 'data': 'alias.test.', 'TTL': 30})
        fixture.age, fixture.duration = '5', 2
        fixture.resolve()  # IPv4 expires at 1025; lookup finishes at 1004.
        fixture.clock = 1024.9
        fixture.resolve()
        self.assertEqual(fixture.calls, [1, 28])
        fixture.clock = 1025
        fixture.resolve()
        self.assertEqual(fixture.calls, [1, 28, 1])

    def test_cache_caps_large_ttls(self):
        fixture = DNSFixture()
        for data in fixture.data.values(): data['Answer'][0]['TTL'] = 3600
        fixture.resolve()
        fixture.clock += 299
        fixture.resolve()
        self.assertEqual(len(fixture.calls), 2)
        fixture.clock += 1
        fixture.resolve()
        self.assertEqual(fixture.calls, [1, 28, 1, 28])

    def test_invalid_zero_missing_or_aged_ttl_does_not_cache(self):
        for ttl, age in [(0, '0'), (-1, '0'), (True, '0'), ('30', '0'),
                         (None, '0'), (2**31, '0'), (30, '30'),
                         (30, '-1'), (30, 'bad')]:
            with self.subTest(ttl=ttl, age=age):
                fixture = DNSFixture()
                for data in fixture.data.values():
                    if ttl is None: data['Answer'][0].pop('TTL')
                    else: data['Answer'][0]['TTL'] = ttl
                fixture.age = age
                fixture.resolve()
                fixture.resolve()
                self.assertEqual(fixture.calls, [1, 28, 1, 28])

    def test_empty_family_not_cached_without_negative_ttl(self):
        fixture = DNSFixture()
        fixture.data[28] = answer(28, ())
        fixture.resolve()
        fixture.resolve()
        self.assertEqual(fixture.calls, [1, 28, 28])

    def test_failed_or_mixed_response_never_cached_or_served_stale(self):
        fixture = DNSFixture()
        fixture.resolve()
        fixture.clock += 301
        fixture.data[28] = answer(28, ('2606:4700:4700::1111', '::1'))
        for _ in range(2):
            with self.assertRaisesRegex(CallbackError, 'non_public_address'):
                fixture.resolve()
        self.assertEqual(fixture.calls, [1, 28] * 3)
        fixture.data[28] = answer(28, ('2606:4700:4700::1111',))
        fixture.resolve()
        self.assertEqual(fixture.calls, [1, 28] * 4)

    def test_concurrent_misses_share_one_lookup(self):
        fixture = DNSFixture()
        reading, release, second_started = threading.Event(), threading.Event(), threading.Event()
        def on_read():
            reading.set()
            self.assertTrue(release.wait(2))
        def second():
            second_started.set()
            return fixture.resolve()
        fixture.on_read = on_read
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(fixture.resolve)
            try:
                self.assertTrue(reading.wait(2))
                other = pool.submit(second)
                self.assertTrue(second_started.wait(2))
            finally:
                release.set()
            self.assertEqual(first.result(timeout=2), other.result(timeout=2))
        self.assertEqual(fixture.calls, [1, 28])

    def test_doh_redirect_rejected_without_following(self):
        calls=[]
        class Conn:
            def __init__(self,*_):calls.append(1)
            def request(self,*_,**kw):pass
            def getresponse(self):return type('R',(),{'status':302})()
            def close(self):pass
        with self.assertRaises(CallbackError) as caught:
            CloudflareResolver(Conn)(CALLBACK_HOST,443,type=socket.SOCK_STREAM)
        self.assertEqual(caught.exception.http_status,302)
        self.assertEqual(len(calls),1)

    def test_exact_callback_port_bootstrap_and_no_credentials(self):
        for url in ['http://'+CALLBACK_HOST+'/', 'https://'+CALLBACK_HOST+':444/',
                    'https://evil.test/', 'https://user:pass@'+CALLBACK_HOST+'/']:
            with self.assertRaises(CallbackError):proxy_transport([CALLBACK_HOST],True).validate_url(url)
        for host,ip in [(CALLBACK_HOST,'127.0.0.1'),(DOH_HOST,'8.8.8.8'),('evil.test','8.8.8.8')]:
            with self.assertRaises(CallbackError):ProxyPinnedHTTPSConnection(host,ip,10)


if __name__=='__main__':unittest.main()
