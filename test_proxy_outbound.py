import copy
import json
import socket
import ssl
import unittest
from unittest.mock import patch, MagicMock
from outbound import CallbackError
from proxy_outbound import (CALLBACK_HOST, DOH_HOST, DOH_BOOTSTRAP, PROXY,
    CloudflareResolver, ProxyPinnedHTTPSConnection, parse_answers, proxy_transport)


def answer(kind=1, addresses=('8.8.8.8',)):
    return {'Status': 0, 'TC': False,
        'Question': [{'name': CALLBACK_HOST+'.', 'type': kind}],
        'Answer': [{'name': CALLBACK_HOST+'.', 'type': kind, 'data': ip} for ip in addresses]}


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
                return type('R',(),{'status':200,'read':lambda _,n:raw})()
            def close(self):pass
        with patch('socket.getaddrinfo',side_effect=AssertionError('system DNS forbidden')):
            result=CloudflareResolver(Conn)(CALLBACK_HOST,443,type=socket.SOCK_STREAM)
        self.assertEqual(result[0][4][0],'8.8.8.8')
        self.assertEqual(calls,[(DOH_HOST,DOH_BOOTSTRAP)]*2)
        with self.assertRaises(CallbackError):CloudflareResolver(Conn)('evil.test',443,type=socket.SOCK_STREAM)

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
