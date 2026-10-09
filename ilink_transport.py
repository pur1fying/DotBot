"""Broker-only exact iLink HTTPS transport. No logging or environment credentials."""
import json
import socket
import ssl
from outbound import CallbackError, destination
from proxy_outbound import CloudflareResolver, ProxyPinnedHTTPSConnection, DOH_HOST
from ilink_adapter import API_HOST


def connection(host, address, timeout):
    return ProxyPinnedHTTPSConnection(host, address, timeout, allowed_hosts=(API_HOST, DOH_HOST))


class ILinkHTTPS:
    def __init__(self, resolver=None, factory=connection):
        self.resolver = resolver or CloudflareResolver(query_host=API_HOST)
        self.factory = factory

    def request(self, method, path, headers, body=None):
        if method not in ('GET', 'POST'):
            raise CallbackError('method_forbidden')
        host, target = destination('https://' + API_HOST + path, {API_HOST})
        allowed = {'/ilink/bot/get_bot_qrcode', '/ilink/bot/get_qrcode_status',
                   '/ilink/bot/getupdates', '/ilink/bot/sendmessage', '/ilink/bot/getuploadurl'}
        if target.split('?')[0] not in allowed:
            raise CallbackError('endpoint_forbidden')
        raw = None if body is None else json.dumps(body, ensure_ascii=False).encode()
        if raw is not None and len(raw) > 262144:
            raise CallbackError('payload_too_large')
        addresses = self.resolver(host, 443, type=socket.SOCK_STREAM)
        from proxy_outbound import public_ip
        ips = [str(public_ip(entry[4][0])) for entry in addresses]
        if not ips:
            raise CallbackError('doh_no_address')
        conn = self.factory(host, ips[0], 40)
        try:
            conn.request(method, target, body=raw, headers=dict(headers, Host=host))
            response = conn.getresponse()
            if 300 <= response.status < 400:
                raise CallbackError('redirect_forbidden')
            if response.status != 200:
                raise CallbackError('api_http_failed', response.status)
            data = response.read(1048577)
            if len(data) > 1048576:
                raise CallbackError('response_too_large')
            result = json.loads(data)
            if not isinstance(result, dict):
                raise CallbackError('invalid_response')
            return result
        except CallbackError:
            raise
        except ssl.SSLCertVerificationError:
            raise CallbackError('tls_certificate_failed') from None
        except ssl.SSLError:
            raise CallbackError('tls_failed') from None
        except TimeoutError:
            raise CallbackError('timeout') from None
        except Exception:
            raise CallbackError('api_exchange_failed') from None
        finally:
            conn.close()

    def exchange(self, path, headers, body):
        return self.request('POST', path, headers, body)
