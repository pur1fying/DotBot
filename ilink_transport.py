"""Broker-only exact iLink HTTPS transport. No logging or environment credentials."""
import json
import socket
import ssl
import threading
import time
import select
from outbound import CallbackError, destination
from proxy_outbound import CloudflareResolver, ProxyPinnedHTTPSConnection, DOH_HOST
from ilink_adapter import API_HOST


def connection(host, address, timeout):
    return ProxyPinnedHTTPSConnection(host, address, timeout, allowed_hosts=(API_HOST, DOH_HOST))


class ILinkHTTPS:
    def __init__(self, resolver=None, factory=connection, now=time.monotonic):
        self.resolver = resolver or CloudflareResolver(query_host=API_HOST)
        self.factory = factory
        self.connections = {}
        self.last_used = {}
        self.now = now
        self.locks = {'poll': threading.Lock(), 'send': threading.Lock()}

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
        slot = 'poll' if target.split('?')[0] == '/ilink/bot/getupdates' else 'send'
        with self.locks[slot]:
            return self._exchange(slot, host, method, target, raw, headers)

    def _exchange(self, slot, host, method, target, raw, headers):
        conn = self.connections.get(slot)
        if conn is not None:
            sock = getattr(conn, 'sock', False)
            idle = self.now() - self.last_used.get(slot, 0)
            try:
                # An idle HTTP connection has no application data to receive.
                # Readability may be EOF/TLS close; discard before any new write.
                readable = sock is not False and (sock is None or bool(select.select([sock], [], [], 0)[0]))
            except (OSError, ValueError):
                readable = True
            if idle >= 15 or readable:
                conn.close()
                self.connections.pop(slot, None)
                self.last_used.pop(slot, None)
                conn = None
        if conn is None:
            addresses = self.resolver(host, 443, type=socket.SOCK_STREAM)
            from proxy_outbound import public_ip
            ips = [str(public_ip(entry[4][0])) for entry in addresses]
            if not ips:
                raise CallbackError('doh_no_address')
            conn = self.factory(host, ips[0], 40)
        reusable = False
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
            reusable = not getattr(response, 'will_close', True)
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
            if reusable:
                self.connections[slot] = conn
                self.last_used[slot] = self.now()
            else:
                self.connections.pop(slot, None)
                self.last_used.pop(slot, None)
                conn.close()

    def close(self):
        for slot, lock in self.locks.items():
            with lock:
                conn = self.connections.pop(slot, None)
                self.last_used.pop(slot, None)
                if conn is not None:
                    conn.close()

    def exchange(self, path, headers, body):
        return self.request('POST', path, headers, body)
