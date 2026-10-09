"""Pinned-IP HTTPS callback transport. Default disabled; no proxy or redirects.

Resolve on every attempt; reject the entire DNS answer if any IP is non-public.
Connect to a validated numeric IP, preserving original hostname for TLS and Host.
Test seams inject DNS and HTTP connections; tests never make internet requests.
"""
import http.client
import ipaddress
import json
import socket
import ssl
from urllib.parse import urlsplit


class CallbackError(ValueError):
    def __init__(self, reason, http_status=None):
        super().__init__(reason)
        self.http_status = http_status


class CallbackResponse(dict):
    def __init__(self, category):
        super().__init__()
        self.category = category


def destination(url, allowed_hosts):
    if not isinstance(url, str) or any(ord(c) <= 32 or ord(c) == 127 for c in url):
        raise CallbackError("invalid_url")
    try:
        p = urlsplit(url)
        port = p.port
    except ValueError:
        raise CallbackError("invalid_url") from None
    if p.scheme != "https" or p.username or p.password or p.fragment or port not in (None, 443):
        raise CallbackError("https_443_required")
    host = p.hostname
    if not host or host != host.lower() or host.endswith(".") or host not in allowed_hosts:
        raise CallbackError("host_not_allowlisted")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise CallbackError("ip_literal_forbidden")
    return host, (p.path or "/") + ("?" + p.query if p.query else "")


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, address, timeout):
        super().__init__(host, 443, timeout=timeout, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        # Numeric address cannot undergo a second hostname lookup / DNS rebinding.
        raw = socket.create_connection((self.address, 443), self.timeout)
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


class SafeHTTPS:
    def __init__(self, allowed_hosts=(), enabled=False, resolver=socket.getaddrinfo,
                 connection_factory=PinnedHTTPSConnection):
        self.allowed_hosts = frozenset(allowed_hosts)
        self.enabled, self.resolver, self.connection_factory = enabled, resolver, connection_factory

    def validate_url(self, url):
        return destination(url, self.allowed_hosts)

    def post(self, url, body, headers):
        if not self.enabled:
            raise CallbackError("outbound_disabled")
        host, path = self.validate_url(url)
        if len(body) > 262144:
            raise CallbackError("payload_too_large")
        try:
            addresses = self.resolver(host, 443, type=socket.SOCK_STREAM)
        except socket.gaierror:
            raise CallbackError("dns_failed") from None
        ips = [entry[4][0] for entry in addresses]
        if not ips or any(not ipaddress.ip_address(ip).is_global for ip in ips):
            raise CallbackError("non_public_address")
        conn = self.connection_factory(host, ips[0], 10)
        try:
            conn.request("POST", path, body=body, headers=headers)
            response = conn.getresponse()
            raw = response.read(65537)
            if len(raw) > 65536:
                raise CallbackError("response_too_large", response.status)
            if 300 <= response.status < 400:
                raise CallbackError("redirect_forbidden", response.status)
            if not raw:
                return response.status, CallbackResponse("response_empty")
            try:
                result = json.loads(raw)
            except (ValueError, UnicodeError):
                # Event acknowledgement needs only 2xx. A verification request
                # still fails when the parsed challenge is absent.
                result = CallbackResponse("response_not_json")
            return response.status, result
        except ssl.SSLCertVerificationError:
            raise CallbackError("tls_certificate_failed") from None
        except ssl.SSLError:
            raise CallbackError("tls_failed") from None
        except (TimeoutError, socket.timeout):
            raise CallbackError("timeout") from None
        except ConnectionError:
            raise CallbackError("connection_failed") from None
        except http.client.HTTPException:
            raise CallbackError("http_protocol_failed") from None
        except OSError:
            raise CallbackError("network_failed") from None
        finally:
            conn.close()
