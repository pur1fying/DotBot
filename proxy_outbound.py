"""Exact-scope CONNECT transport; no system DNS or environment proxy lookup.

Bootstrap is Cloudflare's officially published 1.1.1.1 address, authenticated
with cloudflare-dns.com TLS. The local proxy is an explicitly trusted component:
CONNECT specifies numeric destinations; a malicious proxy is outside this trust
boundary. No proxy authentication or platform credentials are accepted here.
"""
import http.client
import ipaddress
import json
import socket
import ssl
from urllib.parse import urlencode
from outbound import CallbackError, SafeHTTPS

CALLBACK_HOST = "connectors.api.openai.com"
DOH_HOST = "cloudflare-dns.com"
DOH_BOOTSTRAP = "1.1.1.1"
PROXY = ("127.0.0.1", 7890)


def public_ip(value):
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        raise CallbackError("doh_invalid_answer") from None
    if not ip.is_global:
        raise CallbackError("non_public_address")
    return ip


class ProxyPinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, address, timeout, allowed_hosts=(CALLBACK_HOST, DOH_HOST)):
        public_ip(address)
        if host not in allowed_hosts:
            raise CallbackError("host_not_allowlisted")
        if host == DOH_HOST and address != DOH_BOOTSTRAP:
            raise CallbackError("doh_bootstrap_forbidden")
        super().__init__(host, 443, timeout=timeout, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        # Standard HTTPConnection.connect performs the configured CONNECT.
        # Never send a hostname as the CONNECT target or ask proxy-side DNS.
        helper = http.client.HTTPConnection(PROXY[0], PROXY[1], timeout=self.timeout)
        helper.set_tunnel(self.address, 443)
        try:
            helper.connect()
            raw = helper.sock
            helper.sock = None
            try:
                self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
            except BaseException:
                raw.close()
                raise
        except ssl.SSLError:
            raise
        except (OSError, http.client.HTTPException):
            raise CallbackError("proxy_connect_failed") from None
        finally:
            helper.close()


def parse_answers(data, query_type, query_host=CALLBACK_HOST):
    if not isinstance(data, dict) or data.get("Status") != 0 or data.get("TC") is not False:
        raise CallbackError("doh_invalid_response")
    questions = data.get("Question")
    if not isinstance(questions, list) or len(questions) != 1 or not isinstance(questions[0], dict) or not isinstance(questions[0].get("name"), str) or questions[0]["name"].rstrip(".").lower() != query_host or questions[0].get("type") != query_type:
        raise CallbackError("doh_question_mismatch")
    records = data.get("Answer", [])
    if not isinstance(records, list) or len(records) > 64:
        raise CallbackError("doh_invalid_answer")
    aliases, addresses = {}, []
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("name"), str):
            raise CallbackError("doh_invalid_answer")
        name = record["name"].rstrip(".").lower()
        kind, value = record.get("type"), record.get("data")
        if kind == 5:
            if not isinstance(value, str) or name in aliases:
                raise CallbackError("doh_invalid_answer")
            aliases[name] = value.rstrip(".").lower()
        elif kind in (1, 28):
            ip = public_ip(value)  # Validate ALL address answers, even unrelated.
            if kind != query_type or (kind == 1) != (ip.version == 4):
                raise CallbackError("doh_invalid_answer")
            addresses.append((name, str(ip)))
        else:
            raise CallbackError("doh_invalid_answer")
    reachable = {query_host}
    current = query_host
    for _ in range(16):
        if current not in aliases:
            break
        current = aliases[current]
        if current in reachable:
            raise CallbackError("doh_invalid_answer")
        reachable.add(current)
    else:
        raise CallbackError("doh_invalid_answer")
    if any(name not in reachable for name, _ in addresses) or any(name not in reachable for name in aliases):
        raise CallbackError("doh_invalid_answer")
    return [ip for _, ip in addresses]


class CloudflareResolver:
    def __init__(self, connection_factory=ProxyPinnedHTTPSConnection, query_host=CALLBACK_HOST):
        self.connection_factory = connection_factory
        self.query_host = query_host

    def __call__(self, host, port, type):
        if host != self.query_host or port != 443 or type != socket.SOCK_STREAM:
            raise CallbackError("doh_query_forbidden")
        ips = []
        for query_type in (1, 28):
            conn = self.connection_factory(DOH_HOST, DOH_BOOTSTRAP, 10)
            try:
                path = "/dns-query?" + urlencode({"name": self.query_host, "type": query_type})
                conn.request("GET", path, headers={"Accept": "application/dns-json", "Host": DOH_HOST})
                response = conn.getresponse()
                if response.status != 200:
                    raise CallbackError("doh_http_failed", response.status)
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise CallbackError("response_too_large", response.status)
                try:
                    data = json.loads(raw)
                except (ValueError, UnicodeError):
                    raise CallbackError("doh_invalid_response") from None
                ips.extend(parse_answers(data, query_type, self.query_host))
            except ssl.SSLCertVerificationError:
                raise CallbackError("tls_certificate_failed") from None
            except ssl.SSLError:
                raise CallbackError("tls_failed") from None
            except TimeoutError:
                raise CallbackError("timeout") from None
            except (OSError, http.client.HTTPException):
                raise CallbackError("doh_network_failed") from None
            finally:
                conn.close()
        if not ips:
            raise CallbackError("doh_no_address")
        return [(socket.AF_INET if ipaddress.ip_address(ip).version == 4 else socket.AF_INET6,
                 socket.SOCK_STREAM, 6, "", (ip, 443)) for ip in dict.fromkeys(ips)]


def proxy_transport(hosts, enabled=False):
    if frozenset(hosts) != frozenset({CALLBACK_HOST}):
        raise CallbackError("host_not_allowlisted")
    return SafeHTTPS(hosts, enabled, resolver=CloudflareResolver(), connection_factory=ProxyPinnedHTTPSConnection)
