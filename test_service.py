import io
import json
import os
from pathlib import Path
import socket
import ssl
import base64
import hashlib
import hmac
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from service import FIXTURE_SECRET, OfflineDelivery, ReloadableDelivery, PersistentService, process as raw_process, run_stdio
from bridge import signed
from outbound import CallbackError, CallbackResponse, PinnedHTTPSConnection, SafeHTTPS


META = {"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {}}


def modern(request):
    request.setdefault("params", {})["_meta"] = dict(META)
    return request


def process(service, request):
    return raw_process(service, modern(request))


class PersistentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.sqlite"
        self.clock = [1800000000]
        self.now = lambda: self.clock[0]
        self.service = PersistentService(self.path, now=self.now, sleeper=lambda _: None)
        self.params = {"name": "message.created", "arguments": {"channel_id": "sandbox"},
            "delivery": {"mode": "webhook", "url": "https://mock.invalid/callback", "secret": FIXTURE_SECRET}}

    def tearDown(self):
        self.service.close()
        self.tmp.cleanup()

    def tool(self, name, args=None):
        return self.service.dispatch("tools/call", {"name": name, "arguments": args or {}})["structuredContent"]

    def restart(self):
        self.service.close()
        self.service = PersistentService(self.path, now=self.now, sleeper=lambda _: None)

    def test_restart_subscriptions_messages_reply_dedup(self):
        sid = self.service.dispatch("events/subscribe", self.params)["id"]
        self.tool("inject_mock_message", {"message_id": "nonce1", "text": "合成消息"})
        reply = {"message_id": "nonce1", "text": "nonce1: 合成回复", "idempotency_key": "nonce1"}
        self.tool("reply_to_message", reply)
        self.restart()
        self.assertEqual(self.service.dispatch("events/subscribe", self.params)["id"], sid)
        self.tool("reply_to_message", reply)
        self.assertEqual(self.tool("inject_mock_message", {"message_id": "nonce1", "text": "合成消息"})["delivered"], 0)
        self.assertEqual(self.tool("inject_mock_message", {"message_id": "nonce2", "text": "重启后"})["delivered"], 1)
        status = self.tool("read_mock_status")
        self.assertEqual(len(status["replies"]), 1)
        self.assertNotIn("whsec_", json.dumps(status))
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_expired_after_restart_then_refresh_and_unsubscribe(self):
        self.params["ttlMs"] = 1000
        sid = self.service.dispatch("events/subscribe", self.params)["id"]
        self.clock[0] += 2
        self.restart()
        self.assertEqual(self.tool("inject_mock_message", {"message_id": "expired", "text": "x"})["delivered"], 0)
        self.assertEqual(self.service.dispatch("events/subscribe", self.params)["id"], sid)
        self.assertEqual(self.tool("inject_mock_message", {"message_id": "new", "text": "x"})["delivered"], 1)
        self.service.dispatch("events/unsubscribe", self.params)
        self.restart()
        self.assertEqual(self.tool("inject_mock_message", {"message_id": "stopped", "text": "x"})["delivered"], 0)

    def test_pending_delivery_survives_restart_and_body_is_stable(self):
        self.service.dispatch("events/subscribe", self.params)
        original = self.service.delivery
        calls = []
        def unavailable(url, body, headers):
            calls.append((body, headers["webhook-id"]))
            return 503, {}
        self.service.delivery.post = unavailable
        self.assertEqual(self.tool("inject_mock_message", {"message_id": "pending", "text": "x"})["delivered"], 0)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(set(calls)), 1)
        self.restart()
        self.assertEqual(self.tool("retry_pending")["delivered"], 1)
        self.assertEqual(self.tool("read_mock_status")["deliveries"][0]["attempts"], 4)

    def test_terminal_410_not_retried(self):
        self.service.dispatch("events/subscribe", self.params)
        self.service.delivery.post = lambda *_: (410, {})
        self.tool("inject_mock_message", {"message_id": "gone", "text": "x"})
        self.assertEqual(self.tool("retry_pending")["delivered"], 0)
        self.assertEqual(self.tool("read_mock_status")["deliveries"][0]["attempts"], 1)

    def test_failed_challenge_not_persisted(self):
        self.service.delivery.post = lambda *_: (200, {"challenge": "wrong"})
        response = process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        self.assertEqual(response["error"]["code"], -32015)
        self.restart()
        self.assertEqual(self.service.db.execute("SELECT count(*) FROM subscriptions").fetchone()[0], 0)

    def test_unsubscribe_after_policy_revocation_never_contacts_callback(self):
        self.service.dispatch("events/subscribe", self.params)
        class Revoked:
            def validate_url(self, *_):
                raise AssertionError("unsubscribe must not validate callback")
            def post(self, *_):
                raise AssertionError("unsubscribe must not send")
        self.service.delivery = Revoked()
        self.service.dispatch("events/unsubscribe", self.params)
        self.assertEqual(self.service.db.execute("SELECT count(*) FROM subscriptions").fetchone()[0], 0)
        self.service.dispatch("events/unsubscribe", self.params)

    def test_reloadable_policy_enable_revoke_invalid_and_symlink(self):
        path = Path(self.tmp.name) / "outbound.json"
        calls = []
        class FakeHTTPS:
            def __init__(self, hosts, enabled):
                calls.append((hosts, enabled))
            def validate_url(self, url):
                return "approved.example.test", "/"
            def post(self, *_):
                return 200, {}
        delivery = ReloadableDelivery(path, factory=FakeHTTPS)
        def policy(data):
            path.write_text(json.dumps(data)); path.chmod(0o600)
        with self.assertRaises(CallbackError):
            delivery.validate_url("https://approved.example.test/private")
        policy({"enabled": False, "callback_hosts": ["approved.example.test"]})
        with self.assertRaises(CallbackError):
            delivery.post("https://approved.example.test/private", b"x", {})
        self.assertEqual(calls, [])
        policy({"enabled": True, "callback_hosts": ["approved.example.test"]})
        self.assertEqual(delivery.post("https://approved.example.test/private", b"x", {})[0], 200)
        policy({"enabled": False, "callback_hosts": []})
        with self.assertRaises(CallbackError):
            delivery.post("https://approved.example.test/private", b"x", {})
        for data in [{"enabled": True, "callback_hosts": ["*.example.test"]}, {"enabled": True, "callback_hosts": [], "secret": "never"}]:
            policy(data)
            with self.assertRaises(CallbackError): delivery.transport()
        policy({"enabled": True, "callback_hosts": ["approved.example.test"]})
        path.chmod(0o644)
        with self.assertRaises(CallbackError): delivery.transport()
        path.unlink(); path.symlink_to(self.path)
        with self.assertRaises(CallbackError): delivery.transport()

    def test_reloadable_transport_reuses_resolver_but_rechecks_policy(self):
        path = Path(self.tmp.name) / 'outbound.json'
        enabled = {'enabled': True, 'callback_hosts': ['connectors.api.openai.com'],
                   'transport': 'clash_cloudflare_doh'}
        def policy(data):
            path.write_text(json.dumps(data)); path.chmod(0o600)
        delivery = ReloadableDelivery(path)
        policy(enabled)
        first = delivery.transport()
        delivery.validate_url('https://connectors.api.openai.com/test')
        self.assertIs(delivery.transport(), first)
        with patch.object(first, 'post', return_value=(200, {})) as post:
            delivery.post('https://connectors.api.openai.com/test', b'x', {})
            post.assert_called_once()
        # Each revoked/missing/invalid policy discards the old resolver cache.
        for state in ('disabled', 'missing', 'invalid', 'shared', 'symlink'):
            with self.subTest(state=state):
                if state == 'disabled': policy({**enabled, 'enabled': False})
                elif state == 'missing': path.unlink()
                elif state == 'invalid': path.write_text('{')
                elif state == 'shared': path.chmod(0o644)
                else:
                    path.unlink(); path.symlink_to(self.path)
                if state in ('disabled', 'missing'):
                    self.assertIs(delivery.transport(), delivery.offline)
                else:
                    with self.assertRaises(CallbackError): delivery.transport()
                path.unlink(missing_ok=True)
                policy(enabled)
                replacement = delivery.transport()
                self.assertIsNot(replacement, first)
                first = replacement
        policy({**enabled, 'transport': 'direct'})
        direct = delivery.transport()
        self.assertIsNot(direct, first)
        policy({**enabled, 'transport': 'direct', 'callback_hosts': ['other.example.test']})
        self.assertIsNot(delivery.transport(), direct)

    def test_diagnostics_policy_redacted_and_persistent(self):
        params = {**self.params, "delivery": {"mode": "webhook", "url": "https://callback.example.test/private-path?token=NEVER_LOG", "secret": "whsec_NEVER_LOG"}}
        response = process(self.service, {"jsonrpc": "2.0", "id": 9, "method": "events/subscribe", "params": params})
        self.assertEqual(response["error"]["code"], -32015)
        self.restart()
        rows = self.tool("read_mock_status")["diagnostics"]
        row = rows[0]
        self.assertEqual(set(row), {"time", "method", "callback_hostname", "error_code", "stage", "error_category", "http_status"})
        self.assertEqual(row["callback_hostname"], "callback.example.test")
        self.assertEqual(row["stage"], "callback_policy")
        self.assertEqual(row["error_code"], -32015)
        self.assertIsNone(row["http_status"])
        serialized = json.dumps(rows)
        for forbidden in ["NEVER_LOG", "private-path", "https://", "whsec_", "headers", "signature"]:
            self.assertNotIn(forbidden, serialized)

    def test_diagnostics_verification_stage_and_bounded_unknown_method(self):
        self.service.delivery.post = lambda *_: (200, {"challenge": "wrong"})
        process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        row = self.tool("read_mock_status")["diagnostics"][0]
        self.assertEqual(row["stage"], "callback_verification")
        self.assertEqual(row["error_code"], -32015)
        self.assertEqual(row["error_category"], "challenge_mismatch")
        self.assertEqual(row["http_status"], 200)
        for i in range(105):
            process(self.service, {"jsonrpc": "2.0", "id": i, "method": "SECRET_UNKNOWN_METHOD"})
        self.assertEqual(self.service.db.execute("SELECT COUNT(*) FROM diagnostics").fetchone()[0], 100)
        rows = self.tool("read_mock_status")["diagnostics"]
        self.assertEqual(len(rows), 20)
        self.assertEqual(rows[0]["method"], "unknown")
        self.assertNotIn("SECRET_UNKNOWN_METHOD", json.dumps(rows))

    def test_callback_failure_categories_status_and_no_exception_leak(self):
        cases = [(403, {}, "http_non_2xx"), (200, {}, "challenge_missing"),
                 (200, CallbackResponse("response_not_json"), "response_not_json"),
                 (204, CallbackResponse("response_empty"), "response_empty")]
        for status, result, category in cases:
            with self.subTest(category=category):
                self.service.delivery.post = lambda *_, s=status, r=result: (s, r)
                process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
                row = self.tool("read_mock_status")["diagnostics"][0]
                self.assertEqual((row["http_status"], row["error_category"]), (status, category))
        def fail(*_): raise CallbackError("https://private/path?secret=NEVER_LOG")
        self.service.delivery.post = fail
        response = process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        self.assertEqual(response["error"]["data"]["reason"], "challenge_failed")
        self.assertNotIn("NEVER_LOG", json.dumps(self.tool("read_mock_status")["diagnostics"]))

    def test_verification_exact_body_standard_webhooks_and_echo(self):
        def receiver(url, body, headers):
            payload = json.loads(body)
            self.assertEqual(set(payload), {"type", "challenge"})
            self.assertEqual(payload["type"], "verification")
            self.assertEqual(headers["Content-Type"], "application/json")
            signed_input = headers["webhook-id"].encode()+b'.'+headers["webhook-timestamp"].encode()+b'.'+body
            expected = 'v1,'+base64.b64encode(hmac.digest(base64.b64decode(FIXTURE_SECRET[6:]), signed_input, hashlib.sha256)).decode()
            self.assertEqual(headers["webhook-signature"], expected)
            self.assertEqual(headers["X-MCP-Subscription-Id"], self.service.subscription_id(self.params))
            return 200, {"challenge": payload["challenge"]}
        self.service.delivery.post = receiver
        result = process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        self.assertIn("result", result)

    def test_non_public_dns_fails_before_connection_and_is_diagnosed(self):
        self.params["delivery"]["url"] = 'https://callback.example/private?never=log'
        def forbidden_connection(*_): self.fail('private address must not connect')
        self.service.delivery = SafeHTTPS(['callback.example'], True,
            resolver=lambda *_, **kw: answers('198.18.0.1'), connection_factory=forbidden_connection)
        response = process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        self.assertEqual(response['error']['data']['reason'], 'non_public_address')
        row = self.tool('read_mock_status')['diagnostics'][0]
        self.assertEqual(row['error_category'], 'non_public_address')
        self.assertIsNone(row['http_status'])
        self.assertNotIn('private', json.dumps(row))

    def test_channel_permissions_and_secret_validation(self):
        self.params["arguments"]["channel_id"] = "private"
        result = process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        self.assertEqual(result["error"]["code"], -32012)
        self.params["arguments"]["channel_id"] = "sandbox"
        self.params["delivery"]["secret"] = "whsec_bad"
        result = process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "events/subscribe", "params": self.params})
        self.assertEqual(result["error"]["code"], -32602)

    def test_stdio_frames_notifications_and_errors(self):
        messages = [b"not-json", b"[]", json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode(),
            json.dumps(modern({"jsonrpc": "2.0", "id": 1, "method": "ping"})).encode(),
            json.dumps(modern({"jsonrpc": "2.0", "id": 2, "method": "server/discover"})).encode(),
            json.dumps(modern({"jsonrpc": "2.0", "id": 3, "method": "not-supported"})).encode()]
        sink = io.BytesIO()
        run_stdio(self.service, io.BytesIO(b"\n".join(messages) + b"\n"), sink)
        responses = [json.loads(line) for line in sink.getvalue().splitlines()]
        self.assertEqual(len(responses), 5)
        self.assertEqual(responses[0]["error"]["code"], -32700)
        self.assertEqual(responses[1]["error"]["code"], -32600)
        self.assertEqual(responses[2]["result"]["resultType"], "complete")
        self.assertIn("events", responses[3]["result"]["capabilities"])
        self.assertEqual(responses[4]["error"]["code"], -32601)

    def test_real_subprocess_stdio_and_restart(self):
        command = [sys.executable, "-B", str(Path(__file__).with_name("service.py")), "--db", str(Path(self.tmp.name) / "child.sqlite")]
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "server/discover"},
            {"jsonrpc": "2.0", "id": 2, "method": "events/subscribe", "params": self.params},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "inject_mock_message", "arguments": {"message_id": "child-nonce", "text": "子进程测试"}}},
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "reply_to_message", "arguments": {"message_id": "child-nonce", "text": "child-nonce reply", "idempotency_key": "child-nonce"}}}]
        data = "".join(json.dumps(modern(r), ensure_ascii=False) + "\n" for r in requests)
        result = subprocess.run(command, input=data, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(len(result.stdout.splitlines()), 4)
        self.assertTrue(json.loads(result.stdout.splitlines()[-1])["result"]["structuredContent"]["simulated"])
        request = {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "read_mock_status", "arguments": {}}}
        result = subprocess.run(command, input=json.dumps(modern(request)) + "\n", text=True, capture_output=True, timeout=10)
        self.assertEqual(json.loads(result.stdout)["result"]["structuredContent"]["replies"][0]["message_id"], "child-nonce")

    def test_required_modern_metadata_and_unsupported_version(self):
        missing = raw_process(self.service, {"jsonrpc": "2.0", "id": 1, "method": "server/discover"})
        self.assertEqual(missing["error"]["code"], -32602)
        request = modern({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        request["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = "1900-01-01"
        unsupported = raw_process(self.service, request)
        self.assertEqual(unsupported["error"]["code"], -32022)
        self.assertEqual(unsupported["error"]["data"]["supported"], ["2026-07-28"])
        for method in ("server/discover", "events/list", "tools/list"):
            result = process(self.service, {"jsonrpc": "2.0", "id": 3, "method": method})["result"]
            self.assertEqual(result["resultType"], "complete")
            self.assertIn("io.modelcontextprotocol/serverInfo", result["_meta"])

    def test_secret_rotation_window_survives_restart(self):
        self.service.dispatch("events/subscribe", self.params)
        new_secret = "whsec_" + __import__("base64").b64encode(b"second-public-test-fixture-key!!!").decode()
        self.params["delivery"]["secret"] = new_secret
        signatures = []
        def capture(url, body, headers):
            value = json.loads(body)
            if value.get("type") == "verification":
                return 200, {"challenge": value["challenge"]}
            signatures.append(headers["webhook-signature"])
            return 202, {}
        self.service.delivery.post = capture
        self.service.dispatch("events/subscribe", self.params)
        self.restart()
        self.service.delivery.post = capture
        self.tool("inject_mock_message", {"message_id": "rotating", "text": "x"})
        self.assertEqual(len(signatures[-1].split()), 2)
        self.clock[0] += 301
        self.tool("inject_mock_message", {"message_id": "rotated", "text": "x"})
        self.assertEqual(len(signatures[-1].split()), 1)

    def test_cancellation_during_callback_suppresses_response(self):
        self.service.dispatch("events/subscribe", self.params)
        entered, release = threading.Event(), threading.Event()
        def blocking_callback(*_):
            entered.set()
            self.assertTrue(release.wait(5))
            return 503, {}
        self.service.delivery.post = blocking_callback
        request = modern({"jsonrpc": "2.0", "id": "cancel-me", "method": "tools/call", "params":
            {"name": "inject_mock_message", "arguments": {"message_id": "canceled", "text": "x"}}})
        frames = [json.dumps(request).encode() + b"\n", json.dumps({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": "cancel-me"}}).encode() + b"\n", b""]
        class Source:
            index = 0
            def readline(inner, _limit):
                if inner.index == 1:
                    self.assertTrue(entered.wait(5))
                if inner.index == 2:
                    release.set()
                result = frames[inner.index]
                inner.index += 1
                return result
        sink = io.BytesIO()
        run_stdio(self.service, Source(), sink)
        self.assertEqual(sink.getvalue(), b"")
        self.assertEqual(self.service.db.execute("SELECT attempts FROM deliveries").fetchone()[0], 1)


def answers(*ips):
    return [(socket.AF_INET6 if ":" in ip else socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443)) for ip in ips]


class OutboundTests(unittest.TestCase):
    def test_network_errors_are_fixed_categories_not_exception_text(self):
        for error, category in [(socket.gaierror('secret'), 'dns_failed'),
                                (ssl.SSLCertVerificationError('secret'), 'tls_certificate_failed'),
                                (ssl.SSLError('secret'), 'tls_failed'),
                                (TimeoutError('secret'), 'timeout'),
                                (ConnectionRefusedError('secret'), 'connection_failed'),
                                (OSError('secret'), 'network_failed')]:
            with self.subTest(category=category):
                client = self.client()
                if category == 'dns_failed':
                    def resolver(*_, **kw): raise error
                    client.resolver = resolver
                else:
                    class Failing:
                        def __init__(self, *_): pass
                        def request(self, *_, **kw): raise error
                        def close(self): pass
                    client.connection_factory = Failing
                with self.assertRaises(CallbackError) as caught:
                    client.post('https://callback.example/private', b'{}', {})
                self.assertEqual(str(caught.exception), category)
                self.assertNotIn('secret', str(caught.exception))

    def test_redirect_and_oversize_preserve_status_without_body(self):
        for status, body, category in [(302, b'private response', 'redirect_forbidden'), (200, b'x'*65537, 'response_too_large')]:
            with self.assertRaises(CallbackError) as caught:
                self.client(status=status, body=body).post('https://callback.example/a', b'{}', {})
            self.assertEqual(str(caught.exception), category)
            self.assertEqual(caught.exception.http_status, status)

    def client(self, ips=("8.8.8.8",), status=202, body=b"{}"):
        self.seen = []
        self.responses = []
        outer = self
        class Conn:
            def __init__(self, host, ip, timeout):
                outer.seen.append((host, ip, timeout))
            def request(self, method, path, body, headers):
                outer.responses.append((method, path, body, headers))
            def getresponse(self):
                return type("Response", (), {"status": status, "read": lambda _, limit: body[:limit]})()
            def close(self):
                pass
        return SafeHTTPS(["callback.example"], True, resolver=lambda *_args, **_kw: answers(*ips), connection_factory=Conn)

    def test_disabled_without_even_dns(self):
        resolver = lambda *_args, **_kw: self.fail("DNS must not run")
        with self.assertRaises(CallbackError):
            SafeHTTPS(["callback.example"], resolver=resolver).post("https://callback.example/a", b"{}", {})

    def test_public_address_pinned_and_exact_request(self):
        client = self.client()
        client.post("https://callback.example/path?q=1", b"exact bytes", {"webhook-id": "id"})
        self.assertEqual(self.seen, [("callback.example", "8.8.8.8", 10)])
        self.assertEqual(self.responses[0][:3], ("POST", "/path?q=1", b"exact bytes"))

    def test_local_private_mixed_and_mapped_ips_rejected(self):
        for ips in [("127.0.0.1",), ("10.0.0.1",), ("::1",), ("169.254.169.254",), ("8.8.8.8", "192.168.1.1"), ("::ffff:127.0.0.1",)]:
            with self.subTest(ips=ips), self.assertRaises(CallbackError):
                self.client(ips).post("https://callback.example/a", b"{}", {})
            self.assertEqual(self.seen, [])

    def test_dns_rebinding_revalidated_per_request(self):
        client = self.client()
        client.post("https://callback.example/a", b"{}", {})
        client.resolver = lambda *_a, **_kw: answers("127.0.0.1")
        with self.assertRaises(CallbackError):
            client.post("https://callback.example/a", b"{}", {})
        self.assertEqual(len(self.seen), 1)

    def test_url_policy_redirect_and_bounds(self):
        client = self.client()
        for url in ["http://callback.example/a", "https://evil.example/a", "https://callback.example:8443/a", "https://user:pass@callback.example/a", "https://callback.example/a#x", "https://callback.example/a\n", "https://127.0.0.1/a"]:
            with self.subTest(url=url), self.assertRaises(CallbackError):
                client.post(url, b"{}", {})
        with self.assertRaises(CallbackError):
            self.client(status=302).post("https://callback.example/a", b"{}", {})
        with self.assertRaises(CallbackError):
            self.client(body=b"x" * 65537).post("https://callback.example/a", b"{}", {})
        with self.assertRaises(CallbackError):
            client.post("https://callback.example/a", b"x" * 262145, {})

    def test_numeric_ip_connect_preserves_tls_hostname(self):
        conn = PinnedHTTPSConnection("callback.example", "8.8.8.8", 10)
        with patch("outbound.socket.create_connection") as create, patch.object(conn._context, "wrap_socket") as wrap:
            conn.connect()
            create.assert_called_once_with(("8.8.8.8", 443), 10)
            wrap.assert_called_once_with(create.return_value, server_hostname="callback.example")

    def test_plaintext_success_acknowledgement(self):
        self.assertEqual(self.client(status=202, body=b"accepted").post("https://callback.example/a", b"{}", {})[0], 202)


if __name__ == "__main__":
    unittest.main()
