import base64
import unittest
from bridge import Bridge, MockChatCallback, encoded, signed


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.clock = [1800000000]
        self.now = lambda: self.clock[0]
        # Public fixed test fixture, not an access credential. Never persisted as a subscription.
        self.secret = "whsec_" + base64.b64encode(b"offline-test-fixture-key-32-bytes!").decode()
        self.callback = MockChatCallback(self.secret, self.now)
        self.bridge = Bridge(self.callback, self.now)
        self.params = {"name": "message.created", "arguments": {"channel_id": "sandbox"},
                       "delivery": {"mode": "webhook", "url": "https://mock.invalid/callback", "secret": self.secret}}

    def subscribe(self):
        return self.bridge.dispatch("events/subscribe", self.params)

    def test_discovery(self):
        self.assertIn("events", self.bridge.dispatch("server/discover")["capabilities"])
        self.assertEqual(self.bridge.dispatch("events/list")["events"][0]["name"], "message.created")
        self.assertEqual(self.bridge.dispatch("tools/list")["tools"][0]["name"], "reply_to_message")

    def test_simulated_receive_then_explicit_tool_reply(self):
        self.subscribe()
        self.assertEqual(self.bridge.receive("sandbox", "m1", "你好"), [202])
        self.assertEqual(self.callback.events[0]["data"]["text"], "你好")
        self.assertNotIn("context_token", self.callback.events[0]["data"])
        self.assertEqual(len(self.bridge.replies), 0)  # No agent/model was invoked.
        result = self.bridge.dispatch("tools/call", {"name": "reply_to_message", "arguments":
            {"message_id": "m1", "text": "模拟回复", "idempotency_key": "r1"}})
        self.assertTrue(result["structuredContent"]["simulated"])

    def test_filter(self):
        self.subscribe()
        self.assertEqual(self.bridge.receive("different", "m2", "忽略"), [])

    def test_duplicate_event(self):
        self.subscribe()
        self.bridge.receive("sandbox", "m1", "hello")
        self.bridge.receive("sandbox", "m1", "hello")
        self.assertEqual(len(self.callback.events), 1)

    def test_reply_idempotency_and_conflict(self):
        self.bridge.receive("sandbox", "m1", "hello")
        p = {"name": "reply_to_message", "arguments": {"message_id": "m1", "text": "reply", "idempotency_key": "r"}}
        self.bridge.dispatch("tools/call", p)
        self.bridge.dispatch("tools/call", p)
        self.assertEqual(len(self.bridge.replies), 1)
        p["arguments"]["text"] = "changed"
        with self.assertRaises(ValueError):
            self.bridge.dispatch("tools/call", p)

    def test_expiry_refresh_and_unsubscribe(self):
        self.params["ttlMs"] = 1000
        first = self.subscribe()
        self.clock[0] += 2
        self.assertEqual(self.bridge.receive("sandbox", "m1", "expired"), [])
        self.assertEqual(first["id"], self.subscribe()["id"])
        self.assertEqual(self.bridge.receive("sandbox", "m2", "refreshed"), [202])
        self.bridge.dispatch("events/unsubscribe", self.params)
        self.bridge.dispatch("events/unsubscribe", self.params)
        self.assertEqual(self.bridge.receive("sandbox", "m3", "stopped"), [])

    def test_challenge_failure_is_not_activated(self):
        self.bridge.callback = lambda *_: (200, {"challenge": "wrong"})
        with self.assertRaises(ValueError):
            self.subscribe()
        self.assertEqual(self.bridge.subscriptions, {})

    def test_tampered_and_stale_signature(self):
        body = encoded({"type": "verification", "challenge": "test"})
        headers = signed(self.secret, "sub", "verify", body, self.now())
        with self.assertRaises(ValueError):
            self.callback(body + b" ", headers)
        self.clock[0] += 301
        with self.assertRaises(ValueError):
            self.callback(body, headers)

    def test_validation_and_no_network(self):
        self.params["delivery"]["url"] = "https://example.com/callback"
        with self.assertRaises(ValueError):
            self.subscribe()
        with self.assertRaises(ValueError):
            self.bridge.receive("sandbox", "m1", "x" * 262144)
        self.assertEqual(self.bridge.messages, {})
        with self.assertRaises(PermissionError):
            self.bridge.dispatch("events/list", owner="other")
        self.assertIn("error", self.bridge.rpc({"id": 1, "method": "missing"}))


if __name__ == "__main__":
    unittest.main()
