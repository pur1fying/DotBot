"""Offline MCP Events protocol prototype. No network, credentials, or model calls.

This is a test harness, NOT a connectable production MCP server. Transport,
authentication, durable subscriptions, SSRF-safe HTTPS and retries are gated
future work. The injected callback is exclusively an in-process simulator.
"""
import base64
import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime, timezone

EVENT = "message.created"


def schema(fields):
    return {"type": "object", "properties": {k: {"type": "string", "minLength": 1} for k in fields},
            "required": list(fields), "additionalProperties": False}


def validate(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ValueError("unexpected or missing fields")
    if any(not isinstance(value[k], str) or not value[k] for k in fields):
        raise ValueError("nonempty strings required")


def key(secret):
    if not isinstance(secret, str) or not secret.startswith("whsec_"):
        raise ValueError("whsec_ secret required")
    raw = base64.b64decode(secret[6:], validate=True)
    if not 24 <= len(raw) <= 64:
        raise ValueError("signing key must contain 24–64 bytes")
    return raw


def signed(secret, subscription_id, event_id, body, now):
    stamp = str(int(now))
    digest = hmac.new(key(secret), event_id.encode() + b"." + stamp.encode() + b"." + body,
                      hashlib.sha256).digest()
    return {"Content-Type": "application/json", "webhook-id": event_id,
            "webhook-timestamp": stamp, "webhook-signature": "v1," + base64.b64encode(digest).decode(),
            "X-MCP-Subscription-Id": subscription_id}


def encoded(value):
    body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    if len(body) > 262144:
        raise ValueError("payload exceeds 256 KiB")
    return body


class MockChatCallback:
    """Verifies deliveries and queues events; never represents a real dot."""
    def __init__(self, secret, now=time.time):
        self.secret, self.now = secret, now
        self.events, self.seen = [], set()

    def __call__(self, body, headers):
        if abs(self.now() - int(headers["webhook-timestamp"])) > 300:
            raise ValueError("stale signature")
        expected = signed(self.secret, headers["X-MCP-Subscription-Id"], headers["webhook-id"],
                          body, int(headers["webhook-timestamp"]))["webhook-signature"]
        if not hmac.compare_digest(expected, headers["webhook-signature"]):
            raise ValueError("invalid signature")
        data = json.loads(body)
        if data.get("type") == "verification":
            return 200, {"challenge": data["challenge"]}
        if data["eventId"] != headers["webhook-id"]:
            raise ValueError("event ID mismatch")
        if data["eventId"] not in self.seen:
            self.events.append(data)
            self.seen.add(data["eventId"])
        return 202, {}


class Bridge:
    def __init__(self, callback, now=time.time):
        self.callback, self.now = callback, now
        self.subscriptions, self.messages, self.replies = {}, {}, {}

    def identity(self, owner, p):
        validate(p["arguments"], ["channel_id"])
        if p["name"] != EVENT or p["delivery"]["mode"] != "webhook":
            raise ValueError("unsupported event or delivery")
        # Hard fail for real callback URLs: this harness has no networking.
        if p["delivery"]["url"] != "https://mock.invalid/callback":
            raise ValueError("only the in-process mock callback is supported")
        canonical = json.dumps([owner, p["delivery"]["url"], p["name"], p["arguments"]], sort_keys=True)
        return "sub_" + hashlib.sha256(canonical.encode()).hexdigest()[:24]

    def dispatch(self, method, p=None, owner="mock-user"):
        p = p or {}
        if owner != "mock-user":
            raise PermissionError("unknown mock principal")
        if method == "server/discover":
            return {"resultType": "complete", "supportedVersions": ["2026-07-28"],
                    "capabilities": {"tools": {}, "events": {}}}
        if method == "events/list":
            return {"events": [{"name": EVENT, "description": "A simulated incoming message in the selected channel.",
                    "delivery": ["webhook"], "inputSchema": schema(["channel_id"]),
                    "payloadSchema": schema(["channel_id", "message_id", "text"])}]}
        if method == "tools/list":
            return {"tools": [{"name": "reply_to_message", "description": "Record a reply to an existing simulated message. No real messages are sent.",
                    "inputSchema": schema(["message_id", "text", "idempotency_key"]),
                    "annotations": {"readOnlyHint": False, "destructiveHint": False,
                                    "idempotentHint": True, "openWorldHint": False}}]}
        if method in ("events/subscribe", "events/unsubscribe"):
            sid = self.identity(owner, p)
            if method == "events/unsubscribe":
                self.subscriptions.pop(sid, None)
                return {}
            secret = p["delivery"]["secret"]
            key(secret)
            ttl = p.get("ttlMs", 3600000)
            if ttl is None or isinstance(ttl, bool) or not isinstance(ttl, int) or ttl <= 0:
                raise ValueError("mock requires a positive finite ttlMs")
            expiry = self.now() + min(ttl, 3600000) / 1000
            challenge = str(uuid.uuid4())
            body = encoded({"type": "verification", "challenge": challenge})
            status, reply = self.callback(body, signed(secret, sid, "verify_" + str(uuid.uuid4()), body, self.now()))
            if not 200 <= status < 300 or not hmac.compare_digest(reply.get("challenge", ""), challenge):
                raise ValueError("CallbackEndpointError: challenge_failed")
            self.subscriptions[sid] = {"channel": p["arguments"]["channel_id"], "secret": secret, "expiry": expiry}
            return {"id": sid, "refreshBefore": datetime.fromtimestamp(expiry, timezone.utc).isoformat(),
                    "cursor": None, "truncated": False}
        if method == "tools/call":
            if p["name"] != "reply_to_message":
                raise ValueError("unknown tool")
            a = p["arguments"]
            validate(a, ["message_id", "text", "idempotency_key"])
            if a["message_id"] not in self.messages:
                raise ValueError("unknown message")
            previous = self.replies.get(a["idempotency_key"])
            if previous and previous != a:
                raise ValueError("idempotency key conflict")
            self.replies[a["idempotency_key"]] = dict(a)
            return {"content": [{"type": "text", "text": "Simulated reply recorded"}],
                    "structuredContent": {"simulated": True, "message_id": a["message_id"]}}
        raise ValueError("unknown method")

    def rpc(self, request):
        try:
            result = self.dispatch(request["method"], request.get("params"))
            return {"jsonrpc": "2.0", "id": request.get("id"), "result": result}
        except (ValueError, KeyError, PermissionError) as e:
            return {"jsonrpc": "2.0", "id": request.get("id"), "error": {"code": -32602, "message": str(e)}}

    def receive(self, channel_id, message_id, text, context_token="mock-context"):
        data = {"channel_id": channel_id, "message_id": message_id, "text": text}
        validate(data, ["channel_id", "message_id", "text"])
        # Stable event ID across retries; context token stays in bridge, not event payload.
        event = {"eventId": "evt_" + hashlib.sha256(message_id.encode()).hexdigest(), "name": EVENT,
                 "timestamp": datetime.fromtimestamp(self.now(), timezone.utc).isoformat(), "data": data, "cursor": None}
        previous = self.messages.get(message_id)
        if previous and previous != (data, context_token):
            raise ValueError("message ID conflict")
        body = encoded(event)
        self.messages[message_id] = (data, context_token)
        statuses = []
        for sid, sub in self.subscriptions.items():
            if sub["channel"] == channel_id and sub["expiry"] > self.now():
                status, _ = self.callback(body, signed(sub["secret"], sid, event["eventId"], body, self.now()))
                statuses.append(status)
        return statuses
