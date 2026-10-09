"""Exercise a real stdio child twice; use a disposable synthetic database."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from service import FIXTURE_SECRET


def request(rid, method, params=None):
    p = params or {}
    p["_meta"] = {"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {}}
    return {"jsonrpc": "2.0", "id": rid, "method": method, "params": p}


def main():
    with tempfile.TemporaryDirectory(prefix="dot-synthetic-") as folder:
        command = [sys.executable, "-B", str(Path(__file__).with_name("service.py")), "--db", str(Path(folder) / "state.sqlite")]
        subscribe = {"name": "message.created", "arguments": {"channel_id": "sandbox"},
            "delivery": {"mode": "webhook", "url": "https://mock.invalid/callback", "secret": FIXTURE_SECRET}}
        frames = [request(1, "server/discover"), request(2, "events/list"), request(3, "tools/list"),
            request(4, "events/subscribe", subscribe),
            request(5, "tools/call", {"name": "inject_mock_message", "arguments": {"message_id": "demo-nonce", "text": "合成消息 demo-nonce"}}),
            request(6, "tools/call", {"name": "reply_to_message", "arguments": {"message_id": "demo-nonce", "text": "显式测试回复 demo-nonce", "idempotency_key": "demo-nonce"}})]
        result = subprocess.run(command, input="".join(json.dumps(f) + "\n" for f in frames), text=True, capture_output=True, timeout=10, check=True)
        responses = [json.loads(line) for line in result.stdout.splitlines()]
        assert all("result" in r for r in responses), responses
        print(json.dumps({"phase": "stdio-child", "rpc_responses": len(responses), "discovery_version": responses[0]["result"]["supportedVersions"], "synthetic_callback_accepted": responses[4]["result"]["structuredContent"]["delivered"], "reply_called_by": "demo.py (not dot)"}, ensure_ascii=False))
        result = subprocess.run(command, input=json.dumps(request(7, "tools/call", {"name": "read_mock_status", "arguments": {}})) + "\n", text=True, capture_output=True, timeout=10, check=True)
        status = json.loads(result.stdout)["result"]["structuredContent"]
        assert status["replies"][0]["message_id"] == "demo-nonce"
        print(json.dumps({"phase": "second-child-after-restart", "status": status, "real_dot_verified": False, "network_calls": 0}, ensure_ascii=False))


if __name__ == "__main__":
    main()
