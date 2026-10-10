"""Offline-only iLink text adapter. No socket, credential loader or MCP registration.

An injected exchange is for synthetic tests only. Live transport and service
authorization require a separate reviewed implementation and consent checkpoint.
"""
import base64
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import time
from urllib.parse import urlencode, urlsplit
from outbound import CallbackError

API_HOST = "ilinkai.weixin.qq.com"
VERSION = "2.4.9"  # Official package metadata reviewed 2026-09-30; not service permission.
CLIENT_VERSION = str((2 << 16) | (4 << 8) | 9)
BASE_INFO = {"channel_version": VERSION, "bot_agent": "DotWeixinPrototype/0.1.0"}
FUTURE_EVENT = "wechat.message.created"
FUTURE_TOOLS = ("wechat_read_messages", "wechat_reply_text")
READ_PAGE_SIZE = 20
REPLY_MAX_AGE = 3600


def validate_read_arguments(arguments, *, pending_only=False):
    allowed = {'after_seq', 'limit'} | (set() if pending_only else {'message_id'})
    if not isinstance(arguments, dict) or set(arguments) - allowed:
        raise AdapterError('invalid_read_arguments')
    if 'message_id' in arguments:
        value = arguments['message_id']
        if not isinstance(value, str) or not 0 < len(value) <= 128:
            raise AdapterError('invalid_read_message')
        if 'after_seq' in arguments:
            raise AdapterError('invalid_read_arguments')
    if 'after_seq' in arguments and (type(arguments['after_seq']) is not int or arguments['after_seq'] < 0):
        raise AdapterError('invalid_read_cursor')
    if 'limit' in arguments and (type(arguments['limit']) is not int or not 1 <= arguments['limit'] <= READ_PAGE_SIZE):
        raise AdapterError('invalid_read_limit')


def valid_message_id(value):
    if isinstance(value, str) and value.isascii() and value.isdecimal() and len(value) <= 20:
        value = int(value)
    return type(value) is int and 0 < value < 2**64


def acknowledgement(result):
    """Conservative official optional-ret compatibility, never a phone receipt."""
    for name in ('ret', 'errcode'):
        if name in result and (type(result[name]) is not int or result[name] != 0):
            return 'api_rejected', 'explicit_error_or_invalid_code'
    if 'ret' in result:
        return 'api_accepted', 'explicit_zero_ret'
    if valid_message_id(result.get('message_id')) and not result.get('errmsg'):
        return 'api_accepted', 'message_id_ack_ret_omitted'
    return 'unknown', 'ack_missing'


class AdapterError(Exception):
    """Fixed, non-sensitive error category only."""


@dataclass(repr=False)
class Binding:
    bot_id: str
    user_id: str
    token: str = field(repr=False)

    def __repr__(self):
        return "Binding(<redacted>)"


def confirmed_binding(response):
    if response.get("status") != "confirmed":
        raise AdapterError("authorization_not_confirmed")
    parsed = urlsplit(response.get("baseurl", ""))
    if (parsed.scheme != "https" or parsed.hostname != API_HOST
            or parsed.port not in (None, 443) or parsed.username or parsed.password
            or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise AdapterError("new_service_endpoint_requires_review")
    values = [response.get(k) for k in ("ilink_bot_id", "ilink_user_id", "bot_token")]
    if not all(isinstance(v, str) and 0 < len(v) <= 4096 for v in values):
        raise AdapterError("invalid_binding")
    return Binding(*values)


def qr_request():
    """Pure request preparation; never contacts server or displays a QR."""
    return ("POST", "/ilink/bot/get_bot_qrcode?bot_type=3",
            {"AuthorizationType": "ilink_bot_token", "Content-Type": "application/json",
             "X-WECHAT-UIN": base64.b64encode(str(secrets.randbits(32)).encode()).decode()},
            {"local_token_list": []})


def qr_status_request(qrcode, verify_code=None):
    params = {"qrcode": qrcode}
    if verify_code is not None:
        params["verify_code"] = verify_code
    return ("GET", "/ilink/bot/get_qrcode_status?" + urlencode(params),
            {"iLink-App-Id": "bot", "iLink-App-ClientVersion": CLIENT_VERSION}, None)


class TextAdapter:
    def __init__(self, path, binding, *, test_exchange=None, clock=time.time, recover_sending=True):
        self.binding, self.exchange, self.clock = binding, test_exchange, clock
        path = Path(path)
        # Private state contains context tokens. No real state is created by default.
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        info = os.fstat(fd)
        os.close(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise AdapterError("unsafe_state_permissions")
        self.db = sqlite3.connect(path)
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
          CREATE TABLE IF NOT EXISTS messages(id TEXT PRIMARY KEY, text TEXT, context TEXT,
            received REAL, digest TEXT);
          CREATE TABLE IF NOT EXISTS replies(k TEXT PRIMARY KEY, message_id TEXT,
            text TEXT, client_id TEXT, state TEXT);
          CREATE INDEX IF NOT EXISTS replies_message_id ON replies(message_id);
          CREATE TABLE IF NOT EXISTS reply_diagnostics(k TEXT PRIMARY KEY,
            time REAL, elapsed_ms INTEGER, category TEXT, http_status INTEGER);
        """)
        if 'http_status' not in {r[1] for r in self.db.execute('PRAGMA table_info(reply_diagnostics)')}:
            self.db.execute('ALTER TABLE reply_diagnostics ADD COLUMN http_status INTEGER')
        self.session = hashlib.sha256((binding.bot_id + "\0" + binding.user_id).encode()).hexdigest()
        old = self._get("session")
        if old is not None and old != self.session:
            self.db.close()
            raise AdapterError("binding_state_mismatch")
        with self.db:
            self._set("session", self.session)
            if recover_sending:
                self.db.execute("UPDATE replies SET state='unknown' WHERE state='sending'")

    def close(self):
        self.db.close()

    def _get(self, k, default=None):
        row = self.db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else default

    def _set(self, k, v):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, str(v)))

    def _request(self, path, body):
        if self.exchange is None:
            raise AdapterError("live_transport_not_implemented")
        headers = {"AuthorizationType": "ilink_bot_token",
                   "Authorization": "Bearer " + self.binding.token,
                   "Content-Type": "application/json", "iLink-App-Id": "bot",
                   "iLink-App-ClientVersion": CLIENT_VERSION,
                   "X-WECHAT-UIN": base64.b64encode(str(secrets.randbits(32)).encode()).decode()}
        try:
            result = self.exchange(path, headers, dict(body, base_info=dict(BASE_INFO)))
        except CallbackError as error:
            failure = AdapterError('exchange_failed')
            allowed = {'api_http_failed', 'api_exchange_failed', 'redirect_forbidden', 'doh_http_failed', 'doh_invalid_response', 'doh_invalid_answer', 'doh_question_mismatch', 'doh_query_forbidden', 'doh_bootstrap_forbidden', 'doh_network_failed', 'doh_no_address', 'non_public_address', 'proxy_connect_failed', 'tls_certificate_failed', 'tls_failed', 'timeout', 'response_too_large', 'invalid_response'}
            failure.category = str(error) if str(error) in allowed else 'exchange_failed'
            failure.http_status = error.http_status if type(error.http_status) is int and 100 <= error.http_status <= 599 else None
            raise failure from None
        except Exception:
            raise AdapterError("exchange_failed") from None
        if not isinstance(result, dict):
            raise AdapterError("invalid_response")
        return result

    def poll(self):
        if float(self._get("paused_until", "0")) > self.clock():
            return {"state": "session_paused"}
        result = self._request("/ilink/bot/getupdates", {"get_updates_buf": self._get("cursor", "")})
        if result.get("ret") == -14 or result.get("errcode") == -14:
            with self.db:
                self._set("paused_until", self.clock() + 3600)
            return {"state": "session_paused"}
        # Official protobuf JSON types make ret optional; omitted numeric zero
        # is normal. Still reject any explicit nonzero/malformed error code.
        if any(type(result.get(k, 0)) is not int or result.get(k, 0) != 0 for k in ("ret", "errcode")) or ("ret" not in result and result.get("errmsg")):
            raise AdapterError("poll_rejected")
        return self.ingest(result)

    def ingest(self, result):
        msgs = result.get("msgs", [])
        cursor = result.get("get_updates_buf", "")
        if not isinstance(msgs, list) or len(msgs) > 1000 or not isinstance(cursor, str) or len(cursor) > 65536:
            raise AdapterError("invalid_batch")
        accepted, rejected, duplicates = 0, 0, 0
        with self.db:
            for msg in msgs:
                parsed = self._parse(msg)
                if parsed is None:
                    rejected += 1
                    continue
                mid, text, context = parsed
                digest = hashlib.sha256(json.dumps([text, context]).encode()).hexdigest()
                old = self.db.execute("SELECT digest FROM messages WHERE id=?", (mid,)).fetchone()
                if old:
                    if old[0] != digest:
                        raise AdapterError("message_id_conflict")
                    duplicates += 1
                    continue
                self.db.execute("INSERT INTO messages VALUES (?,?,?,?,?)",
                                (mid, text, context, self.clock(), digest))
                accepted += 1
            if cursor:
                self._set("cursor", cursor)
        return {"accepted": accepted, "rejected": rejected, "duplicates": duplicates}

    def _parse(self, m):
        if not isinstance(m, dict):
            return None
        if (m.get("from_user_id") != self.binding.user_id
                or m.get("to_user_id") != self.binding.bot_id
                or m.get("group_id") not in (None, "")
                or m.get("message_type") != 1 or m.get("message_state") != 2):
            return None
        mid, items, context = m.get("message_id"), m.get("item_list"), m.get("context_token")
        if isinstance(mid, str) and mid.isascii() and mid.isdecimal() and len(mid) <= 20:
            mid = int(mid)
        if type(mid) is not int or not 0 < mid < 2**64 or not isinstance(items, list) or not 1 <= len(items) <= 32:
            return None
        if not isinstance(context, str) or not 0 < len(context) <= 16384:
            return None
        texts = []
        for item in items:
            if not isinstance(item, dict) or item.get("type") != 1:
                return None
            value = item.get("text_item")
            if not isinstance(value, dict) or not isinstance(value.get("text"), str):
                return None
            texts.append(value["text"])
        text = "\n".join(texts)
        if not text or len(text.encode()) > 32768:
            return None
        return "wx_" + hashlib.sha256((self.session + ":" + str(mid)).encode()).hexdigest(), text, context

    def read_messages(self, after_seq=0, limit=READ_PAGE_SIZE, message_id=None, *, pending_only=False):
        return self.read_page(after_seq, limit, message_id, pending_only=pending_only)['messages']

    def read_page(self, after_seq=0, limit=READ_PAGE_SIZE, message_id=None, *, pending_only=False):
        arguments = {'limit': limit}
        if message_id is None:
            arguments['after_seq'] = after_seq
        else:
            if after_seq != 0:
                raise AdapterError('invalid_read_arguments')
            arguments['message_id'] = message_id
        validate_read_arguments(arguments, pending_only=pending_only)
        now = self.clock()
        if message_id is None:
            where, values = ['m.rowid > ?'], [after_seq]
        else:
            where, values = ['m.id = ?'], [message_id]
        if pending_only:
            where += ['m.received >= ?', 'm.received <= ?',
                      'NOT EXISTS (SELECT 1 FROM replies r WHERE r.message_id=m.id)']
            values += [now - REPLY_MAX_AGE, now]
        # State is evaluated in the same SELECT as the page. Reading never
        # claims a message or moves the relay's independent cursor.
        rows = self.db.execute('''SELECT m.id,m.text,m.rowid,m.received,
            CASE
              WHEN EXISTS (SELECT 1 FROM replies r WHERE r.message_id=m.id AND r.state='api_accepted') THEN 'api_accepted'
              WHEN EXISTS (SELECT 1 FROM replies r WHERE r.message_id=m.id AND r.state='sending') THEN 'sending'
              WHEN EXISTS (SELECT 1 FROM replies r WHERE r.message_id=m.id AND r.state='unknown') THEN 'unknown'
              WHEN EXISTS (SELECT 1 FROM replies r WHERE r.message_id=m.id AND r.state='api_rejected') THEN 'api_rejected'
              WHEN EXISTS (SELECT 1 FROM replies r WHERE r.message_id=m.id) THEN 'unknown'
              ELSE 'never_replied'
            END
            FROM messages m WHERE ''' + ' AND '.join(where) +
            ' ORDER BY m.rowid LIMIT ?', (*values, limit + 1)).fetchall()
        messages = [{"message_id": row[0], "text": row[1], "session_id": self.session,
                     "trust": "untrusted_external_data", "grants_permissions": False, "local_seq": row[2],
                     "received_at": datetime.fromtimestamp(row[3], timezone.utc).isoformat(),
                     "reply_state": row[4],
                     "reply_eligible": row[4] == 'never_replied' and 0 <= now - row[3] <= REPLY_MAX_AGE}
                    for row in rows[:limit]]
        return {'messages': messages, 'has_more': len(rows) > limit,
                'next_after_seq': None if message_id is not None else
                    (messages[-1]['local_seq'] if messages else after_seq)}

    def reply(self, message_id, text, idempotency_key):
        if not isinstance(text, str) or not text or len(text.encode()) > 2000:
            raise AdapterError("invalid_reply_text")
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 128:
            raise AdapterError("invalid_idempotency_key")
        old = self.db.execute("SELECT message_id,text,state FROM replies WHERE k=?", (idempotency_key,)).fetchone()
        if old:
            if old[:2] != (message_id, text):
                raise AdapterError("reply_idempotency_conflict")
            return {"state": old[2], "mobile_receipt": "unverified"}
        row = self.db.execute("SELECT context,received FROM messages WHERE id=?", (message_id,)).fetchone()
        if row is None:
            raise AdapterError("unknown_message")
        if self.clock() - row[1] > REPLY_MAX_AGE:
            raise AdapterError("local_context_age_limit")
        if self.exchange is None:
            raise AdapterError("live_transport_not_implemented")
        client_id = "dot_" + hashlib.sha256((self.session + "\0" + idempotency_key).encode()).hexdigest()
        with self.db:
            self.db.execute("INSERT INTO replies VALUES (?,?,?,?,?)", (idempotency_key, message_id, text, client_id, "sending"))
        state = "unknown"
        started = time.monotonic()
        category = 'exchange_failed'
        http_status = None
        try:
            result = self._request("/ilink/bot/sendmessage", {"msg": {
                "from_user_id": "", "to_user_id": self.binding.user_id,
                "client_id": client_id, "message_type": 2, "message_state": 2,
                "context_token": row[0], "item_list": [{"type": 1, "text_item": {"text": text}}]}})
            state, category = acknowledgement(result)
            http_status = 200  # ILinkHTTPS only returns a parsed object for HTTP200.
        except AdapterError as error:
            category = getattr(error, 'category', 'exchange_failed')
            http_status = getattr(error, 'http_status', None)
        with self.db:
            self.db.execute("UPDATE replies SET state=? WHERE k=?", (state, idempotency_key))
            self.db.execute("INSERT OR REPLACE INTO reply_diagnostics VALUES (?,?,?,?,?)", (idempotency_key, self.clock(), int((time.monotonic() - started) * 1000), category, http_status))
        return {"state": state, "mobile_receipt": "unverified"}
