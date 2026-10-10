"""Broker-owned durable routes, dot submissions and WeChat outbox.

No network or credentials. All methods use the broker's private SQLite store.
Notifications are hints; this journal is the source of truth after restart.
"""
import hashlib
import json
import time
import uuid
from dot_auth import DotError

REPLY_WINDOW = 3600
TERMINAL = ('api_accepted', 'failed', 'expired', 'rolled_back')
CANARY_PREFIX = '【DotBot直连测试】'


def initialize(db):
    if db.execute("SELECT v FROM meta WHERE k='dot.schema_version'").fetchone() == ('2',):
        return
    db.executescript('''
      CREATE TABLE IF NOT EXISTS bridge_routes (
        message_id TEXT PRIMARY KEY, route TEXT NOT NULL, assigned_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS dot_jobs (
        message_id TEXT PRIMARY KEY,
        client_message_id TEXT NOT NULL UNIQUE, reply_chain_id TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL, received_at REAL NOT NULL, claimed_at REAL,
        written_at REAL, admitted_at REAL, turn_id TEXT, first_reply_at REAL,
        error_category TEXT);
      CREATE TABLE IF NOT EXISTS dot_items (
        item_id TEXT PRIMARY KEY, message_id TEXT NOT NULL, digest TEXT NOT NULL,
        received_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS dot_outbox (
        item_id TEXT NOT NULL, message_id TEXT NOT NULL, part INTEGER NOT NULL,
        text TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL DEFAULT 'ready', started_at REAL, finished_at REAL,
        PRIMARY KEY (item_id,part));
      CREATE INDEX IF NOT EXISTS dot_outbox_message ON dot_outbox(message_id,state);
      CREATE TABLE IF NOT EXISTS dot_metrics (
        message_id TEXT NOT NULL, stage TEXT NOT NULL, at REAL NOT NULL,
        PRIMARY KEY (message_id,stage));
      CREATE TABLE IF NOT EXISTS dot_room_messages (
        room_message_id TEXT PRIMARY KEY, message_id TEXT NOT NULL, parent_id TEXT,
        received_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS dot_room_inbox (
        room_message_id TEXT PRIMARY KEY, payload TEXT NOT NULL, received_at REAL NOT NULL);
    ''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(dot_jobs)')}
    for name, sql_type in (('room_message_id', 'TEXT'), ('relay_replayed', 'INTEGER NOT NULL DEFAULT 0')):
        if name not in columns:
            db.execute(f'ALTER TABLE dot_jobs ADD COLUMN {name} {sql_type}')
    db.execute('CREATE UNIQUE INDEX IF NOT EXISTS dot_jobs_room_message ON dot_jobs(room_message_id)')
    # Existing messages stay on their original route; activating never replays history.
    with db:
        db.execute("INSERT OR IGNORE INTO bridge_routes SELECT id,'webhook',received FROM messages")
        db.execute("INSERT OR REPLACE INTO meta VALUES ('dot.schema_version','2')")


def split_text(text, limit=2000):
    if not isinstance(text, str) or not text.strip():
        raise DotError('dot_empty_reply')
    if len(text.encode()) <= limit:
        return [text]
    # Reserve space for [index/total]; never split a UTF-8 code point or discard text.
    budget = limit - 32
    parts = []
    while text:
        end, size = 0, 0
        for character in text:
            count = len(character.encode())
            if size + count > budget:
                break
            end += 1
            size += count
        if end < len(text):
            paragraph = text.rfind('\n', 0, end)
            if paragraph >= end // 2:
                end = paragraph + 1
        parts.append(text[:end])
        text = text[end:]
    return [f'[{index}/{len(parts)}] ' + part for index, part in enumerate(parts, 1)]


def visible_text(item):
    value = item.get('text', '')
    if not isinstance(value, str):
        raise DotError('dot_invalid_reply')
    questions = item.get('questions') or []
    if not isinstance(questions, list):
        raise DotError('dot_invalid_reply')
    extra = []
    for question in questions:
        if not isinstance(question, dict) or not isinstance(question.get('title'), str):
            raise DotError('dot_invalid_reply')
        extra.append(question['title'])
        options = question.get('options') or []
        if not isinstance(options, list) or any(not isinstance(option, str) for option in options):
            raise DotError('dot_invalid_reply')
        extra.extend(f'{i}. {option}' for i, option in enumerate(options, 1))
    return '\n\n'.join(part for part in (value, '\n'.join(extra)) if part)


class DotStore:
    def __init__(self, db, now=time.time):
        self.db, self.now = db, now

    def _get(self, key, default=None):
        row = self.db.execute('SELECT v FROM meta WHERE k=?', ('dot.' + key,)).fetchone()
        return json.loads(row[0]) if row else default

    def _put(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',
                        ('dot.' + key, json.dumps(value, ensure_ascii=False)))

    @property
    def mode(self):
        return self._get('mode', 'webhook')

    @property
    def binding(self):
        return self._get('binding')

    def bind(self, binding):
        required = {'account_id', 'user_id', 'dot_id', 'thread_id', 'name', 'protocol_verified'}
        if isinstance(binding, dict) and binding.get('transport') == 'desktop_native_room':
            required |= {'transport', 'messaging_room_id', 'reply_anchor_message_id'}
        if not isinstance(binding, dict) or set(binding) != required:
            raise DotError('dot_invalid_binding')
        if any(not isinstance(binding[k], str) or not 0 < len(binding[k]) <= 256
               for k in required - {'protocol_verified'}) or type(binding['protocol_verified']) is not bool:
            raise DotError('dot_invalid_binding')
        if self.mode != 'webhook' or self.db.execute(
                "SELECT 1 FROM dot_jobs WHERE state NOT IN ('api_accepted','failed','expired','rolled_back') LIMIT 1").fetchone():
            raise DotError('dot_binding_in_use')
        with self.db:
            if self.binding != binding:
                self._put('room_cursor', binding.get('reply_anchor_message_id'))
            self._put('binding', binding)
        return {'bound': True, 'protocol_verified': binding['protocol_verified']}

    def record_incoming(self, message_id, received_at, *, force_webhook=False):
        """Called inside the message-ingest transaction, before its commit."""
        route = 'webhook'
        if not force_webhook and self.mode == 'direct':
            route = 'direct'
        elif not force_webhook and self.mode == 'canary':
            row = self.db.execute('SELECT text FROM messages WHERE id=?', (message_id,)).fetchone()
            if row and row[0].startswith(CANARY_PREFIX):
                route = 'direct'
        self.db.execute('INSERT OR IGNORE INTO bridge_routes VALUES (?,?,?)',
                        (message_id, route, self.now()))
        if route == 'direct':
            self.db.execute('''INSERT OR IGNORE INTO dot_jobs
              (message_id,client_message_id,reply_chain_id,state,received_at)
              VALUES (?,?,?,'queued',?)''',
              (message_id, str(uuid.uuid4()), str(uuid.uuid4()), received_at))

    def route(self, message_id):
        row = self.db.execute('SELECT route FROM bridge_routes WHERE message_id=?', (message_id,)).fetchone()
        return row[0] if row else 'webhook'

    def activate(self, *, canary=False):
        if not self.binding or not self.binding['protocol_verified']:
            raise DotError('dot_protocol_unverified')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if self.db.execute("SELECT 1 FROM replies WHERE state='sending' LIMIT 1").fetchone():
                raise DotError('wechat_send_in_progress')
            if self.db.execute('''SELECT 1 FROM messages m JOIN bridge_routes r ON r.message_id=m.id
              WHERE r.route='webhook' AND m.received BETWEEN ? AND ?
              AND NOT EXISTS (SELECT 1 FROM replies p WHERE p.message_id=m.id) LIMIT 1''',
              (self.now() - REPLY_WINDOW, self.now())).fetchone():
                raise DotError('webhook_messages_in_progress')
            cutoff = self.db.execute('SELECT COALESCE(MAX(rowid),0) FROM messages').fetchone()[0]
            self._put('mode', 'canary' if canary else 'direct')
            self._put('cutover_seq', cutoff)
        return {'mode': self.mode, 'cutover_seq': cutoff}

    def rollback(self):
        self.expire()
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            self._put('mode', 'webhook')
            self.db.execute("UPDATE bridge_routes SET route='webhook' WHERE message_id IN (SELECT message_id FROM dot_jobs WHERE state='queued')")
            result = self.db.execute("UPDATE dot_jobs SET state='rolled_back' WHERE state='queued'")
        return {'mode': 'webhook', 'returned_unsent': result.rowcount}

    def recover(self):
        with self.db:
            self.db.execute("UPDATE dot_jobs SET state='submission_unknown' WHERE state='submitting'")
            self.db.execute("UPDATE dot_jobs SET state='send_unknown' WHERE message_id IN (SELECT message_id FROM dot_outbox WHERE state='sending')")
            self.db.execute("UPDATE dot_outbox SET state='unknown' WHERE state='sending'")

    def expire(self):
        with self.db:
            self.db.execute("UPDATE dot_jobs SET state='expired',error_category='reply_window_expired' WHERE state NOT IN ('expired','rolled_back') AND received_at<?",
                            (self.now() - REPLY_WINDOW,))
            self.db.execute("UPDATE dot_outbox SET state='expired' WHERE state='ready' AND message_id IN (SELECT message_id FROM dot_jobs WHERE state='expired')")

    def job(self, message_id):
        cursor = self.db.execute('''SELECT j.*,m.text FROM dot_jobs j
          JOIN messages m ON m.id=j.message_id WHERE j.message_id=?''', (message_id,))
        row = cursor.fetchone()
        return dict(zip([column[0] for column in cursor.description], row)) if row else None

    def next_job(self):
        self.expire()
        row = self.db.execute("SELECT message_id FROM dot_jobs WHERE state NOT IN ('api_accepted','failed','expired','rolled_back') ORDER BY received_at,rowid LIMIT 1").fetchone()
        if row is None:
            return None
        job = self.job(row[0])
        if job['state'] == 'queued' and self.mode not in ('direct', 'canary'):
            return None
        return job

    def submitting(self, message_id):
        self.expire()
        if self.mode not in ('direct', 'canary'):
            raise DotError('dot_route_disabled')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            first = self.db.execute("SELECT message_id FROM dot_jobs WHERE state NOT IN ('api_accepted','failed','expired','rolled_back') ORDER BY received_at,rowid LIMIT 1").fetchone()
            if first is None or first[0] != message_id:
                raise DotError('dot_job_not_next')
            result = self.db.execute("UPDATE dot_jobs SET state='submitting',claimed_at=? WHERE message_id=? AND state='queued'", (self.now(), message_id))
            if result.rowcount != 1:
                raise DotError('dot_job_not_queued')
            self.metric(message_id, 'claimed')
        return self.job(message_id)

    def submitted(self, message_id, *, turn_id=None, written_at=None, error=None):
        job = self.job(message_id)
        if job is None:
            raise DotError('dot_unknown_job')
        state = 'submission_unknown' if error else 'waiting_reply'
        with self.db:
            self.db.execute('''UPDATE dot_jobs SET
              state=CASE WHEN state IN ('submitting','submission_unknown') THEN ? ELSE state END,
              written_at=COALESCE(written_at,?),admitted_at=COALESCE(admitted_at,?),
              turn_id=COALESCE(turn_id,?),error_category=? WHERE message_id=?''',
              (state, written_at, None if error else self.now(), turn_id, error, message_id))
            if written_at is not None:
                self.metric(message_id, 'native_send_started' if (self.binding or {}).get('transport') == 'desktop_native_room' else 'ws_written', written_at)
            if not error:
                self.metric(message_id, 'admitted')

    def mark(self, message_id, state, category):
        if state not in ('submission_unknown', 'requires_user_action', 'failed'):
            raise DotError('dot_invalid_job_state')
        if not isinstance(category, str) or not category.replace('_', '').isalnum() or len(category) > 100:
            raise DotError('dot_invalid_error_category')
        with self.db:
            self.db.execute('UPDATE dot_jobs SET state=?,error_category=? WHERE message_id=?',
                            (state, category, message_id))

    def metric(self, message_id, stage, at=None):
        self.db.execute('INSERT OR IGNORE INTO dot_metrics VALUES (?,?,?)',
                        (message_id, stage, self.now() if at is None else at))

    def accept_item(self, thread_id, item, *, completed):
        binding = self.binding
        if not completed or not binding or thread_id != binding['thread_id'] or not isinstance(item, dict):
            return {'accepted': False}
        if item.get('type') != 'agentMessage' or item.get('delivery') != 'async':
            return {'accepted': False}
        metadata = item.get('messageMetadata')
        aeon = metadata.get('aeon') if isinstance(metadata, dict) else None
        if not isinstance(aeon, dict) or type(aeon.get('version')) is not int or aeon['version'] != 1:
            return {'accepted': False, 'category': 'dot_reply_metadata_missing'}
        chain, item_id = aeon.get('threadId'), item.get('id')
        if not isinstance(chain, str) or not isinstance(item_id, str) or not 0 < len(item_id) <= 256:
            return {'accepted': False}
        row = self.db.execute("SELECT message_id FROM dot_jobs WHERE reply_chain_id=? AND state NOT IN ('expired','rolled_back','queued')", (chain,)).fetchone()
        if row is None:
            return {'accepted': False}
        message_id = row[0]
        job = self.job(message_id)
        if self.now() - job['received_at'] > REPLY_WINDOW:
            self.expire()
            return {'accepted': False, 'category': 'reply_window_expired'}
        return self._enqueue(message_id, item_id, visible_text(item))

    def _enqueue(self, message_id, item_id, text):
        if not text.strip():
            return {'accepted': False, 'category': 'dot_empty_reply'}
        digest = hashlib.sha256(text.encode()).hexdigest()
        previous = self.db.execute('SELECT message_id,digest FROM dot_items WHERE item_id=?', (item_id,)).fetchone()
        if previous:
            if previous != (message_id, digest):
                raise DotError('dot_reply_changed_after_completion')
            return {'accepted': False, 'duplicate': True}
        parts = split_text(text)
        first = not self.db.execute('SELECT 1 FROM dot_items WHERE message_id=?', (message_id,)).fetchone()
        suffix = hashlib.sha256(item_id.encode()).hexdigest()[:20]
        with self.db:
            self.db.execute('INSERT INTO dot_items VALUES (?,?,?,?)', (item_id, message_id, digest, self.now()))
            for index, part in enumerate(parts):
                key = message_id + '-dot-reply'
                if not first or index:
                    key += '-' + suffix + '-' + str(index)
                self.db.execute('INSERT INTO dot_outbox(item_id,message_id,part,text,idempotency_key) VALUES (?,?,?,?,?)',
                                (item_id, message_id, index, part, key))
            self.db.execute("UPDATE dot_jobs SET state=CASE WHEN state IN ('submitting','submission_unknown','admitted','waiting_reply','requires_user_action') THEN 'reply_ready' ELSE state END,first_reply_at=COALESCE(first_reply_at,?) WHERE message_id=?",
                            (self.now(), message_id))
            self.metric(message_id, 'reply_complete')
        return {'accepted': True, 'message_id': message_id, 'parts': len(parts)}

    def unsent(self, message_id):
        """Only a proven pre-send failure may return to queued with the same ID."""
        with self.db:
            result = self.db.execute("UPDATE dot_jobs SET state='queued',error_category=NULL WHERE message_id=? AND state='submitting' AND room_message_id IS NULL", (message_id,))
            if result.rowcount != 1:
                raise DotError('dot_submission_not_proven_unsent')

    def room_message(self, message):
        binding = self.binding or {}
        if (binding.get('transport') != 'desktop_native_room' or not isinstance(message, dict) or
                message.get('roomId') != binding.get('messaging_room_id') or
                message.get('deleted') or message.get('deliveryState') not in ('', None)):
            return {'accepted': False}
        iid = message.get('id')
        if not isinstance(iid, str) or not 0 < len(iid) <= 256:
            raise DotError('dot_invalid_room_message')
        if message.get('self') is True and message.get('role') == 'user':
            row = self.db.execute("SELECT message_id,room_message_id FROM dot_jobs WHERE client_message_id=? AND state NOT IN ('queued','expired','rolled_back')", (message.get('requestId'),)).fetchone()
            if not row:
                return {'accepted': False}
            if message.get('replyToMessageId') != binding['reply_anchor_message_id']:
                raise DotError('dot_room_submission_parent_mismatch')
            if row[1] is not None and row[1] != iid:
                raise DotError('dot_submission_id_conflict')
            with self.db:
                self.db.execute('UPDATE dot_jobs SET room_message_id=? WHERE message_id=?', (iid, row[0]))
                self.db.execute('INSERT OR IGNORE INTO dot_room_messages VALUES (?,?,?,?)',
                                (iid, row[0], message.get('replyToMessageId'), self.now()))
                self.submitted(row[0])
            return {'accepted': True, 'admission': True, 'message_id': row[0]}
        if (message.get('self') is not False or message.get('role') != 'user' or
                message.get('senderAeonId') != binding.get('dot_id') or
                message.get('replyRootMessageId') != binding['reply_anchor_message_id']):
            return {'accepted': False}
        parent = message.get('replyToMessageId')
        row = self.db.execute('SELECT message_id FROM dot_room_messages WHERE room_message_id=?', (parent,)).fetchone()
        if not row:
            with self.db:
                self.db.execute('INSERT OR IGNORE INTO dot_room_inbox VALUES (?,?,?)',
                                (iid, json.dumps(message, ensure_ascii=False), self.now()))
            return {'accepted': False, 'category': 'dot_reply_parent_unmatched'}
        job = self.job(row[0])
        if not job or job['state'] in ('queued', 'expired', 'rolled_back') or self.now() - job['received_at'] > REPLY_WINDOW:
            return {'accepted': False, 'category': 'reply_window_expired'}
        text = visible_text(message)
        if not text.strip():
            text = '爱丽丝发来需要确认的请求，请打开 Codex 查看并处理。'
        result = self._enqueue(row[0], iid, text)
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO dot_room_messages VALUES (?,?,?,?)',
                            (iid, row[0], parent, self.now()))
            self.db.execute('DELETE FROM dot_room_inbox WHERE room_message_id=?', (iid,))
            if message.get('requiresUserAction'):
                self.db.execute("UPDATE dot_jobs SET error_category='dot_requires_user_action' WHERE message_id=?", (row[0],))
        return result

    def reconcile_room_inbox(self):
        accepted = 0
        with self.db:
            self.db.execute('DELETE FROM dot_room_inbox WHERE received_at<?', (self.now() - REPLY_WINDOW,))
        while True:
            progress = False
            for (payload,) in self.db.execute('SELECT payload FROM dot_room_inbox ORDER BY received_at,rowid').fetchall():
                message = json.loads(payload)
                if not self.db.execute('SELECT 1 FROM dot_room_messages WHERE room_message_id=?', (message.get('replyToMessageId'),)).fetchone():
                    continue
                result = self.room_message(message)
                if result.get('accepted') or result.get('duplicate'):
                    accepted += int(result.get('accepted', False))
                    progress = True
            if not progress:
                return {'accepted': accepted}

    def room_cursor(self, cursor=None):
        if cursor is not None:
            if not isinstance(cursor, str) or not 0 < len(cursor) <= 256:
                raise DotError('dot_invalid_room_cursor')
            with self.db:
                self._put('room_cursor', cursor)
        return self._get('room_cursor') or (self.binding or {}).get('reply_anchor_message_id')

    def next_part(self):
        self.expire()
        cursor = self.db.execute('''SELECT o.* FROM dot_outbox o JOIN dot_jobs j USING(message_id)
          WHERE o.state='ready' AND j.state NOT IN ('expired','send_unknown','failed','rolled_back')
          ORDER BY j.received_at,o.rowid LIMIT 1''')
        row = cursor.fetchone()
        return dict(zip([column[0] for column in cursor.description], row)) if row else None

    def begin_part(self, item_id, part):
        with self.db:
            result = self.db.execute("UPDATE dot_outbox SET state='sending',started_at=? WHERE item_id=? AND part=? AND state='ready'", (self.now(), item_id, part))
            if result.rowcount != 1:
                raise DotError('dot_part_not_ready')
            mid = self.db.execute('SELECT message_id FROM dot_outbox WHERE item_id=? AND part=?', (item_id, part)).fetchone()[0]
            self.db.execute("UPDATE dot_jobs SET state='sending' WHERE message_id=?", (mid,))
            self.metric(mid, 'wechat_send_started')

    def finish_part(self, item_id, part, state):
        if state not in ('api_accepted', 'api_rejected', 'unknown', 'sending'):
            state = 'unknown'
        with self.db:
            mid = self.db.execute('SELECT message_id FROM dot_outbox WHERE item_id=? AND part=?', (item_id, part)).fetchone()[0]
            self.db.execute('UPDATE dot_outbox SET state=?,finished_at=? WHERE item_id=? AND part=?',
                            (state, self.now(), item_id, part))
            remaining = self.db.execute("SELECT 1 FROM dot_outbox WHERE message_id=? AND state!='api_accepted' LIMIT 1", (mid,)).fetchone()
            job_state = ('api_accepted' if not remaining else 'reply_ready') if state == 'api_accepted' else ('failed' if state == 'api_rejected' else 'send_unknown')
            self.db.execute('UPDATE dot_jobs SET state=? WHERE message_id=?', (job_state, mid))
            if not remaining:
                self.metric(mid, 'wechat_api_accepted')

    def status(self):
        self.expire()
        counts = dict(self.db.execute('SELECT state,COUNT(*) FROM dot_jobs GROUP BY state').fetchall())
        return {'mode': self.mode, 'binding': self.binding, 'jobs': counts,
                'outbox': dict(self.db.execute('SELECT state,COUNT(*) FROM dot_outbox GROUP BY state').fetchall()),
                'unmatched_room_messages': self.db.execute('SELECT COUNT(*) FROM dot_room_inbox').fetchone()[0],
                'cutover_seq': self._get('cutover_seq')}

    def metrics(self):
        result = []
        for mid, received, state in self.db.execute('SELECT message_id,received_at,state FROM dot_jobs ORDER BY received_at DESC LIMIT 100'):
            stages = dict(self.db.execute('SELECT stage,at FROM dot_metrics WHERE message_id=?', (mid,)))
            result.append({'message_id': mid, 'received_at': received, 'state': state, 'stages': stages})
        return {'jobs': result}
