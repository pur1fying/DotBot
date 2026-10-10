import tempfile
from pathlib import Path
import unittest

from dot_auth import DotError
from dot_store import split_text
from ilink_adapter import Binding, TextAdapter, AdapterError


class DotStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'messages.sqlite'
        self.clock = 10000
        self.sent = []
        self.adapter = self.open()
        self.store = self.adapter.dot
        self.binding = {'account_id': 'account', 'user_id': 'user', 'dot_id': 'alice',
                        'thread_id': 'thread', 'name': 'Alice', 'protocol_verified': True}
        self.store.bind(self.binding)

    def open(self):
        return TextAdapter(self.path, Binding('bot', 'owner', 'SYNTHETIC'),
                           test_exchange=self.exchange, clock=lambda: self.clock)

    def exchange(self, path, headers, body):
        self.sent.append(body)
        return {'ret': 0}

    def tearDown(self):
        self.adapter.close()
        self.tmp.cleanup()

    def ingest(self, mid=1):
        self.adapter.ingest({'msgs': [{'message_id': mid, 'from_user_id': 'owner',
            'to_user_id': 'bot', 'message_type': 1, 'message_state': 2,
            'context_token': 'SYNTHETIC-CONTEXT',
            'item_list': [{'type': 1, 'text_item': {'text': str(mid)}}]}]})
        return self.adapter.read_messages()[-1]['message_id']

    def queued(self, mid=1):
        if self.store.mode != 'direct': self.store.activate()
        return self.ingest(mid)

    def item(self, mid, *, text='完整答复', iid='item-1'):
        return {'id': iid, 'type': 'agentMessage', 'delivery': 'async', 'text': text,
                'messageMetadata': {'aeon': {'version': 1, 'threadId': self.store.job(mid)['reply_chain_id']}}}

    def drain(self):
        while part := self.store.next_part():
            self.store.begin_part(part['item_id'], part['part'])
            result = self.adapter.reply(part['message_id'], part['text'], part['idempotency_key'], _route='direct')
            self.store.finish_part(part['item_id'], part['part'], result['state'])

    def test_routes_hide_direct_jobs_from_legacy_pending_and_reply(self):
        mid = self.queued()
        self.assertEqual(self.adapter.read_page(pending_only=True)['messages'], [])
        self.assertFalse(self.adapter.read_page()['messages'][0]['reply_eligible'])
        with self.assertRaisesRegex(AdapterError, 'message_route_mismatch'):
            self.adapter.reply(mid, 'legacy', mid + '-dot-reply')
        self.assertEqual(self.sent, [])

    def test_activation_requires_protocol_and_drained_old_messages(self):
        self.store.bind({**self.binding, 'protocol_verified': False})
        with self.assertRaisesRegex(DotError, 'dot_protocol_unverified'): self.store.activate()
        self.store.bind(self.binding)
        mid = self.ingest()
        with self.assertRaisesRegex(DotError, 'webhook_messages_in_progress'): self.store.activate()
        self.adapter.reply(mid, 'old', mid + '-dot-reply')
        self.assertEqual(self.store.activate()['cutover_seq'], 1)
        self.assertEqual(self.store.route(mid), 'webhook')

    def test_ids_persist_and_duplicate_ingest_does_not_enqueue_twice(self):
        mid = self.queued()
        before = self.store.job(mid)
        self.ingest()
        self.adapter.close()
        self.adapter = self.open(); self.store = self.adapter.dot
        after = self.store.job(mid)
        self.assertEqual(before['client_message_id'], after['client_message_id'])
        self.assertEqual(before['reply_chain_id'], after['reply_chain_id'])
        self.assertEqual(self.store.status()['jobs'], {'queued': 1})

    def test_fifo_waits_for_first_reply_and_supports_later_followup(self):
        first = self.queued(1); second = self.queued(2)
        self.store.submitting(first)
        self.store.submitted(first, turn_id='turn', written_at=self.clock)
        self.assertEqual(self.store.next_job()['message_id'], first)
        self.store.accept_item('thread', self.item(first), completed=True)
        self.drain()
        self.assertEqual(self.store.next_job()['message_id'], second)
        self.store.accept_item('thread', self.item(first, text='后续完整消息', iid='item-2'), completed=True)
        self.drain()
        self.assertEqual(len(self.sent), 2)

    def test_only_complete_async_messages_with_matching_chain_are_accepted(self):
        mid = self.queued(); self.store.submitting(mid)
        item = self.item(mid)
        for wrong in ({**item, 'type': 'reasoning'}, {**item, 'delivery': None},
                      {**item, 'messageMetadata': {'aeon': {'version': 1, 'threadId': 'other'}}},
                      {**item, 'messageMetadata': {'aeon': {'version': True, 'threadId': 'other'}}}):
            self.assertFalse(self.store.accept_item('thread', wrong, completed=True)['accepted'])
        self.assertFalse(self.store.accept_item('other-thread', item, completed=True)['accepted'])
        self.assertFalse(self.store.accept_item('thread', item, completed=False)['accepted'])
        # Async commentary is an explicit visible dot message, unlike internal commentary.
        self.assertTrue(self.store.accept_item('thread', {**item, 'phase': 'commentary'}, completed=True)['accepted'])

    def test_reply_before_ack_does_not_regress_ready_state(self):
        mid = self.queued(); self.store.submitting(mid)
        self.store.accept_item('thread', self.item(mid), completed=True)
        self.store.submitted(mid, turn_id='turn')
        self.assertEqual(self.store.job(mid)['state'], 'reply_ready')

    def test_replay_and_completed_content_conflict(self):
        mid = self.queued(); self.store.submitting(mid)
        item = self.item(mid)
        self.store.accept_item('thread', item, completed=True); self.drain()
        self.assertTrue(self.store.accept_item('thread', item, completed=True)['duplicate'])
        self.drain(); self.assertEqual(len(self.sent), 1)
        with self.assertRaisesRegex(DotError, 'dot_reply_changed_after_completion'):
            self.store.accept_item('thread', {**item, 'text': 'changed'}, completed=True)

    def test_partial_send_restart_never_resends_accepted_or_unknown_parts(self):
        mid = self.queued(); self.store.submitting(mid)
        self.store.accept_item('thread', self.item(mid, text='中文🌍\n' * 1200), completed=True)
        first = self.store.next_part()
        self.assertEqual(first['idempotency_key'], mid + '-dot-reply')
        self.store.begin_part(first['item_id'], first['part'])
        self.adapter.reply(mid, first['text'], first['idempotency_key'], _route='direct')
        self.store.finish_part(first['item_id'], first['part'], 'api_accepted')
        second = self.store.next_part(); self.store.begin_part(second['item_id'], second['part'])
        self.adapter.close(); self.adapter = self.open(); self.store = self.adapter.dot
        self.assertEqual(self.store.job(mid)['state'], 'send_unknown')
        self.assertIsNone(self.store.next_part())
        self.assertEqual(self.store.status()['outbox']['api_accepted'], 1)
        self.assertEqual(len(self.sent), 1)

    def test_submission_unknown_stays_on_direct_route_during_rollback(self):
        first = self.queued(1); second = self.queued(2)
        self.store.submitting(first)
        self.adapter.close(); self.adapter = self.open(); self.store = self.adapter.dot
        self.assertEqual(self.store.job(first)['state'], 'submission_unknown')
        self.assertEqual(self.store.rollback()['returned_unsent'], 1)
        self.assertEqual(self.store.route(first), 'direct')
        self.assertEqual(self.store.route(second), 'webhook')
        self.assertEqual([m['message_id'] for m in self.adapter.read_page(pending_only=True)['messages']], [second])

    def test_expiry_and_questions(self):
        mid = self.queued(); self.store.submitting(mid)
        item = self.item(mid, text='')
        item['questions'] = [{'title': '请选择', 'options': ['一', '二']}]
        self.store.accept_item('thread', item, completed=True)
        self.assertIn('请选择', self.store.next_part()['text'])
        self.clock += 3601
        self.assertIsNone(self.store.next_part())
        self.assertIsNone(self.store.next_job())
        self.assertEqual(self.sent, [])

    def test_utf8_split_preserves_all_text(self):
        text = ('中文🌍\n' * 1000) + '结尾'
        parts = split_text(text)
        self.assertTrue(all(len(p.encode()) <= 2000 for p in parts))
        self.assertEqual(''.join(p.split('] ', 1)[1] for p in parts), text)


if __name__ == '__main__': unittest.main()
