import tempfile
import unittest
from pathlib import Path
from ilink_adapter import Binding
from ilink_broker import Broker
from service import PersistentService, OfflineDelivery, FIXTURE_SECRET
from bridge import MockChatCallback


class ChannelTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.private = Path(self.tmp.name)
        self.sent = []
        self.binding = Binding('SYNTHETIC-BOT', 'SYNTHETIC-OWNER', 'SYNTHETIC-TOKEN')
        def exchange(path, headers, body):
            self.sent.append((path, body))
            return {'ret': 0}
        self.broker = Broker(self.private, self.binding, exchange, clock=lambda: 1000)
        self.callback = MockChatCallback(FIXTURE_SECRET, now=lambda: 1000)
        delivery = OfflineDelivery(now=lambda: 1000)
        delivery.callback = self.callback
        self.s = PersistentService(self.private / 'mcp.sqlite', delivery, now=lambda: 1000,
                                   wechat=lambda method, arguments: self.broker.call({'method': method, 'arguments': arguments}))
        self.msg = {'message_id': 1, 'from_user_id': 'SYNTHETIC-OWNER', 'to_user_id': 'SYNTHETIC-BOT',
                    'message_type': 1, 'message_state': 2, 'context_token': 'SYNTHETIC-CONTEXT',
                    'item_list': [{'type': 1, 'text_item': {'text': 'synthetic owner text'}}]}

    def tearDown(self):
        self.s.close()
        self.tmp.cleanup()

    def ingest(self):
        a = self.broker.adapter()
        try:
            a.ingest({'msgs': [self.msg]})
        finally:
            a.close()

    def subscription(self, event, channel):
        return {'name': event, 'arguments': {'channel_id': channel}, 'ttlMs': 60000,
                'delivery': {'mode': 'webhook', 'url': 'https://mock.invalid/callback', 'secret': FIXTURE_SECRET}}

    def test_real_event_schema_and_mock_subscription_isolation(self):
        events = self.s.dispatch('events/list', {})['events']
        self.assertEqual([e['name'] for e in events], ['message.created', 'wechat.message.created'])
        self.s.dispatch('events/subscribe', self.subscription('message.created', 'sandbox'))
        self.s.dispatch('events/subscribe', self.subscription('wechat.message.created', 'wechat-owner'))
        self.ingest()
        self.assertEqual(self.s.sync_wechat(), 1)
        self.assertEqual(len(self.callback.events), 1)
        self.assertEqual(self.callback.events[0]['data']['channel_id'], 'wechat-owner')
        self.assertNotIn('SYNTHETIC-CONTEXT', str(self.callback.events))
        self.assertEqual(self.s.sync_wechat(), 0)
        mock = self.s.dispatch('tools/call', {'name': 'read_mock_status', 'arguments': {}})['structuredContent']
        self.assertEqual(mock['messages'], [])
        self.assertEqual(mock['deliveries'], [])

    def test_reply_only_known_owner_message_no_proactive_send(self):
        with self.assertRaises(Exception):
            self.broker.call({'method': 'reply', 'arguments': {'message_id': 'unknown', 'text': 'x', 'idempotency_key': 'k'}})
        self.assertEqual(self.sent, [])
        self.ingest()
        message = self.broker.call({'method': 'read', 'arguments': {}})['messages'][0]
        self.s.sync_wechat()
        with self.assertRaises(Exception):
            self.s.dispatch('tools/call', {'name': 'reply_to_message', 'arguments': {'message_id': message['message_id'], 'text': 'x', 'idempotency_key': 'k'}})
        args = {'message_id': message['message_id'], 'text': 'synthetic reply', 'idempotency_key': 'once'}
        result = self.s.dispatch('tools/call', {'name': 'wechat_reply_text', 'arguments': args})['structuredContent']
        self.assertEqual(result['state'], 'api_accepted')
        self.s.dispatch('tools/call', {'name': 'wechat_reply_text', 'arguments': args})
        self.assertEqual(len(self.sent), 1)

    def test_default_mock_catalog_unchanged(self):
        other = PersistentService(self.private / 'mock.sqlite')
        try:
            self.assertEqual(len(other.dispatch('events/list', {})['events']), 1)
            self.assertFalse(any(t['name'].startswith('wechat_') for t in other.tools()['tools']))
        finally:
            other.close()

    def test_reopening_poll_adapter_does_not_reset_inflight_send(self):
        self.ingest()
        a = self.broker.adapter()
        try:
            mid = a.read_messages()[0]['message_id']
            with a.db:
                a.db.execute('INSERT INTO replies VALUES (?,?,?,?,?)', ('k', mid, 'text', 'client', 'sending'))
        finally:
            a.close()
        b = self.broker.adapter()
        try:
            self.assertEqual(b.db.execute('SELECT state FROM replies').fetchone()[0], 'sending')
        finally:
            b.close()


if __name__ == '__main__':
    unittest.main()
