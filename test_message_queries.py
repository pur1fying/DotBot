import json
from pathlib import Path
import tempfile
import unittest

import jsonschema

from ilink_adapter import Binding, REPLY_MAX_AGE
from ilink_broker import Broker
from service import PersistentService, process


class MessageQueryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.now = 10000
        self.sent = []
        def exchange(path, headers, body):
            self.sent.append(path)
            return {'ret': 0}
        self.broker = Broker(self.root, Binding('bot', 'owner', 'SYNTHETIC_TOKEN'),
                             exchange, clock=lambda: self.now)
        self.service = PersistentService(self.root / 'mcp.sqlite', now=lambda: self.now,
            wechat=lambda method, args: self.broker.call({'method': method, 'arguments': args}))

    def tearDown(self):
        self.service.close()
        self.tmp.cleanup()

    def ingest(self, count):
        adapter = self.broker.adapter()
        try:
            adapter.ingest({'msgs': [
                {'message_id': i, 'from_user_id': 'owner', 'to_user_id': 'bot',
                 'message_type': 1, 'message_state': 2, 'context_token': 'SYNTHETIC_CONTEXT',
                 'item_list': [{'type': 1, 'text_item': {'text': 'message ' + str(i)}}]}
                for i in range(1, count + 1)]})
            return [r[0] for r in adapter.db.execute('SELECT id FROM messages ORDER BY rowid')]
        finally:
            adapter.close()

    def read(self, arguments=None, pending=False):
        name = 'wechat_read_pending_messages' if pending else 'wechat_read_messages'
        result = self.service.dispatch('tools/call', {'name': name, 'arguments': arguments or {}})
        schema = next(t['outputSchema'] for t in self.service.tools()['tools'] if t['name'] == name)
        jsonschema.validate(result['structuredContent'], schema, format_checker=jsonschema.FormatChecker())
        self.assertEqual(json.loads(result['content'][0]['text']), result['structuredContent'])
        self.assertNotIn('SYNTHETIC_CONTEXT', json.dumps(result))
        self.assertNotIn('SYNTHETIC_TOKEN', json.dumps(result))
        return result['structuredContent']

    def test_exact_lookup_and_pagination_reach_message_25_in_text_mode(self):
        mids = self.ingest(25)
        first = self.read()
        self.assertEqual([m['local_seq'] for m in first['messages']], list(range(1, 21)))
        self.assertTrue(first['has_more'])
        second = self.read({'after_seq': first['next_after_seq']})
        self.assertEqual([m['local_seq'] for m in second['messages']], list(range(21, 26)))
        self.assertFalse(second['has_more'])
        self.assertEqual(second['next_after_seq'], 25)
        exact = self.read({'message_id': mids[-1]})
        self.assertEqual(exact['messages'][0]['text'], 'message 25')
        self.assertIsNone(exact['next_after_seq'])
        self.assertEqual(exact['messages'][0]['received_at'], '1970-01-01T02:46:40+00:00')
        self.assertEqual(exact['messages'][0]['reply_state'], 'never_replied')
        self.assertTrue(exact['messages'][0]['reply_eligible'])
        self.assertEqual(self.read({'message_id': 'not-present'})['messages'], [])

    def test_pending_excludes_expired_future_and_every_send_attempt(self):
        mids = self.ingest(8)
        adapter = self.broker.adapter()
        try:
            with adapter.db:
                adapter.db.execute('UPDATE messages SET received=? WHERE id=?', (self.now-REPLY_MAX_AGE-1, mids[0]))
                adapter.db.execute('UPDATE messages SET received=? WHERE id=?', (self.now-REPLY_MAX_AGE, mids[1]))
                adapter.db.execute('UPDATE messages SET received=? WHERE id=?', (self.now+1, mids[6]))
                for index, state in enumerate(('api_accepted', 'sending', 'unknown', 'api_rejected'), 2):
                    adapter.db.execute('INSERT INTO replies VALUES (?,?,?,?,?)',
                        ('key'+str(index), mids[index], 'already attempted', 'client'+str(index), state))
        finally:
            adapter.close()
        pending = self.read(pending=True)
        self.assertEqual([m['local_seq'] for m in pending['messages']], [2, 8])
        self.assertEqual(self.read(pending=True), pending)
        self.assertEqual(self.sent, [])
        all_messages = self.read()['messages']
        self.assertFalse(all_messages[0]['reply_eligible'])
        self.assertFalse(all_messages[6]['reply_eligible'])
        self.assertEqual([m['reply_state'] for m in all_messages[2:6]],
                         ['api_accepted', 'sending', 'unknown', 'api_rejected'])

    def test_pending_pagination_and_acknowledged_reply_removal(self):
        mids = self.ingest(25)
        first = self.read({'limit': 2}, pending=True)
        self.assertTrue(first['has_more'])
        second = self.read({'after_seq': first['next_after_seq'], 'limit': 2}, pending=True)
        self.assertEqual([m['local_seq'] for m in second['messages']], [3, 4])
        args = {'message_id': mids[0], 'text': 'synthetic reply', 'idempotency_key': mids[0]+'-dot-reply'}
        for _ in range(2):
            self.service.dispatch('tools/call', {'name': 'wechat_reply_text', 'arguments': args})
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.read({'limit': 1}, pending=True)['messages'][0]['message_id'], mids[1])
        replied = self.read({'message_id': mids[0]})['messages'][0]
        self.assertEqual(replied['reply_state'], 'api_accepted')
        self.assertFalse(replied['reply_eligible'])

    def test_queries_do_not_move_relay_cursor(self):
        self.ingest(25)
        self.service.sync_wechat()
        self.assertEqual(self.service.db.execute('SELECT seq FROM wechat_cursor').fetchone()[0], 20)
        self.read({'after_seq': 24})
        self.read(pending=True)
        self.assertEqual(self.service.db.execute('SELECT seq FROM wechat_cursor').fetchone()[0], 20)
        self.service.sync_wechat()
        self.assertEqual(self.service.db.execute('SELECT seq FROM wechat_cursor').fetchone()[0], 25)
        self.assertEqual(self.service.db.execute("SELECT count(*) FROM messages WHERE channel='wechat-owner'").fetchone()[0], 25)

    def test_invalid_query_arguments_are_rpc_parameter_errors(self):
        meta = {'io.modelcontextprotocol/protocolVersion': '2026-07-28',
                'io.modelcontextprotocol/clientCapabilities': {}}
        invalid = [({'message_id': 'id', 'after_seq': 0}, False), ({'message_id': None}, False),
                   ({'message_id': ''}, False), ({'after_seq': True}, False),
                   ({'after_seq': -1}, False), ({'limit': 0}, False), ({'limit': 21}, False),
                   ({'limit': True}, False), ({'unknown': 1}, False), ({'message_id': 'id'}, True)]
        for args, pending in invalid:
            name = 'wechat_read_pending_messages' if pending else 'wechat_read_messages'
            with self.subTest(args=args, pending=pending):
                response = process(self.service, {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                    'params': {'name': name, 'arguments': args, '_meta': meta}})
                self.assertEqual(response['error']['code'], -32602)
        self.assertEqual(self.sent, [])


if __name__ == '__main__':
    unittest.main()
