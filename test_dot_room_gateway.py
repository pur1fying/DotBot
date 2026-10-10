import asyncio
import tempfile
from pathlib import Path
import unittest

from dot_auth import DotError
from dot_runner import Gateway
from dot_store import CANARY_PREFIX
from ilink_adapter import Binding
from ilink_broker import Broker
from service import PersistentService, OfflineDelivery, FIXTURE_SECRET

BINDING = {'account_id': 'account', 'user_id': 'user', 'dot_id': 'alice',
           'thread_id': 'thread', 'name': 'Alice', 'protocol_verified': True,
           'transport': 'desktop_native_room', 'messaging_room_id': 'room',
           'reply_anchor_message_id': 'anchor'}


class RoomGatewayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name); self.path.chmod(0o700)
        self.sent, self.batch = [], []
        self.clock = 10000
        def exchange(path, headers, body):
            if path.endswith('getupdates'):
                rows, self.batch = self.batch, []
                return {'ret': 0, 'msgs': rows}
            self.sent.append(body)
            return {'ret': 0}
        self.broker = Broker(self.path, Binding('bot', 'owner', 'SYNTHETIC'), exchange,
                             clock=lambda: self.clock)
        self.call('dot_bind', binding=BINDING)

    def tearDown(self): self.tmp.cleanup()

    def call(self, method, **args):
        return self.broker.call({'method': method, 'arguments': args})

    async def rpc(self, method, args=None): return self.call(method, **(args or {}))

    def add(self, mid, text='测试文字'):
        self.batch.append({'message_id': mid, 'from_user_id': 'owner', 'to_user_id': 'bot',
            'message_type': 1, 'message_state': 2, 'context_token': 'SYNTHETIC-CONTEXT',
            'item_list': [{'type': 1, 'text_item': {'text': text}}]})
        self.broker.poll_once()
        return self.call('read')['messages'][-1]['message_id']

    def own(self, job, iid='incoming'):
        return {'id': iid, 'roomId': 'room', 'role': 'user', 'self': True,
            'requestId': job['client_message_id'], 'replyToMessageId': 'anchor',
            'replyRootMessageId': 'anchor', 'text': '测试文字', 'createdAt': '2026-01-01T00:00:00Z'}

    def reply(self, iid='reply', parent='incoming', text='完整答复', **changes):
        return dict({'id': iid, 'roomId': 'room', 'role': 'user', 'self': False,
            'senderAeonId': 'alice', 'turnTrigger': 'chatgpt_messaging',
            'replyToMessageId': parent, 'replyRootMessageId': 'anchor',
            'text': text, 'createdAt': '2026-01-01T00:00:01Z'}, **changes)

    async def test_canary_only_claims_explicit_text_prefix(self):
        self.call('dot_canary')
        first = self.add(1, CANARY_PREFIX + '测试')
        other = self.add(2, '普通来信')
        self.assertEqual(self.call('dot_next')['message_id'], first)
        self.assertEqual([m['message_id'] for m in self.call('read_pending')['messages']], [other])

    async def test_gateway_fifo_native_parent_binding_replay_and_later_followup(self):
        self.call('dot_activate'); first = self.add(1); second = self.add(2)
        sends, messages = [], []
        fixture = self
        class Room:
            async def invoke(self, operation, args=None):
                if operation == 'status': return {'subscribed': True}
                if operation == 'send':
                    sends.append(args)
                    return fixture.own(fixture.call('dot_next'))
                if operation == 'messages':
                    return {'messages': messages, 'after': None}
        gateway = Gateway(self.rpc, Room(), BINDING, now=lambda: self.clock)
        await gateway.submit()
        self.assertEqual(len(sends), 1)
        self.assertEqual(sends[0]['replyToMessageId'], 'anchor')
        self.assertIn('确认', sends[0]['text'])
        await gateway.submit(); self.assertEqual(len(sends), 1)
        messages[:] = [self.reply(text='中文🌍\n' * 700)]
        await gateway.read_history(drain=True)
        self.assertGreater(len(self.sent), 1)
        self.assertTrue(all(len(row['msg']['item_list'][0]['text_item']['text'].encode()) <= 2000 for row in self.sent))
        self.assertEqual(self.call('dot_next')['message_id'], second)
        count = len(self.sent)
        self.call('dot_room_message', message=self.reply(text='中文🌍\n' * 700))
        await gateway.drain(); self.assertEqual(len(self.sent), count)
        self.call('dot_room_message', message=self.reply(iid='followup', parent='reply', text='稍后完整追问'))
        await gateway.drain(); self.assertEqual(len(self.sent), count + 1)
        self.assertNotIn('SYNTHETIC-CONTEXT', str(self.call('dot_metrics')))

    async def test_uncertain_submission_is_not_retried_after_gateway_restart(self):
        self.call('dot_activate'); self.add(1)
        attempts = []
        class Room:
            async def invoke(self, operation, args=None):
                if operation == 'status': return {'subscribed': True}
                attempts.append(args); raise DotError('dot_submission_unknown')
        gateway = Gateway(self.rpc, Room(), BINDING)
        with self.assertRaises(DotError): await gateway.submit()
        self.call('dot_recover')
        await Gateway(self.rpc, Room(), BINDING).submit()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(self.call('dot_next')['state'], 'submission_unknown')

    async def test_out_of_order_reply_is_saved_until_own_admission_is_proven(self):
        self.call('dot_activate'); self.add(1)
        job = self.call('dot_next'); self.call('dot_submitting', message_id=job['message_id'])
        self.call('dot_room_message', message=self.reply())
        self.assertIsNone(self.call('dot_drain_one'))
        self.call('dot_room_message', message=self.own(job))
        self.assertEqual(self.call('dot_room_reconcile')['accepted'], 1)
        self.assertEqual(self.call('dot_drain_one')['state'], 'api_accepted')
        self.assertEqual(self.call('dot_status')['unmatched_room_messages'], 0)

    async def test_other_dot_room_parent_internal_message_and_expired_reply_do_not_send(self):
        self.call('dot_activate'); self.add(1)
        job = self.call('dot_next'); self.call('dot_submitting', message_id=job['message_id'])
        self.call('dot_room_message', message=self.own(job))
        for message in [self.reply(roomId='foreign'), self.reply(senderAeonId='other'),
                        self.reply(role='assistant'),
                        self.reply(replyRootMessageId='other'), self.reply(parent='unknown')]:
            self.assertFalse(self.call('dot_room_message', message=message)['accepted'])
        self.clock += 3601
        self.assertFalse(self.call('dot_room_message', message=self.reply())['accepted'])
        self.assertIsNone(self.call('dot_drain_one'))

    async def test_persistent_mode_public_reply_is_backfilled_behind_cursor(self):
        self.call('dot_activate'); self.add(1)
        job = self.call('dot_next'); self.call('dot_submitting', message_id=job['message_id'])
        self.call('dot_room_message', message=self.own(job))
        self.call('dot_room_set_cursor', cursor='already-passed-reply')
        message = self.reply(iid='persistent-reply', text='5', turnTrigger='persistent_mode')
        queries = []
        class Room:
            async def invoke(self, operation, args=None):
                queries.append(args)
                return {'messages': [message], 'after': None}
        gateway = Gateway(self.rpc, Room(), BINDING)
        await gateway.reconcile_pending(drain=True)
        self.assertEqual(queries[0]['after'], 'incoming')
        self.assertEqual(self.call('dot_status')['jobs'], {'api_accepted': 1})
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0]['msg']['item_list'][0]['text_item']['text'], '5')
        await gateway.drain()
        self.assertEqual(len(self.sent), 1)

    async def test_visible_confirmation_options_are_text_only_and_not_approved(self):
        self.call('dot_activate'); self.add(1)
        job = self.call('dot_next'); self.call('dot_submitting', message_id=job['message_id'])
        self.call('dot_room_message', message=self.own(job))
        self.call('dot_room_message', message=self.reply(text='', questions=[{
            'title': '是否继续？', 'options': ['继续', '取消']}], requiresUserAction=True))
        self.call('dot_drain_one')
        text = self.sent[0]['msg']['item_list'][0]['text_item']['text']
        self.assertIn('是否继续？', text); self.assertIn('1. 继续', text); self.assertIn('2. 取消', text)

    async def test_rollback_replays_unsent_jobs_even_after_relay_cursor_passed(self):
        self.call('dot_activate'); mid = self.add(1)
        service = PersistentService(self.path/'events.sqlite', OfflineDelivery(now=lambda: self.clock),
            now=lambda: self.clock, wechat=lambda method, args: self.call(method, **args))
        try:
            service.dispatch('events/subscribe', {'name': 'wechat.message.created',
                'arguments': {'channel_id': 'wechat-owner'}, 'delivery': {'mode': 'webhook',
                'url': 'https://mock.invalid/callback', 'secret': FIXTURE_SECRET}})
            service.sync_wechat()
            self.assertEqual(service.db.execute('SELECT seq FROM wechat_cursor').fetchone()[0], 1)
            self.assertEqual(service.db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)
            self.call('dot_rollback')
            self.assertEqual(service.sync_wechat(), 1)
            self.assertEqual(service.db.execute('SELECT id FROM messages').fetchone()[0], mid)
            self.assertEqual(self.call('read_relay', after_seq=1)['messages'], [])
        finally: service.close()


if __name__ == '__main__': unittest.main()
