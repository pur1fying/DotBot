import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dot_auth import DotError
from ilink_adapter import Binding
from ilink_broker import Broker
from service import PersistentService, OfflineDelivery, FIXTURE_SECRET


class DotChannelTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name); self.path.chmod(0o700)
        self.batch, self.sent = [], []
        def exchange(path, headers, body):
            if path.endswith('getupdates'):
                rows, self.batch = self.batch, []
                return {'ret': 0, 'msgs': rows}
            self.sent.append(body)
            return {'ret': 0}
        self.broker = Broker(self.path, Binding('bot', 'owner', 'SYNTHETIC'), exchange, clock=lambda: 10000)

    def call(self, method, **args):
        return self.broker.call({'method': method, 'arguments': args})

    def bind(self):
        self.call('dot_bind', binding={'account_id': 'account', 'user_id': 'user',
            'dot_id': 'alice', 'thread_id': 'thread', 'name': 'Alice', 'protocol_verified': True})
        self.call('dot_activate')

    def add(self, mid):
        self.batch.append({'message_id': mid, 'from_user_id': 'owner', 'to_user_id': 'bot',
            'message_type': 1, 'message_state': 2, 'context_token': 'SYNTHETIC-CONTEXT',
            'item_list': [{'type': 1, 'text_item': {'text': str(mid)}}]})
        self.broker.poll_once()

    def tearDown(self): self.tmp.cleanup()

    def test_wait_wakes_after_commit_without_blocking_regular_read(self):
        waiting = threading.Event()
        original = self.broker.changed.wait
        def wait(timeout):
            waiting.set()
            return original(timeout)
        self.broker.changed.wait = wait
        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(self.call, 'wait_inbox', after_seq=0, timeout_ms=2000)
            self.assertTrue(waiting.wait(1))
            read = pool.submit(self.call, 'read')
            self.assertEqual(read.result(timeout=1)['messages'], [])
            self.add(1)
            self.assertEqual(pending.result(timeout=1)['local_seq'], 1)

    def test_complete_item_drains_once_and_never_exposes_context(self):
        self.bind(); self.add(1)
        job = self.call('dot_next')
        self.assertNotIn('SYNTHETIC-CONTEXT', str(job))
        self.call('dot_submitting', message_id=job['message_id'])
        item = {'id': 'item', 'type': 'agentMessage', 'delivery': 'async', 'text': '完整答复',
                'messageMetadata': {'aeon': {'version': 1, 'threadId': job['reply_chain_id']}}}
        self.call('dot_item', thread_id='thread', item=item, completed=True)
        self.assertEqual(self.call('dot_drain_one')['state'], 'api_accepted')
        self.call('dot_item', thread_id='thread', item=item, completed=True)
        self.assertIsNone(self.call('dot_drain_one'))
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.call('read_pending')['messages'], [])

    def test_relay_advances_over_entire_direct_page_then_delivers_webhook(self):
        self.bind()
        for mid in range(1, 21):
            self.add(mid)
            job = self.call('dot_next')
            self.call('dot_submitting', message_id=job['message_id'])
            self.call('dot_item', thread_id='thread', completed=True, item={
                'id': 'item-'+str(mid), 'type': 'agentMessage', 'delivery': 'async', 'text': 'ok',
                'messageMetadata': {'aeon': {'version': 1, 'threadId': job['reply_chain_id']}}})
            self.call('dot_drain_one')
        self.call('dot_rollback'); self.add(21)
        delivery = OfflineDelivery(now=lambda: 10000)
        service = PersistentService(self.path/'events.sqlite', delivery, now=lambda: 10000,
            wechat=lambda method, args: self.broker.call({'method': method, 'arguments': args}))
        try:
            service.dispatch('events/subscribe', {'name': 'wechat.message.created',
                'arguments': {'channel_id': 'wechat-owner'},
                'delivery': {'mode': 'webhook', 'url': 'https://mock.invalid/callback', 'secret': FIXTURE_SECRET}})
            self.assertEqual(service.sync_wechat(), 0)
            self.assertEqual(service.db.execute('SELECT seq FROM wechat_cursor').fetchone()[0], 20)
            self.assertEqual(service.sync_wechat(), 1)
            self.assertEqual(service.db.execute('SELECT count(*) FROM messages').fetchone()[0], 1)
        finally: service.close()


if __name__ == '__main__': unittest.main()
