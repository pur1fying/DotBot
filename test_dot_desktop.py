import unittest
from dot_auth import DotError
from dot_desktop import DesktopRoomClient, select_target


def target(**changes):
    return dict({'id': 'main', 'type': 'page', 'url': 'app://-/index.html',
                 'webSocketDebuggerUrl': 'ws://127.0.0.1:9229/devtools/page/main'}, **changes)


class DesktopTargetTest(unittest.TestCase):
    def test_ignores_webpages_and_refuses_remote_or_ambiguous_target(self):
        self.assertEqual(select_target([target(url='https://chatgpt.com/'), target()], 9229)['id'], 'main')
        for records in ([target(), target(id='second')],
                        [target(webSocketDebuggerUrl='ws://192.168.1.2:9229/devtools/page/main')],
                        [target(webSocketDebuggerUrl='ws://127.0.0.1:9230/devtools/page/main')],
                        [target(url='app://evil/index.html')]):
            with self.assertRaises(DotError):
                select_target(records, 9229)
        self.assertEqual(select_target([target(), target(id='second')], 9229, 'second')['id'], 'second')


class DesktopDispatchTest(unittest.IsolatedAsyncioTestCase):
    async def test_send_timeout_is_unknown_and_never_retried(self):
        client = DesktopRoomClient()
        client.object_id = 'bridge'
        calls = []
        async def fail(method, params, **kwargs):
            calls.append(params)
            raise DotError('desktop_request_timeout')
        client._request = fail
        with self.assertRaisesRegex(DotError, 'dot_submission_unknown'):
            await client.invoke('send', {'text': 'test', 'requestId': 'same-persisted-id'})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['arguments'][1]['value']['requestId'], 'same-persisted-id')

    async def test_native_rejection_stays_rejected(self):
        client = DesktopRoomClient()
        client.object_id = 'bridge'
        async def reject(*args, **kwargs):
            return {'result': {'value': {'ok': False, 'error': 'desktop_room_request_failed', 'http_status': 403}}}
        client._request = reject
        with self.assertRaises(DotError) as caught:
            await client.invoke('send', {'text': 'test'})
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(caught.exception.category, 'desktop_room_request_failed')


if __name__ == '__main__':
    unittest.main()
