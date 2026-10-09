import json
from pathlib import Path
import re
import tempfile
import unittest
from service import PersistentService, process, SERVER_INFO


class DiscoveryTests(unittest.TestCase):
    def test_required_cache_fields_and_unchanged_mock_entries(self):
        with tempfile.TemporaryDirectory() as d:
            old = PersistentService(Path(d)/'old.sqlite')
            new = PersistentService(Path(d)/'new.sqlite', wechat=lambda *a: {})
            try:
                meta = {'io.modelcontextprotocol/protocolVersion':'2026-07-28', 'io.modelcontextprotocol/clientCapabilities':{}}
                results = {}
                for label, service in [('original', old), ('wechat', new)]:
                    results[label] = {}
                    for m in ('server/discover','tools/list','events/list'):
                        result = process(service, {'jsonrpc':'2.0','id':1,'method':m,'params':{'_meta':meta}})['result']
                        self.assertEqual(result['resultType'], 'complete')
                        self.assertEqual(result['cacheScope'], 'private')
                        self.assertIs(type(result['ttlMs']), int)
                        self.assertEqual(result['_meta']['io.modelcontextprotocol/serverInfo'], SERVER_INFO)
                        results[label][m] = result
                self.assertEqual(results['original']['tools/list']['tools'], results['wechat']['tools/list']['tools'][:4])
                self.assertEqual(results['original']['events/list']['events'], results['wechat']['events/list']['events'][:1])
                names = [t['name'] for t in results['wechat']['tools/list']['tools']]
                self.assertEqual(len(names), len(set(names)))
                self.assertTrue(all(re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', n) for n in names))
                self.assertTrue(all(len('mcp__codex_apps__dot_'+n)<64 for n in names))
                for t in results['wechat']['tools/list']['tools'][4:]:
                    self.assertIn('title',t)
                    self.assertEqual(t['inputSchema']['type'],'object')
                    self.assertIn('outputSchema',t)
                    self.assertTrue(all(type(v) is bool for v in t['annotations'].values()))
            finally:
                old.close(); new.close()

    def test_prevalidation_failure_is_diagnosed_without_request_data(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'state.sqlite'
            service = PersistentService(path)
            try:
                result = process(service, {'jsonrpc':'2.0','id':1,'method':'initialize','params':{'SYNTHETIC-SECRET':'must-not-log'}})
                self.assertEqual(result['error']['code'], -32602)
                text = Path(str(path)+'.discovery.jsonl').read_text()
                self.assertNotIn('SYNTHETIC-SECRET',text)
                self.assertNotIn('must-not-log',text)
                self.assertEqual(json.loads(text)['stage'],'metadata_missing')
            finally:
                service.close()


if __name__ == '__main__':
    unittest.main()
