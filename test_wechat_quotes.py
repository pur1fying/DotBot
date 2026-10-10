import tempfile,unittest,json
from pathlib import Path
from ilink_adapter import Binding,TextAdapter
from media_adapter import MediaAdapter
from wechat_quotes import read,prompt

class QuoteTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'messages.sqlite'
        self.a=MediaAdapter(self.path,Binding('bot','owner','SYNTHETIC'),images_only=True,
            test_exchange=lambda *args:{'ret':0,'message_id':'9007199254740993'},clock=lambda:10000)
    def tearDown(self):self.a.close();self.tmp.cleanup()
    def add(self,mid,text,quote=None):
        item={'type':1,'text_item':{'text':text}}
        if quote is not None:item['ref_msg']=quote
        msg={'message_id':mid,'from_user_id':'owner','to_user_id':'bot','message_type':1,
             'message_state':2,'context_token':'SYNTHETIC','item_list':[item]}
        self.assertEqual(self.a.ingest({'msgs':[msg]})['accepted'],1)
        return self.a.read_page()['messages'][-1]['message_id']
    def test_inline_quote_preserves_large_server_id_and_separates_current_text(self):
        mid=self.add(1,'这个可以吗',{'svr_id':'9007199254740993','title':'摘要','message_item':{
            'type':1,'text_item':{'text':'引用的完整文字'},'ref_msg':{'secret':'not-exported'}}})
        q=read(self.a.db,mid)
        self.assertEqual(q[0]['server_id'],'9007199254740993')
        self.assertEqual(q[0]['text'],'引用的完整文字');self.assertEqual(q[0]['content_source'],'inline')
        self.assertEqual(self.a.read_page()['messages'][0]['text'],'这个可以吗')
        self.assertNotIn('not-exported',json.dumps(q))
        self.assertIn('引用的完整文字',prompt('这个可以吗',q))
    def test_id_only_quote_resolves_inbound_and_new_outbound_messages(self):
        original=self.add(1,'原消息内容')
        reply=self.add(2,'回复原消息',{'svr_id':'1'})
        self.assertEqual(read(self.a.db,reply)[0]['referenced_local_id'],original)
        self.assertEqual(read(self.a.db,reply)[0]['text'],'原消息内容')
        self.a.reply(original,'机器人答复','reply-key')
        quoted=self.add(3,'追问',{'svr_id':'9007199254740993'})
        q=read(self.a.db,quoted)[0]
        self.assertEqual(q['direction'],'outbound');self.assertEqual(q['text'],'机器人答复')
    def test_old_inbound_mapping_can_be_resolved_without_stored_raw_id(self):
        mid=self.add(1,'历史文字');self.a.db.execute('DELETE FROM wechat_server_messages');self.a.db.commit()
        quoted=self.add(2,'追问',{'svr_id':'1'})
        self.assertEqual(read(self.a.db,quoted)[0]['referenced_local_id'],mid)
    def test_unknown_id_and_media_quote_never_export_keys_or_claim_pixels(self):
        mid=self.add(1,'看这个',{'svr_id':'999','message_item':{'type':4,'file_item':{
            'file_name':'photo.png','media':{'aes_key':'SECRET','full_url':'SECRET_URL'}}}})
        q=read(self.a.db,mid)[0]
        self.assertEqual(q['content_source'],'unavailable');self.assertEqual(q['file_name'],'photo.png')
        self.assertNotIn('SECRET',json.dumps(q));self.assertNotIn('referenced_local_id',q)
    def test_quote_capture_survives_reopen_and_is_bounded(self):
        mid=self.add(1,'正文',{'svr_id':'10','title':'中'*10000,'message_item':{'type':1,'text_item':{'text':'a'*10000}}})
        self.a.close();self.a=TextAdapter(self.path,Binding('bot','owner','SYNTHETIC'))
        q=read(self.a.db,mid);self.assertLessEqual(len(q[0]['title'].encode()),2000)
        self.assertLessEqual(len(prompt('正文',q).encode()),18000)

    def test_opaque_reference_id_is_preserved_even_without_a_cache_match(self):
        mid=self.add(1,'正文',{'svr_id':'opaque-server-reference'})
        q=read(self.a.db,mid)[0]
        self.assertEqual(q['server_id'],'opaque-server-reference')
        self.assertEqual(q['content_source'],'unavailable')

    def test_embedded_item_id_fallback_resolves_historical_message(self):
        original=self.add(7514628646295365128,'将这张图原图发给我')
        self.a.db.execute('DELETE FROM wechat_server_messages');self.a.db.commit()
        mid=self.add(2,'我引用了一句话',{'message_item':{'msg_id':'7514628646295365128'}})
        q=read(self.a.db,mid)[0]
        self.assertEqual(q['server_id'],'7514628646295365128')
        self.assertEqual(q['id_source'],'ref_msg.message_item.msg_id')
        self.assertEqual(q['referenced_local_id'],original)
        self.assertEqual(q['text'],'将这张图原图发给我')

if __name__=='__main__':unittest.main()
