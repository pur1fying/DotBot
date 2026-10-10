"""Image direct routing, native attachment correlation and durable send behavior."""
import base64,json,tempfile,unittest
from pathlib import Path
import jsonschema
from dot_auth import DotError
from dot_runner import Gateway
from ilink_adapter import Binding
from ilink_broker import Broker
from service import PersistentService,OfflineDelivery
from test_dot_room_gateway import BINDING
from test_media import FakeCDN,image_item,voice_item,png

class ImageGatewayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name);self.path.chmod(0o700)
        self.now=10000;self.sent=[];self.cdn=FakeCDN();self.send_failure=False
        def exchange(path,headers,body):
            if path.endswith('getuploadurl'):return {'ret':0,'upload_param':'SYNTHETIC-PRIVATE-UPLOAD'}
            self.sent.append(body)
            if self.send_failure:raise OSError('private error')
            return {'ret':0}
        self.b=Broker(self.path,Binding('SYNTHETIC-BOT','SYNTHETIC-OWNER','SYNTHETIC'),exchange,
            clock=lambda:self.now,images_enabled=True,media_transport=self.cdn)
        self.call('dot_bind',binding=BINDING);self.call('dot_activate')
    def tearDown(self):self.tmp.cleanup()
    def call(self,method,**args):return self.b.call({'method':method,'arguments':args})
    async def rpc(self,method,args=None):return self.call(method,**(args or {}))
    def ingest(self,items=None,mid=1):
        a=self.b.adapter()
        try:return a.ingest({'msgs':[{'message_id':mid,'from_user_id':'SYNTHETIC-OWNER',
            'to_user_id':'SYNTHETIC-BOT','message_type':1,'message_state':2,'context_token':'SYNTHETIC-CONTEXT',
            'item_list':items or [image_item()]}]})
        finally:a.close()
    def own(self,job):return {'id':'input','roomId':'room','role':'user','self':True,
        'requestId':job['client_message_id'],'replyToMessageId':'anchor','replyRootMessageId':'anchor','text':'input'}
    def reply(self,**changes):return dict({'id':'output','roomId':'room','role':'user','self':False,
        'senderAeonId':'alice','replyToMessageId':'input','replyRootMessageId':'anchor','text':'',
        'images':[{'attachment_id':'image1','file_id':'file1','mime_type':'image/png','loading':False}]},**changes)
    def admitted(self):
        self.ingest();job=self.call('dot_next');self.call('dot_submitting',message_id=job['message_id'])
        self.call('dot_room_message',message=self.own(job));return job
    async def test_image_only_mode_keeps_public_schema_and_rejects_voice(self):
        self.assertEqual(self.ingest()['accepted'],1);self.assertEqual(self.ingest()['duplicates'],1)
        self.assertEqual(self.ingest([voice_item()],mid=2)['rejected'],1)
        self.assertEqual(self.call('read_pending')['messages'],[]);self.assertEqual(self.call('read_relay')['messages'],[])
        self.assertEqual(len(self.call('dot_next')['images']),1)
        service=PersistentService(self.path/'mcp.sqlite',OfflineDelivery(),wechat=lambda m,a:self.call(m,**a))
        try:
            tools=service.dispatch('tools/list',{})['tools'];self.assertEqual(len(tools),8)
            for name,method in [('wechat_read_messages','read'),('wechat_read_status','status')]:
                schema=next(t['outputSchema'] for t in tools if t['name']==name)
                jsonschema.validate(self.call(method),schema)
        finally:service.close()
    async def test_uploaded_file_is_reused_after_proven_unsent_message(self):
        self.ingest();uploads=[];sends=[];fixture=self
        class Room:
            async def invoke(self,op,args=None):
                if op=='status':return {'subscribed':True}
                if op=='upload_image':uploads.append(args);return {'file_id':'native-file'}
                if op=='send':
                    sends.append(args)
                    if len(sends)==1:
                        e=DotError('desktop_room_request_failed');e.submission_started=False;raise e
                    return fixture.own(fixture.call('dot_next'))
        g=Gateway(self.rpc,Room(),BINDING)
        with self.assertRaises(DotError):await g.submit()
        self.call('dot_recover');await Gateway(self.rpc,Room(),BINDING).submit()
        self.assertEqual(len(uploads),1);self.assertEqual(len(sends),2)
        self.assertEqual(sends[0]['requestId'],sends[1]['requestId']);self.assertEqual(sends[1]['attachmentIds'],['native-file'])
        self.assertEqual(base64.b64decode(uploads[0]['data_base64']),png())
    async def test_uncertain_upload_never_retries_or_submits(self):
        self.ingest();attempts=[]
        class Room:
            async def invoke(self,op,args=None):
                if op=='status':return {'subscribed':True}
                attempts.append(op);raise DotError('dot_image_upload_unknown')
        with self.assertRaises(DotError):await Gateway(self.rpc,Room(),BINDING).submit()
        self.call('dot_recover');await Gateway(self.rpc,Room(),BINDING).submit()
        self.assertEqual(attempts,['upload_image']);self.assertEqual(self.call('dot_next')['state'],'requires_user_action')
    async def test_crash_during_upload_is_unknown_after_recovery(self):
        self.ingest();job=self.call('dot_next');self.call('dot_input_image_begin',message_id=job['message_id'],media_id=job['images'][0]['media_id'])
        self.call('dot_recover');self.assertEqual(self.call('dot_next')['images'][0]['state'],'unknown')
        self.assertEqual(self.call('dot_next')['state'],'requires_user_action')
    async def test_image_only_reply_is_parent_bound_and_sent_exactly_once(self):
        self.admitted();reply=self.reply();self.call('dot_room_message',message=reply);calls=[]
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args));return {'data_base64':base64.b64encode(png()).decode()}
        g=Gateway(self.rpc,Room(),BINDING);await g.drain()
        self.assertEqual(calls[0][1],{'message_id':'output','parent_id':'input','attachment_id':'image1'})
        self.assertEqual(self.sent[0]['msg']['item_list'][0]['type'],2)
        self.call('dot_room_message',message=reply);self.call('dot_recover');await g.drain()
        self.assertEqual(len(self.sent),1);self.assertEqual(len(calls),1)
        self.assertEqual(self.call('dot_status')['jobs'],{'api_accepted':1})
    async def test_late_image_after_text_and_loading_update_is_not_lost(self):
        self.admitted();self.call('dot_room_message',message=self.reply(images=[],text='准备回图'))
        self.call('dot_drain_one');self.assertEqual(len(self.sent),1)
        r=self.reply(text='准备回图');r['images'][0]['loading']=True
        self.call('dot_room_message',message=r);attempts=[]
        class Room:
            async def invoke(self,op,args=None):
                attempts.append(op)
                if len(attempts)==1:raise DotError('dot_image_not_ready')
                return {'data_base64':base64.b64encode(png()).decode()}
        g=Gateway(self.rpc,Room(),BINDING);await g.drain();self.assertEqual(len(self.sent),1)
        r['images'][0]['loading']=False;self.call('dot_room_message',message=r);await g.drain()
        self.assertEqual([x['msg']['item_list'][0]['type'] for x in self.sent],[1,2])
        self.call('dot_room_message',message=r);await g.drain();self.assertEqual(len(self.sent),2)
    async def test_unknown_wechat_image_send_is_not_repeated(self):
        self.admitted();self.call('dot_room_message',message=self.reply());self.send_failure=True
        class Room:
            async def invoke(self,op,args=None):return {'data_base64':base64.b64encode(png()).decode()}
        g=Gateway(self.rpc,Room(),BINDING);await g.drain();self.call('dot_recover');await g.drain()
        self.assertEqual(len(self.sent),1);self.assertEqual(self.call('dot_status')['jobs'],{'send_unknown':1})
        self.assertEqual(len([r for r in self.cdn.calls if r[0]=='POST']),1)
    async def test_foreign_image_reply_does_not_enter_outbox(self):
        self.admitted();self.assertFalse(self.call('dot_room_message',message=self.reply(senderAeonId='foreign'))['accepted'])
        self.assertIsNone(self.call('dot_output_next'));self.assertEqual(self.sent,[])

if __name__=='__main__':unittest.main()
