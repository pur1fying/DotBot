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
from test_media import FakeCDN,image_item,voice_item,png,KEY

def image_file_item(name='original.png', length=None):
    field = image_item()['image_item']
    # Match test_media's fixed key; file keys use base64(hex) on the wire.
    field['media']['aes_key'] = base64.b64encode(KEY.hex().encode()).decode()
    return {'type':4,'file_item':{'file_name':name,'len':str(len(png()) if length is None else length),'media':field['media']}}

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

    async def test_image_file_bytes_and_receipt_are_preserved(self):
        self.assertEqual(self.ingest([image_file_item()])['accepted'],1)
        job=self.call('dot_next');aid=job['images'][0]['media_id']
        image=self.call('dot_read_image',message_id=job['message_id'],media_id=aid)
        self.assertEqual(base64.b64decode(image['content'][0]['data']),png())
        info=self.call('dot_image_file_info',message_id=job['message_id'],media_id=aid)
        self.assertEqual(info,{'source_type':4,'file_name':'original.png','declared_mime':'image/png',
            'declared_size_bytes':len(png()),'downloaded_size_bytes':len(png())})
        self.assertEqual(self.ingest([image_file_item()])['duplicates'],1)

    async def test_image_file_size_and_type_mismatch_are_rejected_before_upload(self):
        from wechat_media import MediaError
        for name,size,category in [('original.jpg',len(png()),'mime_content_mismatch'),
                                   ('original.png',len(png())+1,'file_size_mismatch')]:
            self.ingest([image_file_item(name,size)],mid=10+size)
            a=self.b.adapter()
            try:
                job=a.dot.job(a.read_page()['messages'][-1]['message_id']);aid=job['images'][0]['media_id']
                with self.assertRaisesRegex(MediaError,category):a.read_media(job['message_id'],aid)
                self.assertIsNone(a.db.execute('SELECT blob FROM media WHERE id=?',(aid,)).fetchone()[0])
            finally:a.close()
        for name in ['archive.zip','document.pdf','voice.wav','vector.svg']:
            self.assertEqual(self.ingest([image_file_item(name)],mid=100)['rejected'],1)

    async def test_recovery_only_ingests_latest_owner_file_and_never_rewinds_cursor(self):
        self.ingest([{'type':1,'text_item':{'text':'anchor'}}]);self.now+=3
        def message(mid,at,owner='SYNTHETIC-OWNER'):
            return {'message_id':mid,'from_user_id':owner,'to_user_id':'SYNTHETIC-BOT','message_type':1,
                'message_state':2,'create_time_ms':at,'context_token':'SYNTHETIC-CONTEXT','item_list':[image_file_item()]}
        batch=[message(2,1000),message(3,10002000,'other'),message(4,10001000),message(5,10002000)]
        self.b.exchange=lambda *args:{'ret':0,'msgs':batch,'get_updates_buf':'must-not-replace-live-cursor'}
        a=self.b.adapter()
        with a.db:a._set('cursor','live-cursor')
        a.close()
        args={'checkpoint_cursor':'old-checkpoint','after_seq':1,'commit':False}
        preview=self.call('dot_recover_image_file',**args)
        self.assertTrue(preview['found']);self.assertFalse(preview['recovered'])
        self.assertEqual(len(self.call('read')['messages']),1)
        args['commit']=True;recovered=self.call('dot_recover_image_file',**args)
        self.assertEqual(preview['message_id'],recovered['message_id']);self.assertTrue(recovered['recovered'])
        self.assertEqual(len(self.call('read')['messages']),2)
        a=self.b.adapter()
        try:self.assertEqual(a._get('cursor'),'live-cursor')
        finally:a.close()
        again=self.call('dot_recover_image_file',**args)
        self.assertTrue(again['already_ingested']);self.assertFalse(again['recovered'])
        self.assertEqual(len(self.call('read')['messages']),2)
        self.assertEqual(self.call('dot_image_file_info',message_id=recovered['message_id'],
            media_id=self.call('dot_reconcile_jobs')['jobs'][-1]['images'][0]['media_id'])['source_type'],4)


    def saved_image_source(self):
        self.ingest();job=self.call('dot_next');aid=job['images'][0]['media_id']
        self.call('dot_input_image_begin',message_id=job['message_id'],media_id=aid)
        self.call('dot_input_image_done',message_id=job['message_id'],media_id=aid,file_id='CalpicoFile_original')
        self.call('dot_submitting',message_id=job['message_id'])
        self.call('dot_room_message',message=self.own(job))
        self.call('dot_room_message',message=self.reply(images=[],text='收到图片'))
        self.call('dot_drain_one')
        return job['message_id']

    def add_image_quote(self,reference='1',with_image=False,repeat=False):
        item={'type':1,'text_item':{'text':'引用的图片是什么'},'ref_msg':{'message_item':{'msg_id':reference}}}
        items=([image_item()] if with_image else [])+[item]
        if repeat:items.append(item)
        self.ingest(items,mid=2)

    async def test_expired_inbound_image_reuses_native_file_without_uploading_again(self):
        self.saved_image_source();self.now+=86401
        a=self.b.adapter();a.prune();a.close()
        self.add_image_quote(repeat=True);calls=[];fixture=self
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args))
                if op=='status':return {'subscribed':True}
                if op=='check_image_file':return {'available':True,'file_id':args['file_id']}
                if op=='send':return dict(fixture.own(fixture.call('dot_next')),id='quoted-input',attachmentCount=1)
                raise AssertionError(op)
        await Gateway(self.rpc,Room(),BINDING).submit()
        sent=next(args for op,args in calls if op=='send')
        self.assertEqual(sent['attachmentIds'],['CalpicoFile_original'])
        self.assertEqual([op for op,_ in calls].count('check_image_file'),1)
        self.assertIn('"status": "attached"',sent['text']);self.assertIn('"attachment_number": 1',sent['text'])
        self.assertNotIn('[媒体消息，请读取附件]',sent['text'])
        self.assertNotIn('CalpicoFile_original',sent['text'])
        self.assertNotIn('upload_image',[op for op,_ in calls])

    async def test_current_image_and_quoted_image_have_distinct_attachment_numbers(self):
        self.saved_image_source();self.add_image_quote(with_image=True);calls=[];fixture=self
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args))
                if op=='status':return {'subscribed':True}
                if op=='upload_image':return {'file_id':'CalpicoFile_current'}
                if op=='check_image_file':return {'available':True}
                if op=='send':return dict(fixture.own(fixture.call('dot_next')),id='quoted-input',attachmentCount=2)
        await Gateway(self.rpc,Room(),BINDING).submit()
        sent=next(args for op,args in calls if op=='send')
        self.assertEqual(sent['attachmentIds'],['CalpicoFile_current','CalpicoFile_original'])
        self.assertIn('"attachment_number": 2',sent['text'])

    async def test_forgotten_quote_image_is_not_read_or_reattached(self):
        original=self.saved_image_source();a=self.b.adapter();a.forget_media(original);a.close()
        self.add_image_quote();calls=[];fixture=self
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args))
                if op=='status':return {'subscribed':True}
                if op=='send':return dict(fixture.own(fixture.call('dot_next')),id='quoted-input',attachmentCount=0)
                raise AssertionError(op)
        await Gateway(self.rpc,Room(),BINDING).submit()
        sent=next(args for op,args in calls if op=='send')
        self.assertNotIn('attachmentIds',sent);self.assertIn('"reason": "forgotten"',sent['text'])

    async def test_disabling_images_disables_quote_attachment_reuse(self):
        self.saved_image_source();self.b.images_enabled=False;self.add_image_quote()
        self.assertEqual(self.call('dot_next')['quoted_images'],[])

    async def test_missing_native_quote_file_is_reported_without_blocking_current_text(self):
        self.saved_image_source();self.add_image_quote();calls=[];fixture=self
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args))
                if op=='status':return {'subscribed':True}
                if op=='check_image_file':raise DotError('desktop_room_request_failed',status=404)
                if op=='send':return dict(fixture.own(fixture.call('dot_next')),id='quoted-input',attachmentCount=0)
        await Gateway(self.rpc,Room(),BINDING).submit()
        sent=next(args for op,args in calls if op=='send')
        self.assertNotIn('attachmentIds',sent);self.assertIn('native_image_unavailable',sent['text'])
        self.assertNotIn('"status": "attached"',sent['text'])

    async def test_forget_during_quote_check_invalidates_the_prepared_send(self):
        original=self.saved_image_source();self.add_image_quote();calls=[];fixture=self
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args))
                if op=='status':return {'subscribed':True}
                if op=='check_image_file':
                    a=fixture.b.adapter();a.forget_media(original);a.close()
                    return {'available':True}
                raise AssertionError('send must not occur after forget')
        await Gateway(self.rpc,Room(),BINDING).submit()
        self.assertEqual(self.call('dot_next')['state'],'queued')
        self.assertNotIn('send',[op for op,_ in calls])

    async def test_quote_of_sent_image_reuses_its_bound_native_attachment(self):
        exchange=self.b.exchange
        self.b.exchange=lambda *args:{**exchange(*args),'message_id':'900'}
        self.admitted();self.call('dot_room_message',message=self.reply());fixture=self;calls=[]
        class Room:
            async def invoke(self,op,args=None):
                calls.append((op,args))
                if op=='status':return {'subscribed':True}
                if op=='download_image':return {'data_base64':base64.b64encode(png()).decode(),
                    'source_type':'file','source_file_id':'CalpicoFile_reply'}
                if op=='send':return dict(fixture.own(fixture.call('dot_next')),id='quoted-input',attachmentCount=1)
                raise AssertionError(op)
        gateway=Gateway(self.rpc,Room(),BINDING);await gateway.drain()
        self.add_image_quote(reference='900');await gateway.submit()
        sent=next(args for op,args in calls if op=='send')
        self.assertEqual(sent['attachmentIds'],['CalpicoFile_reply'])
        self.assertIn('"direction": "outbound"',sent['text'])
        self.assertEqual([args for op,args in calls if op=='download_image'][-1],
                         {'message_id':'output','parent_id':'input','attachment_id':'image1'})

if __name__=='__main__':unittest.main()
