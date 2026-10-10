import test from 'node:test';
import assert from 'node:assert/strict';
import {createDesktopRoomBridge} from './desktop_room_bridge.mjs';

const binding = {account_id: 'account', user_id: 'user', dot_id: 'dot',
  thread_id: 'thread', messaging_room_id: 'room', reply_anchor_message_id: 'anchor'};
const requestId = '00000000-0000-4000-8000-000000000001';
function fixture(options = {}) {
  const state = {ready: true, account: 'account', user: 'user',
    structure: {status: 'loaded', structure: 'personal'}};
  let watch, observer, configuration, stopped = 0;
  const sends = [], uploads = [], downloads = [];
  const msg = {id: 'message', roomId: 'room', role: 'user', self: true,
    requestId, text: '完整文字', attachments: [{url: 'private-signed-media'}],
    messageMetadata: {secret: 'not-exported'}};
  if (options.imageReply) Object.assign(msg, {self: false, senderAeonId: 'dot',
    replyRootMessageId: 'anchor', attachments: [{type: 'file', attachmentId: 'image',
      fileId: 'file', mime: 'image/png', url: 'private-signed-media'}]});
  const scope = {get: key => state[key], watch: fn => { watch = fn; return () => { stopped++; }; }};
  const shared = {Agt() {}, NOt: 'ready', AOt: 'account', FOt: 'user', kOt: 'structure', MOt: 'accountUser',
    ZKt: {scope}, kgt: () => ({subscribe(handler) {observer = handler; return () => {stopped++;};}})};
  const native = {wN() {}, CN(config) {
    configuration = config;
    return {client: {
      async getRoom() { config.onRoomSnapshot({id: 'room', type: 'DM', app_source: 'chatgpt:messaging',
        aeon_id: options.otherDot ? 'other' : 'dot', members: [{aeon_id: 'dot'}]}); },
      async messages() { return {messages: [msg], before: 'before', after: null}; },
      async send(args) { sends.push(args); config.beforeSend(); if (options.unknown)
        throw Object.assign(new Error('must not be exposed'), {deliveryUnknown: true});
        return {...msg, ...args}; }
    }, media: {
      async upload(roomId, file) { uploads.push({roomId, name: file.name, mime: file.type});
        return {fileId: 'uploaded-file', mime: file.type, status: 'UPLOADED'}; },
      async image(...args) { downloads.push(args);
        return {image: new Blob([Buffer.from('89504e470d0a1a0a', 'hex')], {type: 'image/png'})}; }
    }, subscribeSendIntegrity() { return () => {stopped++;}; }};
  }};
  return {shared, native, state, sends, uploads, downloads, get stopped() {return stopped;},
    notify: data => observer.onUpdate(data), receive: (id, data) => observer.onMessageReceived(id, data),
    changeAccount() {state.account = 'another'; watch();}, get configuration() {return configuration;}};
}

test('uses native send with stable ID and exports only projected correlation fields', async () => {
  const f = fixture(); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  const sent = await bridge.invoke('send', {requestId, text: '微信完整文字'});
  assert.equal(sent.ok, true);
  assert.deepEqual(f.sends, [{requestId, text: '微信完整文字', roomId: 'room'}]);
  assert.equal(f.configuration.conversationId, 'thread');
  assert.equal(sent.value.text, '微信完整文字');
  assert.equal(JSON.stringify(sent).includes('private-signed-media'), false);
  assert.equal(JSON.stringify(sent).includes('not-exported'), false);
  assert.equal((await bridge.invoke('status')).value.protocol_verified, false);
  bridge.dispose();
});
test('different account or dot is rejected before any send', async () => {
  const f = fixture(); f.state.account = 'another';
  await assert.rejects(createDesktopRoomBridge(f.shared, f.native, binding), /auth_account_changed/);
  const g = fixture({otherDot: true});
  await assert.rejects(createDesktopRoomBridge(g.shared, g.native, binding), /dot_room_identity_mismatch/);
  assert.equal(g.sends.length, 0); assert.ok(g.stopped > 0);
});
test('account change aborts subscriptions and refuses further submissions', async () => {
  const f = fixture(); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  f.changeAccount();
  assert.equal(f.configuration.signal.aborted, true);
  assert.equal((await bridge.invoke('send', {requestId, text: 'text'})).error, 'auth_account_changed');
  assert.equal(f.sends.length, 0); assert.equal(f.stopped, 3);
});
test('uncertain native delivery never retries or leaks native error text', async () => {
  const f = fixture({unknown: true}); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  const response = await bridge.invoke('send', {requestId, text: 'test'});
  assert.equal(response.error, 'dot_submission_unknown');
  assert.equal(response.delivery_unknown, true); assert.equal(f.sends.length, 1);
  assert.equal(JSON.stringify(response).includes('must not'), false); bridge.dispose();
});
test('only the pinned room wakes the receiver; overflow requires history resync', async () => {
  const f = fixture(); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  f.notify({type: 'message', roomId: 'other', messageId: 'foreign'});
  f.receive('other', {id: 'foreign'});
  f.receive('room', {id: 'reply', request_id: requestId, reply_to: {message_id: 'parent'}});
  let events = (await bridge.invoke('wait')).value;
  assert.deepEqual(events, [{type: 'message-added', messageId: 'reply', requestId, replyToMessageId: 'parent'}]);
  for (let i = 0; i < 300; i++) f.notify({type: 'message', roomId: 'room', messageId: String(i)});
  events = (await bridge.invoke('wait')).value;
  assert.equal(events[0].type, 'resync'); assert.ok(events.length <= 256); bridge.dispose();
});
test('cannot use the bridge as generic HTTP or room creation', async () => {
  const f = fixture(); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  assert.equal((await bridge.invoke('httpFetch')).error, 'desktop_operation_not_allowed');
  assert.equal((await bridge.invoke('send', {requestId, text: 'x', roomId: 'other'})).error, 'dot_invalid_submission');
  assert.equal((await bridge.invoke('messages', {limit: 101})).error, 'dot_invalid_message_query');
  assert.equal(f.sends.length, 0); bridge.dispose();
});
test('image upload uses the native media client in the pinned room and rejects other file kinds', async () => {
  const f = fixture(); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  const png = Buffer.from('89504e470d0a1a0a', 'hex').toString('base64');
  const uploaded = await bridge.invoke('upload_image', {data_base64: png, mime_type: 'image/png'});
  assert.equal(uploaded.value.file_id, 'uploaded-file');
  assert.deepEqual(f.uploads, [{roomId: 'room', name: 'wechat-image.png', mime: 'image/png'}]);
  for (const args of [{data_base64: png, mime_type: 'audio/wav'},
    {data_base64: Buffer.from('private file').toString('base64'), mime_type: 'image/png'},
    {data_base64: png, mime_type: 'image/png', roomId: 'other'}])
    assert.equal((await bridge.invoke('upload_image', args)).error, 'dot_invalid_image_file');
  assert.equal(f.uploads.length, 1);
  await bridge.invoke('send', {requestId, text: '看图片', attachmentIds: ['uploaded-file']});
  assert.deepEqual(f.sends[0].attachmentIds, ['uploaded-file']); bridge.dispose();
});
test('image download verifies the dot author and opaque attachment; no signed URL is returned', async () => {
  const f = fixture({imageReply: true}); const bridge = await createDesktopRoomBridge(f.shared, f.native, binding);
  const projected = await bridge.invoke('messages', {limit: 20});
  assert.equal(projected.value.messages[0].images[0].attachment_id, 'image');
  assert.equal(JSON.stringify(projected).includes('private-signed-media'), false);
  const result = await bridge.invoke('download_image', {message_id: 'message', attachment_id: 'image', parent_id: 'parent'});
  assert.equal(result.value.mime_type, 'image/png'); assert.equal(f.downloads.length, 1);
  assert.equal((await bridge.invoke('download_image', {message_id: 'foreign', attachment_id: 'image', parent_id: 'parent'})).error,
    'dot_image_message_unavailable');
  assert.equal(f.downloads.length, 1); bridge.dispose();
});
