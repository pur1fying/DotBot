// Runs in the existing Codex renderer. All network, auth, integrity preparation,
// route caching and message projection remain in the installed desktop client.
// dot_desktop.py pins and normalizes the installed bundle exports; unknown builds fail closed.
export async function createDesktopRoomBridge(shared, native, binding) {
  for (const key of ['account_id', 'user_id', 'dot_id', 'thread_id', 'messaging_room_id']) {
    if (typeof binding[key] !== 'string' || !binding[key] || binding[key].length > 256)
      throw new Error('dot_invalid_binding');
  }
  if (typeof native.wN !== 'function' || typeof native.CN !== 'function' ||
      typeof shared.Agt !== 'function' || typeof shared.kgt !== 'function')
    throw new Error('desktop_bundle_changed');
  native.wN();
  shared.Agt();
  const scope = shared.ZKt?.scope;
  if (!scope) throw new Error('desktop_window_not_ready');
  const controller = new AbortController();
  let stopIdentity, stopUpdates, stopIntegrity, waiter, lease, verified = false;
  let subscribed = false, closed = false, sendInFlight = false, sendStarted = false, sendStartedAt;
  const updates = [];
  function sameAccount() {
    return scope.get(shared.NOt) && scope.get(shared.AOt) === binding.account_id &&
      scope.get(shared.FOt) === binding.user_id;
  }
  function assertCurrent() {
    if (!sameAccount()) throw new Error('auth_account_changed');
    if (closed) throw new Error('desktop_bridge_closed');
  }
  function enqueue(update) {
    if (closed) return;
    if (updates.length >= 256) {
      updates.length = 0;
      updates.push({type: 'resync', reason: 'buffer_overflow'});
    }
    updates.push(update);
    if (waiter) { const resolve = waiter; waiter = null; resolve(updates.splice(0)); }
  }
  function dispose() {
    if (closed) return;
    closed = true;
    clearTimeout(lease);
    controller.abort();
    stopUpdates?.(); stopIntegrity?.(); stopIdentity?.();
    if (waiter) { const resolve = waiter; waiter = null; resolve([]); }
  }
  function renewLease() { clearTimeout(lease); lease = setTimeout(dispose, 90000); }
  assertCurrent();
  const structure = scope.get(shared.kOt);
  const accountUserId = structure?.status === 'loaded' && structure.structure === 'personal'
    ? binding.user_id : scope.get(shared.MOt);
  if (structure?.status !== 'loaded' || !accountUserId)
    throw new Error('desktop_account_not_ready');
  renewLease();
  stopIdentity = scope.watch(() => { if (!sameAccount()) dispose(); });
  const roomId = binding.messaging_room_id;
  let room;
  try { room = native.CN({scope, signal: controller.signal,
    accountId: binding.account_id, userId: binding.user_id, accountUserId,
    conversationId: binding.thread_id, beforeSend() { sendStarted = true; sendStartedAt = Date.now() / 1000; },
    onRoomSnapshot(snapshot) {
      if (snapshot.id !== roomId || snapshot.type !== 'DM' ||
          snapshot.app_source !== 'chatgpt:messaging' || snapshot.aeon_id !== binding.dot_id ||
          snapshot.members?.filter(member => member.aeon_id === binding.dot_id).length !== 1)
        throw new Error('dot_room_identity_mismatch');
      verified = true;
    }}); } catch (error) { dispose(); throw error; }
  try {
    await room.client.getRoom(roomId);
    assertCurrent();
    if (!verified) throw new Error('dot_room_identity_mismatch');
    const cache = shared.kgt(scope);
    if (!cache) throw new Error('desktop_room_subscription_unavailable');
    stopUpdates = cache.subscribe({
      onUpdate(update) {
        if (!sameAccount()) { dispose(); return; }
        if (update.type === 'resync' || update.roomId === roomId)
          enqueue({type: update.type, messageId: update.messageId});
      },
      onSubscribedChange(value) { subscribed = value; enqueue({type: 'subscription', connected: value}); },
      onMessageReceived(id, message) {
        if (sameAccount() && id === roomId)
          enqueue({type: 'message-added', messageId: message.id,
            requestId: message.request_id, replyToMessageId: message.reply_to?.message_id});
      }});
    stopIntegrity = room.subscribeSendIntegrity();
  } catch (error) { dispose(); throw error; }
  // Return projected text, opaque image IDs and correlation fields. No tokens, signed media,
  // widgets, browser auth actions, raw message parts or internal tool output.
  function isImage(attachment) {
    return attachment && (['image/png', 'image/jpeg', 'image/webp'].includes(attachment.mime) ||
      attachment.type === 'media' && attachment.hasPreview && attachment.contentType !== 'video');
  }
  function project(message) {
    if (message.roomId !== roomId) throw new Error('dot_room_identity_mismatch');
    const aeon = message.messageMetadata?.aeon;
    const aeonMetadata = aeon && typeof aeon === 'object' ? Object.fromEntries(
      ['version', 'threadId', 'messageId'].filter(key => typeof aeon[key] === 'number' ||
        (typeof aeon[key] === 'string' && aeon[key].length <= 256)).map(key => [key, aeon[key]])) : undefined;
    const elicitation = room.elicitations?.get(message.id);
    const questions = [];
    if (elicitation && typeof elicitation === 'object') {
      const schema = elicitation.requestedSchema ?? elicitation.requested_schema;
      if (schema?.properties && typeof schema.properties === 'object') {
        for (const [key, value] of Object.entries(schema.properties)) {
          if (!value || typeof value !== 'object') continue;
          questions.push({title: typeof value.title === 'string' ? value.title : key,
            options: Array.isArray(value.enum) ? value.enum.filter(option => typeof option === 'string') : []});
        }
      }
      if (!questions.length && typeof elicitation.message === 'string')
        questions.push({title: elicitation.message, options: []});
    }
    return {id: message.id, roomId, role: message.role, self: message.self,
      senderAeonId: message.senderAeonId, requestId: message.requestId,
      replyToMessageId: message.replyTo?.messageId,
      replyRootMessageId: message.replyRootMessageId,
      createdAt: message.createdAt, updatedAt: message.updatedAt,
      deleted: Boolean(message.deletedAt), deliveryState: message.deliveryState,
      text: message.text, attachmentCount: message.attachments?.length ?? 0,
      images: (message.attachments ?? []).filter(isImage).slice(0, 8).map(attachment => ({
        attachment_id: attachment.attachmentId, file_id: attachment.fileId || null,
        mime_type: attachment.mime || null, loading: Boolean(attachment.isLoading)})),
      questions, requiresUserAction: Boolean(elicitation),
      metadataKeys: Object.keys(message.messageMetadata ?? {}), aeonMetadata,
      turnTrigger: /^[a-z_]{1,80}$/.test(message.messageMetadata?.turn_trigger ?? '')
        ? message.messageMetadata.turn_trigger : undefined};
  }
  async function invoke(operation, args = {}) {
    try {
      assertCurrent();
      renewLease();
      if (operation === 'status') return {ok: true, value: {room_verified: verified,
        subscribed, transport: 'desktop_native_room', protocol_verified: false}};
      if (operation === 'messages') {
        const allowed = ['before', 'after', 'replyRootMessageId', 'limit'];
        if (Object.keys(args).some(key => !allowed.includes(key)) ||
            !Number.isInteger(args.limit ?? 20) || (args.limit ?? 20) < 1 || (args.limit ?? 20) > 100 ||
            allowed.slice(0, 3).some(key => args[key] != null &&
              (typeof args[key] !== 'string' || !args[key] || args[key].length > 256)))
          throw new Error('dot_invalid_message_query');
        const page = await room.client.messages(roomId, args);
        assertCurrent();
        return {ok: true, value: {messages: page.messages.map(project), before: page.before, after: page.after}};
      }
      if (operation === 'send') {
        if (sendInFlight) throw new Error('desktop_submission_in_progress');
        sendStarted = false; sendStartedAt = undefined;
        if (Object.keys(args).some(key => !['requestId', 'text', 'replyToMessageId', 'attachmentIds'].includes(key)) ||
            !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(args.requestId ?? '') ||
            typeof args.text !== 'string' || !args.text.trim() || new TextEncoder().encode(args.text).length > 20000 ||
            (args.replyToMessageId != null && (typeof args.replyToMessageId !== 'string' ||
              !args.replyToMessageId || args.replyToMessageId.length > 256)) ||
            (args.attachmentIds != null && (!Array.isArray(args.attachmentIds) || args.attachmentIds.length > 8 ||
              args.attachmentIds.some(id => typeof id !== 'string' || !id || id.length > 256 || /[\x00-\x20]/.test(id)))))
          throw new Error('dot_invalid_submission');
        // No retry here. Native client handles preparation and reports uncertainty.
        sendInFlight = true; sendStarted = false;
        try {
          const accepted = await room.client.send({...args, roomId});
          assertCurrent();
          return {ok: true, value: {...project(accepted), nativeSendStartedAt: sendStartedAt}};
        } catch (error) {
          if (sendStarted && !sameAccount())
            return {ok: false, error: 'dot_submission_unknown', delivery_unknown: true};
          throw error;
        } finally { sendInFlight = false; }
      }
      if (operation === 'upload_image') {
        if (Object.keys(args).some(key => !['data_base64', 'mime_type'].includes(key)) ||
            typeof args.data_base64 !== 'string' || args.data_base64.length > 5592408 ||
            args.data_base64.length % 4 || !/^[A-Za-z0-9+/]+={0,2}$/.test(args.data_base64) ||
            !['image/png', 'image/jpeg', 'image/webp'].includes(args.mime_type))
          throw new Error('dot_invalid_image_file');
        const binary = atob(args.data_base64);
        const signature = args.mime_type === 'image/png' ? binary.startsWith('\x89PNG\r\n\x1a\n') :
          args.mime_type === 'image/jpeg' ? binary.startsWith('\xff\xd8\xff') :
          binary.startsWith('RIFF') && binary.slice(8, 12) === 'WEBP';
        if (!signature || binary.length > 4 * 1024 * 1024)
          throw new Error('dot_invalid_image_file');
        const bytes = Uint8Array.from(binary, character => character.charCodeAt(0));
        const suffix = {'image/png': 'png', 'image/jpeg': 'jpg', 'image/webp': 'webp'}[args.mime_type];
        const file = new File([bytes], 'wechat-image.' + suffix, {type: args.mime_type});
        const uploaded = await room.media.upload(roomId, file, {
          signal: controller.signal, onProgress() {}});
        assertCurrent();
        if (typeof uploaded?.fileId !== 'string' || !uploaded.fileId || uploaded.fileId.length > 256)
          throw new Error('dot_image_upload_invalid_confirmation');
        return {ok: true, value: {file_id: uploaded.fileId, mime_type: uploaded.mime,
          status: uploaded.status, size_bytes: binary.length}};
      }
      if (operation === 'check_image_file') {
        if (Object.keys(args).some(key => key !== 'file_id') ||
            !/^CalpicoFile_[A-Za-z0-9_-]{1,200}$/.test(args.file_id ?? ''))
          throw new Error('dot_invalid_image_query');
        // Use the native file reader in the pinned room. The gateway only
        // supplies IDs already recorded for this bound owner's images.
        const result = await room.media.image(roomId, '', {
          type: 'file', attachmentId: args.file_id, fileId: args.file_id, mime: ''
        }, controller.signal);
        assertCurrent();
        const mime = result.attachment?.mime || result.image?.type;
        const available = result.image instanceof Blob && result.image.size > 0 &&
          result.image.size <= 4 * 1024 * 1024 &&
          ['image/png', 'image/jpeg', 'image/webp'].includes(mime);
        return {ok: true, value: {available, file_id: args.file_id,
          mime_type: available ? mime : null,
          size_bytes: available ? result.image.size : null}};
      }
      if (operation === 'download_image') {
        if (Object.keys(args).some(key => !['message_id', 'attachment_id', 'parent_id'].includes(key)) ||
            ['message_id', 'attachment_id', 'parent_id'].some(key => typeof args[key] !== 'string' ||
              !args[key] || args[key].length > 256))
          throw new Error('dot_invalid_image_query');
        let cursor = args.parent_id, message;
        for (let pageIndex = 0; pageIndex < 5; pageIndex++) {
          const page = await room.client.messages(roomId, {
            replyRootMessageId: binding.reply_anchor_message_id, after: cursor, limit: 20});
          message = page.messages.find(value => value.id === args.message_id);
          if (message || !page.messages.length || (!page.after && page.messages.length < 20)) break;
          cursor = page.messages.at(-1).id;
        }
        assertCurrent();
        if (!message || message.self !== false || message.role !== 'user' ||
            message.senderAeonId !== binding.dot_id ||
            message.replyRootMessageId !== binding.reply_anchor_message_id)
          throw new Error('dot_image_message_unavailable');
        const attachment = message.attachments?.find(value => value.attachmentId === args.attachment_id);
        if (!isImage(attachment)) throw new Error('dot_image_attachment_unavailable');
        if (attachment.isLoading) throw new Error('dot_image_not_ready');
        const result = await room.media.image(roomId, message.id, attachment, controller.signal);
        assertCurrent();
        if (!(result.image instanceof Blob) || !result.image.size) throw new Error('dot_image_not_ready');
        if (result.image.size > 4 * 1024 * 1024) throw new Error('dot_image_too_large');
        const bytes = new Uint8Array(await result.image.arrayBuffer());
        let binary = '';
        for (let offset = 0; offset < bytes.length; offset += 32768)
          binary += String.fromCharCode(...bytes.subarray(offset, offset + 32768));
        return {ok: true, value: {data_base64: btoa(binary), mime_type: result.image.type || attachment.mime,
          size_bytes: bytes.length, source_type: attachment.type,
          source_file_id: attachment.fileId || null,
          declared_size_bytes: result.attachment?.sizeBytes ?? null,
          width: result.width ?? null, height: result.height ?? null}};
      }
      if (operation === 'wait') {
        if (waiter) throw new Error('desktop_wait_already_active');
        if (updates.length) return {ok: true, value: updates.splice(0)};
        const value = await new Promise(resolve => {
          const timer = setTimeout(() => { waiter = null; resolve([]); }, 25000);
          waiter = value => { clearTimeout(timer); resolve(value); };
        });
        assertCurrent();
        return {ok: true, value};
      }
      throw new Error('desktop_operation_not_allowed');
    } catch (error) {
      const known = /^(auth_|dot_|desktop_)[a-z_]+$/;
      return {ok: false, error: known.test(error?.message ?? '') ? error.message :
        error?.deliveryUnknown ? 'dot_submission_unknown' : 'desktop_room_request_failed',
        delivery_unknown: error?.deliveryUnknown === true,
        submission_started: operation === 'send' ? sendStarted : undefined,
        http_status: Number.isInteger(error?.status) ? error.status : null};
    }
  }
  return {invoke, dispose};
}
