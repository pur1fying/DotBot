// Runs in the existing Codex renderer. All network, auth, integrity preparation,
// route caching and message projection remain in the installed desktop client.
// This adapter is pinned to desktop 26.928.21956; a changed bundle must fail closed.
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
  let subscribed = false, closed = false, sendInFlight = false, sendStarted = false;
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
    conversationId: binding.thread_id, beforeSend() { sendStarted = true; },
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
  // Return only projected text and correlation fields. No tokens, signed media,
  // widgets, browser auth actions, raw message parts or internal tool output.
  function project(message) {
    if (message.roomId !== roomId) throw new Error('dot_room_identity_mismatch');
    return {id: message.id, roomId, role: message.role, self: message.self,
      senderAeonId: message.senderAeonId, requestId: message.requestId,
      replyToMessageId: message.replyTo?.messageId,
      replyRootMessageId: message.replyRootMessageId,
      createdAt: message.createdAt, updatedAt: message.updatedAt,
      deleted: Boolean(message.deletedAt), deliveryState: message.deliveryState,
      text: message.text, attachmentCount: message.attachments?.length ?? 0};
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
        if (Object.keys(args).some(key => !['requestId', 'text', 'replyToMessageId'].includes(key)) ||
            !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(args.requestId ?? '') ||
            typeof args.text !== 'string' || !args.text.trim() || new TextEncoder().encode(args.text).length > 20000 ||
            (args.replyToMessageId != null && (typeof args.replyToMessageId !== 'string' ||
              !args.replyToMessageId || args.replyToMessageId.length > 256)))
          throw new Error('dot_invalid_submission');
        // No retry here. Native client handles preparation and reports uncertainty.
        sendInFlight = true; sendStarted = false;
        try {
          const accepted = await room.client.send({...args, roomId});
          assertCurrent();
          return {ok: true, value: project(accepted)};
        } catch (error) {
          if (sendStarted && !sameAccount())
            return {ok: false, error: 'dot_submission_unknown', delivery_unknown: true};
          throw error;
        } finally { sendInFlight = false; }
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
        http_status: Number.isInteger(error?.status) ? error.status : null};
    }
  }
  return {invoke, dispose};
}
