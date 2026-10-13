"""Opt-in owner media adapter, on-demand download, private bounded cache.

CDN references/keys remain in the broker's existing private SQLite database.
No media bytes are placed in an event or silently uploaded to another service.
"""
import base64
import hashlib
import json
import secrets
import time
import io
import wave

from ilink_adapter import TextAdapter, AdapterError, acknowledgement
from wechat_media import (MediaError, MAX_CACHE, RETENTION, CDN_BASE, reference,
                          crypt, validate_media, decode_base64, cdn_url)
from urllib.parse import urlencode

IMAGE_FILE_TYPES = {'.png': 'image/png', '.jpg': 'image/jpeg',
                    '.jpeg': 'image/jpeg', '.webp': 'image/webp'}


class MediaAdapter(TextAdapter):
    def __init__(self, *args, media_transport=None, silk_decoder=None, tts=None,
                 images_only=False, direct_images=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.images_only, self.direct_images = images_only, direct_images
        self.dot.quote_images_enabled = direct_images
        self.media_transport, self.silk_decoder = media_transport, silk_decoder
        self.tts = tts
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS media (
            id TEXT PRIMARY KEY, message_id TEXT, kind TEXT, reference TEXT,
            blob BLOB, metadata TEXT, expires REAL, state TEXT);
          CREATE TABLE IF NOT EXISTS staged_media (
            id TEXT PRIMARY KEY, message_id TEXT, blob BLOB, mime_type TEXT, expires REAL);
          CREATE TABLE IF NOT EXISTS tts_generations (
            k TEXT PRIMARY KEY, message_id TEXT, digest TEXT, media_id TEXT,
            state TEXT, expires REAL, error_category TEXT);
        ''')
        if kwargs.get('recover_sending', True):
            with self.db:
                self.db.execute("UPDATE tts_generations SET state='unknown' WHERE state='generating'")

    def _parse_media(self, m):
        if not isinstance(m, dict) or not isinstance(m.get('item_list'), list):
            return None
        texts, refs = [], []
        try:
            for index, item in enumerate(m['item_list']):
                if not isinstance(item, dict):
                    return None
                kind = item.get('type')
                if self.images_only and kind not in (1, 2, 4):
                    return None
                if kind == 1:
                    value = item.get('text_item', {}).get('text')
                    if not isinstance(value, str):
                        return None
                    texts.append(value)
                elif kind in (2, 3):
                    field = item.get('image_item' if kind == 2 else 'voice_item')
                    if not isinstance(field, dict):
                        return None
                    media_kind = 'image' if kind == 2 else 'audio'
                    ref = reference(field, media_kind)
                    if kind == 3:
                        duration = field.get('playtime')
                        if duration is not None and (type(duration) is not int or not 0 < duration <= 60000):
                            return None
                        transcript = field.get('text')
                        if transcript is not None:
                            if not isinstance(transcript, str) or len(transcript.encode()) > 32768:
                                return None
                            if transcript:
                                texts.append('[微信提供的语音转写，未校对] ' + transcript)
                    refs.append((index, media_kind, ref))
                elif kind == 4:
                    field = item.get('file_item')
                    name = field.get('file_name') if isinstance(field, dict) else None
                    if not isinstance(name, str) or not name or len(name.encode()) > 1024 or any(ord(c) < 32 for c in name):
                        return None
                    mime = next((mime for suffix, mime in IMAGE_FILE_TYPES.items() if name.lower().endswith(suffix)), None)
                    if mime is None and (self.images_only or not name.lower().endswith(('.wav', '.silk'))):
                        return None
                    length = field.get('len')
                    if length is not None and (not isinstance(length, str) or not length.isascii() or not length.isdecimal() or not 0 < int(length) <= 4 * 1024 * 1024):
                        return None
                    media_kind = 'image' if mime else 'audio'
                    ref = reference(field, media_kind)
                    if mime:
                        # Private transport provenance; never choose a local path
                        # or grant permissions based on an external filename.
                        ref.update(source_type=4, file_name=name, declared_mime=mime,
                                   declared_size_bytes=int(length) if length is not None else None)
                    refs.append((index, media_kind, ref))
                else:
                    return None
            if len(refs) > 8:
                return None
            # Base parser remains the authority for sender, recipient, group,
            # message state, actual message ID, context and text limits.
            copy = dict(m, item_list=[{'type': 1, 'text_item': {'text': '\n'.join(texts) or '[媒体消息，请读取附件]'}}]) if refs else m
            parsed = super()._parse(copy)
            if parsed is None or not 1 <= len(m['item_list']) <= 32:
                return None
            return (*parsed, refs)
        except (ValueError, TypeError, AttributeError, KeyError):
            return None

    def ingest(self, result):
        msgs, cursor = result.get('msgs', []), result.get('get_updates_buf', '')
        if not isinstance(msgs, list) or len(msgs) > 1000 or not isinstance(cursor, str) or len(cursor) > 65536:
            raise AdapterError('invalid_batch')
        accepted = rejected = duplicates = 0
        with self.db:
            for msg in msgs:
                parsed = self._parse_media(msg)
                if parsed is None:
                    rejected += 1
                    continue
                mid, text, context, refs = parsed
                from wechat_quotes import capture
                capture(self.db, mid, msg, text)
                # Keep the old digest for pure text, allowing upgrade without replay.
                values = [text, context] + ([refs] if refs else [])
                digest = hashlib.sha256(json.dumps(values).encode()).hexdigest()
                old = self.db.execute('SELECT digest FROM messages WHERE id=?', (mid,)).fetchone()
                if old:
                    if old[0] != digest:
                        raise AdapterError('message_id_conflict')
                    duplicates += 1
                    continue
                received = self.clock()
                self.db.execute('INSERT INTO messages VALUES (?,?,?,?,?)', (mid, text, context, received, digest))
                direct = self.direct_images and refs and all(kind == 'image' for _, kind, _ in refs)
                self.dot.record_incoming(mid, received, force_webhook=bool(refs) and not direct)
                for index, kind, ref in refs:
                    aid = 'media_' + hashlib.sha256((mid + ':' + str(index)).encode()).hexdigest()
                    self.db.execute('INSERT INTO media VALUES (?,?,?,?,?,?,?,?)',
                                    (aid, mid, kind, json.dumps(ref), None, None, received + RETENTION, 'pending'))
                accepted += 1
            if cursor:
                self._set('cursor', cursor)
        self.prune()
        return {'accepted': accepted, 'rejected': rejected, 'duplicates': duplicates}

    def prune(self):
        # Wipe references as well as plaintext, retaining opaque tombstones so
        # old messages cannot be re-downloaded after expiry/forget.
        with self.db:
            self.db.execute("UPDATE media SET reference=NULL,blob=NULL,metadata=NULL,state='expired' WHERE expires<=? AND state!='forgotten'", (self.clock(),))
            self.db.execute('DELETE FROM staged_media WHERE expires<=?', (self.clock(),))
            self.db.execute("UPDATE tts_generations SET state='expired',media_id=NULL WHERE expires<=? AND state!='forgotten'", (self.clock(),))

    def read_page(self, after_seq=0, limit=20, message_id=None, *, pending_only=False):
        self.prune()
        page = super().read_page(after_seq, limit, message_id, pending_only=pending_only)
        # Image direct routing uses private dot_* IPC. Keep the original public
        # eight-tool MCP contract until full media tools are explicitly enabled.
        if self.images_only:
            return page
        for msg in page['messages']:
            msg['attachments'] = []
            for aid, kind, state, metadata in self.db.execute('SELECT id,kind,state,metadata FROM media WHERE message_id=? ORDER BY rowid', (msg['message_id'],)):
                msg['attachments'].append({'media_id': aid, 'kind': kind, 'state': state,
                                           **(json.loads(metadata) if metadata else {})})
        return page

    def read_media(self, message_id, media_id):
        self.prune()
        row = self.db.execute('SELECT kind,reference,blob,metadata,state FROM media WHERE id=? AND message_id=?', (media_id, message_id)).fetchone()
        if row is None:
            raise MediaError('unknown_message_media')
        kind, ref, raw, metadata, state = row
        if state in ('expired', 'forgotten'):
            raise MediaError('media_unavailable')
        ref = json.loads(ref)
        if raw is None:
            if self.media_transport is None:
                raise MediaError('media_network_disabled')
            raw = self.media_transport.request('GET', ref['url'])
            if ref['key']:
                raw = crypt(raw, bytes.fromhex(ref['key']))
        meta = validate_media(raw, kind, ref.get('declared_mime'))
        if ref.get('declared_size_bytes') is not None and len(raw) != ref['declared_size_bytes']:
            raise MediaError('file_size_mismatch')
        if meta['mime_type'] == 'audio/silk' and self.silk_decoder is not None:
            try:
                raw = self.silk_decoder(raw)
            except Exception:
                raise MediaError('silk_decode_failed') from None
            meta = validate_media(raw, 'audio', 'audio/wav')
        with self.db:
            used = self.db.execute('SELECT COALESCE(SUM(length(blob)),0) FROM media WHERE id!=?', (media_id,)).fetchone()[0]
            used += self.db.execute('SELECT COALESCE(SUM(length(blob)),0) FROM staged_media').fetchone()[0]
            if used + len(raw) > MAX_CACHE:
                raise MediaError('media_cache_quota')
            self.db.execute("UPDATE media SET blob=?,metadata=?,state='ready' WHERE id=?", (raw, json.dumps(meta), media_id))
        result = {'message_id': message_id, 'media_id': media_id, **meta,
                  'trust': 'untrusted_external_data', 'grants_permissions': False}
        # Raw SILK is retained only in private state until a decoder is available.
        if meta['mime_type'] != 'audio/silk':
            result['content'] = [{'type': kind, 'mimeType': meta['mime_type'], 'data': base64.b64encode(raw).decode()}]
        else:
            result['content'] = []
        return result

    def forget_media(self, message_id):
        if not self.db.execute('SELECT 1 FROM messages WHERE id=?', (message_id,)).fetchone():
            raise MediaError('unknown_message')
        with self.db:
            changed = self.db.execute("UPDATE media SET reference=NULL,blob=NULL,metadata=NULL,state='forgotten' WHERE message_id=?", (message_id,)).rowcount
            changed += self.db.execute('DELETE FROM staged_media WHERE message_id=?', (message_id,)).rowcount
            self.db.execute("UPDATE tts_generations SET state='forgotten',media_id=NULL WHERE message_id=?", (message_id,))
        return {'message_id': message_id, 'forgotten': changed}

    def stage_media(self, message_id, data_base64, mime_type):
        """Local operator IPC only: place explicitly selected output bytes, no send."""
        self.prune()
        if not self.db.execute('SELECT 1 FROM messages WHERE id=?', (message_id,)).fetchone():
            raise MediaError('unknown_message')
        kind = 'image' if mime_type.startswith('image/') else 'audio'
        raw = decode_base64(data_base64)
        validate_media(raw, kind, mime_type)
        if mime_type not in ('image/png', 'image/jpeg', 'image/webp', 'audio/wav'):
            raise MediaError('unsupported_media_kind')
        aid = 'out_' + secrets.token_hex(16)
        with self.db:
            used = self.db.execute('SELECT COALESCE(SUM(length(blob)),0) FROM media').fetchone()[0]
            used += self.db.execute('SELECT COALESCE(SUM(length(blob)),0) FROM staged_media').fetchone()[0]
            if used + len(raw) > MAX_CACHE:
                raise MediaError('media_cache_quota')
            self.db.execute('INSERT INTO staged_media VALUES (?,?,?,?,?)', (aid, message_id, raw, mime_type, self.clock() + RETENTION))
        return {'message_id': message_id, 'media_id': aid, 'mime_type': mime_type, 'staged': True, 'sent': False}

    def generate_audio(self, message_id, text, idempotency_key):
        """Generate+stage only. Hash the text; never put it in logs or the TTS DB."""
        from local_tts import validate_text, VOICE
        from wechat_media import ERROR_CATEGORIES
        validate_text(text)
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 128:
            raise MediaError('invalid_idempotency_key')
        self.prune()
        digest = hashlib.sha256(json.dumps([message_id, text, VOICE], ensure_ascii=False).encode()).hexdigest()
        old = self.db.execute('SELECT message_id,digest,media_id,state,error_category FROM tts_generations WHERE k=?', (idempotency_key,)).fetchone()
        if old:
            if old[:2] != (message_id, digest):
                raise MediaError('tts_idempotency_conflict')
            if old[3] != 'ready':
                raise MediaError('tts_result_unavailable')
            row = self.db.execute('SELECT blob FROM staged_media WHERE id=? AND message_id=? AND expires>?', (old[2], message_id, self.clock())).fetchone()
            if row is None:
                raise MediaError('tts_result_unavailable')
            return self._tts_result(message_id, old[2], row[0])
        row = self.db.execute('SELECT received FROM messages WHERE id=?', (message_id,)).fetchone()
        if row is None:
            raise MediaError('unknown_message')
        if self.clock() - row[0] > 3600:
            raise MediaError('local_context_age_limit')
        if self.tts is None:
            raise MediaError('tts_library_missing')
        expires = self.clock() + RETENTION
        with self.db:
            self.db.execute('INSERT INTO tts_generations VALUES (?,?,?,?,?,?,?)',
                            (idempotency_key, message_id, digest, None, 'generating', expires, None))
        try:
            raw = self.tts(text)
            validate_media(raw, 'audio', 'audio/wav')
            with wave.open(io.BytesIO(raw)) as wav:
                if wav.getnchannels() != 1 or not any(wav.readframes(wav.getnframes())):
                    raise MediaError('tts_output_invalid')
            aid = 'out_' + secrets.token_hex(16)
            with self.db:
                used = self.db.execute('SELECT COALESCE(SUM(length(blob)),0) FROM media').fetchone()[0]
                used += self.db.execute('SELECT COALESCE(SUM(length(blob)),0) FROM staged_media').fetchone()[0]
                if used + len(raw) > MAX_CACHE:
                    raise MediaError('media_cache_quota')
                self.db.execute('INSERT INTO staged_media VALUES (?,?,?,?,?)', (aid, message_id, raw, 'audio/wav', expires))
                self.db.execute("UPDATE tts_generations SET media_id=?,state='ready' WHERE k=?", (aid, idempotency_key))
            return self._tts_result(message_id, aid, raw)
        except Exception as error:
            category = str(error) if isinstance(error, MediaError) and str(error) in ERROR_CATEGORIES else 'tts_synthesis_failed'
            with self.db:
                self.db.execute("UPDATE tts_generations SET state='failed',error_category=? WHERE k=?", (category, idempotency_key))
            raise MediaError(category) from None

    def _tts_result(self, message_id, media_id, raw):
        meta = validate_media(raw, 'audio', 'audio/wav')
        with wave.open(io.BytesIO(raw)) as stream:
            rate = stream.getframerate()
        return {'message_id': message_id, 'media_id': media_id, 'mime_type': 'audio/wav',
                'staged': True, 'sent': False, 'engine': 'espeak-ng', 'voice': 'cmn',
                'duration_ms': meta['duration_ms'], 'sample_rate_hz': rate,
                'size_bytes': len(raw), 'sha256': meta['sha256']}

    def reply_media(self, message_id, mime_type, idempotency_key, delivery_mode, data_base64=None, media_id=None, *, _route='webhook'):
        if self.dot.route(message_id) != _route:
            raise AdapterError('message_route_mismatch')
        if delivery_mode not in ('image', 'audio_attachment'):
            raise MediaError('native_voice_not_verified')
        kind = 'image' if delivery_mode == 'image' else 'audio'
        if (data_base64 is None) == (media_id is None):
            raise MediaError('media_source_required')
        if media_id is not None:
            self.prune()
            cached = self.db.execute("SELECT blob FROM media WHERE id=? AND message_id=? AND state='ready' AND expires>?", (media_id, message_id, self.clock())).fetchone()
            if cached is None:
                cached = self.db.execute('SELECT blob FROM staged_media WHERE id=? AND message_id=? AND expires>?', (media_id, message_id, self.clock())).fetchone()
            if cached is None or cached[0] is None:
                raise MediaError('media_not_ready_or_unknown')
            raw = cached[0]
        else:
            raw = decode_base64(data_base64)
        meta = validate_media(raw, kind, mime_type)
        if kind == 'audio' and mime_type != 'audio/wav':
            raise MediaError('outgoing_audio_requires_wav')
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 128:
            raise MediaError('invalid_idempotency_key')
        fingerprint = 'media:' + json.dumps([delivery_mode, mime_type, meta['sha256']], separators=(',', ':'))
        old = self.db.execute('SELECT message_id,text,state FROM replies WHERE k=?', (idempotency_key,)).fetchone()
        if old:
            if old[:2] != (message_id, fingerprint):
                raise MediaError('reply_idempotency_conflict')
            return {'state': old[2], 'mobile_receipt': 'unverified', 'delivery_mode': delivery_mode, 'native_voice': False}
        row = self.db.execute('SELECT context,received FROM messages WHERE id=?', (message_id,)).fetchone()
        if row is None:
            raise MediaError('unknown_message')
        if self.clock() - row[1] > 3600:
            raise MediaError('local_context_age_limit')
        if self.media_transport is None or self.exchange is None:
            raise MediaError('media_network_disabled')
        client_id = 'dot_' + hashlib.sha256((self.session + '\0' + idempotency_key).encode()).hexdigest()
        with self.db:
            self.db.execute('INSERT INTO replies VALUES (?,?,?,?,?)', (idempotency_key, message_id, fingerprint, client_id, 'sending'))
        # Persist before upload: ambiguity/restart must never auto-upload/send.
        state, category, http_status = 'unknown', 'media_exchange_failed', None
        started = time.monotonic()
        try:
            key, filekey = secrets.token_bytes(16), secrets.token_hex(16)
            ciphertext = crypt(raw, key, encrypt=True)
            upload = self._request('/ilink/bot/getuploadurl', {
                'filekey': filekey, 'media_type': 1 if kind == 'image' else 3,
                'to_user_id': self.binding.user_id, 'rawsize': len(raw),
                'rawfilemd5': hashlib.md5(raw).hexdigest(), 'filesize': len(ciphertext),
                'no_need_thumb': True, 'aeskey': key.hex()})
            if any(type(upload.get(k, 0)) is not int or upload.get(k, 0) != 0 for k in ('ret', 'errcode')) or upload.get('errmsg'):
                raise MediaError('upload_url_rejected')
            url = upload.get('upload_full_url')
            if not url:
                param = upload.get('upload_param')
                if not isinstance(param, str) or not 0 < len(param) <= 8192:
                    raise MediaError('upload_url_missing')
                url = CDN_BASE + '/upload?' + urlencode({'encrypted_query_param': param, 'filekey': filekey})
            cdn_url(url, 'upload')
            download_param = self.media_transport.request('POST', url, ciphertext)
            if not isinstance(download_param, str) or not 0 < len(download_param) <= 8192 or any(ord(c) < 32 for c in download_param):
                raise MediaError('cdn_upload_ack_missing')
            media = {'encrypt_query_param': download_param, 'aes_key': base64.b64encode(key.hex().encode()).decode(), 'encrypt_type': 1}
            item = {'type': 2, 'image_item': {'media': media, 'mid_size': len(ciphertext)}} if kind == 'image' else {
                'type': 4, 'file_item': {'media': media, 'file_name': 'alice-audio.wav', 'len': str(len(raw))}}
            ack = self._request('/ilink/bot/sendmessage', {'msg': {
                'from_user_id': '', 'to_user_id': self.binding.user_id, 'client_id': client_id,
                'message_type': 2, 'message_state': 2, 'context_token': row[0], 'item_list': [item]}})
            state, category = acknowledgement(ack)
            if state == 'api_accepted':
                from wechat_quotes import record_sent
                with self.db:
                    record_sent(self.db, ack, idempotency_key, '[图片]' if kind == 'image' else '[音频文件]')
            http_status = 200
        except AdapterError as e:
            category, http_status = getattr(e, 'category', 'media_exchange_failed'), getattr(e, 'http_status', None)
        except MediaError:
            category = 'media_transfer_failed'
        except Exception:
            category = 'media_exchange_failed'
        with self.db:
            self.db.execute('UPDATE replies SET state=? WHERE k=?', (state, idempotency_key))
            self.db.execute('INSERT OR REPLACE INTO reply_diagnostics VALUES (?,?,?,?,?)',
                            (idempotency_key, self.clock(), int((time.monotonic() - started) * 1000), category, http_status))
        return {'state': state, 'mobile_receipt': 'unverified', 'delivery_mode': delivery_mode, 'native_voice': False}
