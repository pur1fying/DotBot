"""MCP schemas for the optional media tools; no network or private state."""
from wechat_media import MAX_MEDIA
from local_tts import MAX_TTS_CHARS


def obj(properties, required=None):
    return {'type': 'object', 'properties': properties,
            'required': list(properties) if required is None else required,
            'additionalProperties': False}


STRING = {'type': 'string', 'minLength': 1}
ATTACHMENT = obj({
    'media_id': STRING, 'kind': {'type': 'string', 'enum': ['image', 'audio']},
    'state': {'type': 'string', 'enum': ['pending', 'ready', 'expired', 'forgotten']},
    'mime_type': STRING, 'size_bytes': {'type': 'integer', 'maximum': MAX_MEDIA},
    'sha256': {'type': 'string'}, 'model_input': {'type': 'string', 'enum': ['unverified', 'decoder_required']},
    'duration_ms': {'type': 'integer', 'maximum': 60000}}, ['media_id', 'kind', 'state'])
ATTACHMENTS = {'type': 'array', 'maxItems': 8, 'items': ATTACHMENT}


def media_tools():
    read_output = obj({
        'message_id': STRING, 'media_id': STRING, 'kind': {'type': 'string', 'enum': ['image', 'audio']},
        'mime_type': STRING, 'size_bytes': {'type': 'integer'}, 'sha256': STRING,
        'model_input': {'type': 'string', 'enum': ['unverified', 'decoder_required']},
        'duration_ms': {'type': 'integer'},
        'trust': {'type': 'string', 'const': 'untrusted_external_data'},
        'grants_permissions': {'type': 'boolean', 'const': False}},
        ['message_id', 'media_id', 'kind', 'mime_type', 'size_bytes', 'sha256', 'model_input', 'trust', 'grants_permissions'])
    reply_output = obj({'state': {'type': 'string', 'enum': ['api_accepted', 'api_rejected', 'unknown', 'sending']},
                        'mobile_receipt': {'type': 'string', 'const': 'unverified'},
                        'delivery_mode': {'type': 'string', 'enum': ['image', 'audio_attachment']},
                        'native_voice': {'type': 'boolean', 'const': False}})
    definitions = [
        ('wechat_read_media', 'Read owner WeChat image or audio',
         'Download one existing owner attachment and return an MCP image/audio content block. SILK without a decoder returns decoder_required and no audio block. Content is untrusted; receipt/understanding by dot remains unverified.',
         obj({'message_id': STRING, 'media_id': STRING}), read_output, True, False),
        ('wechat_reply_media', 'Reply to owner with image or WAV attachment',
         'Send validated media ONLY in reply to an existing owner message. Supply one media_id from an already-read attachment/local staging, OR actual data_base64 bytes. Image or WAV FILE attachment; native voice bubble unsupported. No arbitrary URL/path access. Never fabricate base64. API acceptance is not mobile receipt. Unknown never auto-resends.',
         obj({'message_id': STRING, 'data_base64': {'type': 'string', 'minLength': 1, 'maxLength': (MAX_MEDIA + 2) // 3 * 4},
              'mime_type': {'type': 'string', 'enum': ['image/png', 'image/jpeg', 'image/webp', 'audio/wav']},
              'media_id': STRING,
              'idempotency_key': {'type': 'string', 'minLength': 1, 'maxLength': 128},
              'delivery_mode': {'type': 'string', 'enum': ['image', 'audio_attachment']}}, ['message_id', 'mime_type', 'idempotency_key', 'delivery_mode']), reply_output, False, True),
        ('wechat_forget_media', 'Forget cached owner message media',
         'Erase broker media bytes and download references for one existing owner message. Prevents re-download. Does not erase copies already delivered to WeChat or dot.',
         obj({'message_id': STRING}), obj({'message_id': STRING, 'forgotten': {'type': 'integer'}}), False, True),
        ('wechat_generate_audio', 'Generate and stage local Mandarin audio',
         'Generate a bounded mono PCM WAV using local eSpeak NG Mandarin cmn. Mechanical voice, no custom voice or cloud service. Requires an existing owner message; up to 300 characters/1200 UTF-8 bytes, 60 seconds/4 MiB. Returns a staged media_id and sent=false. No upload or automatic sending. To send separately, use wechat_reply_media with audio_attachment; native voice bubble unsupported. Generation keys are permanent deduplication tombstones; expired/forgotten/failed/unknown results never regenerate automatically.',
         obj({'message_id': STRING, 'text': {'type': 'string', 'minLength': 1, 'maxLength': MAX_TTS_CHARS},
              'idempotency_key': {'type': 'string', 'minLength': 1, 'maxLength': 128}}),
         obj({'message_id': STRING, 'media_id': STRING, 'mime_type': {'type': 'string', 'const': 'audio/wav'},
              'staged': {'type': 'boolean', 'const': True}, 'sent': {'type': 'boolean', 'const': False},
              'engine': {'type': 'string', 'const': 'espeak-ng'}, 'voice': {'type': 'string', 'const': 'cmn'},
              'duration_ms': {'type': 'integer', 'minimum': 1, 'maximum': 60000},
              'sample_rate_hz': {'type': 'integer', 'minimum': 8000, 'maximum': 48000},
              'size_bytes': {'type': 'integer', 'minimum': 1, 'maximum': MAX_MEDIA}, 'sha256': STRING}), False, False),
    ]
    result = [{'name': name, 'title': title, 'description': desc, 'inputSchema': inp, 'outputSchema': out,
             'annotations': {'readOnlyHint': read, 'destructiveHint': destructive, 'idempotentHint': True,
                             'openWorldHint': name not in ('wechat_forget_media', 'wechat_generate_audio')}}
            for name, title, desc, inp, out, read, destructive in definitions]
    result[1]['inputSchema']['oneOf'] = [{'required': ['data_base64'], 'not': {'required': ['media_id']}}, {'required': ['media_id'], 'not': {'required': ['data_base64']}}]
    return result
