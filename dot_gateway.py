"""Inspect the real dot protocol before enabling any message delivery.

The current desktop supports both older thread input and newer messaging rooms.
This preflight never assumes that a readable dot root accepts direct input.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
import time

from dot_auth import CodexAuth, DotError
from dot_transport import DotClient, DotHTTP
from ilink_ipc import BrokerError


def read_private_json(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 16384:
            raise DotError('dot_unsafe_verification_file')
        return json.load(stream)


async def operations(args):
    from ilink_ipc import call
    from dot_desktop import DesktopRoomClient
    from dot_runner import Gateway, serve
    async def broker(method, values=None):
        return await asyncio.to_thread(call, method, values, args.broker_socket)
    if args.command == 'serve':
        await serve(args.broker_socket, args.state_dir, port=args.debug_port, target_id=args.desktop_target)
        return {}
    if args.command == 'bind':
        if not args.verification:
            raise DotError('dot_verification_file_required')
        proof = read_private_json(args.verification)
        if proof.get('protocol') != 'desktop_room_reply_v1' or proof.get('verified') is not True:
            raise DotError('dot_protocol_unverified')
        binding = proof.get('binding', {})
        room = DesktopRoomClient(port=args.debug_port, target_id=args.desktop_target)
        try:
            await room.open(binding)
            result = await broker('dot_bind', {'binding': binding})
            await broker('dot_room_set_cursor', {'cursor': proof['initial_room_cursor']})
            return result
        finally:
            await room.close()
    if args.command == 'status':
        result = await broker('dot_status')
        try:
            result['gateway'] = read_private_json(args.state_dir / 'gateway-status.json')
        except (OSError, ValueError, DotError):
            result['gateway'] = {'state': 'unavailable'}
        return result
    if args.command in ('activate', 'canary'):
        health = read_private_json(args.state_dir / 'gateway-status.json')
        age = time.time() - datetime.fromisoformat(health['updated_at']).timestamp()
        if health.get('state') != 'ready' or not 0 <= age <= 60:
            raise DotError('dot_gateway_not_ready')
        return await broker('dot_canary' if args.command == 'canary' else 'dot_activate')
    if args.command == 'rollback':
        return await broker('dot_rollback')
    if args.command == 'metrics':
        return await broker('dot_metrics')
    if args.command == 'reconcile':
        binding = await broker('dot_binding')
        if not binding:
            raise DotError('dot_not_bound')
        room = DesktopRoomClient(port=args.debug_port, target_id=args.desktop_target)
        try:
            await room.open(binding)
            await Gateway(broker, room, binding).read_history()
            return await broker('dot_status')
        finally:
            await room.close()
    raise DotError('dot_operation_not_allowed')


def candidate_from_response(response, identity, source):
    if not isinstance(response, dict):
        raise DotError('dot_identity_invalid_response')
    selection, profile = response.get('selection'), response.get('profile')
    if not isinstance(selection, dict) or not isinstance(profile, dict):
        raise DotError('dot_primary_unavailable')
    thread_id = profile.get('active_root_thread_id')
    if (selection.get('available') is not True or profile.get('aeon_kind') != 'orbit'
            or profile.get('status') != 'active' or profile.get('id') != selection.get('aeon_id')
            or thread_id != selection.get('thread_id')):
        raise DotError('dot_identity_mismatch')
    values = [profile.get('id'), thread_id, profile.get('display_name')]
    if any(not isinstance(value, str) or not 0 < len(value) <= 256 for value in values):
        raise DotError('dot_identity_incomplete')
    return {'account_id': identity.account_id, 'user_id': identity.user_id,
            'dot_id': values[0], 'thread_id': values[1], 'name': values[2],
            'messaging_room_id': profile.get('messaging_room_id'),
            'paused': profile.get('is_paused') is True, 'identity_source': source}


def desktop_candidate(path, identity):
    """Read only the exact dot metadata key, never auth, cookies or chat content."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_size > 32 * 1024 * 1024:
            raise DotError('dot_desktop_metadata_unavailable')
        data = json.load(stream)
    cached = data.get('electron-persisted-atom-state', {}).get('primary-aeon-selection-v1')
    if not isinstance(cached, dict):
        raise DotError('dot_desktop_metadata_unavailable')
    if cached.get('accountId') != identity.account_id or cached.get('userId') != identity.user_id:
        raise DotError('auth_account_changed')
    return candidate_from_response(cached.get('response'), identity, 'desktop_account_cache')


async def preflight(auth, *, cache_path=None, http=None, client_factory=DotClient):
    authorization = await auth.get()
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'auth': 'ok',
              'protocol_verified': False, 'ready_to_activate': False,
              'application_message_sent': False}
    transport = http or DotHTTP()
    try:
        primary = await asyncio.to_thread(transport.get, '/tbo/primary', authorization)
        candidate = candidate_from_response(primary, authorization.identity, 'identity_http')
        report['identity_http'] = 'ok'
    except DotError as error:
        report['identity_http'] = error.category
        report['identity_http_status'] = error.status
        if cache_path is None:
            raise
        candidate = desktop_candidate(cache_path, authorization.identity)
    report['candidate'] = candidate
    client = client_factory(authorization)
    try:
        await client.open()
        report['websocket'] = 'ok'
        result = await client.request('thread/read', {'threadId': candidate['thread_id'], 'includeTurns': False})
        thread = result.get('thread', {}) if isinstance(result, dict) else {}
        if thread.get('id') != candidate['thread_id'] or thread.get('threadSource') != 'aeon':
            raise DotError('dot_thread_identity_mismatch')
        report['authenticated_thread_read'] = 'ok'
        report['can_accept_direct_input'] = thread.get('canAcceptDirectInput')
        report['thread_status'] = thread.get('status', {}).get('type')
        if candidate['paused']:
            report['blocker'] = 'dot_paused'
        elif thread.get('canAcceptDirectInput') is not True:
            report['blocker'] = 'dot_direct_input_unavailable'
            report['required_transport'] = 'messaging_room' if candidate['messaging_room_id'] else 'unverified'
        else:
            report['blocker'] = 'dot_reply_protocol_not_yet_verified'
        return report
    finally:
        await client.close()


async def run(args):
    if args.command not in ('preflight', 'preflight-desktop'):
        result = await operations(args)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    auth = CodexAuth(args.codex)
    try:
        if args.command == 'preflight-desktop':
            from dot_desktop import DesktopRoomClient
            authorization = await auth.get()
            candidate = desktop_candidate(args.desktop_state, authorization.identity)
            client = DesktopRoomClient(port=args.debug_port, target_id=args.desktop_target)
            try:
                status = await client.open(candidate)
                # Verify a read as well as the room snapshot without logging content.
                page = await client.invoke('messages', {'limit': 1})
                result = {'checked_at': datetime.now(timezone.utc).isoformat(),
                          'candidate': candidate, **status,
                          'history_read': 'ok', 'history_sample_count': len(page['messages']),
                          'application_message_sent': False, 'ready_to_activate': False,
                          'blocker': 'dot_room_reply_protocol_not_yet_verified'}
            finally:
                await client.close()
        else:
            result = await preflight(auth, cache_path=args.desktop_state)
    finally:
        await auth.close()
    if args.output:
        path = Path(args.output)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['ready_to_activate'] else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['preflight', 'preflight-desktop', 'bind', 'status',
                                          'serve', 'reconcile', 'canary', 'activate', 'rollback', 'metrics'])
    parser.add_argument('--codex', default='/usr/lib/chatgpt/resources/codex')
    parser.add_argument('--desktop-state', type=Path, default=Path.home()/'.codex/.codex-global-state.json')
    parser.add_argument('--output', help='Private JSON result; no token or message content')
    parser.add_argument('--debug-port', type=int, default=9229,
                        help='Already enabled desktop debugging port on 127.0.0.1')
    parser.add_argument('--desktop-target', help='Exact renderer target ID when multiple app windows are open')
    parser.add_argument('--broker-socket', type=Path,
                        default=Path(__file__).parent / 'runtime/wechat-private/broker.sock')
    parser.add_argument('--state-dir', type=Path, default=Path(__file__).parent / 'runtime/dot-private')
    parser.add_argument('--verification', type=Path, help='Private result of a verified native room reply test')
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except DotError as error:
        print(json.dumps({'ready_to_activate': False, 'error': error.category,
                          'http_status': error.status, 'rpc_code': error.code}))
        return 2
    except BrokerError as error:
        print(json.dumps({'error': error.category}))
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception:
        print(json.dumps({'ready_to_activate': False, 'error': 'dot_preflight_failed'}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
