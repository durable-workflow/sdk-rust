"""Installed Rust caller of the accepted shared committed-response-loss fixture."""
import concurrent.futures
from datetime import datetime, timezone
import hashlib
import json
import os
import stat
import threading
import types

import sqlite_legacy


FIXTURE_COMMIT = '117118c83093822469b3824ba9587ff4068ed29f'
FIXTURE_SHA256 = 'bd1e8ccbbf76f63fc2eafa278a023eb181d405b68d9c30016392e395a1eff725'


def fixture(path):
    """Execute only the exact accepted source bytes, without a second path read."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or info.st_mode & 0o022 or not 0 < info.st_size <= 64 * 1024):
            raise ValueError('loss fixture must be an owned bounded single-link regular file')
        source = os.pread(descriptor, 64 * 1024 + 1, 0)
        if hashlib.sha256(source).hexdigest() != FIXTURE_SHA256:
            raise ValueError('loss fixture differs from accepted public source')
    finally:
        os.close(descriptor)
    module = types.ModuleType('accepted_committed_response_loss')
    exec(compile(source, '<accepted-committed-response-loss>', 'exec'), module.__dict__)
    return module


def rows(snapshot, table):
    data = snapshot['tables'][table]
    return [dict(zip(data['columns'], (value[1] if len(value) == 2 else None for value in row)))
            for row in data['rows']]


def committed_identity(before, kind, workflow_id, run_id, request_id):
    token = kind + '-v1:' + hashlib.sha256(request_id.encode()).hexdigest()
    commands = [row for row in rows(before, 'workflow_commands')
                if row['workflow_instance_id'] == workflow_id and row['workflow_run_id'] == run_id
                and row['command_type'] == kind and row['request_id'] == token]
    assert len(commands) == 1 and commands[0]['id']
    original = {'command_id':commands[0]['id'], 'run_id':run_id}
    if kind == 'update':
        updates = [row for row in rows(before, 'workflow_updates')
                   if row['workflow_command_id'] == original['command_id']]
        assert len(updates) == 1 and updates[0]['id'] and updates[0]['accepted_at']
        assert updates[0]['workflow_instance_id'] == workflow_id and updates[0]['workflow_run_id'] == run_id
        original.update(update_id=updates[0]['id'], accepted_at=updates[0]['accepted_at'])
    return original


def timestamp(value, *, sqlite_utc=False):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        # Native SQLite stores UTC without a zone; wire receipts must carry one.
        assert sqlite_utc
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def run(path, origin, workflow_id, run_id, invoke, snapshot, work):
    """No worker, transform or upgrade: prove committed loss and explicit retries."""
    module = fixture(path)
    proxy = module.CommittedResponseLoss(origin)
    thread = threading.Thread(target=proxy.serve_forever, kwargs={'poll_interval':0.01}, daemon=True)
    thread.start()
    cases = []
    try:
        for kind, operation in [('signal','touch'), ('update','mutate')]:
            request_id = 'committed-loss-' + kind
            args = {'op':kind, 'workflow_id':workflow_id, 'name':operation,
                    'input':[1], 'request_id':request_id}
            route = '/api/workflows/' + workflow_id + '/' + kind + '/' + operation
            proxy.arm('POST', route)
            lost = invoke(args, proxy.url)
            assert lost['kind'] == 'transport', lost
            dropped = [row for row in proxy.records if row['path'] == route and row['dropped']]
            assert len(dropped) == 1 and dropped[0]['method'] == 'POST'
            assert dropped[0]['upstream_status'] == 202 and dropped[0]['upstream_complete'] is True
            assert 0 < dropped[0]['upstream_body_bytes'] <= module.MAX_RESPONSE_BYTES
            # Capture the committed identity before any same-key retry. The
            # accepted proxy retains hashes/metadata only, never response bodies.
            before = snapshot()
            original = committed_identity(before, kind, workflow_id, run_id, request_id)
            exported = invoke({'op':'history','workflow_id':workflow_id,'run_id':run_id}, origin)
            assert exported['kind'] == 'ok'
            events = exported['value']['history_events']
            event_type = 'SignalReceived' if kind == 'signal' else 'UpdateAccepted'
            assert sum(event['type'] == event_type for event in events) == 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                replies = list(pool.map(lambda _:invoke(args, proxy.url), range(4)))
            for reply in replies:
                assert reply['kind'] == ('ok' if kind == 'signal' else 'pending'), reply
                receipt = reply['value'] if kind == 'signal' else reply['receipt']
                assert receipt['retained'] is True and receipt['intent_verified'] is True
                assert receipt['request_id'] == request_id
                for field,value in original.items():
                    if field == 'accepted_at':
                        assert timestamp(receipt[field]) == timestamp(value, sqlite_utc=True)
                    else:
                        assert value and receipt[field] == value
            conflict = invoke(dict(args,input=[True]), origin)
            assert conflict['kind'] == 'http' and conflict['status'] == 409 and conflict['conflict'] is True
            assert conflict['request_id'] == request_id and conflict['body']['reason'] == 'request_id_conflict'
            assert invoke({'op':'history','workflow_id':workflow_id,'run_id':run_id}, origin)['value']['history_events'] == events
            after = snapshot()
            assert after == before, 'postcommit retries/conflict changed logical rows/schema'
            for suffix,value in [('before',before), ('after',after)]:
                with open(os.open(work / (kind+'-loss-'+suffix+'.json'),
                                  os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600),'w') as output:
                    json.dump(value,output,sort_keys=True)
            cases.append({'command_type':kind,'outcome':'pass','original':original,
                          'returned':{field:receipt[field] for field in original},'concurrency':4,
                          'retained':True,'intent_verified':True,
                          'upstream':dropped[0],'zero_write_sha256':sqlite_legacy.fingerprint(before)})
        return {'cell':'postcommit-response-loss-only','cases':cases,
                'fixture_commit':FIXTURE_COMMIT,'fixture_sha256':FIXTURE_SHA256}
    finally:
        proxy.shutdown(); proxy.server_close(); thread.join(timeout=5)
        if thread.is_alive():
            raise ValueError('loss fixture failed to stop')
