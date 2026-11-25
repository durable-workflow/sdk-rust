"""Read a producer-owned physical seed without synthesizing legacy identities."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat

import execution_guards as guards
import sqlite_legacy


PROFILE = Path(__file__).with_name('physical-sqlite-profile.json')
PROFILE_SHA256 = '650f94305e9638dd164d853680f9ce5c3b8e5219dd5c12351652a99e29e194cd'


def profile():
    raw = PROFILE.read_bytes()
    if hashlib.sha256(raw).hexdigest() != PROFILE_SHA256:
        raise ValueError('physical profile differs from the reviewed tagged source')
    return json.loads(raw)


def require_handoff(commit):
    selected = profile()
    if commit != selected['producer_contract_commit']:
        raise ValueError('producer handoff commit differs from the reviewed physical profile')
    return selected


def attribution(commit, declared_execution):
    selected = require_handoff(commit)
    if declared_execution != selected['baseline_seed_execution']:
        raise ValueError('historical seed source/driver identity differs from the frozen execution')
    return {'accepted_contract_source':commit,
            'baseline_seed_execution':deepcopy(selected['baseline_seed_execution'])}


def read_owned(path, digest, limit):
    path = Path(path)
    if (not path.is_absolute() or path.resolve() != path
            or not re.fullmatch('[0-9a-f]{64}', digest)):
        raise ValueError('physical input needs a canonical path and exact SHA-256')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not 0 < os.fstat(descriptor).st_size <= limit:
            raise ValueError('physical input exceeds its byte bound')
        record = guards._record(descriptor)
        raw = os.pread(descriptor, limit + 1, 0)
        if (record['sha256'] != digest or len(raw) != record['size']
                or hashlib.sha256(raw).hexdigest() != digest
                or guards._record(descriptor) != record
                or guards._identity(path.lstat()) != tuple(record['identity'])):
            raise ValueError('physical input identity/content differs')
        return raw, record
    finally:
        os.close(descriptor)


def prepare(backup, backup_sha, receipts, receipts_sha):
    selected = profile()
    seed = selected['baseline_seed_execution']
    if backup_sha != seed['backup_sha256'] or receipts_sha != seed['receipts_sha256']:
        raise ValueError('physical baseline provenance differs from the frozen seed execution')
    raw, original = read_owned(backup, backup_sha, sqlite_legacy.MAX_DATABASE_BYTES)
    for suffix in ('-wal', '-journal'):
        sidecar = Path(str(backup) + suffix)
        if sidecar.is_symlink() or (sidecar.exists() and sidecar.stat().st_size):
            raise ValueError('physical backup must be stopped and checkpointed')
    packet_raw, _ = read_owned(receipts, receipts_sha, 4 * 1024 * 1024)
    packet = json.loads(packet_raw)
    assert packet['schema'] == 'durable-workflow.command-key-physical-seed.v1'
    assert packet['manifest_sha256'] == selected['producer_manifest_sha256']
    assert packet['expected_external_baseline'] == {
        'tag':selected['baseline_version'],'source_commit':selected['baseline_source'],
        'index_digest':selected['baseline_index']}
    assert len(packet['cases']) == 6
    assert len({row['workflow_id'] for row in packet['cases']}) == 6
    assert len({row['receipt']['command_id'] for row in packet['cases']}) == 6
    assert {(row['kind'],row['state']) for row in packet['cases']} == {
        (kind,state) for kind in ('signal','update') for state in ('open','completed','continued')}
    for row in packet['cases']:
        receipt = row['receipt']
        assert receipt['workflow_id'] == row['workflow_id']
        assert all(isinstance(receipt.get(key),str) and receipt[key] for key in ('command_id','run_id'))
        if row['kind'] == 'update':
            assert all(isinstance(receipt.get(key),str) and receipt[key] for key in ('update_id','accepted_at','update_name'))
            assert isinstance(row['original_body']['request_id'],str) and row['original_body']['request_id']
    return raw, original, packet, selected


def copy_backup(raw, destination):
    parent = destination.parent.stat()
    if (not destination.is_absolute() or destination.parent.resolve() != destination.parent
            or not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ValueError('physical copy needs an owned private parent')
    with open(os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as output:
        output.write(raw)


def _without_added_columns(table, before_sql, after_sql, value, declared):
    """Strip one table's declared appended columns after checking their defaults."""
    names = [name for name, _, _ in declared]
    assert value['columns'][-len(names):] == names, (table, value['columns'])
    for _, definition, _ in declared:
        assert after_sql.count(', '+definition) == 1, (table, definition)
        after_sql = after_sql.replace(', '+definition, '', 1)
    assert after_sql == before_sql, 'physical upgrade altered undeclared columns of '+table
    defaults = [default for _, _, default in declared]
    for row in value['rows']:
        assert row[-len(names):] == defaults, (table, row)
    return {'columns':value['columns'][:-len(names)],
            'rows':sorted((row[:-len(names)] for row in value['rows']), key=sqlite_legacy._json)}


def validate_upgrade(before, after, observed, started, finished, selected):
    assert observed == {'backend':'sqlite','from_schema_version':selected['schema_before'],
                        'schema_version':selected['schema_after'],'status':'upgraded'}, observed
    added_tables = set(selected['added_tables'])
    assert not added_tables & set(before['tables'])
    assert set(after['tables']) == set(before['tables']) | added_tables
    for table in added_tables:
        assert not after['tables'][table]['rows'], table
    expected = deepcopy(before)
    expected['schema'] = [list(row) for row in before['schema']]
    version = expected['tables']['dw_server_schema']['columns'].index('version')
    assert len(expected['tables']['dw_server_schema']['rows']) == 1
    assert expected['tables']['dw_server_schema']['rows'][0][version] == ['integer',selected['schema_before']]
    expected['tables']['dw_server_schema']['rows'][0][version] = ['integer',selected['schema_after']]
    journal = after['tables']['_sqlx_migrations']
    assert len(journal['rows']) == 1
    entry = dict(zip(journal['columns'],journal['rows'][0]))
    assert set(entry) == {'version','description','installed_on','success','checksum','execution_time'}
    assert entry['version'] == ['integer',selected['schema_after']] and entry['success'] == ['integer',1]
    assert entry['execution_time'] == ['integer',0]
    assert entry['description'] == ['text',selected['journal_description']]
    assert entry['checksum'] == ['blob',selected['journal_checksum_sha384']]
    assert entry['installed_on'][0] == 'text'
    installed = datetime.strptime(entry['installed_on'][1],'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
    assert started - 1 <= installed <= finished + 1
    expected['tables']['_sqlx_migrations'] = journal
    projected = deepcopy(after)
    projected['schema'] = [list(row) for row in after['schema']]
    for table in added_tables:
        del projected['tables'][table]
    declared_objects = selected['new_schema_objects']
    original = expected['schema']
    added = [row for row in projected['schema'] if row not in original]
    altered = selected['added_columns']
    before_sql = {row[1]:row[3] for row in original if row[0] == 'table'}
    for row in added:
        if row[0] == 'table' and row[1] in altered and [row[0],row[1],row[2],before_sql.get(row[1])] in original:
            continue  # An ALTER TABLE ... ADD COLUMN rewrites the stored CREATE statement.
        assert declared_objects.get(row[1]) == row[2], ('undeclared schema object', row[:3])
    assert {row[1] for row in added if row[1] not in altered} == set(declared_objects)
    for table, declared in altered.items():
        [after_row] = [row for row in projected['schema'] if row[0] == 'table' and row[1] == table]
        projected['tables'][table] = _without_added_columns(
            table, before_sql[table], after_row[3], projected['tables'][table], declared)
    projected['schema'] = [row for row in projected['schema'] if row in original] + [
        [row[0],row[1],row[2],before_sql[row[1]]] for row in added if row[1] in altered]
    projected['schema'].sort(key=lambda row:(row[0],row[1]))
    assert projected == expected, 'physical upgrade altered undeclared business rows/catalog'


def validate_original_commands(value, packet):
    commands = value['tables']['workflow_commands']
    columns = commands['columns']
    for case in packet['cases']:
        original = case['receipt']
        matches = [dict(zip(columns,row)) for row in commands['rows']
                   if row[columns.index('id')] == ['text',original['command_id']]]
        assert len(matches) == 1
        row = matches[0]
        assert row['command_type'] == ['text',case['kind']]
        assert row['workflow_instance_id'] == ['text',case['workflow_id']]
        assert row['workflow_run_id'] == ['text',original['run_id']]
        expected_key = ['null'] if case['kind'] == 'signal' else ['text',case['original_body']['request_id']]
        assert row['request_id'] == expected_key


def readbacks(packet, invoke, snapshot, work):
    before = snapshot()
    validate_original_commands(before,packet)
    assert not before['tables']['dw_command_intents']['rows']
    outcomes = []
    for case in packet['cases']:
        original = case['receipt']
        history_args = {'op':'history','workflow_id':case['workflow_id'],'run_id':original['run_id']}
        history = invoke(history_args)
        assert history['kind'] == 'ok'
        events = history['value']['history_events']
        event_type = 'SignalReceived' if case['kind'] == 'signal' else 'UpdateAccepted'
        expected_count = sum(event['event_type'] == event_type for event in case['history']['events'])
        assert expected_count > 0 and sum(event['type'] == event_type for event in events) == expected_count
        if case['kind'] == 'signal':
            outcomes.append({'kind':'signal','state':case['state'],'scope':'NULL-key compatibility only',
                             'original_command_id':original['command_id'],'original_run_id':original['run_id'],
                             'history_sha256':sqlite_legacy.fingerprint(events)})
            continue
        request = {'op':'update_raw','workflow_id':case['workflow_id'],'name':original['update_name'],
                   'request_id':case['original_body']['request_id'],'input':[999]}
        fields = ('command_id','run_id','update_id','accepted_at')
        with ThreadPoolExecutor(max_workers=4) as pool:
            replies = list(pool.map(lambda _:invoke(request),range(4)))
        replies.append(invoke(dict(request,run_id=original['run_id'])))
        for reply in replies:
            assert reply['kind'] == 'ok' and reply['value']['status'] in (200,202), reply
            body = reply['value']['body']
            assert body['retained'] is True and body['intent_verified'] is False
            assert body['request_id'] == request['request_id']
            assert all(body[field] == original[field] for field in fields)
            if case['state'] != 'open':assert body['update_status'] == 'completed'
        high = invoke(dict(request,op='update'))
        if case['state'] == 'open':
            assert high['kind'] == 'pending' and high['receipt']['retained'] is True
            assert high['receipt']['intent_verified'] is False
            assert all(high['receipt'][field] == original[field] for field in fields)
        else:
            assert high['kind'] == 'ok' and high['value'] is None
        for changed in (dict(request,name='other'),dict(request,run_id='00000000000000000000000000')):
            refused = invoke(changed)
            assert refused['kind'] == 'ok' and refused['value']['status'] == 409
            assert refused['value']['body']['reason'] == 'request_id_conflict'
            assert refused['value']['body']['retained_command_id'] == original['command_id']
        assert invoke(history_args)['value']['history_events'] == events
        outcomes.append({'kind':'update','state':case['state'],'outcome':'pass',
                         'original':{field:original[field] for field in fields},
                         'returned':{field:replies[0]['value']['body'][field] for field in fields},
                         'retained':True,'intent_verified':False,'concurrency':4})
    after = snapshot()
    assert after == before, 'physical SDK request window altered full logical rows/schema'
    for label,value in (('before',before),('after',after)):
        with open(os.open(work/('physical-'+label+'-snapshot.json'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as output:
            json.dump(value,output,sort_keys=True)
    return {'cell':'physical-sqlite-upgrade-sdk-readback-only','outcome':'pass',
            'cases':outcomes,'request_window_sha256':sqlite_legacy.fingerprint(before)}
