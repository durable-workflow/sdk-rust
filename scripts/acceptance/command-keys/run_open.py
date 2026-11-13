#!/usr/bin/env python3
"""One named command-key acceptance cell, with separate evidence scopes."""
import argparse
import re
import tomllib
import base64
import concurrent.futures
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import time

SOURCE = Path(__file__).resolve().parent
sys.path.insert(0, str(SOURCE))
import sqlite_legacy as legacy
import execution_guards as guards
import response_loss
import physical_upgrade



def capture(*args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def ambiguity_receipt(result, request_id, expected):
    assert result['kind'] == 'ok' and result['value']['status'] == expected['status'], result
    body = result['value']['body']
    assert body['reason'] == expected['reason'], body
    if 'request_id' in body:
        assert body['request_id'] == request_id, body
    for field in ('retained', 'intent_verified', 'retryable'):
        assert body[field] is expected[field], body
    for field in ('command_id', 'run_id', 'update_id', 'accepted_at', 'retained_command_id', 'receipt'):
        assert body.get(field) is None, body
    return body


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', choices=['open', 'completed', 'continued'], default='open')
    parser.add_argument('--scenario', choices=['readback', 'two-rows', 'legacy-token', 'modern-raw', 'case-only'], default='readback')
    parser.add_argument('--response-loss-fixture', type=Path,
                        help='accepted shared proxy file; selects the separate postcommit-loss cell')
    parser.add_argument('--physical-backup',type=Path)
    parser.add_argument('--physical-backup-sha256')
    parser.add_argument('--physical-receipts',type=Path)
    parser.add_argument('--physical-receipts-sha256')
    parser.add_argument('--physical-source-commit',help='accepted producer seed-source commit from its owner handoff')
    parser.add_argument('--work-dir',type=Path,required=True)
    parser.add_argument('--consumer-binary',type=Path,required=True)
    parser.add_argument('--consumer-lock',type=Path,required=True)
    parser.add_argument('--build-receipt',type=Path,required=True)
    parser.add_argument('--sdk-version',required=True)
    parser.add_argument('--sdk-sha256',required=True)
    parser.add_argument('--sdk-source',required=True)
    parser.add_argument('--server-index',required=True)
    parser.add_argument('--server-version',required=True)
    parser.add_argument('--server-source',required=True)
    options=parser.parse_args()
    physical_args = (options.physical_backup, options.physical_backup_sha256, options.physical_receipts,
                     options.physical_receipts_sha256, options.physical_source_commit)
    physical_mode = any(value is not None for value in physical_args)
    if physical_mode and (not all(physical_args) or options.state != 'open' or options.scenario != 'readback'
                          or options.response_loss_fixture is not None
                          or not re.fullmatch('[0-9a-f]{40}',options.physical_source_commit)):
        parser.error('physical upgrade needs all exact input identities and its own cell')
    if physical_mode:
        physical_upgrade.require_handoff(options.physical_source_commit)
    if options.state != 'open' and options.scenario != 'readback':
        parser.error('ambiguity/case scenarios require their own open-state cell')
    if options.response_loss_fixture is not None and (options.state != 'open' or options.scenario != 'readback'):
        parser.error('postcommit loss uses its own open-state admissions without a legacy transform')
    if not __debug__:
        parser.error('run conformance assertions without Python optimization')
    if (not re.fullmatch(r'sha256:[0-9a-f]{64}',options.server_index)
            or not re.fullmatch(r'[0-9a-f]{64}',options.sdk_sha256)
            or not re.fullmatch(r'[0-9a-f]{40}',options.sdk_source)
            or not re.fullmatch(r'[0-9a-f]{40}',options.server_source)):
        parser.error('exact immutable artifact identities are required')
    if not options.work_dir.is_absolute() or options.work_dir.exists():
        parser.error('work directory must be a new absolute owned path')
    if not options.consumer_binary.is_absolute():
        parser.error('consumer executable path must be absolute')
    lock=tomllib.loads(options.consumer_lock.read_text())
    packages=[p for p in lock['package'] if p['name']=='durable-workflow']
    assert len(packages)==1 and packages[0]['version']==options.sdk_version
    assert packages[0]['checksum']==options.sdk_sha256
    assert packages[0]['source']=='registry+https://github.com/rust-lang/crates.io-index'
    build, build_receipt_sha = guards.verify_build_receipt(
        options.build_receipt, SOURCE, options.consumer_binary, options.consumer_lock,
        sdk_version=options.sdk_version, sdk_sha256=options.sdk_sha256, sdk_source=options.sdk_source)
    if options.response_loss_fixture is not None:
        response_loss.fixture(options.response_loss_fixture)  # Refuse changed source before effects.
    if physical_mode:
        physical_raw, physical_original, physical_packet, physical_profile = physical_upgrade.prepare(
            options.physical_backup,options.physical_backup_sha256,
            options.physical_receipts,options.physical_receipts_sha256)
        if (options.server_source != physical_profile['server_source']
                or options.server_index != physical_profile['server_index']
                or options.server_version != physical_profile['server_version']):
            parser.error('physical profile does not bind this exact published producer tuple')
    image='ghcr.io/durable-workflow/server@'+options.server_index
    info=json.loads(capture('docker','image','inspect',image))[0]
    assert info['Config']['Labels']['org.opencontainers.image.revision']==options.server_source
    assert info['Config']['Labels']['dev.durable-workflow.release.tag']==options.server_version
    assert info['Os']=='linux' and info['Architecture']=='amd64', 'this runner cell is qualified for linux/amd64 only'
    assert options.consumer_binary.is_file()
    document=legacy.manifest()
    started_at = datetime.now(timezone.utc).isoformat()
    work = options.work_dir
    work.mkdir(mode=0o700)
    database = work / 'db'; database.mkdir(mode=0o700)
    envfile = work / 'runtime.env'
    token = 'synthetic-' + secrets.token_hex(24)
    key = 'base64:' + base64.b64encode(secrets.token_bytes(32)).decode()
    with open(os.open(envfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as file:
        file.write('DW_AUTH_TOKEN='+token+'\nDW_SERVER_KEY='+key+'\n')
    name = ('sdk-rust-216-physical-' if physical_mode else 'sdk-rust-216-legacy-'+options.state+'-') + secrets.token_hex(5)
    owned_id = None
    binary = None
    worker_pool = None
    origin = None

    def start(filename):
        nonlocal origin,owned_id
        owned_id = capture('docker','create','--init','--read-only','--name',name,
            '--cpus','1','--memory','1g','--pids-limit','256','--cap-drop','ALL',
            '--security-opt','no-new-privileges','--tmpfs','/tmp:mode=1777,size=128m',
            '--tmpfs','/app/storage:uid=1000,gid=1000,mode=0700,size=64m',
            '--tmpfs','/app/database:uid=1000,gid=1000,mode=0700,size=16m',
            '-v',str(database)+':/cohort:rw','-p','127.0.0.1::8080','--env-file',str(envfile),
            '-e','DW_BIND_ADDRESS=0.0.0.0:8080','-e','DW_RUST_MAINTENANCE_LOOP=0',
            '-e','DW_V2_UPDATE_WAIT_COMPLETION_TIMEOUT_SECONDS=1',
            '-e','DB_CONNECTION=sqlite','-e','DB_DATABASE=/cohort/'+filename,image)
        capture('docker','start',owned_id)
        address=capture('docker','port',owned_id,'8080/tcp')
        host,port=address.split(':');assert host=='127.0.0.1' and 1<=int(port)<=65535
        origin='http://'+address
        end=time.monotonic()+60
        while True:
            try:
                guards.probe_json(origin, '/api/ready', token)
                info = guards.probe_json(origin, '/api/cluster/info', token)
                assert info['version']==options.server_version
                assert info['capabilities']['command_request_keys'] is True
                break
            except OSError:
                if time.monotonic()>end:raise RuntimeError('synthetic runtime did not become ready') from None
                time.sleep(.1)

    def stop():
        nonlocal owned_id
        capture('docker','stop','--time','10',owned_id)
        assert capture('docker','inspect','--format','{{.State.Running}}',owned_id)=='false'
        capture('docker','rm','-v',owned_id)
        owned_id=None

    def invoke(args, base=None):
        env={'PATH':os.environ['PATH'],'TMPDIR':str(work),
             'DURABLE_WORKFLOW_LOCAL_ISOLATED':'1','DURABLE_WORKFLOW_SERVER_URL':base or origin,
             'DURABLE_WORKFLOW_AUTH_TOKEN':token}
        result=binary.run(input=json.dumps(args), env=env, timeout=20)
        return json.loads(result.stdout)

    def ok(args):
        result=invoke(args);assert result['kind']=='ok',result
        return result['value']

    def snapshot(path):
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as connection:
            connection.execute('BEGIN')
            return legacy.snapshot(connection)

    try:
        binary = guards.BoundExecutable(options.consumer_binary, build['binary'])
        if physical_mode:
            physical_path=database/'physical.sqlite'
            physical_upgrade.copy_backup(physical_raw,physical_path)
            del physical_raw
            original_snapshot=snapshot(physical_path)
            physical_upgrade.validate_original_commands(original_snapshot,physical_packet)
            upgrade_started=time.time()
            owned_id=capture('docker','create','--init','--read-only','--name',name+'-upgrade',
                '--cpus','1','--memory','1g','--pids-limit','256','--network','none',
                '--cap-drop','ALL','--security-opt','no-new-privileges',
                '--tmpfs','/tmp:mode=1777,size=128m',
                '--tmpfs','/app/storage:uid=1000,gid=1000,mode=0700,size=64m',
                '--tmpfs','/app/database:uid=1000,gid=1000,mode=0700,size=16m',
                '-v',str(database)+':/cohort:rw','--env-file',str(envfile),
                '-e','DB_CONNECTION=sqlite','-e','DB_DATABASE=/cohort/physical.sqlite',image,'schema-upgrade')
            capture('docker','start',owned_id)
            exit_code=capture('docker','wait',owned_id)
            upgrade_finished=time.time()
            upgrade_log=capture('docker','logs',owned_id)
            with open(os.open(work/'schema-upgrade.log',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as output:
                output.write(upgrade_log+'\n')
            assert exit_code == '0', 'physical schema-upgrade exited unsuccessfully'
            upgraded=snapshot(physical_path)
            physical_upgrade.validate_upgrade(original_snapshot,upgraded,json.loads(upgrade_log),
                                               upgrade_started,upgrade_finished,physical_profile)
            physical_upgrade.validate_original_commands(upgraded,physical_packet)
            capture('docker','rm','-v',owned_id);owned_id=None
            start(physical_path.name)
            cell=physical_upgrade.readbacks(physical_packet,invoke,lambda:snapshot(physical_path),work)
            stop()
            _, rechecked = physical_upgrade.read_owned(options.physical_backup,options.physical_backup_sha256,
                                                       legacy.MAX_DATABASE_BYTES)
            assert rechecked == physical_original, 'original baseline changed during the owned-copy experiment'
            binary.check()
            cell.update(started_at=started_at,finished_at=datetime.now(timezone.utc).isoformat(),
                baseline_sha256=options.physical_backup_sha256,seed_receipts_sha256=options.physical_receipts_sha256,
                producer_seed_source=options.physical_source_commit,
                profile_sha256=physical_upgrade.PROFILE_SHA256,
                server_source=options.server_source,server_index=options.server_index,
                server_version=options.server_version,server_platform='linux/amd64',
                sdk_version=options.sdk_version,sdk_sha256=options.sdk_sha256,sdk_source=options.sdk_source,
                runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                physical_adapter_sha256=hashlib.sha256((SOURCE/'physical_upgrade.py').read_bytes()).hexdigest(),
                binary_sha256=binary.record['sha256'],build_receipt_sha256=build_receipt_sha,
                build_subject=build['subject'],build_command=build['build_command'])
            print(json.dumps(cell),flush=True)
            return
        start('source.sqlite')
        queue='legacy-'+options.state+'-'+secrets.token_hex(5)
        ok({'op':'register','queue':queue,'null_update':True})
        workflow_type = 'keys.continue' if options.state == 'continued' else 'keys.wait'
        created=ok({'op':'start','workflow_type':workflow_type,'workflow_id':queue,'queue':queue,'input':[0]})
        if options.response_loss_fixture is not None:
            cell = response_loss.run(options.response_loss_fixture, origin, queue, created['run_id'],
                                     invoke, lambda:snapshot(database/'source.sqlite'), work)
            stop()
            binary.check()
            cell.update(outcome='pass', started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                server_source=options.server_source,server_index=options.server_index,
                server_version=options.server_version,server_platform='linux/amd64',
                sdk_version=options.sdk_version,sdk_sha256=options.sdk_sha256,sdk_source=options.sdk_source,
                runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                loss_adapter_sha256=hashlib.sha256((SOURCE/'response_loss.py').read_bytes()).hexdigest(),
                binary_sha256=binary.record['sha256'],build_receipt_sha256=build_receipt_sha,
                build_subject=build['subject'],build_command=build['build_command'])
            print(json.dumps(cell),flush=True)
            return
        seed={};selections=[]
        caller_key = document['keys']['case_exact'] if options.scenario == 'case-only' else document['keys']['original']
        for kind,operation in [('signal','touch'),('update','mutate')]:
            admission={'op':kind+'_raw','workflow_id':queue,'name':operation,'input':[document['legacy_readback']['original_input']],'request_id':caller_key}
            receipt=ok(admission);assert receipt['status']==202,receipt
            body=receipt['body'];assert body['command_id'] and body['run_id']==created['run_id']
            seed[kind]=(admission,body)
            selected = {'command_id':body['command_id'],'command_type':kind,
                'workflow_instance_id':queue,'workflow_run_id':body['run_id'],
                'caller_key':caller_key,'legacy_request_id':caller_key}
            if options.scenario == 'two-rows':
                second = ok(dict(admission, input=[2], request_id=document['keys']['second']))
                assert second['status'] == 202 and second['body']['run_id'] == created['run_id']
                assert second['body']['command_id'] and second['body']['command_id'] != body['command_id']
                selected.update(command_id=second['body']['command_id'], caller_key=document['keys']['second'])
            elif options.scenario == 'legacy-token':
                selected['legacy_request_id'] = document['keys']['tokens'][kind]
            if options.scenario != 'modern-raw':
                selections.append(selected)
        successor_run_id = None
        if options.state != 'open':
            # Use the same held descriptor and pre/post checks for the worker.
            # Its adapter lifetime and the outer invocation timeout are bounded.
            worker_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            worker = worker_pool.submit(ok, {'op':'worker', 'queue':queue,
                                            'null_update':True, 'lifetime':8})
            end=time.monotonic()+20
            while True:
                outcome=ok({'op':'outcome','workflow_id':queue,'update_id':seed['update'][1]['update_id']})
                if outcome['update_status']=='completed':break
                if time.monotonic()>end:raise AssertionError('update did not complete before close')
                time.sleep(.05)
            assert ok(dict(seed['update'][0],op='update')) is None
            ok({'op':'signal','workflow_id':queue,'name':'finish','input':[],'request_id':'close-setup'})
            end=time.monotonic()+20
            while True:
                current=ok({'op':'describe','workflow_id':queue})
                if options.state=='completed' and current['status']=='completed':break
                if options.state=='continued' and current['run_id']!=created['run_id']:break
                if time.monotonic()>end:raise AssertionError('workflow did not close or continue')
                time.sleep(.05)
            assert worker.result(timeout=20)['stopped'] is True
            worker_pool.shutdown(wait=True); worker_pool=None
            if options.state == 'continued':
                successor_run_id = current['run_id']
                assert isinstance(successor_run_id,str) and successor_run_id
                assert successor_run_id != created['run_id']
        stop()
        source=database/'source.sqlite'; copied=database/'legacy.sqlite'
        with closing(sqlite3.connect(source)) as connection:
            assert connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]==0
            columns=[row[1] for row in connection.execute('PRAGMA table_info(workflow_commands)')]
            assert 'command_type' in columns
            catalog=connection.execute("SELECT sql FROM sqlite_schema WHERE name='workflow_commands'").fetchone()[0]
        (work/'producer-command-catalog.sql').write_text(catalog+';\n')
        if options.scenario == 'modern-raw':
            # The accepted vector explicitly preserves the modern admission.
            # No synthetic mutation or physical-upgrade authority is claimed.
            copied = source
            digest = legacy.fingerprint(snapshot(source))
            transform = {'cell':'modern-raw-unchanged', 'transformed':0,
                         'manifest_sha256':legacy.MANIFEST_SHA256,
                         'before_sha256':digest, 'after_sha256':digest}
        else:
            transform=legacy.transform_copy(source,copied,selections,stopped=True)
        print(json.dumps({'stage':('real-api-modern-admission-retained' if options.scenario == 'modern-raw' else 'real-api-copy-transformed'),'catalog_column':'command_type',
                          'transform':transform}),flush=True)
        start(copied.name)
        before=snapshot(copied)
        guards.verify_restart_baseline(before, transform['after_sha256'])
        for kind,(admission,original) in seed.items():
            retry=dict(admission,input=[document['legacy_readback']['retry_input']])
            if options.scenario in ('two-rows', 'legacy-token', 'modern-raw'):
                if options.scenario == 'modern-raw':
                    retry['request_id'] = document['keys']['tokens'][kind]
                with concurrent.futures.ThreadPoolExecutor(max_workers=document['legacy_readback']['concurrency']) as pool:
                    replies=list(pool.map(lambda _:invoke(retry),range(document['legacy_readback']['concurrency'])))
                for reply in replies:
                    ambiguity_receipt(reply, retry['request_id'], document['ambiguity_expected'])
                high = invoke(dict(retry,op=kind))
                assert high['kind'] == 'http' and high['status'] == 409, high
                assert high['request_id'] == high['body'].get('request_id'), high
                ambiguity_receipt({'kind':'ok','value':{'status':high['status'],'body':high['body']}},
                                  retry['request_id'], document['ambiguity_expected'])
                print(json.dumps({'stage':'ambiguity-readback','shape':options.scenario,
                    'command_type':kind,'outcome':'pass','selected_receipt':False,
                    'retained':True,'intent_verified':False,'retryable':False,
                    'concurrency':document['legacy_readback']['concurrency']}),flush=True)
                continue
            fields = ['command_id', 'run_id'] + (['update_id', 'accepted_at'] if kind == 'update' else [])
            assert all(original[field] for field in fields)
            with concurrent.futures.ThreadPoolExecutor(max_workers=document['legacy_readback']['concurrency']) as pool:
                replies=list(pool.map(lambda _:ok(retry),range(document['legacy_readback']['concurrency'])))
            for result in replies:
                assert result['status'] in document['legacy_readback']['expected']['success_statuses'][kind],result
                receipt=result['body']
                assert receipt['retained'] is True and receipt['intent_verified'] is False,receipt
                for field in fields:
                    assert receipt[field]==original[field],field
                if kind == 'update' and options.state != 'open':
                    assert receipt['update_status'] == 'completed'
            pinned=ok(dict(retry,run_id=original['run_id']))['body']
            assert pinned['retained'] is True and pinned['intent_verified'] is False
            assert all(pinned[field] == original[field] for field in fields)
            for changed in [dict(retry,name='other'),dict(retry,run_id='00000000000000000000000000')]:
                refusal=invoke(changed)
                assert refusal['kind']=='ok' and refusal['value']['status']==409,refusal
                assert refusal['value']['body']['reason']=='request_id_conflict',refusal
                assert refusal['value']['body']['retained_command_id']==original['command_id']
            high=invoke(dict(retry,op=kind))
            if kind=='signal':
                assert high['kind']=='ok' and high['value']['intent_verified'] is False,high
                assert high['value']['retained'] is True
                assert all(high['value'][field] == original[field] for field in fields)
            elif options.state == 'open':
                assert high['kind']=='pending' and high['receipt']['intent_verified'] is False,high
                assert high['receipt']['retained'] is True
                assert all(high['receipt'][field] == original[field] for field in fields)
            else:
                assert high['kind']=='ok' and high['value'] is None,high
            print(json.dumps({'stage':options.state+'-readback','command_type':kind,'outcome':'pass',
                'original':{field:original[field] for field in fields},
                'returned':{field:replies[0]['body'][field] for field in fields},
                'retained':True,'intent_verified':False,
                'concurrency':document['legacy_readback']['concurrency']}),flush=True)
        after=snapshot(copied)
        assert after==before,'readbacks or conflicts changed complete logical database rows/schema'
        case_admissions = []
        if options.scenario == 'case-only':
            # SQLite's binary comparison admits the different byte string.
            # Zero-write proof above applies only to exact-byte legacy retries.
            for kind,(admission,original) in seed.items():
                fresh = ok(dict(admission, request_id=document['keys']['case_other']))
                assert fresh['status'] == document['case_only']['sqlite_postgresql_binary']['status'], fresh
                body = fresh['body']
                assert body['command_id'] and body['command_id'] != original['command_id'], body
                assert body['run_id'] == original['run_id'] and body['request_id'] == document['keys']['case_other']
                case_admissions.append({'command_type':kind,'original_command_id':original['command_id'],
                    'distinct_command_id':body['command_id'],'run_id':body['run_id']})
            admitted=snapshot(copied)
            assert admitted != after
            commands = after['tables']['workflow_commands']
            assert all(row in admitted['tables']['workflow_commands']['rows'] for row in commands['rows'])
            with open(os.open(work/'case-admitted-snapshot.json',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600),'w') as file:
                json.dump(admitted,file,sort_keys=True)
        for filename, value in [('before-snapshot.json', before), ('after-snapshot.json', after)]:
            with open(os.open(work / filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as file:
                json.dump(value, file, sort_keys=True)
        stop()
        binary.check()
        cell = options.state if options.scenario == 'readback' else options.scenario
        print(json.dumps({'cell':'synthetic-legacy-'+cell+'-only','outcome':'pass',
            'state':options.state,'successor_run_id':successor_run_id,
            'scenario':options.scenario,'case_distinct_admissions':case_admissions,
            'zero_write_scope':'before/after exact legacy readbacks or ambiguity only; excludes case-distinct admissions',
            'started_at':started_at,'finished_at':datetime.now(timezone.utc).isoformat(),
            'zero_write_sha256':legacy.fingerprint(before),'server_source':options.server_source,
            'server_index':options.server_index,'server_version':options.server_version,
            'server_platform':'linux/amd64',
            'sdk_version':options.sdk_version,'sdk_sha256':options.sdk_sha256,'sdk_source':options.sdk_source,'manifest_sha256':legacy.MANIFEST_SHA256,
            'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'binary_sha256':binary.record['sha256'],'build_receipt_sha256':build_receipt_sha,
            'build_subject':build['subject'],'build_command':build['build_command'],
            'helper_sha256':hashlib.sha256((SOURCE/'sqlite_legacy.py').read_bytes()).hexdigest()}),flush=True)
    finally:
        try:
            if worker_pool is not None:
                worker_pool.shutdown(wait=True, cancel_futures=True)
            if owned_id is not None:
                subprocess.run(['docker','rm','-fv',owned_id],check=True,stdout=subprocess.DEVNULL)
            envfile.unlink(missing_ok=True)
            for path in database.iterdir():path.unlink()
            database.rmdir()
            print(json.dumps({'cleanup':'pass','scope':'owned container/volumes/database/env removed; private snapshots and sanitized receipt/catalog retained for seven days'}),flush=True)
        finally:
            if binary is not None:
                binary.close()


if __name__=='__main__':main()
