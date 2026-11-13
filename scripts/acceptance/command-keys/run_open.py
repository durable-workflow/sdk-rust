#!/usr/bin/env python3
"""One synthetic open-state legacy cell; no physical upgrade or response-loss authority."""
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
import urllib.request

SOURCE = Path(__file__).resolve().parent
sys.path.insert(0, str(SOURCE))
import sqlite_legacy as legacy



def capture(*args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work-dir',type=Path,required=True)
    parser.add_argument('--consumer-binary',type=Path,required=True)
    parser.add_argument('--consumer-lock',type=Path,required=True)
    parser.add_argument('--sdk-version',required=True)
    parser.add_argument('--sdk-sha256',required=True)
    parser.add_argument('--sdk-source',required=True)
    parser.add_argument('--server-index',required=True)
    parser.add_argument('--server-version',required=True)
    parser.add_argument('--server-source',required=True)
    options=parser.parse_args()
    if not __debug__:
        parser.error('run conformance assertions without Python optimization')
    if (not re.fullmatch(r'sha256:[0-9a-f]{64}',options.server_index)
            or not re.fullmatch(r'[0-9a-f]{64}',options.sdk_sha256)
            or not re.fullmatch(r'[0-9a-f]{40}',options.sdk_source)
            or not re.fullmatch(r'[0-9a-f]{40}',options.server_source)):
        parser.error('exact immutable artifact identities are required')
    if not options.work_dir.is_absolute() or options.work_dir.exists():
        parser.error('work directory must be a new absolute owned path')
    options.consumer_binary = options.consumer_binary.resolve(strict=True)
    lock=tomllib.loads(options.consumer_lock.read_text())
    packages=[p for p in lock['package'] if p['name']=='durable-workflow']
    assert len(packages)==1 and packages[0]['version']==options.sdk_version
    assert packages[0]['checksum']==options.sdk_sha256
    assert packages[0]['source']=='registry+https://github.com/rust-lang/crates.io-index'
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
    name = 'sdk-rust-216-legacy-open-' + secrets.token_hex(5)
    owned_id = None
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
                request=urllib.request.Request(origin+'/api/ready',headers={'Authorization':'Bearer '+token})
                with urllib.request.urlopen(request,timeout=2) as response: response.read(4096)
                request=urllib.request.Request(origin+'/api/cluster/info',headers={'Authorization':'Bearer '+token})
                with urllib.request.urlopen(request,timeout=2) as response:
                    info=json.loads(response.read(1024*1024))
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

    def invoke(args):
        env={'PATH':os.environ['PATH'],'TMPDIR':str(work),
             'DURABLE_WORKFLOW_LOCAL_ISOLATED':'1','DURABLE_WORKFLOW_SERVER_URL':origin,
             'DURABLE_WORKFLOW_AUTH_TOKEN':token}
        result=subprocess.run([str(options.consumer_binary)],input=json.dumps(args),text=True,capture_output=True,
                              env=env,timeout=20,check=True)
        return json.loads(result.stdout)

    def ok(args):
        result=invoke(args);assert result['kind']=='ok',result
        return result['value']

    def snapshot(path):
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as connection:
            connection.execute('BEGIN')
            return legacy.snapshot(connection)

    try:
        start('source.sqlite')
        queue='legacy-open-'+secrets.token_hex(5)
        ok({'op':'register','queue':queue,'null_update':True})
        created=ok({'op':'start','workflow_type':'keys.wait','workflow_id':queue,'queue':queue,'input':[0]})
        seed={};selections=[]
        for kind,operation in [('signal','touch'),('update','mutate')]:
            admission={'op':kind+'_raw','workflow_id':queue,'name':operation,'input':[document['legacy_readback']['original_input']],'request_id':document['keys']['original']}
            receipt=ok(admission);assert receipt['status']==202,receipt
            body=receipt['body'];assert body['command_id'] and body['run_id']==created['run_id']
            seed[kind]=(admission,body)
            selections.append({'command_id':body['command_id'],'command_type':kind,
                'workflow_instance_id':queue,'workflow_run_id':body['run_id'],
                'caller_key':document['keys']['original'],'legacy_request_id':document['keys']['original']})
        stop()
        source=database/'source.sqlite'; copied=database/'legacy.sqlite'
        with closing(sqlite3.connect(source)) as connection:
            assert connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]==0
            columns=[row[1] for row in connection.execute('PRAGMA table_info(workflow_commands)')]
            assert 'command_type' in columns
            catalog=connection.execute("SELECT sql FROM sqlite_schema WHERE name='workflow_commands'").fetchone()[0]
        (work/'producer-command-catalog.sql').write_text(catalog+';\n')
        transform=legacy.transform_copy(source,copied,selections,stopped=True)
        print(json.dumps({'stage':'real-api-copy-transformed','catalog_column':'command_type',
                          'transform':transform}),flush=True)
        start('legacy.sqlite')
        before=snapshot(copied)
        for kind,(admission,original) in seed.items():
            retry=dict(admission,input=[document['legacy_readback']['retry_input']])
            with concurrent.futures.ThreadPoolExecutor(max_workers=document['legacy_readback']['concurrency']) as pool:
                replies=list(pool.map(lambda _:ok(retry),range(document['legacy_readback']['concurrency'])))
            for result in replies:
                assert result['status']==202,result
                receipt=result['body']
                assert receipt['retained'] is True and receipt['intent_verified'] is False,receipt
                for field in ['command_id','run_id']+(['update_id','accepted_at'] if kind=='update' else []):
                    assert receipt[field]==original[field],field
            pinned=ok(dict(retry,run_id=original['run_id']))['body']
            assert pinned['command_id']==original['command_id'] and pinned['intent_verified'] is False
            for changed in [dict(retry,name='other'),dict(retry,run_id='00000000000000000000000000')]:
                refusal=invoke(changed)
                assert refusal['kind']=='ok' and refusal['value']['status']==409,refusal
                assert refusal['value']['body']['reason']=='request_id_conflict',refusal
            high=invoke(dict(retry,op=kind))
            if kind=='signal':
                assert high['kind']=='ok' and high['value']['intent_verified'] is False,high
            else:
                assert high['kind']=='pending' and high['receipt']['intent_verified'] is False,high
            fields = ['command_id', 'run_id'] + (['update_id', 'accepted_at'] if kind == 'update' else [])
            print(json.dumps({'stage':'open-readback','command_type':kind,'outcome':'pass',
                'original':{field:original[field] for field in fields},
                'returned':{field:replies[0]['body'][field] for field in fields},
                'retained':True,'intent_verified':False,
                'concurrency':document['legacy_readback']['concurrency']}),flush=True)
        after=snapshot(copied)
        assert after==before,'readbacks or conflicts changed complete logical database rows/schema'
        for filename, value in [('before-snapshot.json', before), ('after-snapshot.json', after)]:
            with open(os.open(work / filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as file:
                json.dump(value, file, sort_keys=True)
        stop()
        print(json.dumps({'cell':'synthetic-legacy-open-only','outcome':'pass',
            'started_at':started_at,'finished_at':datetime.now(timezone.utc).isoformat(),
            'zero_write_sha256':legacy.fingerprint(before),'server_source':options.server_source,
            'server_index':options.server_index,'server_version':options.server_version,
            'server_platform':'linux/amd64',
            'sdk_version':options.sdk_version,'sdk_sha256':options.sdk_sha256,'sdk_source':options.sdk_source,'manifest_sha256':legacy.MANIFEST_SHA256,
            'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'helper_sha256':hashlib.sha256((SOURCE/'sqlite_legacy.py').read_bytes()).hexdigest()}),flush=True)
    finally:
        if owned_id is not None:
            subprocess.run(['docker','rm','-fv',owned_id],check=True,stdout=subprocess.DEVNULL)
        envfile.unlink(missing_ok=True)
        for path in database.iterdir():path.unlink()
        database.rmdir()
        print(json.dumps({'cleanup':'pass','scope':'owned container/volumes/database/env removed; private snapshots and sanitized receipt/catalog retained for seven days'}),flush=True)


if __name__=='__main__':main()
