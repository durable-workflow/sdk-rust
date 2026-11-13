#!/usr/bin/env python3
"""Run the metrics contract test against one disposable published native image."""
import base64
import json
import os
import pathlib
import secrets
import subprocess
import tempfile
import time
import urllib.request


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


SERVER_IMAGE = 'ghcr.io/durable-workflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e'
SERVER_VERSION = '3.8.0'
SERVER_REVISION = '28f084513235f6647e740c4b3d9e3aace7a323b7'


def loopback_origin(address):
    host, separator, port = address.partition(':')
    if host != '127.0.0.1' or not separator or not port.isascii() or not port.isdecimal():
        raise ValueError('the fixture must have exactly one IPv4 loopback binding')
    if not 1 <= int(port) <= 65535:
        raise ValueError('the fixture port must be valid')
    return f'http://127.0.0.1:{int(port)}'


def main():
    subprocess.run(['docker', 'pull', SERVER_IMAGE], check=True)
    image = json.loads(capture('docker', 'image', 'inspect', SERVER_IMAGE))[0]
    labels = image['Config']['Labels']
    if labels.get('org.opencontainers.image.revision') != SERVER_REVISION:
        raise RuntimeError('published image revision mismatch')
    if labels.get('dev.durable-workflow.release.tag') != SERVER_VERSION:
        raise RuntimeError('published image release mismatch')

    name = 'sdk-rust-metrics-' + secrets.token_hex(6)
    token = 'fixture-' + secrets.token_hex(24)
    key = 'base64:' + base64.b64encode(secrets.token_bytes(32)).decode()
    with tempfile.TemporaryDirectory(prefix='sdk-rust-metrics-') as directory:
        env_file = pathlib.Path(directory) / 'runtime.env'
        with open(os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
            stream.write(f'DW_AUTH_TOKEN={token}\nDW_SERVER_KEY={key}\n')
        try:
            capture('docker', 'run', '-d', '--init', '--read-only', '--name', name,
                    '--cpus', '1', '--memory', '1g', '--pids-limit', '256',
                    '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                    '--tmpfs', '/tmp:mode=1777,size=128m',
                    '--tmpfs', '/app/storage:uid=1000,gid=1000,mode=0700,size=64m',
                    '-p', '127.0.0.1::8080', '--env-file', str(env_file),
                    '-e', 'DW_BIND_ADDRESS=0.0.0.0:8080',
                    '-e', 'DW_RUST_MAINTENANCE_LOOP=1',
                    '-e', 'DB_CONNECTION=sqlite', '-e', 'DB_DATABASE=/tmp/metrics.sqlite',
                    SERVER_IMAGE)
            address = capture('docker', 'port', name, '8080/tcp')
            # This address is a disposable loopback-only fixture, never a remote
            # endpoint or production credential. Native Server speaks HTTP here.
            origin = loopback_origin(address)

            def get(path):
                request = urllib.request.Request(origin + '/api' + path, headers={
                    'Authorization': 'Bearer ' + token, 'X-Namespace': 'default'})
                with urllib.request.urlopen(request, timeout=2) as response:
                    return json.load(response)

            deadline = time.monotonic() + 60
            while True:
                try:
                    get('/ready')
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError('published native Server did not become ready') from None
                    time.sleep(0.2)
            info = get('/cluster/info')
            if info.get('version') != SERVER_VERSION:
                raise RuntimeError('cluster version mismatch')
            if info.get('worker_protocol', {}).get('server_capabilities', {}).get('claimed_task_ready_age') is not True:
                raise RuntimeError('published Server does not advertise ready-age metrics')
            env = dict(os.environ, CARGO_BUILD_JOBS='2', DURABLE_WORKFLOW_LOCAL_ISOLATED='1',
                       DURABLE_WORKFLOW_SERVER_URL=origin, DURABLE_WORKFLOW_AUTH_TOKEN=token)
            subprocess.run(['cargo', 'test', '--locked', '--all-features', '--test',
                            'metrics_server', '--', '--ignored', '--nocapture'], env=env, check=True)
        finally:
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=True)
    print(json.dumps({'outcome': 'pass', 'cleanup': 'pass', 'server_image': SERVER_IMAGE,
                      'server_revision': SERVER_REVISION, 'server_version': SERVER_VERSION,
                      'sdk_commit': capture('git', 'rev-parse', 'HEAD')}))


if __name__ == '__main__':
    main()
