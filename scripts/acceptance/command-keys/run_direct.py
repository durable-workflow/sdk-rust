#!/usr/bin/env python3
"""Run the direct installed command-key tests against one owned published Server.

The runner verifies the exact published Server image, starts one capped,
read-only SQLite container on a numeric loopback port with a synthetic
credential, waits until it advertises `command_request_keys`, then runs the
given consumer command once with the runtime variables
`tests/command_keys_server.rs` reads. The container is always removed and its
absence verified.
"""
import argparse
import base64
import json
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.request

SERVER_REPOSITORY = 'ghcr.io/durable-workflow/server'


def capture(*args, timeout=30):
    return subprocess.check_output(args, text=True, timeout=timeout).strip()


def probe(origin, path, token):
    request = urllib.request.Request(origin + path, headers={
        'Authorization': 'Bearer ' + token, 'Accept': 'application/json'})
    with urllib.request.urlopen(request, timeout=2) as response:
        return json.loads(response.read(65536))


def server_arguments(name, image, envfile):
    return ['docker', 'create', '--init', '--read-only', '--name', name,
            '--cpus', '1', '--memory', '1g', '--pids-limit', '256', '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:mode=1777,size=128m',
            '--tmpfs', '/app/storage:uid=1000,gid=1000,mode=0700,size=64m',
            '--tmpfs', '/app/database:uid=1000,gid=1000,mode=0700,size=64m',
            '-p', '127.0.0.1::8080', '--env-file', envfile,
            '-e', 'DW_BIND_ADDRESS=0.0.0.0:8080', '-e', 'DW_RUST_MAINTENANCE_LOOP=0',
            '-e', 'DW_V2_UPDATE_WAIT_COMPLETION_TIMEOUT_SECONDS=1',
            '-e', 'DB_CONNECTION=sqlite', '-e', 'DB_DATABASE=/app/database/direct.sqlite', image]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server-index', required=True)
    parser.add_argument('--server-version', required=True)
    parser.add_argument('--server-source', required=True)
    parser.add_argument('--container-prefix', default='sdk-rust-216-direct')
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    options = parser.parse_args()
    command = options.command[1:] if options.command[:1] == ['--'] else options.command
    if not command:
        parser.error('a consumer command is required after --')
    if (not re.fullmatch(r'sha256:[0-9a-f]{64}', options.server_index)
            or not re.fullmatch(r'[0-9a-f]{40}', options.server_source)
            or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,40}', options.container_prefix)):
        parser.error('exact immutable Server identities and a plain container prefix are required')
    work = options.work_dir
    if not os.path.isabs(work) or os.path.exists(work):
        parser.error('work directory must be a new absolute path')
    image = SERVER_REPOSITORY + '@' + options.server_index
    info = json.loads(capture('docker', 'image', 'inspect', image))[0]
    labels = info['Config']['Labels']
    assert labels['org.opencontainers.image.revision'] == options.server_source, labels
    assert labels['dev.durable-workflow.release.tag'] == options.server_version, labels
    assert info['Os'] == 'linux' and info['Architecture'] == 'amd64'
    os.mkdir(work, 0o700)
    envfile = os.path.join(work, 'runtime.env')
    token = 'synthetic-' + secrets.token_hex(24)
    key = 'base64:' + base64.b64encode(secrets.token_bytes(32)).decode()
    with open(os.open(envfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as file:
        file.write('DW_AUTH_TOKEN=' + token + '\nDW_SERVER_KEY=' + key + '\n')
    name = options.container_prefix + '-' + secrets.token_hex(5)
    owned = None
    outcome = 'fail'
    started = time.time()
    try:
        owned = capture(*server_arguments(name, image, envfile))
        capture('docker', 'start', owned)
        host, port = capture('docker', 'port', owned, '8080/tcp').splitlines()[0].rsplit(':', 1)
        assert host == '127.0.0.1' and 0 < int(port) < 65536
        origin = 'http://127.0.0.1:' + port
        deadline = time.monotonic() + 60
        while True:
            try:
                probe(origin, '/api/ready', token)
                cluster = probe(origin, '/api/cluster/info', token)
                assert cluster['version'] == options.server_version, cluster['version']
                assert cluster['capabilities']['command_request_keys'] is True
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError('owned Server did not become ready') from None
                time.sleep(0.2)
        environment = dict(os.environ, DURABLE_WORKFLOW_LOCAL_ISOLATED='1',
                           DURABLE_WORKFLOW_SERVER_URL=origin, DURABLE_WORKFLOW_AUTH_TOKEN=token)
        result = subprocess.run(command, env=environment, timeout=1800)
        outcome = 'pass' if result.returncode == 0 else 'fail'
    finally:
        if owned is not None:
            subprocess.run(['docker', 'rm', '-fv', owned], check=True, stdout=subprocess.DEVNULL)
            gone = subprocess.run(['docker', 'inspect', owned], capture_output=True, text=True)
            assert gone.returncode != 0, 'owned Server container still exists'
        os.unlink(envfile)
        os.rmdir(work)
        print(json.dumps({'cell': 'direct-installed', 'outcome': outcome,
                          'server_index': options.server_index,
                          'server_version': options.server_version,
                          'server_source': options.server_source,
                          'server_platform': 'linux/amd64', 'container': name,
                          'started_at': started, 'finished_at': time.time(),
                          'cleanup': 'owned container removed and absent'}), flush=True)
    sys.exit(0 if outcome == 'pass' else 1)


if __name__ == '__main__':
    main()
