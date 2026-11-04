"""Client-independent Cell bounds. Docker is always stubbed."""
import os
import contextlib
import io
import json
import pathlib
import subprocess
import shutil
import socket
import time
import tempfile
import unittest
from unittest import mock

import run
import test_run
from test_run import FakeDocker, args, write_binary, done, TOKEN, FOREIGN


class CellLifetime(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = pathlib.Path(self.tmp.name)
        self.fixture = root / 'fixture'
        write_binary(self.fixture)(None)
        self.harness = run.Run('sdk-rust', token='0123456789abcdef')
        self.cell = run.Cell(args(), 'lifecycle', root, {'executed_manifest': 'img'},
                             self.fixture, self.harness)

    def test_every_native_role_has_independent_deadline(self):
        docker = FakeDocker()
        with mock.patch.object(run, 'call', docker):
            for role in ('runtime', 'proxy', 'worker', 'driver', 'history'):
                self.cell.start(role, 'img', ['sleep', '60'], credentials='auth')
        deadlines = []
        for command, _ in docker.runs():
            self.assertIn('--entrypoint', command)
            self.assertEqual(command[command.index('--entrypoint') + 1], '/bin/sh')
            tail = command[command.index('img') + 1:]
            self.assertEqual(tail[0], '/acceptance-supervisor.sh')
            deadlines.append(tail[1])
            self.assertEqual(tail[2], str(run.CELL_KILL_AFTER_SECONDS))
            self.assertEqual(tail[3], 'auth')
        self.assertEqual(len(set(deadlines)), 1)

    def test_no_credential_is_written_to_host_state(self):
        cell = self.cell

        class Recording(FakeDocker):
            def __call__(inner, command, **kwargs):
                if command[:3] == ['docker', 'network', 'create']:
                    self.assertFalse((cell.state / 'credentials.json').exists())
                return super().__call__(command, **kwargs)

        docker = Recording(faults={cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            cell.run()
        self.assertFalse(os.path.lexists(cell.state / 'credentials.json'))

    def test_php_roles_share_deadline_and_keep_capability_bounds(self):
        docker = FakeDocker()
        with mock.patch.object(run, 'call', docker):
            self.cell.start_php(self.cell.token)
        self.assertEqual(len(docker.runs()), 6)
        for command, _ in docker.runs():
            self.assertIn('/acceptance-supervisor.sh', command)
            offset = command.index('/acceptance-supervisor.sh')
            self.assertEqual(command[offset + 1:offset + 3],
                             [str(self.cell.deadline), str(run.CELL_KILL_AFTER_SECONDS)])
            self.assertIn('--init', command)
            self.assertIn('no-new-privileges', command)
            self.assertNotIn('--privileged', command)
            self.assertNotIn(self.cell.token, ' '.join(command))
            self.assertNotIn(self.cell.server_key, ' '.join(command))
            role = command[command.index('--name') + 1].rsplit('-', 1)[1]
            self.assertEqual(command[offset + 3], {'mysql': 'none', 'redis': 'none', 'bootstrap': 'server',
                                                  'runtime': 'both', 'queue': 'server', 'repair': 'server'}[role])
            if role in ('bootstrap', 'runtime', 'queue', 'repair'):
                self.assertIn('/app/bootstrap/cache:mode=0777,size=8m,noexec,nosuid,nodev', command)

    def test_token_uses_only_atomic_stdin_delivery_to_private_tmpfs(self):
        deliveries = []

        class Recording(FakeDocker):
            def __call__(inner, command, **kwargs):
                if command[:3] == ['docker', 'exec', '-i']:
                    deliveries.append((list(command), kwargs.get('input')))
                return super().__call__(command, **kwargs)

        docker = Recording()
        with mock.patch.object(run, 'call', docker):
            self.cell.start('worker', 'img', ['x'], credentials='auth')
        [(command, _)] = docker.runs()
        self.assertIn('/credentials:mode=0700,uid=%d,gid=%d,size=64k,noexec,nosuid,nodev' %
                      (os.getuid(), os.getgid()), command)
        self.assertIn('--read-only', command)
        self.assertNotIn(self.cell.token, json.dumps(docker.commands))
        [(delivery, value)] = deliveries
        self.assertEqual(value, self.cell.token + '\n\n')
        self.assertIn('umask 077', delivery[-1])
        self.assertIn('mv /credentials/ready.tmp /credentials/ready', delivery[-1])
        self.assertNotIn(self.cell.server_key, value)

    def test_image_entrypoint_and_default_command_remain_argv(self):
        class Entrypoint(FakeDocker):
            def __call__(inner, command, **kwargs):
                if command[:3] == ['docker', 'image', 'inspect']:
                    return done(command, stdout=json.dumps([{'Config': {
                        'Entrypoint': ['/entrypoint', 'fixed'], 'Cmd': ['default', 'with spaces']}}]))
                return super().__call__(command, **kwargs)
        docker = Entrypoint()
        with mock.patch.object(run, 'call', docker):
            self.cell.start('runtime', 'img', [])
            self.cell.start('override', 'img', ['explicit;literal'])
        [first, second] = [c for c, _ in docker.runs()]
        self.assertEqual(first[-4:], ['/entrypoint', 'fixed', 'default', 'with spaces'])
        self.assertEqual(second[-3:], ['/entrypoint', 'fixed', 'explicit;literal'])

    def test_expired_cell_cannot_start_a_late_role(self):
        docker = FakeDocker()
        with mock.patch.object(run.time, 'time', return_value=self.cell.deadline), \
                mock.patch.object(run, 'call', docker), self.assertRaisesRegex(RuntimeError, 'expired'):
            self.cell.start('history', 'img', ['x'])
        self.assertEqual(docker.commands, [])

    def test_supervisor_wraps_credential_wait_and_fixed_term_kill(self):
        # A stub timeout records argv instead of starting a process or container.
        root = pathlib.Path(self.tmp.name)
        timeout = root / 'timeout'
        timeout.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
        timeout.chmod(0o700)
        script = pathlib.Path(run.HERE) / 'cell-supervisor.sh'
        result = subprocess.run(['/bin/sh', str(script), str(self.cell.deadline),
                                 str(run.CELL_KILL_AFTER_SECONDS), 'auth', 'child', 'literal arg'],
                                env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH']),
                                text=True, capture_output=True, timeout=5, check=True)
        lines = result.stdout.splitlines()
        self.assertEqual(lines[:2], ['--signal=TERM', '--kill-after=10s'])
        self.assertLessEqual(int(lines[2][:-1]), run.CELL_WALL_SECONDS)
        self.assertEqual(lines[3:], ['/bin/sh', str(script), '--command', 'auth', 'child', 'literal arg'])

    def local_supervisor(self, token):
        # Substitute only the container mount path so the real shell supervisor
        # can exercise its token gate without Docker or a privileged chroot.
        root = pathlib.Path(self.tmp.name)
        credential = root / 'token'
        credential.write_text(token)
        (root / 'server-key').write_text(self.cell.server_key)
        (root / 'ready').touch()
        script = root / 'supervisor.sh'
        script.write_text((run.HERE / 'cell-supervisor.sh').read_text().replace(
            '/credentials/', str(root) + '/'))
        return script

    def test_malformed_or_partial_token_never_starts_the_role(self):
        marker = pathlib.Path(self.tmp.name) / 'started'
        for token in ('partial', 'fixture-' + '0' * 47, 'fixture-' + 'z' * 48):
            with self.subTest(token=token):
                script = self.local_supervisor(token)
                result = subprocess.run(['/bin/sh', str(script), '--command', 'auth', 'touch', str(marker)],
                                        capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 125)
                self.assertFalse(marker.exists())

    def test_valid_token_starts_the_original_role(self):
        script = self.local_supervisor('fixture-' + '0' * 48)
        result = subprocess.run(['/bin/sh', str(script), '--command', 'auth', '/bin/sh', '-c',
                                 'test ${#DW_AUTH_TOKEN} -eq 56 && printf started'],
                                capture_output=True, timeout=5)
        self.assertEqual((result.returncode, result.stdout), (0, b'started'))

    @unittest.skipUnless(shutil.which('timeout'), 'GNU timeout runs on the hosted Linux runner')
    def test_real_supervisor_kills_a_role_that_ignores_term(self):
        script = self.local_supervisor('fixture-' + '0' * 48)
        marker = pathlib.Path(self.tmp.name) / 'term-observed'
        child = ('import signal,time,pathlib; '
                 'signal.signal(signal.SIGTERM,lambda *_:pathlib.Path(%r).write_text("TERM")); '
                 'time.sleep(30)') % str(marker)
        import sys
        started = time.monotonic()
        result = subprocess.run(['/bin/sh', str(script), str(int(time.time()) + 2), '1', 'auth',
                                 sys.executable, '-c', child], capture_output=True, timeout=6)
        self.assertIn(result.returncode, (-9, 137))
        self.assertEqual(marker.read_text(), 'TERM')
        self.assertLess(time.monotonic() - started, 5)

    def test_roles_receive_only_their_required_credentials(self):
        deliveries = []

        class Recording(FakeDocker):
            def __call__(inner, command, **kwargs):
                if command[:3] == ['docker', 'exec', '-i']:
                    deliveries.append((command[3], kwargs['input']))
                return super().__call__(command, **kwargs)
        docker = Recording()
        with mock.patch.object(run, 'call', docker):
            self.cell.start_php(self.cell.token)
        self.assertEqual({name.rsplit('-', 1)[1]: value for name, value in deliveries}, {
            'bootstrap': '\n' + self.cell.server_key + '\n',
            'runtime': self.cell.token + '\n' + self.cell.server_key + '\n',
            'queue': '\n' + self.cell.server_key + '\n',
            'repair': '\n' + self.cell.server_key + '\n'})
        # Docker inspect's Config.Env/Config.Cmd come from create metadata, not
        # the environment exported later by the supervised child.
        metadata = [{'Env': [c[i + 1] for i, s in enumerate(c) if s == '-e'],
                     'Cmd': c[c.index('/acceptance-supervisor.sh'):]}
                    for c, _ in docker.runs()]
        for secret in (self.cell.token, self.cell.server_key):
            self.assertNotIn(secret, json.dumps(metadata))

    def test_no_key_or_token_in_error_evidence(self):
        docker = FakeDocker(faults={self.cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            self.cell.run()
        for path in pathlib.Path(self.tmp.name).rglob('*'):
            if path.is_file():
                for secret in (self.cell.token, self.cell.server_key):
                    self.assertNotIn(secret.encode(), path.read_bytes())
        for secret in (self.cell.token, self.cell.server_key):
            self.assertNotIn(secret, json.dumps(self.harness.cleanup_errors))
            self.assertNotIn(secret, json.dumps(docker.commands))

    def test_no_credentials_role_does_not_wait_for_delivery(self):
        script = self.local_supervisor('unused')
        (pathlib.Path(self.tmp.name) / 'ready').unlink()
        result = subprocess.run(['/bin/sh', str(script), '--command', 'none', '/bin/sh', '-c',
                                 'test -z "${DW_AUTH_TOKEN-}${DW_SERVER_KEY-}" && printf started'],
                                capture_output=True, timeout=5)
        self.assertEqual((result.returncode, result.stdout), (0, b'started'))

    def test_server_only_role_does_not_receive_auth(self):
        script = self.local_supervisor('unused')
        result = subprocess.run(['/bin/sh', str(script), '--command', 'server', '/bin/sh', '-c',
                                 'test -z "${DW_AUTH_TOKEN-}" && test ${#DW_SERVER_KEY} -eq 51 && printf started'],
                                capture_output=True, timeout=5)
        self.assertEqual((result.returncode, result.stdout), (0, b'started'))

    def test_real_main_records_and_logs_never_include_either_secret(self):
        import sys
        from test_run import BUILD_STDOUT
        original_cell = run.Cell
        for runtime in ('native', 'php'):
            with self.subTest(runtime=runtime):
                root = pathlib.Path(self.tmp.name) / runtime
                fixture = root / 'fixture'
                write_binary(fixture)(None)
                (fixture / 'build.stdout').write_text(BUILD_STDOUT)
                work = root / 'work'
                argv = ['run.py', '--crate-version', '4.11.2', '--server-index', 'sha256:fixture',
                        '--fixture-from', str(fixture), '--work-dir', str(work), '--cells', 'lifecycle',
                        '--container-prefix', 'sdk-rust', '--runtime', runtime]
                cells = []
                docker = FakeDocker()

                def cell(*a, **kw):
                    instance = original_cell(*a, **kw)
                    cells.append(instance)
                    docker.faults[instance.task + '-proxy'] = 'created-then-error'
                    return instance

                output = io.StringIO()
                with mock.patch.object(sys, 'argv', argv), mock.patch.object(run, 'call', docker), \
                        mock.patch.object(run, 'server_identity', return_value={'executed_manifest': 'img'}), \
                        mock.patch.object(run, 'crate_provenance', return_value={'baselines': {'pass': True}}), \
                        mock.patch.object(run, 'Cell', cell), contextlib.redirect_stdout(output), \
                        self.assertRaises(RuntimeError):
                    run.main()
                [instance] = cells
                self.assertTrue((work / 'run.json').exists())
                self.assertTrue((work / 'summary.json').exists())
                blobs = [output.getvalue().encode(), json.dumps(docker.commands).encode()]
                blobs += [p.read_bytes() for p in work.rglob('*') if p.is_file()]
                for value in (instance.token, instance.server_key):
                    self.assertFalse(any(value.encode() in blob for blob in blobs))

    def test_adversarial_logs_are_redacted_before_any_save_and_fail_acceptance(self):
        logs = self.cell.dir / 'logs'
        logs.mkdir(parents=True)
        original_write = pathlib.Path.write_bytes
        unsafe_writes = []

        def write(path, data):
            unsafe_writes.append(any(s.encode() in data for s in (self.cell.token, self.cell.server_key)))
            return original_write(path, data)

        with mock.patch.object(run, 'call', return_value=done([], stdout=self.cell.token,
                                                             stderr=self.cell.server_key)), \
                mock.patch.object(pathlib.Path, 'write_bytes', write):
            self.cell.save_logs(logs, self.cell.task + '-worker', 'worker-id')
        self.assertFalse(any(unsafe_writes))
        self.assertEqual((logs / 'worker.stdout').read_text(), '[REDACTED]')
        self.assertEqual((logs / 'worker.stderr').read_text(), '[REDACTED]')
        self.assertIn({'step': 'credential-output', 'object': 'lifecycle/logs', 'error': 'CredentialOutput'},
                      self.harness.cleanup_errors)

    def test_adversarial_structured_and_partial_output_is_fenced(self):
        self.cell.state.mkdir(parents=True)
        for name in ('driver.json', 'history.json'):
            encoded = json.dumps({'token': self.cell.token, 'key': self.cell.server_key}).replace(
                'fixture-', '\\u0066ixture-').replace('base64:', '\\u0062ase64:')
            (self.cell.state / name).write_text(encoded)
        (self.cell.state / 'acks.jsonl').write_text(json.dumps({'response': self.cell.server_key}) + '\n')
        (self.cell.state / 'partial.stdout').write_text('partial ' + self.cell.token)
        self.cell.fence_artifacts()
        for path in self.cell.state.iterdir():
            data = path.read_bytes()
            self.assertFalse(any(s.encode() in data for s in (self.cell.token, self.cell.server_key)))
        self.assertTrue(self.harness.cleanup_errors)
        self.assertEqual(json.loads((self.cell.state / 'driver.json').read_text()),
                         {'token': '[REDACTED]', 'key': '[REDACTED]'})

    def test_credential_in_exception_cannot_escape_cell_or_evidence(self):
        docker = FakeDocker(faults={self.cell.task + '-proxy': 'created-then-error'})
        original = docker.__call__

        def call(command, **kwargs):
            if command[:2] == ['docker', 'run'] and command[command.index('--name') + 1].endswith('-proxy'):
                raise RuntimeError('child emitted ' + self.cell.server_key)
            return original(command, **kwargs)

        with mock.patch.object(run, 'call', call), self.assertRaises(RuntimeError) as raised:
            self.cell.run()
        self.assertEqual(str(raised.exception), 'Cell failed with credential-bearing diagnostics')
        self.assertTrue(any(e['step'] == 'credential-output' for e in self.harness.cleanup_errors))

    def test_uncertifiable_evidence_is_purged_on_normal_cleanup(self):
        docker = FakeDocker(faults={self.cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), \
                mock.patch.object(self.cell, 'fence_artifacts', side_effect=PermissionError('unreadable')), \
                self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertFalse(self.cell.dir.exists())
        self.assertTrue(any(e['step'] == 'evidence-fence' for e in self.harness.cleanup_errors))

    def test_credential_bearing_artifact_name_is_purged(self):
        def create(command):
            if command[command.index('--name') + 1].endswith('-runtime'):
                (self.cell.state / (self.cell.token + '.stdout')).write_text('diagnostic')
        docker = FakeDocker(created=create, faults={self.cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertFalse(self.cell.dir.exists())
        self.assertTrue(any(e['step'] == 'credential-output' for e in self.harness.cleanup_errors))

    def test_fifo_socket_and_hard_link_fail_and_purge_without_touching_foreign_files(self):
        for kind in ('fifo', 'socket', 'hard-link'):
            with self.subTest(kind=kind):
                root = pathlib.Path(self.tmp.name) / kind
                fixture = root / 'fixture'
                write_binary(fixture)(None)
                cell = run.Cell(args(), 'lifecycle', root, {'executed_manifest': 'img'}, fixture, self.harness)
                foreign = root / 'foreign'
                foreign.write_text('keep')
                sockets = []

                def create(command):
                    if command[command.index('--name') + 1].endswith('-runtime'):
                        artifact = cell.state / 'unexpected'
                        if kind == 'fifo':
                            os.mkfifo(artifact)
                        elif kind == 'socket':
                            handle = socket.socket(socket.AF_UNIX)
                            sockets.append(handle)
                            handle.bind(str(artifact))
                        else:
                            os.link(foreign, artifact)

                docker = FakeDocker(created=create, faults={cell.task + '-proxy': 'created-then-error'})
                try:
                    with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
                        cell.run()
                    self.assertFalse(cell.dir.exists())
                    self.assertEqual(foreign.read_text(), 'keep')
                    self.assertEqual(foreign.stat().st_nlink, 1)
                finally:
                    for handle in sockets:
                        handle.close()

    def test_normal_cleanup_refuses_symlinked_cell_root_and_only_unlinks_it(self):
        foreign = pathlib.Path(self.tmp.name) / 'foreign'
        (foreign / 'state/database').mkdir(parents=True)
        marker = foreign / 'state/database/keep'
        marker.write_text('untouched')
        (foreign / 'logs').mkdir()
        log_marker = foreign / 'logs/keep'
        log_marker.write_text('untouched log')

        def create(command):
            if command[command.index('--name') + 1].endswith('-runtime'):
                shutil.rmtree(self.cell.dir)
                self.cell.dir.symlink_to(foreign)

        docker = FakeDocker(created=create, faults={self.cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertFalse(os.path.lexists(self.cell.dir))
        self.assertEqual(marker.read_text(), 'untouched')
        self.assertEqual(log_marker.read_text(), 'untouched log')
        self.assertEqual([p.name for p in (foreign / 'logs').iterdir()], ['keep'])

    def test_known_report_fifo_is_refused_before_any_read_can_block(self):
        self.cell.state.mkdir(parents=True)
        report = self.cell.state / 'driver.json'
        os.mkfifo(report)
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, 'nonregular'):
            self.cell.artifact_bytes(report)
        self.assertLess(time.monotonic() - started, 1)

    def test_replacement_race_cannot_redirect_redaction_to_foreign_file(self):
        foreign = pathlib.Path(self.tmp.name) / 'foreign'
        foreign.write_text('untouched')
        artifact = self.cell.state / 'report.stdout'

        def create(command):
            if command[command.index('--name') + 1].endswith('-runtime'):
                artifact.write_text(self.cell.token)

        original = self.cell.evidence

        def evidence(data, label):
            clean = original(data, label)
            if label == 'artifact' and self.cell.token.encode() in data:
                artifact.unlink()
                artifact.symlink_to(foreign)
            return clean

        docker = FakeDocker(created=create, faults={self.cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), mock.patch.object(self.cell, 'evidence', evidence), \
                self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertEqual(foreign.read_text(), 'untouched')
        self.assertFalse(self.cell.dir.exists())

    def test_surviving_writer_prevents_retained_scan_and_forces_purge(self):
        runtime = self.cell.task + '-runtime'
        docker = FakeDocker(faults={self.cell.task + '-proxy': 'created-then-error'},
                            raise_on={(('docker', 'rm', '-f'), 'id-' + runtime): RuntimeError('remove failed')})
        with mock.patch.object(run, 'call', docker), mock.patch.object(self.cell, 'fence_artifacts') as fence, \
                self.assertRaises(RuntimeError):
            self.cell.run()
        fence.assert_not_called()
        self.assertIn(runtime, docker.containers)
        self.assertFalse(self.cell.dir.exists())
        self.assertTrue(self.harness.cleanup_errors)

    def test_unreadable_walk_entry_fails_and_purges(self):
        def denied(root, **kwargs):
            kwargs['onerror'](PermissionError('denied'))
            return iter(())
        docker = FakeDocker(faults={self.cell.task + '-proxy': 'created-then-error'})
        with mock.patch.object(run, 'call', docker), mock.patch.object(run.os, 'walk', denied), \
                self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertFalse(self.cell.dir.exists())
        self.assertTrue(any(e['step'] == 'evidence-fence' for e in self.harness.cleanup_errors))

    def test_same_token_wrong_owner_container_is_never_logged_or_removed(self):
        name = self.cell.task + '-proxy'
        docker = FakeDocker(containers={name: ('foreign-id', self.harness.token)}, owners={name: 'other'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertIn(name, docker.containers)
        self.assertFalse(any(c[-1] == 'foreign-id' for c, _ in docker.commands
                             if c[:2] == ['docker', 'logs'] or c[:3] == ['docker', 'rm', '-f']))

    def test_same_token_wrong_owner_network_is_never_removed(self):
        name = self.cell.task
        docker = FakeDocker(networks={name: ('foreign-network', self.harness.token)}, owners={name: 'other'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            self.cell.run()
        self.assertIn(name, docker.networks)
        self.assertNotIn(['docker', 'network', 'rm', 'foreign-network'], docker.removals())


class RecoveryDocker(FakeDocker):
    def __init__(self, *a, owners=None, **kw):
        super().__init__(*a, **kw)
        self.owners = owners or {}

    def __call__(self, command, timeout=120, check=True, **kwargs):
        inventory = command[:3] == ['docker', 'ps', '-aq'] or command[:4] == ['docker', 'network', 'ls', '-q']
        if inventory:
            self.commands.append((list(command), timeout))
            table = self.containers if command[1] == 'ps' else self.networks
            filters = [command[i + 1] for i, s in enumerate(command) if s == '--filter']
            token = next(s.split('=', 2)[2] for s in filters if run.RUN_LABEL in s)
            owner = next(s.split('=', 2)[2] for s in filters if run.OWNER_LABEL in s)
            ids = [v[0] for name, v in table.items()
                   if v[1] == token and self.owners.get(name, 'sdk-rust') == owner]
            return done(command, stdout='\n'.join(ids))
        if command[:3] in (['docker', 'container', 'inspect'], ['docker', 'network', 'inspect']) and run.OWNER_LABEL in command[-2]:
            self.commands.append((list(command), timeout))
            table = self.containers if command[1] == 'container' else self.networks
            for name, (object_id, token) in table.items():
                if object_id == command[-1]:
                    return done(command, stdout='%s %s\n' % (token, self.owners.get(name, 'sdk-rust')))
            raise RuntimeError('missing object')
        return super().__call__(command, timeout, check, **kwargs)


class ClientDeathRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.harness = run.Run('sdk-rust', token=TOKEN)
        (self.root / 'run.json').write_text(json.dumps({
            'schema': 'durable-workflow.acceptance-run/v1', 'owner': 'sdk-rust',
            'token': TOKEN, 'cells': ['lifecycle']}))
        for name in ('database', 'storage'):
            (self.root / 'cells/lifecycle/state' / name).mkdir(parents=True)

    def recover(self, docker):
        with mock.patch.object(run, 'call', docker):
            return run.recover_run(self.root, self.harness, ['lifecycle'])

    def test_client_death_removes_only_exact_run_and_state(self):
        # A dead owner cannot recover the secret values to redact these partial
        # files. Recovery must purge the complete unqualified tree.
        state = self.root / 'cells/lifecycle/state'
        (state / 'driver.json').write_text('unqualified credential-bearing output')
        (self.root / 'cells/lifecycle/logs').mkdir()
        (self.root / 'cells/lifecycle/logs/worker.stdout').write_text('partial output')
        docker = RecoveryDocker(containers={'c': ('cid', TOKEN), 'foreign': ('fid', FOREIGN), 'owner': ('oid', TOKEN)},
                                networks={'n': ('nid', TOKEN)}, owners={'owner': 'other'})
        result = self.recover(docker)
        self.assertTrue(result['pass'])
        self.assertEqual(docker.containers, {'foreign': ('fid', FOREIGN), 'owner': ('oid', TOKEN)})
        self.assertEqual(docker.networks, {})
        self.assertFalse((self.root / 'cells/lifecycle/state/database').exists())
        self.assertFalse((self.root / 'cells/lifecycle/state/storage').exists())
        self.assertFalse((self.root / 'cells/lifecycle').exists())

    def test_label_is_rechecked_after_inventory_before_removal(self):
        class Changed(RecoveryDocker):
            def __call__(inner, command, **kwargs):
                result = super().__call__(command, **kwargs)
                if command[:3] == ['docker', 'ps', '-aq']:
                    inner.owners['c'] = 'foreign'
                return result
        docker = Changed(containers={'c': ('cid', TOKEN)})
        result = self.recover(docker)
        self.assertFalse(result['pass'])
        self.assertEqual(docker.containers, {'c': ('cid', TOKEN)})
        self.assertEqual(docker.removals(), [])

    def test_failed_query_is_not_an_empty_inventory(self):
        class Failed(RecoveryDocker):
            def __call__(inner, command, **kwargs):
                if command[:3] == ['docker', 'ps', '-aq']:
                    raise RuntimeError('query failed')
                return super().__call__(command, **kwargs)
        result = self.recover(Failed())
        self.assertFalse(result['pass'])
        self.assertIsNone(result['leftover_containers'])

    def test_silent_remove_survivor_fails(self):
        class Survivor(RecoveryDocker):
            def _drop(inner, table, object_id):
                pass
        result = self.recover(Survivor(containers={'c': ('cid', TOKEN)}))
        self.assertFalse(result['pass'])
        self.assertEqual(result['leftover_containers'], ['cid'])

    def test_wrong_work_directory_record_cannot_touch_docker(self):
        docker = RecoveryDocker()
        with mock.patch.object(run, 'call', docker), self.assertRaisesRegex(SystemExit, 'does not belong'):
            run.cleanup_main(['--work-dir', str(self.root), '--container-prefix', 'other', '--run-token', TOKEN])
        self.assertEqual(docker.commands, [])

    def test_cleanup_command_returns_nonzero_for_survivor(self):
        class Survivor(RecoveryDocker):
            def _drop(inner, table, object_id):
                pass
        output = io.StringIO()
        with mock.patch.object(run, 'call', Survivor(containers={'c': ('cid', TOKEN)})), \
                contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as exited:
            run.cleanup_main(['--work-dir', str(self.root), '--container-prefix', 'sdk-rust', '--run-token', TOKEN])
        self.assertEqual(exited.exception.code, 1)
        self.assertFalse(json.loads(output.getvalue())['pass'])

    def test_partial_create_then_client_death_is_recovered(self):
        fixture = self.root / 'fixture'
        write_binary(fixture)(None)
        cell = run.Cell(args(), 'lifecycle', self.root, {'executed_manifest': 'img'}, fixture, self.harness)
        name = cell.task + '-runtime'
        docker = RecoveryDocker(faults={name: 'created-then-timeout'})
        with mock.patch.object(run, 'call', docker), self.assertRaises(subprocess.TimeoutExpired):
            cell.start('runtime', 'img', ['x'])
        self.assertIn(name, docker.containers)
        self.assertEqual(cell.containers, [name])
        self.assertTrue(self.recover(docker)['pass'])
        self.assertEqual(docker.containers, {})

    def test_recovery_never_follows_inner_state_symlinks(self):
        foreign = self.root / 'foreign'
        foreign.mkdir()
        marker = foreign / 'keep'
        marker.write_text('untouched')
        state = self.root / 'cells/lifecycle/state'
        shutil.rmtree(state / 'database')
        (state / 'database').symlink_to(foreign)
        result = self.recover(RecoveryDocker())
        self.assertTrue(result['pass'])
        self.assertEqual(marker.read_text(), 'untouched')

    def test_recovery_refuses_symlinked_cell_root(self):
        foreign = self.root / 'foreign'
        foreign.mkdir()
        marker = foreign / 'keep'
        marker.write_text('untouched')
        cell = self.root / 'cells/lifecycle'
        shutil.rmtree(cell)
        cell.symlink_to(foreign)
        result = self.recover(RecoveryDocker())
        self.assertFalse(result['pass'])
        self.assertEqual(marker.read_text(), 'untouched')


class SummaryCredentialFence(unittest.TestCase):
    main = test_run.MainOutcome.main

    def test_adversarial_summary_is_redacted_and_refuses_approval(self):
        token = 'fixture-' + '0' * 48
        key = 'base64:' + 'A' * 43 + '='

        def outcome(harness):
            harness.secret_values.extend((token, key))
            return {'outcome': 'pass', 'driver': {'value': token}, 'history': {'value': key}}

        code, receipt, summary = self.main(cell_run=outcome)
        self.assertEqual((code, receipt['outcome'], summary['outcome']), (1, 'fail', 'fail'))
        self.assertFalse(summary['cleanup']['pass'])
        self.assertFalse((self.work / 'approved-package.json').exists())
        for path in self.work.rglob('*'):
            if path.is_file():
                self.assertFalse(any(s.encode() in path.read_bytes() for s in (token, key)))
        self.assertEqual(summary['cells']['lifecycle']['driver']['value'], '[REDACTED]')

    def test_adversarial_error_summary_is_sanitized(self):
        token = 'fixture-' + '0' * 48

        def outcome(harness):
            harness.secret_values.append(token)
            raise RuntimeError('failure containing ' + token)

        with self.assertRaises(RuntimeError):
            self.main(cell_run=outcome)
        summary_bytes = (self.work / 'summary.json').read_bytes()
        self.assertFalse(token.encode() in summary_bytes)
        self.assertIn(b'[REDACTED]', summary_bytes)


if __name__ == '__main__':
    unittest.main()
