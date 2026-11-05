"""External guest controls use fake native commands. No VM or disk is created."""
import copy
import ctypes
import json
import os
import pathlib
import plistlib
import shutil
import sys
import tempfile
import time
import types
import unittest
from unittest import mock
from test_subject import HERE, load

host = load('condition_guest_host_tests', HERE / 'guest-host.py')
TOKEN = '0123456789abcdef'
BOOT = '11111111-1111-1111-1111-111111111111'


class GuestHost(unittest.TestCase):
    def setUp(self):
        self.temp = pathlib.Path(tempfile.mkdtemp(dir='/tmp', prefix='sr-')).resolve()
        self.addCleanup(shutil.rmtree, self.temp)
        self.parent = self.temp / ('sr-' + TOKEN)
        self.parent.mkdir(mode=0o700)
        self.record = {'parent': str(self.parent), 'token': TOKEN, 'limactl': '/reviewed/limactl',
            'runner_commit': 'a' * 40, 'manifest_sha256': 'b' * 64, 'guardian_sha256': 'c' * 64,
            'started_unix': int(time.time()), 'active_unix': int(time.time()) + 1740,
            'cleanup_unix': int(time.time()) + 1800, '_cleanup_mono': time.monotonic() + 1800}
        self.scoped = host.paths(str(self.parent), TOKEN)
        self.profile = json.loads((HERE / 'guest-profile.json').read_bytes())

    def put(self, name, value):
        path = self.parent / name
        path.write_bytes(host.canonical(value))
        return host.digest(path.read_bytes())

    def proposal(self, profile=None):
        return host.plan(str(self.parent), TOKEN, self.record['limactl'],
                         str(self.scoped['mount'] / 'a/guest.raw.gz'), profile or self.profile)

    def test_plan_uses_fixed_store_and_fresh_lima_home(self):
        with mock.patch.object(host.subprocess, 'Popen', side_effect=AssertionError('inert plan')):
            value = self.proposal()
        self.assertEqual(value['host_store_bytes'], 32 * 1024**3)
        self.assertEqual(value['guest_disk_bytes'], 20 * 1024**3)
        self.assertEqual(value['create_store'][2:4], ['-sectors', '67108864'])
        self.assertEqual(value['environment']['LIMA_HOME'], str(self.scoped['home']))
        self.assertNotIn('SSH_AUTH_SOCK', value['environment'])
        self.assertNotIn('DOCKER_HOST', value['environment'])
        self.assertNotIn('HTTP_PROXY', value['environment'])
        self.assertEqual(value['profile']['mounts'], [])

    def test_any_template_change_is_a_new_source_subject(self):
        for key, value in {'mounts': [{'location': '~'}], 'base': ['template:default'],
                'disk': '21GiB', 'cpus': 8, 'networks': [], 'provision': [],
                'propagateProxyEnv': True, 'portForwards': [],
                'ssh': dict(self.profile['ssh'], forwardAgent=True)}.items():
            candidate = copy.deepcopy(self.profile)
            candidate[key] = value
            with self.subTest(key=key), self.assertRaises(SystemExit):
                self.proposal(candidate)

    def test_foreign_scope_and_unbounded_image_paths_refuse(self):
        for root, token, image in [('/tmp/foreign', TOKEN, '/tmp/image'),
                (str(self.parent), 'foreign', '/tmp/image'),
                (str(self.parent), TOKEN, '/tmp/image')]:
            with self.subTest(root=root, token=token), self.assertRaises(SystemExit):
                host.plan(root, token, '/reviewed/limactl', image, self.profile)

    def test_public_darwin_struct_and_consistent_kernel_epoch(self):
        self.assertEqual(ctypes.sizeof(host.BsdInfo), 136)
        process = {'pid': 1234, 'pgid': 1234, 'uid': os.getuid(),
                   'start_seconds': 1000, 'start_microseconds': 123456}
        with mock.patch.object(host, 'boot_identity', return_value=BOOT), \
                mock.patch.object(host, 'bsd_identity', return_value=process):
            self.assertEqual(host.process_identity(1234), dict(process, boot_id=BOOT))
        for changed in (dict(process, start_microseconds=123457), None):
            with mock.patch.object(host, 'boot_identity', return_value=BOOT), \
                    mock.patch.object(host, 'bsd_identity', side_effect=[process, changed]), self.assertRaises(SystemExit):
                host.process_identity(1234)
        with mock.patch.object(host, 'boot_identity', side_effect=[BOOT, 'other']), \
                mock.patch.object(host, 'bsd_identity', return_value=process), self.assertRaises(SystemExit):
            host.process_identity(1234)

    def exports(self):
        storage = {'guest_boot_id': BOOT}
        summary_sha = self.put('guest-summary.json', {'runner_commit': self.record['runner_commit'],
            'manifest_sha256': self.record['manifest_sha256'], 'checks_outcome': 'pass', 'storage': storage})
        audit_sha = self.put('guest-audit.json', {'outcome': 'pending_external_guest_teardown',
            'manifest_sha256': self.record['manifest_sha256'], 'summary_sha256': summary_sha, 'storage': storage})
        exports_sha = self.put('exports.json', {'record_sha256': 'd' * 64, 'guest_boot_id': BOOT,
            'guest-summary.json': summary_sha, 'guest-audit.json': audit_sha})
        self.put('finish.json', {'record_sha256': 'd' * 64, 'exports_sha256': exports_sha})

    def test_finish_requires_exact_external_exports_and_digests(self):
        self.assertFalse(host.finish_requested(self.record, 'd' * 64))
        self.exports()
        self.assertTrue(host.finish_requested(self.record, 'd' * 64))
        self.put('finish.json', {'record_sha256': 'foreign', 'exports_sha256': 'e' * 64})
        with self.assertRaises(SystemExit):host.finish_requested(self.record, 'd' * 64)
        self.exports()
        self.put('guest-summary.json', {'checks_outcome': 'pass'})
        with self.assertRaises(SystemExit):host.finish_requested(self.record, 'd' * 64)

    def audit_files(self):
        self.exports()
        self.put('host-ready.json', {'record_sha256': 'd' * 64, 'source_sha256': 'c' * 64,
                                    'process': {'pid': 1234}})
        self.put('vm-process.json', {'pid': 2345})
        self.put('host-cleanup.json', {'record_sha256': 'd' * 64, 'guardian_sha256': 'c' * 64,
            'outcome': 'pending_independent_host_audit', 'guest_absent': True,
            'store_unmounted': True, 'image_absent': True})

    def test_independent_host_audit_requires_guest_proof_and_both_processes_absent(self):
        self.audit_files()
        with mock.patch.object(host, 'process_identity', return_value=None):
            self.assertEqual(host.audit(self.record, 'd' * 64, 'c' * 64)['outcome'], 'pass')
        with mock.patch.object(host, 'process_identity', return_value=None), self.assertRaises(FileExistsError):
            host.audit(self.record, 'd' * 64, 'c' * 64)
        (self.parent / 'host-audit.json').unlink()
        for survivor in (1234, 2345):
            def observe(pid):return {'pid': pid} if pid == survivor else None
            with self.subTest(survivor=survivor), mock.patch.object(host, 'process_identity', side_effect=observe), \
                    self.assertRaises(SystemExit):host.audit(self.record, 'd' * 64, 'c' * 64)
        self.scoped['image'].write_bytes(b'survivor')
        with self.assertRaises(SystemExit):host.audit(self.record, 'd' * 64, 'c' * 64)

    def test_host_audit_refuses_rebound_guest_and_forged_cleanup(self):
        self.audit_files()
        for filename, key, value in [('guest-summary.json', 'runner_commit', 'e' * 40),
                ('guest-audit.json', 'outcome', 'pass'), ('host-cleanup.json', 'guardian_sha256', 'foreign')]:
            self.audit_files()
            path = self.parent / filename
            data = json.loads(path.read_bytes());data[key] = value
            self.put(filename, data)
            with self.subTest(filename=filename), mock.patch.object(host, 'process_identity', return_value=None), \
                    self.assertRaises(SystemExit):host.audit(self.record, 'd' * 64, 'c' * 64)

    def test_native_control_stream_has_hard_output_and_time_bounds(self):
        # This launches only an inert Python child, with neither native VM tools nor network.
        result = host.call(self.record, [sys.executable, '-I', '-S', '-c', 'print("bounded")'])
        self.assertEqual(result, b'bounded\n')
        with self.assertRaisesRegex(SystemExit, 'oversize'):
            host.call(self.record, [sys.executable, '-I', '-S', '-c', 'import os;os.write(1,b"x"*2200000)'])
        self.record['_cleanup_mono'] = time.monotonic() - 1
        with mock.patch.object(host.subprocess, 'Popen', side_effect=AssertionError('deadline before launch')), \
                self.assertRaisesRegex(SystemExit, 'deadline'):
            host.call(self.record, ['/never/launch'])

    def test_foreign_store_refuses_all_cleanup_mutations(self):
        with mock.patch.object(host, 'verify_store', side_effect=SystemExit('foreign volume')), \
                mock.patch.object(host, 'call', side_effect=AssertionError('must not mutate')), self.assertRaises(SystemExit):
            host.cleanup(self.record)

    def test_changed_process_epoch_refuses_scoped_stop(self):
        self.put('vm-process.json', {'pid': 2345, 'start_microseconds': 1})
        with mock.patch.object(host, 'verify_store'), \
                mock.patch.object(host, 'process_identity', return_value={'pid': 2345, 'start_microseconds': 2}), \
                mock.patch.object(host, 'call', side_effect=AssertionError('must not stop')), self.assertRaises(SystemExit):
            host.cleanup(self.record)

    def test_bad_guard_observation_still_attempts_cleanup(self):
        self.record['_active_mono'] = time.monotonic() + 20
        result = {'guest_absent': True, 'store_unmounted': True, 'image_absent': True}
        with mock.patch.object(host.sys, 'orig_argv', ['python', '-I', '-S', '-c', 'captured']), \
                mock.patch.object(host, 'authority', return_value=self.record), mock.patch.object(host, 'verify_store'), \
                mock.patch.object(host, 'process_identity', return_value={'pid': 1234}), \
                mock.patch.object(host, 'capture_agent', side_effect=SystemExit('uncertain')), \
                mock.patch.object(host, 'cleanup', return_value=result) as cleanup, self.assertRaises(SystemExit):
            host.main(['guard', '--record', '/unused', '--record-sha256', 'd' * 64])
        cleanup.assert_called_once_with(self.record)
        receipt = json.loads((self.parent / 'host-cleanup.json').read_bytes())
        self.assertEqual(receipt['outcome'], 'fail')
        self.assertEqual(receipt['control_error_type'], 'SystemExit')

    def fake_setup(self, failure=None):
        self.parent.rmdir()
        self.record['_stage_mono'] = time.monotonic() + 300
        self.record.update(schema='durable-workflow.condition-host-setup/v1', host_boot_id=BOOT,
                           guest_agent='/public/guest-agent', cached_image='/public/guest.raw.gz')
        proposal = self.proposal()
        calls = []
        def command(record, argv):
            calls.append(argv)
            if argv == proposal['create_store']:
                if failure == 'create':raise SystemExit('failed native create')
                self.scoped['image'].write_bytes(b'fixed-image')
            if argv == proposal['attach_store'] and failure == 'attach':raise SystemExit('lost attach response')
            return b''
        def copy_input(path, size, sha, destination=None, deadline=None):
            if destination:
                if failure == 'copy':raise OSError('fixed disk full')
                destination.write_bytes(b'pinned-public-image')
        return proposal, calls, command, copy_input

    def test_resident_watchdog_owns_setup_and_never_starts_guest(self):
        proposal, calls, command, copy_input = self.fake_setup()
        with mock.patch.object(host, 'capacity', return_value={'available_bytes': 12 * 1024**3}), \
                mock.patch.object(host, 'input_bytes', side_effect=copy_input), \
                mock.patch.object(host, 'call', side_effect=command), mock.patch.object(host, 'verify_store'), \
                mock.patch.object(host, 'process_identity', return_value={'pid': 1234}), \
                mock.patch.object(host, 'observe_attachment', return_value={'whole_device': '/dev/disk9', 'volume_uuid': BOOT}):
            value, sha = host.prepare(self.record, self.profile)
        self.assertEqual(set(value), host.AUTHORITY_KEYS)
        self.assertEqual(sha, host.digest((self.parent / 'host-record.json').read_bytes()))
        self.assertEqual(calls, [proposal['create_store'], proposal['attach_store']])
        self.assertTrue((self.parent / 'setup-ready.json').exists())
        self.assertEqual(value['profile_sha256'], proposal['profile_sha256'])
        self.assertNotIn('_stage_mono', self.record)

    def test_setup_failures_have_owned_or_explicit_uncertain_custody(self):
        for failure in ('create', 'attach', 'copy'):
            with self.subTest(stage=failure):
                if self.parent.exists():shutil.rmtree(self.parent)
                self.parent.mkdir(mode=0o700)
                _, _, command, copy_input = self.fake_setup(failure)
                with mock.patch.object(host, 'capacity', return_value={}), \
                        mock.patch.object(host, 'input_bytes', side_effect=copy_input), \
                        mock.patch.object(host, 'call', side_effect=command), mock.patch.object(host, 'verify_store'), \
                        mock.patch.object(host, 'process_identity', return_value={'pid': 1234}), \
                        mock.patch.object(host, 'observe_attachment', return_value={'whole_device': '/dev/disk9', 'volume_uuid': BOOT}), \
                        mock.patch.object(host, 'remove_store') as remove, self.assertRaises((SystemExit, OSError)):
                    host.prepare(self.record, self.profile)
                receipt = json.loads((self.parent / 'setup-failure.json').read_bytes())
                self.assertEqual(receipt['outcome'], 'fail')
                if failure == 'create':
                    self.assertEqual(receipt['custody'], 'retained_uncertain_task_store')
                    remove.assert_not_called()
                else:
                    self.assertEqual(receipt['custody'], 'owned_store_removed')
                    remove.assert_called_once()
                    self.assertIn('image_ino', receipt['store_authority'])
                self.record.pop('image_ino', None)
                self.record.pop('image_dev', None)

    def test_capacity_refuses_before_creating_a_disk_or_scope(self):
        self.parent.rmdir()
        with mock.patch.object(host, 'capacity', side_effect=SystemExit('insufficient memory')), \
                mock.patch.object(host, 'call', side_effect=AssertionError('no native mutation')), self.assertRaises(SystemExit):
            host.prepare(self.record, self.profile)
        self.assertFalse(self.parent.exists())

    def test_native_memory_and_disk_readbacks_have_exact_refusal_gates(self):
        def native(record, argv):
            if '-productVersion' in argv:return b'15.8.2\n'
            if '-buildVersion' in argv:return b'24H232\n'
            return b'Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: 2000000.\nPages inactive: 1000000.\nPages speculative: 0.\n'
        with mock.patch.object(host.sys, 'platform', 'darwin'), \
                mock.patch.object(host.os, 'uname', return_value=types.SimpleNamespace(machine='x86_64')), \
                mock.patch.object(host, 'call', side_effect=native), \
                mock.patch.object(host.os, 'statvfs', return_value=types.SimpleNamespace(f_bavail=60 * 1024**3, f_frsize=1)):
            self.assertEqual(host.capacity(self.record)['available_bytes'], 3_000_000 * 4096)
            with mock.patch.object(host.os, 'statvfs', return_value=types.SimpleNamespace(f_bavail=1024, f_frsize=1)), \
                    self.assertRaises(SystemExit):host.capacity(self.record)
        with mock.patch.object(host.sys, 'platform', 'darwin'), \
                mock.patch.object(host.os, 'uname', return_value=types.SimpleNamespace(machine='x86_64')), \
                mock.patch.object(host, 'call', return_value=b'unknown'), self.assertRaises(SystemExit):
            host.capacity(self.record)

    def test_pinned_input_copy_refuses_changed_bytes_existing_destination_and_cutoff(self):
        path = self.parent / 'public';path.write_bytes(b'public immutable input')
        sha = host.digest(path.read_bytes());size = path.stat().st_size
        target = self.parent / 'copied'
        host.input_bytes(path, size, sha, target, time.monotonic() + 2)
        self.assertEqual(path.read_bytes(), target.read_bytes())
        with self.assertRaises(FileExistsError):host.input_bytes(path, size, sha, target)
        for length, digest, deadline in ((size + 1, sha, None), (size, 'f' * 64, None), (size, sha, time.monotonic() - 1)):
            with self.subTest(length=length, digest=digest), self.assertRaises(SystemExit):
                host.input_bytes(path, length, digest, deadline=deadline)

    def test_native_attachment_requires_unique_fixed_device_and_exact_mount(self):
        attachment = {'images': [{'image-path': str(self.scoped['image']), 'system-entities': [
            {'dev-entry': '/dev/disk9'}, {'dev-entry': '/dev/disk10', 'mount-point': str(self.scoped['mount'])}]}]}
        def native(record, argv):
            if 'hdiutil' in argv[0]:return plistlib.dumps(attachment)
            if argv[-1] == '/dev/disk9':return plistlib.dumps({'TotalSize': host.STORE_BYTES})
            if argv[-1] == '/dev/disk10':return plistlib.dumps({'TotalSize': host.STORE_BYTES - 4096})
            return plistlib.dumps({'FilesystemType': 'apfs', 'MountPoint': str(self.scoped['mount']),
                                  'TotalSize': host.STORE_BYTES - 4096, 'VolumeUUID': BOOT})
        with mock.patch.object(host, 'call', side_effect=native):
            self.assertEqual(host.observe_attachment(self.record), {'whole_device': '/dev/disk9', 'volume_uuid': BOOT})
            attachment['images'][0]['system-entities'][1]['mount-point'] = '/foreign'
            with self.assertRaises(SystemExit):host.observe_attachment(self.record)


if __name__ == '__main__':
    unittest.main()
