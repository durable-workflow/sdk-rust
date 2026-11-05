"""External guest controls use fake native commands. No VM or disk is created."""
import copy
import ctypes
import json
import os
import pathlib
import plistlib
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock
from test_subject import HERE, load, parent_clock

host = load('condition_guest_host_tests', HERE / 'guest-host.py')
TOKEN = '0123456789abcdef'
BOOT = '11111111-1111-1111-1111-111111111111'


class GuestHost(unittest.TestCase):
    def setUp(self):
        self.temp = pathlib.Path(tempfile.mkdtemp(dir='/tmp', prefix='sr-')).resolve()
        self.addCleanup(shutil.rmtree, self.temp)
        self.parent = self.temp / ('sr-' + TOKEN)
        self.parent.mkdir(mode=0o700)
        active_ns = time.monotonic_ns() + 1740 * 1_000_000_000
        self.record = {'active_monotonic_ns': active_ns, 'cleanup_monotonic_ns': active_ns + 60_000_000_000,
            'parent': str(self.parent), 'token': TOKEN, 'limactl': '/reviewed/bin/limactl',
            'runner_commit': 'a' * 40, 'manifest_sha256': 'b' * 64, 'guardian_sha256': 'c' * 64,
            'guest_agent_binary_sha256': 'e' * 64,
            'started_unix': int(time.time()), 'active_unix': int(time.time()) + 1740,
            'cleanup_unix': int(time.time()) + 1800, '_cleanup_mono': time.monotonic() + 1800,
            '_active_mono': time.monotonic() + 1740}
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
        clock = parent_clock(self.record['started_unix'])
        self.record['host_boot_id'] = clock['host_boot_id']
        for key in ('started_unix', 'active_unix', 'cleanup_unix', 'active_monotonic_ns', 'cleanup_monotonic_ns'):clock['host_'+key] = self.record[key]
        self.record['_clock_sha256'] = self.put('guest-clock.json', clock)
        storage = {'guest_boot_id': BOOT}
        summary_sha = self.put('guest-summary.json', {'runner_commit': self.record['runner_commit'],
            'manifest_sha256': self.record['manifest_sha256'], 'checks_outcome': 'pass', 'storage': storage, 'parent_clock': clock})
        audit_sha = self.put('guest-audit.json', {'outcome': 'pending_external_guest_teardown',
            'manifest_sha256': self.record['manifest_sha256'], 'summary_sha256': summary_sha, 'storage': storage, 'parent_clock': clock})
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
        self.put('vm-started.json', {'record_sha256': 'd' * 64,
            'agent': {'guest_boot_id': BOOT, 'sha256': self.record['guest_agent_binary_sha256']}})
        self.put('host-cleanup.json', {'record_sha256': 'd' * 64, 'guardian_sha256': 'c' * 64,
            'outcome': 'pending_independent_host_audit', 'guest_absent': True,
            'store_unmounted': True, 'image_absent': True, 'clock_sha256': self.record['_clock_sha256']})

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
                ('guest-audit.json', 'outcome', 'pass'), ('host-cleanup.json', 'guardian_sha256', 'foreign'),
                ('vm-started.json', 'agent', {'guest_boot_id': BOOT, 'sha256': 'foreign'})]:
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
                           guest_agent='/reviewed/share/lima/lima-guestagent.Linux-x86_64.gz', cached_image='/public/guest.raw.gz')
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
                mock.patch.object(host, 'bound_input', return_value={'device': 1, 'inode': 2, 'bytes': host.IMAGE_BYTES, 'sha256': host.IMAGE_SHA}), \
                mock.patch.object(host, 'agent_binary_sha', return_value='e' * 64), \
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
                        mock.patch.object(host, 'bound_input', return_value={'device': 1, 'inode': 2, 'bytes': host.IMAGE_BYTES, 'sha256': host.IMAGE_SHA}), \
                        mock.patch.object(host, 'agent_binary_sha', return_value='e' * 64), \
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

    def held(self):
        data = subprocess.check_output(['git', '--no-replace-objects', '-C', str(HERE.parents[2]), 'show',
            'b5cf1e9df9b3ffe88e3fa3290074ae10a8422171:scripts/acceptance/condition-history/guest-host.py'])
        module = types.ModuleType('held_b5_guest_source')
        exec(compile(data, 'git:held-b5-guardian', 'exec'), module.__dict__)
        return module

    def store_fixture(self):
        self.scoped['home'].mkdir(parents=True)
        self.scoped['image'].write_bytes(b'fixed-image')
        self.record['profile'] = self.proposal()['profile']
        self.record['profile_sha256'] = self.put('unused-profile.json', self.record['profile'])
        (self.scoped['mount'] / 'guest.yaml').write_bytes(host.canonical(self.record['profile']))
        info = self.scoped['image'].stat()
        self.record.update(image_dev=info.st_dev, image_ino=info.st_ino, whole_device='/dev/disk9',
                           volume_uuid=BOOT, host_boot_id=BOOT)
        def native(record, argv):
            if 'hdiutil' in argv[0]:
                return plistlib.dumps({'images': [{'image-path': str(self.scoped['image']),
                    'system-entities': [{'dev-entry': '/dev/disk9', 'mount-point': str(self.scoped['mount'])}]}]})
            if argv[-1] == '/dev/disk9':return plistlib.dumps({'TotalSize': host.STORE_BYTES})
            return plistlib.dumps({'VolumeUUID': BOOT, 'TotalSize': host.STORE_BYTES - 4096,
                'MountPoint': str(self.scoped['mount']), 'FilesystemType': 'apfs'})
        return native

    def test_actual_held_store_check_admits_destination_corruption(self):
        old = self.held();native = self.store_fixture()
        target = self.scoped['mount'] / 'a/guest.raw.gz';target.parent.mkdir();target.write_bytes(b'old')
        self.record['guest_image'] = host.bound_input(target, 3, host.digest(b'old'))
        target.write_bytes(b'bad')
        with mock.patch.object(old, 'call', side_effect=native):old.verify_store(self.record)
        with mock.patch.object(host, 'IMAGE_BYTES', 3), mock.patch.object(host, 'IMAGE_SHA', host.digest(b'old')), \
                self.assertRaisesRegex(SystemExit, 'digest'):
            host.verify_guest_inputs(self.record)
        replacement = target.with_name('replacement');replacement.write_bytes(b'old');replacement.replace(target)
        with mock.patch.object(old, 'call', side_effect=native):old.verify_store(self.record)
        with mock.patch.object(host, 'IMAGE_BYTES', 3), mock.patch.object(host, 'IMAGE_SHA', host.digest(b'old')), \
                self.assertRaisesRegex(SystemExit, 'identity'):
            host.verify_guest_inputs(self.record)

    def test_actual_held_plan_admits_profile_edit_restore_but_new_create_consumes_captured_stdin(self):
        old = self.held();native = self.store_fixture()
        legacy = old.plan(str(self.parent), TOKEN, self.record['limactl'],
            str(self.scoped['mount'] / 'a/guest.raw.gz'), self.profile)
        path = self.scoped['mount'] / 'guest.yaml';reviewed = path.read_bytes()
        foreign = json.loads(reviewed);foreign['mounts'] = [{'location': '/foreign/host'}]
        path.write_bytes(host.canonical(foreign))
        self.assertEqual(json.loads(pathlib.Path(legacy['create_vm'][-1]).read_bytes())['mounts'], foreign['mounts'])
        path.write_bytes(reviewed)
        with mock.patch.object(old, 'call', side_effect=native):old.verify_store(self.record)
        self.record['guest_image'] = {'device': 1, 'inode': 2, 'bytes': 3, 'sha256': 'f' * 64}
        self.put('launch.json', {'record_sha256': 'd' * 64, 'manifest_sha256': self.record['manifest_sha256'], 'action': 'create-and-start'})
        consumed = []
        def command(record, argv, input_data=None):
            if 'create' in argv:
                path.write_bytes(host.canonical(foreign))
                consumed.append((argv, input_data))
                path.write_bytes(reviewed)
            if 'shell' in argv:
                return host.canonical({'pid': 100, 'start_ticks': 1000, 'guest_boot_id': BOOT,
                                       'sha256': self.record['guest_agent_binary_sha256']})
            return b''
        with mock.patch.object(host, 'verify_store'), mock.patch.object(host, 'verify_guest_inputs'), \
                mock.patch.object(host, 'call', side_effect=command), \
                mock.patch.object(host, 'effective_config', side_effect=[{'status': 'Stopped'}, {'status': 'Stopped'}, {'status': 'Running'}]), \
                mock.patch.object(host, 'capture_agent', return_value={'pid': 2345}):
            host.launch_guest(self.record, 'd' * 64)
        self.assertEqual(consumed[0][0][-1], '-')
        self.assertEqual(consumed[0][1], reviewed)
        self.assertEqual(json.loads(consumed[0][1])['mounts'], [])
        self.assertTrue((self.parent / 'vm-started.json').exists())

    def test_actual_held_authority_accepts_correct_hash_at_unselected_agent_path(self):
        old = self.held();self.store_fixture()
        binary = self.temp / 'tools/bin/limactl';binary.parent.mkdir(parents=True);binary.write_bytes(b'launcher')
        unrelated = self.temp / 'unselected-agent.gz';unrelated.write_bytes(b'pinned agent bytes')
        value = {key: self.record.get(key) for key in old.AUTHORITY_KEYS}
        value.update(schema='durable-workflow.condition-host/v1', limactl=str(binary), guest_agent=str(unrelated))
        sha = self.put('legacy-record.json', value)
        with mock.patch.object(old, 'LIMA_SHA', host.digest(binary.read_bytes())), \
                mock.patch.object(old, 'AGENT_SHA', host.digest(unrelated.read_bytes())), \
                mock.patch.object(old, 'boot_identity', return_value=BOOT):
            accepted = old.authority(self.parent / 'legacy-record.json', sha, self.record['guardian_sha256'])
        with self.assertRaisesRegex(SystemExit, 'not the executable-selected'):
            host.selected_agent(accepted)

    def test_effective_config_refuses_resource_share_image_and_forwarding_changes(self):
        self.record['profile'] = self.proposal()['profile']
        expected = {k: v for k, v in self.record['profile'].items() if k not in ('base', 'minimumLimaVersion')}
        value = {'name': self.scoped['name'], 'dir': str(self.scoped['instance']), 'status': 'Stopped',
                 'vmType': 'vz', 'arch': 'x86_64', 'cpus': 4, 'memory': host.MEMORY_BYTES,
                 'disk': host.GUEST_BYTES, 'config': expected}
        with mock.patch.object(host, 'call', return_value=host.canonical(value)):
            self.assertEqual(host.effective_config(self.record, 'Stopped'), value)
        for field, change in [('mounts', [{'location': '/foreign'}]), ('portForwards', []),
                              ('images', []), ('ssh', dict(expected['ssh'], forwardAgent=True))]:
            changed = copy.deepcopy(value);changed['config'][field] = change
            with self.subTest(field=field), mock.patch.object(host, 'call', return_value=host.canonical(changed)), \
                    self.assertRaises(SystemExit):host.effective_config(self.record, 'Stopped')
        for key, change in [('cpus', 8), ('memory', host.MEMORY_BYTES + 1), ('disk', host.GUEST_BYTES + 1)]:
            changed = copy.deepcopy(value);changed[key] = change
            with self.subTest(key=key), mock.patch.object(host, 'call', return_value=host.canonical(changed)), \
                    self.assertRaises(SystemExit):host.effective_config(self.record, 'Stopped')

    def test_launch_refuses_unbound_request_before_native_create(self):
        self.put('launch.json', {'action': 'create-and-start', 'record_sha256': 'foreign'})
        with mock.patch.object(host, 'call', side_effect=AssertionError('must not create')), self.assertRaises(SystemExit):
            host.launch_guest(self.record, 'd' * 64)

    def test_actual_held_agent_capture_accepts_unselected_path_but_new_capture_refuses(self):
        old = self.held()
        self.scoped['instance'].mkdir(parents=True)
        self.record['guest_agent'] = '/reviewed/share/lima/lima-guestagent.Linux-x86_64.gz'
        (self.scoped['instance'] / 'ha.pid').write_text('2345\n')
        process = {'pid': 2345, 'start_seconds': self.record['started_unix'], 'start_microseconds': 123}
        command = [self.record['limactl'], 'hostagent', '--pidfile', str(self.scoped['instance'] / 'ha.pid'),
                   '--guestagent', '/unselected/agent.gz', self.scoped['name']]
        import shlex
        raw = shlex.join(command).encode()
        with mock.patch.object(old, 'call', return_value=raw), mock.patch.object(old, 'process_identity', return_value=process):
            self.assertEqual(old.capture_agent(self.record), process)
        with mock.patch.object(host, 'call', return_value=raw), mock.patch.object(host, 'process_identity', return_value=process), \
                self.assertRaisesRegex(SystemExit, 'foreign'):
            host.capture_agent(self.record)
        command[command.index('--guestagent') + 1] = self.record['guest_agent']
        with mock.patch.object(host, 'call', return_value=shlex.join(command).encode()), \
                mock.patch.object(host, 'process_identity', return_value=process):
            self.assertEqual(host.capture_agent(self.record), process)

    def test_running_guest_agent_kernel_hash_must_match_pinned_selected_binary(self):
        self.store_fixture()
        self.record['guest_image'] = {'device': 1, 'inode': 2, 'bytes': 3, 'sha256': 'f' * 64}
        self.put('launch.json', {'record_sha256': 'd' * 64, 'manifest_sha256': self.record['manifest_sha256'], 'action': 'create-and-start'})
        def command(record, argv, input_data=None):
            if 'shell' in argv:return host.canonical({'pid': 100, 'start_ticks': 1000, 'guest_boot_id': BOOT, 'sha256': 'wrong'})
            return b''
        with mock.patch.object(host, 'verify_store'), mock.patch.object(host, 'verify_guest_inputs'), \
                mock.patch.object(host, 'call', side_effect=command), \
                mock.patch.object(host, 'effective_config', side_effect=[{'status': 'Stopped'}, {'status': 'Stopped'}, {'status': 'Running'}]), \
                mock.patch.object(host, 'capture_agent', return_value={'pid': 2345}), \
                self.assertRaisesRegex(SystemExit, 'running guest agent'):
            host.launch_guest(self.record, 'd' * 64)
        self.assertTrue((self.parent / 'vm-created.json').exists())
        self.assertFalse((self.parent / 'vm-started.json').exists())


    def test_late_budget_refuses_before_store_allocation_and_guest_create(self):
        self.record['_active_mono']=time.monotonic()+779
        self.record['_stage_mono']=time.monotonic()+300
        with mock.patch.object(host,'capacity',return_value={}),mock.patch.object(host,'selected_agent'), \
                mock.patch.object(host,'input_bytes'),mock.patch.object(host,'call') as command, \
                self.assertRaisesRegex(SystemExit,'reserve consumed'):
            host.prepare(self.record,self.profile)
        command.assert_not_called()
        self.put('launch.json',{'record_sha256':'d'*64,'manifest_sha256':self.record['manifest_sha256'],'action':'create-and-start'})
        self.record['profile']=self.proposal()['profile'];self.record['profile_sha256']=host.digest(host.canonical(self.record['profile']))
        with mock.patch.object(host,'verify_store'),mock.patch.object(host,'verify_guest_inputs'), \
                mock.patch.object(host,'call') as command,self.assertRaisesRegex(SystemExit,'reserve consumed'):
            host.launch_guest(self.record,'d'*64)
        command.assert_not_called()

    def test_resident_clock_receipt_binds_original_parent_and_guest_domains(self):
        self.record['host_boot_id']=BOOT
        self.put('guest-clock-request.json',{'record_sha256':'d'*64,'action':'capture-guest-clock'})
        self.put('vm-started.json',{'agent':{'guest_boot_id':BOOT}})
        mono=time.monotonic_ns()
        sample={'guest_boot_id':BOOT,'guest_before_monotonic_ns':900000000000,
                'guest_after_monotonic_ns':900000000010,'guest_unix_ns':2000000000000000000}
        with mock.patch.object(host,'call',return_value=host.canonical(sample)), \
                mock.patch.object(host.time,'monotonic_ns',side_effect=[mono,mono+2000000000]):
            clock=host.capture_guest_clock(self.record,'d'*64)
        self.assertEqual(clock['host_record_sha256'],'d'*64)
        self.assertEqual(clock['host_active_monotonic_ns'],self.record['active_monotonic_ns'])
        self.assertEqual(clock['host_cleanup_monotonic_ns'],self.record['cleanup_monotonic_ns'])
        self.assertEqual(clock['host_after_monotonic_ns']-clock['host_before_monotonic_ns'],2000000000)
        self.assertEqual(clock['guest_unix_ns'],sample['guest_unix_ns'])
        self.assertEqual(json.loads((self.parent/'guest-clock.json').read_bytes()),clock)
        self.assertEqual(self.record['_clock_sha256'], host.digest(host.canonical(clock)))
        with mock.patch.object(host,'call',return_value=host.canonical(sample)),self.assertRaises(FileExistsError):
            host.capture_guest_clock(self.record,'d'*64)

    def test_budget_consumed_by_input_preflight_refuses_before_scope_creation(self):
        # Read-only inspection must not make an initially adequate budget stale.
        self.parent.rmdir()
        self.record['_stage_mono'] = time.monotonic() + 300
        self.record.update(guest_agent='/synthetic/guest-agent.gz', cached_image='/synthetic/guest.raw.gz')
        def inspect(*args, **kwargs):
            self.record['_active_mono'] = time.monotonic() + 779
        with mock.patch.object(host, 'capacity', return_value={}), \
                mock.patch.object(host, 'selected_agent'), mock.patch.object(host, 'input_bytes', side_effect=inspect), \
                mock.patch.object(host, 'call') as command, self.assertRaisesRegex(SystemExit, 'reserve consumed'):
            host.prepare(self.record, self.profile)
        command.assert_not_called()
        self.assertFalse(self.parent.exists())

    def test_clock_probe_refuses_wrong_boot_slow_sample_or_late_budget(self):
        self.record['host_boot_id']=BOOT
        self.put('guest-clock-request.json',{'record_sha256':'d'*64,'action':'capture-guest-clock'})
        self.put('vm-started.json',{'agent':{'guest_boot_id':BOOT}})
        sample={'guest_boot_id':'foreign','guest_before_monotonic_ns':100,'guest_after_monotonic_ns':200,'guest_unix_ns':1000}
        with mock.patch.object(host,'call',return_value=host.canonical(sample)),self.assertRaises(SystemExit):
            host.capture_guest_clock(self.record,'d'*64)
        sample['guest_boot_id']=BOOT
        mono=time.monotonic_ns()
        with mock.patch.object(host,'call',return_value=host.canonical(sample)), \
                mock.patch.object(host.time,'monotonic_ns',side_effect=[mono,mono+6000000000]),self.assertRaises(SystemExit):
            host.capture_guest_clock(self.record,'d'*64)
        self.record['_active_mono']=time.monotonic()+779
        with mock.patch.object(host,'call') as command,self.assertRaises(SystemExit):
            host.capture_guest_clock(self.record,'d'*64)
        command.assert_not_called();self.assertFalse((self.parent/'guest-clock.json').exists())

    def test_final_host_audit_rejects_rebound_clock_even_with_matching_exports(self):
        self.audit_files()
        clock=json.loads((self.parent/'guest-clock.json').read_bytes());clock['host_active_unix']+=1
        self.put('guest-clock.json',clock)
        digests={}
        for name in ('guest-summary.json','guest-audit.json'):
            value=json.loads((self.parent/name).read_bytes());value['parent_clock']=clock
            digests[name]=self.put(name,value)
        export_sha=self.put('exports.json',dict(record_sha256='d'*64,guest_boot_id=BOOT,**digests))
        self.put('finish.json',{'record_sha256':'d'*64,'exports_sha256':export_sha})
        with mock.patch.object(host,'process_identity',return_value=None),self.assertRaisesRegex(SystemExit,'clock'):
            host.audit(self.record,'d'*64,'c'*64)

    def test_actual_clock_draft_admits_rebound_samples_but_resident_digest_refuses(self):
        self.audit_files()
        old_source = subprocess.check_output(['git', 'show',
            'cc359c063085cfa6ef9227c7e216833c47b231dd:scripts/acceptance/condition-history/guest-host.py'], text=True)
        old = types.ModuleType('unbound_clock_draft')
        exec(compile(old_source, 'git:cc359c0:guest-host.py', 'exec'), old.__dict__)
        clock = json.loads((self.parent / 'guest-clock.json').read_bytes())
        clock['host_after_monotonic_ns'] -= 100_000_000_000
        self.put('guest-clock.json', clock)
        digests = {}
        for name in ('guest-summary.json', 'guest-audit.json'):
            value = json.loads((self.parent / name).read_bytes());value['parent_clock'] = clock
            if name == 'guest-audit.json':
                value['summary_sha256'] = digests['guest-summary.json']
            digests[name] = self.put(name, value)
        export_sha = self.put('exports.json', dict(record_sha256='d'*64, guest_boot_id=BOOT, **digests))
        self.put('finish.json', {'record_sha256':'d'*64, 'exports_sha256':export_sha})
        self.assertTrue(old.finish_requested(self.record, 'd'*64))
        with self.assertRaisesRegex(SystemExit, 'clock receipt changed'):
            host.finish_requested(self.record, 'd'*64)
        with mock.patch.object(host, 'process_identity', return_value=None), \
                mock.patch.object(old, 'process_identity', return_value=None):
            self.assertEqual(old.audit(self.record, 'd'*64, 'c'*64)['outcome'], 'pass')
            (self.parent / 'host-audit.json').unlink()
            with self.assertRaisesRegex(SystemExit, 'clock receipt changed'):
                host.audit(self.record, 'd'*64, 'c'*64)


    def test_reopening_host_authority_never_renews_monotonic_ceilings(self):
        self.store_fixture()
        binary=self.temp/'launcher';binary.write_bytes(b'launcher')
        agent=self.temp/'agent.gz';agent.write_bytes(b'agent')
        value={key:self.record.get(key) for key in host.AUTHORITY_KEYS}
        value.update(schema='durable-workflow.condition-host/v2',limactl=str(binary),guest_agent=str(agent),
                     guest_agent_binary_sha256=host.AGENT_BINARY_SHA)
        sha=self.put('clock-authority.json',value)
        with mock.patch.object(host,'LIMA_SHA',host.digest(binary.read_bytes())), \
                mock.patch.object(host,'AGENT_SHA',host.digest(agent.read_bytes())), \
                mock.patch.object(host,'selected_agent'),mock.patch.object(host,'boot_identity',return_value=BOOT), \
                mock.patch.object(host.time,'time',return_value=self.record['started_unix']-3600):
            observed=host.authority(self.parent/'clock-authority.json',sha,self.record['guardian_sha256'])
        self.assertEqual(observed['_active_mono'],value['active_monotonic_ns']/1000000000)
        self.assertEqual(observed['_cleanup_mono'],value['cleanup_monotonic_ns']/1000000000)


    def test_budget_consumed_during_create_refuses_start_without_renewal(self):
        self.store_fixture()
        self.record['guest_image']={'device':1,'inode':2,'bytes':3,'sha256':'f'*64}
        self.put('launch.json',{'record_sha256':'d'*64,'manifest_sha256':self.record['manifest_sha256'],'action':'create-and-start'})
        calls=[]
        def command(record,argv,input_data=None):
            calls.append(argv)
            if 'create' in argv:record['_active_mono']=time.monotonic()+779
            return b''
        original=self.record['active_monotonic_ns']
        with mock.patch.object(host,'verify_store'),mock.patch.object(host,'verify_guest_inputs'), \
                mock.patch.object(host,'call',side_effect=command), \
                mock.patch.object(host,'effective_config',return_value={'status':'Stopped'}), \
                self.assertRaisesRegex(SystemExit,'reserve consumed'):
            host.launch_guest(self.record,'d'*64)
        self.assertEqual(len(calls),1);self.assertIn('create',calls[0]);self.assertNotIn('start',calls[0])
        self.assertEqual(self.record['active_monotonic_ns'],original)


if __name__ == '__main__':
    unittest.main()
