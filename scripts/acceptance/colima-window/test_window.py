"""Inert lifecycle faults. Native service, Docker and VM calls are all fake."""
import copy
import importlib.util
import json
import os
import pathlib
import plistlib
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('colima_window_controls', HERE / 'window.py')
window = importlib.util.module_from_spec(spec)
spec.loader.exec_module(window)
CODE = (HERE.parent / 'condition-history/guest-host.py').read_text()
SOURCE = window.sha((HERE / 'window.py').read_bytes())
TOKEN = '0123456789abcdef'
BOOT = '11111111-1111-1111-1111-111111111111'


class Child:
    pid = 4321
    alive = True

    def poll(self):return None if self.alive else 0
    def terminate(self):self.alive = False
    def wait(self, timeout):self.alive = False


class WindowControls(unittest.TestCase):
    def setUp(self):
        self.temp = pathlib.Path(tempfile.mkdtemp(prefix='sw-', dir='/tmp')).resolve()
        self.addCleanup(shutil.rmtree, self.temp)
        self.root = self.temp / ('sdk-rust-window-' + TOKEN)
        self.root.mkdir(mode=0o700)
        self.sdk = window.load_sdk(CODE)
        self.owner_pid = 3456
        def epoch(pid):
            return dict(pid=pid, pgid=pid, uid=os.getuid(), start_seconds=1000,
                        start_microseconds=123456, boot_id=BOOT)
        self.epoch = epoch
        self.processes = {pid:epoch(pid) for pid in (self.owner_pid, os.getpid(), 5678)}
        self.sdk.process_identity = lambda pid:copy.deepcopy(self.processes.get(pid))
        self.sdk.boot_identity = lambda:BOOT
        self.loaded, self.healthy, self.controller = True, True, True
        self.calls, self.bootstraps, self.notices = [], [], []
        self.native_fault, self.bad_properties, self.bad_arguments = None, False, False
        self.original_files = {}
        tools = {}
        for name in ('python', 'launchctl', 'plutil', 'colima', 'docker', 'dm'):
            path = self.temp / name
            path.write_bytes(('fake executable ' + name).encode())
            tools[name] = self.bind(path)
        # Native absolute launchctl/plutil paths have synthetic, checked bytes.
        for name, path in [('launchctl', '/bin/launchctl'), ('plutil', '/usr/bin/plutil')]:
            tools[name]['path'] = path
            self.original_files[path] = ('fake executable ' + name).encode()
        verify = window.verify_file
        def fake_system_file(sdk, binding):
            if binding['path'] in self.original_files:
                raw = self.original_files[binding['path']]
                window.require(set(binding) == {'path', 'bytes', 'sha256'} and len(raw) == binding['bytes']
                               and window.sha(raw) == binding['sha256'], 'native tool changed')
            else:verify(sdk, binding)
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(window, 'verify_file', side_effect=fake_system_file).start()
        home = self.temp / 'profile'
        configs = []
        for suffix in ('default/colima.yaml', '_lima/colima/lima.yaml'):
            path = home / suffix;path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'unchanged original configuration\n');configs.append(self.bind(path))
        disk = home / 'default/diffdisk';disk.write_bytes(b'original disk')
        info = disk.stat()
        plist = self.temp / 'original.plist'
        self.original_plist = plistlib.dumps({'Label':'org.example.colima', 'RunAtLoad':True,
            'KeepAlive':{'SuccessfulExit':False}, 'ProgramArguments':[tools['colima']['path'], 'start', '-f', '--cpu', '4', '--memory', '6', '--disk', '60', '--vm-type', 'vz', '--mount-type', 'virtiofs'],
            'EnvironmentVariables':{'COLIMA_HOME':str(home), 'PATH':'/original/bin:/usr/bin:/bin'}})
        plist.write_bytes(self.original_plist)
        started = int(time.time())
        task = {key:'unused' for key in self.sdk.INTENT_KEYS}
        task.update(schema='durable-workflow.condition-host-setup/v1', token=TOKEN,
            runner_commit=window.SDK_R, manifest_sha256=window.SDK_M, guardian_sha256=window.SDK_CODE_SHA,
            started_unix=started + 120, active_unix=started + 1860, cleanup_unix=started + 1920,
            parent=str(self.temp / ('sr-' + TOKEN)), host_boot_id=BOOT)
        self.engine = {'info':{'ID':'original-engine', 'ServerVersion':'29.5.2', 'Architecture':'x86_64',
            'OSType':'linux', 'NCPU':4, 'DockerRootDir':'/var/lib/docker'},
            'memory_ceiling_bytes':6 * 1024**3, 'images':['sha256:public'], 'volumes':['original-volume']}
        self.record = dict(schema='durable-workflow.colima-window/v1', token=TOKEN, source_sha256=SOURCE,
            host_boot_id=BOOT, uid=os.getuid(), scope=str(self.root), owner_process=epoch(self.owner_pid),
            started_unix=started, sdk_intent=task, tools=tools, profile_home=str(home),
            service=dict(label='org.example.colima', plist=self.bind(plist),
                arguments=plistlib.loads(self.original_plist)['ProgramArguments'], keep_alive={'SuccessfulExit':False},
                properties='runatload | inferred program', vm_processes=[epoch(5678)]),
            config_files=configs, disks=[dict(path=str(disk), device=info.st_dev, inode=info.st_ino)],
            docker_host='unix://' + str(home / 'default/docker.sock'), engine=copy.deepcopy(self.engine))
        self.intent_sha = window.sha(window.canonical(self.record))
        self.w = window.Window(self.sdk, self.record, self.intent_sha, CODE)
        self.w.call = self.call
        self.put('go.json', {'intent_sha256':self.intent_sha, 'action':'one-window'})

    def bind(self, path):
        raw = path.read_bytes()
        return dict(path=str(path), bytes=len(raw), sha256=window.sha(raw))

    def put(self, name, value):
        (self.root / name).write_bytes(window.canonical(value))

    def ready(self):
        self.put('window-ready.json', dict(intent_sha256=self.intent_sha,
            source_sha256=SOURCE, process=self.epoch(os.getpid())))

    def call(self, argv, cutoff, *, pass_fds=(), stop=False, check=True):
        window.require(cutoff > time.monotonic(), 'fake fixed cutoff expired')
        self.calls.append(argv)
        name = pathlib.Path(argv[0]).name
        if name == self.native_fault:raise SystemExit('injected native fault')
        args = argv[1:]
        if name == 'launchctl':
            if args[0] == 'print':
                controller = args[1].endswith(window.job_label(self.record))
                if not (self.controller if controller else self.loaded):
                    return 113, b'', b'Could not find service'
                pid = os.getpid() if controller else 6789
                program = self.record['tools']['python' if controller else 'colima']['path']
                properties = 'wrong' if self.bad_properties else self.record['service']['properties']
                # Nested fields and source text must never become root authority.
                return 0, (f'job = {{\n\tprogram = {program}\n\tpid = {pid}\n'
                    f'\tproperties = {properties}\n\targuments = {{\n\t\tpid = 9999\n'
                    '\t\tprogram = attacker\n    pid = 8888\n\t}\n}\n').encode(), b''
            if args[0] == 'bootout':self.loaded = False
            elif args[0] == 'bootstrap':
                descriptor = int(args[-1].split('/')[-1]);self.assertIn(descriptor, pass_fds)
                os.lseek(descriptor, 0, os.SEEK_SET)
                self.bootstraps.append(os.read(descriptor, 512 * 1024))
                self.loaded = True
            elif args[0] == 'kickstart':self.healthy = True
            elif args[0] == 'remove':self.controller = False
            else:self.failTest('unexpected service mutation ' + str(args))
        elif name == 'colima':
            self.assertEqual(args, ['--profile', 'default', 'stop']);self.assertTrue(stop)
            self.healthy = False;self.processes.pop(5678, None)
        elif name == 'ps':
            values = ['different'] if self.bad_arguments else self.record['service']['arguments']
            return 0, (' '.join(values)).encode(), b''
        elif name == 'docker':
            self.assertEqual(args[:2], ['--host', self.record['docker_host']])
            window.require(self.healthy, 'fake original engine unavailable')
            operation = args[2:]
            if operation[0] == 'info':return 0, window.canonical(dict(self.engine['info'], MemTotal=6 * 1024**3)), b''
            if operation[0] == 'ps':return 0, b'', b''
            if operation[0] == 'image':return 0, '\n'.join(self.engine['images']).encode(), b''
            if operation[0] == 'volume':return 0, '\n'.join(self.engine['volumes']).encode(), b''
            self.failTest('unexpected Docker operation')
        elif name == 'dm':self.notices.append(args[0])
        elif name != 'plutil':self.failTest('unexpected native command ' + name)
        return 0, b'', b''

    def run_fake_task(self, *, survive=False, fail=False):
        def start(profile):
            self.child = Child();self.w.child = self.child
            self.processes.pop(self.owner_pid, None)  # hard owner stop after handoff
        def monitor():
            if not survive:self.child.alive = False
            if fail:raise SystemExit('injected task failure')
        self.w.start_task = start;self.w.monitor_task = monitor
        self.w.run(SOURCE, {})
        return json.loads((self.root / 'window-result.json').read_bytes())

    def test_frozen_source_is_hash_bound(self):
        self.assertEqual(window.sha(CODE.encode()), window.SDK_CODE_SHA)
        with self.assertRaises(SystemExit):window.load_sdk(CODE + '\n')

    def test_complete_private_record_validates(self):
        window.validate(self.sdk, self.record, SOURCE)
        self.assertEqual(self.w.engine(), self.record['engine'])

    def test_closed_source_epoch_cutoffs_and_scope(self):
        mutations = [lambda r:r.update(extra=True), lambda r:r.update(source_sha256='f'*64),
            lambda r:r.update(host_boot_id='old-boot'), lambda r:r['owner_process'].update(start_microseconds=1_000_000),
            lambda r:r['sdk_intent'].update(runner_commit='f'*40),
            lambda r:r['sdk_intent'].update(cleanup_unix=r['sdk_intent']['cleanup_unix']+1),
            lambda r:r['tools']['launchctl'].update(path='/tmp/launchctl'),
            lambda r:r['service'].update(keep_alive=True),
            lambda r:r.update(docker_host='tcp://127.0.0.1:2375'),
            lambda r:r.update(scope=str(self.temp)), lambda r:r.update(config_files=[])]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                candidate = copy.deepcopy(self.record);mutate(candidate)
                with self.assertRaises(SystemExit):window.validate(self.sdk, candidate, SOURCE)

    def test_original_tool_config_plist_and_disk_drift_refuse_before_stop(self):
        paths = [self.record['tools']['docker']['path'], self.record['config_files'][0]['path'],
                 self.record['service']['plist']['path'], self.record['disks'][0]['path']]
        for name in paths:
            with self.subTest(path=name):
                path = pathlib.Path(name);raw = path.read_bytes()
                if name == self.record['disks'][0]['path']:
                    moved = path.with_suffix('.held');path.rename(moved);path.write_bytes(raw)
                else:path.write_bytes(raw + b'changed')
                self.ready()
                with self.assertRaises(SystemExit):self.w.stop_default()
                self.assertFalse(self.w.stop_attempted)
                path.unlink()
                if name == self.record['disks'][0]['path']:moved.rename(path)
                else:path.write_bytes(raw)
        self.assertFalse(any('bootout' in call for call in self.calls))

    def test_missing_or_tampered_outside_ready_refuses_stop(self):
        with self.assertRaises(FileNotFoundError):self.w.stop_default()
        self.ready();value = json.loads((self.root/'window-ready.json').read_bytes())
        for key, replacement in [('source_sha256','f'*64), ('intent_sha256','f'*64), ('process',self.epoch(9999))]:
            with self.subTest(key=key):
                self.put('window-ready.json', dict(value, **{key:replacement}))
                with self.assertRaises(SystemExit):self.w.stop_default()
        self.assertFalse(self.w.stop_attempted)

    def test_kernel_owner_loss_and_pid_reuse_refuse_stop(self):
        self.ready()
        for process in (None, dict(self.epoch(self.owner_pid), start_microseconds=123457)):
            self.processes[self.owner_pid] = process
            self.assertFalse(self.w.owner_alive())
            with self.assertRaises(SystemExit):self.w.stop_default()
        self.assertFalse(self.w.stop_attempted)

    def test_effective_service_arguments_properties_and_engine_drift_refuse(self):
        self.ready()
        for field in ('bad_arguments','bad_properties'):
            setattr(self, field, True)
            with self.assertRaises(SystemExit):self.w.stop_default()
            setattr(self, field, False)
        self.engine['images'].append('foreign-image')
        with self.assertRaises(SystemExit):self.w.stop_default()
        self.assertFalse(self.w.stop_attempted)

    def test_top_level_job_schema_ignores_nested_source_fields(self):
        self.assertEqual(self.w.job('org.example.colima')['pid'], 6789)
        self.assertEqual(self.w.service_matches()['program'], self.record['tools']['colima']['path'])

    def test_original_vm_epoch_and_fixed_stop_cutoff_refuse(self):
        self.ready();self.processes[5678]['start_microseconds'] += 1
        with self.assertRaises(SystemExit):self.w.stop_default()
        self.processes[5678] = self.epoch(5678);self.w.stop_cutoff = time.monotonic()-1
        with self.assertRaises(SystemExit):self.w.stop_default()
        self.assertFalse(self.w.stop_attempted)

    def test_hard_owner_stop_leaves_independent_restore_and_controller_cleanup(self):
        with mock.patch.object(window.subprocess, 'Popen', side_effect=AssertionError('no native experiment')):
            result = self.run_fake_task()
        self.assertEqual(result['outcome'], 'pending_independent_audit')
        self.assertTrue(result['task_absent']);self.assertTrue(self.loaded and self.healthy)
        self.assertFalse(self.controller)
        self.assertEqual(self.bootstraps, [self.original_plist])
        self.assertFalse(self.notices)

    def test_partial_stop_fault_restores_original_service(self):
        self.native_fault = 'colima'
        result = self.run_fake_task()
        self.assertEqual(result['outcome'], 'fail')
        self.assertTrue(result['original_engine_unchanged']);self.assertTrue(self.loaded)
        self.assertEqual(self.bootstraps, [self.original_plist]);self.assertFalse(self.controller)

    def test_preflight_failure_still_removes_controller(self):
        pathlib.Path(self.record['config_files'][0]['path']).write_bytes(b'drift')
        result = self.run_fake_task()
        self.assertEqual(result['outcome'], 'fail');self.assertFalse(self.controller)
        self.assertFalse(any('bootout' in call for call in self.calls))

    def test_surviving_child_blocks_restore_and_notifies_operator(self):
        result = self.run_fake_task(survive=True)
        self.assertEqual(result['outcome'], 'fail');self.assertFalse(self.loaded)
        self.assertTrue(result['operator_notified']);self.assertTrue(self.notices[0].startswith('sdk-rust:'))
        self.assertFalse(self.bootstraps);self.assertFalse(self.controller)

    def test_task_failure_with_proven_absence_restores_without_claiming_pass(self):
        result = self.run_fake_task(fail=True)
        self.assertEqual(result['outcome'], 'fail');self.assertTrue(result['task_absent'])
        self.assertTrue(self.loaded and self.healthy);self.assertFalse(self.notices)

    def test_restore_consumes_held_plist_not_mutable_path(self):
        self.loaded = False
        raw = self.original_plist
        def mutate_during_lint(argv, cutoff, **kwargs):
            if pathlib.Path(argv[0]).name == 'plutil':
                pathlib.Path(self.record['service']['plist']['path']).write_bytes(b'edited then restored')
            return self.call(argv, cutoff, **kwargs)
        self.w.call = mutate_during_lint
        self.w.bootstrap_fd(raw)
        self.assertEqual(self.bootstraps, [raw])

    def test_restoration_inventory_mismatch_is_terminal(self):
        self.ready();self.w.stop_default()
        self.engine['volumes'].append('foreign')
        self.w.restore_cutoff = time.monotonic() + 0.02
        with mock.patch.object(window.time, 'sleep'), self.assertRaises(SystemExit):self.w.restore(self.original_plist)

    def test_uncertain_service_query_cannot_authorize_bootstrap(self):
        self.w.native = lambda *args, **kwargs:(5,b'',b'permission denied')
        with self.assertRaises(SystemExit):self.w.restore(self.original_plist)
        self.assertFalse(self.bootstraps)

    def test_task_image_instance_and_recorded_process_survivors_refuse(self):
        scoped = self.sdk.paths(self.record['sdk_intent']['parent'], TOKEN)
        scoped['parent'].mkdir(mode=0o700)
        for path in (scoped['image'], scoped['instance']):
            path.parent.mkdir(parents=True, exist_ok=True);path.write_bytes(b'survivor')
            self.assertFalse(self.w.task_absent());path.unlink()
        for name,key in [('vm-process.json',None),('host-ready.json','process'),('vm-started.json','host_process')]:
            process = self.epoch(5678)
            path = scoped['parent']/name;path.write_bytes(window.canonical({key:process} if key else process))
            self.assertFalse(self.w.task_absent());path.unlink()
        (scoped['parent']/'host-ready.json').write_bytes(b'not JSON')
        with self.assertRaises(ValueError):self.w.task_absent()

    def test_prelaunch_owner_loss_aborts_owned_child_then_frozen_cleanup(self):
        self.w.child = Child();self.w.task_record = {'frozen':'authority'}
        self.processes.pop(self.owner_pid)
        with mock.patch.object(self.sdk, 'cleanup') as cleanup:
            self.w.monitor_task()
        cleanup.assert_called_once_with(self.w.task_record)
        self.assertIsNotNone(self.w.child.poll());self.assertFalse(self.w.launched)

    def test_postlaunch_owner_loss_preserves_absolute_child_cleanup(self):
        self.w.child = Child();self.w.task_record = {};self.w.launched = True
        self.processes.pop(self.owner_pid)
        def finish(_):self.w.child.alive = False
        with mock.patch.object(self.sdk, 'cleanup') as cleanup, mock.patch.object(window.time,'sleep',side_effect=finish):
            self.w.monitor_task()
        cleanup.assert_not_called();self.assertTrue(self.w.task_absent())

    def test_live_owner_launches_exact_frozen_request_once(self):
        scoped = self.sdk.paths(self.record['sdk_intent']['parent'],TOKEN);scoped['parent'].mkdir()
        self.w.child = Child();self.w.task_record = {};self.w.task_record_sha = 'd'*64
        def finish(_):self.w.child.alive = False
        with mock.patch.object(window.time,'sleep',side_effect=finish):self.w.monitor_task()
        self.assertEqual(json.loads((scoped['parent']/'launch.json').read_bytes()),
            dict(record_sha256='d'*64,manifest_sha256=window.SDK_M,action='create-and-start'))

    def test_foreign_child_readiness_refuses_before_authority(self):
        scoped = self.sdk.paths(self.record['sdk_intent']['parent'],TOKEN);scoped['parent'].mkdir()
        self.w.child = Child()
        (scoped['parent']/'host-ready.json').write_bytes(window.canonical(dict(process=self.epoch(9999),
            source_sha256=window.SDK_CODE_SHA, record_sha256='d'*64)))
        with mock.patch.object(self.sdk,'authority') as authority, self.assertRaises(SystemExit):self.w.capture_ready()
        authority.assert_not_called()

    def test_result_overwrite_refuses_and_still_removes_controller(self):
        self.put('window-result.json', {'forged':'pass'})
        with self.assertRaises(FileExistsError):self.run_fake_task()
        self.assertEqual(json.loads((self.root/'window-result.json').read_bytes()),{'forged':'pass'})
        self.assertFalse(self.controller)

    def completed(self):
        self.run_fake_task();self.processes.pop(os.getpid())

    def test_independent_final_audit_passes_and_is_exclusive(self):
        self.completed();window.audit(self.w,SOURCE)
        self.assertEqual(json.loads((self.root/'window-audit.json').read_bytes())['outcome'],'pass')
        with self.assertRaises(FileExistsError):window.audit(self.w,SOURCE)

    def test_final_audit_refuses_controller_job_task_and_engine_survivors(self):
        self.completed()
        for fault in ('process','job','task','engine','marker','digest'):
            with self.subTest(fault=fault):
                if fault=='process':self.processes[os.getpid()] = self.epoch(os.getpid())
                elif fault=='job':self.controller = True
                elif fault=='task':self.w.child.alive = True
                elif fault=='engine':self.engine['images'].append('drift')
                elif fault=='marker':self.put('controller-cleanup-failure.json',{'outcome':'fail'})
                else:
                    saved = (self.root/'window-result.json').read_bytes()
                    value=json.loads(saved);value['intent_sha256']='foreign';self.put('window-result.json',value)
                with self.assertRaises(SystemExit):window.audit(self.w,SOURCE)
                self.assertFalse((self.root/'window-audit.json').exists())
                self.processes.pop(os.getpid(),None);self.controller=False;self.w.child.alive=False
                self.engine['images'][:]=['sha256:public']
                (self.root/'controller-cleanup-failure.json').unlink(missing_ok=True)
                if fault=='digest':(self.root/'window-result.json').write_bytes(saved)

    def test_controller_remove_failure_records_fault_and_prevents_acceptance(self):
        call=self.call
        def broken(argv, cutoff, **kwargs):
            if 'remove' in argv:raise SystemExit('self removal failed')
            return call(argv,cutoff,**kwargs)
        self.w.call=broken
        with self.assertRaises(SystemExit):self.run_fake_task()
        self.assertTrue((self.root/'controller-cleanup-failure.json').exists())
        self.assertTrue(self.notices)
        with self.assertRaises(SystemExit):window.audit(self.w,SOURCE)

    def test_owner_binds_one_shot_launchd_job_and_releases_only_exact_ready(self):
        proposal=copy.deepcopy(self.record);proposal['owner_process']=None
        proposal['tools']['python']['path']=str(pathlib.Path(sys.executable).resolve())
        (self.root/'go.json').unlink()
        captured=[]
        def bootstrap(raw):
            job=plistlib.loads(raw);captured.append(job)
            argv=job['ProgramArguments']
            record=json.loads(argv[argv.index('--intent-json')+1])
            digest=argv[argv.index('--record-sha256')+1]
            self.put('window-ready.json',dict(intent_sha256=digest,source_sha256=SOURCE,process=self.epoch(4321)))
            self.assertEqual(record['owner_process'],self.epoch(os.getpid()))
            self.assertFalse((self.root/'go.json').exists())
        self.processes[4321]=self.epoch(4321)
        self.w.bootstrap_fd=bootstrap
        self.w.job=mock.Mock(return_value={'pid':4321})
        args=type('Args',(),{'sdk_code':CODE,'profile':str(self.root/'profile')})()
        profile=(HERE.parent/'condition-history/guest-profile.json').read_bytes()
        code=(HERE/'window.py').read_text()
        with mock.patch.object(window.os,'isatty',return_value=True), mock.patch.object(window.sys,'platform','darwin'), \
                mock.patch.object(window,'validate'), mock.patch.object(window,'Window',return_value=self.w), \
                mock.patch.object(window.selectors,'DefaultSelector',side_effect=SystemExit('test after go')), \
                mock.patch('builtins.print'), self.assertRaisesRegex(SystemExit,'test after go'):
            window.owner(self.sdk,proposal,'a'*64,args,code,profile)
        job=captured[0]
        self.assertFalse(job['KeepAlive']);self.assertTrue(job['RunAtLoad'] and job['LaunchOnlyOnce'])
        self.assertFalse(job['AbandonProcessGroup'])
        self.assertEqual(job['StandardOutPath'],'/dev/null');self.assertEqual(job['StandardErrorPath'],'/dev/null')
        self.assertEqual(job['ProgramArguments'][1:4],['-I','-S','-c'])
        self.assertIn('--intent-json',job['ProgramArguments']);self.assertIn('--profile-json',job['ProgramArguments'])
        self.assertTrue((self.root/'go.json').exists())
        self.assertEqual(json.loads((self.root/'docker-config/config.json').read_bytes()),{'auths':{}})

    def test_late_audit_does_not_extend_absolute_cutoffs(self):
        later = self.record['sdk_intent']['cleanup_unix'] + 500
        with mock.patch.object(window.time, 'time', return_value=later):
            subject = window.Window(self.sdk, self.record, self.intent_sha, CODE)
        self.assertLessEqual(subject.cleanup_cutoff, time.monotonic())
        self.assertLessEqual(subject.restore_cutoff, time.monotonic())

    def test_watch_validation_failure_removes_its_own_registered_job(self):
        args = ['watch', '--record', str(self.root/'unused'), '--record-sha256', self.intent_sha,
            '--sdk-code', CODE, '--profile', str(self.root/'unused-profile'),
            '--intent-json', window.canonical(self.record).decode(),
            '--profile-json', (HERE.parent/'condition-history/guest-profile.json').read_text()]
        self.w.job = mock.Mock(return_value={'pid':os.getpid()})
        with mock.patch.object(window.sys, 'orig_argv', ['python','-I','-S','-c',(HERE/'window.py').read_text()]), \
                mock.patch.object(window.sys, 'platform', 'darwin'), mock.patch.object(window.os, 'getppid', return_value=1), \
                mock.patch.object(window, 'load_sdk', return_value=self.sdk), \
                mock.patch.object(window, 'Window', return_value=self.w), \
                mock.patch.object(window, 'validate', side_effect=SystemExit('original drift')), self.assertRaises(SystemExit):
            window.main(args)
        self.assertFalse(self.controller)
        self.assertFalse(any('bootout' in call for call in self.calls))

    def test_watch_refuses_agent_parent_before_native_service_actions(self):
        args = ['watch', '--record', str(self.root/'unused'), '--record-sha256', self.intent_sha,
            '--sdk-code', CODE, '--profile', str(self.root/'unused-profile'),
            '--intent-json', window.canonical(self.record).decode(),
            '--profile-json', (HERE.parent/'condition-history/guest-profile.json').read_text()]
        with mock.patch.object(window.sys, 'orig_argv', ['python','-I','-S','-c',(HERE/'window.py').read_text()]), \
                mock.patch.object(window.sys, 'platform', 'darwin'), mock.patch.object(window.os,'getppid',return_value=9999), \
                mock.patch.object(window, 'load_sdk',return_value=self.sdk), \
                mock.patch.object(window, 'Window',return_value=self.w), self.assertRaises(SystemExit):window.main(args)
        self.assertFalse(self.calls)

    def test_captured_watch_does_not_reopen_mutable_record_or_profile(self):
        profile = (HERE.parent/'condition-history/guest-profile.json').read_text()
        args = ['watch', '--record', str(self.root/'missing'), '--record-sha256', self.intent_sha,
            '--sdk-code', CODE, '--profile', str(self.root/'missing-profile'),
            '--intent-json', window.canonical(self.record).decode(), '--profile-json', profile]
        self.w.job = mock.Mock(return_value={'pid':os.getpid()})
        self.record['tools']['python']['path'] = str(pathlib.Path(sys.executable).resolve())
        captured = window.canonical(self.record).decode()
        args[args.index('--intent-json')+1] = captured
        args[args.index('--record-sha256')+1] = window.sha(captured.encode())
        with mock.patch.object(window.sys,'orig_argv',['python','-I','-S','-c',(HERE/'window.py').read_text()]), \
                mock.patch.object(window.sys,'platform','darwin'), mock.patch.object(window.os,'getppid',return_value=1), \
                mock.patch.object(window,'load_sdk',return_value=self.sdk), mock.patch.object(window,'Window',return_value=self.w), \
                mock.patch.object(window,'validate') as validate, mock.patch.object(self.w,'run') as run:
            window.main(args)
        validate.assert_called_once();run.assert_called_once_with(SOURCE,json.loads(profile))

    def test_late_task_start_refuses_before_popen(self):
        self.record['sdk_intent']['started_unix'] = int(time.time()) - 10
        with mock.patch.object(window.subprocess,'Popen') as popen, self.assertRaises(SystemExit):self.w.start_task({})
        popen.assert_not_called()

    def test_restoration_fault_notifies_without_retry_or_pass(self):
        call=self.call
        attempts=[]
        def broken(argv,cutoff,**kwargs):
            if 'bootstrap' in argv:
                attempts.append(argv)
                raise SystemExit('restoration bootstrap failed')
            return call(argv,cutoff,**kwargs)
        self.w.call=broken
        result=self.run_fake_task()
        self.assertEqual(result['outcome'],'fail');self.assertTrue(result['operator_notified'])
        self.assertFalse(self.loaded);self.assertFalse(self.controller)
        self.assertEqual(len([x for x in self.calls if 'bootout' in x]),1)
        self.assertEqual(len(attempts),1)
        self.assertFalse(self.bootstraps)

    def test_original_file_symlink_hardlink_and_writable_input_refuse(self):
        path=self.temp/'input';path.write_bytes(b'original')
        binding=self.bind(path)
        link=self.temp/'hardlink';os.link(path,link)
        with self.assertRaises(SystemExit):window.verify_file(self.sdk,binding)
        link.unlink();path.chmod(0o666)
        with self.assertRaises(SystemExit):window.verify_file(self.sdk,binding)
        path.chmod(0o600);link.symlink_to(path)
        with self.assertRaises(SystemExit):window.verify_file(self.sdk,dict(binding,path=str(link)))

    def test_minimal_environment_excludes_forwarded_credentials_and_default_lima_home(self):
        with mock.patch.dict(os.environ,{'SSH_AUTH_SOCK':'secret','DOCKER_AUTH_CONFIG':'secret','LIMA_HOME':'foreign'}):
            env=self.w.env();stop_env=self.w.env(stop=True)
        self.assertEqual(set(env),{'PATH','HOME','TMPDIR','DOCKER_CONFIG'})
        self.assertEqual(stop_env['COLIMA_HOME'],self.record['profile_home'])
        self.assertEqual(stop_env['PATH'],'/original/bin:/usr/bin:/bin')
        self.assertNotIn('LIMA_HOME',stop_env)


if __name__=='__main__':unittest.main()
