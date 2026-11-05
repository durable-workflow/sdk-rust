"""One-shot shared-Colima window proposal. No native operation is admitted.

The separately frozen task controller remains df617fb. This outer subject owns
the original service handoff, restoration, and its own launchd registration.
"""
import argparse
import hashlib
import json
import os
import pathlib
import plistlib
import re
import selectors
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
import types

SDK_R = 'df617fb681958801182e67ca63e886041bec6de0'
SDK_M = '9ce029dcfb64220ff233e9f42605400ea4a73aceb598272a4034b3e8a65d6e38'
SDK_CODE_SHA = 'bf9dbd8166c71dd1b87bc5761640ce26d07e624d56b19d26e40e5ff02eeab9a1'
KEYS = {'schema', 'token', 'source_sha256', 'host_boot_id', 'uid', 'scope', 'owner_process',
        'started_unix', 'sdk_intent', 'tools', 'service', 'config_files', 'disks',
        'docker_host', 'engine', 'profile_home'}
SAFE_DIAGNOSTICS = (b'permission denied', b'operation not permitted', b'input/output error',
                    b'could not find service', b'bad request', b'connection refused',
                    b'cannot connect to the docker daemon', b'no such file or directory')


class NativeFailure(SystemExit):
    def __init__(self, reason, status, output=b'', errors=b'', os_errno=None):
        super().__init__(reason)
        self.reason, self.status, self.output, self.errors = reason, status, output, errors
        self.os_errno = os_errno


def diagnostic(data):
    # No arbitrary diagnostic bytes enter the receipt or terminal. Only these
    # fixed phrases are exposed; the complete bounded captured bytes are hashed.
    lower = data.lower()
    return {'bytes': len(data), 'sha256': sha(data),
            'safe_excerpt': [phrase.decode() for phrase in SAFE_DIAGNOSTICS if phrase in lower]}


def require(ok, message):
    if not ok:
        raise SystemExit(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def load_sdk(code):
    require(sha(code.encode()) == SDK_CODE_SHA, 'wrong frozen task controller')
    module = types.ModuleType('frozen_df617fb_guest')
    exec(compile(code, 'git:' + SDK_R + ':guest-host.py', 'exec'), module.__dict__)
    return module


def root_of(record):
    return pathlib.Path(record['scope'])


def job_label(record):
    return 'sdk-rust-colima-window-' + record['token']


def target(record, label=None):
    return 'gui/' + str(record['uid']) + '/' + (label or job_label(record))


def verify_file(sdk, binding):
    require(set(binding) == {'path', 'bytes', 'sha256'}, 'wrong file binding')
    path = pathlib.Path(binding['path'])
    require(path.is_absolute() and path.resolve() == path, 'noncanonical original input')
    require(type(binding['bytes']) is int and 0 < binding['bytes'] <= 64 * 1024**2, 'invalid original input bound')
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_uid in (0, os.getuid())
            and before.st_nlink == 1 and not before.st_mode & 0o022, 'unsafe original input')
    sdk.input_bytes(path, binding['bytes'], binding['sha256'])


def verify_original(sdk, record):
    for binding in [*record['tools'].values(), *record['config_files']]:
        verify_file(sdk, binding)
    for disk in record['disks']:
        require(set(disk) == {'path', 'device', 'inode'}, 'wrong original disk binding')
        path = pathlib.Path(disk['path'])
        info = path.lstat()
        require(path.is_absolute() and path.resolve() == path and stat.S_ISREG(info.st_mode)
                and info.st_nlink == 1 and info.st_uid == record['uid']
                and (info.st_dev, info.st_ino) == (disk['device'], disk['inode']), 'original disk identity changed')
    service = record['service']
    verify_file(sdk, service['plist'])
    raw = sdk.regular(pathlib.Path(service['plist']['path']), 64 * 1024)
    require(sha(raw) == service['plist']['sha256'], 'original LaunchAgent changed')
    value = plistlib.loads(raw)
    require(value['Label'] == service['label'] and value['RunAtLoad'] is True
            and value['KeepAlive'] == service['keep_alive']
            and value['ProgramArguments'] == service['arguments']
            and pathlib.Path(value['ProgramArguments'][0]).resolve() == pathlib.Path(record['tools']['colima']['path']),
            'original LaunchAgent configuration changed')
    actual_home = value.get('EnvironmentVariables', {}).get('COLIMA_HOME', str(pathlib.Path.home() / '.colima'))
    require(actual_home == record['profile_home'], 'original default profile changed')
    arguments = value['ProgramArguments']
    require(all(isinstance(item, str) and not any(ch.isspace() for ch in item)
                for item in value['ProgramArguments']), 'unsupported service argument shape')
    expected = [arguments[0], 'start', '-f', '--cpu', '4', '--memory', '6', '--disk', '60',
                '--vm-type', 'vz', '--mount-type', 'virtiofs']
    require(arguments == expected, 'unsupported original default-service invocation')
    environment = value.get('EnvironmentVariables', {})
    require(set(environment) <= {'PATH', 'HOME', 'COLIMA_HOME'}
            and environment.get('HOME', str(pathlib.Path.home())) == str(pathlib.Path.home()),
            'unexpected original service environment')
    return raw


def validate(sdk, record, source_sha):
    require(set(record) == KEYS and record['schema'] == 'durable-workflow.colima-window/v1', 'wrong window schema')
    require(record['source_sha256'] == source_sha and re.fullmatch('[0-9a-f]{16}', record['token']), 'wrong window source/token')
    require(type(record['uid']) is int and record['uid'] == os.getuid() and record['uid'] > 0
            and record['host_boot_id'] == sdk.boot_identity(), 'wrong window host/UID')
    root = root_of(record)
    info = root.lstat()
    require(root.is_absolute() and root.resolve() == root and root.name == 'sdk-rust-window-' + record['token']
            and stat.S_ISDIR(info.st_mode) and info.st_uid == record['uid']
            and stat.S_IMODE(info.st_mode) == 0o700, 'wrong private window scope')
    require(type(record['started_unix']) is int, 'missing absolute window epoch')
    owner = record['owner_process']
    require(set(owner) == {'pid', 'pgid', 'uid', 'start_seconds', 'start_microseconds', 'boot_id'}
            and all(type(owner[key]) is int for key in ('pid', 'pgid', 'uid', 'start_seconds', 'start_microseconds'))
            and owner['pid'] > 1 and owner['pgid'] > 0 and owner['uid'] == record['uid']
            and owner['start_seconds'] > 0 and 0 <= owner['start_microseconds'] < 1_000_000
            and owner['boot_id'] == record['host_boot_id'], 'wrong native owner epoch')
    task = record['sdk_intent']
    require(set(task) == sdk.INTENT_KEYS and task['runner_commit'] == SDK_R
            and task['manifest_sha256'] == SDK_M and task['guardian_sha256'] == SDK_CODE_SHA
            and task['host_boot_id'] == record['host_boot_id'] and task['token'] == record['token']
            and task['started_unix'] == record['started_unix'] + 120
            and task['active_unix'] == task['started_unix'] + 1740
            and task['cleanup_unix'] == task['started_unix'] + 1800, 'wrong frozen task/window cutoffs')
    sdk.paths(task['parent'], task['token'])
    require(record['docker_host'].startswith('unix:///') and pathlib.Path(record['profile_home']).is_absolute(), 'local original profile required')
    home = pathlib.Path(record['profile_home'])
    require(home.resolve() == home and record['docker_host'] == 'unix://' + str(home / 'default/docker.sock'), 'wrong default engine endpoint')
    require(set(record['tools']) == {'python', 'launchctl', 'plutil', 'colima', 'docker', 'ps', 'dm'}, 'wrong window tool set')
    require(record['tools']['launchctl']['path'] == '/bin/launchctl'
            and record['tools']['plutil']['path'] == '/usr/bin/plutil'
            and record['tools']['ps']['path'] == '/bin/ps', 'native service tools required')
    service = record['service']
    require(set(service) == {'label', 'plist', 'arguments', 'keep_alive', 'properties', 'vm_processes'}
            and service['label'] != job_label(record) and not service['label'].startswith('sdk-rust-')
            and re.fullmatch('[A-Za-z0-9_.-]+', service['label'])
            and service['keep_alive'] == {'SuccessfulExit': False}, 'unsupported original service policy')
    require(service['vm_processes'] and all(process['boot_id'] == record['host_boot_id']
            and process['uid'] == record['uid'] for process in service['vm_processes']), 'original VM process authority required')
    require(record['config_files'] and record['disks'], 'original profile/disk inventory required')
    require(len(record['config_files']) <= 8 and len(record['disks']) <= 8, 'oversize original inventory')
    require({str(home / 'default/colima.yaml'), str(home / '_lima/colima/lima.yaml')}
            <= {binding['path'] for binding in record['config_files']}, 'original profile configurations are missing')
    engine = record['engine']
    require(set(engine) == {'info', 'memory_ceiling_bytes', 'images', 'volumes'}
            and set(engine['info']) == {'ID', 'ServerVersion', 'Architecture', 'OSType', 'NCPU', 'DockerRootDir'}
            and engine['info']['NCPU'] == 4 and engine['info']['OSType'] == 'linux'
            and engine['info']['Architecture'] in ('x86_64', 'amd64')
            and engine['memory_ceiling_bytes'] == 6 * 1024**3
            and all(isinstance(item, str) for key in ('images', 'volumes') for item in engine[key])
            and all(engine[key] == sorted(set(engine[key])) for key in ('images', 'volumes')),
            'wrong original engine fingerprint')
    verify_original(sdk, record)


class Window:
    def __init__(self, sdk, record, intent_sha, sdk_code):
        self.sdk, self.record, self.intent_sha, self.sdk_code = sdk, record, intent_sha, sdk_code
        self.root = root_of(record)
        now, tick = time.time(), time.monotonic()
        self.stop_cutoff = tick + max(0, record['started_unix'] + 120 - now)
        self.task_cutoff = tick + max(0, record['sdk_intent']['cleanup_unix'] - now)
        self.restore_cutoff = tick + max(0, record['sdk_intent']['cleanup_unix'] + 300 - now)
        self.cleanup_cutoff = tick + max(0, record['sdk_intent']['cleanup_unix'] + 360 - now)
        self.child = None
        self.task_record = None
        self.task_record_sha = None
        self.launched = False
        self.stop_attempted = False
        self.original_plist = None
        self.stage = 'created'
        self.native_faults = []
        self.native_fault_count = 0
        self.last_native = None

    def evidence(self, tool, argv, status, output, errors, reason):
        operation = argv[2] if tool == 'docker' and len(argv) > 2 else (argv[0] if argv else '')
        if tool == 'colima':operation = 'stop'
        if tool == 'ps':operation = 'command'
        if tool == 'dm':operation = 'notify'
        if operation not in {'print', 'bootout', 'bootstrap', 'kickstart', 'remove', '-lint',
                             'info', 'ps', 'image', 'volume', 'stop', 'command', 'notify'}:operation = 'other'
        return {'stage': self.stage, 'tool': tool, 'operation': operation, 'exit_status': status,
                'reason': reason, 'stdout': diagnostic(output), 'stderr': diagnostic(errors)}

    def remember_fault(self, evidence):
        self.native_fault_count += 1
        # Keep the first eight failures and latest eight, including late restore
        # or notification faults, within a fixed receipt/storage bound.
        if len(self.native_faults) == 16:del self.native_faults[8]
        self.native_faults.append(evidence)

    def failure_details(self, error):
        return {'failed_stage': self.stage, 'error_type': type(error).__name__,
                'native_fault_count': self.native_fault_count, 'native_faults': list(self.native_faults),
                'last_native_result': self.last_native}

    def env(self, stop=False):
        result = {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'HOME': str(pathlib.Path.home()),
                  'TMPDIR': str(self.root), 'DOCKER_CONFIG': str(self.root / 'docker-config')}
        if stop:
            raw = self.original_plist or verify_original(self.sdk, self.record)
            result['PATH'] = plistlib.loads(raw).get('EnvironmentVariables', {}).get('PATH', result['PATH'])
            result['COLIMA_HOME'] = self.record['profile_home']
        return result

    def call(self, argv, cutoff, *, pass_fds=(), stop=False, check=True):
        remaining = cutoff - time.monotonic()
        if remaining <= 0:raise NativeFailure('deadline', None)
        child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, pass_fds=pass_fds, env=self.env(stop), close_fds=True)
        output, errors = bytearray(), bytearray()
        end = time.monotonic() + min(120 if stop else 20, remaining)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ, output)
                selector.register(child.stderr, selectors.EVENT_READ, errors)
                while selector.get_map():
                    if time.monotonic() >= end:
                        raise NativeFailure('timeout', None, bytes(output), bytes(errors))
                    for key, _ in selector.select(min(0.1, max(0, end - time.monotonic()))):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if not data:selector.unregister(key.fileobj);continue
                        if len(output) + len(errors) + len(data) > 2 * 1024**2:
                            key.data.extend(data[:2 * 1024**2 - len(output) - len(errors)])
                            raise NativeFailure('output-limit', None, bytes(output), bytes(errors))
                        key.data.extend(data)
                child.wait(timeout=max(0.001, end - time.monotonic()))
            if check and child.returncode != 0:
                raise NativeFailure('exit-status', child.returncode, bytes(output), bytes(errors))
            return child.returncode, bytes(output), bytes(errors)
        except (SystemExit, OSError, subprocess.TimeoutExpired) as error:
            if child.poll() is None:child.kill();child.wait(timeout=2)
            if isinstance(error, NativeFailure):
                if error.status is None:error.status = child.returncode
                raise
            raise NativeFailure(type(error).__name__, child.returncode, bytes(output), bytes(errors),
                                getattr(error, 'errno', None)) from None
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=2)
            child.stdout.close();child.stderr.close()

    def native(self, name, *argv, cutoff=None, check=True, **kwargs):
        try:
            verify_file(self.sdk, self.record['tools'][name])
            result = self.call([self.record['tools'][name]['path'], *argv],
                               cutoff or self.restore_cutoff, check=False, **kwargs)
        except NativeFailure as error:
            self.last_native = dict(self.evidence(name, argv, error.status, error.output, error.errors, error.reason),
                                    os_errno=error.os_errno)
            self.remember_fault(self.last_native)
            raise
        except (OSError, SystemExit) as error:
            self.last_native = dict(self.evidence(name, argv, None, b'', b'', type(error).__name__),
                                    os_errno=getattr(error, 'errno', None))
            self.remember_fault(self.last_native)
            raise
        self.last_native = self.evidence(name, argv, *result, 'returned')
        if check and result[0] != 0:
            self.remember_fault(dict(self.last_native, reason='exit-status'))
            raise NativeFailure('exit-status', *result)
        return result

    def job(self, label, cutoff=None):
        code, output, error = self.native('launchctl', 'print', target(self.record, label), cutoff=cutoff, check=False)
        if code:
            expected = ('Bad request.\nCould not find service "' + label
                        + '" in domain for user gui: ' + str(self.record['uid']) + '\n').encode()
            if code == 113 and output == b'' and error == expected:return None
            self.remember_fault(dict(self.last_native, reason='uncertain-absence'))
            raise SystemExit('uncertain launchd query')
        if error:
            self.remember_fault(dict(self.last_native, reason='unexpected-print-diagnostic'))
            raise SystemExit('unexpected launchd diagnostic')
        text = output.decode()
        def one(name, optional=False):
            # Only top-level fields, never a similarly named line inside the
            # controller's multiline -c argument or an environment block.
            matches = re.findall(r'^\t' + name + r' = (.+)$', text, re.MULTILINE)
            if optional and not matches:return None
            require(len(matches) == 1, 'unexpected launchd effective schema')
            return matches[0].strip()
        pid = one('pid', optional=True)
        return {'pid': int(pid) if pid else None, 'program': one('program'), 'properties': one('properties')}

    def engine(self):
        def docker(*args):return self.native('docker', '--host', self.record['docker_host'], *args)[1].decode().strip()
        value = json.loads(docker('info', '--format', '{{json .}}'))
        require(not docker('ps', '-aq'), 'original engine is no longer idle')
        # Usable Linux MemTotal can vary slightly across a boot; the captured
        # original service/config fixes its 6-GiB allocation, not that page count.
        require(5 * 1024**3 <= value['MemTotal'] <= 6 * 1024**3, 'original engine memory is outside its profile')
        return {'info': {key: value[key] for key in ('ID', 'ServerVersion', 'Architecture', 'OSType', 'NCPU', 'DockerRootDir')},
                'memory_ceiling_bytes': 6 * 1024**3,
                'images': sorted(set(docker('image', 'ls', '--no-trunc', '--format', '{{.ID}}').splitlines())),
                'volumes': sorted(docker('volume', 'ls', '--format', '{{.Name}}').splitlines())}

    def service_matches(self):
        job = self.job(self.record['service']['label'])
        require(job and job['pid'] and pathlib.Path(job['program']).resolve() == pathlib.Path(self.record['tools']['colima']['path'])
                and job['properties'] == self.record['service']['properties'], 'effective original service changed')
        command = self.native('ps', '-ww', '-p', str(job['pid']), '-o', 'command=')[1]
        require(shlex.split(command.decode()) == self.record['service']['arguments'], 'effective original service arguments changed')
        return job

    def bootstrap_fd(self, raw):
        # An unlinked descriptor holds the captured plist; no edited pathname
        # can replace the LaunchAgent configuration consumed by launchctl.
        with tempfile.TemporaryFile(dir=self.root) as stream:
            stream.write(raw);stream.flush();stream.seek(0)
            descriptor = '/dev/fd/' + str(stream.fileno())
            self.native('plutil', '-lint', descriptor, pass_fds=(stream.fileno(),))
            self.native('launchctl', 'bootstrap', 'gui/' + str(self.record['uid']), descriptor, pass_fds=(stream.fileno(),))

    def owner_alive(self):
        expected = self.record['owner_process']
        return self.sdk.process_identity(expected['pid']) == expected

    def stop_default(self):
        require(self.owner_alive(), 'owner disappeared before stop')
        ready = json.loads(self.sdk.regular(self.root / 'window-ready.json'))
        require(ready['intent_sha256'] == self.intent_sha
                and ready['source_sha256'] == self.record['source_sha256']
                and ready['process'] == self.sdk.process_identity(os.getpid()), 'outside-agent readiness changed')
        verify_original(self.sdk, self.record)
        self.service_matches()
        require(self.engine() == self.record['engine'], 'original engine inventory changed')
        require(all(self.sdk.process_identity(process['pid']) == process
                    for process in self.record['service']['vm_processes']), 'original VM process changed')
        require(time.monotonic() < self.stop_cutoff, 'stop window expired before mutation')
        self.stop_attempted = True
        self.native('launchctl', 'bootout', target(self.record, self.record['service']['label']), cutoff=self.stop_cutoff)
        self.native('colima', '--profile', 'default', 'stop', cutoff=self.stop_cutoff, stop=True)
        require(self.job(self.record['service']['label'], self.stop_cutoff) is None, 'original service survived stop')
        require(all(self.sdk.process_identity(process['pid']) is None
                    for process in self.record['service']['vm_processes']), 'original VM survived stop')
        self.sdk.exclusive(self.root / 'default-stopped.json', {'intent_sha256': self.intent_sha})

    def start_task(self, profile):
        while time.time() < self.record['sdk_intent']['started_unix']:
            require(self.owner_alive() and time.monotonic() < self.stop_cutoff, 'owner/start window lost')
            time.sleep(0.1)
        require(self.owner_alive() and time.time() < self.record['sdk_intent']['started_unix'] + 5,
                'owner/start window lost before task')
        sdk_intent = self.record['sdk_intent']
        intent_sha = self.sdk.exclusive(self.root / 'sdk-intent.json', sdk_intent)
        self.sdk.exclusive(self.root / 'guest-profile.json', profile)
        argv = [self.record['tools']['python']['path'], '-I', '-S', '-c', self.sdk_code, 'prepare',
                '--record', str(self.root / 'sdk-intent.json'), '--record-sha256', intent_sha,
                '--profile', str(self.root / 'guest-profile.json')]
        self.child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True, env=self.env())

    def capture_ready(self):
        scoped = self.sdk.paths(self.record['sdk_intent']['parent'], self.record['token'])
        ready_path = scoped['parent'] / 'host-ready.json'
        if not ready_path.exists():return False
        ready = json.loads(self.sdk.regular(ready_path))
        require(self.child and ready['process']['pid'] == self.child.pid
                and self.sdk.process_identity(self.child.pid) == ready['process']
                and ready['source_sha256'] == SDK_CODE_SHA, 'task readiness is not the owned child')
        self.task_record_sha = ready['record_sha256']
        self.task_record = self.sdk.authority(scoped['parent'] / 'host-record.json', self.task_record_sha, SDK_CODE_SHA)
        require(self.task_record['runner_commit'] == SDK_R and self.task_record['manifest_sha256'] == SDK_M,
                'task readiness tuple changed')
        require(self.task_record['parent'] == self.record['sdk_intent']['parent']
                and self.task_record['token'] == self.record['token'], 'task readiness scope changed')
        return True

    def task_absent(self):
        scoped = self.sdk.paths(self.record['sdk_intent']['parent'], self.record['token'])
        if self.child and self.child.poll() is None:return False
        if os.path.lexists(scoped['image']) or os.path.ismount(scoped['mount']) or os.path.lexists(scoped['instance']):return False
        for filename, key in (('vm-process.json', None), ('vm-started.json', 'host_process'), ('host-ready.json', 'process')):
            path = scoped['parent'] / filename
            if path.exists():
                process = json.loads(self.sdk.regular(path))
                if key:process = process[key]
                if self.sdk.process_identity(process['pid']) is not None:return False
        return True

    def monitor_task(self):
        scoped = self.sdk.paths(self.record['sdk_intent']['parent'], self.record['token'])
        while self.child.poll() is None and time.monotonic() < self.task_cutoff:
            ready = self.task_record is not None or self.capture_ready()
            if ready and not self.launched and self.owner_alive():
                self.sdk.exclusive(scoped['parent'] / 'launch.json', {'record_sha256': self.task_record_sha,
                    'manifest_sha256': SDK_M, 'action': 'create-and-start'})
                self.launched = True
            # No owner is needed for the frozen child's absolute-cutoff cleanup.
            # Before VM launch, a complete ready record also permits early abort.
            if ready and not self.launched and not self.owner_alive() and time.monotonic() + 30 < self.task_cutoff:
                self.child.terminate();self.child.wait(timeout=5)
                self.sdk.cleanup(self.task_record)
                break
            time.sleep(0.25)
        require(self.task_absent(), 'task cleanup is uncertain; original service must remain stopped')

    def restore(self, original_plist):
        require(self.task_absent(), 'task survivor blocks restoration')
        verify_original(self.sdk, self.record)
        if self.job(self.record['service']['label']) is None:self.bootstrap_fd(original_plist)
        self.native('launchctl', 'kickstart', target(self.record, self.record['service']['label']))
        while time.monotonic() < self.restore_cutoff:
            try:
                self.service_matches()
                require(self.engine() == self.record['engine'], 'restored engine differs from original')
                verify_original(self.sdk, self.record)
                return {'original_engine_unchanged': True, 'task_absent': True}
            except (SystemExit, OSError, ValueError, KeyError):
                time.sleep(0.5)
        raise SystemExit('original service restoration is uncertain')

    def notify_failure(self):
        # The host's existing local dm cache is used only for a real fault.
        # No credential or vault value is copied into the task guest or a log.
        previous, self.stage = self.stage, 'fault-notify'
        try:
            self.native('dm', 'sdk-rust: default Colima window has unresolved task/restoration custody; inspect the private window receipt.', cutoff=self.cleanup_cutoff)
            return True
        except BaseException:return False
        finally:self.stage = previous

    def run(self, source_sha, profile):
        original = None
        restoration_attempted = False
        result = {'outcome': 'fail', 'intent_sha256': self.intent_sha, 'source_sha256': source_sha}
        try:
            self.stage = 'preflight'
            original = verify_original(self.sdk, self.record)
            self.original_plist = original
            self.sdk.exclusive(self.root / 'window-ready.json', {'intent_sha256': self.intent_sha,
                'source_sha256': source_sha, 'process': self.sdk.process_identity(os.getpid())})
            self.stage = 'wait-go'
            while not (self.root / 'go.json').exists():
                require(self.owner_alive() and time.monotonic() < self.stop_cutoff, 'owner/go window lost')
                time.sleep(0.1)
            require(json.loads(self.sdk.regular(self.root / 'go.json')) == {'intent_sha256': self.intent_sha, 'action': 'one-window'}, 'wrong window request')
            self.stage = 'stop-default'
            self.stop_default()
            self.stage = 'start-task'
            self.start_task(profile)
            self.stage = 'monitor-task'
            self.monitor_task()
            restoration_attempted = True
            self.stage = 'restore-original'
            result.update(self.restore(original), outcome='pending_independent_audit')
        except BaseException as error:
            result.update(self.failure_details(error))
            if self.stop_attempted:
                if restoration_attempted:
                    result['operator_notified'] = self.notify_failure()
                else:
                    try:
                        self.stage = 'restore-after-fault'
                        result.update(self.restore(original))
                    except BaseException as restoration_error:
                        result['restoration_failure'] = self.failure_details(restoration_error)
                        result['operator_notified'] = self.notify_failure()
        finally:
            try:
                result.update(native_fault_count=self.native_fault_count, native_faults=list(self.native_faults),
                              last_native_result=self.last_native)
                result['controller_process'] = self.sdk.process_identity(os.getpid())
                self.sdk.exclusive(self.root / 'window-result.json', result)
            finally:
                self.remove_controller()

    def remove_controller(self):
        # Receipt precedes self-removal. A separate invocation must prove that
        # this job/process is gone; this process cannot certify its own absence.
        self.stage = 'controller-remove'
        try:
            self.native('launchctl', 'remove', job_label(self.record), cutoff=self.cleanup_cutoff)
        except BaseException as error:
            detail = self.failure_details(error)
            notified = self.notify_failure()
            detail.update(native_fault_count=self.native_fault_count, native_faults=list(self.native_faults),
                          last_native_result=self.last_native)
            self.sdk.exclusive(self.root / 'controller-cleanup-failure.json', {
                **detail, 'outcome': 'fail', 'intent_sha256': self.intent_sha, 'operator_notified': notified})
            raise


def audit(window, source_sha):
    sdk, record = window.sdk, window.record
    result = json.loads(sdk.regular(root_of(record) / 'window-result.json'))
    require(result['intent_sha256'] == window.intent_sha and result['source_sha256'] == source_sha
            and result['outcome'] == 'pending_independent_audit' and result['task_absent'] is True
            and result['original_engine_unchanged'] is True, 'window did not complete cleanly')
    require(not (root_of(record) / 'controller-cleanup-failure.json').exists(), 'controller cleanup failed')
    require(sdk.process_identity(result['controller_process']['pid']) is None
            and window.job(job_label(record)) is None and window.task_absent(), 'window/controller survivor')
    window.service_matches()
    require(window.engine() == record['engine'], 'final original engine drift')
    verify_original(sdk, record)
    sdk.exclusive(root_of(record) / 'window-audit.json', {'outcome': 'pass', 'intent_sha256': window.intent_sha,
        'source_sha256': source_sha, 'controller_absent': True, 'task_absent': True, 'original_engine_unchanged': True})


def owner(sdk, proposal, proposal_sha, args, code, profile_raw):
    require(os.isatty(0), 'foreground owner TTY required')
    require(sys.platform == 'darwin', 'native macOS owner required')
    require(proposal['owner_process'] is None, 'owner epoch must be filled by the native bootstrap')
    record = dict(proposal, owner_process=sdk.process_identity(os.getpid()))
    source_sha = sha(code.encode())
    validate(sdk, record, source_sha)
    require(pathlib.Path(sys.executable).resolve() == pathlib.Path(record['tools']['python']['path']),
            'owner Python differs from the captured launcher')
    require(record['started_unix'] <= time.time() < record['started_unix'] + 30, 'stale owner launch')
    root = root_of(record)
    intent_sha = sdk.exclusive(root / 'window-intent.json', record)
    (root / 'docker-config').mkdir(mode=0o700)
    sdk.exclusive(root / 'docker-config/config.json', {'auths': {}})
    window = Window(sdk, record, intent_sha, args.sdk_code)
    job = {'Label': job_label(record), 'ProgramArguments': [record['tools']['python']['path'], '-I', '-S', '-c', code,
        'watch', '--record', str(root / 'window-intent.json'), '--record-sha256', intent_sha,
        '--intent-json', canonical(record).decode(), '--profile-json', profile_raw.decode(),
        '--sdk-code', args.sdk_code, '--profile', args.profile], 'RunAtLoad': True, 'KeepAlive': False,
        'LaunchOnlyOnce': True, 'AbandonProcessGroup': False, 'WorkingDirectory': '/',
        'EnvironmentVariables': window.env(), 'StandardOutPath': '/dev/null', 'StandardErrorPath': '/dev/null'}
    try:
        window.stage = 'owner-bootstrap'
        window.bootstrap_fd(plistlib.dumps(job))
        window.stage = 'owner-readiness'
        while time.monotonic() < window.stop_cutoff:
            ready_path = root / 'window-ready.json'
            if ready_path.exists():
                ready = json.loads(sdk.regular(ready_path))
                effective = window.job(job_label(record))
                require(ready['intent_sha256'] == intent_sha and ready['source_sha256'] == source_sha
                        and effective and ready['process']['pid'] == effective['pid']
                        and sdk.process_identity(effective['pid']) == ready['process'], 'independent controller readiness mismatch')
                break
            time.sleep(0.1)
        else:raise SystemExit('outside-agent controller did not become ready')
    except BaseException as error:
        sdk.exclusive(root / 'owner-failure.json', {'intent_sha256': intent_sha,
            'source_sha256': source_sha, **window.failure_details(error)})
        raise
    sdk.exclusive(root / 'go.json', {'intent_sha256': intent_sha, 'action': 'one-window'})
    print(json.dumps({'window_intent_sha256': intent_sha, 'source_sha256': source_sha,
                      'proposal_sha256': proposal_sha, 'owner_process': record['owner_process']}), flush=True)
    with selectors.DefaultSelector() as selector:
        selector.register(sys.stdin, selectors.EVENT_READ)
        while time.monotonic() < window.cleanup_cutoff:
            result_path = root / 'window-result.json'
            if result_path.exists() and window.job(job_label(record)) is None and \
                    sdk.process_identity(json.loads(sdk.regular(result_path))['controller_process']['pid']) is None:
                audit(window, source_sha)
                print(json.dumps({'operational_window': 'pass', 'sdk_acceptance': 'separate'}), flush=True)
                return
            for key, _ in selector.select(0.25):
                if not os.read(key.fileobj.fileno(), 1024):
                    raise SystemExit('owner input closed; independent controller retains cleanup custody')
    raise SystemExit('owner final-audit cutoff')


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('owner', 'watch', 'audit'))
    parser.add_argument('--record', required=True)
    parser.add_argument('--record-sha256', required=True)
    parser.add_argument('--sdk-code', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--intent-json')
    parser.add_argument('--profile-json')
    args = parser.parse_args(argv)
    require('-I' in sys.orig_argv and '-S' in sys.orig_argv and '-c' in sys.orig_argv, 'captured outer source required')
    source_sha = sha(sys.orig_argv[sys.orig_argv.index('-c') + 1].encode())
    sdk = load_sdk(args.sdk_code)
    if args.mode == 'watch':
        require(args.intent_json is not None and args.profile_json is not None, 'captured watch inputs required')
        raw = args.intent_json.encode()
        profile_raw = args.profile_json.encode()
    else:
        require(args.intent_json is None and args.profile_json is None, 'unexpected watch inputs')
        raw = sdk.regular(pathlib.Path(args.record), 512 * 1024)
        profile_raw = sdk.regular(pathlib.Path(args.profile), 64 * 1024)
    require(sha(raw) == args.record_sha256, 'window intent digest mismatch')
    record = json.loads(raw)
    require(sha(profile_raw) == sdk.PROFILE_SHA, 'wrong frozen SDK profile')
    if args.mode == 'owner':
        owner(sdk, record, args.record_sha256, args, sys.orig_argv[sys.orig_argv.index('-c') + 1], profile_raw)
        return
    window = Window(sdk, record, args.record_sha256, args.sdk_code)
    if args.mode == 'watch':
        require(sys.platform == 'darwin' and os.getppid() == 1, 'outside-agent launchd parent required')
        require(record['uid'] == os.getuid() and re.fullmatch('[0-9a-f]{16}', record['token']), 'wrong controller identity')
        job = window.job(job_label(record))
        require(job and job['pid'] == os.getpid(), 'controller does not own its launchd registration')
        try:
            validate(sdk, record, source_sha)
            require(pathlib.Path(sys.executable).resolve() == pathlib.Path(record['tools']['python']['path']),
                    'controller Python differs from captured launcher')
            require(record['started_unix'] <= time.time() < record['started_unix'] + 30, 'stale outer launch')
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        except BaseException:
            window.remove_controller()
            raise
        window.run(source_sha, json.loads(profile_raw))
    else:
        validate(sdk, record, source_sha)
        audit(window, source_sha)


if __name__ == '__main__':
    main()
