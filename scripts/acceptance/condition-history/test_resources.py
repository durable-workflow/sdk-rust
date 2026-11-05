"""Adversarial effective-cap/collision controls with a fake Docker CLI only."""
import ast
import copy
import subprocess
import importlib.util
import json
import pathlib
import tempfile
import time
import types
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('resource_controls', HERE / 'resources.py')
resources = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resources)
IMAGE = 'fixture@sha256:' + 'a' * 64

class Frozen:
    sha256 = 'b' * 64
    def __init__(self):self.program = (HERE / 'role-supervisor.sh').read_bytes()
    def verify(self):pass
    def read(self, path):return self.program

class Resources(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.args = types.SimpleNamespace(work_dir=str(self.root / 'unused'), run_token='0123456789abcdef',
            docker_endpoint='unix:///synthetic.sock', active_unix=int(time.time()) + 1740,
            active_monotonic_ns=time.monotonic_ns()+1740*1_000_000_000)
        self.controller = resources.Docker(self.args, Frozen(), {})
        self.image = {'sha256': 'c' * 64, 'raw': {'Id': 'image-id', 'Os': 'linux', 'Architecture': 'amd64',
            'RootFS': {'Layers': []}, 'Config': {'Entrypoint': ['/entry'], 'Cmd': ['default'],
            'Env': ['PATH=/public'], 'WorkingDir': '', 'Volumes': {}}}}
        self.controller.images[IMAGE] = self.image
        self.controller.images['image-id'] = self.image

    def role(self, role='consumer'):
        return self.controller.profile(role, IMAGE, ['explicit'], '65534:65534', 'none', self.root / 'fixture',
            self.root / 'state', env={'CARGO_BUILD_JOBS': '2'} if role in ('fetch', 'build') else {},
            deadline=int(time.time()) + 500)

    def frame(self, role):
        return {'Name': '/' + role['name'], 'Id': 'd' * 64, 'Image': 'image-id', 'State': {'Running': True, 'Pid': 44},
            'Config': {'Image': IMAGE, 'User': role['user'], 'Entrypoint': ['/bin/sh'], 'Cmd': role['argv'],
                       'Env': ['PATH=/public'] + [k + '=' + v for k, v in role['explicit_env'].items()],
                       'Labels': self.controller.labels(role['role']),
                       'WorkingDir': '/work' if role['role'] in ('fetch', 'build') else ''},
            'HostConfig': {'NanoCpus': role['cpus'], 'Memory': role['memory'], 'MemorySwap': role['memory'],
                'PidsLimit': role['pids'], 'ReadonlyRootfs': role['readonly'], 'Init': True, 'Privileged': False,
                'CapDrop': ['ALL'], 'CapAdd': role['cap_add'], 'SecurityOpt': ['no-new-privileges'],
                'NetworkMode': role['network'], 'Tmpfs': role['tmpfs'], 'PortBindings': {},
                'PublishAllPorts': False, 'Devices': [], 'VolumesFrom': []},
            'Mounts': [{'Type': 'bind', 'Source': s, 'Destination': d, 'RW': rw} for s, d, rw in role['binds']] + [{'Type':'tmpfs', 'Destination':d} for d in role['tmpfs']],
            'NetworkSettings': {'Networks': {role['network']: {'Aliases': [role['alias']] if role['alias'] else []}}}}

    def network_frame(self):
        return {'Id': 'e' * 64, 'Name': self.controller.task, 'Driver': 'bridge', 'Scope': 'local',
            'Internal': True, 'Attachable': False, 'Ingress': False, 'EnableIPv4': True, 'EnableIPv6': False,
            'ConfigOnly': False, 'ConfigFrom': {'Network': ''}, 'Labels': self.controller.labels('network'),
            'IPAM': copy.deepcopy(resources.NETWORK_IPAM), 'Options': dict(resources.NETWORK_OPTIONS),
            'Containers': {'d' * 64: {}}}

    def network(self):
        return types.SimpleNamespace(stdout=json.dumps([self.network_frame()]))

    def test_actual_predecessor_wall_clock_rollback_extends_command_budget(self):
        import types
        old_source=subprocess.check_output(['git','show','df617fb:scripts/acceptance/condition-history/resources.py'],text=True)
        old=types.ModuleType('old_wall_budget');exec(compile(old_source,'frozen-df617-resources','exec'),old.__dict__)
        self.args.active_unix=int(time.time())+2
        mono=time.monotonic_ns();self.args.active_monotonic_ns=mono+2*1000000000
        with mock.patch.object(time,'time',return_value=self.args.active_unix-3600), \
                mock.patch.object(time,'monotonic_ns',return_value=mono), \
                mock.patch.object(subprocess,'run',return_value=types.SimpleNamespace(returncode=0)) as command:
            old.call(self.args,['never-executed'])
            self.assertEqual(command.call_args.kwargs['timeout'],120)
            resources.call(self.args,['never-executed'])
            self.assertEqual(command.call_args.kwargs['timeout'],2)

    def test_actual_held_build_has_no_swap_cap(self):
        repo=HERE.parents[2]
        old=subprocess.check_output(['git','--no-replace-objects','-C',str(repo),'show',
            '67a37f7f0e42ba63ee4e68cae3090a9ba73e1cc2:scripts/acceptance/condition-history/run.py'],text=True)
        node=next(n for n in ast.parse(old).body if isinstance(n,ast.FunctionDef) and n.name=='build')
        ledger=[]
        namespace={'remaining':lambda args:100,'os':__import__('os'),'RUST':IMAGE,
            'command':lambda args,line,**kw:ledger.append(line) or types.SimpleNamespace(stdout='',stderr=''),
            'native':types.SimpleNamespace(remove_owned_container=lambda *a:None)}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'held-f155-build','exec'),namespace)
        owner=types.SimpleNamespace(base='old',labels=lambda name:[],best_effort=lambda *a:None)
        namespace['build'](self.args,self.root,owner,'never-executed','build')
        self.assertIn('--memory',ledger[0]);self.assertNotIn('--memory-swap',ledger[0])
        new=self.controller.create_arguments(self.role('build'))
        self.assertEqual(new[new.index('--memory')+1],new[new.index('--memory-swap')+1])

    def test_every_role_has_literal_equal_swap_and_full_caps(self):
        expected = {'fetch': (3 * 1024**3, 1024), 'build': (3 * 1024**3, 1024),
            'mysql': (1280 * 1024**2, 512), 'redis': (128 * 1024**2, 256),
            'bootstrap': (768 * 1024**2, 256), 'runtime': (768 * 1024**2, 256),
            'queue': (512 * 1024**2, 256), 'consumer': (768 * 1024**2, 256)}
        for name, (memory, pids) in expected.items():
            role = self.role(name);argv = self.controller.create_arguments(role)
            self.assertEqual(argv[argv.index('--memory') + 1], str(memory))
            self.assertEqual(argv[argv.index('--memory-swap') + 1], str(memory))
            self.assertEqual(argv[argv.index('--pids-limit') + 1], str(pids))
            self.assertIn('--init', argv);self.assertIn('no-new-privileges', argv)
            with self.subTest(role=name), mock.patch.object(self.controller, 'call', return_value=self.network()):
                self.assertEqual(self.controller.validate(role, self.frame(role))['memory_swap'], memory)

    def test_each_bad_effective_field_refuses_including_offline_build(self):
        for name in ('consumer', 'build'):
            role = self.role(name)
            changes = [('HostConfig', 'MemorySwap', role['memory'] * 2), ('HostConfig', 'Memory', 0),
                ('HostConfig', 'NanoCpus', 0), ('HostConfig', 'PidsLimit', -1),
                ('HostConfig', 'ReadonlyRootfs', not role['readonly']), ('HostConfig', 'Init', False),
                ('HostConfig', 'Privileged', True), ('HostConfig', 'CapAdd', ['SYS_ADMIN']),
                ('HostConfig', 'CapDrop', []), ('HostConfig', 'SecurityOpt', []),
                ('HostConfig', 'NetworkMode', 'host'), ('HostConfig', 'PortBindings', {'80/tcp': [{}]}),
                ('HostConfig', 'PidMode', 'container:foreign'), ('HostConfig', 'IpcMode', 'container:foreign'),
                ('HostConfig', 'UTSMode', 'host'), ('HostConfig', 'DeviceRequests', [{'Count': -1}]),
                ('HostConfig', 'DeviceCgroupRules', ['a *:* rwm']),
                ('HostConfig', 'Tmpfs', {}), ('Config', 'User', '0:0'), ('Config', 'Cmd', ['foreign']),
                ('Config', 'Entrypoint', ['foreign']), ('Config', 'Env', ['CARGO_BUILD_JOBS=99']),
                ('Config', 'Image', 'foreign'), ('Config', 'WorkingDir', '/foreign'), ('Config', 'Labels', {}),
                ('State', 'Running', False), ('State', 'Pid', 0)]
            for section, key, value in changes:
                frame = copy.deepcopy(self.frame(role));frame[section][key] = value
                with self.subTest(role=name, field=key), mock.patch.object(self.controller, 'call', return_value=self.network()), self.assertRaises(SystemExit):
                    self.controller.validate(role, frame)

    def test_internal_network_and_mount_authority_are_effective_checks(self):
        role = self.role()
        frame = self.frame(role)
        network = self.network_frame();network['Internal'] = False
        with mock.patch.object(self.controller, 'call', return_value=types.SimpleNamespace(stdout=json.dumps([network]))), self.assertRaises(SystemExit):
            self.controller.validate(role, frame)
        for mutate in (lambda f: f['Mounts'].append({'Type': 'volume', 'Destination': '/extra'}),
                       lambda f: f['Mounts'].pop(),
                       lambda f: f.update(Name='/foreign'),
                       lambda f: f['Mounts'][0].update(RW=True),
                       lambda f: f['Mounts'][0].update(Source='/foreign'),
                       lambda f: f['NetworkSettings'].update(Networks={'foreign': {}})):
            frame = copy.deepcopy(self.frame(role));mutate(frame)
            with self.subTest(mutate=mutate), mock.patch.object(self.controller, 'call', return_value=self.network()), self.assertRaises(SystemExit):
                self.controller.validate(role, frame)

    def test_no_child_or_credential_gate_opens_before_readback(self):
        role = self.role();frame = self.frame(role);frame['HostConfig']['MemorySwap'] *= 2
        ledger = []
        def cli(line, **kwargs):
            ledger.append(line)
            return types.SimpleNamespace(stdout=json.dumps([frame]) if line[:2] == ['docker', 'inspect'] else '')
        with mock.patch.object(self.controller, 'call', cli), self.assertRaises(SystemExit):
            self.controller.start(role, token='synthetic-only', key='synthetic-only')
        self.assertFalse(any(line[:2] == ['docker', 'exec'] for line in ledger))
        self.assertEqual(self.controller.receipts, [])

    def test_valid_readback_precedes_private_credential_release(self):
        role = self.role();frame = self.frame(role)
        ledger = []
        def cli(line, **kwargs):
            ledger.append((line, kwargs))
            if line[:3] == ['docker', 'network', 'inspect']:
                return self.network()
            if line[:2] == ['docker', 'inspect']:
                return types.SimpleNamespace(stdout=json.dumps([frame]))
            return types.SimpleNamespace(stdout='')
        with mock.patch.object(self.controller, 'call', cli):
            self.assertEqual(self.controller.start(role, token='synthetic-only'), role['name'])
        self.assertEqual(len(self.controller.receipts), 1)
        self.assertEqual(ledger[-1][0][:3], ['docker', 'exec', '-i'])
        self.assertEqual(ledger[-1][1]['input'], 'synthetic-only\n\n')
        self.assertLess(next(i for i, (line, _) in enumerate(ledger) if line[:2] == ['docker', 'inspect']), len(ledger) - 1)

    def test_compile_input_and_replay_input_mounts_are_readonly(self):
        build = self.role('build');consumer = self.role('consumer')
        self.assertIn((str(self.root / 'fixture'), '/work', False), build['binds'])
        self.assertIn((str(self.root / 'fixture'), '/work', False), consumer['binds'])
        self.assertEqual({m[1] for m in build['binds'] if m[2]}, {'/work/.cargo', '/work/target'})
        self.assertEqual(self.role('fetch')['network'], 'bridge')
        self.assertEqual(build['network'], 'none')

    def test_wrong_platform_and_image_content_refuse(self):
        controller = resources.Docker(self.args, Frozen(), {})
        raw = copy.deepcopy(self.image['raw']);raw['Architecture'] = 'arm64'
        with mock.patch.object(controller, 'call', return_value=types.SimpleNamespace(stdout=json.dumps([raw]))), self.assertRaises(SystemExit):
            controller.image(IMAGE)
        role = self.role();self.controller.images['image-id'] = dict(self.image, sha256='foreign')
        frame = self.frame(role)
        with self.assertRaises(SystemExit):self.controller.validate(role, frame)

    def test_any_owner_or_exact_name_collision_precedes_mutation(self):
        for collide in ('container', 'network', 'volume', 'foreign-name', 'uncertain'):
            ledger = []
            def cli(args, line, **kwargs):
                ledger.append(line)
                family = 'container' if line[1] == 'ps' else 'network' if line[1:3] == ['network', 'ls'] else 'volume'
                if 'inspect' not in line:
                    return types.SimpleNamespace(stdout='existing' if family == collide else '', returncode=0, stderr='')
                return types.SimpleNamespace(stdout='existing' if collide == 'foreign-name' else '',
                    returncode=0 if collide == 'foreign-name' else 1,
                    stderr='daemon unavailable' if collide == 'uncertain' else 'No such object')
            with self.subTest(collide=collide), mock.patch.object(resources, 'call', cli), self.assertRaises(SystemExit):
                resources.preflight_collisions(self.args)
            self.assertFalse(pathlib.Path(self.args.work_dir).exists())
            self.assertFalse(any(x in line for line in ledger for x in ('create', 'pull', 'start', 'rm')))

    def test_complete_network_profile_and_ipam_are_read_back(self):
        role = self.role();frame = self.frame(role)
        cases = [('Driver', 'overlay'), ('Scope', 'swarm'), ('Internal', False), ('Attachable', True),
                 ('Ingress', True), ('EnableIPv4', False), ('EnableIPv6', True), ('ConfigOnly', True),
                 ('ConfigFrom', {'Network': 'foreign'}), ('Options', {}), ('Labels', {}),
                 ('IPAM', dict(resources.NETWORK_IPAM, Driver='foreign')),
                 ('IPAM', dict(resources.NETWORK_IPAM, Config=[{'Subnet': '10.0.0.0/8'}])),
                 ('IPAM', dict(resources.NETWORK_IPAM, Options={'foreign': 'true'})),
                 ('Containers', {'d' * 64: {}, 'f' * 64: {}})]
        for key, value in cases:
            network = self.network_frame();network[key] = value
            with self.subTest(key=key, value=value), mock.patch.object(self.controller, 'call',
                    return_value=types.SimpleNamespace(stdout=json.dumps([network]))), self.assertRaises(SystemExit):
                self.controller.validate(role, frame)
        args = self.controller.network_arguments()
        self.assertIn('--ipv4=true', args);self.assertIn('--ipv6=false', args)
        self.assertEqual(args[args.index('--subnet') + 1], '172.30.254.0/28')

    def test_held_network_check_accepts_foreign_driver_scope_and_ipam(self):
        source = subprocess.check_output(['git', '--no-replace-objects', '-C', str(HERE.parents[2]), 'show',
            'e309116cb0dd79e415ae6a25b809515354cea026:scripts/acceptance/condition-history/resources.py'])
        old = types.ModuleType('held_network_resource_controls')
        exec(compile(source, 'held-e309-resources', 'exec'), old.__dict__)
        controller = old.Docker(self.args, Frozen(), {})
        controller.images = dict(self.controller.images)
        role = self.role();frame = self.frame(role)
        network = self.network_frame();network.update(Driver='overlay', Scope='swarm', IPAM={'Driver': 'foreign'})
        with mock.patch.object(controller, 'call', return_value=types.SimpleNamespace(stdout=json.dumps([network]))):
            self.assertEqual(controller.validate(role, frame)['container_id'], frame['Id'])
        with mock.patch.object(self.controller, 'call', return_value=types.SimpleNamespace(stdout=json.dumps([network]))), \
                self.assertRaisesRegex(SystemExit, 'network profile'):
            self.controller.validate(role, frame)


if __name__ == '__main__':unittest.main()
