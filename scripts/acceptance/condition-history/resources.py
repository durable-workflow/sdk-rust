"""Literal Docker role profiles and effective readbacks. Tests supply a fake CLI."""
import hashlib
import json
import os
import pathlib
import re
import subprocess
import time

RUN = 'dev.durable-workflow.acceptance.run'
OWNER = 'dev.durable-workflow.acceptance.owner'
SUBJECT = 'dev.durable-workflow.acceptance.subject'
ROLE = 'dev.durable-workflow.acceptance.role'
PREFIX = 'sdk-rust-condition'
CELL = 'condition-history'
PROFILES = {
    'fetch': (2_000_000_000, 3 * 1024**3, 1024, True, ()),
    'build': (2_000_000_000, 3 * 1024**3, 1024, True, ()),
    'mysql': (1_000_000_000, 1280 * 1024**2, 512, False, ('CHOWN', 'DAC_OVERRIDE', 'SETGID', 'SETUID')),
    'redis': (250_000_000, 128 * 1024**2, 256, False, ('CHOWN', 'SETGID', 'SETUID')),
    'bootstrap': (750_000_000, 768 * 1024**2, 256, False, ('DAC_OVERRIDE',)),
    'runtime': (750_000_000, 768 * 1024**2, 256, False, ('CHOWN', 'DAC_OVERRIDE', 'KILL', 'SETGID', 'SETUID')),
    'queue': (500_000_000, 512 * 1024**2, 256, False, ()),
    'consumer': (500_000_000, 768 * 1024**2, 256, True, ()),
}


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def names(token):
    base = PREFIX + '-' + token
    task = base + '-' + CELL
    return task, {r: base + '-' + r if r in ('fetch', 'build') else task + '-' + r for r in PROFILES}


def environment(args):
    root = pathlib.Path(args.work_dir)
    return {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(root / 'public-home'),
            'DOCKER_HOST': args.docker_endpoint, 'DOCKER_CONFIG': str(root / 'public-docker')}


def call(args, line, timeout=120, check=True, **kwargs):
    remaining = int(args.active_unix - time.time())
    require(remaining > 0, 'active deadline exhausted')
    result = subprocess.run(line, capture_output=True, timeout=min(timeout, remaining),
                            env=environment(args), **dict({'text': True}, **kwargs))
    require(not check or result.returncode == 0, 'bounded command failed')
    return result


def preflight_collisions(args):
    """Every query is read-only and precedes work/snapshot/registry creation."""
    task, roles = names(args.run_token)
    filters = ['--filter', 'label=' + RUN + '=' + args.run_token, '--filter', 'label=' + OWNER + '=' + PREFIX]
    for command in (['docker', 'ps', '-aq'], ['docker', 'network', 'ls', '-q'], ['docker', 'volume', 'ls', '-q']):
        require(not call(args, command + filters).stdout.split(), 'preexisting owner/run object')
    for kind, candidates in [('container', roles.values()), ('network', [task])]:
        for name in candidates:
            result = call(args, ['docker', kind, 'inspect', '--format', '{{.Id}}', name], check=False)
            if result.returncode == 0:
                raise SystemExit('preexisting exact object name')
            require(any(s in result.stderr.lower() for s in ('no such object', 'no such container', 'no such network')),
                    'collision query is uncertain')


def caps(values):
    return sorted(v.removeprefix('CAP_').upper() for v in values or [])


def env_map(values):
    pairs = [v.split('=', 1) for v in values or []]
    require(all(len(p) == 2 for p in pairs), 'malformed environment readback')
    return dict(pairs)


class Docker:
    def __init__(self, args, subject, record):
        self.args, self.subject, self.record = args, subject, record
        self.task, self.names = names(args.run_token)
        self.images, self.receipts, self.created = {}, [], []

    def call(self, line, **kwargs):
        self.subject.verify()
        return call(self.args, line, **kwargs)

    def labels(self, role):
        return {RUN: self.args.run_token, OWNER: PREFIX, SUBJECT: self.subject.sha256, ROLE: role}

    def image(self, reference):
        if reference not in self.images:
            data = json.loads(self.call(['docker', 'image', 'inspect', reference]).stdout)[0]
            require(data['Os'] == 'linux' and data['Architecture'] == 'amd64', 'wrong image platform')
            content = json.dumps({'layers': data['RootFS']['Layers'], 'config': data['Config']}, sort_keys=True).encode()
            self.images[reference] = {'raw': data, 'sha256': hashlib.sha256(content).hexdigest()}
        return self.images[reference]

    def profile(self, role, image, command, user, credentials, fixture, state=None, env=None, alias=None, deadline=None):
        require(role in PROFILES and credentials in ('none', 'auth', 'server', 'both'), 'unknown role/credential profile')
        require(deadline is not None and deadline + 10 <= self.args.active_unix, 'role exceeds active deadline')
        cpus, memory, pids, readonly, added = PROFILES[role]
        metadata = self.image(image)['raw']['Config']
        child = (metadata.get('Entrypoint') or []) + (command or metadata.get('Cmd') or [])
        require(child and all(isinstance(v, str) for v in child), 'invalid image entrypoint')
        uid, gid = user.split(':')
        require(uid.isdecimal() and gid.isdecimal() and (role not in ('fetch', 'build', 'consumer') or int(uid) != 0), 'unsafe role user')
        tmpfs = {'/credentials': 'mode=0700,uid=%s,gid=%s,size=64k,noexec,nosuid,nodev' % (uid, gid),
                 '/tmp': 'mode=1777,size=64m,nosuid,nodev'}
        binds = []
        if role in ('fetch', 'build'):
            binds.append((str(fixture), '/work', role == 'fetch'))
            if role == 'build':
                binds += [(str(fixture / '.cargo'), '/work/.cargo', True),
                          (str(fixture / 'target'), '/work/target', True)]
        elif role == 'consumer':
            binds += [(str(fixture), '/work', False), (str(fixture / 'target/debug'), '/fixture', False),
                      (str(state), '/state', True)]
        elif role == 'mysql':
            tmpfs['/var/lib/mysql'] = 'mode=0750,size=512m,nosuid,nodev,noexec'
        elif role == 'redis':
            tmpfs['/data'] = 'mode=0750,size=64m,nosuid,nodev,noexec'
        if role in ('bootstrap', 'runtime', 'queue'):
            tmpfs['/app/bootstrap/cache'] = 'mode=0777,size=8m,noexec,nosuid,nodev'
        declared = set((metadata.get('Volumes') or {}).keys())
        require(declared <= set(tmpfs) | {b[1] for b in binds}, 'uncovered image volume')
        network = 'bridge' if role == 'fetch' else 'none' if role == 'build' else self.task
        program = self.subject.read('scripts/acceptance/condition-history/role-supervisor.sh').decode()
        argv = ['-c', program, '--', program, str(deadline), '10', credentials] + child
        expected_env = env_map(metadata.get('Env'))
        expected_env.update(env or {})
        return {'role': role, 'name': self.names[role], 'image': image, 'user': user, 'credentials': credentials,
                'cpus': cpus, 'memory': memory, 'pids': pids, 'readonly': readonly, 'cap_add': list(added),
                'tmpfs': tmpfs, 'binds': binds, 'network': network, 'alias': alias, 'deadline': deadline,
                'argv': argv, 'env': expected_env, 'explicit_env': env or {}}

    def create_arguments(self, spec):
        line = ['docker', 'create', '--init', '--name', spec['name'], '--platform', 'linux/amd64',
                '--user', spec['user'], '--cpus', str(spec['cpus'] / 1_000_000_000),
                '--memory', str(spec['memory']), '--memory-swap', str(spec['memory']), '--pids-limit', str(spec['pids']),
                '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--network', spec['network'],
                '--entrypoint', '/bin/sh']
        if spec['readonly']:
            line += ['--read-only']
        for cap in spec['cap_add']:
            line += ['--cap-add', cap]
        for key, value in self.labels(spec['role']).items():
            line += ['--label', key + '=' + value]
        for destination, options in spec['tmpfs'].items():
            line += ['--tmpfs', destination + ':' + options]
        for source, destination, writable in spec['binds']:
            line += ['--mount', 'type=bind,src=%s,dst=%s%s' % (source, destination, '' if writable else ',readonly')]
        if spec['alias']:
            line += ['--network-alias', spec['alias']]
        if spec['role'] in ('fetch', 'build'):
            line += ['-w', '/work']
        for key, value in spec['explicit_env'].items():
            line += ['-e', key + '=' + value]
        return line + [spec['image']] + spec['argv']

    def validate(self, spec, data):
        host, config = data['HostConfig'], data['Config']
        require(data['Name'] == '/' + spec['name'] and re.fullmatch('[0-9a-f]{64}', data['Id']), 'wrong effective name/ID')
        require(config['Image'] == spec['image'] and self.image(data['Image'])['sha256'] == self.image(spec['image'])['sha256'], 'wrong effective image')
        require(data['State']['Running'] is True and type(data['State']['Pid']) is int and data['State']['Pid'] > 0, 'role not held for readback')
        require(config.get('WorkingDir', '') == ('/work' if spec['role'] in ('fetch', 'build') else self.image(spec['image'])['raw']['Config'].get('WorkingDir', '')), 'wrong effective working directory')
        require(config['User'] == spec['user'] and config['Entrypoint'] == ['/bin/sh']
                and config['Cmd'] == spec['argv'], 'wrong user or deadline supervisor')
        require(host['NanoCpus'] == spec['cpus'] and host['Memory'] == spec['memory']
                and host['MemorySwap'] == spec['memory'] and host['PidsLimit'] == spec['pids'], 'wrong effective resource cap')
        require(host['ReadonlyRootfs'] is spec['readonly'] and host['Init'] is True
                and host['Privileged'] is False, 'wrong root/init/privilege mode')
        require(caps(host['CapDrop']) == ['ALL'] and caps(host['CapAdd']) == caps(spec['cap_add']), 'wrong capability set')
        require(sorted(v.removesuffix(':true') for v in host['SecurityOpt']) == ['no-new-privileges'], 'wrong security options')
        require(host['NetworkMode'] == spec['network'] and not host.get('PortBindings') and not host.get('PublishAllPorts')
                and not host.get('Devices') and not host.get('DeviceRequests') and not host.get('DeviceCgroupRules')
                and not host.get('VolumesFrom') and host.get('PidMode', '') == ''
                and host.get('IpcMode', 'private') == 'private' and host.get('UTSMode', '') == '',
                'wrong effective network/device sharing')
        if spec['network'] == self.task:
            network = json.loads(self.call(['docker', 'network', 'inspect', self.task]).stdout)[0]
            require(network['Internal'] is True and all(network['Labels'].get(k) == v for k, v in self.labels('network').items()), 'wrong internal network authority')
            aliases = set(data['NetworkSettings']['Networks'].get(self.task, {}).get('Aliases') or [])
            require(not spec['alias'] or spec['alias'] in aliases, 'missing effective network alias')
        require(set(data['NetworkSettings']['Networks']) == {spec['network']}, 'unexpected attached network')
        require(all(config['Labels'].get(k) == v for k, v in self.labels(spec['role']).items()), 'wrong effective ownership')
        require(env_map(config['Env']) == spec['env'], 'wrong effective environment')
        require(host.get('Tmpfs', {}) == spec['tmpfs'], 'wrong tmpfs destinations/options')
        actual = [(m['Source'], m['Destination'], m['RW']) for m in data['Mounts'] if m['Type'] == 'bind']
        require(sorted(actual) == sorted(spec['binds']) and all(m['Type'] in ('bind', 'tmpfs') for m in data['Mounts']), 'wrong bind/volume mounts')
        require({m['Destination'] for m in data['Mounts'] if m['Type'] == 'tmpfs'} == set(spec['tmpfs'])
                and len(data['Mounts']) == len(spec['tmpfs']) + len(spec['binds']), 'missing, duplicate or unexpected mount')
        return {'role': spec['role'], 'container_id': data['Id'], 'image': spec['image'],
                'image_content_sha256': self.image(spec['image'])['sha256'], 'user': spec['user'],
                'memory': spec['memory'], 'memory_swap': spec['memory'], 'nano_cpus': spec['cpus'],
                'pids': spec['pids'], 'readonly': spec['readonly'], 'cap_drop': ['ALL'], 'cap_add': spec['cap_add'],
                'security_opt': ['no-new-privileges'], 'network': spec['network'], 'deadline_unix': spec['deadline'],
                'supervisor_argv_sha256': hashlib.sha256(json.dumps(spec['argv']).encode()).hexdigest(),
                'mount_destinations': sorted(set(spec['tmpfs']) | {m[1] for m in spec['binds']}),
                'labels': self.labels(spec['role'])}

    def start(self, spec, token='', key=''):
        self.subject.verify()
        self.created.append(spec['name'])  # before a possibly partial daemon create
        self.call(self.create_arguments(spec))
        self.call(['docker', 'start', spec['name']])
        data = json.loads(self.call(['docker', 'inspect', spec['name']]).stdout)[0]
        receipt = self.validate(spec, data)
        self.receipts.append(receipt)
        # No image command starts before the effective independent readback.
        self.call(['docker', 'exec', '-i', spec['name'], '/bin/sh', '-c',
            'umask 077; IFS= read -r token && IFS= read -r key && '
            '{ [ -z "$token" ] || printf %s "$token" > /credentials/token; } && '
            '{ [ -z "$key" ] || printf %s "$key" > /credentials/server-key; } && '
            ': > /credentials/ready.tmp && mv /credentials/ready.tmp /credentials/ready'],
            input=token + '\n' + key + '\n', timeout=20)
        return spec['name']
