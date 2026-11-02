#!/usr/bin/env python3
"""Check CI concurrency and the required final gate (#149).

Obsolete pull request revisions are cancelled job by job, so the final gate of
an obsolete run never holds a workflow-wide slot. The gate itself must still
run after cancellation and fail unless every required dependency succeeded.

Groups are evaluated in the scheduler's context. GitHub sets github.job only
inside steps; it is null when jobs.<id>.concurrency is evaluated, so each group
must name its job literally.
"""

import json
from pathlib import Path
import re
import subprocess
import unittest

import yaml

GATE = 'target-branch-qualification'
GATE_NAME = 'Target branch qualification'
OPTIONAL = {'cooperative-server', 'local-server'}
EXPRESSION = re.compile(r'^\$\{\{\s*(needs\.([a-z-]+)\.result|inputs\.([a-z_]+)|github\.server_url == \'https://github\.com\')\s*\}\}$')


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_NAME = 'CI'
# The former key took job identity from github.job, which is null here.
GITHUB_JOB_KEY = "ci-${{ github.event_name }}-${{ github.job }}-${{ matrix.rust || 'single' }}-${{ github.event_name == 'pull_request' && github.ref || github.sha }}"


def workflow(name='ci.yml'):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


def evaluate(expression, contexts):
    """Evaluate a GitHub expression body with Node, as test-rust-ci-cache.py does."""
    expression = expression.strip()
    if expression.startswith('${{'):
        expression = expression[3:-2].strip()
    program = '''
      const fs = require('node:fs');
      const vm = require('node:vm');
      const data = JSON.parse(fs.readFileSync(0, 'utf8'));
      process.stdout.write(JSON.stringify(data.contexts.map(context =>
        vm.runInNewContext(data.expression, context, {timeout: 1000}))));
    '''
    result = subprocess.run(
        ['node', '-e', program],
        input=json.dumps({'expression': expression, 'contexts': contexts}),
        text=True, capture_output=True, check=True,
    )
    return json.loads(result.stdout)


def group_template(group):
    # GitHub's ${{ a }}-${{ b }} interpolation as a JavaScript template literal;
    # GitHub renders null and missing values as empty strings.
    return '`' + re.sub(r'\$\{\{\s*(.*?)\s*\}\}', r"${((\1) ?? '')}", group) + '`'


def context(event, ref, sha, matrix=None, workflow_name=WORKFLOW_NAME):
    """The scheduler's view: github.job is null, as outside any step."""
    return {'github': {'event_name': event, 'ref': ref, 'sha': sha, 'job': None,
                       'workflow': workflow_name},
            'matrix': matrix or {}, 'inputs': {}}


def literal_text(template):
    return re.sub(r'\$\{\{.*?\}\}', '\0', template)


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.workflow = workflow()
        self.jobs = self.workflow['jobs']

    def test_gate_holds_no_concurrency_slot(self):
        self.assertNotIn('concurrency', self.workflow)
        gate = self.jobs[GATE]
        self.assertEqual(GATE_NAME, gate['name'])
        self.assertNotIn('concurrency', gate)
        self.assertEqual('${{ always() }}', gate['if'])

    def resolve(self, job, contexts, group=None):
        self.assertIn('concurrency', self.jobs[job], f'{job} has no concurrency group')
        concurrency = self.jobs[job]['concurrency']
        groups = evaluate(group_template(group or concurrency['group']), contexts)
        cancels = evaluate(concurrency['cancel-in-progress'], contexts)
        return list(zip(groups, cancels))

    def test_every_other_job_has_a_revision_scoped_group(self):
        for job in self.jobs:
            if job == GATE:
                continue
            with self.subTest(job=job):
                matrix = {'rust': '1.86.0'} if job == 'test' else None
                (a, cancel_pr), (b, _), (main_a, cancel_main), (main_b, _), (dispatch, cancel_dispatch) = self.resolve(job, [
                    context('pull_request', 'refs/pull/7/merge', 'a' * 40, matrix),
                    context('pull_request', 'refs/pull/7/merge', 'b' * 40, matrix),
                    context('push', 'refs/heads/main', 'c' * 40, matrix),
                    context('push', 'refs/heads/main', 'd' * 40, matrix),
                    context('workflow_dispatch', 'refs/heads/main', 'e' * 40, matrix),
                ])
                # A new PR revision replaces the same job of the obsolete one.
                self.assertEqual(a, b)
                self.assertIs(True, cancel_pr)
                # Main pushes and dispatches never cancel one another.
                self.assertNotEqual(main_a, main_b)
                self.assertIs(False, cancel_main)
                self.assertIs(False, cancel_dispatch)
                self.assertIn('e' * 40, dispatch)
                # Other pull requests keep their own groups.
                ((other, _),) = self.resolve(job, [
                    context('pull_request', 'refs/pull/8/merge', 'a' * 40, matrix)])
                self.assertNotEqual(a, other)

    def test_groups_name_the_job_literally_and_the_workflow(self):
        for job in self.jobs:
            if job == GATE:
                continue
            with self.subTest(job=job):
                group = self.jobs[job]['concurrency']['group']
                self.assertNotIn('github.job', group)
                self.assertIn(f'-{job}-', literal_text(group))
                self.assertTrue(group.startswith('${{ github.workflow }}-'), group)
                matrix = {'rust': '1.86.0'} if job == 'test' else None
                (ci, _), (renamed, _) = self.resolve(job, [
                    context('pull_request', 'refs/pull/7/merge', 'a' * 40, matrix),
                    context('pull_request', 'refs/pull/7/merge', 'a' * 40, matrix, 'Other'),
                ])
                self.assertTrue(ci.startswith(f'{WORKFLOW_NAME}-{job}-'), ci)
                self.assertNotEqual(ci, renamed)

    def test_scheduler_model_catches_a_github_job_key(self):
        # Counterfactual: with github.job null, the former key gives every
        # non-matrix job the same group, so they would cancel one another.
        pr = context('pull_request', 'refs/pull/7/merge', 'a' * 40)
        groups = {self.resolve(job, [pr], GITHUB_JOB_KEY)[0][0]
                  for job in ('docs', 'package', 'protocol')}
        self.assertEqual(1, len(groups), groups)

    def test_jobs_and_matrix_legs_never_share_a_group(self):
        groups = {}
        for job in self.jobs:
            if job == GATE:
                continue
            legs = self.jobs[job].get('strategy', {}).get('matrix', {}).get('rust', [None])
            for rust in legs:
                matrix = {'rust': rust} if rust else None
                pr = context('pull_request', 'refs/pull/7/merge', 'a' * 40, matrix)
                groups[(job, rust)] = self.resolve(job, [pr])[0][0]
        self.assertEqual(len(groups), len(set(groups.values())), groups)
        self.assertIn(('test', '1.86.0'), groups)
        self.assertIn(('test', 'stable'), groups)

    def test_groups_do_not_collide_with_other_workflows(self):
        ours = set()
        for job in self.jobs:
            if job == GATE:
                continue
            for rust in self.jobs[job].get('strategy', {}).get('matrix', {}).get('rust', [None]):
                matrix = {'rust': rust} if rust else None
                for event, ref in (('pull_request', 'refs/pull/7/merge'), ('push', 'refs/heads/main')):
                    ours.add(self.resolve(job, [context(event, ref, 'a' * 40, matrix)])[0][0])
        for path in sorted((ROOT / '.github/workflows').glob('*.yml')):
            if path.name == 'ci.yml':
                continue
            other = workflow(path.name)
            groups = [other.get('concurrency')] + [
                spec.get('concurrency') for spec in other.get('jobs', {}).values()]
            for spec in filter(None, groups):
                group = spec['group'] if isinstance(spec, dict) else spec
                for event, ref in (('pull_request', 'refs/pull/7/merge'), ('push', 'refs/heads/main')):
                    with self.subTest(workflow=path.name, event=event):
                        (theirs,) = evaluate(group_template(group), [
                            context(event, ref, 'a' * 40, workflow_name=other.get('name', path.name))])
                        self.assertNotIn(theirs, ours)


class GateTest(unittest.TestCase):
    def setUp(self):
        self.jobs = workflow()['jobs']
        self.gate = self.jobs[GATE]

    def test_gate_needs_every_other_job(self):
        self.assertEqual(set(self.jobs) - {GATE}, set(self.gate['needs']))

    def run_gate(self, results, inputs=None, server_url='https://github.com'):
        """Run the gate's steps as GitHub would; return True when all pass."""
        inputs = inputs or {}
        for step in self.gate['steps']:
            if 'if' in step:
                (enabled,) = evaluate(step['if'], [{'github': {'server_url': server_url}}])
                if not enabled:
                    continue
            env = {}
            for name, value in step.get('env', {}).items():
                match = EXPRESSION.match(value)
                self.assertIsNotNone(match, f'unmodelled gate expression {value}')
                if match.group(2):
                    env[name] = results.get(match.group(2), '')
                else:
                    raw = inputs.get(match.group(3), '')
                    env[name] = str(raw).lower() if isinstance(raw, bool) else str(raw)
            process = subprocess.run(
                ['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', step['run']],
                env={'PATH': '/usr/bin:/bin', **env},
            )
            if process.returncode:
                return False
        return True

    def ordinary_results(self):
        return {job: 'success' for job in set(self.jobs) - {GATE} - OPTIONAL} | {
            job: 'skipped' for job in OPTIONAL}

    def test_passes_when_required_dependencies_succeed(self):
        self.assertTrue(self.run_gate(self.ordinary_results()))

    def test_fails_closed_for_every_required_dependency(self):
        for job in sorted(set(self.jobs) - {GATE} - OPTIONAL):
            for result in ('failure', 'cancelled', 'skipped', ''):
                with self.subTest(job=job, result=result or 'missing'):
                    self.assertFalse(self.run_gate(self.ordinary_results() | {job: result}))

    def test_policy_preflight_is_only_waived_off_github_com(self):
        results = self.ordinary_results() | {'action-policy': 'skipped'}
        self.assertTrue(self.run_gate(results, server_url='https://ghes.example'))
        self.assertFalse(self.run_gate(results))

    def test_requested_server_qualification_must_succeed(self):
        cases = [
            ({'cooperative_qualification': True}, 'cooperative-server'),
            ({'local_qualification': True}, 'local-server'),
            ({'sticky_qualification': True}, 'local-server'),
            ({'session_qualification': True, 'server_commit': 'f' * 40}, 'cooperative-server'),
            ({'session_qualification': True, 'server_commit': ''}, 'local-server'),
        ]
        for inputs, job in cases:
            for result in ('failure', 'cancelled', 'skipped', ''):
                with self.subTest(inputs=inputs, result=result or 'missing'):
                    self.assertFalse(self.run_gate(self.ordinary_results() | {job: result}, inputs))
            with self.subTest(inputs=inputs, result='success'):
                self.assertTrue(self.run_gate(self.ordinary_results() | {job: 'success'}, inputs))



class TemporaryLiveFailureControl(unittest.TestCase):
    # TEMPORARY (#149 live experiment): forces Regression corpus to fail so the
    # required gate's refusal can be observed. Reverted by the next commit.
    def test_forced_failure(self):
        self.fail('#149 live control: deliberate Regression corpus failure')


if __name__ == '__main__':
    unittest.main()
