#!/usr/bin/env python3
"""Check CI concurrency and the required final gate (#149).

Obsolete pull request revisions are cancelled job by job, so the final gate of
an obsolete run never holds a workflow-wide slot. The gate itself must still
run after cancellation and fail unless every required dependency succeeded.
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


def workflow():
    return yaml.safe_load(
        (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
    )


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
    # GitHub's ${{ a }}-${{ b }} interpolation as a JavaScript template literal.
    return '`' + re.sub(r'\$\{\{\s*(.*?)\s*\}\}', r'${\1}', group) + '`'


def context(event, ref, sha, job, matrix=None):
    return {'github': {'event_name': event, 'ref': ref, 'sha': sha, 'job': job},
            'matrix': matrix or {}}


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

    def resolve(self, job, contexts):
        self.assertIn('concurrency', self.jobs[job], f'{job} has no concurrency group')
        concurrency = self.jobs[job]['concurrency']
        groups = evaluate(group_template(concurrency['group']), contexts)
        cancels = evaluate(concurrency['cancel-in-progress'], contexts)
        return list(zip(groups, cancels))

    def test_every_other_job_has_a_revision_scoped_group(self):
        for job in self.jobs:
            if job == GATE:
                continue
            with self.subTest(job=job):
                matrix = {'rust': '1.86.0'} if job == 'test' else None
                (a, cancel_pr), (b, _), (main_a, cancel_main), (main_b, _), (dispatch, cancel_dispatch) = self.resolve(job, [
                    context('pull_request', 'refs/pull/7/merge', 'a' * 40, job, matrix),
                    context('pull_request', 'refs/pull/7/merge', 'b' * 40, job, matrix),
                    context('push', 'refs/heads/main', 'c' * 40, job, matrix),
                    context('push', 'refs/heads/main', 'd' * 40, job, matrix),
                    context('workflow_dispatch', 'refs/heads/main', 'e' * 40, job, matrix),
                ])
                # A new PR revision replaces the same job of the obsolete one.
                self.assertEqual(a, b)
                self.assertIs(True, cancel_pr)
                # Main pushes and dispatches never cancel one another.
                self.assertNotEqual(main_a, main_b)
                self.assertIs(False, cancel_main)
                self.assertIs(False, cancel_dispatch)
                self.assertIn(job, a)
                self.assertIn('e' * 40, dispatch)

    def test_jobs_and_matrix_legs_never_share_a_group(self):
        groups = {}
        for job in self.jobs:
            if job == GATE:
                continue
            legs = self.jobs[job].get('strategy', {}).get('matrix', {}).get('rust', [None])
            for rust in legs:
                matrix = {'rust': rust} if rust else None
                pr = context('pull_request', 'refs/pull/7/merge', 'a' * 40, job, matrix)
                groups[(job, rust)] = self.resolve(job, [pr])[0][0]
        self.assertEqual(len(groups), len(set(groups.values())), groups)


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


if __name__ == '__main__':
    unittest.main()
