#!/usr/bin/env python3
"""Check which event contexts may restore executable Rust build output."""

import json
from pathlib import Path
import subprocess
import unittest

import yaml


class RustCacheTrustTest(unittest.TestCase):
    def workflow(self):
        return yaml.safe_load(
            (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
        )

    def evaluate_condition(self, condition, contexts):
        condition = condition.strip()
        self.assertTrue(condition.startswith('${{') and condition.endswith('}}'))
        program = '''
          const fs = require('node:fs');
          const vm = require('node:vm');
          const data = JSON.parse(fs.readFileSync(0, 'utf8'));
          process.stdout.write(JSON.stringify(data.contexts.map(context =>
            vm.runInNewContext(data.expression, {
              github: context.github,
              always: () => true,
              success: () => !context.failed,
              failure: () => Boolean(context.failed),
              cancelled: () => Boolean(context.cancelled),
            }, {timeout: 1000}))));
        '''
        result = subprocess.run(
            ['node', '-e', program],
            input=json.dumps({'expression': condition[3:-2].strip(), 'contexts': contexts}),
            text=True, capture_output=True, check=True,
        )
        outcomes = json.loads(result.stdout)
        self.assertEqual(len(outcomes), len(contexts))
        return outcomes

    def test_restore_event_boundary(self):
        workflow = self.workflow()
        restore = next(
            step for step in workflow['jobs']['test']['steps']
            if step.get('id') == 'rust-cache'
        )
        condition = restore.get('if', '${{ true }}')
        repository = 'durable-workflow/sdk-rust'
        def context(event_name, ref, head_repository=None):
            return {
                'repository': repository, 'event_name': event_name, 'ref': ref,
                'event': {'pull_request': {'head': {'repo': {'full_name': head_repository}}}},
            }
        cases = [
            ('fork PR', context('pull_request', 'refs/pull/99/merge', 'external/sdk-rust'), False),
            ('fork with main ref', context('pull_request', 'refs/heads/main', 'external/sdk-rust'), False),
            ('same repository PR', context('pull_request', 'refs/pull/99/merge', repository), True),
            ('protected main push', context('push', 'refs/heads/main'), True),
            ('other branch push', context('push', 'refs/heads/topic'), False),
            ('manual dispatch', context('workflow_dispatch', 'refs/heads/main'), False),
            ('pull request target', context('pull_request_target', 'refs/heads/main', 'external/sdk-rust'), False),
        ]
        outcomes = self.evaluate_condition(
            condition, [{'github': case[1]} for case in cases],
        )
        for (name, _, expected), actual in zip(cases, outcomes):
            with self.subTest(event=name):
                self.assertIs(actual, expected)

    def test_required_qualification_is_not_skipped_after_failure_or_cancellation(self):
        gate = self.workflow()['jobs']['target-branch-qualification']
        cases = [
            ('passed dependencies', {'failed': False, 'cancelled': False}, True),
            ('failed dependencies', {'failed': True, 'cancelled': False}, True),
            ('cancelled after success', {'failed': False, 'cancelled': True}, True),
            ('cancelled after failure', {'failed': True, 'cancelled': True}, True),
        ]
        outcomes = self.evaluate_condition(gate['if'], [case[1] for case in cases])
        for (name, _, expected), actual in zip(cases, outcomes):
            with self.subTest(status=name):
                self.assertIs(actual, expected)


if __name__ == '__main__':
    unittest.main()
