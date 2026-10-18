import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {validateDetail, validateOutput} from './catalog-contract.mjs';

const root = '/state';
const records = fs.readdirSync(`${root}/outcomes`).filter(name => name.endsWith('.json'))
  .map(name => JSON.parse(fs.readFileSync(`${root}/outcomes/${name}`, 'utf8')));
assert.equal(records.length, 40);
assert.equal(new Set(records.map(row => row.service_call_id)).size, 40);
const directions = ['rust:php', 'php:rust', 'rust:rust', 'rust:python', 'python:rust'];
for (const namespace of ['default', 'nexus-source']) {
  for (const direction of directions) {
    for (const scenario of ['success', 'failure', 'external', 'missing']) {
      const selected = records.filter(row => row.namespace === namespace
        && `${row.caller}:${row.target}` === direction && row.scenario === scenario);
      assert.equal(selected.length, 1, `one original ${namespace}/${direction}/${scenario}`);
    }
  }
}
for (const row of records) {
  assert.equal(row.outcome, 'pass');
  assert.equal(row.rust_source, process.env.NEXUS_RUST_SOURCE);
  assert.equal(row.rust_binary_sha256, process.env.NEXUS_RUST_BINARY_SHA256);
  assert.equal(row.worker_exits.length, 2);
  assert.ok(row.worker_exits.every(worker => worker.exit_code === 0 && worker.oom_killed === false));
}
const origins = {default: 'http://catalog-default:8080', 'nexus-source': 'http://catalog-named:8080'};
const output = `${root}/catalog`;
fs.mkdirSync(output, {recursive: true});
const reports = [], errors = [], mutations = [];
const {chromium} = createRequire(`${root}/browser/package.json`)('playwright');
const browser = await chromium.launch({args: ['--no-sandbox']});
async function api(origin, suffix) {
  const response = await fetch(`${origin}/waterline/api/v2/services/${suffix}`, {
    redirect: 'error', signal: AbortSignal.timeout(15000),
  });
  return {status: response.status, body: await response.json()};
}
async function unchanged(row) {
  for (const [namespace, workflow, run, original] of [
    [row.namespace, row.case, row.caller_run, row.catalog.caller_history],
    [row.target_namespace, row.target_id, row.target_run, row.catalog.target_history],
  ]) {
    if (!original) continue;
    const response = await fetch(`http://server:8080/api/workflows/${workflow}/runs/${run}/history`, {
      headers: {Authorization: 'Bearer test-token', 'X-Namespace': namespace,
        'X-Durable-Workflow-Control-Plane-Version': '2'}, signal: AbortSignal.timeout(10000),
    });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), original, 'inspection must preserve complete original history');
  }
}
try {
  for (const row of records) {
    await unchanged(row);
    const unrelated = await api('http://catalog-unrelated:8080', `calls/${row.service_call_id}`);
    assert.equal(unrelated.status, 404);
    for (const [scope, namespace] of [['owner', row.target_namespace], ['caller', row.namespace]]) {
      const origin = origins[namespace];
      const response = await api(origin, `calls/${row.service_call_id}`);
      assert.equal(response.status, 200);
      validateDetail(response.body, row, scope);
      assert.equal((await api(origin, `calls/${row.service_call_id}-wrong`)).status, 404);
      for (const [width, theme] of [[390, 'light'], [1440, 'dark']]) {
        const context = await browser.newContext({viewport: {width, height: 900}});
        try {
          await context.addInitScript(value => localStorage.setItem('waterline-theme', value), theme);
          const page = await context.newPage();
          page.setDefaultTimeout(15000);
          page.on('pageerror', error => errors.push(error.message));
          page.on('request', request => {
            if (request.url().includes('/waterline/api/') && !['GET', 'HEAD'].includes(request.method())) {
              mutations.push(`${request.method()} ${request.url()}`);
            }
          });
          const query = new URLSearchParams({tab: 'calls', status: row.status,
            operation_id: response.body.workflow_service_operation_id || ''});
          if (!response.body.workflow_service_operation_id) query.delete('operation_id');
          await page.goto(`${origin}/waterline/services?${query}`, {waitUntil: 'networkidle'});
          const table = page.locator('.service-catalog-view tbody tr').filter({hasText: row.service_call_id});
          await table.getByRole('button', {name: 'View', exact: true}).click();
          const panel = page.locator('.service-catalog-view__detail');
          await panel.getByRole('heading', {name: 'Terminal result', exact: true}).waitFor();
          const text = await panel.innerText();
          for (const value of [row.case, 'cleanup', row.scenario]) assert.ok(text.includes(value));
          if (response.body.call_deadline_at) {
            await panel.getByText(response.body.call_deadline_at.replace('T', ' ').replace(/Z$/, ''), {exact: true}).waitFor();
          }
          const result = panel.locator('section').filter({has: page.getByRole('heading', {name: 'Terminal result', exact: true})});
          if (scope === 'caller') {
            assert.ok(text.includes('The result belongs to another namespace.'));
            const reference = panel.locator('.service-catalog-view__reference').filter({hasText: 'Target Run'});
            assert.equal(await reference.locator('a').count(), 0);
          } else if (row.status === 'completed') {
            validateOutput(JSON.parse(await result.locator('pre').innerText()), row);
          } else {
            if (row.scenario === 'missing') {
              assert.ok(text.includes(response.body.failure_message));
              assert.ok(text.toLowerCase().includes('rejected not found'));
            } else {
              assert.ok(text.includes(response.body.failure_type));
            }
            assert.equal(await result.locator('pre').count(), 0);
          }
          assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'no page overflow');
          let screenshot;
          if (row.caller === 'rust' && row.target === 'php' && row.namespace === 'default') {
            screenshot = `${row.scenario}-${scope}-${width}-${theme}.png`;
            await page.screenshot({path: path.join(output, screenshot)});
          }
          reports.push({case: row.case, call_id: row.service_call_id, scope, namespace,
            scenario: row.scenario, width, theme, screenshot, outcome: 'pass'});
        } finally {
          await context.close();
        }
      }
    }
    await unchanged(row);
  }
  assert.deepEqual(errors, []);
  assert.deepEqual(mutations, []);
  assert.equal(reports.length, 160);
  fs.writeFileSync(`${output}/report.json`, `${JSON.stringify({classification: 'actual_api_browser',
    outcome: 'pass', original_calls: 40, unrelated_refusals: 40, wrong_call_refusals: 80,
    mutations, errors, cases: reports}, null, 2)}\n`);
  console.log('40 original calls / 160 real-browser inspections PASS; no mutations or page errors');
} finally {
  await browser.close();
}
