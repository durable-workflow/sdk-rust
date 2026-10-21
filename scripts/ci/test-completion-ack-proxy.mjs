import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer, request as httpRequest } from 'node:http';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseProxyOrigin, proxyTargetUrl } from './proxy-target-url.mjs';

const parsedOrigin = parseProxyOrigin('https://server.internal:8443');
assert.equal(
  proxyTargetUrl('/api/workflows?namespace=default', parsedOrigin).href,
  'https://server.internal:8443/api/workflows?namespace=default',
);
assert.equal(
  proxyTargetUrl('http://169.254.169.254/latest/meta-data?probe=1', parsedOrigin).href,
  'https://server.internal:8443/latest/meta-data?probe=1',
);
assert.equal(
  proxyTargetUrl('//127.0.0.1:9/private?probe=2', parsedOrigin).href,
  'https://server.internal:8443/private?probe=2',
);
for (const invalid of [
  'ftp://server.internal',
  'https://user:secret@server.internal',
  'https://server.internal/prefix',
  'https://server.internal?query=1',
  'https://server.internal#fragment',
]) {
  assert.throws(() => parseProxyOrigin(invalid), /exact HTTP\(S\) origin/);
}

const script = process.env.ACK_PROXY_SCRIPT
  || fileURLToPath(new URL('./local-activity-ack-proxy.mjs', import.meta.url));
const submitted = [];
const upstreamTargets = [];
const upstream = createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : {};
  submitted.push(body);
  upstreamTargets.push(request.url);
  response.writeHead(200, { 'content-type': 'application/json' });
  response.end(JSON.stringify({ recorded: body.unconfirmed !== true, outcome: 'completed' }));
});
upstream.listen(38080, '127.0.0.1');
await once(upstream, 'listening');

let forbiddenRequests = 0;
const forbidden = createServer((_request, response) => {
  forbiddenRequests++;
  response.writeHead(200);
  response.end('wrong origin');
});
forbidden.listen(38083, '127.0.0.1');
await once(forbidden, 'listening');

function sendRawTarget(port, target) {
  return new Promise((resolve, reject) => {
    const request = httpRequest({ host: '127.0.0.1', port, method: 'GET', path: target }, response => {
      response.resume();
      response.once('end', () => resolve(response.statusCode));
      response.once('error', reject);
    });
    request.once('error', reject);
    request.end();
  });
}

async function check(commandType, port) {
  const child = spawn(process.execPath, [script], { env: {
    ...process.env, PORT: String(port),
    DURABLE_WORKFLOW_ACK_UPSTREAM: 'http://127.0.0.1:38080',
    ...(commandType ? { DURABLE_WORKFLOW_ACK_COMMAND: commandType } : {}),
  }, stdio: ['ignore', 'pipe', 'pipe'] });
  const exit = once(child, 'exit');
  let errors = '';
  child.stderr.on('data', value => { errors = (errors + value).slice(-2000); });
  const base = `http://127.0.0.1:${port}`;
  const receipt = async () => (await fetch(`${base}/__qualification/lost-ack`)).json();
  const submit = body => fetch(`${base}/api/worker/workflow-tasks/original-task/complete`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  });
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      try { await receipt(); ready = true; break; } catch { await delay(10); }
    }
    assert.ok(ready, `Proxy did not start: ${errors}`);
    for (const [target, expectedPath] of [
      ['http://127.0.0.1:38083/absolute-target?probe=1', '/absolute-target?probe=1'],
      ['//127.0.0.1:38083/network-path?probe=2', '/network-path?probe=2'],
    ]) {
      const submittedBefore = submitted.length;
      const forbiddenBefore = forbiddenRequests;
      assert.equal(await sendRawTarget(port, target), 200);
      assert.equal(submitted.length, submittedBefore + 1);
      assert.equal(upstreamTargets.at(-1), expectedPath);
      assert.equal(forbiddenRequests, forbiddenBefore, 'Request target must not replace the upstream origin');
    }
    const selected = commandType || 'record_local_activity';
    const other = selected === 'record_local_activity' ? 'start_service_operation' : 'record_local_activity';
    assert.equal((await submit({ commands: [{ type: other }] })).status, 200);
    assert.equal(await receipt(), null, 'Another command must not lose its acknowledgement');
    const command = { type: selected, endpoint_name: 'original-endpoint', service_name: 'service',
      operation_name: 'sync-completed', namespace: 'other-namespace', idempotency_key: 'original-key' };
    if (selected === 'start_service_operation') {
      assert.equal((await submit({ commands: [command], unconfirmed: true })).status, 200);
      assert.equal(await receipt(), null, 'An unrecorded reply is not commit authority');
    }
    const before = submitted.length;
    await assert.rejects(submit({ commands: [command] }), TypeError);
    assert.equal(submitted.length, before + 1, 'The dropped response must follow one real upstream POST');
    const captured = await receipt();
    assert.equal(captured.acknowledgement_dropped, true);
    assert.equal(captured.upstream_status, 200);
    assert.match(captured.command_sha256, /^[a-f0-9]{64}$/);
    if (selected === 'start_service_operation') {
      assert.equal(captured.upstream_recorded, true);
      assert.equal(captured.command_type, selected);
      assert.deepEqual(captured.intent, { endpoint_name: 'original-endpoint', service_name: 'service',
        operation_name: 'sync-completed', namespace: 'other-namespace', idempotency_key: 'original-key' });
    }
    assert.equal((await submit({ commands: [command] })).status, 200);
    assert.deepEqual(await receipt(), captured, 'A later request must preserve the first lost acknowledgement');
  } finally {
    child.kill('SIGTERM');
    await exit;
  }
}

try {
  await check('start_service_operation', 38081);
  await check(null, 38082);
  console.log('Nexus confirmed commit and legacy local-activity acknowledgement controls PASS');
} finally {
  await Promise.all([
    new Promise(resolve => upstream.close(resolve)),
    new Promise(resolve => forbidden.close(resolve)),
  ]);
}
