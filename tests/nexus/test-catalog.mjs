import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {test} from 'node:test';
import {originalCases, validateDetail, validateOutput} from './catalog-contract.mjs';

test('process-state sidecars do not become extra originals or hide unfinished cases', () => {
  const completed = {case: 'original-completed', outcome: 'pass'};
  const pending = {case: 'original-pending'};
  assert.deepEqual(originalCases([completed, {ExitCode: 0}, pending, {Running: false}]), [completed, pending]);
});

function fixture() {
  const data = 'x'.repeat(17);
  const row = {case: 'original-caller', namespace: 'caller', target_namespace: 'owner',
    caller_run: 'caller-run', target_run: 'target-run', scenario: 'success', status: 'completed',
    data_bytes: 17, data_sha256: createHash('sha256').update(data).digest('hex'),
    catalog: {original: {id: 'original-call', call_deadline_at: '2026-10-18T20:03:00.123456Z'}}};
  const output = {number: {$type: 'long', value: '9223372036854775807'}, nothing: null, text: '雪 😀', data};
  const detail = {...row.catalog.original, operator_scope: {namespace: 'owner'},
    backend: {read_only: true, capabilities: {services: true}}, payloads_in_observer_namespace: true,
    caller_link: {workflow_run_id: row.caller_run}, linked_run_ref: {workflow_run_id: row.target_run, in_observer_namespace: true},
    deadline_policy: {timeout_seconds: 180}, idempotency_key: row.case, output_available: true, output};
  return {row, detail, output};
}

test('owned typed result retains exact original identity and deadline', () => {
  const {row, detail} = fixture();
  validateDetail(detail, row, 'owner');
});
test('large integer rounding is refused', () => {
  const {row, output} = fixture();
  output.number = 9223372036854775807;
  assert.throws(() => validateOutput(output, row));
});
test('changed original deadline and target run are refused separately', () => {
  for (const mutate of [d => {d.call_deadline_at = '2026-10-18T20:04:00.123456Z';},
    d => {d.linked_run_ref.workflow_run_id = 'another-run';}]) {
    const {row, detail} = fixture(); mutate(detail);
    assert.throws(() => validateDetail(detail, row, 'owner'));
  }
});
test('caller view must redact result payloads and foreign links', () => {
  const {row, detail} = fixture();
  detail.operator_scope.namespace = 'caller';
  detail.payloads_in_observer_namespace = false;
  detail.linked_run_ref.in_observer_namespace = false;
  for (const field of ['input_payload_reference', 'output_payload_reference', 'failure_payload_reference']) detail[field] = null;
  assert.throws(() => validateDetail(detail, row, 'caller'));
  delete detail.output;
  detail.output_available = false;
  validateDetail(detail, row, 'caller');
  detail.linked_run_ref.in_observer_namespace = true;
  assert.throws(() => validateDetail(detail, row, 'caller'));
});
test('missing-operation diagnostic preserves no admission, deadline or target', () => {
  const {row, detail} = fixture();
  row.scenario = 'missing'; row.status = 'failed';
  row.catalog.original.call_deadline_at = null;
  detail.call_deadline_at = null; detail.accepted_at = null; detail.linked_run_ref = null;
  detail.output_available = false; delete detail.output;
  detail.failure_type = null; detail.outcome = 'rejected_not_found';
  detail.failure_message = 'Service contract component [operation] did not resolve.';
  detail.metadata = {failure_reason: 'resolution_failure', resolution_failed_at: 'operation'};
  validateDetail(detail, row, 'owner');
  detail.accepted_at = '2026-10-18T20:00:00.123456Z';
  assert.throws(() => validateDetail(detail, row, 'owner'));
  detail.accepted_at = null;
  detail.failure_type = 'invented-target-exception';
  assert.throws(() => validateDetail(detail, row, 'owner'));
  detail.failure_type = null;
  detail.metadata.resolution_failed_at = 'service';
  assert.throws(() => validateDetail(detail, row, 'owner'));
});
