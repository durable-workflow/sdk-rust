import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';

export function logicalPreview(value) {
  if (value?.$type === 'map') {
    return Object.fromEntries(value.entries.map(row => [row.key, logicalPreview(row.value)]));
  }
  if (Array.isArray(value)) return value.map(logicalPreview);
  return value;
}

export function validateOutput(value, row) {
  const output = logicalPreview(value);
  assert.deepEqual(output.number, {$type: 'long', value: '9223372036854775807'});
  assert.equal(output.nothing, null);
  assert.equal(output.text, '雪 😀');
  assert.equal(Buffer.byteLength(output.data), row.data_bytes);
  assert.equal(createHash('sha256').update(output.data).digest('hex'), row.data_sha256);
}

export function validateDetail(detail, row, scope) {
  for (const [field, value] of Object.entries(row.catalog.original)) {
    assert.deepEqual(detail[field], value, `original ${field}`);
  }
  const namespace = scope === 'owner' ? row.target_namespace : row.namespace;
  assert.equal(detail.operator_scope.namespace, namespace);
  assert.equal(detail.backend.read_only, true);
  assert.equal(detail.backend.capabilities.services, true);
  assert.equal(detail.payloads_in_observer_namespace, scope === 'owner');
  assert.equal(detail.caller_link.workflow_run_id, row.caller_run);
  if (row.scenario === 'missing') {
    assert.equal(detail.accepted_at, null);
    assert.equal(detail.call_deadline_at, null);
    assert.equal(detail.linked_run_ref, null);
    assert.equal(detail.failure_type, null);
    assert.equal(detail.outcome, 'rejected_not_found');
    assert.equal(detail.metadata.failure_reason, 'resolution_failure');
    assert.equal(detail.metadata.resolution_failed_at, 'operation');
    assert.ok(typeof detail.failure_message === 'string' && detail.failure_message.trim());
  } else {
    assert.equal(detail.deadline_policy.timeout_seconds, 180);
    assert.equal(detail.idempotency_key, row.case);
    assert.equal(detail.linked_run_ref.workflow_run_id, row.target_run);
    assert.equal(detail.linked_run_ref.in_observer_namespace, scope === 'owner');
  }
  if (scope !== 'owner') {
    assert.notEqual(detail.output_available, true);
    for (const field of ['input_payload_reference', 'output_payload_reference', 'failure_payload_reference']) {
      assert.equal(detail[field], null);
    }
    assert.equal(detail.output, undefined);
    assert.equal(detail.metadata?.nexus_result, undefined);
  } else if (row.status === 'completed') {
    assert.equal(detail.output_available, true);
    validateOutput(detail.output, row);
  } else {
    assert.equal(detail.output_available, false);
    if (row.scenario !== 'missing') assert.ok(detail.failure_type, 'typed failure remains inspectable');
    if (row.scenario === 'failure') assert.match(detail.failure_message, /nexus-target-failure/);
  }
}
