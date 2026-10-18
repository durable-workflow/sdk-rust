<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\ServiceOperationFailed;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

require getenv('NEXUS_PHP_AUTOLOAD') ?: __DIR__.'/vendor/autoload.php';

$role = getenv('NEXUS_ROLE') ?: 'caller';
if (!in_array($role, ['caller', 'target'], true)) {
    throw new InvalidArgumentException('NEXUS_ROLE must be caller or target.');
}
$endpoint = getenv('NEXUS_ENDPOINT') ?: 'cooperative';
$service = getenv('NEXUS_SERVICE') ?: 'cleanup';
$client = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL') ?: 'http://server:8080',
    token: getenv('DURABLE_WORKFLOW_WORKER_TOKEN') ?: 'test-token',
    namespace: getenv('DURABLE_WORKFLOW_RUNTIME_NAMESPACE') ?: 'default',
);
$worker = new Worker(
    $client,
    getenv('TASK_QUEUE') ?: 'nexus-workers',
    workerId: getenv('WORKER_ID') ?: 'nexus-'.$role,
    enableCooperativeCancellation: true,
);
$worker->registerWorkflow('nexus.qualification.neighbor', static fn (WorkflowContext $ctx): array => ['alive' => true, 'runtime' => 'php']);
$worker->registerWorkflow(
    getenv('WORKFLOW_TYPE') ?: 'nexus.'.$role,
    static function (WorkflowContext $ctx, string $operation, mixed $value = null, ?string $key = null, ?string $targetNamespace = null) use ($role, $endpoint, $service): mixed {
        try {
            if ($role === 'caller') {
                if (getenv('NEXUS_QUALIFY_ADMISSION') === '1') {
                    if (!in_array($operation, ['sync-completed', 'sync-accepted', 'async-completed', 'async-accepted'], true) || $key === null || $key === '') {
                        throw new InvalidArgumentException('Expected an admission profile and original idempotency key.');
                    }
                    $result = $ctx->serviceOperation($endpoint, $service, $operation, [$operation, $value, $key, $targetNamespace], [
                        'mode_override' => str_starts_with($operation, 'async-') ? 'async' : 'sync',
                        'wait_for' => str_ends_with($operation, '-accepted') ? 'accepted' : 'completed',
                        'idempotency_key' => $key,
                        'namespace' => $targetNamespace,
                    ]);
                    $snapshot = ['call_id' => $result->serviceCallId, 'status' => $result->status,
                        'response' => $result->responsePayload, 'details' => $result->serviceCall];
                    $event = $operation === 'sync-completed' ? 'caller.result' : 'caller.admission';
                    echo json_encode(['event' => $event, 'snapshot' => $snapshot], JSON_THROW_ON_ERROR).PHP_EOL;
                    fflush(STDOUT);
                    $ctx->sleep(3);
                    return $snapshot;
                }
                $arguments = getenv('NEXUS_QUALIFY_OUTCOMES') === '1' ? [$operation, $value] : [$operation];
                return $ctx->serviceOperation($endpoint, $service, $operation, $arguments)->responsePayload;
            }
            if (getenv('NEXUS_QUALIFY_ADMISSION') === '1') {
                $ctx->sleep(8);
                return $value;
            }
            if (getenv('NEXUS_QUALIFY_OUTCOMES') === '1') {
                if ($operation === 'failure') {
                    throw new RuntimeException('nexus-target-failure');
                }
                return $value;
            }
            $ctx->sleep(300);

            return $operation;
        } catch (ServiceOperationFailed $failure) {
            if (getenv('NEXUS_QUALIFY_OUTCOMES') !== '1') {
                throw $failure;
            }
            return ['caught_class' => 'ServiceOperationFailed', 'call_id' => $failure->serviceCallId, 'message' => $failure->getMessage()];
        } catch (WorkflowCancelled $cancelled) {
            echo json_encode(['event' => $role.'.cleanup', 'context' => $cancelled->context?->toArray()], JSON_THROW_ON_ERROR).PHP_EOL;
            $ctx->cancellationShield(static fn () => $ctx->sleep($role === 'target' ? 8 : 1));
            throw $cancelled;
        }
    },
);
$worker->run(1);
