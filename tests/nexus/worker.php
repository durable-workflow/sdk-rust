<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Exception\WorkflowCancelled;
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
);
$worker = new Worker(
    $client,
    getenv('TASK_QUEUE') ?: 'nexus-workers',
    workerId: getenv('WORKER_ID') ?: 'nexus-'.$role,
    enableCooperativeCancellation: true,
);
$worker->registerWorkflow(
    getenv('WORKFLOW_TYPE') ?: 'nexus.'.$role,
    static function (WorkflowContext $ctx, string $operation) use ($role, $endpoint, $service): mixed {
        try {
            if ($role === 'caller') {
                return $ctx->serviceOperation($endpoint, $service, $operation, [$operation])->responsePayload;
            }
            $ctx->sleep(300);

            return $operation;
        } catch (WorkflowCancelled $cancelled) {
            echo json_encode(['event' => $role.'.cleanup', 'context' => $cancelled->context?->toArray()], JSON_THROW_ON_ERROR).PHP_EOL;
            $ctx->cancellationShield(static fn () => $ctx->sleep($role === 'target' ? 8 : 1));
            throw $cancelled;
        }
    },
);
$worker->run(1);
