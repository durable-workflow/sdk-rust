"""Installed Python counterpart for the Rust Nexus HTTP controls."""

import asyncio
import json
import os
import signal

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import NexusOperationFailed, WorkflowCancelled

ROLE = os.environ.get("NEXUS_ROLE", "caller")
if ROLE not in {"caller", "target"}:
    raise ValueError("NEXUS_ROLE must be caller or target")
ENDPOINT = os.environ.get("NEXUS_ENDPOINT", "cooperative")
SERVICE = os.environ.get("NEXUS_SERVICE", "cleanup")


@workflow.defn(name="nexus.qualification.neighbor")
class Neighbor:
    def run(self, ctx):
        return {"alive": True, "runtime": "python"}


@workflow.defn(name=os.environ.get("WORKFLOW_TYPE", f"nexus.{ROLE}"))
class NexusFixture:
    def run(self, ctx, operation, value=None, key=None, target_namespace=None):
        try:
            if ROLE == "caller":
                if os.environ.get("NEXUS_QUALIFY_ADMISSION") == "1":
                    if operation not in {"sync-completed", "sync-accepted", "async-completed", "async-accepted"} or not key:
                        raise ValueError("Expected an admission profile and original idempotency key")
                    result = yield ctx.call_nexus_service(
                        ENDPOINT, SERVICE, operation, [operation, value, key, target_namespace], idempotency_key=key,
                        target_namespace=target_namespace,
                        mode="async" if operation.startswith("async-") else "sync",
                        wait_for="accepted" if operation.endswith("-accepted") else "completed",
                    )
                    snapshot = {"call_id": result.service_call_id, "status": result.status,
                                "response": result.result, "details": result.response_or_failure_surface}
                    event = "caller.result" if operation == "sync-completed" else "caller.admission"
                    print(json.dumps({"event": event, "snapshot": snapshot}), flush=True)
                    yield ctx.start_timer(3)
                    return snapshot
                arguments = [operation, value] if os.environ.get("NEXUS_QUALIFY_OUTCOMES") == "1" else [operation]
                result = yield ctx.call_nexus_service(ENDPOINT, SERVICE, operation, arguments)
                return result.result
            if os.environ.get("NEXUS_QUALIFY_ADMISSION") == "1":
                yield ctx.start_timer(8)
                return value
            if os.environ.get("NEXUS_QUALIFY_OUTCOMES") == "1":
                if operation == "failure":
                    raise RuntimeError("nexus-target-failure")
                return value
            yield ctx.start_timer(300)
            return operation
        except NexusOperationFailed as failure:
            if os.environ.get("NEXUS_QUALIFY_OUTCOMES") != "1":
                raise
            return {"caught_class": type(failure).__name__, "call_id": failure.service_call_id,
                    "message": str(failure), "status": failure.status}
        except WorkflowCancelled as cancelled:
            print(json.dumps({"event": f"{ROLE}.cleanup", "context": cancelled.context.to_dict()}), flush=True)
            with ctx.cancellation_shield():
                yield ctx.start_timer(8 if ROLE == "target" else 1)
            raise


async def main():
    async with Client(
        os.environ.get("DURABLE_WORKFLOW_RUNTIME_URL", "http://server:8080"),
        token=os.environ.get("DURABLE_WORKFLOW_WORKER_TOKEN", "test-token"),
        namespace=os.environ.get("DURABLE_WORKFLOW_RUNTIME_NAMESPACE", "default"),
    ) as client:
        worker = Worker(
            client, task_queue=os.environ.get("TASK_QUEUE", "nexus-workers"),
            worker_id=os.environ.get("WORKER_ID", f"nexus-{ROLE}"), workflows=[NexusFixture, Neighbor],
            capabilities=["cooperative_cancellation", "nexus_cooperative_cancellation"],
            poll_timeout=1, max_concurrent_workflow_tasks=1, max_concurrent_activity_tasks=1,
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        running = asyncio.create_task(worker.run())
        stopping = asyncio.create_task(stop.wait())
        try:
            await asyncio.wait({running, stopping}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopping.cancel()
            await worker.stop()
            await running


if __name__ == "__main__":
    asyncio.run(main())
