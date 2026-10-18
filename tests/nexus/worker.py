"""Installed Python counterpart for the Rust Nexus HTTP controls."""

import asyncio
import json
import os
import signal

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import WorkflowCancelled

ROLE = os.environ.get("NEXUS_ROLE", "caller")
if ROLE not in {"caller", "target"}:
    raise ValueError("NEXUS_ROLE must be caller or target")
ENDPOINT = os.environ.get("NEXUS_ENDPOINT", "cooperative")
SERVICE = os.environ.get("NEXUS_SERVICE", "cleanup")


@workflow.defn(name=os.environ.get("WORKFLOW_TYPE", f"nexus.{ROLE}"))
class NexusFixture:
    def run(self, ctx, operation):
        try:
            if ROLE == "caller":
                result = yield ctx.call_nexus_service(ENDPOINT, SERVICE, operation, [operation])
                return result.result
            yield ctx.start_timer(300)
            return operation
        except WorkflowCancelled as cancelled:
            print(json.dumps({"event": f"{ROLE}.cleanup", "context": cancelled.context.to_dict()}), flush=True)
            with ctx.cancellation_shield():
                yield ctx.start_timer(8 if ROLE == "target" else 1)
            raise


async def main():
    async with Client(
        os.environ.get("DURABLE_WORKFLOW_RUNTIME_URL", "http://server:8080"),
        token=os.environ.get("DURABLE_WORKFLOW_WORKER_TOKEN", "test-token"),
    ) as client:
        worker = Worker(
            client, task_queue=os.environ.get("TASK_QUEUE", "nexus-workers"),
            worker_id=os.environ.get("WORKER_ID", f"nexus-{ROLE}"), workflows=[NexusFixture],
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
