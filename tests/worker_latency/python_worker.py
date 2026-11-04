"""Published Python SDK reference for examples/worker_latency.rs.

Install the exact published SDK under comparison in an isolated environment.
The Rust driver supplies the identical workflow payload and checks the result.
"""

import asyncio
import json
import os
import signal

from durable_workflow import Client, Worker, activity, workflow
from durable_workflow.metrics import CLIENT_REQUEST_DURATION_SECONDS


class Requests:
    def __init__(self):
        self.routes = {}

    def increment(self, _name, _value=1, tags=None):
        pass

    def record(self, name, value, tags=None):
        if name == CLIENT_REQUEST_DURATION_SECONDS:
            self.routes.setdefault((tags or {}).get("route", ""), []).append(value * 1000)

    def summary(self):
        return {
            route: {"count": len(values), "samples_ms": values}
            for route, values in self.routes.items()
        }


@activity.defn(name="bench.echo")
async def echo(payload: str) -> str:
    return payload


@workflow.defn(name="bench.one-activity")
class OneActivity:
    def run(self, ctx, payload):
        return (yield ctx.schedule_activity("bench.echo", [payload]))


async def main():
    requests = Requests()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, stop.set)
    capacity = int(os.environ.get("SDK_BENCH_CAPACITY", "16"))
    if not 1 <= capacity <= 16:
        raise ValueError("benchmark capacity must be 1..16")
    async with Client(
        os.environ["DURABLE_WORKFLOW_SERVER_URL"],
        token=os.environ.get("DURABLE_WORKFLOW_TOKEN"),
        namespace="default",
        metrics=requests,
    ) as client:
        worker = Worker(
            client, task_queue=os.environ["TASK_QUEUE"],
            workflows=[OneActivity], activities=[echo],
            max_concurrent_workflow_tasks=capacity,
            max_concurrent_activity_tasks=capacity,
        )
        runner = asyncio.create_task(worker.run())
        stopping = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait(
                [runner, stopping], timeout=180, return_when=asyncio.FIRST_COMPLETED
            )
            if runner in done:
                await runner
        finally:
            await worker.stop()
            await runner
            stopping.cancel()
    print(json.dumps({"mode": "worker", "capacity": capacity, "requests": requests.summary()}))


if __name__ == "__main__":
    asyncio.run(main())
