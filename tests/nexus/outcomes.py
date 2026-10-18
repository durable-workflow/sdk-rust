"""Actual typed Nexus results/failures with independently cold SDK reads."""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys

import httpx
from durable_workflow import Client, WorkflowHandle
from durable_workflow.errors import WorkflowFailed

from observe import history, one, request, require, until

URL = os.environ.get("DURABLE_WORKFLOW_RUNTIME_URL", "http://server:8080")
TOKEN = os.environ.get("DURABLE_WORKFLOW_WORKER_TOKEN", "test-token")


def specimen(scenario):
    return {"number": 9223372036854775807, "nothing": None, "text": "雪 😀",
            "data": "x" * (3 * 1024 * 1024 if scenario == "external" else 17)}


def exact_value(value, expected):
    require(isinstance(value, dict) and value == expected, "Typed result or exact bytes changed")
    require(type(value["number"]) is int and value["nothing"] is None, "Int64/null branch changed")


async def setup(http, namespace):
    await request(http, "POST", "/api/namespaces", {"name": namespace}, statuses=(201, 409))
    await request(http, "PUT", f"/api/namespaces/{namespace}/external-storage", {
        "driver": "local", "enabled": True, "threshold_bytes": 32768,
        "config": {"uri": f"file:///nexus-payloads/{namespace}"},
    })
    discovery = await request(http, "GET", "/api/cluster/info")
    require(discovery["namespace"]["name"] == namespace, "Discovery selected another namespace")
    policy = discovery["namespace"]["external_payload_storage"]
    require(policy["enabled"] is True and policy["status"] == "available", "External transport is not available")
    require("workflow_run" in discovery["worker_protocol"]["server_capabilities"].get(
        "nexus_cooperative_cancellation_binding_kinds", []), "Installed workflow-run binding unavailable")


async def run(case, caller, target, scenario, namespace, http, client):
    path = Path(f"/state/outcomes/{case}.json")
    require(not path.exists(), "Use a fresh original case identity")

    async def registered():
        rows = (await request(http, "GET", "/api/workers"))["workers"]
        selected = [row for row in rows if row["worker_id"] in {f"{case}-caller", f"{case}-target"}]
        return selected if len(selected) == 2 else None

    await until(registered)
    await request(http, "POST", "/api/service-endpoints", {"endpoint_name": case}, statuses=(201,))
    await request(http, "POST", f"/api/service-endpoints/{case}/services", {"service_name": "cleanup"}, statuses=(201,))
    if scenario != "missing":
        await request(http, "POST", f"/api/service-endpoints/{case}/services/cleanup/operations", {
            "operation_name": scenario, "operation_mode": "sync", "handler_binding_kind": "start_workflow",
            "handler_target_reference": "nexus.target",
            "handler_binding": {"workflow_type": "nexus.target", "queue": f"{case}-target"},
            "deadline_policy": {"timeout_seconds": 180},
        }, statuses=(201,))
    expected = specimen(scenario)
    handle = await client.start_workflow(workflow_type="nexus.caller", task_queue=f"{case}-caller",
                                         workflow_id=case, input=[scenario, expected])
    first = await history(http, case, handle.run_id)
    value = await handle.result(timeout=30, poll_interval=0.1)
    final = await history(http, case, handle.run_id)
    require(final["events"][:len(first["events"])] == first["events"], "Committed caller prefix changed")
    if scenario == "missing":
        failure = one(final, "ServiceCallFailed")["payload"]
        require(not any(event["event_type"] == "ServiceCallStarted" for event in final["events"]),
                "Unresolved operation was reported as started")
        call_id = failure["service_call_id"]
        call = failure["service_call"]
        require(call["reason"] == "operation_not_found", "Missing operation failed for another reason")
    else:
        call_id = one(final, "ServiceCallStarted")["payload"]["service_call_id"]
        call_path = f"/api/service-endpoints/{case}/services/cleanup/operations/{scenario}/service-calls/{call_id}"
        call = await request(http, "GET", call_path)
    require(call["caller_workflow_instance_id"] == case and call["caller_workflow_run_id"] == handle.run_id,
            "Service call lost original caller identity")
    if scenario != "missing":
        require(call["deadline_policy"]["timeout_seconds"] == 180, "Original service policy changed")
    target_id, target_run = call.get("linked_workflow_instance_id"), call.get("linked_workflow_run_id")
    target_history = None
    if scenario == "missing":
        require(target_id is None and target_run is None, "Missing operation created target work")
    else:
        require(target_id and target_run, "Committed call lost its target identity")
        target_history = await history(http, target_id, target_run)
        await cold_target(client, scenario, target_id, target_run, expected)
    if scenario in {"success", "external"}:
        exact_value(value, expected)
        require(call["status"] == "completed", "Successful operation did not commit completed")
        require(one(final, "ServiceCallCompleted")["payload"]["service_call_id"] == call_id,
                "Completion refers to another call")
        if scenario == "external":
            require("external_storage" in one(final, "ServiceCallCompleted")["payload"]["response_payload"],
                    "Large service result was not externally retained")
    else:
        expected_class = "ServiceOperationFailed" if caller == "php" else "NexusOperationFailed"
        require(value["caught_class"] == expected_class and value["call_id"] == call_id,
                "Caller did not catch the typed failure for its original call")
        require(call["status"] == "failed", "Failed operation did not commit failed")
        require(one(final, "ServiceCallFailed")["payload"]["service_call_id"] == call_id,
                "Failure refers to another call")
        if scenario == "failure":
            require("nexus-target-failure" in value["message"], "Original target failure message changed")
    async with Client(URL, token=TOKEN, namespace=namespace) as cold:
        repeated = await WorkflowHandle(cold, case, handle.run_id).result(timeout=1)
        require(repeated == value, "Cold selected-run result changed")
    require(await history(http, case, handle.run_id) == final, "Cold read mutated caller history")
    if target_history is not None:
        require(await history(http, target_id, target_run) == target_history, "Cold read mutated target history")
    summary = {"case": case, "namespace": namespace, "caller": caller, "target": target,
               "scenario": scenario, "caller_run": handle.run_id, "service_call_id": call_id,
               "target_id": target_id, "target_run": target_run, "status": call["status"],
               "caller_events": len(final["events"]), "target_events": len(target_history["events"]) if target_history else 0,
               "data_bytes": len(expected["data"]), "data_sha256": hashlib.sha256(expected["data"].encode()).hexdigest(),
               "rust_source": os.environ["NEXUS_RUST_SOURCE"],
               "rust_binary_sha256": os.environ["NEXUS_RUST_BINARY_SHA256"], "workflow_outcome": "pass"}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary), encoding="utf-8")
    print(json.dumps(summary), flush=True)


async def cold_target(client, scenario, target_id, target_run, expected):
    handle = WorkflowHandle(client, target_id, target_run)
    if scenario == "failure":
        try:
            await handle.result(timeout=1)
        except WorkflowFailed as failure:
            require("nexus-target-failure" in str(failure), "Cold target failure changed")
        else:
            raise AssertionError("Target did not retain typed workflow failure")
    else:
        exact_value(await handle.result(timeout=1), expected)


async def main():
    phase, namespace, *args = sys.argv[1:]
    headers = {"Authorization": f"Bearer {TOKEN}", "X-Namespace": namespace,
               "X-Durable-Workflow-Control-Plane-Version": "2"}
    async with httpx.AsyncClient(base_url=URL, headers=headers, timeout=30) as http:
        if phase == "setup":
            await setup(http, namespace)
        elif phase == "run":
            async with Client(URL, token=TOKEN, namespace=namespace) as client:
                await run(*args, namespace, http, client)
        else:
            raise ValueError("Unknown outcome phase")


if __name__ == "__main__":
    asyncio.run(main())
