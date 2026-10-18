"""Actual typed Nexus results/failures with independently cold SDK reads."""

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

import httpx
from durable_workflow import Client, WorkflowHandle
from durable_workflow.errors import WorkflowFailed

from observe import history, one, request, require, stamp, until

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


async def run(case, caller, target, scenario, namespace, target_namespace, http, client, target_http, target_client):
    path = Path(f"/state/outcomes/{case}.json")
    require(not path.exists(), "Use a fresh original case identity")

    async def registered():
        callers = (await request(http, "GET", "/api/workers"))["workers"]
        targets = (await request(target_http, "GET", "/api/workers"))["workers"]
        selected = ([row for row in callers if row["worker_id"] == f"{case}-caller"]
                    + [row for row in targets if row["worker_id"] == f"{case}-target"])
        return selected if len(selected) == 2 else None

    await until(registered)
    await request(target_http, "POST", "/api/service-endpoints", {"endpoint_name": case}, statuses=(201,))
    await request(target_http, "POST", f"/api/service-endpoints/{case}/services", {"service_name": "cleanup"}, statuses=(201,))
    if scenario != "missing":
        await request(target_http, "POST", f"/api/service-endpoints/{case}/services/cleanup/operations", {
            "operation_name": scenario, "operation_mode": "sync", "handler_binding_kind": "start_workflow",
            "handler_target_reference": "nexus.target",
            "handler_binding": {"workflow_type": "nexus.target", "queue": f"{case}-target"},
            "deadline_policy": {"timeout_seconds": 180},
        }, statuses=(201,))
    expected = specimen(scenario)
    handle = await client.start_workflow(workflow_type="nexus.caller", task_queue=f"{case}-caller",
                                         workflow_id=case, input=[scenario, expected, case, target_namespace])
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = {"case": case, "namespace": namespace, "target_namespace": target_namespace,
               "scenario": scenario, "caller_run": handle.run_id, "workflow_outcome": "pending",
               "rust_source": os.environ["NEXUS_RUST_SOURCE"]}
    path.write_text(json.dumps(pending), encoding="utf-8")
    print(json.dumps({"phase": "admitted", "case": case, "caller_run": handle.run_id}), flush=True)
    first = await history(http, case, handle.run_id)
    deadline = stamp(one(first, "WorkflowStarted")["timestamp"]) + timedelta(seconds=30)
    pending["observation_deadline"] = deadline.isoformat()
    path.write_text(json.dumps(pending), encoding="utf-8")
    available = (deadline - datetime.now(timezone.utc)).total_seconds()
    require(available > 0, "Original thirty-second observation expired")
    value = await handle.result(timeout=available, poll_interval=0.1)
    final = await history(http, case, handle.run_id)
    require(stamp(one(final, "WorkflowCompleted")["timestamp"]) <= deadline, "Original observation budget changed")
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
        call = await request(target_http, "GET", call_path)
    caller_identity = failure if scenario == "missing" else call
    require(caller_identity["caller_workflow_instance_id"] == case
            and caller_identity["caller_workflow_run_id"] == handle.run_id,
            "Service call lost original caller identity")
    require(call["namespace"] == target_namespace, "Service call escaped its selected namespace")
    if scenario != "missing":
        require(call["deadline_policy"]["timeout_seconds"] == 180, "Original service policy changed")
        require(call["caller_namespace"] == namespace and call["target_namespace"] == target_namespace
                and call["idempotency_key"] == case, "Original namespace or idempotency authority changed")
    target_id, target_run = call.get("linked_workflow_instance_id"), call.get("linked_workflow_run_id")
    target_history = None
    if scenario == "missing":
        require(target_id is None and target_run is None, "Missing operation created target work")
        require(call["accepted_at"] is None and call["started_at"] is None and call["call_deadline_at"] is None,
                "Unresolved operation fabricated admission or an operation deadline")
    else:
        require(target_id and target_run, "Committed call lost its target identity")
        target_history = await history(target_http, target_id, target_run)
        await cold_target(target_client, scenario, target_id, target_run, expected)
    if scenario in {"success", "external"}:
        exact_value(value, expected)
        require(call["status"] == "completed", "Successful operation did not commit completed")
        require(one(final, "ServiceCallCompleted")["payload"]["service_call_id"] == call_id,
                "Completion refers to another call")
        if scenario == "external":
            envelope = one(final, "ServiceCallCompleted")["payload"]["response_payload"]
            await external_result(http, envelope)
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
        require(await history(target_http, target_id, target_run) == target_history, "Cold read mutated target history")
    foreign_refusals = await foreign_namespace(http, namespace, case, handle.run_id, target_id, target_run,
                                             target_namespace=target_namespace)
    summary = {"case": case, "namespace": namespace, "target_namespace": target_namespace,
               "caller": caller, "target": target, "observation_deadline": deadline.isoformat(),
               "scenario": scenario, "caller_run": handle.run_id, "service_call_id": call_id,
               "target_id": target_id, "target_run": target_run, "status": call["status"],
               "caller_events": len(final["events"]), "target_events": len(target_history["events"]) if target_history else 0,
               "data_bytes": len(expected["data"]), "data_sha256": hashlib.sha256(expected["data"].encode()).hexdigest(),
               "rust_source": os.environ["NEXUS_RUST_SOURCE"],
               "rust_binary_sha256": os.environ["NEXUS_RUST_BINARY_SHA256"],
               "foreign_namespace_refusals": foreign_refusals, "workflow_outcome": "pass"}
    if os.environ.get("NEXUS_QUALIFY_UI") == "1":
        fields = ("namespace", "endpoint_name", "service_name", "operation_name", "status",
                  "outcome", "accepted_at", "call_deadline_at", "idempotency_key")
        original = {key: call[key] for key in fields if key in call}
        original.update({"id": call_id, "caller_namespace": namespace,
                         "target_namespace": target_namespace,
                         "caller_workflow_instance_id": case, "caller_workflow_run_id": handle.run_id,
                         "linked_workflow_instance_id": target_id, "linked_workflow_run_id": target_run})
        summary["catalog"] = {"original": original, "caller_history": final,
                              "target_history": target_history}
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


async def external_result(http, envelope):
    reference = envelope.get("external_payload")
    require(isinstance(reference, dict), "Large service result lacks its opaque runtime reference")
    require(reference["schema"] == "durable-workflow.v2.runtime-external-payload-reference.v1"
            and reference["codec"] == "avro" and reference["size_bytes"] > 3 * 1024 * 1024,
            "External result metadata changed")
    response = await http.get(f"/api/external-payloads/v1/{reference['reference_id']}", headers={
        "X-Durable-Workflow-Payload-Codec": reference["codec"],
        "X-Durable-Workflow-Payload-Size": str(reference["size_bytes"]),
        "X-Durable-Workflow-Payload-SHA256": reference["sha256"],
    })
    require(response.status_code == 200 and len(response.content) == reference["size_bytes"],
            "External transport lost the exact original byte count")
    require(hashlib.sha256(response.content).hexdigest() == reference["sha256"],
            "External transport changed the exact original byte hash")


async def foreign_namespace(http, namespace, caller_id, caller_run, target_id, target_run, target_namespace=None):
    foreign = "nexus-source" if namespace == "default" else "default"
    selected = [(caller_id, caller_run, foreign)]
    if target_id is not None:
        target_foreign = "nexus-source" if (target_namespace or namespace) == "default" else "default"
        selected.append((target_id, target_run, target_foreign))
    for workflow_id, run_id, selected_namespace in selected:
        response = await http.get(f"/api/workflows/{workflow_id}/runs/{run_id}/history",
                                  headers={"X-Namespace": selected_namespace})
        require(response.status_code == 404, "Foreign namespace exposed the original run")
    return len(selected)


async def main():
    phase, namespace, *args = sys.argv[1:]
    headers = {"Authorization": f"Bearer {TOKEN}", "X-Namespace": namespace,
               "X-Durable-Workflow-Control-Plane-Version": "2"}
    target_namespace = os.environ.get("NEXUS_TARGET_NAMESPACE", namespace)
    async with (httpx.AsyncClient(base_url=URL, headers=headers, timeout=30) as http,
                httpx.AsyncClient(base_url=URL, headers={**headers, "X-Namespace": target_namespace}, timeout=30) as target_http):
        if phase == "setup":
            await setup(http, namespace)
        elif phase == "run":
            async with (Client(URL, token=TOKEN, namespace=namespace) as client,
                        Client(URL, token=TOKEN, namespace=target_namespace) as target_client):
                await run(*args, namespace, target_namespace, http, client, target_http, target_client)
        else:
            raise ValueError("Unknown outcome phase")


if __name__ == "__main__":
    asyncio.run(main())
