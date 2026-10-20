"""A real service admission commits before its HTTP acknowledgement is lost."""

import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import re
import sys

import httpx
from durable_workflow import Client, WorkflowHandle

from admissions import TERMINAL, remaining, snapshots
from observe import history, one, request, require, stamp, until
from outcomes import exact_value, foreign_namespace, specimen

URL = os.environ.get("DURABLE_WORKFLOW_RUNTIME_URL", "http://server:8080")
TOKEN = os.environ.get("DURABLE_WORKFLOW_WORKER_TOKEN", "test-token")
DIRECTORY = Path("/state/acknowledgements")


def save(record):
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    (DIRECTORY / f"{record['case']}.json").write_text(json.dumps(record), encoding="utf-8")


async def prepare(case, caller, target, namespace, target_namespace, http, client, target_http, proxy):
    require(not (DIRECTORY / f"{case}.json").exists(), "Use a fresh original identity")

    async def registered():
        callers = (await request(http, "GET", "/api/workers"))["workers"]
        targets = (await request(target_http, "GET", "/api/workers"))["workers"]
        return True if (any(row["worker_id"] == f"{case}-caller" for row in callers)
                        and any(row["worker_id"] == f"{case}-target" for row in targets)) else None

    await until(registered)
    receipt = await proxy.get("/__qualification/lost-ack")
    require(receipt.status_code == 200 and receipt.json() is None, "Proxy retained another original")
    await request(target_http, "POST", "/api/service-endpoints", {"endpoint_name": case}, statuses=(201,))
    await request(target_http, "POST", f"/api/service-endpoints/{case}/services", {"service_name": "cleanup"}, statuses=(201,))
    await request(target_http, "POST", f"/api/service-endpoints/{case}/services/cleanup/operations", {
        "operation_name": "sync-completed", "operation_mode": "sync", "handler_binding_kind": "start_workflow",
        "handler_target_reference": "nexus.target",
        "handler_binding": {"workflow_type": "nexus.target", "queue": f"{case}-target"},
        "deadline_policy": {"timeout_seconds": 180},
    }, statuses=(201,))
    handle = await client.start_workflow(workflow_type="nexus.caller", task_queue=f"{case}-caller",
        workflow_id=case, input=["sync-completed", specimen("success"), case, target_namespace])
    initial = await history(http, case, handle.run_id)
    start = one(initial, "WorkflowStarted")
    require(start["payload"]["workflow_instance_id"] == case
            and start["payload"]["workflow_run_id"] == handle.run_id, "Wrong original start")
    record = {"case": case, "caller": caller, "target": target, "namespace": namespace,
              "target_namespace": target_namespace, "caller_run": handle.run_id, "initial_history": initial,
              "observation_deadline": (stamp(start["timestamp"]) + timedelta(seconds=30)).isoformat(),
              "rust_source": os.environ["NEXUS_RUST_SOURCE"],
              "rust_binary_sha256": os.environ["NEXUS_RUST_BINARY_SHA256"]}
    save(record)

    async def committed():
        reply = await proxy.get("/__qualification/lost-ack")
        require(reply.status_code == 200, "Proxy receipt became unavailable")
        captured = reply.json()
        if captured is None:
            return None
        current = await history(http, case, handle.run_id)
        events = [row for row in current["events"] if row["event_type"] == "ServiceCallStarted"]
        return (captured, current) if events else None

    async with asyncio.timeout(remaining(record)):
        captured, current = await until(committed)
        admission = one(current, "ServiceCallStarted")["payload"]
        require(captured["acknowledgement_dropped"] is True and captured["upstream_status"] == 200
                and captured["upstream_recorded"] is True
                and captured["command_type"] == "start_service_operation", "No confirmed original ACK loss")
        require(captured["path"] == f"/api/worker/workflow-tasks/{admission['task']['id']}/complete",
                "Proxy fault selected another original task")
        require(captured["intent"] == {"endpoint_name": case, "service_name": "cleanup",
                "operation_name": "sync-completed", "namespace": target_namespace, "idempotency_key": case},
                "Proxy fault selected another service intent")
        require(re.fullmatch(r"[0-9a-f]{64}", captured["command_sha256"]) is not None,
                "Original completion body hash is missing")
        require(admission["sequence"] == 1 and admission["operation_mode"] == "sync"
                and admission["wait_for"] == "completed"
                and admission["service_call"]["target_namespace"] == target_namespace
                and admission["service_call"]["caller_namespace"] == namespace,
                "Original admission authority changed")
        require(not any(row["event_type"] in TERMINAL for row in current["events"]),
                "Caller became terminal before cold recovery")
        record["receipt"], record["admission_history"] = captured, current
        record["call_id"] = admission["service_call_id"]
        record["call_path"] = (f"/api/service-endpoints/{case}/services/cleanup/operations/"
                               f"sync-completed/service-calls/{record['call_id']}")
        record["original_call"] = await request(target_http, "GET", record["call_path"])
    save(record)
    print(json.dumps({"phase": "lost-ack", "case": case, "caller_run": handle.run_id,
                      "call_id": record["call_id"]}), flush=True)


async def retire(record, client):
    reply = await client.deregister_worker_registration(f"{record['case']}-caller")
    require(type(reply["recovered_workflow_task_count"]) is int and reply["recovered_workflow_task_count"] == 0,
            "Original completed claim was recovered as an active lease")
    record["retired_registration"] = reply


async def target_complete(record, http, target_http, target_client):
    async def complete():
        call = await request(target_http, "GET", record["call_path"])
        if call["status"] != "completed":
            return None
        caller = await history(http, record["case"], record["caller_run"])
        return call, caller

    call, caller = await until(complete)
    require(not any(row["event_type"] in TERMINAL for row in caller["events"]),
            "Original caller completed without cold replacement")
    require(one(caller, "ServiceCallStarted")["payload"]["service_call_id"]
            == call["id"] == record["call_id"], "Terminal result changed original call identity")
    # Core stages terminal delivery durably; recordForClaim appends its history
    # event only after the replacement claims the original caller task.
    deliveries = [row for row in caller["events"]
                  if row["event_type"] in {"ServiceCallCompleted", "ServiceCallFailed", "ServiceCallCancelled"}]
    require(len(deliveries) <= 1 and all(row["event_type"] == "ServiceCallCompleted"
            and row["payload"]["service_call_id"] == record["call_id"] for row in deliveries),
            "Pending delivery contradicts the original completed call")
    for key in ("id", "namespace", "caller_namespace", "caller_workflow_instance_id", "caller_workflow_run_id",
                "target_namespace", "linked_workflow_instance_id", "linked_workflow_run_id",
                "idempotency_key", "deadline_policy", "accepted_at", "call_deadline_at"):
        require(call[key] == record["original_call"][key], f"Original {key} changed")
    require(call["namespace"] == call["target_namespace"] == record["target_namespace"]
            and call["caller_namespace"] == record["namespace"]
            and call["caller_workflow_instance_id"] == record["case"]
            and call["caller_workflow_run_id"] == record["caller_run"]
            and call["idempotency_key"] == record["case"]
            and call["deadline_policy"]["timeout_seconds"] == 180, "Original service authority changed")
    selected = await history(target_http, call["linked_workflow_instance_id"], call["linked_workflow_run_id"])
    origin = one(selected, "WorkflowStarted")["payload"]
    require(origin["workflow_instance_id"] == call["linked_workflow_instance_id"]
            and origin["workflow_run_id"] == call["linked_workflow_run_id"], "Target selector changed")
    timer = one(selected, "TimerScheduled")["payload"]
    require(timer["sequence"] == 1 and timer["delay_seconds"] == 8
            and one(selected, "TimerFired")["payload"]["timer_id"] == timer["timer_id"], "Target timer changed")
    exact_value(await WorkflowHandle(target_client, call["linked_workflow_instance_id"],
                                    call["linked_workflow_run_id"]).result(timeout=1), specimen("success"))
    record["final_call"], record["before_cold_history"], record["target_history"] = call, caller, selected


async def caller_complete(record, http, client):
    result = await WorkflowHandle(client, record["case"], record["caller_run"]).result(
        timeout=remaining(record), poll_interval=0.1)
    require(result["call_id"] == record["call_id"] and result["status"] == "completed",
            "Cold caller returned another admission or a pending result")
    exact_value(result["response"], specimen("success"))
    current = await history(http, record["case"], record["caller_run"])
    prefix = record["before_cold_history"]["events"]
    require(current["events"][:len(prefix)] == prefix, "Committed original prefix changed")
    require(one(current, "ServiceCallStarted")["payload"]["service_call_id"]
            == one(current, "ServiceCallCompleted")["payload"]["service_call_id"] == record["call_id"],
            "Cold replay readmitted the original call")
    timer = one(current, "TimerScheduled")["payload"]
    require(timer["sequence"] == 2 and timer["delay_seconds"] == 3
            and one(current, "TimerFired")["payload"]["timer_id"] == timer["timer_id"], "Caller timer changed")
    require(stamp(one(record["target_history"], "WorkflowCompleted")["timestamp"])
            < stamp(one(current, "WorkflowCompleted")["timestamp"]) <= stamp(record["observation_deadline"]),
            "Cold schedule or original observation changed")
    async with Client(URL, token=TOKEN, namespace=record["namespace"]) as cold:
        require(await WorkflowHandle(cold, record["case"], record["caller_run"]).result(timeout=1) == result,
                "Independent cold read changed the exact result")
    require(await history(http, record["case"], record["caller_run"]) == current, "Cold result read changed history")
    record["result"], record["caller_history"] = result, current


async def verify(record, http, target_http):
    case = record["case"]
    original = json.loads((DIRECTORY / f"{case}-original-exit.json").read_text(encoding="utf-8"))
    require(original["Running"] is False and original["OOMKilled"] is False, "Original Worker is not retired")
    observed = snapshots(DIRECTORY / f"{case}-caller-final.log", event="caller.result")
    require(observed and all(value == record["result"] for value in observed),
            "Workflow-facing terminal result changed on cold replay")
    states = []
    for role in ("caller", "target"):
        state = json.loads((DIRECTORY / f"{case}-{role}-exit.json").read_text(encoding="utf-8"))
        require(state["ExitCode"] == 0 and state["OOMKilled"] is False, "Final Worker shutdown failed")
        states.append({"role": role, "exit_code": 0, "oom_killed": False})
    call = record["final_call"]
    if record["namespace"] != record["target_namespace"]:
        await request(target_http, "GET", f"/api/workflows/{case}/runs/{record['caller_run']}/history", statuses=(404,))
        await request(http, "GET", f"/api/workflows/{call['linked_workflow_instance_id']}/runs/{call['linked_workflow_run_id']}/history", statuses=(404,))
        record["foreign_namespace_refusals"] = 2
    else:
        record["foreign_namespace_refusals"] = await foreign_namespace(http, record["namespace"], case,
            record["caller_run"], call["linked_workflow_instance_id"], call["linked_workflow_run_id"])
    record["original_worker_exit_code"], record["worker_exits"] = original["ExitCode"], states
    record["outcome"] = "pass"
    print(json.dumps({key: record[key] for key in ("case", "namespace", "target_namespace", "caller", "target",
        "caller_run", "call_id", "foreign_namespace_refusals", "rust_source", "outcome")}), flush=True)


async def main():
    phase, namespace, case, *roles = sys.argv[1:]
    require(re.fullmatch(r"[a-z0-9-]{1,80}", case) is not None, "Use a bounded original identity")
    target_namespace = os.environ["NEXUS_TARGET_NAMESPACE"]
    headers = {"Authorization": f"Bearer {TOKEN}", "X-Namespace": namespace,
               "X-Durable-Workflow-Control-Plane-Version": "2"}
    async with (httpx.AsyncClient(base_url=URL, headers=headers, timeout=10) as http,
                httpx.AsyncClient(base_url=URL, headers={**headers, "X-Namespace": target_namespace}, timeout=10) as target_http,
                httpx.AsyncClient(base_url="http://ack-proxy:3000", timeout=10) as proxy,
                Client(URL, token=TOKEN, namespace=namespace) as client,
                Client(URL, token=TOKEN, namespace=target_namespace) as target_client):
        if phase == "prepare":
            await prepare(case, *roles, namespace, target_namespace, http, client, target_http, proxy)
            return
        record = json.loads((DIRECTORY / f"{case}.json").read_text(encoding="utf-8"))
        require(record["namespace"] == namespace and record["target_namespace"] == target_namespace,
                "Observer namespace changed during the original")
        if phase == "verify":
            await verify(record, http, target_http)
        else:
            async with asyncio.timeout(remaining(record)):
                if phase == "retire":
                    await retire(record, client)
                elif phase == "target":
                    await target_complete(record, http, target_http, target_client)
                elif phase == "caller":
                    await caller_complete(record, http, client)
                else:
                    raise ValueError("Unknown lost-ack phase")
        save(record)


if __name__ == "__main__":
    asyncio.run(main())
