"""Original Nexus admission survives ordinary and killed-caller replay."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys

import httpx
from durable_workflow import Client, WorkflowHandle

from observe import history, one, request, require, stamp, until
from outcomes import exact_value, foreign_namespace, setup, specimen

URL = os.environ.get("DURABLE_WORKFLOW_RUNTIME_URL", "http://server:8080")
TOKEN = os.environ.get("DURABLE_WORKFLOW_WORKER_TOKEN", "test-token")
DIRECTORY = Path("/state/admissions")
TERMINAL = {"WorkflowCompleted", "WorkflowFailed", "WorkflowCancelled", "WorkflowTerminated", "WorkflowTimedOut"}


def save(record):
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    (DIRECTORY / f"{record['case']}.json").write_text(json.dumps(record), encoding="utf-8")


def remaining(record):
    seconds = (stamp(record["observation_deadline"]) - datetime.now(timezone.utc)).total_seconds()
    require(seconds > 0, "Original thirty-second observation expired")
    return seconds


async def prepare(case, caller, target, profile, schedule, namespace, http, client):
    require(not (DIRECTORY / f"{case}.json").exists(), "Use a fresh original identity")
    async def registered():
        workers = (await request(http, "GET", "/api/workers"))["workers"]
        selected = [row for row in workers if row["worker_id"] in {f"{case}-caller", f"{case}-target"}]
        return selected if len(selected) == 2 else None
    registrations = await until(registered)
    await request(http, "POST", "/api/service-endpoints", {"endpoint_name": case}, statuses=(201,))
    await request(http, "POST", f"/api/service-endpoints/{case}/services", {"service_name": "cleanup"}, statuses=(201,))
    await request(http, "POST", f"/api/service-endpoints/{case}/services/cleanup/operations", {
        "operation_name": profile, "operation_mode": profile.split("-")[0],
        "handler_binding_kind": "start_workflow", "handler_target_reference": "nexus.target",
        "handler_binding": {"workflow_type": "nexus.target", "queue": f"{case}-target"},
        "deadline_policy": {"timeout_seconds": 180},
    }, statuses=(201,))
    handle = await client.start_workflow(workflow_type="nexus.caller", task_queue=f"{case}-caller",
                                         workflow_id=case, input=[profile, specimen("success"), case])
    initial = await history(http, case, handle.run_id)
    origin = one(initial, "WorkflowStarted")["payload"]
    require(origin["workflow_instance_id"] == case and origin["workflow_run_id"] == handle.run_id,
            "Initial history selected another original run")
    start = stamp(one(initial, "WorkflowStarted")["timestamp"])
    record = {"case": case, "caller": caller, "target": target, "profile": profile,
              "schedule": schedule, "namespace": namespace, "caller_run": handle.run_id,
              "observation_deadline": (start + timedelta(seconds=30)).isoformat(),
              "initial_history": initial, "registrations": registrations,
              "rust_source": os.environ["NEXUS_RUST_SOURCE"],
              "rust_binary_sha256": os.environ["NEXUS_RUST_BINARY_SHA256"], "workflow_outcome": "pending"}
    save(record)
    print(json.dumps({"phase": "admitted", "case": case, "caller_run": handle.run_id}), flush=True)


async def admitted(record, http):
    async def committed():
        current = await history(http, record["case"], record["caller_run"])
        if not any(event["event_type"] == "TimerScheduled" for event in current["events"]):
            return None
        timer = one(current, "TimerScheduled")["payload"]
        require(timer["sequence"] == 2 and timer["delay_seconds"] == 3, "Caller continuation timer changed")
        return current
    current = await until(committed)
    if record["schedule"] == "cold":
        require(not any(event["event_type"] in TERMINAL for event in current["events"]),
                "Caller finished before the cold-replacement boundary")
    admission = one(current, "ServiceCallStarted")["payload"]
    mode, wait_for = record["profile"].split("-")
    require(admission["sequence"] == 1 and admission["operation_mode"] == mode and admission["wait_for"] == wait_for,
            "Original authored mode/wait policy changed")
    record["admission_history"] = current
    record["call_id"] = admission["service_call_id"]
    record["call_path"] = (f"/api/service-endpoints/{record['case']}/services/cleanup/operations/"
                           f"{record['profile']}/service-calls/{record['call_id']}")
    record["original_call"] = await request(http, "GET", record["call_path"])
    require(record["original_call"]["idempotency_key"] == record["case"], "Original idempotency key changed")


async def retire(record, client):
    reply = await client.deregister_worker_registration(f"{record['case']}-caller")
    require(type(reply["recovered_workflow_task_count"]) is int and reply["recovered_workflow_task_count"] == 0,
            "Killed caller retained a lease at its durable boundary")
    record["retired_registration"] = reply


async def target_complete(record, http, client):
    async def complete():
        call = await request(http, "GET", record["call_path"])
        if call["status"] != "completed":
            return None
        selected = await history(http, call["linked_workflow_instance_id"], call["linked_workflow_run_id"])
        return (call, selected) if any(event["event_type"] == "WorkflowCompleted" for event in selected["events"]) else None
    call, selected = await until(complete)
    require(call["id"] == record["call_id"] and call["namespace"] == record["namespace"]
            and call["target_namespace"] == record["namespace"]
            and call["caller_workflow_instance_id"] == record["case"]
            and call["caller_workflow_run_id"] == record["caller_run"], "Original call authority changed")
    origin = one(selected, "WorkflowStarted")["payload"]
    require(origin["workflow_instance_id"] == call["linked_workflow_instance_id"]
            and origin["workflow_run_id"] == call["linked_workflow_run_id"], "Target history selected another run")
    require(call["idempotency_key"] == record["case"] and call["deadline_policy"]["timeout_seconds"] == 180,
            "Original call identity or policy changed")
    before = record["original_call"]
    for key in ("accepted_at", "call_deadline_at", "linked_workflow_instance_id", "linked_workflow_run_id"):
        if before.get(key) is not None:
            require(call[key] == before[key], f"Original {key} changed")
    timer = one(selected, "TimerScheduled")["payload"]
    require(timer["sequence"] == 1 and timer["delay_seconds"] == 8, "Target timer changed")
    require(one(selected, "TimerFired")["payload"]["timer_id"] == timer["timer_id"], "Target resumed another timer")
    exact_value(await WorkflowHandle(client, call["linked_workflow_instance_id"],
                                     call["linked_workflow_run_id"]).result(timeout=1), specimen("success"))
    record["final_call"], record["target_history"] = call, selected
    if record["schedule"] == "cold":
        waiting = await history(http, record["case"], record["caller_run"])
        require(not any(event["event_type"] in TERMINAL for event in waiting["events"]),
                "Killed caller completed without its replacement")
        record["cold_waiting_history"] = waiting


async def caller_complete(record, http, client):
    value = await WorkflowHandle(client, record["case"], record["caller_run"]).result(
        timeout=remaining(record), poll_interval=0.1)
    current = await history(http, record["case"], record["caller_run"])
    admission = one(current, "ServiceCallStarted")["payload"]
    require(value["call_id"] == record["call_id"] and value["status"] == admission["status"]
            and value["status"] in {"accepted", "started"} and value["response"] is None,
            "Replay replaced the original admission with a terminal response")
    require(not any(event["event_type"] in {"ServiceCallCompleted", "ServiceCallFailed", "ServiceCallCancelled"}
                    for event in current["events"]), "Acceptance-only call gained a terminal delivery")
    prefix = record["admission_history"]["events"]
    require(current["events"][:len(prefix)] == prefix, "Original admission prefix changed")
    caller_time = stamp(one(current, "WorkflowCompleted")["timestamp"])
    target_time = stamp(one(record["target_history"], "WorkflowCompleted")["timestamp"])
    require(caller_time <= stamp(record["observation_deadline"]) and target_time <= stamp(record["observation_deadline"]),
            "Original observation budget changed")
    require((caller_time < target_time) if record["schedule"] == "ordinary" else (caller_time > target_time),
            "Caller did not follow its declared admission/cold schedule")
    timer = one(current, "TimerScheduled")["payload"]
    require(one(current, "TimerFired")["payload"]["timer_id"] == timer["timer_id"], "Caller resumed another timer")
    async with Client(URL, token=TOKEN, namespace=record["namespace"]) as cold:
        require(await WorkflowHandle(cold, record["case"], record["caller_run"]).result(timeout=1) == value,
                "Independent cold result changed original admission")
    require(await history(http, record["case"], record["caller_run"]) == current, "Cold read mutated caller history")
    record["result"], record["caller_history"] = value, current


def snapshots(path):
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("event") == "caller.admission":
            values.append(value["snapshot"])
    return values


async def verify(record, http):
    case = record["case"]
    original = snapshots(DIRECTORY / f"{case}-caller-original.log")
    final = snapshots(DIRECTORY / f"{case}-caller-final.log")
    require(original and final and all(value == original[0] == record["result"] for value in original + final),
            "Workflow-facing admission changed on replay")
    states = []
    for role in ("caller", "target"):
        state = json.loads((DIRECTORY / f"{case}-{role}-exit.json").read_text(encoding="utf-8"))
        require(state["ExitCode"] == 0 and state["OOMKilled"] is False, "Final Worker shutdown failed")
        states.append({"role": role, "exit_code": 0, "oom_killed": False})
    call = record["final_call"]
    record["foreign_namespace_refusals"] = await foreign_namespace(
        http, record["namespace"], case, record["caller_run"], call["linked_workflow_instance_id"], call["linked_workflow_run_id"])
    record["worker_exits"] = states
    record["workflow_outcome"], record["outcome"] = "pass", "pass"
    print(json.dumps({key: record[key] for key in ("case", "namespace", "caller", "target", "profile",
                     "schedule", "caller_run", "call_id", "foreign_namespace_refusals", "rust_source", "outcome")}), flush=True)


async def main():
    phase, namespace, *args = sys.argv[1:]
    headers = {"Authorization": f"Bearer {TOKEN}", "X-Namespace": namespace,
               "X-Durable-Workflow-Control-Plane-Version": "2"}
    async with httpx.AsyncClient(base_url=URL, headers=headers, timeout=10) as http:
        if phase == "setup":
            await setup(http, namespace)
            return
        case, *roles = args
        require(re.fullmatch(r"[a-z0-9-]{1,80}", case) is not None, "Use a bounded fixture case name")
        async with Client(URL, token=TOKEN, namespace=namespace) as client:
            if phase == "prepare":
                await prepare(case, *roles, namespace, http, client)
                return
            record = json.loads((DIRECTORY / f"{case}.json").read_text(encoding="utf-8"))
            if phase == "verify":
                await verify(record, http)
            else:
                async with asyncio.timeout(remaining(record)):
                    if phase == "admitted":
                        await admitted(record, http)
                    elif phase == "retire":
                        await retire(record, client)
                    elif phase == "target":
                        await target_complete(record, http, client)
                    elif phase == "caller":
                        await caller_complete(record, http, client)
                    else:
                        raise ValueError("Unknown admission phase")
            save(record)


if __name__ == "__main__":
    asyncio.run(main())
