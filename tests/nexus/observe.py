"""Real HTTP controls. Durable commands are authored by the installed SDK workers."""

import asyncio
import copy
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import sys

import httpx
from durable_workflow import Client, WorkflowHandle
from durable_workflow.errors import WorkflowCancelled

URL = os.environ.get("DURABLE_WORKFLOW_RUNTIME_URL", "http://server:8080")
TOKEN = os.environ.get("DURABLE_WORKFLOW_WORKER_TOKEN", "test-token")
DIRECTORY = Path("/state/cases")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "X-Namespace": "default",
           "X-Durable-Workflow-Control-Plane-Version": "2"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def one(history, kind):
    matches = [event for event in history["events"] if event["event_type"] == kind]
    require(len(matches) == 1, f"Expected one {kind}, observed {len(matches)}")
    return matches[0]


def stamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.utcoffset() == timedelta(0), "Expected an original UTC timestamp")
    return parsed


def save(case, record):
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    (DIRECTORY / f"{case}.json").write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")


async def request(http, method, path, body=None, statuses=(200,)):
    response = await http.request(method, path, json=body)
    data = response.json()
    require(response.status_code in statuses,
            f"{method} {path}: HTTP {response.status_code}, reason={data.get('reason')}")
    return data


async def history(http, workflow_id, run_id):
    data = await request(http, "GET", f"/api/workflows/{workflow_id}/runs/{run_id}/history?page_size=1000")
    require(data.get("next_page_token") is None, "Bounded fixture history was paginated")
    return data


async def until(check):
    async with asyncio.timeout(30):
        while True:
            result = await check()
            if result is not None:
                return result
            await asyncio.sleep(0.1)


async def prepare(case, caller, target, http, client):
    require(not (DIRECTORY / f"{case}.json").exists(), "Use a fresh case identity")
    discovery = await request(http, "GET", "/api/cluster/info")
    protocol = discovery["worker_protocol"]
    require(protocol["version"] == "1.20", "Expected the declared protocol profile")
    caps = protocol["server_capabilities"]
    require(caps.get("cooperative_cancellation") is True and caps.get("nexus_cooperative_cancellation") is True,
            "Installed runtime must affirm both cooperative capabilities")
    require("workflow_run" in caps.get("nexus_cooperative_cancellation_binding_kinds", []),
            "Installed runtime lacks the workflow-run bridge")
    await request(http, "POST", "/api/namespaces", {"name": "default"}, statuses=(201, 409))
    await request(http, "POST", "/api/service-endpoints", {"endpoint_name": case}, statuses=(201,))
    await request(http, "POST", f"/api/service-endpoints/{case}/services", {"service_name": "cleanup"}, statuses=(201,))
    await request(http, "POST", f"/api/service-endpoints/{case}/services/cleanup/operations", {
        "operation_name": "start", "operation_mode": "sync", "handler_binding_kind": "start_workflow",
        "handler_target_reference": "nexus.target",
        "handler_binding": {"workflow_type": "nexus.target", "queue": f"{case}-target"},
        "deadline_policy": {"timeout_seconds": 180},
        "cancellation_policy": {"allow_cancel": True, "propagate_to_linked_workflow": True},
    }, statuses=(201,))

    async def registered():
        rows = (await request(http, "GET", "/api/workers"))["workers"]
        selected = [row for row in rows if row["worker_id"] in {f"{case}-caller", f"{case}-target"}]
        if len(selected) != 2:
            return None
        for row in selected:
            require({"cooperative_cancellation", "nexus_cooperative_cancellation"}.issubset(row["capabilities"]),
                    "Actual registration lacks a required capability")
        return selected

    registrations = await until(registered)
    handle = await client.start_workflow(workflow_type="nexus.caller", task_queue=f"{case}-caller",
                                         workflow_id=case, input=["start"])
    record = {"case": case, "caller": caller, "target": target, "caller_id": case,
              "caller_run": handle.run_id, "discovery": discovery, "registrations": registrations,
              "rust_source": os.environ["NEXUS_RUST_SOURCE"],
              "rust_binary_sha256": os.environ["NEXUS_RUST_BINARY_SHA256"]}
    save(case, record)
    print(json.dumps({"phase": "admitted", "case": case, "run_id": handle.run_id}), flush=True)


async def started(record, http):
    async def ready():
        caller = await history(http, record["caller_id"], record["caller_run"])
        events = [event for event in caller["events"] if event["event_type"] == "ServiceCallStarted"]
        if not events:
            return None
        require(len(events) == 1, "Nexus call was admitted more than once")
        call_id = events[0]["payload"]["service_call_id"]
        path = f"/api/service-endpoints/{record['case']}/services/cleanup/operations/start/service-calls/{call_id}"
        call = await request(http, "GET", path)
        target_id, target_run = call.get("linked_workflow_instance_id"), call.get("linked_workflow_run_id")
        if not target_id or not target_run:
            return None
        target = await history(http, target_id, target_run)
        if not any(event["event_type"] == "TimerScheduled" for event in target["events"]):
            return None
        return {"caller": caller, "target": target, "call": call, "call_path": path,
                "target_id": target_id, "target_run": target_run}

    record["before"] = await until(ready)


async def cancel(record, client):
    reply = await client.request_workflow_cancellation(record["caller_id"], run_id=record["caller_run"],
                                                       reason="nexus-source-control", cleanup_timeout_seconds=30)
    require(reply["duplicate"] is False, "First cancellation was not original")
    duplicate = await client.request_workflow_cancellation(record["caller_id"], run_id=record["caller_run"],
                                                           reason="nexus-source-control", cleanup_timeout_seconds=300)
    require(duplicate["duplicate"] is True and duplicate["cancellation_request"] == reply["cancellation_request"],
            "Duplicate changed the original cancellation context or budget")
    record["admission"] = reply


async def cleanup(record, http, client):
    async def committed():
        target = await history(http, record["before"]["target_id"], record["before"]["target_run"])
        delivered = [event for event in target["events"] if event["event_type"] == "CooperativeCancellationDelivered"]
        timers = [event for event in target["events"] if event["event_type"] == "TimerScheduled"
                  and event["payload"].get("delay_seconds") == 8]
        return target if delivered and timers else None
    record["committed_cleanup"] = await until(committed)
    caller = await history(http, record["caller_id"], record["caller_run"])
    forbidden = {"CooperativeCancellationDelivered", "WorkflowCompleted", "WorkflowFailed", "WorkflowCancelled"}
    require(not any(event["event_type"] in forbidden for event in caller["events"]), "Caller settled while target cleanup was pending")
    neighbor = await client.start_workflow(workflow_type="nexus.qualification.neighbor",
                                          task_queue=f"{record['case']}-caller", workflow_id=f"{record['case']}-neighbor", input=[])
    result = await neighbor.result(timeout=30, poll_interval=0.1)
    require(result == {"alive": True, "runtime": record["caller"]}, "Original cleanup prevented unrelated work or leaked cancellation")
    record["neighbor"] = await history(http, neighbor.workflow_id, neighbor.run_id)
    pending_target = await history(http, record["before"]["target_id"], record["before"]["target_run"])
    require(not any(event["event_type"] in {"WorkflowCompleted", "WorkflowFailed", "WorkflowCancelled"}
                    for event in pending_target["events"]), "Unrelated work only ran after target cleanup ended")
    record["pending_caller"] = caller


def validate(record):
    before, after = record["before"], record["after"]
    receipt = record["admission"]["cancellation_request"]
    contexts = {}
    for role, workflow_id, run_id, kind in (
        ("caller", record["caller_id"], record["caller_run"], "service_operation"),
        ("target", before["target_id"], before["target_run"], "timer"),
    ):
        original, final = before[role], after[role]
        require(final["events"][:len(original["events"])] == original["events"], "Acknowledged history prefix changed")
        start = one(final, "WorkflowStarted")["payload"]
        require(start["workflow_instance_id"] == workflow_id and start["workflow_run_id"] == run_id,
                "History selected another original workflow/run")
        requested = one(final, "CooperativeCancellationRequested")["payload"]
        delivered = one(final, "CooperativeCancellationDelivered")["payload"]
        context = requested["cancellation"]
        require(delivered["cancellation"] == context and delivered["workflow_command_id"] == context["request_id"],
                "Delivery changed canonical request context")
        require(delivered["sequence"] == 1 and delivered["call_kind"] == kind, "Wrong authored delivery boundary")
        terminal = one(final, "WorkflowCancelled")
        require(stamp(terminal["timestamp"]) <= stamp(receipt["cleanup_deadline_at"]), "Terminal outcome exceeded original cleanup deadline")
        require(context["cleanup_deadline_at"] == receipt["cleanup_deadline_at"], "Cleanup budget changed")
        require(context["root_request_id"] == receipt["request_id"] and context["root_workflow_run_id"] == record["caller_run"]
                and context["root_workflow_instance_id"] == record["caller_id"], "Root cancellation identity changed")
        contexts[role] = context
        observations = record["callback_contexts"][role]
        require(observations and all(snapshot == context for snapshot in observations), "Workflow-facing context differs from committed authority")
    root, child = contexts["caller"], contexts["target"]
    target_terminal = stamp(one(after["target"], "WorkflowCancelled")["timestamp"])
    require(stamp(one(after["caller"], "CooperativeCancellationDelivered")["timestamp"]) >= target_terminal,
            "Caller delivery preceded original target cleanup")
    require(stamp(one(record["neighbor"], "WorkflowCompleted")["timestamp"]) < target_terminal,
            "Unrelated work did not progress during target cleanup")
    require(not any(event["event_type"] in {"CooperativeCancellationDelivered", "WorkflowCompleted", "WorkflowFailed", "WorkflowCancelled"}
                    for event in record["pending_caller"]["events"]), "Pending caller had already settled")
    require(root["request_id"] == receipt["request_id"] and root["parent_request_id"] is None, "Root request was replaced")
    require(stamp(root["cleanup_deadline_at"]) - stamp(root["requested_at"]) == timedelta(seconds=30), "Original cleanup budget is not 30 seconds")
    require(child["parent_request_id"] == root["request_id"] and child["requested_at"] == root["requested_at"], "Child lost original root origin")
    require(len(root["lineage"]) == 1 and len(child["lineage"]) == 2 and child["lineage"][:1] == root["lineage"], "Ordered lineage changed")
    caller = after["caller"]
    started_call = one(caller, "ServiceCallStarted")["payload"]["service_call_id"]
    require(one(caller, "ServiceCallCancelled")["payload"]["service_call_id"] == started_call == before["call"]["id"] == after["call"]["id"], "Original service admission was replaced")
    require(after["call"]["status"] == "cancelled", "Original service call did not cancel")
    for key in ("caller_workflow_instance_id", "caller_workflow_run_id", "linked_workflow_instance_id", "linked_workflow_run_id",
                "idempotency_key", "call_deadline_at", "deadline_policy"):
        require(after["call"][key] == before["call"][key], f"Service-call {key} changed")
    require(before["call"]["deadline_policy"]["timeout_seconds"] == 180, "Wrong original service deadline policy")
    require(stamp(before["call"]["call_deadline_at"]) > stamp(root["cleanup_deadline_at"]), "Service and cleanup deadlines were conflated")


async def terminal(record, http):
    async def completed():
        caller = await history(http, record["caller_id"], record["caller_run"])
        target = await history(http, record["before"]["target_id"], record["before"]["target_run"])
        if not all(any(event["event_type"] == "WorkflowCancelled" for event in row["events"]) for row in (caller, target)):
            return None
        return {"caller": caller, "target": target, "call": await request(http, "GET", record["before"]["call_path"])}

    record["after"] = await until(completed)


def refusal_controls(record):
    mutations = [
        lambda row: row["after"]["caller"]["events"].append(copy.deepcopy(one(row["after"]["caller"], "CooperativeCancellationDelivered"))),
        lambda row: row["after"]["call"].update(id="another-service-call"),
        lambda row: row["callback_contexts"]["caller"][0].update(request_id="another-request"),
        lambda row: row["after"]["caller"]["events"][0]["payload"].update(corrupted=True),
        lambda row: row["after"]["call"].update(call_deadline_at="2099-01-01T00:00:00.000000Z"),
        lambda row: one(row["after"]["target"], "WorkflowCancelled").update(timestamp="2099-01-01T00:00:00.000000Z"),
        lambda row: one(row["after"]["caller"], "CooperativeCancellationDelivered").update(timestamp="2000-01-01T00:00:00.000000Z"),
        lambda row: one(row["neighbor"], "WorkflowCompleted").update(timestamp="2099-01-01T00:00:00.000000Z"),
        lambda row: row["pending_caller"]["events"].append(copy.deepcopy(one(row["after"]["caller"], "WorkflowCancelled"))),
    ]
    for mutate in mutations:
        changed = copy.deepcopy(record)
        mutate(changed)
        try:
            validate(changed)
        except AssertionError:
            continue
        raise AssertionError("Actual-record corruption was incorrectly accepted")
    return len(mutations)


async def verify(record, client):
    require("after" in record, "Wait for terminal histories before collecting worker output")
    record["callback_contexts"] = {}
    for role in ("caller", "target"):
        values = []
        for path in DIRECTORY.glob(f"{record['case']}-{role}*.log"):
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict) and value.get("event") == f"{role}.cleanup":
                    values.append(value["context"])
        record["callback_contexts"][role] = values
    validate(record)
    for workflow_id, run_id in ((record["caller_id"], record["caller_run"]),
                               (record["before"]["target_id"], record["before"]["target_run"])):
        try:
            await WorkflowHandle(client, workflow_id, run_id).result(timeout=1)
        except WorkflowCancelled:
            continue
        raise AssertionError("Cold SDK result did not return typed workflow cancellation")
    record["reader_refusals"] = refusal_controls(record)
    record["workflow_outcome"] = "pass"
    print(json.dumps({"phase": "verified", "case": record["case"], "caller": record["caller"], "target": record["target"],
                      "request_id": record["admission"]["cancellation_request"]["request_id"],
                      "caller_events": len(record["after"]["caller"]["events"]),
                      "target_events": len(record["after"]["target"]["events"])}), flush=True)


def finished(record):
    require(record.get("workflow_outcome") == "pass", "Workflow component is incomplete")
    exits = []
    for role in ("caller", "target"):
        paths = list(DIRECTORY.glob(f"{record['case']}-{role}*-exit.json"))
        require(len(paths) == 1, "Expected one final worker exit per role")
        state = json.loads(paths[0].read_text(encoding="utf-8"))
        require(state["ExitCode"] == 0 and state["OOMKilled"] is False, "Worker shutdown was not successful")
        exits.append({"role": role, "exit_code": state["ExitCode"], "oom_killed": state["OOMKilled"]})
    record["worker_exits"] = exits
    record["outcome"] = "pass"
    print(json.dumps({"phase": "finished", "case": record["case"], "outcome": "pass", "reader_refusals": record["reader_refusals"]}), flush=True)


async def main():
    phase, case, *roles = sys.argv[1:]
    require(re.fullmatch(r"[a-z0-9-]{1,80}", case) is not None, "Use a bounded fixture case name")
    async with httpx.AsyncClient(base_url=URL, headers=HEADERS, timeout=10) as http, Client(URL, token=TOKEN) as client:
        if phase == "prepare":
            await prepare(case, *roles, http, client)
            return
        record = json.loads((DIRECTORY / f"{case}.json").read_text(encoding="utf-8"))
        if phase == "started":
            await started(record, http)
        elif phase == "cancel":
            await cancel(record, client)
        elif phase == "cleanup":
            await cleanup(record, http, client)
        elif phase == "terminal":
            await terminal(record, http)
        elif phase == "verify":
            await verify(record, client)
        elif phase == "finished":
            finished(record)
        else:
            raise ValueError("Unknown observation phase")
        save(case, record)


if __name__ == "__main__":
    asyncio.run(main())
