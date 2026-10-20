"""The replacement must claim durable Nexus delivery before history can record it."""

import asyncio
import copy
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

path = Path(os.environ.get("ACK_READER_PATH", Path(__file__).with_name("acknowledgements.py")))
spec = importlib.util.spec_from_file_location("ack_reader", path)
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)


def fixture():
    call = {"id": "original-call", "status": "completed", "namespace": "target-namespace",
        "caller_namespace": "caller-namespace", "target_namespace": "target-namespace",
        "caller_workflow_instance_id": "original-caller", "caller_workflow_run_id": "caller-run",
        "linked_workflow_instance_id": "original-target", "linked_workflow_run_id": "target-run",
        "idempotency_key": "original-caller", "deadline_policy": {"timeout_seconds": 180},
        "accepted_at": "2026-01-01T00:00:00.000001Z", "call_deadline_at": "2026-01-01T00:03:00.000001Z"}
    record = {"case": "original-caller", "caller_run": "caller-run", "call_id": "original-call",
        "namespace": "caller-namespace", "target_namespace": "target-namespace", "call_path": "/original-call",
        "original_call": {**call, "status": "started"}}
    caller = {"events": [{"event_type": "ServiceCallStarted", "payload": {"service_call_id": "original-call"}}]}
    target = {"events": [
        {"event_type": "WorkflowStarted", "payload": {"workflow_instance_id": "original-target", "workflow_run_id": "target-run"}},
        {"event_type": "TimerScheduled", "payload": {"sequence": 1, "delay_seconds": 8, "timer_id": "original-timer"}},
        {"event_type": "TimerFired", "payload": {"timer_id": "original-timer"}},
        {"event_type": "WorkflowCompleted", "payload": {"output": "retained"}},
    ]}
    return record, call, caller, target


class TargetValue:
    async def result(self, **_):
        return reader.specimen("success")


class PendingDelivery(unittest.IsolatedAsyncioTestCase):
    async def observe(self, rows):
        record, call, caller, target = rows

        async def history(_http, workflow_id, _run_id):
            return copy.deepcopy(caller if workflow_id == record["case"] else target)

        with (patch.object(reader, "request", AsyncMock(return_value=call)),
              patch.object(reader, "history", history),
              patch.object(reader, "WorkflowHandle", return_value=TargetValue())):
            await asyncio.wait_for(reader.target_complete(record, object(), object(), object()), 0.25)
        return record

    async def test_completed_target_can_precede_caller_claim_and_terminal_history(self):
        record = await self.observe(fixture())
        self.assertEqual(record["final_call"]["status"], "completed")
        self.assertEqual([row["event_type"] for row in record["before_cold_history"]["events"]], ["ServiceCallStarted"])

    async def test_target_must_actually_complete(self):
        rows = fixture()
        rows[1]["status"] = "started"
        with self.assertRaises(TimeoutError):
            await self.observe(rows)

    async def test_namespace_authority_remains_required(self):
        rows = fixture()
        for call in (rows[0]["original_call"], rows[1]):
            call["namespace"] = call["target_namespace"] = "wrong-namespace"
        with self.assertRaises(AssertionError):
            await self.observe(rows)

    async def test_original_target_run_remains_required(self):
        rows = fixture()
        rows[3]["events"][0]["payload"]["workflow_run_id"] = "another-run"
        with self.assertRaises(AssertionError):
            await self.observe(rows)

    async def test_conflicting_terminal_delivery_is_refused(self):
        rows = fixture()
        rows[2]["events"].append({"event_type": "ServiceCallFailed", "payload": {"service_call_id": "original-call"}})
        with self.assertRaises(AssertionError):
            await self.observe(rows)

    async def test_caller_must_still_await_replacement(self):
        rows = fixture()
        rows[2]["events"].append({"event_type": "WorkflowCompleted", "payload": {}})
        with self.assertRaises(AssertionError):
            await self.observe(rows)

    async def test_original_admission_identity_remains_required(self):
        rows = fixture()
        rows[2]["events"][0]["payload"]["service_call_id"] = "another-call"
        with self.assertRaises(AssertionError):
            await self.observe(rows)


class CompletedClaim(unittest.IsolatedAsyncioTestCase):
    async def check(self, **changes):
        value = {"task_id": "committed-task", "workflow_task_attempt": 1,
                 "reason": "task_not_leased", "task_status": "completed", **changes}
        http = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(
            status_code=409, json=lambda: value)))
        return await reader.completed_claim(http, {
            "id": "committed-task", "attempt_count": 1, "lease_owner": "original-worker"})

    async def test_completed_physical_claim_is_required(self):
        self.assertEqual((await self.check())["task_status"], "completed")
        self.assertEqual((await self.check(reason="run_closed"))["task_status"], "completed")

    async def test_recovered_ready_or_leased_original_is_refused(self):
        for state in ("ready", "leased", None):
            with self.subTest(state=state), self.assertRaises(AssertionError):
                await self.check(task_status=state)

    async def test_another_task_attempt_or_unknown_refusal_is_refused(self):
        for change in ({"task_id": "followup"}, {"workflow_task_attempt": 2},
                       {"workflow_task_attempt": True}, {"reason": "task_not_found"}):
            with self.subTest(change=change), self.assertRaises(AssertionError):
                await self.check(**change)


class RetirementIdentity(unittest.TestCase):
    def check(self, task_id="followup", count=1, mutate=False, run_id="run"):
        before = {"events": [{"event_type": "ServiceCallStarted", "payload": {
            "task": {"id": "committed-task"}}}]}
        after = copy.deepcopy(before)
        if mutate:
            after["events"][0]["payload"]["task"]["id"] = "rewritten"
        after["events"].append({"event_type": "RepairRequested", "payload": {
            "task": {"id": task_id, "type": "workflow"}, "workflow_run_id": run_id,
            "command": {"request_path": "/original-retirement"}}})
        return reader.retirement_recoveries(before, after,
            {"recovered_workflow_task_count": count}, {"id": "committed-task"},
            "/original-retirement", "run")

    def test_recovery_of_a_distinct_followup_is_valid(self):
        self.assertEqual(self.check(), ["followup"])

    def test_recovery_of_original_committed_claim_is_refused(self):
        with self.assertRaises(AssertionError):
            self.check(task_id="committed-task")

    def test_missing_or_untyped_count_and_wrong_run_are_refused(self):
        for count in (0, 2, True, "1", -1):
            with self.subTest(count=count), self.assertRaises(AssertionError):
                self.check(count=count)
        with self.assertRaises(AssertionError):
            self.check(run_id="other-run")

    def test_original_history_cannot_change(self):
        with self.assertRaises(AssertionError):
            self.check(mutate=True)


if __name__ == "__main__":
    unittest.main()
