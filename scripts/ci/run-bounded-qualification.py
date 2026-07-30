#!/usr/bin/env python3
"""Run the bounded structural source-qualification contract."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = Path(__file__).with_name("bounded-qualification.json")
TIMING_SCHEMA = "durable-workflow.sdk-rust.bounded-qualification-timing/v1"


class ContractError(ValueError):
    """The bounded qualification contract is malformed."""


def load_contract() -> dict[str, Any]:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise ContractError("bounded qualification contract must be an object")
    expected_fields = {
        "schema",
        "budget_seconds",
        "job_timeout_minutes",
        "checks",
        "complete_checks",
    }
    if set(contract) != expected_fields:
        raise ContractError("unexpected bounded qualification contract fields")
    if (
        contract.get("schema")
        != "durable-workflow.sdk-rust.bounded-qualification/v1"
    ):
        raise ContractError("unexpected bounded qualification contract schema")

    budget = contract.get("budget_seconds")
    if not isinstance(budget, int) or isinstance(budget, bool) or budget != 150:
        raise ContractError("budget_seconds must remain 150 seconds")
    if contract.get("job_timeout_minutes") != 3:
        raise ContractError("job_timeout_minutes must remain three minutes")

    checks = contract.get("checks")
    if not isinstance(checks, list) or not checks:
        raise ContractError("checks must be a non-empty list")

    seen_ids: set[str] = set()
    for check in checks:
        if not isinstance(check, dict):
            raise ContractError("every check must be an object")
        check_id = check.get("id")
        command = check.get("command")
        if not isinstance(check_id, str) or not check_id or check_id in seen_ids:
            raise ContractError("every check id must be a unique non-empty string")
        if (
            not isinstance(command, list)
            or not command
            or any(
                not isinstance(argument, str) or not argument for argument in command
            )
        ):
            raise ContractError(f"{check_id} command must be a non-empty string array")
        if set(check) - {"id", "command", "quiet_stdout"}:
            raise ContractError(f"{check_id} contains unsupported contract fields")
        if "quiet_stdout" in check and not isinstance(check["quiet_stdout"], bool):
            raise ContractError(f"{check_id} quiet_stdout must be boolean")
        seen_ids.add(check_id)

    complete = contract.get("complete_checks")
    if (
        not isinstance(complete, list)
        or not complete
        or len(complete) != len(set(complete))
        or any(not isinstance(check, str) or not check for check in complete)
    ):
        raise ContractError("complete_checks must be unique strings")

    return contract


def candidate_range() -> str:
    event = os.environ.get("CANDIDATE_EVENT_NAME", "")
    base = os.environ.get("CANDIDATE_BASE_SHA", "")
    before = os.environ.get("CANDIDATE_BEFORE_SHA", "")
    head = os.environ.get("CANDIDATE_HEAD_SHA", "")
    current = os.environ.get("CANDIDATE_CURRENT_SHA", "HEAD")

    if event == "pull_request" and base and head:
        return f"{base}..{head}"
    if event == "push" and before and not set(before) <= {"0"}:
        return f"{before}..{current}"
    if (
        subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD^"],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    ):
        return "HEAD^..HEAD"
    return "HEAD"


def write_evidence(path: Path, evidence: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    path.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


def main() -> int:
    try:
        contract = load_contract()
    except (ContractError, json.JSONDecodeError, OSError) as error:
        print(f"invalid bounded qualification contract: {error}", file=sys.stderr)
        return 1

    budget = contract["budget_seconds"]
    selected_range = candidate_range()
    output = Path(
        os.environ.get(
            "BOUNDED_QUALIFICATION_TIMING",
            ROOT / "target/ci/bounded-qualification-timing.json",
        )
    )
    evidence: dict[str, Any] = {
        "schema": TIMING_SCHEMA,
        "budget_seconds": budget,
        "candidate_range": selected_range,
        "checks": [],
        "outcome": "running",
    }
    started = time.monotonic()
    environment = os.environ.copy()
    environment["PUBLIC_BOUNDARY_GIT_RANGE"] = selected_range

    for check in contract["checks"]:
        elapsed = time.monotonic() - started
        remaining = budget - elapsed
        if remaining <= 0:
            evidence["outcome"] = "budget-exceeded"
            break

        command = [
            selected_range if argument == "{candidate_range}" else argument
            for argument in check["command"]
        ]
        check_started = time.monotonic()
        try:
            result = subprocess.run(
                command,
                cwd=ROOT,
                env=environment,
                stdout=subprocess.DEVNULL if check.get("quiet_stdout") else None,
                check=False,
                timeout=remaining,
            )
            outcome = "pass" if result.returncode == 0 else "fail"
            return_code: int | None = result.returncode
        except subprocess.TimeoutExpired:
            outcome = "budget-exceeded"
            return_code = None

        evidence["checks"].append(
            {
                "id": check["id"],
                "elapsed_seconds": round(time.monotonic() - check_started, 3),
                "outcome": outcome,
                "return_code": return_code,
            }
        )
        if outcome != "pass":
            evidence["outcome"] = outcome
            break
    else:
        evidence["outcome"] = "pass"

    evidence["elapsed_seconds"] = round(time.monotonic() - started, 3)
    if evidence["elapsed_seconds"] >= budget:
        evidence["outcome"] = "budget-exceeded"
    write_evidence(output, evidence)
    return 0 if evidence["outcome"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
