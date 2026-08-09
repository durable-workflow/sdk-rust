#!/usr/bin/env python3
"""Retire unsupported crate releases and audit crates.io discovery."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import tomllib
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PLAN = ROOT / "scripts/ci/crates-io-retirement-plan.json"
DEFAULT_MANIFEST = ROOT / "Cargo.toml"
REGISTRY_BASE = "https://crates.io/api/v1/crates"
PLAN_SCHEMA = "durable-workflow.crates-io-retirement-plan/v1"
EVIDENCE_SCHEMA = "durable-workflow.crates-io-maintenance-evidence/v1"
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


class MaintenanceError(Exception):
    """A safe, public diagnostic for a registry maintenance failure."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


@dataclass(frozen=True)
class RegistryResponse:
    status: int
    payload: dict[str, Any] | None


class RegistryClient:
    def __init__(self, package: str, current_version: str) -> None:
        self.package = package
        self.user_agent = (
            f"durable-workflow-sdk-rust/{current_version} "
            "(support@durable-workflow.com)"
        )

    def get(self, suffix: str = "") -> RegistryResponse:
        url = f"{REGISTRY_BASE}/{quote(self.package, safe='')}"
        if suffix:
            url = f"{url}/{quote(suffix, safe='')}"
        request = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": self.user_agent},
        )
        delay = 2
        for attempt in range(1, 6):
            try:
                with urlopen(request, timeout=30) as response:
                    return RegistryResponse(
                        response.status,
                        json.loads(response.read().decode("utf-8")),
                    )
            except HTTPError as error:
                if error.code == 404:
                    return RegistryResponse(404, None)
                if error.code != 429 and error.code < 500:
                    raise MaintenanceError(
                        "registry_http_error",
                        f"crates.io returned HTTP {error.code}",
                    ) from error
                last_error = f"HTTP {error.code}"
            except (URLError, TimeoutError, json.JSONDecodeError) as error:
                last_error = type(error).__name__
            if attempt == 5:
                raise MaintenanceError(
                    "registry_lookup_exhausted",
                    f"crates.io lookup failed after {attempt} attempts ({last_error})",
                )
            time.sleep(delay)
            delay *= 2
        raise AssertionError("registry retry loop did not terminate")


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_semver(version: str) -> tuple[int, int, int, str | None]:
    match = SEMVER.fullmatch(version)
    if match is None:
        raise MaintenanceError("invalid_semver", f"invalid crate version: {version}")
    return (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
        match.group(4),
    )


def is_pre_2_final(version: str) -> bool:
    major, _minor, _patch, prerelease = parse_semver(version)
    return major < 2 and prerelease is None


def is_2_0_release_candidate(version: str) -> bool:
    major, minor, patch, prerelease = parse_semver(version)
    return (
        (major, minor, patch) == (2, 0, 0)
        and prerelease is not None
        and prerelease.startswith("rc.")
    )


def load_authority(
    plan_path: Path, manifest_path: Path
) -> tuple[dict[str, Any], str, str]:
    try:
        plan_bytes = plan_path.read_bytes()
        plan = json.loads(plan_bytes)
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        raise MaintenanceError("authority_unreadable", str(error)) from error

    if plan.get("schema") != PLAN_SCHEMA:
        raise MaintenanceError("invalid_plan", "unsupported retirement plan schema")
    package = plan.get("package")
    versions = plan.get("retired_versions")
    if not isinstance(package, str) or not package:
        raise MaintenanceError("invalid_plan", "retirement plan package is missing")
    if (
        not isinstance(versions, list)
        or not versions
        or not all(isinstance(version, str) for version in versions)
    ):
        raise MaintenanceError(
            "invalid_plan", "retired_versions must be non-empty strings"
        )
    if len(versions) != len(set(versions)):
        raise MaintenanceError("invalid_plan", "retired_versions contains duplicates")
    if not all(is_pre_2_final(version) for version in versions):
        raise MaintenanceError(
            "unsafe_retirement_target",
            "every retirement target must be a final version below 2.0.0",
        )

    package_metadata = manifest.get("package", {})
    current_package = package_metadata.get("name")
    current_version = package_metadata.get("version")
    if current_package != package:
        raise MaintenanceError(
            "authority_mismatch", "manifest and retirement plan name different packages"
        )
    if not isinstance(current_version, str) or not is_2_0_release_candidate(
        current_version
    ):
        raise MaintenanceError(
            "stable_release_not_authorized",
            "registry maintenance requires the manifest to name an exact 2.0 release candidate",
        )
    return plan, current_version, hashlib.sha256(plan_bytes).hexdigest()


def public_version(version: dict[str, Any]) -> dict[str, Any]:
    return {
        "checksum": version.get("checksum"),
        "created_at": version.get("created_at"),
        "download_path": version.get("dl_path"),
        "num": version.get("num"),
        "updated_at": version.get("updated_at"),
        "yanked": version.get("yanked"),
    }


def exact_version(client: RegistryClient, version: str) -> dict[str, Any]:
    response = client.get(version)
    if response.status == 404:
        return {
            "http_status": 404,
            "state": "missing_artifact",
            "version": version,
        }
    payload = response.payload
    if not isinstance(payload, dict):
        raise MaintenanceError(
            "malformed_registry_response",
            f"crates.io returned malformed metadata for {version}",
        )
    item = payload.get("version")
    if (
        response.status != 200
        or not isinstance(item, dict)
        or item.get("num") != version
        or not isinstance(item.get("yanked"), bool)
    ):
        raise MaintenanceError(
            "malformed_registry_response",
            f"crates.io returned malformed metadata for {version}",
        )
    return {
        "http_status": response.status,
        "registry_response": public_version(item),
        "state": "yanked" if item.get("yanked") is True else "active",
        "version": version,
    }


def observe_registry(
    client: RegistryClient, plan: dict[str, Any], current_version: str
) -> dict[str, Any]:
    response = client.get()
    payload = response.payload
    if not isinstance(payload, dict):
        raise MaintenanceError(
            "malformed_registry_response",
            "crates.io returned malformed crate-root metadata",
        )
    crate = payload.get("crate")
    versions = payload.get("versions")
    if (
        response.status != 200
        or not isinstance(crate, dict)
        or not isinstance(versions, list)
    ):
        raise MaintenanceError(
            "malformed_registry_response",
            "crates.io returned malformed crate-root metadata",
        )
    if not all(
        isinstance(item, dict)
        and isinstance(item.get("num"), str)
        and isinstance(item.get("yanked"), bool)
        for item in versions
    ):
        raise MaintenanceError(
            "malformed_registry_response", "crate-root version metadata is malformed"
        )

    by_version = {item["num"]: item for item in versions}
    if len(by_version) != len(versions):
        raise MaintenanceError(
            "malformed_registry_response", "crate-root metadata repeats a version"
        )
    historical: list[dict[str, Any]] = []
    for version in plan["retired_versions"]:
        item = by_version.get(version)
        if item is None:
            historical.append(exact_version(client, version))
        else:
            historical.append(
                {
                    "http_status": response.status,
                    "registry_response": public_version(item),
                    "state": "yanked" if item.get("yanked") is True else "active",
                    "version": version,
                }
            )

    discovered_pre_2_finals = sorted(
        (version for version in by_version if is_pre_2_final(version)),
        key=lambda version: parse_semver(version)[:3],
    )
    release_candidates = [
        {
            "version": version,
            "yanked": by_version[version].get("yanked"),
        }
        for version in sorted(
            (version for version in by_version if is_2_0_release_candidate(version)),
            key=lambda version: parse_semver(version)[:3]
            + (parse_semver(version)[3] or "",),
        )
    ]
    stable_2_versions = sorted(
        version
        for version in by_version
        if parse_semver(version)[0] >= 2 and parse_semver(version)[3] is None
    )

    return {
        "crate_root": {
            "default_version": crate.get("default_version"),
            "http_status": response.status,
            "newest_version": crate.get("newest_version"),
        },
        "current_exact_version": exact_version(client, current_version),
        "discovered_pre_2_final_versions": discovered_pre_2_finals,
        "historical_versions": historical,
        "release_candidates": release_candidates,
        "stable_2_versions": stable_2_versions,
    }


def registry_violations(
    observation: dict[str, Any],
    plan: dict[str, Any],
    current_version: str,
    *,
    require_retired: bool,
) -> list[str]:
    violations: list[str] = []
    planned = plan["retired_versions"]
    if set(observation["discovered_pre_2_final_versions"]) != set(planned):
        violations.append("pre_2_final_inventory_differs_from_reviewed_plan")
    for item in observation["historical_versions"]:
        if item["state"] == "missing_artifact":
            violations.append(f"historical_artifact_missing:{item['version']}")
        elif not item.get("registry_response", {}).get("checksum"):
            violations.append(f"historical_checksum_missing:{item['version']}")
        elif not item.get("registry_response", {}).get("download_path"):
            violations.append(f"historical_download_missing:{item['version']}")
        elif require_retired and item["state"] != "yanked":
            violations.append(f"historical_version_not_yanked:{item['version']}")

    exact = observation["current_exact_version"]
    if exact["state"] != "active":
        violations.append(f"current_release_candidate_not_active:{current_version}")
    elif not exact.get("registry_response", {}).get("checksum"):
        violations.append(
            f"current_release_candidate_checksum_missing:{current_version}"
        )
    root = observation["crate_root"]
    if root["newest_version"] != current_version:
        violations.append("crate_root_newest_version_is_not_current_release_candidate")
    if require_retired and root["default_version"] != current_version:
        violations.append("crate_root_default_version_is_not_current_release_candidate")
    for candidate in observation["release_candidates"]:
        if candidate["yanked"] is True:
            violations.append(f"release_candidate_yanked:{candidate['version']}")
    if observation["stable_2_versions"]:
        violations.append("stable_2_release_exists_without_authorization")
    return violations


def base_evidence(
    operation: str,
    plan: dict[str, Any],
    current_version: str,
    plan_sha256: str,
) -> dict[str, Any]:
    return {
        "credential_handling": {
            "environment_variable": "CARGO_REGISTRY_TOKEN",
            "value_recorded": False,
        },
        "current_release_candidate": current_version,
        "generated_at": utc_now(),
        "operation": operation,
        "outcome": "pending",
        "package": plan["package"],
        "plan_sha256": plan_sha256,
        "retirement_plan": list(plan["retired_versions"]),
        "schema": EVIDENCE_SCHEMA,
        "version": 1,
    }


def write_evidence(path: Path, evidence: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def cargo_yank(package: str, version: str) -> int:
    result = subprocess.run(
        [
            "cargo",
            "yank",
            "--quiet",
            "--registry",
            "crates-io",
            "--version",
            version,
            package,
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode


def poll_registry(
    client: RegistryClient,
    plan: dict[str, Any],
    current_version: str,
    attempts: int,
    delay: float,
) -> tuple[dict[str, Any], list[str]]:
    observation: dict[str, Any] | None = None
    violations: list[str] = []
    for attempt in range(1, attempts + 1):
        observation = observe_registry(client, plan, current_version)
        violations = registry_violations(
            observation, plan, current_version, require_retired=True
        )
        if not violations:
            return observation, []
        if attempt < attempts:
            time.sleep(delay)
    assert observation is not None
    return observation, violations


def retire(
    plan: dict[str, Any],
    current_version: str,
    plan_sha256: str,
    evidence_path: Path,
    attempts: int,
    delay: float,
    client: RegistryClient | None = None,
) -> int:
    evidence = base_evidence("retire", plan, current_version, plan_sha256)
    write_evidence(evidence_path, evidence)
    client = client or RegistryClient(plan["package"], current_version)
    try:
        if not os.environ.get("CARGO_REGISTRY_TOKEN"):
            raise MaintenanceError(
                "registry_token_missing",
                "CARGO_REGISTRY_TOKEN is required for protected registry maintenance",
            )
        before = observe_registry(client, plan, current_version)
        evidence["registry_before"] = before
        violations = registry_violations(
            before, plan, current_version, require_retired=False
        )
        if violations:
            evidence["violations"] = violations
            raise MaintenanceError(
                "registry_preflight_failed",
                "registry state does not match the reviewed plan",
            )

        actions: list[dict[str, Any]] = []
        evidence["retirement_actions"] = actions
        for item in before["historical_versions"]:
            version = item["version"]
            if item["state"] == "yanked":
                actions.append({"action": "already_yanked", "version": version})
                continue
            action = {"action": "yank_requested", "version": version}
            actions.append(action)
            action["cargo_exit_code"] = cargo_yank(plan["package"], version)
            if action["cargo_exit_code"] != 0:
                action["action"] = "yank_failed"
                raise MaintenanceError(
                    "cargo_yank_failed", f"cargo yank failed for {version}"
                )
            action["action"] = "yank_accepted"

        after, violations = poll_registry(
            client, plan, current_version, attempts, delay
        )
        evidence["registry_after"] = after
        if violations:
            evidence["violations"] = violations
            raise MaintenanceError(
                "post_retirement_audit_failed",
                "crates.io did not expose the required retirement state",
            )
        evidence["outcome"] = "passed"
        evidence["reason"] = "unsupported_pre_2_final_versions_retired"
        print(
            f"Retired {len(plan['retired_versions'])} historical versions; "
            f"crates.io defaults to {current_version}."
        )
        return 0
    except MaintenanceError as error:
        evidence["outcome"] = "failed"
        evidence["reason"] = error.code
        print(f"crates.io maintenance failed: {error}", file=sys.stderr)
        return 1
    finally:
        evidence["generated_at"] = utc_now()
        write_evidence(evidence_path, evidence)


def verify_exact_install(
    package: str, current_version: str, expected_checksum: str
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix="durable-workflow-crates-io-audit-"
    ) as temporary:
        root = Path(temporary)
        (root / "src").mkdir()
        (root / "src/lib.rs").write_text("", encoding="utf-8")
        (root / "Cargo.toml").write_text(
            "[package]\n"
            'name = "durable-workflow-registry-audit"\n'
            'version = "0.0.0"\n'
            'edition = "2021"\n\n'
            "[dependencies]\n"
            f'{package} = "={current_version}"\n',
            encoding="utf-8",
        )
        environment = os.environ.copy()
        environment.pop("CARGO_REGISTRY_TOKEN", None)
        environment["CARGO_HOME"] = str(root / "cargo-home")
        environment["CARGO_TERM_COLOR"] = "never"
        result = subprocess.run(
            ["cargo", "fetch", "--manifest-path", str(root / "Cargo.toml")],
            check=False,
            cwd=root,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode != 0:
            raise MaintenanceError(
                "exact_requirement_not_installable",
                f'Cargo could not fetch {package} ="={current_version}"',
            )
        try:
            lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise MaintenanceError(
                "cargo_lock_unreadable", "Cargo did not emit a readable lockfile"
            ) from error
        matches = [
            item
            for item in lock.get("package", [])
            if item.get("name") == package and item.get("version") == current_version
        ]
        if len(matches) != 1:
            raise MaintenanceError(
                "exact_requirement_resolved_incorrectly",
                "Cargo did not resolve the exact current release candidate",
            )
        resolved = matches[0]
        if resolved.get("checksum") != expected_checksum:
            raise MaintenanceError(
                "registry_checksum_mismatch",
                "Cargo.lock and crates.io report different checksums for the current release candidate",
            )
        return {
            "credential_available": False,
            "lock_checksum": resolved.get("checksum"),
            "requirement": f"={current_version}",
            "resolved_version": resolved.get("version"),
            "state": "installable",
        }


def audit(
    plan: dict[str, Any],
    current_version: str,
    plan_sha256: str,
    evidence_path: Path,
    attempts: int,
    delay: float,
    client: RegistryClient | None = None,
) -> int:
    evidence = base_evidence("audit", plan, current_version, plan_sha256)
    write_evidence(evidence_path, evidence)
    client = client or RegistryClient(plan["package"], current_version)
    try:
        observation, violations = poll_registry(
            client, plan, current_version, attempts, delay
        )
        evidence["registry"] = observation
        if violations:
            evidence["violations"] = violations
            raise MaintenanceError(
                "live_registry_audit_failed",
                "crates.io discovery does not match the supported prerelease policy",
            )
        checksum = observation["current_exact_version"]["registry_response"]["checksum"]
        evidence["exact_installation"] = verify_exact_install(
            plan["package"], current_version, checksum
        )
        evidence["outcome"] = "passed"
        evidence["reason"] = "registry_discovery_and_exact_install_verified"
        print(
            f"crates.io defaults to {current_version}; exact Cargo resolution passed "
            f"and {len(plan['retired_versions'])} historical versions remain yanked."
        )
        return 0
    except MaintenanceError as error:
        evidence["outcome"] = "failed"
        evidence["reason"] = error.code
        print(f"crates.io audit failed: {error}", file=sys.stderr)
        return 1
    finally:
        evidence["generated_at"] = utc_now()
        write_evidence(evidence_path, evidence)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("audit", "retire"))
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--delay", type=float, default=5)
    args = parser.parse_args()
    if args.attempts < 1 or args.delay < 0:
        parser.error("--attempts must be positive and --delay must be non-negative")
    try:
        plan, current_version, plan_sha256 = load_authority(args.plan, args.manifest)
    except MaintenanceError as error:
        print(f"crates.io authority failed: {error}", file=sys.stderr)
        return 1
    if args.operation == "retire":
        return retire(
            plan,
            current_version,
            plan_sha256,
            args.evidence,
            args.attempts,
            args.delay,
        )
    return audit(
        plan,
        current_version,
        plan_sha256,
        args.evidence,
        args.attempts,
        args.delay,
    )


if __name__ == "__main__":
    raise SystemExit(main())
