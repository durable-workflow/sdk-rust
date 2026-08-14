#!/usr/bin/env python3
"""Validate the required responsive rustdoc navigation evidence matrix."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


MATRIX = {
    ("analytics-ui-removed", 1440, 900): {"full_page": False, "interactions": []},
    ("analytics-ui-removed", 800, 900): {"full_page": False, "interactions": []},
    ("analytics-ui-removed", 390, 844): {"full_page": False, "interactions": []},
    ("analytics-ui-removed", 640, 360): {"full_page": True, "interactions": []},
    ("navigation-open", 390, 844): {
        "full_page": False,
        "interactions": [{"type": "click", "selector": ".sidebar-menu-toggle"}],
        "state_scope": "responsive",
    },
    ("navigation-open", 640, 360): {
        "full_page": False,
        "interactions": [{"type": "click", "selector": ".sidebar-menu-toggle"}],
        "state_scope": "responsive",
    },
}

EMPTY_REPORT_FIELDS = (
    "console_errors",
    "console_warnings",
    "page_errors",
    "request_failures",
    "http_errors",
)
EMPTY_GEOMETRY_FIELDS = (
    "clipped_text",
    "clipped_control_text",
    "unreachable_controls",
    "overlapping_floating_elements",
    "displaced_primary_content",
    "orphaned_body_controls",
)
NAVIGATION_ID = "dw-rustdoc-navigation"


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read visual evidence JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"visual evidence JSON must contain an object: {path}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()

    manifest = load_json(args.manifest)
    if manifest.get("schema") != "durable-workflow.pipeline.visual-review/v1":
        raise SystemExit("visual evidence manifest has an unsupported schema")
    captures = manifest.get("captures")
    if not isinstance(captures, list):
        raise SystemExit("visual evidence manifest is missing captures")

    indexed: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for capture in captures:
        if not isinstance(capture, dict):
            continue
        viewport = capture.get("viewport")
        if not isinstance(viewport, dict):
            continue
        key = (capture.get("state"), viewport.get("width"), viewport.get("height"))
        indexed.setdefault(key, []).append(capture)

    failures: list[str] = []
    for key, expected in MATRIX.items():
        matching = indexed.get(key, [])
        if len(matching) != 1:
            failures.append(f"required rustdoc capture {key} must occur exactly once")
            continue
        capture = matching[0]
        for field, value in expected.items():
            if capture.get(field) != value:
                failures.append(f"required rustdoc capture {key} has invalid {field}")
        if capture.get("surface") != "rust-sdk-reference":
            failures.append(f"required rustdoc capture {key} has the wrong surface")

        report_name = capture.get("report")
        screenshot_name = capture.get("screenshot")
        if not isinstance(report_name, str) or not isinstance(screenshot_name, str):
            failures.append(f"required rustdoc capture {key} is missing artifact paths")
            continue
        report_path = args.manifest.parent / report_name
        screenshot_path = args.manifest.parent / screenshot_name
        if not screenshot_path.is_file():
            failures.append(f"required rustdoc capture {key} is missing its screenshot")
        report = load_json(report_path)
        if report.get("title") != "durable_workflow - Rust":
            failures.append(
                f"required rustdoc capture {key} did not render the crate reference"
            )
        if not 200 <= report.get("page_status", 0) < 300:
            failures.append(f"required rustdoc capture {key} did not return HTTP 2xx")
        geometry = report.get("geometry")
        if not isinstance(geometry, dict):
            failures.append(f"required rustdoc capture {key} is missing geometry")
            continue
        if geometry.get("horizontal_overflow"):
            failures.append(f"required rustdoc capture {key} has horizontal overflow")
        for field in EMPTY_REPORT_FIELDS:
            if report.get(field):
                failures.append(f"required rustdoc capture {key} has non-empty {field}")
        for field in EMPTY_GEOMETRY_FIELDS:
            if geometry.get(field):
                failures.append(
                    f"required rustdoc capture {key} has non-empty geometry.{field}"
                )
        if key[0] == "navigation-open":
            overlays = geometry.get("intentional_overlays")
            matching_overlays = (
                [
                    overlay
                    for overlay in overlays
                    if isinstance(overlay, dict)
                    and overlay.get("tag") == "nav"
                    and overlay.get("id") == NAVIGATION_ID
                    and overlay.get("position") == "fixed"
                    and overlay.get("intentional_overlay") is True
                    and isinstance(overlay.get("isolated_background_count"), int)
                    and overlay["isolated_background_count"] >= 1
                    and isinstance(overlay.get("overlaps"), list)
                    and overlay["overlaps"]
                ]
                if isinstance(overlays, list)
                else []
            )
            if len(matching_overlays) != 1:
                failures.append(
                    f"required rustdoc capture {key} did not prove an isolated "
                    f"{NAVIGATION_ID} overlay"
                )

    if failures:
        raise SystemExit("\n".join(failures))
    print("Validated the required closed and open rustdoc navigation evidence matrix.")


if __name__ == "__main__":
    main()
