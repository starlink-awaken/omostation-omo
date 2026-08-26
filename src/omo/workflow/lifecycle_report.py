from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

from ..omo_io import write_yaml_atomic
from .core import WORKSPACE, path_matches
from .lifecycle import read_run
from .lifecycle_claims import (
    claim_covers_path,
    claim_policy,
    claimed_paths,
    is_read_only_workflow,
)


def claim_coverage_report(
    registry: dict[str, Any],
    run_id: str | None,
    changed_files: list[str],
) -> dict[str, Any]:
    policy = claim_policy(registry)
    mode = str(policy["mode"])
    if mode == "off" or not run_id:
        return {
            "ok": True,
            "mode": mode,
            "checked": False,
            "run_id": run_id,
            "required_paths": policy["required_paths"],
            "tiers": policy["tiers"],
            "claimed_paths": [],
            "missing_files": [],
            "missing_required_files": [],
            "missing_advisory_files": [],
            "warnings": [],
        }
    _, payload = read_run(registry, run_id)
    if is_read_only_workflow(registry, str(payload.get("workflow_id") or "")):
        return {
            "ok": True,
            "mode": "read_only_exempt",
            "checked": False,
            "read_only": True,
            "run_id": run_id,
            "required_paths": policy["required_paths"],
            "tiers": policy["tiers"],
            "claimed_paths": claimed_paths(payload),
            "missing_files": [],
            "missing_required_files": [],
            "missing_advisory_files": [],
            "warnings": ["claim_policy skipped: workflow has empty write surfaces (read-only)"],
        }
    claimed = claimed_paths(payload)
    tiers = policy["tiers"] or [{"id": "default", "mode": mode, "paths": policy["required_paths"]}]
    missing_required: list[str] = []
    missing_advisory: list[str] = []
    for item in changed_files:
        matching_tiers = [tier for tier in tiers if not tier.get("paths") or path_matches(tier.get("paths", []), item)]
        if not matching_tiers:
            continue
        if any(claim_covers_path(claimed_path, item) for claimed_path in claimed):
            continue
        if any(tier.get("mode") == "required" for tier in matching_tiers):
            missing_required.append(item)
        else:
            missing_advisory.append(item)
    missing = sorted({*missing_required, *missing_advisory})
    ok = not missing_required
    warnings = [f"unclaimed required file under claim_policy: {item}" for item in missing_required] + [
        f"unclaimed advisory file under claim_policy: {item}" for item in missing_advisory
    ]
    return {
        "ok": ok,
        "mode": mode,
        "checked": True,
        "run_id": run_id,
        "required_paths": policy["required_paths"],
        "tiers": tiers,
        "claimed_paths": claimed,
        "missing_files": missing,
        "missing_required_files": sorted(missing_required),
        "missing_advisory_files": sorted(missing_advisory),
        "warnings": warnings,
    }


def staged_lane_report() -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, "bin/change-lane-check.py", "--staged", "--json"],
        cwd=WORKSPACE,
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        payload = {}
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "lanes": payload.get("lanes", []),
        "files": payload.get("files", []),
        "message": payload.get("message") or completed.stderr.strip(),
    }


def recommended_next(status: dict[str, Any]) -> str:
    if status["stale_locks"] > 0:
        return "Run `agent-workflow observe` and inspect stale locks before editing."
    claim_coverage = status.get("claim_coverage")
    if isinstance(claim_coverage, dict) and claim_coverage.get("missing_files"):
        run_id = status.get("current_run_id") or "<run-id>"
        return f"Claim missing files with `agent-workflow claim {run_id} --path <path>`."
    if status["active_runs"]:
        run_id = status["active_runs"][0]
        return f"Continue with `agent-workflow verify {run_id} --from-diff --execute` or closeout."
    if not status["staged_lane"]["ok"]:
        return "Resolve the staged lane split or use a run-scoped/file-scoped gate for AGCP work."
    return "Start a governed run with `agent-workflow start <workflow-id> --profile <agent-profile>`."
