"""Verify report helpers for workflow diagnostics."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from .core import (
    WORKSPACE,
    WorkflowError,
    changed_files_from_git,
    command_display,
    normalize_repo_path,
    path_matches,
    substitute,
)
from .lifecycle import append_ledger_event, read_run
from .lifecycle_report import claim_coverage_report
from .lint import diff_check_rows


def run_check_command(check: dict[str, Any], context: dict[str, str]) -> dict[str, Any]:
    command = substitute(check["command"], context)
    cwd_raw = check.get("cwd") or check.get("workdir") or "."
    cwd = WORKSPACE / substitute([cwd_raw], context)[0]
    env = os.environ.copy()
    matched_files = check.get("matched_files", [])
    if matched_files:
        env["AGENT_WORKFLOW_MATCHED_FILES"] = json.dumps(matched_files, ensure_ascii=False)
    allowed_lanes = check.get("allowed_lanes") or []
    if matched_files and allowed_lanes:
        env["AGENT_WORKFLOW_ALLOWED_LANES"] = ",".join(str(item) for item in allowed_lanes)
    started = time.monotonic()
    completed = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, check=False)
    duration_s = round(time.monotonic() - started, 3)
    stdout = completed.stdout[-4000:] if completed.stdout else ""
    stderr = completed.stderr[-4000:] if completed.stderr else ""
    return {
        "id": check["id"],
        "description": check.get("description", ""),
        "required": bool(check.get("required", True)),
        "command": command_display(command),
        "cwd": str(cwd.relative_to(WORKSPACE)) if cwd.is_relative_to(WORKSPACE) else str(cwd),
        "returncode": completed.returncode,
        "duration_s": duration_s,
        "ok": completed.returncode == 0 or not check.get("required", True),
        "stdout_tail": stdout,
        "stderr_tail": stderr,
        "matched_files": matched_files,
        "allowed_lanes": allowed_lanes,
    }


def subprocess_run(command: list[str], cwd: Path, env: dict[str, str]) -> Any:
    return subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, check=False)


def select_diff_checks(
    registry: dict[str, Any],
    files: list[str],
    all_checks: bool,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for check in diff_check_rows(registry):
        patterns = check["paths"]
        matched_files = sorted(file for file in files if path_matches(patterns, file))
        if all_checks or check["always"] or matched_files:
            selected.append({**check, "matched_files": matched_files})
    return selected


def build_verify_report(
    registry: dict[str, Any],
    run_id: str | None,
    files: list[str],
    from_diff: bool,
    include_untracked: bool,
    all_checks: bool,
    execute: bool,
) -> dict[str, Any]:
    if from_diff:
        files = [*files, *changed_files_from_git(include_untracked)]
    normalized_files = sorted({normalize_repo_path(item) for item in files})
    if not from_diff and not normalized_files and not all_checks:
        raise WorkflowError("verify requires --from-diff, --file, or --all")
    context: dict[str, str] = {"run_id": run_id or ""}
    if run_id:
        _, run_payload = read_run(registry, run_id)
        context.update({str(key): str(value) for key, value in (run_payload.get("context") or {}).items()})
    checks = select_diff_checks(registry, normalized_files, all_checks)
    results: list[dict[str, Any]] = []
    for check in checks:
        if execute:
            result = run_check_command(check, context)
        else:
            result = {
                "id": check["id"],
                "description": check.get("description", ""),
                "required": bool(check.get("required", True)),
                "command": command_display(substitute(check["command"], context)),
                "cwd": check.get("cwd", "."),
                "matched_files": check.get("matched_files", []),
                "skipped": True,
                "ok": True,
            }
        results.append(result)
    claim_coverage = claim_coverage_report(registry, run_id, normalized_files)
    ok = all(result.get("ok", False) for result in results) and bool(claim_coverage["ok"])
    report = {
        "ok": ok,
        "run_id": run_id,
        "from_diff": from_diff,
        "include_untracked": include_untracked,
        "execute": execute,
        "changed_files": normalized_files,
        "claim_coverage": claim_coverage,
        "check_count": len(results),
        "checks": results,
    }
    if run_id:
        append_ledger_event(
            registry,
            {
                "event": "agent_workflow_verify",
                "run_id": run_id,
                "ok": ok,
                "execute": execute,
                "from_diff": from_diff,
                "changed_files": normalized_files,
                "claim_coverage": {
                    "mode": claim_coverage.get("mode"),
                    "ok": claim_coverage.get("ok"),
                    "missing_files": claim_coverage.get("missing_files"),
                },
                "checks": [
                    {
                        "id": item.get("id"),
                        "ok": item.get("ok"),
                        "required": item.get("required"),
                        "returncode": item.get("returncode"),
                        "duration_s": item.get("duration_s"),
                    }
                    for item in results
                ],
            },
        )
    return report


def print_verify_report(report: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    mode = "executed" if report["execute"] else "planned"
    print(f"agent-workflow verify: {'ok' if report['ok'] else 'failed'} ({mode})")
    print(f"files={len(report['changed_files'])} checks={report['check_count']}")
    claim_coverage = report.get("claim_coverage")
    if isinstance(claim_coverage, dict):
        for warning in claim_coverage.get("warnings") or []:
            print(f"[WARN] claim_policy: {warning}")
    for result in report["checks"]:
        status = (
            "PASS" if result.get("ok") and not result.get("skipped") else "SKIP" if result.get("skipped") else "FAIL"
        )
        print(f"[{status}] {result['id']} :: {result['command']}")
