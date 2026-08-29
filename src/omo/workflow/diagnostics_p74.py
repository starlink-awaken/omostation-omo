#!/usr/bin/env python3
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ..omo_shared import load_yaml
from .lifecycle import append_ledger_event, claim_policy


def p74_solidification_report(
    registry: dict[str, Any],
    events: list[dict[str, Any]],
    runs: dict[str, Any],
) -> dict[str, Any]:
    import fnmatch

    silent_policy = registry.get("silent_workflow_policy") or {}

    # P74 silent detection: workflow is silent iff has_recent_run == False
    # AND has_check_coverage == False (per ADR-0130 §4.4).
    # run_frequency drives warn_after threshold (on_demand=30d, periodic=7d,
    # continuous=1d), single-sourced from SSOT
    # silent_workflow_policy.warn_after_days_by_frequency (ADR-0211 D2/D3).
    # Excluded workflow list removed in ADR-0211; rationale = no double SSOT.

    started_runs: dict[str, str] = {}
    for event in events:
        if str(event.get("event") or "") == "agent_workflow_start":
            workflow_id = str(event.get("workflow_id") or "")
            if workflow_id:
                started_runs[workflow_id] = str(event.get("ts") or "")

    covered_paths: set[str] = set()
    for check in registry.get("diff_checks") or []:
        if isinstance(check, dict):
            for path in check.get("paths") or []:
                if isinstance(path, str):
                    covered_paths.add(path)
    for check in registry.get("doctor_checks") or []:
        if isinstance(check, dict):
            for path in check.get("paths") or []:
                if isinstance(path, str):
                    covered_paths.add(path)

    doctor_commands: list[str] = []
    for check in registry.get("doctor_checks") or []:
        if isinstance(check, dict):
            command = check.get("command") or []
            if isinstance(command, list):
                doctor_commands.extend(str(item) for item in command if isinstance(item, str))

    workflows_summary: list[dict[str, Any]] = []
    for workflow in registry.get("workflows") or []:
        if not isinstance(workflow, dict):
            continue
        workflow_id = str(workflow.get("id") or "")
        surfaces = workflow.get("surfaces") or {}
        write_patterns = surfaces.get("write") if isinstance(surfaces, dict) else None
        read_patterns = surfaces.get("read") if isinstance(surfaces, dict) else None
        workflow_paths = [str(p) for p in (write_patterns or []) if isinstance(p, str)]
        if not workflow_paths:
            workflow_paths = [str(p) for p in (read_patterns or []) if isinstance(p, str)]
        has_check_coverage = (
            any(any(fnmatch.fnmatch(pattern, p) for p in covered_paths) for pattern in workflow_paths)
            if workflow_paths
            else False
        )
        if not has_check_coverage and workflow_id:
            has_check_coverage = any(workflow_id in command for command in doctor_commands)
        last_start = started_runs.get(workflow_id, "")
        run_frequency = str(workflow.get("run_frequency") or "on_demand")
        # ADR-0211 D2: run_frequency drives warn_after threshold. Single-sourced
        # from SSOT silent_workflow_policy.warn_after_days_by_frequency
        # (on_demand=30d / periodic=7d / continuous=1d). Fallback to warn_after_days.
        freq_map = silent_policy.get("warn_after_days_by_frequency") or {}
        warn_after = int(freq_map.get(run_frequency) or silent_policy.get("warn_after_days") or 30)
        has_recent_run = False
        if last_start:
            try:
                last_dt = datetime.fromisoformat(str(last_start).replace("Z", "+00:00"))
                now_dt = datetime.now(UTC)
                age_h = (now_dt - last_dt).total_seconds() / 3600
                has_recent_run = age_h <= warn_after * 24
            except ValueError:
                # unparseable ts → treat as no evidence of recent run
                has_recent_run = False
        silent_health = "active" if has_recent_run or has_check_coverage else "warn"
        workflows_summary.append(
            {
                "workflow_id": workflow_id,
                "run_frequency": run_frequency,
                "warn_after_days": warn_after,
                "has_recent_run": has_recent_run,
                "last_start_ts": last_start,
                "has_check_coverage": has_check_coverage,
                "silent_health": silent_health,
                "agents": workflow.get("agents"),
            }
        )

    warn_count = sum(1 for item in workflows_summary if item["silent_health"] == "warn")
    return {
        "ok": warn_count == 0,
        "policy": silent_policy,
        "summary_count": len(workflows_summary),
        "warn_count": warn_count,
        "workflows": workflows_summary,
    }
