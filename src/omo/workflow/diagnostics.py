from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .core import (
    REGISTRY_PATH,
    WORKSPACE,
    WorkflowError,
    adapter_rows,
    changed_files_from_git,
    command_display,
    display_path,
    integration_rows,
    ledger_path,
    lock_state_dir,
    normalize_repo_path,
    path_matches,
    registry_workspace_root,
    substitute,
)
from .diagnostics_verify import (
    build_verify_report,
    print_verify_report,
    run_check_command,
    select_diff_checks,
    subprocess_run,
)

# 2026-08-29: p74 solidification report extracted to diagnostics_p74.py
from .diagnostics_p74 import p74_solidification_report
from .lifecycle import (
    append_ledger_event,
    claim_policy,
    heal_ledger_for_run,
    ledger_mentions_run,
    load_lock_records,
    load_run_records,
    prune_stale_locks,
    read_run,
    scan_locks,
)
from .lifecycle_report import (
    claim_coverage_report,
    recommended_next,
    staged_lane_report,
)
from .lint import agcp_drift_check, diff_check_rows


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


# Active run 超过该时长无更新视为 stale (僵尸 run, 锁可能已被 TTL 清理)。
STALE_RUN_HOURS = 6


def _is_stale_run(payload: dict[str, Any]) -> bool:
    """判断 active run 是否 stale (超过 STALE_RUN_HOURS 无 updated_at)."""
    updated = payload.get("updated_at") or payload.get("created_at") or ""
    if not updated:
        return False
    ts = parse_utc_timestamp(str(updated).replace("Z", "+00:00"))
    if ts is None:
        return False
    age_hours = (datetime.now(UTC) - ts).total_seconds() / 3600
    return age_hours > STALE_RUN_HOURS


def _resolve_lock_ref(registry: dict[str, Any], raw_ref: Any) -> Path | None:
    """Resolve lock refs only within the registry-owned lock directory."""
    if not isinstance(raw_ref, str) or not raw_ref.strip():
        return None
    workspace_root = registry_workspace_root(registry).resolve()
    lock_root = lock_state_dir(registry).resolve()
    candidate = Path(raw_ref).expanduser()
    if not candidate.is_absolute():
        candidate = workspace_root / candidate
    try:
        resolved = candidate.resolve()
        resolved.relative_to(lock_root)
    except (OSError, ValueError):
        return None
    return resolved


def build_observe_report(registry: dict[str, Any], run_id: str | None) -> dict[str, Any]:
    runs = load_run_records(registry)
    locks = load_lock_records(registry)
    now = datetime.now(UTC)
    findings: list[dict[str, Any]] = []

    selected_runs = {run_id: runs[run_id]} if run_id and run_id in runs else runs
    if run_id and run_id not in runs:
        findings.append(
            {
                "severity": "halt",
                "kind": "run_missing",
                "message": f"run not found: {run_id}",
                "run_id": run_id,
            }
        )

    for lock_path, lock in locks:
        lock_run_id = str(lock.get("run_id") or "")
        if run_id and lock_run_id != run_id:
            continue
        lock_rel = display_path(lock_path)
        if lock.get("parse_error"):
            findings.append(
                {
                    "severity": "halt",
                    "kind": "lock_parse_error",
                    "message": f"lock file is not valid YAML: {lock_rel}",
                    "path": lock_rel,
                }
            )
            continue
        if not lock_run_id or lock_run_id not in runs:
            findings.append(
                {
                    "severity": "halt",
                    "kind": "orphan_lock",
                    "message": f"lock has no matching run record: {lock_rel}",
                    "path": lock_rel,
                    "run_id": lock_run_id or None,
                }
            )
            continue
        expires_at = parse_utc_timestamp(str(lock.get("expires_at") or ""))
        if expires_at and expires_at < now:
            findings.append(
                {
                    "severity": "escalate",
                    "kind": "expired_lock",
                    "message": f"lock expired: {lock_rel}",
                    "path": lock_rel,
                    "run_id": lock_run_id,
                    "expires_at": lock.get("expires_at"),
                }
            )
        run_status = runs[lock_run_id][1].get("status")
        if run_status in {"ok", "failed", "blocked"}:
            findings.append(
                {
                    "severity": "halt",
                    "kind": "closed_run_lock",
                    "message": f"closed run still holds a lock: {lock_rel}",
                    "path": lock_rel,
                    "run_id": lock_run_id,
                    "status": run_status,
                }
            )

    lock_paths_by_run: dict[str, set[str]] = {}
    for lock_path, lock in locks:
        lock_run_id = str(lock.get("run_id") or "")
        resolved_lock_path = _resolve_lock_ref(registry, str(lock_path))
        if lock_run_id and resolved_lock_path is not None:
            lock_paths_by_run.setdefault(lock_run_id, set()).add(str(resolved_lock_path))

    for current_run_id, (path, payload) in selected_runs.items():
        is_active = payload.get("status") == "active"
        expected_locks: set[str] = set()
        raw_lock_refs = payload.get("locks") if is_active else []
        if not isinstance(raw_lock_refs, list):
            findings.append(
                {
                    "severity": "halt",
                    "kind": "invalid_lock_payload",
                    "message": "active run locks payload must be a list",
                    "path": display_path(path),
                    "run_id": current_run_id,
                }
            )
            raw_lock_refs = []
        if is_active:
            for index, raw_lock_ref in enumerate(raw_lock_refs):
                if not isinstance(raw_lock_ref, str) or not raw_lock_ref.strip():
                    findings.append(
                        {
                            "severity": "halt",
                            "kind": "invalid_lock_ref",
                            "message": "active run lock reference must be a non-empty string",
                            "path": display_path(path),
                            "run_id": current_run_id,
                            "index": index,
                        }
                    )
                    continue
                resolved_lock_ref = _resolve_lock_ref(registry, raw_lock_ref)
                if resolved_lock_ref is None:
                    findings.append(
                        {
                            "severity": "halt",
                            "kind": "lock_path_outside_registry_root",
                            "message": f"run lock reference escapes registry lock directory: {raw_lock_ref}",
                            "path": str(raw_lock_ref),
                            "run_id": current_run_id,
                            "index": index,
                        }
                    )
                    continue
                expected_locks.add(str(resolved_lock_ref))
        missing_locks = sorted(expected_locks - lock_paths_by_run.get(current_run_id, set()))
        if is_active and missing_locks:
            # Stale run downgrade: 若 active run 超过 STALE_RUN_HOURS 无更新,
            # 视为僵尸 run (锁可能已被 TTL 清理但 run 状态未同步)。此时缺锁
            # 降级为 warn 而非 halt, 防止僵尸 run 永久阻塞 observe/compliance。
            # 活跃 run (近期更新) 缺锁仍 halt, 保留 fail-closed 保护。
            stale = _is_stale_run(payload)
            findings.append(
                {
                    "severity": "warn" if stale else "halt",
                    "kind": "active_run_missing_locks",
                    "message": (
                        f"active run is missing lock files: {current_run_id}"
                        + (" (stale run, lock likely TTL-cleaned)" if stale else "")
                    ),
                    "run_id": current_run_id,
                    "missing_locks": missing_locks,
                }
            )
        # ADR-0209 A2: self-heal missing ledger rows from run yaml before warn
        if not ledger_mentions_run(registry, current_run_id):
            healed = heal_ledger_for_run(registry, current_run_id, payload)
            if healed and ledger_mentions_run(registry, current_run_id):
                findings.append(
                    {
                        "severity": "info",
                        "kind": "ledger_healed_from_run",
                        "message": (f"ledger missing run event; replayed from run yaml: {current_run_id}"),
                        "run_id": current_run_id,
                        "path": display_path(path),
                    }
                )
            else:
                findings.append(
                    {
                        "severity": "warn",
                        "kind": "ledger_missing_run",
                        "message": f"ledger has no event for run: {current_run_id}",
                        "run_id": current_run_id,
                        "path": display_path(path),
                    }
                )

    severities = {finding["severity"] for finding in findings}
    decision = "escalate" if "escalate" in severities else "halt" if "halt" in severities else "continue"
    report = {
        "ok": decision == "continue",
        "decision": decision,
        "run_count": len(selected_runs),
        "lock_count": len([lock for _, lock in locks if not run_id or str(lock.get("run_id") or "") == run_id]),
        "ledger": display_path(ledger_path(registry)),
        "findings": findings,
    }
    return report


def observe(registry: dict[str, Any], run_id: str | None, as_json: bool) -> int:
    report = build_observe_report(registry, run_id)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"agent-workflow observe: {report['decision']}")
        print(f"runs={report['run_count']} locks={report['lock_count']} ledger={report['ledger']}")
        for finding in report["findings"]:
            print(f"[{finding['severity'].upper()}] {finding['kind']}: {finding['message']}")
    return 0 if report["decision"] == "continue" else 1


def ledger_events(registry: dict[str, Any]) -> list[dict[str, Any]]:
    path = ledger_path(registry)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            events.append({"parse_error": True, "raw": line})
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _git_name_only(args: list[str]) -> list[str]:
    """Return normalized paths from git name-only listing."""
    import subprocess

    completed = subprocess.run(
        args,
        cwd=WORKSPACE,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return []
    out: list[str] = []
    for line in completed.stdout.splitlines():
        item = line.strip()
        if item:
            out.append(normalize_repo_path(item))
    return out


def staged_files_from_git() -> list[str]:
    return sorted(set(_git_name_only(["git", "diff", "--cached", "--name-only"])))


def requirement_iteration_report(registry: dict[str, Any]) -> dict[str, Any]:
    """ADR-0203 gate: staged requirement-scope files require an active workflow run.

    Design notes (multi-agent worktrees):
    - Hard fail uses **staged** files only (about to commit), not dirty unstaged noise.
    - Working-tree (unstaged) in-scope files produce advisory warnings only.
    - Bypass: AGCP_REQUIREMENT_ITERATION_GATE=0
    """
    import os

    policy = registry.get("requirement_iteration_policy")
    if not isinstance(policy, dict):
        return {
            "ok": True,
            "checked": False,
            "mode": "off",
            "reason": "no requirement_iteration_policy",
            "findings": [],
            "staged_in_scope": [],
            "unstaged_in_scope": [],
            "active_runs": [],
        }

    mode = str(policy.get("mode") or "off")
    if mode not in {"off", "advisory", "required"}:
        mode = "off"

    if os.environ.get("AGCP_REQUIREMENT_ITERATION_GATE", "1") in {"0", "false", "no"}:
        return {
            "ok": True,
            "checked": False,
            "mode": mode,
            "bypassed": True,
            "reason": "AGCP_REQUIREMENT_ITERATION_GATE disabled",
            "findings": [],
            "staged_in_scope": [],
            "unstaged_in_scope": [],
            "active_runs": [],
        }

    if mode == "off":
        return {
            "ok": True,
            "checked": False,
            "mode": mode,
            "findings": [],
            "staged_in_scope": [],
            "unstaged_in_scope": [],
            "active_runs": [],
        }

    default_include = [
        "projects/**",
        ".omo/_truth/**",
        ".omo/standards/**",
        ".omo/_knowledge/decisions/**",
        ".omo/_knowledge/patterns/**",
        ".agents/**",
        "bin/**",
        "docs/**",
        "tests/**",
        "AGENTS.md",
        "CLAUDE.md",
        "ARCHITECTURE.md",
        "README.md",
    ]
    default_exclude = [
        ".omo/state/**",
        ".omo/_delivery/**",
        ".omo/_control/**",
        "runtime/**",
        "**/__pycache__/**",
        "**/*.pyc",
    ]
    include = [str(p) for p in (policy.get("in_scope_paths") or default_include) if isinstance(p, str)]
    exclude = [str(p) for p in (policy.get("exclude_paths") or default_exclude) if isinstance(p, str)]

    def in_scope(path: str) -> bool:
        if exclude and path_matches(exclude, path):
            return False
        return path_matches(include, path) if include else False

    staged = [p for p in staged_files_from_git() if in_scope(p)]
    working = changed_files_from_git(include_untracked=False)
    staged_set = set(staged)
    unstaged = [p for p in working if p not in staged_set and in_scope(p)]

    runs = load_run_records(registry)
    active_runs = sorted(run_id for run_id, (_, payload) in runs.items() if payload.get("status") == "active")

    findings: list[dict[str, Any]] = []
    if staged and not active_runs:
        severity = "halt" if mode == "required" else "warn"
        findings.append(
            {
                "severity": severity,
                "kind": "requirement_iteration_no_active_run",
                "message": (
                    f"staged requirement-scope files without active agent-workflow run "
                    f"({len(staged)} file(s)); start+claim first (ADR-0203)"
                ),
                "files": staged[:20],
            }
        )
    if unstaged and not active_runs:
        findings.append(
            {
                "severity": "warn",
                "kind": "requirement_iteration_dirty_without_run",
                "message": (
                    f"unstaged requirement-scope dirty files without active run "
                    f"({len(unstaged)} file(s)); advisory only"
                ),
                "files": unstaged[:20],
            }
        )

    severities = {f["severity"] for f in findings}
    ok = "halt" not in severities
    return {
        "ok": ok,
        "checked": True,
        "mode": mode,
        "bypassed": False,
        "staged_in_scope": staged,
        "unstaged_in_scope": unstaged,
        "active_runs": active_runs,
        "findings": findings,
        "policy_adr": policy.get("adr"),
    }


def _auto_fix_orphan_locks(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """自动修复孤儿锁，返回已修复列表."""
    workspace_root = registry_workspace_root(registry)
    fix_script = workspace_root / "bin" / "gac" / "fix-orphan-locks.py"
    if not fix_script.exists():
        return []
    try:
        proc = subprocess.run(
            [sys.executable, str(fix_script), "--apply", "--registry", str(workspace_root / ".omo")],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode == 0:
            data = json.loads(proc.stdout)
            return data.get("applied", [])
    except Exception:
        pass
    return []


def compliance_report(registry: dict[str, Any], run_id: str | None) -> dict[str, Any]:
    _auto_fix_orphan_locks(registry)
    runs = load_run_records(registry)
    events = ledger_events(registry)
    observe_report = build_observe_report(registry, run_id)
    findings: list[dict[str, Any]] = []
    event_names_by_run: dict[str, set[str]] = {}
    for event in events:
        current_run_id = str(event.get("run_id") or "")
        if current_run_id:
            event_names_by_run.setdefault(current_run_id, set()).add(str(event.get("event") or ""))
        if event.get("parse_error"):
            findings.append(
                {
                    "severity": "halt",
                    "kind": "ledger_parse_error",
                    "message": "ledger contains a non-JSON line",
                }
            )
    selected_runs = {run_id: runs[run_id]} if run_id and run_id in runs else runs
    if run_id and run_id not in runs:
        findings.append(
            {
                "severity": "halt",
                "kind": "run_missing",
                "message": f"run not found: {run_id}",
                "run_id": run_id,
            }
        )
    for current_run_id, (_, payload) in selected_runs.items():
        status = payload.get("status")
        evidence = payload.get("evidence") or []
        if status == "active":
            findings.append(
                {
                    "severity": "warn",
                    "kind": "active_run",
                    "message": f"run is still active: {current_run_id}",
                    "run_id": current_run_id,
                }
            )
        if status == "ok" and not evidence:
            findings.append(
                {
                    "severity": "halt",
                    "kind": "closed_run_missing_evidence",
                    "message": f"closed run has no evidence: {current_run_id}",
                    "run_id": current_run_id,
                }
            )
        event_names = event_names_by_run.get(current_run_id, set())
        if status == "ok" and "agent_workflow_verify" not in event_names:
            # D1 (ADR-0355 方案A): close 手动 evidence 算 manual verify (ADR-0209 A1 protocol honesty),
            # 不 warn missing_verify 噪音; 降级 info 区分 manual vs auto verify.
            # 无 evidence 的 halt 由 closed_run_missing_evidence (上 above) 兜底.
            if evidence:
                findings.append(
                    {
                        "severity": "info",
                        "kind": "closed_run_manual_verify",
                        "message": f"closed run uses manual evidence (ADR-0209 A1), no auto verify event: {current_run_id}",
                        "run_id": current_run_id,
                    }
                )
            else:
                findings.append(
                    {
                        "severity": "warn",
                        "kind": "closed_run_missing_verify_event",
                        "message": f"closed run has no verify event and no evidence: {current_run_id}",
                        "run_id": current_run_id,
                    }
                )
        close_event_names = {"agent_workflow_closeout", "agent_workflow_close"}
        if status == "ok" and not event_names.intersection(close_event_names):
            findings.append(
                {
                    "severity": "warn",
                    "kind": "closed_run_missing_closeout_event",
                    "message": f"closed run did not use closeout: {current_run_id}",
                    "run_id": current_run_id,
                }
            )
    severities = {finding["severity"] for finding in [*findings, *observe_report["findings"]]}
    decision = "halt" if "halt" in severities else "escalate" if "escalate" in severities else "continue"
    p74_report = p74_solidification_report(registry, events, runs)
    req_report = requirement_iteration_report(registry)
    for finding in req_report.get("findings") or []:
        findings.append(finding)
    severities = {finding["severity"] for finding in [*findings, *observe_report["findings"]]}
    decision = "halt" if "halt" in severities else "escalate" if "escalate" in severities else "continue"
    return {
        "ok": decision == "continue",
        "decision": decision,
        "run_count": len(selected_runs),
        "event_count": len(events),
        "observe": observe_report,
        "findings": findings,
        "slo": registry.get("compliance_slo") or {},
        "p74_solidification": p74_report,
        "requirement_iteration": req_report,
    }


def print_compliance_report(report: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(f"agent-workflow compliance: {report['decision']}")
    print(f"runs={report['run_count']} events={report['event_count']}")
    for finding in report["findings"]:
        print(f"[{finding['severity'].upper()}] {finding['kind']}: {finding['message']}")
    for finding in report["observe"]["findings"]:
        print(f"[{finding['severity'].upper()}] {finding['kind']}: {finding['message']}")
    p74 = report.get("p74_solidification") or {}
    if p74:
        ok = "OK" if p74.get("ok") else "WARN"
        print(f"P74 solidification: [{ok}] {p74.get('warn_count', 0)} silent workflow(s)")
        for wf in p74.get("workflows", []):
            if wf.get("silent_health") != "active":
                print(
                    f"  - {wf['workflow_id']}: {wf['silent_health']} "
                    f"(run={wf['has_recent_run']}, check={wf['has_check_coverage']})"
                )
    req = report.get("requirement_iteration") or {}
    if req:
        label = "OK" if req.get("ok") else "HALT" if not req.get("ok") else "WARN"
        if req.get("bypassed"):
            label = "BYPASS"
        print(
            f"requirement_iteration: [{label}] mode={req.get('mode')} "
            f"staged={len(req.get('staged_in_scope') or [])} "
            f"active_runs={len(req.get('active_runs') or [])}"
        )


def last_ledger_event(
    events: list[dict[str, Any]],
    names: set[str],
) -> dict[str, Any] | None:
    for event in reversed(events):
        if str(event.get("event") or "") in names:
            return event
    return None


def build_status_report(
    registry: dict[str, Any],
    include_health: bool,
    include_agcp_drift: bool = True,
) -> dict[str, Any]:
    runs = load_run_records(registry)
    active_runs = sorted(run_id for run_id, (_, payload) in runs.items() if payload.get("status") == "active")
    closed_runs = sorted(
        run_id for run_id, (_, payload) in runs.items() if payload.get("status") in {"ok", "failed", "blocked"}
    )
    observe_report = build_observe_report(registry, None)
    compliance = compliance_report(registry, None)
    events = ledger_events(registry)
    staged_lane = staged_lane_report()
    lock_scan = scan_locks(registry)
    stale_locks = sum(1 for entry in lock_scan if entry["kind"] in ("zombie_expired", "zombie_stale_heartbeat"))
    live_locks = sum(1 for entry in lock_scan if entry["kind"] == "live")
    current_run_id = active_runs[0] if len(active_runs) == 1 else None
    changed_files = changed_files_from_git(include_untracked=False)
    policy = claim_policy(registry)
    claim_coverage = (
        claim_coverage_report(registry, current_run_id, changed_files)
        if current_run_id
        else {
            "ok": True,
            "mode": policy["mode"],
            "checked": False,
            "run_id": current_run_id,
            "required_paths": policy["required_paths"],
            "tiers": policy["tiers"],
            "claimed_paths": [],
            "missing_files": [],
            "missing_required_files": [],
            "missing_advisory_files": [],
            "warnings": ["multiple active runs; pass a run id to verify/closeout"] if len(active_runs) > 1 else [],
        }
    )
    health = build_doctor_report(registry, include_agcp_drift) if include_health else None
    report = {
        "ok": observe_report["decision"] == "continue"
        and compliance["decision"] == "continue"
        and (health is None or bool(health["ok"])),
        "active_runs": active_runs,
        "closed_runs": closed_runs,
        "run_count": len(runs),
        "lock_count": observe_report["lock_count"],
        "stale_locks": stale_locks,
        "live_locks": live_locks,
        "lock_details": lock_scan,
        "current_run_id": current_run_id,
        "last_verify": last_ledger_event(events, {"agent_workflow_verify"}),
        "last_closeout": last_ledger_event(events, {"agent_workflow_closeout", "agent_workflow_close"}),
        "compliance": {
            "ok": compliance["ok"],
            "decision": compliance["decision"],
            "slo": compliance["slo"],
            "findings": compliance["findings"],
            "observe_findings": compliance["observe"]["findings"],
        },
        "requirement_iteration": compliance.get("requirement_iteration") or requirement_iteration_report(registry),
        "staged_lane": staged_lane,
        "changed_files": changed_files,
        "claim_coverage": claim_coverage,
        "health": None if health is None else {"ok": health["ok"], "checks": check_summary(health["checks"])},
    }
    report["recommended_next"] = recommended_next(report)
    return report


def print_status_report(report: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(f"agent-workflow status: {'ok' if report['ok'] else 'attention'}")
    print(
        f"runs active={len(report['active_runs'])} closed={len(report['closed_runs'])} "
        f"locks={report['lock_count']} stale={report['stale_locks']}"
    )
    staged_lane = report["staged_lane"]
    print(f"staged_lane={'PASS' if staged_lane['ok'] else 'WARN'} lanes={','.join(staged_lane['lanes']) or '-'}")
    claim_coverage = report.get("claim_coverage")
    if isinstance(claim_coverage, dict):
        for warning in claim_coverage.get("warnings") or []:
            print(f"[WARN] claim_policy: {warning}")
    print(f"compliance={report['compliance']['decision']}")
    req = report.get("requirement_iteration") or {}
    if req:
        flag = "ok" if req.get("ok") else "attention"
        print(
            f"requirement_iteration={flag} mode={req.get('mode')} "
            f"staged={len(req.get('staged_in_scope') or [])} "
            f"active={len(req.get('active_runs') or [])}"
        )
    print(f"next: {report['recommended_next']}")


def run_doctor_check(check_item: dict[str, Any]) -> dict[str, Any]:
    import subprocess

    command = check_item["command"]
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    try:
        completed = subprocess.run(
            command,
            cwd=WORKSPACE,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        ok = completed.returncode == 0
        return {
            "id": check_item["id"],
            "description": check_item.get("description", ""),
            "required": bool(check_item.get("required", True)),
            "command": command_display(command),
            "ok": ok,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip()[-1000:],
            "stderr": completed.stderr.strip()[-1000:],
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "id": check_item["id"],
            "description": check_item.get("description", ""),
            "required": bool(check_item.get("required", True)),
            "command": command_display(command),
            "ok": False,
            "error": str(exc),
        }


def build_doctor_report(registry: dict[str, Any], include_agcp_drift: bool = True) -> dict[str, Any]:
    integrations = integration_rows(registry)
    for integration in integrations:
        name = str(integration["name"])
        health = None
        health_command = integration.get("health_command")
        health_required = bool(integration.get("health_required", False))
        if isinstance(health_command, list) and health_command:
            health = run_doctor_check(
                {
                    "id": f"integration-{name}-health",
                    "description": f"Internal integration health check for {name}.",
                    "required": health_required,
                    "command": health_command,
                }
            )
        integration["health"] = health

    adapters = adapter_rows(registry)
    for adapter in adapters:
        name = str(adapter["name"])
        health = None
        health_command = adapter.get("health_command")
        health_required = bool(adapter.get("health_required", False))
        if isinstance(health_command, list) and health_command:
            health = run_doctor_check(
                {
                    "id": f"adapter-{name}-health",
                    "description": f"External adapter health check for {name}.",
                    "required": health_required,
                    "command": health_command,
                }
            )
        adapter["health"] = health
    checks = [run_doctor_check(item) for item in registry.get("doctor_checks", [])]
    if include_agcp_drift:
        checks.insert(0, agcp_drift_check(registry))
    required_integration_health = [
        integration["health"]
        for integration in integrations
        if integration.get("health_required") and isinstance(integration.get("health"), dict)
    ]
    required_adapter_health = [
        adapter["health"]
        for adapter in adapters
        if adapter.get("health_required") and isinstance(adapter.get("health"), dict)
    ]
    ok = all(
        (not item["required"]) or item["ok"]
        for item in [*checks, *required_integration_health, *required_adapter_health]
    )
    return {
        "ok": ok,
        "registry": str(REGISTRY_PATH.relative_to(WORKSPACE)),
        "integrations": integrations,
        "adapters": adapters,
        "checks": checks,
    }


def print_doctor_report(report: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(f"registry: {report['registry']}")
    for integration in report["integrations"]:
        health = integration.get("health")
        health_status = ""
        if isinstance(health, dict):
            label = "PASS" if health["ok"] else ("FAIL" if integration.get("health_required") else "WARN")
            health_status = f" health={label}"
        print(f"{integration['name']:<14} {integration['status']:<12} {integration['authority']:<16}{health_status}")
    for adapter in report["adapters"]:
        status = "available" if adapter["available"] else "missing"
        suffix = f" ({adapter['path']})" if adapter["path"] else ""
        health = adapter.get("health")
        if isinstance(health, dict):
            health_status = "PASS" if health["ok"] else ("FAIL" if adapter.get("health_required") else "WARN")
            suffix += f" health={health_status}"
        print(f"{adapter['name']:<14} {status}{suffix}")
    for item in report["checks"]:
        status = "PASS" if item["ok"] else ("WARN" if not item["required"] else "FAIL")
        print(f"[{status}] {item['id']} :: {item['command']}")
        if not item["ok"] and item.get("stderr"):
            print(item["stderr"], file=sys.stderr)


def doctor(registry: dict[str, Any], as_json: bool, include_agcp_drift: bool = True) -> int:
    report = build_doctor_report(registry, include_agcp_drift)
    print_doctor_report(report, as_json)
    return 0 if report["ok"] else 1


def health_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for row in rows:
        health = row.get("health")
        summary.append(
            {
                "name": row.get("name"),
                "status": row.get("status"),
                "authority": row.get("authority"),
                "required": bool(row.get("health_required", False)),
                "health_ok": health.get("ok") if isinstance(health, dict) else None,
                "command": health.get("command")
                if isinstance(health, dict)
                else command_display(row.get("health_command", [])),
                "advisory": not bool(row.get("health_required", False)),
            }
        )
    return summary


def check_summary(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": check.get("id"),
            "required": bool(check.get("required", True)),
            "ok": bool(check.get("ok", False)),
            "command": check.get("command"),
        }
        for check in checks
    ]
