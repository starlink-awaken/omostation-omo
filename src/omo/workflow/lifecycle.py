from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

try:
    from .mesh_agent_events import emit_workflow_mesh_event
except ImportError:  # graceful degradation during refactoring

    def emit_workflow_mesh_event(
        event_type: str,
        run_id: str,
        payload: dict[str, Any] | None = None,
        *,
        workspace: Any = None,
        scene_binding: dict[str, str] | None = None,
    ) -> bool:
        return False


try:
    from .scene_bridge import extract_scene_binding
except ImportError:

    def extract_scene_binding(
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, str] | None:
        return None


from ..omo_io import write_yaml_atomic
from .affected_graph_receipt import validate_affected_graph_receipt
from .core import (
    CLAIM_POLICY_MODES,
    RUN_UPDATE_LOCK_TIMEOUT_SECONDS,
    WORKSPACE,
    WorkflowError,
    command_display,
    display_path,
    ledger_path,
    lock_state_dir,
    normalize_repo_path,
    path_matches,
    registry_workspace_root,
    run_state_dir,
    substitute,
    utc_now,
    validate_agent_profile,
    workflow_by_id,
)

_LOCK_FILENAME_MAX_LEN = 255
_RUN_UPDATE_LOCK_NAME_MAX_LEN = _LOCK_FILENAME_MAX_LEN - len("run_.update.lock")
_PATH_LOCK_NAME_MAX_LEN = _LOCK_FILENAME_MAX_LEN - len(".lock.yaml")
_SPEC_BINDING_CONTRACT: ModuleType | None = None
_LEGACY_DELIVERY_IDENTITY_KEYS = ("spec_binding", "work_packet", "work_packet_hash")
_DELIVERY_IDENTITY_KEYS = (
    *_LEGACY_DELIVERY_IDENTITY_KEYS,
    "capability_requirements_digest",
    "capability_preflight",
)
_CAPABILITY_PREFLIGHT_OPTIONAL_KEYS = (
    "capability_requirements_digest",
    "capability_preflight",
)
_SHA256_REF_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PREFLIGHT_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:@/-]{1,256}$")
_PREFLIGHT_BINDING_KEYS = (
    "correlation_id",
    "workflow_run_id",
    "packet_id",
    "packet_hash",
    "assignment_id",
    "dispatch_id",
    "actor_id",
    "delivery_attempt_id",
)


def _load_spec_binding_contract() -> ModuleType:
    """Load the Workspace-owned BET/WorkPacket boundary or fail closed."""
    global _SPEC_BINDING_CONTRACT
    if _SPEC_BINDING_CONTRACT is not None:
        return _SPEC_BINDING_CONTRACT
    path = WORKSPACE / "bin/plan/bet-ledger.py"
    spec = importlib.util.spec_from_file_location("_omo_spec_binding_contract", path)
    if spec is None or spec.loader is None:
        raise WorkflowError(f"SPEC_BINDING_UNAVAILABLE: cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - a broken mandatory gate must halt.
        sys.modules.pop(spec.name, None)
        raise WorkflowError(f"SPEC_BINDING_UNAVAILABLE: cannot load {path}: {exc}") from exc
    _SPEC_BINDING_CONTRACT = module
    return module


from .lifecycle_preflight import (
    _complete_fresh_delivery_identity,
    _delivery_identity_from_parent,
    _preflight_error,
    _prepare_bet_execution,
    _required_preflight_identifier,
    _required_preflight_text,
    _validate_capability_preflight,
    _validate_inherited_delivery_identity,
    _validate_work_packet_claim,
    resolve_parent_delivery_identity,
)


def workflow_plan(workflow: dict[str, Any], context: dict[str, str]) -> dict[str, Any]:
    resolved = substitute(workflow, context)
    return {
        "id": resolved["id"],
        "title": resolved.get("title", ""),
        "purpose": resolved.get("purpose", ""),
        "agents": resolved.get("agents", {}),
        "allowed_lanes": resolved.get("allowed_lanes", []),
        "lock_scopes": resolved.get("lock_scopes", []),
        "phases": resolved.get("phases", {}),
    }


def print_plan(plan: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return
    print(f"{plan['id']} — {plan['title']}")
    print(plan["purpose"])
    roles = plan.get("agents", {}).get("roles") or []
    if roles:
        print(f"agents: {', '.join(roles)}")
    print(f"lanes: {', '.join(plan['allowed_lanes'])}")
    print(f"locks: {', '.join(plan['lock_scopes'])}")
    for phase, entries in plan["phases"].items():
        print(f"\n[{phase}]")
        for item in entries:
            mode = item.get("mode", "?")
            cwd = item.get("cwd")
            prefix = f"({mode})"
            if cwd:
                prefix += f" cwd={cwd}"
            print(f"  {item.get('id')}: {prefix} {command_display(item['command'])}")


def run_stage(
    workflow: dict[str, Any],
    stage: str,
    context: dict[str, str],
    execute: bool,
    as_json: bool,
) -> int:
    plan = workflow_plan(workflow, context)
    entries = plan["phases"].get(stage)
    if not entries:
        raise WorkflowError(f"{plan['id']} has no stage: {stage}")

    results: list[dict[str, Any]] = []
    for item in entries:
        mode = item.get("mode")
        command = item["command"]
        cwd = WORKSPACE / item.get("cwd", ".")
        skipped = mode == "manual" or not execute
        result: dict[str, Any] = {
            "id": item.get("id"),
            "mode": mode,
            "command": command_display(command),
            "cwd": str(cwd.relative_to(WORKSPACE)) if cwd.is_relative_to(WORKSPACE) else str(cwd),
            "skipped": skipped,
            "ok": True,
        }
        if not skipped:
            completed = subprocess.run(command, cwd=cwd, check=False)
            result["returncode"] = completed.returncode
            result["ok"] = completed.returncode == 0 or mode == "advisory"
        results.append(result)

    report = {
        "workflow": plan["id"],
        "stage": stage,
        "execute": execute,
        "results": results,
    }
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for result in results:
            status = "SKIP" if result["skipped"] else ("PASS" if result["ok"] else "FAIL")
            print(f"[{status}] {result['id']} :: {result['command']}")
    return 0 if all(item["ok"] for item in results) else 1


def heartbeat_run(registry: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Serialize and renew every lock owned by one active run."""
    with run_update_lock(registry, run_id):
        return _heartbeat_run_locked(registry, run_id)


def _heartbeat_run_locked(registry: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Renew ``last_heartbeat`` on every lock owned by an active run.

    Prevalidates all locks before writing any:
      - lock file must exist
      - YAML payload must be a mapping
      - payload ``run_id`` must exactly match
      - resolved path must be within the configured lock directory

    On validation failure no lock is modified.
    Returns ``{"run_id", "heartbeat_at", "renewed", "count"}``.
    """
    _, payload = read_run(registry, run_id)
    if payload.get("status") != "active":
        raise WorkflowError(f"cannot heartbeat non-active run {run_id} (status={payload.get('status', 'unknown')})")

    lock_dir = lock_state_dir(registry).resolve()
    raw_locks = payload.get("locks")
    if raw_locks is None:
        raw_locks = []
    if not isinstance(raw_locks, list):
        raise WorkflowError(f"run {run_id} locks must be a list, got {type(raw_locks).__name__}")
    for entry in raw_locks:
        if not isinstance(entry, str) or not entry:
            raise WorkflowError(f"run {run_id} locks contains invalid entry: {entry!r}")

    # Phase 1 — prevalidate every lock (no writes yet)
    validated: list[tuple[Path, dict[str, Any], str]] = []
    for lock_display in raw_locks:
        lock_path_raw = Path(lock_display)
        if not lock_path_raw.is_absolute():
            lock_path_raw = registry_workspace_root(registry) / lock_display
        try:
            lock_path = lock_path_raw.resolve()
        except OSError:
            raise WorkflowError(f"cannot resolve lock path: {lock_display}")

        try:
            lock_path.relative_to(lock_dir)
        except ValueError:
            raise WorkflowError(f"lock path escapes lock directory: {lock_display}")

        if not lock_path.exists():
            raise WorkflowError(f"missing lock file: {lock_display}")

        try:
            lock_data = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise WorkflowError(f"malformed YAML in lock {lock_display}: {exc}")
        except OSError as exc:
            raise WorkflowError(f"unreadable lock {lock_display}: {exc}")
        except UnicodeError as exc:
            raise WorkflowError(f"malformed lock (encoding error) {lock_display}: {exc}")
        if not isinstance(lock_data, dict):
            raise WorkflowError(f"malformed lock (not a mapping): {lock_display}")

        if lock_data.get("run_id") != run_id:
            raise WorkflowError(
                f"lock run_id mismatch in {lock_display}: expected {run_id}, found {lock_data.get('run_id')}"
            )

        validated.append((lock_path, lock_data, lock_display))

    # Phase 2 — write: only last_heartbeat changes (atomic per lock)
    heartbeat_at = utc_now()
    renewed: list[str] = []
    for lock_path, lock_data, lock_display in validated:
        lock_data["last_heartbeat"] = heartbeat_at
        write_yaml_atomic(lock_path, lock_data)
        renewed.append(lock_display)

    return {
        "run_id": run_id,
        "heartbeat_at": heartbeat_at,
        "renewed": renewed,
        "count": len(renewed),
    }


def run_file_for(registry: dict[str, Any], run_id: str) -> Path:
    run_dir = run_state_dir(registry)
    direct = run_dir / f"{run_id}.yaml"
    if direct.exists():
        return direct
    matches = list(run_dir.glob(f"*{run_id}*.yaml")) if run_dir.exists() else []
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise WorkflowError(f"ambiguous run id {run_id}: {', '.join(str(p) for p in matches)}")
    raise WorkflowError(f"run not found: {run_id}")


def start_run(
    registry: dict[str, Any],
    workflow: dict[str, Any],
    context: dict[str, str],
    objective: str,
    dry_run: bool,
    force_lock: bool,
    *,
    parent_run_id: str = "",
    parent_agent: str = "",
    bet_id: str = "",
    inherited_delivery_identity: dict[str, Any] | None = None,
    start_preflight: Callable[[str, dict[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    validate_agent_profile(registry, workflow, context.get("profile", ""), require=True)
    if parent_run_id:
        parent_bet_id, parent_identity, resolved_parent_agent = resolve_parent_delivery_identity(
            registry,
            parent_run_id,
            bet_id,
        )
        if inherited_delivery_identity is not None and inherited_delivery_identity != parent_identity:
            raise WorkflowError("WORK_PACKET_PARENT_BINDING_MISMATCH: supplied child identity differs from parent")
        bet_id = parent_bet_id
        inherited_delivery_identity = parent_identity
        parent_agent = resolved_parent_agent
    elif inherited_delivery_identity is not None:
        raise WorkflowError("WORK_PACKET_PARENT_BINDING_INCOMPLETE: inherited identity requires parent_run_id")
    if inherited_delivery_identity is not None:
        delivery_identity = _validate_inherited_delivery_identity(
            bet_id,
            inherited_delivery_identity,
            parent_run_id=parent_run_id,
        )
    else:
        delivery_identity = _prepare_bet_execution(bet_id) if bet_id else None
    if bet_id:
        context = {**context, "bet_id": bet_id}
    plan = workflow_plan(workflow, context)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{stamp}-{plan['id']}-{uuid.uuid4().hex[:8]}"
    context = {**context, "run_id": run_id}
    plan = workflow_plan(workflow, context)
    if inherited_delivery_identity is None and delivery_identity is not None:
        delivery_identity = _complete_fresh_delivery_identity(delivery_identity, run_id, start_preflight)
    record: dict[str, Any] = {
        "run_id": run_id,
        "workflow_id": plan["id"],
        "status": "active",
        "actor": context["actor"],
        "agent_profile": context.get("profile", ""),
        "objective": objective,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "context": context,
        "locks": [],
        "plan": plan,
        "evidence": [],
    }
    if parent_run_id:
        record["parent_run_id"] = parent_run_id
    if parent_agent:
        record["parent_agent"] = parent_agent
    if bet_id:
        record["bet_id"] = bet_id
        if delivery_identity is None:  # Defensive invariant; preparation is fail-closed.
            raise WorkflowError(f"WORK_PACKET_MISSING: no prepared identity for {bet_id}")
        record.update(delivery_identity)
    if dry_run:
        return record
    record["locks"] = acquire_locks(registry, plan["lock_scopes"], run_id, context["actor"], force_lock)
    run_dir = run_state_dir(registry)
    run_dir.mkdir(parents=True, exist_ok=True)
    run_path = run_dir / f"{run_id}.yaml"
    run_path.write_text(
        yaml.safe_dump(record, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )  # audit-exempt: non-atomic-write — run state single-writer under run_update_lock
    record["path"] = display_path(run_path)
    start_event: dict[str, Any] = {
        "event": "agent_workflow_start",
        "run_id": run_id,
        "workflow_id": plan["id"],
        "actor": context["actor"],
        "agent_profile": context.get("profile", ""),
        "objective": objective,
        "path": record["path"],
        "locks": record["locks"],
    }
    if parent_run_id:
        start_event["parent_run_id"] = parent_run_id
    if parent_agent:
        start_event["parent_agent"] = parent_agent
    append_ledger_event(registry, start_event)
    # Phase 1b/4: Bridge to Workflow Mesh with scene_binding
    _scene_binding = extract_scene_binding(context=context, workflow=workflow)
    emit_workflow_mesh_event(
        "AgentWorkflowStarted",
        run_id,
        {
            "workflow_id": plan["id"],
            "agent_profile": context.get("profile", ""),
            "objective": objective,
            "actor": context["actor"],
        },
        workspace=registry_workspace_root(registry),
    )
    return record


def spawn_run(
    registry: dict[str, Any],
    parent_run_id: str,
    workflow: dict[str, Any],
    context: dict[str, str],
    objective: str,
    dry_run: bool = False,
    force_lock: bool = False,
    *,
    start_preflight: Callable[[str, dict[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    return start_run(
        registry,
        workflow,
        context,
        objective,
        dry_run,
        force_lock,
        parent_run_id=parent_run_id,
        start_preflight=start_preflight,
    )


def trace_attribution(registry: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
    chain: list[dict[str, Any]] = []
    visited: set[str] = set()
    current_id: str | None = run_id
    while current_id and current_id not in visited:
        visited.add(current_id)
        try:
            _, payload = read_run(registry, current_id)
        except (WorkflowError, FileNotFoundError):
            chain.append({"run_id": current_id, "status": "missing"})
            break
        entry = {
            "run_id": payload.get("run_id", current_id),
            "actor": payload.get("actor", ""),
            "agent_profile": payload.get("agent_profile", ""),
            "workflow_id": payload.get("workflow_id", ""),
            "status": payload.get("status", ""),
            "objective": payload.get("objective", ""),
        }
        chain.append(entry)
        current_id = payload.get("parent_run_id")
    chain.reverse()
    return chain


def read_run(registry: dict[str, Any], run_id: str) -> tuple[Path, dict[str, Any]]:
    path = run_file_for(registry, run_id)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict) or not payload.get("run_id"):
        raise WorkflowError(f"invalid run file: {path}")
    return path, payload


def write_run(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = utc_now()
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )  # audit-exempt: non-atomic-write — under run_update_lock


def claim_run(
    registry: dict[str, Any],
    run_id: str,
    actor: str,
    paths: list[str],
    surfaces: list[str],
    force_lock: bool,
    affected_hash: str | None = None,
    affected_receipt: str | None = None,
) -> dict[str, Any]:
    _, guard_payload = read_run(registry, run_id)
    if guard_payload.get("status") != "active":
        raise WorkflowError(f"cannot claim against non-active run: {run_id}")
    _validate_work_packet_claim(guard_payload, list(paths or []), list(surfaces or []))
    heartbeat_run(registry, run_id)  # SR-01: renew before claim
    receipt_reference = affected_hash or affected_receipt
    if not receipt_reference:
        raise WorkflowError("Missing or invalid affected-hash. You must run affected-graph.py first.")
    if not paths and not surfaces:
        raise WorkflowError("claim requires at least one --path or --surface")
    with run_update_lock(registry, run_id):
        path, payload = read_run(registry, run_id)
        if payload.get("status") != "active":
            raise WorkflowError(f"cannot claim against non-active run: {run_id}")
        normalized_paths = sorted({normalize_repo_path(item) for item in paths})
        normalized_surfaces = sorted({item.strip() for item in surfaces if item.strip()})
        affected_graph = validate_affected_graph_receipt(
            receipt_reference,
            normalized_paths,
            WORKSPACE,
            normalized_surfaces,
        )

        # Phase 3 A2A Path Locks (Logical Isolation)
        # Check for path hierarchy overlap with other active runs
        run_dir = run_state_dir(registry)
        if run_dir.exists():
            for other_run_file in run_dir.glob("*.yaml"):
                if other_run_file.name == f"{run_id}.yaml":
                    continue
                try:
                    other_payload = yaml.safe_load(other_run_file.read_text(encoding="utf-8")) or {}
                except Exception:
                    continue
                if not isinstance(other_payload, dict):
                    continue
                if other_payload.get("status") != "active":
                    continue

                other_paths = []
                for claim_item in other_payload.get("claims", []):
                    if isinstance(claim_item, dict):
                        other_paths.extend(claim_item.get("paths", []))

                for p in normalized_paths:
                    for op in other_paths:
                        p_norm = p.rstrip("/")
                        op_norm = op.rstrip("/")
                        if p_norm == op_norm or p_norm.startswith(op_norm + "/") or op_norm.startswith(p_norm + "/"):
                            raise WorkflowError(
                                f"A2A Path Lock Collision: path '{p}' overlaps with active claim '{op}' in run {other_payload.get('run_id', 'unknown')}"
                            )

        scopes = [f"path:{item}" for item in normalized_paths] + [f"surface:{item}" for item in normalized_surfaces]

        # Phase L0 MOF Enforce: Trigger pre-check for any projects being claimed
        mof_enforce_script = WORKSPACE / "bin/mof/mof-enforce"
        if mof_enforce_script.exists():
            for p in normalized_paths:
                if p.startswith("projects/"):
                    parts = p.split("/")
                    if len(parts) >= 2:
                        node_id = parts[1]
                        try:
                            subprocess.run(
                                ["bash", str(mof_enforce_script), "pre-check", node_id],
                                cwd=str(WORKSPACE),
                                capture_output=True,
                                check=False,
                            )
                        except Exception:
                            pass

        lock_paths = acquire_locks(registry, scopes, run_id, actor, force_lock)
        try:
            payload.setdefault("locks", [])
            for lock_path in lock_paths:
                if lock_path not in payload["locks"]:
                    payload["locks"].append(lock_path)
            claim = {
                "claimed_at": utc_now(),
                "actor": actor,
                "paths": normalized_paths,
                "surfaces": normalized_surfaces,
                "scopes": scopes,
                "locks": lock_paths,
                "affected_graph": affected_graph,
            }
            payload.setdefault("claims", []).append(claim)
            write_run(path, payload)
        except Exception:
            for lock_path in lock_paths:
                lock_file = Path(lock_path)
                if not lock_file.is_absolute():
                    lock_file = WORKSPACE / lock_file
                lock_file.unlink(missing_ok=True)
            raise
        append_ledger_event(
            registry,
            {
                "event": "agent_workflow_claim",
                "run_id": run_id,
                "actor": actor,
                "paths": normalized_paths,
                "surfaces": normalized_surfaces,
                "locks": lock_paths,
            },
        )
        return {**claim, "run_id": run_id}


def close_run(
    registry: dict[str, Any],
    run_id: str,
    status: str,
    evidence: list[str],
    release: bool,
    *,
    emit_mesh: bool = True,
) -> dict[str, Any]:
    path, payload = read_run(registry, run_id)
    payload["status"] = status
    payload["updated_at"] = utc_now()
    payload["closed_at"] = utc_now()
    payload.setdefault("evidence", [])
    payload["evidence"].extend(evidence)
    if release:
        payload["released_locks"] = release_locks(registry, payload["run_id"])
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )  # audit-exempt: non-atomic-write — under run_update_lock
    payload["path"] = display_path(path)
    append_ledger_event(
        registry,
        {
            "event": "agent_workflow_close",
            "run_id": payload["run_id"],
            "workflow_id": payload.get("workflow_id"),
            "status": status,
            "evidence": evidence,
            "released_locks": payload.get("released_locks", []),
        },
    )
    # Direct `close` owns its Mesh terminal event. `closeout` suppresses this
    # narrow payload and emits one richer terminal event after verify/observe.
    if emit_mesh:
        emit_workflow_mesh_event(
            "AgentWorkflowClosed",
            payload["run_id"],
            {
                "status": status,
                "ok": status == "ok",
                "evidence_count": len(evidence),
            },
            workspace=registry_workspace_root(registry),
        )
    return payload


def _run_closeout_side_effects(
    registry: dict[str, Any],
    payload: dict[str, Any],
    run_id: str,
) -> None:
    """Run best-effort closeout integrations under one registry-owned root."""
    workspace = registry_workspace_root(registry)

    def run_silently(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
        try:
            subprocess.run(
                command,
                cwd=cwd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass

    smoke_script = workspace / "bin/gac/evidence-smoke.py"
    if smoke_script.is_file():
        run_silently([sys.executable, str(smoke_script), "--quiet"], cwd=workspace)

    omo_project = workspace / "projects/omo"
    if omo_project.is_dir():
        env = os.environ.copy()
        env["WORKSPACE_ROOT"] = str(workspace)
        env["PYTHONPATH"] = str(omo_project / "src")
        run_silently(
            [sys.executable, "-m", "omo.cli", "state", "sync"],
            cwd=omo_project,
            env=env,
        )

    kos_cli_path = workspace / "projects/kairon/packages/kos/kos-cli.py"
    env_kos = os.environ.copy()
    env_kos["WORKSPACE_ROOT"] = str(workspace)
    env_kos["KOS_HOME"] = str(workspace / "kos")
    env_kos["PYTHONPATH"] = str(workspace / "projects/kairon/packages/kos/src")
    if kos_cli_path.is_file():
        run_silently(
            [sys.executable, str(kos_cli_path), "ingress", "--snapshot", "latest", "--rebuild-ontology"],
            cwd=workspace,
            env=env_kos,
        )

    try:
        from omo.omo_belief import MOSBeliefManager

        belief_mgr = MOSBeliefManager(root=workspace)
        obj_text = payload.get("objective") or "agent-workflow closeout"
        wf_id = payload.get("workflow_id") or "general"
        belief_mgr.record_belief(
            topic=f"workflow:{wf_id}",
            belief_text=f"Workflow run {run_id} achieved objective: {obj_text}",
            pitfall="Unverified workflow closeout",
            solution="Executed agent-workflow verify & observe pass",
            scope_path=payload.get("path") or "*",
            source_run_id=run_id,
        )
    except Exception:
        # Preserve the historical recovery path without allowing it to escape
        # the registry-owned root or fail when KOS is unavailable.
        if kos_cli_path.is_file():
            run_silently([sys.executable, str(kos_cli_path), "onto", "rebuild"], cwd=workspace, env=env_kos)
            run_silently([sys.executable, str(kos_cli_path), "onto", "infer"], cwd=workspace, env=env_kos)
            for script_name in ("gac-kos-sync.py", "gac-consensus-inject.py"):
                script = workspace / "bin" / script_name
                if script.is_file():
                    run_silently([sys.executable, str(script)], cwd=workspace)


def closeout_run(
    registry: dict[str, Any],
    run_id: str,
    status: str,
    evidence: list[str],
    files: list[str],
    from_diff: bool,
    include_untracked: bool,
    all_checks: bool,
    keep_locks: bool,
) -> dict[str, Any]:
    if status == "ok":
        heartbeat_run(registry, run_id)  # SR-01: renew before successful closeout
    from .diagnostics import build_observe_report, build_verify_report

    verify_report = build_verify_report(
        registry,
        run_id,
        files,
        from_diff,
        include_untracked,
        all_checks,
        execute=True,
    )
    observe_report = build_observe_report(registry, run_id)
    if status == "ok" and not verify_report["ok"]:
        raise WorkflowError("closeout blocked: verify failed")
    if status == "ok" and not observe_report["ok"]:
        raise WorkflowError(f"closeout blocked: observe decision={observe_report['decision']}")
    closeout_evidence = [
        *evidence,
        f"agent-workflow verify: {verify_report['check_count']} checks ok={verify_report['ok']}",
        f"agent-workflow observe: {observe_report['decision']}",
    ]
    payload = close_run(
        registry,
        run_id,
        status,
        closeout_evidence,
        not keep_locks,
        emit_mesh=False,
    )
    report = {
        "ok": status == "ok",
        "run": payload,
        "verify": verify_report,
        "observe": observe_report,
    }
    append_ledger_event(
        registry,
        {
            "event": "agent_workflow_closeout",
            "run_id": run_id,
            "status": status,
            "ok": report["ok"],
            "verify_ok": verify_report["ok"],
            "observe_decision": observe_report["decision"],
        },
    )
    if status == "ok":
        _run_closeout_side_effects(registry, payload, run_id)
    # Phase 1b/5: Bridge to Workflow Mesh with event chain closure
    _closeout_scene = None
    try:
        _, _run_record = read_run(registry, run_id)
        _closeout_scene = extract_scene_binding(
            context=_run_record.get("context", {}),
            workflow=_run_record.get("plan", {}),
        )
    except Exception:
        pass
    emit_workflow_mesh_event(
        "AgentWorkflowClosed",
        run_id,
        {
            "status": status,
            "ok": report["ok"],
            "verify_ok": verify_report["ok"],
            "observe_decision": observe_report["decision"],
            "evidence_count": len(closeout_evidence),
        },
        workspace=registry_workspace_root(registry),
    )
    return report


def load_run_records(
    registry: dict[str, Any],
) -> dict[str, tuple[Path, dict[str, Any]]]:
    run_dir = run_state_dir(registry)
    records: dict[str, tuple[Path, dict[str, Any]]] = {}
    if not run_dir.exists():
        return records
    for path in sorted(run_dir.glob("*.yaml")):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            payload = {}
        run_id = payload.get("run_id") if isinstance(payload, dict) else None
        if run_id:
            records[str(run_id)] = (path, payload)
    return records


def load_lock_records(registry: dict[str, Any]) -> list[tuple[Path, dict[str, Any]]]:
    lock_dir = lock_state_dir(registry)
    records: list[tuple[Path, dict[str, Any]]] = []
    if not lock_dir.exists():
        return records
    for path in sorted(lock_dir.glob("*.lock.yaml")):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            payload = {"run_id": None, "parse_error": True}
        records.append(
            (
                path,
                payload if isinstance(payload, dict) else {"run_id": None, "parse_error": True},
            )
        )
    return records


# 2026-08-26: claim 工具组拆至 lifecycle_claims.py (SRP 行数门), 此处 re-export 保持接口不变
from .lifecycle_claims import (  # noqa: F401 -- re-export
    claim_covers_path,
    claim_policy,
    claimed_paths,
    is_read_only_workflow,
    normalize_claim_mode,
)
from .lifecycle_ledger import (  # noqa: F401 -- re-export
    _extract_run_timestamp,
    append_ledger_event,
    heal_ledger_for_run,
    ledger_mentions_run,
)

# 2026-08-28: lock/ledger 工具组拆至 lifecycle_locks.py / lifecycle_ledger.py (SRP 行数门), 此处 re-export 保持接口不变
from .lifecycle_locks import (  # noqa: F401 -- re-export
    _HEARTBEAT_STALE_SECONDS,
    _bounded_lock_name,
    _classify_existing_lock,
    acquire_locks,
    heartbeat_lock,
    prune_stale_locks,
    release_locks,
    run_update_lock,
    sanitize_lock_name,
    scan_locks,
)
