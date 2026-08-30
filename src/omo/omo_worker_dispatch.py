#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from .omo_ingress import archive_done_task, create_audit_report, yield_task_to_planned
from .omo_io import write_text_atomic
from .omo_redaction import redact_sensitive_text
from .omo_task_schema import validate_task_file
from .omo_worker_core import (
    _append_unique,
    _build_launch_argv,
    _find_task_file,
    _find_task_file_safe,
    _load_yaml,
    _omo_path,
    _require_admitted_worker,
    _require_worker_ack_protocol,
    _require_worker_policy,
    _timestamp_slug,
    _utc_now,
    _write_yaml,
)


def _normalised_public_token(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")


def _contains_private_proof_data(value: Any, *, private_proof: str | None) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            token = _normalised_public_token(str(key))
            if "origin_proof" in token and not token.endswith("_digest"):
                return True
            if _contains_private_proof_data(item, private_proof=private_proof):
                return True
        return False
    if isinstance(value, (list, tuple, set)):
        return any(_contains_private_proof_data(item, private_proof=private_proof) for item in value)
    if not isinstance(value, str):
        return False
    if private_proof and private_proof in value:
        return True
    token = _normalised_public_token(value)
    return "origin_proof" in token and not token.endswith("_digest")


def _bridge_dispatch_to_mesh(
    root: Path,
    omo: Path,
    *,
    dispatch_id: str,
    task_id: str,
    worker_id: str,
    workflow_packet: dict[str, Any] | None,
    now: str,
    ack_origin_proof: str,
) -> None:
    """Emit Workflow Mesh events for a worker dispatch.

    When a workflow_packet is provided (Mesh-aware path), emit StepDispatched
    using the packet's admission context. When no packet (legacy path), create
    a minimal admission grant and emit the full chain so the dispatch is
    visible in Mesh.
    """
    from .workflow_mesh import WorkflowMeshStore, new_workflow_event

    if not workflow_packet:
        raise ValueError("unbound legacy dispatch is observer-only and cannot create worker state")

    store = WorkflowMeshStore(omo)

    if workflow_packet:
        run_id = str(workflow_packet.get("workflow_run_id", dispatch_id))
        trace_id = str(workflow_packet.get("trace_id", run_id))
        grant = workflow_packet.get("admission", {})
        admission_id = str(grant.get("admission_id", ""))
        step_run_ids = grant.get("step_run_ids", [f"{run_id}:execute"])
        step_run_id = str(step_run_ids[0]) if step_run_ids else f"{run_id}:execute"
        request_identity = workflow_packet.get("request_identity")
        if not isinstance(request_identity, dict):
            raise ValueError("bound workflow dispatch requires request_identity")

        from .worker_lifecycle import record_step_dispatch

        record_step_dispatch(
            omo,
            workflow_run_id=run_id,
            trace_id=trace_id,
            dispatch_id=dispatch_id,
            worker_id=worker_id,
            step_run_id=step_run_id,
            admission_id=admission_id,
            policy_digest=str(grant.get("policy_digest", "")),
            packet_id=request_identity["packet_id"],
            packet_hash=request_identity["packet_hash"],
            instruction_binding=request_identity["instruction_binding"],
            ack_origin_proof=ack_origin_proof,
        )


def dispatch_task(
    root: Path,
    task_id: str,
    worker_id: str,
    allowed_write_paths: list[str],
    launch: bool = False,
    transport: str = "acp_stdio",  # T1-19: ACP stdio preferred
    prior_evidence: list[str] | None = None,
    prompt_addendum: list[str] | None = None,
    workflow_packet: dict[str, Any] | None = None,
    worker_ack_origin_proof: str | None = None,
    now: str | None = None,
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    omo = _omo_path(root, omo_dir)
    omo_ref = Path(omo_dir)
    task_file = _find_task_file(omo / "tasks" / "active", task_id)
    validation_errors = validate_task_file(task_file)
    if validation_errors:
        raise ValueError("; ".join(validation_errors))
    task = _load_yaml(task_file)
    registry = _load_yaml(omo / "_truth" / "registry" / "workers.yaml")
    public_dispatch_data = {
        "task": task,
        "workflow_packet": workflow_packet,
        "allowed_write_paths": allowed_write_paths,
        "prior_evidence": prior_evidence,
        "prompt_addendum": prompt_addendum,
    }
    if _contains_private_proof_data(public_dispatch_data, private_proof=worker_ack_origin_proof):
        raise ValueError("private proof is forbidden in public dispatch data")
    # Admission is a hard precondition.  Validate before deriving a dispatch
    # id or creating any run/envelope/task/Mesh state so rejection is side-effect free.
    worker = _require_admitted_worker(registry, worker_id, transport)
    _require_worker_policy(
        registry,
        worker,
        task,
        allowed_write_paths=allowed_write_paths,
        workflow_packet=workflow_packet,
    )
    supervision = worker.get("supervision")
    if launch and isinstance(supervision, dict) and supervision.get("controller_direct_start_required") is True:
        raise ValueError(f"worker launch denied: controller direct start is required for worker_id={worker_id}")

    dispatch_now = now or _utc_now()
    request_identity = workflow_packet.get("request_identity") if isinstance(workflow_packet, dict) else None
    workflow_run_id = str(workflow_packet.get("workflow_run_id") or "") if isinstance(workflow_packet, dict) else ""
    exact_request_identity: dict[str, Any] | None = None
    if workflow_packet is not None:
        from .workflow_mesh import WorkflowMeshStore

        exact_snapshot = WorkflowMeshStore(omo).snapshot(workflow_run_id)
        projected_identity = exact_snapshot.get("exact_request_identity")
        if isinstance(projected_identity, dict):
            exact_request_identity = projected_identity
    dispatch_id = (
        str(exact_request_identity.get("dispatch_id") or "")
        if exact_request_identity is not None
        else f"{task_id.lower()}-{worker_id}-{_timestamp_slug(dispatch_now)}"
    )
    if exact_request_identity is not None and not dispatch_id:
        raise ValueError("exact workflow dispatch requires persisted request dispatch_id")
    run_dir = omo / "workers" / "runs"

    # OMO v4.0 Task Gate: Anti-Entropy Mechanism
    debt_dispatch_file = omo / "debt" / "dispatch" / "current.yaml"
    if debt_dispatch_file.exists():
        debt_state = _load_yaml(debt_dispatch_file)
        if debt_state.get("priority") == "P0" and task.get("task_type") != "tech_debt":
            raise ValueError(
                "Task Gate Blocked: Technical debt is P0. You must dispatch a tech_debt task before any new feature tasks."
            )

    # OMO v4.0 Micro-DAG: Workflow Dependency Check
    depends_on = task.get("depends_on", [])
    if depends_on:
        for dep_id in depends_on:
            # Check if dependency is still planned or active
            if _find_task_file_safe(omo / "tasks" / "planned", dep_id) or _find_task_file_safe(
                omo / "tasks" / "active", dep_id
            ):
                raise ValueError(f"Task Gate Blocked: Dependency '{dep_id}' is not yet completed.")

    dispatch_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-dispatch.yaml"
    envelope_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-envelope.yaml"
    prompt_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-prompt.md"
    checkpoint_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-checkpoint.md"
    reclaim_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-reclaim.md"
    review_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-review.md"
    stdout_path = omo_ref / "workers" / "runs" / f"{dispatch_id}-stdout.log"
    if workflow_packet is not None and not isinstance(request_identity, dict):
        raise ValueError("bound workflow dispatch requires request_identity")
    if isinstance(request_identity, dict):
        _require_worker_ack_protocol(registry, worker_id, transport)
    blueprint = (
        {
            "packet_id": request_identity["packet_id"],
            "packet_hash": request_identity["packet_hash"],
            "bet_id": request_identity["bet_id"],
            "instruction_binding": request_identity["instruction_binding"],
        }
        if isinstance(request_identity, dict)
        else None
    )
    control_state = (
        {
            "controller_approval": "granted",
            "transport": "accepted",
            "readiness": "unproven",
            "provider_review": "unknown",
        }
        if blueprint is not None
        else None
    )
    persisted_launch_argv = _build_launch_argv(
        registry,
        worker_id,
        transport,
        f"<prompt:{prompt_path}>",
        workspace_root=root,
        redact_workspace_root=True,
        run_id=workflow_run_id,
        packet_id=request_identity.get("packet_id") if isinstance(request_identity, dict) else None,
        packet_hash=request_identity.get("packet_hash") if isinstance(request_identity, dict) else None,
        instruction_binding=request_identity.get("instruction_binding") if isinstance(request_identity, dict) else None,
    )
    if workflow_packet is None:
        raise ValueError("unbound legacy dispatch is observer-only and cannot create worker state")
    from .worker_lifecycle import new_worker_ack_origin_proof

    ack_origin_proof = (
        worker_ack_origin_proof
        if exact_request_identity is not None
        else worker_ack_origin_proof or new_worker_ack_origin_proof()
    )
    if not ack_origin_proof:
        raise ValueError("worker ACK origin proof is unavailable")
    # A supervised blueprint must not project dispatch artifacts or mutate the
    # Task until StepDispatched is durable.  If this append fails, every file
    # remains exactly at its pre-dispatch state and the exception propagates.
    if blueprint is not None:
        _bridge_dispatch_to_mesh(
            root,
            omo,
            dispatch_id=dispatch_id,
            task_id=task_id,
            worker_id=worker_id,
            workflow_packet=workflow_packet,
            now=dispatch_now,
            ack_origin_proof=ack_origin_proof,
        )
    run_dir.mkdir(parents=True, exist_ok=True)

    source_docs = task.get("source_docs", [])
    deliverables = task.get("deliverables", [])
    allowed_paths = list(allowed_write_paths)
    write_scope = worker.get("write_scope")
    no_worker_writes = isinstance(write_scope, dict) and write_scope.get("mode") == "none"
    write_constraints = (
        [
            "- No repository writes are permitted.",
            "- Return the result on stdout or the governed evidence channel.",
        ]
        if no_worker_writes
        else [
            *(f"- You may write to `{path}`" for path in allowed_paths),
            f"- You may write to `{task_file.relative_to(root)}`",
            f"- You may write to `{review_path}`",
        ]
    )
    deliverable_constraints = (
        [
            *(f"- Expected result reference: `{path}`" for path in deliverables),
            "- The coordinator owns materialization of result references.",
        ]
        if no_worker_writes
        else [
            *(f"- Required deliverable: `{path}`" for path in deliverables),
            "- Updating only the review note is not sufficient when required deliverables are listed.",
            # SR-06 gap-2 (2026-08-16): 空 filesModified 导致 collect 误拒 — 契约层强制
            "- Completion report MUST be a JSON object with a non-empty `filesModified` array",
            "  listing every file you changed (repo-relative); an empty/missing list is treated",
            "  as unproven and the candidate will not be collected.",
        ]
    )
    deliverables_heading = "## Expected output references" if no_worker_writes else "## Required deliverables"
    recovery_lines = list(prompt_addendum or [])
    prompt = "\n".join(
        [
            "# Worker Prompt Contract",
            "",
            f"WORKER_ID: `{worker_id}`",
            f"TASK_ID: `{task_id}`",
            f"TRANSPORT: `{transport}`",
            "READ_BUDGET: `5`",
            "",
            "## Mission",
            "",
            task.get("title", task_id),
            "",
            "## Task SSOT",
            "",
            f"- Task YAML: `{task_file.relative_to(root)}`",
            *(f"- Source doc: `{doc}`" for doc in source_docs),
            "",
            "## Constraints",
            "",
            *write_constraints,
            "- Do not modify global state files.",
            "- Do not mark the task `done`.",
            *(
                "- Workflow Mesh admission: `" + str(workflow_packet.get("admission", {}).get("admission_id")) + "`"
                for _ in [0]
                if workflow_packet
            ),
            "",
            deliverables_heading,
            "",
            *deliverable_constraints,
            *recovery_lines,
        ]
    )
    write_text_atomic(root / prompt_path, prompt + "\n")
    write_text_atomic(
        root / checkpoint_path,
        "# Checkpoint Note\n\n## Last completed step\n\nTBD\n\n## Changed files\n\n- None yet\n",
    )
    write_text_atomic(
        root / reclaim_path,
        "# Reclaim Note\n\n## Reclaim reason\n\nTBD\n\n## Required successor context\n\n- Review the checkpoint note first.\n",
    )
    write_text_atomic(
        root / review_path,
        "# Review Note\n\n## Summary of work done\n\nTBD\n",
    )

    envelope = {
        "version": 1,
        "task_id": task_id,
        "worker_id": worker_id,
        "transport_mode": transport,
        "run_ref": str(dispatch_path),
        "knowledge_refs": source_docs,
        "handoff_refs": [
            str(prompt_path),
            str(checkpoint_path),
            str(review_path),
            str(reclaim_path),
        ],
        "objective": task.get("title", task_id),
        "task_yaml": str(task_file.relative_to(root)),
        "inputs": {
            "source_docs": source_docs,
            "required_context": [str(task_file.relative_to(root))],
            "prior_evidence": list(prior_evidence or []),
            "workflow_mesh": workflow_packet or {},
        },
        "outputs": {
            "required_deliverables": deliverables,
        },
        "scope": {
            "allowed_write_paths": allowed_paths,
            "forbidden_write_paths": [
                ".omo/state/system.yaml",
                ".omo/goals/current.yaml",
                "convergence.yaml",
            ],
            "non_goals": ["Do not modify global state files"],
        },
        "execution_policy": {
            "read_budget": 5,
            "heartbeat_interval_seconds": 300,
            "warning_after_seconds": 900,
            "lease_expired_after_seconds": 1200,
            "reclaim_after_seconds": 1800,
            "checkpoint_required": True,
            "require_partial_output_when_stuck": True,
        },
        "gates": {
            "allowed_operation_level": task.get("allowed_operation_level", "L0"),
            "may_prepare_levels": [],
            "human_approval_required_for": [],
            "approval_ref": task.get("approval_ref"),
            "sensitive_capabilities_blocked": True,
            "workflow_mesh_admission_required": bool(workflow_packet),
        },
        "knowledge_contract": {
            "output_summary_required": True,
            "changed_files_required": True,
            "evidence_required": True,
            "unresolved_risks_required": True,
            "next_handoff_required": True,
        },
        "review": {
            "closeout_owner": "coordinator",
            "worker_may_set_review": True,
            "worker_may_set_done": False,
            "worker_may_set_blocked": False,
        },
    }
    _write_yaml(root / envelope_path, envelope)

    launch_command = " ".join(shlex.quote(argument) for argument in persisted_launch_argv)
    dispatch = {
        "version": 1,
        "dispatch_id": dispatch_id,
        "task_id": task_id,
        "worker_id": worker_id,
        "transport_mode": transport,
        "run_ref": str(dispatch_path),
        "dispatch_state": "dispatched",
        "coordinator": "copilot-cli",
        "launched_at": dispatch_now,
        "lease": {
            "heartbeat_interval_seconds": 300,
            "warning_after_seconds": 900,
            "lease_expired_after_seconds": 1200,
            "reclaim_after_seconds": 1800,
            "last_checkpoint_at": None,
            "last_material_write_at": None,
        },
        "inputs": {
            "task_yaml": str(task_file.relative_to(root)),
            "envelope_file": str(envelope_path),
            "prompt_file": str(prompt_path),
            "source_docs": source_docs,
        },
        "execution": {
            "launch_command": launch_command,
            "approval_ref": task.get("approval_ref"),
            "session_ref": None,
            "log_ref": str(stdout_path),
            "checkpoint_refs": [str(checkpoint_path)],
            "workflow_mesh": workflow_packet or {},
        },
        "handoff": {
            "output_summary_ref": str(review_path),
            "evidence_paths": [],
            "unresolved_risks": [],
            "next_handoff": None,
        },
        "reclaim": {
            "required": False,
            "reason": None,
            "reclaimed_at": None,
            "successor_worker_id": None,
            "successor_dispatch_id": None,
            "note_ref": str(reclaim_path),
        },
        **({"blueprint": blueprint, "control_state": control_state} if blueprint else {}),
    }
    _write_yaml(root / dispatch_path, dispatch)

    task["status"] = "in_progress"
    task["assigned_to"] = worker_id
    task["dispatch_id"] = dispatch_id
    task["run_ref"] = str(dispatch_path)
    task["review_ref"] = str(review_path)
    task["started_at"] = task.get("started_at") or dispatch_now
    task["knowledge_refs"] = _append_unique(task.get("knowledge_refs", []), source_docs)
    task["handoff_refs"] = _append_unique(
        task.get("handoff_refs", []),
        [str(envelope_path), str(prompt_path), str(checkpoint_path)],
    )
    _write_yaml(task_file, task)

    if blueprint is None:
        _bridge_dispatch_to_mesh(
            root,
            omo,
            dispatch_id=dispatch_id,
            task_id=task_id,
            worker_id=worker_id,
            workflow_packet=workflow_packet,
            now=dispatch_now,
            ack_origin_proof=ack_origin_proof,
        )

    if launch:
        prompt_text = (root / prompt_path).read_text(encoding="utf-8")
        argv = _build_launch_argv(
            registry,
            worker_id,
            transport,
            prompt_text,
            workspace_root=root,
            run_id=workflow_run_id,
            packet_id=request_identity.get("packet_id") if isinstance(request_identity, dict) else None,
            packet_hash=request_identity.get("packet_hash") if isinstance(request_identity, dict) else None,
            instruction_binding=request_identity.get("instruction_binding")
            if isinstance(request_identity, dict)
            else None,
        )
        from .workflow_mesh import WorkflowMeshStore, new_workflow_event

        store = WorkflowMeshStore(omo)
        configured_lease_seconds = int((worker.get("lease_policy") or {}).get("lease_expired_after_seconds", 1200))
        lease_seconds = configured_lease_seconds
        if exact_request_identity is not None:
            from .worker_lifecycle import _remaining_exact_admission_seconds

            persisted_admission = store.snapshot(workflow_run_id).get("admission")
            if not isinstance(persisted_admission, dict):
                raise RuntimeError("exact worker launch requires persisted admission")
            remaining_admission_seconds = _remaining_exact_admission_seconds(persisted_admission)
            lease_seconds = min(configured_lease_seconds, int(remaining_admission_seconds))
            if lease_seconds <= 0:
                store.append(
                    new_workflow_event(
                        "StepFailed",
                        workflow_run_id,
                        trace_id=str(workflow_packet.get("trace_id") or workflow_run_id),
                        producer="omo.worker_dispatch",
                        idempotency_key=f"{workflow_run_id}:production-step-failed:{dispatch_id}",
                        payload={
                            "step_run_id": str(workflow_packet["admission"]["step_run_ids"][0]),
                            "step_name": "execute",
                            "admission_id": str(workflow_packet["admission"]["admission_id"]),
                            "dispatch_id": dispatch_id,
                            "worker_id": worker_id,
                            "error": "admission_expired_before_launch",
                        },
                    )
                )
                raise RuntimeError("exact admission expired before worker launch")
        ack_context = {
            "workflow_run_id": workflow_run_id,
            "trace_id": str(workflow_packet.get("trace_id") or workflow_run_id),
            "dispatch_id": dispatch_id,
            "worker_id": worker_id,
            "step_run_id": str(workflow_packet["admission"]["step_run_ids"][0]),
            "admission_id": str(workflow_packet["admission"]["admission_id"]),
            "packet_id": request_identity["packet_id"],
            "packet_hash": request_identity["packet_hash"],
            "instruction_binding": request_identity["instruction_binding"],
            "lease_seconds": lease_seconds,
            "omo_dir": str(omo_ref),
        }
        worker_env = {
            **os.environ,
            "OMO_WORKER_ACK_ORIGIN_PROOF": ack_origin_proof,
            "OMO_WORKER_ACK_CONTEXT_JSON": json.dumps(
                ack_context,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }

        def record_exact_failure(reason: str) -> None:
            if exact_request_identity is None:
                return
            snapshot = store.snapshot(workflow_run_id)
            if snapshot.get("state") not in {"dispatched", "running"}:
                return
            store.append(
                new_workflow_event(
                    "StepFailed",
                    workflow_run_id,
                    trace_id=ack_context["trace_id"],
                    producer="omo.worker_dispatch",
                    idempotency_key=f"{workflow_run_id}:production-step-failed:{dispatch_id}",
                    payload={
                        "step_run_id": ack_context["step_run_id"],
                        "step_name": "execute",
                        "admission_id": ack_context["admission_id"],
                        "dispatch_id": dispatch_id,
                        "worker_id": worker_id,
                        "error": reason,
                    },
                )
            )

        def validated_process_group(process: Any) -> int | None:
            pid = getattr(process, "pid", None)
            if not isinstance(pid, int) or pid <= 1:
                return None
            try:
                os.getpgid(pid)
            except OSError:
                pass
            return pid

        def validated_group_alive(process_group_id: int | None) -> bool:
            if process_group_id is None:
                raise RuntimeError("exact worker process-group identity is unavailable")
            try:
                os.killpg(process_group_id, 0)
            except ProcessLookupError:
                return False
            except OSError:
                return True
            return True

        def reap_spawned_child(process: Any, process_group_id: int | None) -> tuple[str, str]:
            if process_group_id is None:
                raise RuntimeError("exact worker process-group cleanup failed")
            deadline = time.monotonic() + 5.0
            output: tuple[str, str] = ("", "")

            def remaining() -> float:
                return max(0.0, deadline - time.monotonic())

            def bounded_communicate(*, grace_cap: float | None = None) -> tuple[str, str]:
                timeout = remaining()
                if grace_cap is not None:
                    timeout = min(timeout, grace_cap)
                if timeout <= 0:
                    raise subprocess.TimeoutExpired(getattr(process, "args", []), timeout)
                return process.communicate(timeout=timeout)

            term_sent = False
            group_absent = False
            try:
                os.killpg(process_group_id, signal.SIGTERM)
                term_sent = True
            except ProcessLookupError:
                group_absent = True
            except OSError:
                pass

            if term_sent or group_absent:
                try:
                    output = bounded_communicate(grace_cap=1.0)
                except Exception:
                    pass
            if not validated_group_alive(process_group_id):
                return output

            kill_sent = False
            while remaining() > 0:
                try:
                    os.killpg(process_group_id, signal.SIGKILL)
                    kill_sent = True
                    break
                except ProcessLookupError:
                    group_absent = True
                    break
                except OSError:
                    time.sleep(min(0.01, remaining()))

            if kill_sent or group_absent:
                try:
                    output = bounded_communicate()
                except Exception:
                    pass

            while validated_group_alive(process_group_id) and remaining() > 0:
                try:
                    os.killpg(process_group_id, signal.SIGKILL)
                except ProcessLookupError:
                    break
                except OSError:
                    pass
                time.sleep(min(0.01, remaining()))
            if validated_group_alive(process_group_id):
                raise RuntimeError("exact worker process-group cleanup failed")
            return output

        cleanup_attempted = False

        def reap_or_fail_closed(
            process: Any,
            process_group_id: int | None,
            original_error: BaseException,
        ) -> tuple[str, str]:
            nonlocal cleanup_attempted
            cleanup_attempted = True
            try:
                return reap_spawned_child(process, process_group_id)
            except Exception:
                raise RuntimeError("exact worker process-group cleanup failed") from original_error

        def require_durable_ack() -> dict[str, Any]:
            ack_worker = store.worker_snapshot(workflow_run_id)
            if (
                not isinstance(ack_worker, dict)
                or ack_worker.get("ack_decision") != "proceed"
                or ack_worker.get("ack_origin_proof_consumed") is not True
                or ack_worker.get("dispatch_id") != dispatch_id
                or ack_worker.get("worker_id") != worker_id
                or ack_worker.get("step_run_id") != ack_context["step_run_id"]
                or ack_worker.get("admission_id") != ack_context["admission_id"]
            ):
                raise RuntimeError("worker transport returned without a durable proceed ACK")
            return ack_worker

        def run_exact_post_spawn(process: Any, provisional_group_id: int | None) -> tuple[str, str, int, str]:
            process_group_id = provisional_group_id
            stage = "group_derivation"
            try:
                derived_group_id = validated_process_group(process)
                if derived_group_id is None:
                    raise RuntimeError("exact worker process-group identity is unavailable")
                process_group_id = derived_group_id
                stage = "step_started"
                store.append(
                    new_workflow_event(
                        "StepStarted",
                        workflow_run_id,
                        trace_id=ack_context["trace_id"],
                        producer="omo.worker_dispatch",
                        idempotency_key=f"{workflow_run_id}:production-step-started:{dispatch_id}",
                        payload={
                            "step_run_id": ack_context["step_run_id"],
                            "step_name": "execute",
                            "admission_id": ack_context["admission_id"],
                        },
                    )
                )
                stage = "admission_deadline"
                persisted_admission = store.snapshot(workflow_run_id).get("admission")
                if not isinstance(persisted_admission, dict):
                    raise RuntimeError("exact worker wait requires persisted admission")
                remaining_admission_seconds = _remaining_exact_admission_seconds(persisted_admission)
                wait_timeout = min(float(ack_context["lease_seconds"]), remaining_admission_seconds)
                if wait_timeout <= 0:
                    raise RuntimeError("exact admission expired before worker wait")
                stage = "communicate"
                stdout, stderr = process.communicate(timeout=wait_timeout)
                stage = "log_write"
                log_content = redact_sensitive_text((stdout or "") + (stderr or "")).replace(
                    ack_origin_proof, "[REDACTED]"
                )
                write_text_atomic(root / stdout_path, log_content)
                stage = "return_handling"
                returncode = process.returncode
                if returncode != 0:
                    raise RuntimeError(
                        f"worker launch failed: worker_id={worker_id} returncode={returncode} log={stdout_path}"
                    )
                stage = "ack_snapshot"
                require_durable_ack()
                stage = "group_inspection"
                if validated_group_alive(process_group_id):
                    raise RuntimeError("exact worker process group remained live after successful return")
                stage = "authenticated_completion"
                from .worker_lifecycle import record_worker_completion

                result_digest = "sha256:" + hashlib.sha256(log_content.encode("utf-8")).hexdigest()
                record_worker_completion(
                    omo,
                    workflow_run_id=workflow_run_id,
                    trace_id=ack_context["trace_id"],
                    dispatch_id=dispatch_id,
                    worker_id=worker_id,
                    step_run_id=ack_context["step_run_id"],
                    admission_id=ack_context["admission_id"],
                    origin_proof=ack_origin_proof,
                    result_digest=result_digest,
                )
                return stdout, stderr, int(returncode), log_content
            except BaseException as post_spawn_error:
                cleaned_stdout, cleaned_stderr = reap_or_fail_closed(
                    process,
                    process_group_id,
                    post_spawn_error,
                )
                try:
                    record_exact_failure(f"worker_{stage}_failed")
                except Exception:
                    pass
                if isinstance(post_spawn_error, subprocess.TimeoutExpired):
                    timeout_log = redact_sensitive_text((cleaned_stdout or "") + (cleaned_stderr or "")).replace(
                        ack_origin_proof, "[REDACTED]"
                    )
                    write_text_atomic(root / stdout_path, timeout_log)
                    raise RuntimeError(
                        f"worker launch timed out: worker_id={worker_id} "
                        f"timeout={post_spawn_error.timeout} log={stdout_path}"
                    ) from post_spawn_error
                raise

        if exact_request_identity is not None:
            process = None
            provisional_group_id = None
            try:
                process = subprocess.Popen(
                    argv,
                    cwd=root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=worker_env,
                    start_new_session=True,
                )
                spawned_pid = getattr(process, "pid", None)
                provisional_group_id = spawned_pid if isinstance(spawned_pid, int) and spawned_pid > 1 else None
                stdout, stderr, returncode, log_content = run_exact_post_spawn(process, provisional_group_id)
            except BaseException as popen_boundary_error:
                if process is not None and not cleanup_attempted:
                    if provisional_group_id is None:
                        cleanup_pid = getattr(process, "pid", None)
                        provisional_group_id = cleanup_pid if isinstance(cleanup_pid, int) and cleanup_pid > 1 else None
                    reap_or_fail_closed(process, provisional_group_id, popen_boundary_error)
                    try:
                        record_exact_failure("worker_post_popen_boundary_failed")
                    except Exception:
                        pass
                raise
        else:
            result = subprocess.run(argv, cwd=root, capture_output=True, text=True, env=worker_env)
            stdout, stderr = result.stdout, result.stderr
            returncode = result.returncode
            log_content = redact_sensitive_text((stdout or "") + (stderr or "")).replace(ack_origin_proof, "[REDACTED]")
            write_text_atomic(root / stdout_path, log_content)
            if returncode != 0:
                raise RuntimeError(
                    f"worker launch failed: worker_id={worker_id} returncode={returncode} log={stdout_path}"
                )
            require_durable_ack()

        # Phase 28 Step 3: Tri-Plane Bus - Broadcast event to Agora EventBus
        def push_log_to_agora(dispatch_id: str, content: str):
            """Push log synchronization event to Agora via internal Event Bus."""
            try:
                import json
                import os
                import urllib.request

                req = urllib.request.Request(
                    "http://127.0.0.1:7430/api/events",
                    data=json.dumps(
                        {
                            "type": "omo:log_sync",
                            "source": "omo_worker",
                            "payload": {"dispatch_id": dispatch_id, "content": content},
                        }
                    ).encode("utf-8"),
                    method="POST",
                )
                req.add_header("Content-Type", "application/json")

                jwt_secret = os.environ.get("AGORA_JWT_SECRET")
                api_key = os.environ.get("AGORA_API_KEY")
                if jwt_secret:
                    import time

                    import jwt

                    token = jwt.encode(
                        {"role": "system_daemon", "exp": time.time() + 3600},
                        jwt_secret,
                        algorithm="HS256",
                    )
                    req.add_header("Authorization", f"Bearer {token}")
                elif api_key:
                    req.add_header("X-API-Key", api_key)

                # Bypass proxy for 127.0.0.1
                proxy_handler = urllib.request.ProxyHandler({})
                opener = urllib.request.build_opener(proxy_handler)
                opener.open(req, timeout=3.0)
            except Exception as e:  # defensive fallback
                print(f"⚠️ Failed to broadcast log via Tri-Plane Bus: {e}")

        push_log_to_agora(dispatch_id, log_content)
        print(f"✅ Sync'ed {dispatch_id} log via Tri-Plane Bus")

        dispatch["dispatch_state"] = "active"
        dispatch["lease"]["last_material_write_at"] = _utc_now()
        _write_yaml(root / dispatch_path, dispatch)

    return {
        "dispatch_id": dispatch_id,
        "dispatch_path": str(dispatch_path),
        "envelope_path": str(envelope_path),
        "prompt_path": str(prompt_path),
        "checkpoint_path": str(checkpoint_path),
        "reclaim_path": str(reclaim_path),
        "review_path": str(review_path),
        **({"blueprint": blueprint, "control_state": control_state} if blueprint else {}),
    }


def reclaim_task(
    root: Path,
    task_id: str,
    successor_worker_id: str,
    allowed_write_paths: list[str],
    reason: str,
    launch: bool = False,
    transport: str = "acp_stdio",  # T1-19: ACP stdio preferred
    omo_dir: str | Path = ".omo",
) -> dict[str, str]:
    active_dir = _omo_path(root, omo_dir) / "tasks" / "active"
    task_file = _find_task_file(active_dir, task_id)
    task = _load_yaml(task_file)
    run_ref = task.get("run_ref")
    if not run_ref:
        raise ValueError(f"Task has no active run to reclaim: {task_id}")

    prior_dispatch_path = root / run_ref
    prior_dispatch = _load_yaml(prior_dispatch_path)
    checkpoint_refs = list(prior_dispatch.get("execution", {}).get("checkpoint_refs", []))
    reclaim_ref = prior_dispatch.get("reclaim", {}).get("note_ref")
    reclaim_note_path = root / reclaim_ref if reclaim_ref else None

    if reclaim_note_path is not None:
        write_text_atomic(
            reclaim_note_path,
            "\n".join(
                [
                    "# Reclaim Note",
                    "",
                    "## Reclaim reason",
                    "",
                    reason,
                    "",
                    "## Required successor context",
                    "",
                    *(f"- Review checkpoint: `{ref}`" for ref in checkpoint_refs),
                    *(f"- Review reclaim note: `{reclaim_ref}`" for _ in [0] if reclaim_ref),
                    "",
                    "## Successor worker",
                    "",
                    successor_worker_id,
                    "",
                ]
            )
            + "\n",
        )

    prior_dispatch["dispatch_state"] = "reclaimed"
    prior_dispatch["reclaim"]["required"] = True
    prior_dispatch["reclaim"]["reason"] = reason
    prior_dispatch["reclaim"]["reclaimed_at"] = _utc_now()
    prior_dispatch["reclaim"]["successor_worker_id"] = successor_worker_id
    _write_yaml(prior_dispatch_path, prior_dispatch)

    prior_evidence = checkpoint_refs + ([reclaim_ref] if reclaim_ref else [])
    prompt_addendum = [
        "",
        "## Recovery context",
        "",
        f"- Reclaim reason: {reason}",
        *(f"- Resume from checkpoint: `{ref}`" for ref in checkpoint_refs),
        *(f"- Review reclaim handoff: `{reclaim_ref}`" for _ in [0] if reclaim_ref),
        "- Continue from the recorded checkpoint instead of restarting the task.",
    ]
    successor = dispatch_task(
        root,
        task_id=task_id,
        worker_id=successor_worker_id,
        allowed_write_paths=allowed_write_paths,
        launch=launch,
        transport=transport,
        prior_evidence=prior_evidence,
        prompt_addendum=prompt_addendum,
        omo_dir=omo_dir,
    )

    prior_dispatch = _load_yaml(prior_dispatch_path)
    prior_dispatch["reclaim"]["successor_dispatch_id"] = successor["dispatch_id"]
    _write_yaml(prior_dispatch_path, prior_dispatch)
    return successor


def yield_task(root: Path, task_id: str, reason: str, omo_dir: str | Path = ".omo") -> int:
    """[C2G v2] Agent Autonomous Yielding Mechanism"""
    omo_path = _omo_path(root, omo_dir)
    active_dir = omo_path / "tasks" / "active"
    task_file = _find_task_file(active_dir, task_id)
    if not task_file:
        raise ValueError(f"Task {task_id} not found in active tasks.")

    task = _load_yaml(task_file)
    run_ref = task.get("run_ref")
    if run_ref:
        dispatch_path = root / run_ref
        if dispatch_path.exists():
            dispatch = _load_yaml(dispatch_path)
            dispatch["dispatch_state"] = "yielded"
            dispatch["reclaim"] = dispatch.get("reclaim", {})
            dispatch["reclaim"]["required"] = True
            dispatch["reclaim"]["reason"] = f"Yielded to ideation: {reason}"
            _write_yaml(dispatch_path, dispatch)

    yield_task_to_planned(
        omo_path,
        task_id=task_id,
        actor="projects/omo/src/omo/omo_worker_dispatch.py:yield_task",
        reason=reason,
        source_ref=f"omo:worker-dispatch:yield:{task_id}",
    )
    print(f"✅ 战术撤退成功: 任务 {task_id} 已退回沙箱 (candidate)。原因: {reason}")
    return 0


def _worker_gc(root: Path, dry_run: bool = False, retain: int = 50, omo_dir: str | Path = ".omo") -> int:
    """清理旧的 worker dispatch 运行文件。

    Args:
        root: Workspace 根目录
        dry_run: 仅列出拟删除文件，不实际删除
        retain: 保留的最新 dispatch 数目

    Returns:
        0 表示成功，1 表示有错误
    """
    runs_dir = _omo_path(root, omo_dir) / "workers" / "runs"
    if not runs_dir.exists():
        print("No runs directory found at", runs_dir)
        return 0

    # 收集所有 dispatch 文件，按 dispatch_id 中的 timestamp 分组
    dispatch_files: dict[str, list[Path]] = {}
    for f in runs_dir.iterdir():
        if f.is_file():
            # dispatch_id 通常为 dispatch-{task_id}-{timestamp} 格式
            name = f.stem
            # 去掉可能的后缀变体（如 -prompt, -envelope, -review 等后缀）
            name.split(".")[0]
            # 尝试提取 dispatch_id（第一个词和最后一个时间戳之间）
            # 格式举例: dispatch-TASK-1-20260530T161437 → 提取 dispatch-TASK-1-20260530T161437
            # 或者带后缀: dispatch-TASK-1-20260530T161437-prompt → 也属于同一组
            # 简单做法：按文件名前缀（去掉最后一个 - 后缀）分组
            parts = name.rsplit("-", 1)
            if len(parts) > 1 and parts[1] in (
                "prompt",
                "envelope",
                "review",
                "dispatch",
            ):
                group_key = parts[0]
            else:
                group_key = name
            dispatch_files.setdefault(group_key, []).append(f)

    # 按组键名排序（时间戳在键名末尾，排序即按时间）
    sorted_groups = sorted(dispatch_files.keys())

    if len(sorted_groups) <= retain:
        print(f"Total dispatch runs: {len(sorted_groups)} (≤ retain={retain}, nothing to clean)")
        return 0

    to_delete = sorted_groups[:-retain]
    total_files = 0
    for group_key in to_delete:
        files = dispatch_files[group_key]
        total_files += len(files)
        if dry_run:
            print(f"[DRY-RUN] Would delete {len(files)} file(s) for dispatch {group_key}:")
            for f in files:
                print(f"  {f}")
        else:
            for f in files:
                f.unlink()
            print(f"Deleted {len(files)} file(s) for dispatch {group_key}")

    print(f"GC complete: retained {retain} dispatch runs, cleaned {len(to_delete)} old runs ({total_files} files)")

    if not dry_run:
        _fast_track_compaction(root, omo_dir=omo_dir)

    return 0


def _fast_track_compaction(root: Path, omo_dir: str | Path = ".omo"):
    """[C2G v2] Fast-Track 碎片聚变机制
    收集 done 目录下的 FAST-* 任务，聚变成 Markdown 报告，并将原始 yaml 归档。
    """
    omo_path = _omo_path(root, omo_dir)
    done_dir = omo_path / "tasks" / "done"
    audit_dir = omo_path / "_knowledge" / "audits"

    if not done_dir.exists():
        return

    fast_tasks = list(done_dir.glob("FAST-*.yaml"))
    if len(fast_tasks) < 5:
        # 数量不够，暂不聚变
        return

    audit_dir.mkdir(parents=True, exist_ok=True)

    compaction_time = _timestamp_slug(_utc_now())
    report_name = f"Fast-Track-Compaction-{compaction_time}"

    report_lines = [
        f"# 微小价值交付聚变报告 ({compaction_time})",
        "",
        "| Task ID | 标题 | 锚点 | 归档时间 |",
        "|---|---|---|---|",
    ]

    for task_file in fast_tasks:
        try:
            task = _load_yaml(task_file)
            title = task.get("title", "Unknown")
            context_uri = task.get("context_uri", "N/A")
            report_lines.append(f"| {task_file.stem} | {title} | `{context_uri}` | {_utc_now()} |")

            archive_done_task(
                omo_path,
                task_id=task_file.stem,
                actor="projects/omo/src/omo/omo_worker_dispatch.py:_fast_track_compaction",
                source_ref=f"omo:worker-dispatch:fast-track-compaction:{task_file.stem}",
            )
        except Exception as e:  # defensive fallback
            print(f"Failed to compact {task_file}: {e}")

    if len(report_lines) > 4:
        create_audit_report(
            omo_path,
            filename=report_name,
            title=f"微小价值交付聚变报告 ({compaction_time})",
            content="\n".join(report_lines[2:]),
            actor="projects/omo/src/omo/omo_worker_dispatch.py:_fast_track_compaction",
            source_ref="omo:worker-dispatch:fast-track-compaction",
        )
        print(f"✅ Fast-Track 微观碎片已聚变: 归档了 {len(fast_tasks)} 个任务，生成报告 {report_name}.md")
