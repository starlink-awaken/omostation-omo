"""Workflow Mesh admission and worker dispatch bridge.

This module owns the control-plane decision only. It does not execute a worker
or call a backend. A successful result is a signed admission grant plus an
immutable dispatch packet recorded in OMO's append-only event log.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from .omo_shared import load_yaml
from .omo_task_schema import validate_task_file
from .orchestration_contract import (
    OrchestrationContractError,
    validate_capability_requirements,
)
from .workflow_dispatch_errors import WorkflowDispatchError

# 2026-08-29: _approval_state and _validated_request_identity extracted to workflow_dispatch_helpers.py
from .workflow_dispatch_helpers import (
    _approval_state,
    _build_admission_grant,
    _canonical,
    _proof,
    _validated_request_identity,
    renew_admission,
)
from .workflow_mesh import WorkflowMeshStore, new_workflow_event

_AGENT_WORKFLOW_BINDING_FIELDS = (
    "correlation_id",
    "workflow_run_id",
    "packet_id",
    "packet_hash",
    "assignment_id",
    "dispatch_id",
    "actor_id",
    "delivery_attempt_id",
)
_SHA256_REF_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def _parse_health(health: dict[str, Any], required: list[str]) -> dict[str, Any]:
    if not isinstance(health, dict):
        raise WorkflowDispatchError("capability health snapshot is required")
    status = str(health.get("status", "unavailable"))
    capabilities = health.get("capabilities")
    if not isinstance(capabilities, dict):
        raise WorkflowDispatchError("capability health snapshot has no capabilities")
    unavailable = []
    for capability in required:
        item = capabilities.get(capability)
        if not isinstance(item, dict) or not item.get("available", False):
            unavailable.append(capability)
    if unavailable:
        raise WorkflowDispatchError("required capabilities unavailable: " + ", ".join(unavailable))
    if status == "unhealthy":
        raise WorkflowDispatchError("capability health is unhealthy")
    return {
        "status": status,
        "capabilities": {capability: capabilities[capability] for capability in required},
        "observed_at": health.get("observed_at"),
        "source": health.get("source", "agora"),
        "snapshot_digest": hashlib.sha256(_canonical(health)).hexdigest(),
    }


def _requested_event(store: WorkflowMeshStore, workflow_run_id: str) -> dict[str, Any]:
    for event in store.events():
        if str(event.get("workflow_run_id")) == workflow_run_id and event.get("event_type") == "WorkflowRequested":
            return event
    raise WorkflowDispatchError(f"workflow request not found: {workflow_run_id}")


def _task_file_for_request(
    root: Path,
    task_id: str,
    *,
    groups: tuple[str, ...],
    omo_dir: str | Path,
) -> tuple[Path, dict[str, Any]]:
    omo = root / Path(omo_dir)
    for group in groups:
        for path in (omo / "tasks" / group).glob("*.yaml"):
            try:
                payload = load_yaml(path)
            except (OSError, ValueError):
                continue
            if payload.get("id") == task_id:
                return path, payload
    raise WorkflowDispatchError(f"task not found for workflow request: {task_id}")


def _request_context(
    root: Path,
    workflow_run_id: str,
    *,
    omo_dir: str | Path,
) -> tuple[WorkflowMeshStore, dict[str, Any], dict[str, Any]]:
    store = WorkflowMeshStore(root / Path(omo_dir))
    event = _requested_event(store, workflow_run_id)
    snapshot = store.snapshot(workflow_run_id)
    if snapshot.get("state") == "closed":
        raise WorkflowDispatchError("workflow request is already closed")
    payload = event.get("payload")
    if not isinstance(payload, dict) or not payload.get("task_id"):
        raise WorkflowDispatchError("workflow request has no task binding")
    return store, event, snapshot


def _validate_admission_inputs(
    *,
    backend: str,
    required_capabilities: list[str],
    requested_budget: float,
    remaining_budget: float | None,
) -> list[str]:
    if not str(backend).strip():
        raise WorkflowDispatchError("backend is required")
    required = list(dict.fromkeys(str(item).strip() for item in required_capabilities))
    if not required or any(not item for item in required):
        raise WorkflowDispatchError("required capabilities must not be empty")
    if requested_budget < 0:
        raise WorkflowDispatchError("requested budget must be non-negative")
    if remaining_budget is not None and requested_budget > remaining_budget:
        raise WorkflowDispatchError("insufficient execution budget")
    return required


def admit_agent_workflow_start(
    root: Path,
    *,
    record: Mapping[str, Any],
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    """Persist one exact, non-executing admission for an Agent Workflow start."""
    run_id = str(record.get("run_id") or "")
    store = WorkflowMeshStore(root / Path(omo_dir))
    snapshot = store.snapshot(run_id)
    if snapshot.get("state") != "planned":
        raise WorkflowDispatchError(f"agent workflow admission requires planned state: {snapshot.get('state')}")

    work_packet = record.get("work_packet")
    if not isinstance(work_packet, Mapping):
        raise WorkflowDispatchError("agent workflow admission requires a WorkPacket")
    try:
        requirements = validate_capability_requirements(work_packet.get("capability_requirements"))
    except OrchestrationContractError as exc:
        raise WorkflowDispatchError("agent workflow capability requirements are invalid") from exc
    if requirements != work_packet.get("capability_requirements"):
        raise WorkflowDispatchError("agent workflow capability requirements are not canonical")

    requirements_digest = record.get("capability_requirements_digest")
    expected_requirements_digest = "sha256:" + hashlib.sha256(_canonical(requirements)).hexdigest()  # type: ignore[arg-type]
    if requirements_digest != expected_requirements_digest:
        raise WorkflowDispatchError("agent workflow capability requirements digest mismatch")

    preflight = record.get("capability_preflight")
    if not isinstance(preflight, Mapping) or set(preflight) != {
        "requirements_digest",
        "binding",
        "receipts",
        "invoked",
        "value_indicator_policy",
    }:
        raise WorkflowDispatchError("agent workflow capability preflight is invalid")
    if preflight.get("requirements_digest") != requirements_digest:
        raise WorkflowDispatchError("agent workflow preflight requirements digest mismatch")

    receipts = preflight.get("receipts")
    if not isinstance(receipts, list) or len(receipts) != len(requirements):
        raise WorkflowDispatchError("agent workflow preflight receipts are invalid")
    source_receipt_digests: list[str] = []
    for requirement, receipt in zip(requirements, receipts):
        if (
            not isinstance(receipt, Mapping)
            or set(receipt) != {"capability_id", "source_digest", "receipt_digest"}
            or receipt.get("capability_id") != requirement["capability_id"]
            or not isinstance(receipt.get("source_digest"), str)
            or _SHA256_REF_RE.fullmatch(receipt["source_digest"]) is None
            or not isinstance(receipt.get("receipt_digest"), str)
            or _SHA256_REF_RE.fullmatch(receipt["receipt_digest"]) is None
        ):
            raise WorkflowDispatchError("agent workflow preflight receipt order or digest is invalid")
        source_receipt_digests.append(receipt["source_digest"])

    binding = preflight.get("binding")
    if not isinstance(binding, Mapping) or set(binding) != set(_AGENT_WORKFLOW_BINDING_FIELDS):
        raise WorkflowDispatchError("agent workflow preflight binding is invalid")
    if any(not isinstance(binding.get(field), str) or not binding[field] for field in _AGENT_WORKFLOW_BINDING_FIELDS):
        raise WorkflowDispatchError("agent workflow preflight binding fields must be non-empty")
    packet_id = work_packet.get("packet_id")
    packet_hash = record.get("work_packet_hash")
    if (
        binding["correlation_id"] != run_id
        or binding["workflow_run_id"] != run_id
        or binding["packet_id"] != packet_id
        or binding["packet_hash"] != packet_hash
        or not isinstance(packet_id, str)
        or not packet_id
        or not isinstance(packet_hash, str)
        or _SHA256_REF_RE.fullmatch(packet_hash) is None
    ):
        raise WorkflowDispatchError("agent workflow preflight binding mismatch")
    if preflight.get("invoked") is not False or preflight.get("value_indicator_policy") is not False:
        raise WorkflowDispatchError("agent workflow preflight must be non-executing and non-valuing")

    request_identity = {
        field: binding[field]
        for field in _AGENT_WORKFLOW_BINDING_FIELDS
    }
    request_identity.update(
        {
            "capability_requirements": requirements,
            "capability_requirements_digest": requirements_digest,
        }
    )
    requested = _requested_event(store, run_id)
    requested_payload = requested.get("payload")
    if not isinstance(requested_payload, Mapping) or requested_payload.get("request_identity") != request_identity:
        raise WorkflowDispatchError("persisted agent workflow request identity mismatch")
    if requested_payload.get("workflow_id") != record.get("workflow_id"):
        raise WorkflowDispatchError("persisted agent workflow request workflow mismatch")

    policy = {
        "workflow_id": record.get("workflow_id"),
        "workflow_run_id": run_id,
        "packet_id": packet_id,
        "packet_hash": packet_hash,
        "capability_requirements": requirements,
        "capability_requirements_digest": requirements_digest,
        "actor_id": binding["actor_id"],
        "delivery_attempt_id": binding["delivery_attempt_id"],
        "source_receipt_digests": source_receipt_digests,
        "requested_budget": 0.0,
    }
    issued_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    grant = {
        "admission_id": f"admit-{uuid4().hex}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "agent-workflow",
        "step_run_ids": [f"{run_id}:execute"],
        "capabilities": [requirement["capability_id"] for requirement in requirements],
        "policy_digest": hashlib.sha256(_canonical(policy)).hexdigest(),
        "issued_at": issued_at,
        "expires_at": (datetime.fromisoformat(issued_at) + timedelta(hours=1)).isoformat(),
        "request_identity": request_identity,
    }
    grant["proof"] = _proof(grant)
    admitted = store.append(
        new_workflow_event(
            "WorkflowAdmitted",
            run_id,
            trace_id=run_id,
            producer="omo.workflow_dispatch",
            idempotency_key=f"{run_id}:admitted",
            payload={
                "admission": grant,
                "policy": policy,
                "policy_digest": grant["policy_digest"],
                "proof": grant["proof"],
                "request_identity": request_identity,
                "external_side_effects": "disabled",
            },
        )
    )
    persisted = store.snapshot(run_id)
    persisted_grant = persisted.get("admission")
    if (
        persisted.get("state") != "admitted"
        or not isinstance(persisted_grant, Mapping)
        or any(
            persisted_grant.get(field) != grant[field]
            for field in ("admission_id", "policy_digest", "proof", "request_identity")
        )
    ):
        raise WorkflowDispatchError("persisted agent workflow admission re-read mismatch")
    return {
        "status": "admitted",
        "dispatch_state": "admitted",
        "workflow_run_id": run_id,
        "admission": dict(persisted_grant),
        "event": admitted,
        "external_side_effects": "disabled",
        "worker_launch": False,
    }


def close_agent_workflow_run(
    root: Path,
    *,
    workflow_run_id: str,
    status: str,
    payload: Mapping[str, Any],
    omo_dir: str | Path = ".omo",
) -> bool:
    """Close an exact Agent Workflow without inventing execution events."""
    store = WorkflowMeshStore(root / Path(omo_dir))
    snapshot = store.snapshot(workflow_run_id)
    exact_request_identity = snapshot.get("exact_request_identity")
    admission = snapshot.get("admission")
    if not isinstance(exact_request_identity, Mapping):
        return False

    close_payload = {"agent_event_type": "AgentWorkflowClosed", **dict(payload)}
    success_close = bool(payload.get("ok")) or status in {"ok", "succeeded", "verified", "merged"}
    if not isinstance(admission, Mapping):
        if success_close:
            raise WorkflowDispatchError(
                "EXACT_WORKFLOW_ADMISSION_NOT_PERSISTED: exact request has no persisted admission"
            )
        if snapshot.get("state") != "planned":
            raise WorkflowDispatchError(
                f"exact Agent Workflow without admission cannot close from state {snapshot.get('state')}"
            )
        store.append(
            new_workflow_event(
                "WorkflowCancelled",
                workflow_run_id,
                trace_id=workflow_run_id,
                producer="omo.workflow_dispatch",
                idempotency_key=f"{workflow_run_id}:exact-closeout:terminal",
                payload=close_payload,
            )
        )
        store.append(
            new_workflow_event(
                "WorkflowClosed",
                workflow_run_id,
                trace_id=workflow_run_id,
                producer="omo.workflow_dispatch",
                idempotency_key=f"{workflow_run_id}:exact-closeout:closed",
                payload=close_payload,
            )
        )
        if store.snapshot(workflow_run_id).get("state") != "closed":
            raise WorkflowDispatchError("exact Agent Workflow request-only close did not reach closed")
        return True

    request_identity = admission.get("request_identity")
    step_run_ids = admission.get("step_run_ids")
    if (
        not isinstance(admission, Mapping)
        or not isinstance(request_identity, Mapping)
        or request_identity != exact_request_identity
        or not isinstance(step_run_ids, list)
        or not step_run_ids
        or not isinstance(step_run_ids[0], str)
        or not step_run_ids[0]
    ):
        raise WorkflowDispatchError("exact Agent Workflow closeout requires persisted delivery identity")
    admission_id = str(admission.get("admission_id") or "")
    admission_proof = str(admission.get("proof") or "")
    step_run_id = step_run_ids[0]

    state = snapshot.get("state")
    if success_close and state not in {"succeeded", "verified", "merged", "closed"}:
        raise WorkflowDispatchError(
            f"EXACT_WORKFLOW_EXECUTION_NOT_PERSISTED: cannot close exact run from state {state}"
        )
    if success_close and not isinstance(snapshot.get("worker_completion_receipt"), Mapping):
        raise WorkflowDispatchError(
            "EXACT_WORKFLOW_COMPLETION_RECEIPT_NOT_PERSISTED: exact success has no worker completion receipt"
        )
    if not success_close and state not in {"failed", "unavailable", "cancelled", "closed"}:
        if state == "running":
            terminal_type = "StepFailed"
            terminal_payload = {
                **close_payload,
                "step_run_id": step_run_id,
                "step_name": "execute",
                "admission_id": admission_id,
                "error": str(payload.get("error") or "workflow failed"),
            }
        elif state in {"admitted", "dispatched"}:
            terminal_type = "WorkflowCancelled"
            terminal_payload = close_payload
        else:
            raise WorkflowDispatchError(f"exact Agent Workflow cannot close honestly from state {state}")
        store.append(
            new_workflow_event(
                terminal_type,
                workflow_run_id,
                trace_id=workflow_run_id,
                producer="omo.workflow_dispatch",
                idempotency_key=f"{workflow_run_id}:exact-closeout:terminal",
                payload=terminal_payload,
            )
        )
        snapshot = store.snapshot(workflow_run_id)

    if snapshot.get("state") != "closed":
        store.append(
            new_workflow_event(
                "WorkflowClosed",
                workflow_run_id,
                trace_id=workflow_run_id,
                producer="omo.workflow_dispatch",
                idempotency_key=f"{workflow_run_id}:exact-closeout:closed",
                payload=close_payload,
            )
        )

    persisted = store.snapshot(workflow_run_id)
    persisted_admission = persisted.get("admission")
    admissions = [
        event
        for event in store.events()
        if event.get("workflow_run_id") == workflow_run_id and event.get("event_type") == "WorkflowAdmitted"
    ]
    if (
        persisted.get("state") != "closed"
        or not isinstance(persisted_admission, Mapping)
        or persisted_admission.get("admission_id") != admission_id
        or persisted_admission.get("proof") != admission_proof
        or len(admissions) != 1
    ):
        raise WorkflowDispatchError("exact Agent Workflow closeout did not preserve its persisted admission")
    return True


def _check_scene_binding(event: dict[str, Any], scene_binding: Mapping[str, Any] | None) -> None:
    if scene_binding is None:
        return
    payload = event.get("payload")
    recorded = payload.get("scene_binding") if isinstance(payload, dict) else None
    if dict(scene_binding) != recorded:
        raise WorkflowDispatchError("scene binding does not match workflow request")


def preview_requested_workflow(
    root: Path,
    *,
    workflow_run_id: str,
    backend: str,
    required_capabilities: list[str],
    capability_health: dict[str, Any],
    requested_budget: float = 0.0,
    remaining_budget: float | None = None,
    scene_binding: Mapping[str, Any] | None = None,
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    """Evaluate admission gates for an existing request without writing state."""
    _store, event, snapshot = _request_context(root, workflow_run_id, omo_dir=omo_dir)
    _check_scene_binding(event, scene_binding)
    if snapshot.get("state") == "admitted":
        return {
            "status": "deduplicated",
            "dispatch_state": "admitted",
            "workflow_run_id": workflow_run_id,
            "external_side_effects": "disabled",
            "worker_launch": False,
            "admission": snapshot.get("admission"),
        }
    if snapshot.get("state") != "planned":
        return {
            "status": "blocked",
            "blocker": "workflow request is not awaiting admission",
            "workflow_run_id": workflow_run_id,
            "current_state": snapshot.get("state"),
            "external_side_effects": "disabled",
            "worker_launch": False,
        }

    required = _validate_admission_inputs(
        backend=backend,
        required_capabilities=required_capabilities,
        requested_budget=requested_budget,
        remaining_budget=remaining_budget,
    )
    task_id = str(event["payload"]["task_id"])
    task_file, task = _task_file_for_request(root, task_id, groups=("active", "planned"), omo_dir=omo_dir)
    validation_errors = validate_task_file(task_file)
    if validation_errors:
        raise WorkflowDispatchError("; ".join(validation_errors))
    request_task_ref = str(event.get("payload", {}).get("task_ref") or "")
    try:
        approval = _approval_state(
            root,
            task,
            task_file,
            accepted_task_refs={request_task_ref} if request_task_ref else set(),
        )
        health = _parse_health(capability_health, required)
    except WorkflowDispatchError as exc:
        return {
            "status": "blocked",
            "blocker": str(exc),
            "workflow_run_id": workflow_run_id,
            "task_id": task_id,
            "task_group": task_file.parent.name,
            "current_state": "planned",
            "external_side_effects": "disabled",
            "worker_launch": False,
        }
    return {
        "status": "eligible",
        "dispatch_state": "preview",
        "workflow_run_id": workflow_run_id,
        "trace_id": event.get("trace_id", workflow_run_id),
        "task_id": task_id,
        "task_group": task_file.parent.name,
        "backend": backend,
        "required_capabilities": required,
        "approval": approval,
        "capability_health": health,
        "requested_budget": requested_budget,
        "scene_binding": event.get("payload", {}).get("scene_binding"),
        "external_side_effects": "disabled",
        "worker_launch": False,
    }


def admit_requested_workflow(
    root: Path,
    *,
    workflow_run_id: str,
    backend: str,
    required_capabilities: list[str],
    capability_health: dict[str, Any],
    requested_budget: float = 0.0,
    remaining_budget: float | None = None,
    ttl_seconds: int = 900,
    now: str | None = None,
    scene_binding: Mapping[str, Any] | None = None,
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    """Admit only an existing request; never create an implicit request."""
    store, event, snapshot = _request_context(root, workflow_run_id, omo_dir=omo_dir)
    _check_scene_binding(event, scene_binding)
    if snapshot.get("state") == "admitted":
        return {
            "status": "deduplicated",
            "dispatch_state": "admitted",
            "workflow_run_id": workflow_run_id,
            "trace_id": snapshot.get("trace_id", workflow_run_id),
            "task_id": event.get("payload", {}).get("task_id"),
            "admission": snapshot.get("admission"),
            "scene_binding": snapshot.get("scene_binding"),
            "external_side_effects": "disabled",
            "worker_launch": False,
        }
    if snapshot.get("state") != "planned":
        raise WorkflowDispatchError(f"workflow request cannot be admitted from state: {snapshot.get('state')}")
    required = _validate_admission_inputs(
        backend=backend,
        required_capabilities=required_capabilities,
        requested_budget=requested_budget,
        remaining_budget=remaining_budget,
    )
    task_id = str(event["payload"]["task_id"])
    try:
        task_file, task = _task_file_for_request(root, task_id, groups=("active",), omo_dir=omo_dir)
    except WorkflowDispatchError as exc:
        try:
            _task_file_for_request(root, task_id, groups=("planned",), omo_dir=omo_dir)
        except WorkflowDispatchError:
            raise exc
        raise WorkflowDispatchError("active task is required before admission") from exc
    validation_errors = validate_task_file(task_file)
    if validation_errors:
        raise WorkflowDispatchError("; ".join(validation_errors))
    request_task_ref = str(event.get("payload", {}).get("task_ref") or "")
    approval = _approval_state(
        root,
        task,
        task_file,
        accepted_task_refs={request_task_ref} if request_task_ref else set(),
        now=now,
    )
    health = _parse_health(capability_health, required)
    trace_id = str(event.get("trace_id") or workflow_run_id)
    grant = _build_admission_grant(
        workflow_run_id=workflow_run_id,
        trace_id=trace_id,
        task_id=task_id,
        backend=backend,
        required_capabilities=required,
        approval=approval,
        health=health,
        requested_budget=requested_budget,
        ttl_seconds=ttl_seconds,
        now=now,
        request_identity={
            key: event["payload"][key]
            for key in (
                "bet_id",
                "packet_id",
                "packet_hash",
                "task_ref",
                "instruction_binding",
                "capability_requirements",
                "capability_requirements_digest",
            )
            if key in event["payload"]
        }
        or None,
    )
    admitted = store.append(
        new_workflow_event(
            "WorkflowAdmitted",
            workflow_run_id,
            trace_id=trace_id,
            producer="omo.workflow_dispatch",
            idempotency_key=f"{workflow_run_id}:admitted",
            payload={
                "admission": grant,
                **grant,
                "task_id": task_id,
                "backend": backend,
                "required_capabilities": required,
                "capability_health": health,
                "requested_budget": requested_budget,
                "external_side_effects": "disabled",
            },
        )
    )
    return {
        "status": "admitted",
        "dispatch_state": "admitted",
        "workflow_run_id": workflow_run_id,
        "trace_id": trace_id,
        "task_id": task_id,
        "backend": backend,
        "admission": grant,
        "approval": approval,
        "capability_health": health,
        "scene_binding": event.get("payload", {}).get("scene_binding"),
        "event": {
            "event_id": admitted["event_id"],
            "event_type": admitted["event_type"],
            "idempotency_key": admitted["idempotency_key"],
        },
        "external_side_effects": "disabled",
        "worker_launch": False,
    }


def admit_workflow(
    root: Path,
    *,
    task_id: str,
    backend: str,
    required_capabilities: list[str],
    capability_health: dict[str, Any],
    workflow_run_id: str | None = None,
    trace_id: str | None = None,
    requested_budget: float = 0.0,
    remaining_budget: float | None = None,
    ttl_seconds: int = 900,
    now: str | None = None,
    scene_binding: Mapping[str, Any] | None = None,
    request_identity: Mapping[str, Any] | None = None,
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    """Validate gates, append request/admission events, and return a packet."""
    omo = root / Path(omo_dir)
    task_file = next(
        (path for path in (omo / "tasks" / "active").glob("*.yaml") if load_yaml(path).get("id") == task_id),
        None,
    )
    if task_file is None:
        raise WorkflowDispatchError(f"active task not found: {task_id}")
    validation_errors = validate_task_file(task_file)
    if validation_errors:
        raise WorkflowDispatchError("; ".join(validation_errors))
    task = load_yaml(task_file)
    identity = _validated_request_identity(
        request_identity,
        task_id=task_id,
        task_file=task_file,
        root=root,
    )
    planned_task_ref = str((task_file.parent.parent / "planned" / task_file.name).relative_to(root))
    approval = _approval_state(
        root,
        task,
        task_file,
        accepted_task_refs={planned_task_ref},
        now=now,
    )
    health = _parse_health(capability_health, list(dict.fromkeys(required_capabilities)))
    if requested_budget < 0:
        raise WorkflowDispatchError("requested budget must be non-negative")
    if remaining_budget is not None and requested_budget > remaining_budget:
        raise WorkflowDispatchError("insufficient execution budget")

    issued_at = now or datetime.now(UTC).replace(microsecond=0).isoformat()
    run_id = workflow_run_id or f"mesh-{task_id.lower()}-{uuid4().hex[:12]}"
    trace = trace_id or run_id
    step_run_ids = [f"{run_id}:execute"]
    expires_at = (datetime.fromisoformat(issued_at) + timedelta(seconds=ttl_seconds)).isoformat()
    policy = {
        "task_id": task_id,
        "backend": backend,
        "required_capabilities": list(dict.fromkeys(required_capabilities)),
        "approval": approval,
        "health": health["snapshot_digest"],
        "requested_budget": requested_budget,
    }
    grant = {
        "admission_id": f"admit-{uuid4().hex}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": trace,
        "backend": backend,
        "step_run_ids": step_run_ids,
        "capabilities": list(dict.fromkeys(required_capabilities)),
        "policy_digest": hashlib.sha256(_canonical(policy)).hexdigest(),
        "issued_at": issued_at,
        "expires_at": expires_at,
        **({"request_identity": identity} if identity else {}),
    }
    grant["proof"] = _proof(grant)

    store = WorkflowMeshStore(omo)
    store.append(
        new_workflow_event(
            "WorkflowRequested",
            run_id,
            trace_id=trace,
            producer="omo.workflow_dispatch",
            idempotency_key=f"{run_id}:requested",
            payload={
                "task_id": task_id,
                "backend": backend,
                "required_capabilities": grant["capabilities"],
                "approval": approval,
                "health": health,
                "requested_budget": requested_budget,
                **identity,
            },
            scene_binding=scene_binding,
        )
    )
    store.append(
        new_workflow_event(
            "WorkflowAdmitted",
            run_id,
            trace_id=trace,
            producer="omo.workflow_dispatch",
            idempotency_key=f"{run_id}:admitted",
            payload={"admission": grant, **grant, "task_id": task_id, **identity},
        )
    )
    return {
        "workflow_run_id": run_id,
        "trace_id": trace,
        "task_id": task_id,
        "backend": backend,
        "admission": grant,
        "approval": approval,
        "capability_health": health,
        "scene_binding": dict(scene_binding) if scene_binding is not None else None,
        "dispatch_state": "admitted",
        "request_identity": identity or None,
    }


def _dispatch_iris_via_executor(
    root: Path,
    packet: dict[str, Any],
    iris_caps: list[str],
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    """P0 完整第一块: iris capability → mesh-iris-executor 快速路径.

    capability_refs 含 ``iris:xxx`` → subprocess 调 mesh-iris-executor (新 run_id, 自 seed).
    admission gate (admit_workflow) 已验证 iris capability 可用; executor 执行
    list_items + record receipt (mesh 6 事件链 + EvidenceRecorded).
    """
    import subprocess

    executor = root / "bin" / "ssot" / "mesh-iris-executor.py"
    if not executor.exists():
        raise WorkflowDispatchError(f"mesh-iris-executor not found: {executor}")

    admission = packet.get("admission") or {}
    run_id = packet.get("workflow_run_id") or admission.get("workflow_run_id")
    admission_id = admission.get("admission_id")
    results: list[dict[str, Any]] = []
    for cap in iris_caps:
        connector = cap[len("iris:") :]
        argv = [
            "python3",
            str(executor),
            "--connector",
            connector,
            "--omo-dir",
            str(omo_dir),
        ]
        # P0 第二块: 复用 packet (mesh 状态机一致性, 避免 packet run + executor 自 seed run)
        if run_id and admission_id:
            argv += [
                "--skip-seed",
                "--run-id",
                str(run_id),
                "--admission-id",
                str(admission_id),
            ]
        proc = subprocess.run(
            argv,
            cwd=root,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        results.append(
            {
                "capability": cap,
                "connector": connector,
                "returncode": proc.returncode,
                "tail": (proc.stdout or "")[-400:],
                "stderr_tail": (proc.stderr or "")[-200:],
            }
        )
    all_ok = all(r["returncode"] == 0 for r in results)
    return {
        **packet,
        "iris_dispatch": results,
        "dispatch_state": "dispatched" if all_ok else "failed",
    }


def dispatch_admitted_workflow(
    root: Path,
    *,
    task_id: str,
    worker_id: str,
    allowed_write_paths: list[str],
    backend: str,
    required_capabilities: list[str],
    capability_health: dict[str, Any],
    launch: bool = False,
    transport: str = "acp_stdio",
    **admission_options: Any,
) -> dict[str, Any]:
    """Admit first, then hand the immutable packet to the legacy worker bridge."""
    from .omo_worker_core import _build_launch_argv, _require_worker_ack_protocol

    request_identity = admission_options.get("request_identity")
    if not isinstance(request_identity, Mapping):
        raise WorkflowDispatchError("bound worker dispatch requires request identity")
    workflow_run_id = str(
        admission_options.setdefault(
            "workflow_run_id",
            f"mesh-{task_id.lower()}-{uuid4().hex[:12]}",
        )
    )
    registry = load_yaml(root / Path(admission_options.get("omo_dir", ".omo")) / "_truth" / "registry" / "workers.yaml")
    _require_worker_ack_protocol(registry, worker_id, transport)
    _build_launch_argv(
        registry,
        worker_id,
        transport,
        "",
        workspace_root=root,
        run_id=workflow_run_id,
        packet_id=request_identity.get("packet_id"),
        packet_hash=request_identity.get("packet_hash"),
        instruction_binding=request_identity.get("instruction_binding"),
    )
    packet = admit_workflow(
        root,
        task_id=task_id,
        backend=backend,
        required_capabilities=required_capabilities,
        capability_health=capability_health,
        **admission_options,
    )
    # P0 完整第一块: iris capability → mesh-iris-executor 快速路径 (不 launch agent).
    # admission gate 已验证 iris capability 可用, 直接调 executor 执行 + record receipt.
    iris_caps = [c for c in required_capabilities if str(c).startswith("iris:")]
    if iris_caps:
        return _dispatch_iris_via_executor(root, packet, iris_caps)

    from .omo_worker_dispatch import dispatch_task

    worker_dispatch = dispatch_task(
        root,
        task_id=task_id,
        worker_id=worker_id,
        allowed_write_paths=allowed_write_paths,
        launch=launch,
        transport=transport,
        workflow_packet=packet,
    )
    return {
        **packet,
        "worker_dispatch": worker_dispatch,
        "dispatch_state": "dispatched",
    }


def consume_pending_workflow_requests(
    root: Path,
    *,
    capability_health: dict[str, Any],
    backend: str = "iris-executor",
    worker_id: str = "omo-daemon-consumer",
    allowed_write_paths: list[str] | None = None,
    omo_dir: str | Path = ".omo",
    max_per_tick: int = 10,
) -> dict[str, Any]:
    """扫描 planned workflow run → preview gate → admit → dispatch (闭环消费).

    P0 完整第三块: daemon tick 调用, 扫描 WorkflowMeshStore 找 state == "planned"
    的 run, 逐个 preview → admit → dispatch (iris 快速路径 / worker).
    单 run 失败不影响其他 (错误隔离); 跳过 non-planned / blocked / deduplicated.

    required_capabilities 从 WorkflowRequested event payload 读
    (request_workflow_from_task 声明式写入). approval gate 只读不伪造.
    """
    store = WorkflowMeshStore(root / Path(omo_dir))
    consumed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    # producer map: 排除 agent-workflow 的 run (mesh_agent_events 可视化记录,
    # 无 task_id/required_capabilities, 非 task request, 永不该被 consume).
    producer_by_run = {
        str(e.get("workflow_run_id")): e.get("producer", "")
        for e in store.events()
        if e.get("event_type") == "WorkflowRequested"
    }
    planned_runs = [
        s
        for s in store.snapshots()
        if s.get("state") == "planned" and producer_by_run.get(str(s.get("workflow_run_id"))) != "agent-workflow"
    ]
    for snapshot in planned_runs[:max_per_tick]:
        run_id = str(snapshot["workflow_run_id"])
        try:
            event = _requested_event(store, run_id)
            payload = event.get("payload") or {}
            task_id = str(payload.get("task_id") or "")
            required = list(payload.get("required_capabilities") or [])
        except WorkflowDispatchError as exc:
            failed.append({"workflow_run_id": run_id, "error": str(exc)})
            continue
        if not task_id or not required:
            skipped.append(
                {
                    "workflow_run_id": run_id,
                    "reason": "missing task_id or required_capabilities",
                }
            )
            continue
        # 1. preview gate (approval / health / scene, 不写状态)
        try:
            preview = preview_requested_workflow(
                root,
                workflow_run_id=run_id,
                backend=backend,
                required_capabilities=required,
                capability_health=capability_health,
                omo_dir=omo_dir,
            )
        except WorkflowDispatchError as exc:
            failed.append({"workflow_run_id": run_id, "error": f"preview: {exc}"})
            continue
        if preview.get("status") != "eligible":
            skipped.append(
                {
                    "workflow_run_id": run_id,
                    "reason": preview.get("status", "unknown"),
                }
            )
            continue
        # 2. admit (写 WorkflowAdmitted, state → admitted)
        try:
            packet = admit_requested_workflow(
                root,
                workflow_run_id=run_id,
                backend=backend,
                required_capabilities=required,
                capability_health=capability_health,
                omo_dir=omo_dir,
            )
        except WorkflowDispatchError as exc:
            failed.append({"workflow_run_id": run_id, "error": f"admit: {exc}"})
            continue
        # 3. dispatch (iris 快速路径 / worker)
        iris_caps = [c for c in required if str(c).startswith("iris:")]
        try:
            if iris_caps:
                result = _dispatch_iris_via_executor(root, packet, iris_caps, omo_dir=omo_dir)
            else:
                from .omo_worker_dispatch import dispatch_task

                result = dispatch_task(
                    root,
                    task_id=task_id,
                    worker_id=worker_id,
                    allowed_write_paths=allowed_write_paths or [],
                    launch=False,
                    transport="acp_stdio",
                    workflow_packet=packet,
                )
        except Exception as exc:  # defensive: 单 run dispatch 失败不炸 tick
            failed.append({"workflow_run_id": run_id, "error": f"dispatch: {exc}"})
            continue
        consumed.append(
            {
                "workflow_run_id": run_id,
                "task_id": task_id,
                "dispatch_state": result.get("dispatch_state"),
                "iris": bool(iris_caps),
            }
        )
    return {
        "consumed": consumed,
        "skipped": skipped,
        "failed": failed,
        "total_planned": len(planned_runs),
        "max_per_tick": max_per_tick,
    }


__all__ = [
    "WorkflowDispatchError",
    "admit_requested_workflow",
    "admit_workflow",
    "consume_pending_workflow_requests",
    "dispatch_admitted_workflow",
    "preview_requested_workflow",
]
