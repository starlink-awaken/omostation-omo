#!/usr/bin/env python3
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
from .workflow_mesh import WorkflowMeshStore, new_workflow_event


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _proof(grant: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in grant.items() if key != "proof"}
    return hashlib.sha256(_canonical(unsigned)).hexdigest()


def _approval_state(
    root: Path,
    task: dict[str, Any],
    task_file: Path,
    *,
    accepted_task_refs: set[str] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    required = (
        task.get("risk_level") in {"L2", "L3"}
        or task.get("allowed_operation_level") in {"L2", "L3"}
        or bool(task.get("human_approval_required"))
    )
    approval_ref = task.get("approval_ref")
    if not required:
        return {"required": False, "status": "not_required", "ref": approval_ref}
    if not approval_ref:
        raise WorkflowDispatchError("human approval is required before dispatch")
    try:
        approval_path = (root / str(approval_ref)).resolve(strict=True)
        approval_path.relative_to(root.resolve())
    except (OSError, ValueError) as exc:
        raise WorkflowDispatchError("approval record is missing or invalid") from exc
    if not approval_path.is_file():
        raise WorkflowDispatchError("approval record is missing or invalid")
    try:
        approval = load_yaml(approval_path)
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise WorkflowDispatchError("approval record is missing or invalid") from exc
    if approval.get("task_id") != task.get("id"):
        raise WorkflowDispatchError("approval record task mismatch")
    if approval.get("approval_status") != "granted":
        raise WorkflowDispatchError("approval is not granted")
    expires_at = approval.get("expires_at")
    if expires_at:
        try:
            expiry = datetime.fromisoformat(str(expires_at))
            observed = datetime.fromisoformat(now) if now else datetime.now(UTC)
            if expiry <= observed:
                raise WorkflowDispatchError("approval record is expired")
        except WorkflowDispatchError:
            raise
        except (TypeError, ValueError) as exc:
            raise WorkflowDispatchError("approval expiry is invalid") from exc
    scope = approval.get("approval_scope")
    if scope not in {"workflow.execute", "task.promote_apply"}:
        raise WorkflowDispatchError("approval scope does not authorize execution")
    task_ref = str(task_file.relative_to(root))
    refs = approval.get("refs", {})
    valid_task_refs = {task_ref, str(approval_ref), *(accepted_task_refs or set())}
    if refs.get("task_ref") not in valid_task_refs:
        raise WorkflowDispatchError("approval record task reference mismatch")
    return {
        "required": True,
        "status": "granted",
        "ref": str(approval_ref),
        "approval_id": approval.get("approval_id"),
        "scope": scope,
        "expires_at": expires_at,
    }


def _validated_request_identity(
    request_identity: Mapping[str, Any] | None,
    *,
    task_id: str,
    task_file: Path,
    root: Path,
) -> dict[str, Any]:
    if request_identity is None:
        return {}
    legacy_required = {
        "bet_id",
        "packet_id",
        "packet_hash",
        "task_ref",
        "instruction_binding",
    }
    exact_fields = {
        "capability_requirements",
        "capability_requirements_digest",
    }
    provided_fields = set(request_identity)
    if provided_fields != legacy_required and provided_fields != legacy_required | exact_fields:
        raise WorkflowDispatchError("request identity must contain the complete delivery binding")
    has_exact_requirements = exact_fields.issubset(provided_fields)
    scalar_fields = legacy_required - {"instruction_binding"}
    if has_exact_requirements:
        scalar_fields.add("capability_requirements_digest")
    identity: dict[str, Any] = {key: str(request_identity.get(key) or "").strip() for key in scalar_fields}
    if not all(identity.values()):
        raise WorkflowDispatchError("request identity fields must be non-empty")
    if not identity["packet_id"].startswith("WP-"):
        raise WorkflowDispatchError("request identity packet_id is invalid")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity["packet_hash"]):
        raise WorkflowDispatchError("request identity packet_hash is invalid")
    task_ref = str(task_file.relative_to(root))
    if identity["task_ref"] != task_ref:
        raise WorkflowDispatchError("request identity task_ref mismatch")
    instruction = request_identity.get("instruction_binding")
    instruction_fields = {
        "instruction_ref",
        "instruction_version",
        "content_digest",
        "instruction_profile",
    }
    if not isinstance(instruction, Mapping) or set(instruction) != instruction_fields:
        raise WorkflowDispatchError("request identity instruction_binding is incomplete")
    instruction_binding = {key: str(instruction.get(key) or "").strip() for key in instruction_fields}
    if not all(instruction_binding.values()):
        raise WorkflowDispatchError("request identity instruction_binding fields must be non-empty")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", instruction_binding["content_digest"]):
        raise WorkflowDispatchError("request identity instruction_binding digest is invalid")
    if instruction_binding["instruction_profile"] != "executor":
        raise WorkflowDispatchError("request identity instruction profile is invalid")
    identity["instruction_binding"] = instruction_binding
    if has_exact_requirements:
        try:
            capability_requirements = validate_capability_requirements(request_identity.get("capability_requirements"))
        except OrchestrationContractError as exc:
            raise WorkflowDispatchError("request identity capability requirements are invalid") from exc
        canonical_requirements = json.dumps(capability_requirements, sort_keys=True, separators=(",", ":"))
        expected_requirements_digest = "sha256:" + hashlib.sha256(canonical_requirements.encode()).hexdigest()
        if identity["capability_requirements_digest"] != expected_requirements_digest:
            raise WorkflowDispatchError("request identity capability requirements digest mismatch")
        identity["capability_requirements"] = capability_requirements
    return identity


def _build_admission_grant(
    *,
    workflow_run_id: str,
    trace_id: str,
    task_id: str,
    backend: str,
    required_capabilities: list[str],
    approval: dict[str, Any],
    health: dict[str, Any],
    requested_budget: float,
    ttl_seconds: int,
    now: str | None,
    request_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    issued_at = now or datetime.now(UTC).replace(microsecond=0).isoformat()
    if ttl_seconds <= 0:
        raise WorkflowDispatchError("admission ttl must be positive")
    expires_at = (datetime.fromisoformat(issued_at) + timedelta(seconds=ttl_seconds)).isoformat()
    policy = {
        "task_id": task_id,
        "backend": backend,
        "required_capabilities": required_capabilities,
        "approval": approval,
        "health": health["snapshot_digest"],
        "requested_budget": requested_budget,
    }
    grant = {
        "admission_id": f"admit-{uuid4().hex}",
        "status": "admitted",
        "workflow_run_id": workflow_run_id,
        "trace_id": trace_id,
        "backend": backend,
        "step_run_ids": [f"{workflow_run_id}:execute"],
        "capabilities": required_capabilities,
        "policy_digest": hashlib.sha256(_canonical(policy)).hexdigest(),
        "issued_at": issued_at,
        "expires_at": expires_at,
        **({"request_identity": dict(request_identity)} if request_identity else {}),
    }
    grant["proof"] = _proof(grant)
    return grant


def renew_admission(
    root: Path,
    *,
    workflow_run_id: str,
    admission_id: str,
    ttl_seconds: int = 900,
    now: str | None = None,
    omo_dir: str | Path = ".omo",
) -> dict[str, Any]:
    """SR-06 gap-1: renew an expired-but-in-flight admission instead of deadlocking.

    事故 (2026-08-16 SR-06 演练): execute 失败重试时 dispatch 被 mesh 幂等拦截,
    admission 又已过 TTL — 两者互锁无路可走。本函数在 dispatched 态原位续期:
    追加 AdmissionRenewed 事件 (自环), 新 expires_at 写入事件 payload。
    终态 (failed/verified/closed...) 拒绝续期 — 防已结案复燃。
    """
    store = WorkflowMeshStore(root / Path(omo_dir))
    snapshot = store.snapshot(workflow_run_id)
    if snapshot.get("state") != "dispatched":
        raise WorkflowDispatchError(f"admission renewal requires dispatched state, got {snapshot.get('state')}")
    admission = snapshot.get("admission") or {}
    if admission.get("admission_id") != admission_id:
        raise WorkflowDispatchError("admission identity mismatch on renewal")
    issued_at = now or datetime.now(UTC).replace(microsecond=0).isoformat()
    if ttl_seconds <= 0:
        raise WorkflowDispatchError("renewal ttl must be positive")
    expires_at = (datetime.fromisoformat(issued_at) + timedelta(seconds=ttl_seconds)).isoformat()
    event = new_workflow_event(
        "AdmissionRenewed",
        workflow_run_id,
        producer="omo-workflow-dispatch",
        payload={
            "admission_id": admission_id,
            "previous_expires_at": admission.get("expires_at"),
            "expires_at": expires_at,
            "renewed_at": issued_at,
        },
        idempotency_key=f"{workflow_run_id}:admission-renewed:{issued_at}",
    )
    store.append(event)
    return {
        "renewed": True,
        "admission_id": admission_id,
        "expires_at": expires_at,
    }
