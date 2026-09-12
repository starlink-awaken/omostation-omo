"""Workflow Mesh 事件存储与运行态投影。

OMO 是 Workflow Mesh 的证据平面：只接受完整事件信封，使用 append-only
JSONL 保存原始事件，并从事件重建可审计的运行态。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from omo.omo_io import AppendOnlyLog, fcntl_lock

WORKFLOW_MESH_LOG = Path("_knowledge/workflow-mesh/events.jsonl")
REQUIRED_EVENT_FIELDS = {
    "event_id",
    "event_type",
    "trace_id",
    "workflow_run_id",
    "occurred_at",
    "producer",
    "schema_version",
    "idempotency_key",
    "payload",
}
EVENT_STATE = {
    "WorkflowRequested": "planned",
    "WorkflowAdmitted": "admitted",
    "StepDispatched": "dispatched",
    "BackendDispatched": "dispatched",
    "HandoffRecorded": "running",
    "StepStarted": "running",
    "StepHeartbeat": "running",
    "StepRetryScheduled": "running",
    "CheckpointSaved": "running",
    "ToolInvocationRecorded": "running",
    "EvidenceRecorded": "succeeded",
    "ApprovalRequested": "waiting_approval",
    "ApprovalGranted": "running",
    "ApprovalTimeout": "failed",
    "CompensationStarted": "compensating",
    "StepFailed": "failed",
    "BackendUnavailable": "unavailable",
    "WorkflowRecovered": "running",
    "WorkflowVerified": "verified",
    "PRMerged": "merged",
    "WorkflowSucceeded": "succeeded",
    "WorkflowFailed": "failed",
    "WorkflowCancelled": "cancelled",
    "WorkflowClosed": "closed",
    "WorkerAcknowledged": "dispatched",
    "WorkerLeaseRenewed": "running",
    "WorkerLeaseExpired": "unavailable",
    # SR-06 gap-1 (2026-08-16): admission TTL 到期但 worker 在途时, 控制器可续期而非死锁
    "AdmissionRenewed": "dispatched",
    "WorkerReclaimed": "running",
}
TOOL_OUTCOMES = frozenset({"succeeded", "failed", "unavailable"})
# 只有 closed 才是不可再推进的生命周期终态。succeeded/failed/unavailable
# 仍可能进入验证、关闭或受控恢复。
TERMINAL_STATES = {"closed"}
_ALLOWED_TRANSITIONS = {
    "unknown": {"planned", "dispatched", "running"},
    "planned": {
        "admitted",
        "running",
        "failed",
        "unavailable",
        "cancelled",
    },
    "admitted": {"dispatched", "running", "failed", "unavailable", "cancelled"},
    "dispatched": {"dispatched", "running", "failed", "unavailable", "cancelled"},  # + AdmissionRenewed 自环
    "running": {
        "running",
        "waiting_approval",
        "compensating",
        "failed",
        "unavailable",
        "succeeded",
        "cancelled",
    },
    "waiting_approval": {"running", "failed", "unavailable", "cancelled"},
    "compensating": {"running", "failed", "unavailable", "succeeded", "cancelled"},
    "failed": {"running", "failed", "closed"},
    "unavailable": {"running", "unavailable", "closed"},
    "succeeded": {"succeeded", "verified", "compensating", "closed"},
    "verified": {"merged", "closed"},
    "merged": {"closed"},
    "cancelled": {"closed"},
    "closed": set(),
}
_ALLOWED_EVENTS = {
    "unknown": {"WorkflowRequested", "BackendDispatched", "HandoffRecorded"},
    "planned": {
        "WorkflowAdmitted",
        "StepStarted",
        "WorkflowFailed",
        "BackendUnavailable",
        "WorkflowCancelled",
    },
    "admitted": {
        "StepDispatched",
        "StepStarted",
        "WorkflowFailed",
        "BackendUnavailable",
        "WorkflowCancelled",
    },
    "dispatched": {
        "StepDispatched",
        "AdmissionRenewed",
        "StepStarted",
        "WorkerAcknowledged",
        "WorkerLeaseRenewed",
        "WorkerLeaseExpired",
        "WorkflowFailed",
        "BackendUnavailable",
        "WorkflowCancelled",
    },
    "running": {
        "StepDispatched",
        "StepStarted",
        "StepHeartbeat",
        "StepRetryScheduled",
        "CheckpointSaved",
        "ToolInvocationRecorded",
        "WorkerAcknowledged",
        "WorkerLeaseRenewed",
        "WorkerLeaseExpired",
        "ApprovalRequested",
        "CompensationStarted",
        "StepFailed",
        "BackendUnavailable",
        "WorkflowSucceeded",
        "WorkflowCancelled",
    },
    "waiting_approval": {
        "ApprovalGranted",
        "ApprovalTimeout",
        "StepFailed",
        "BackendUnavailable",
        "WorkflowCancelled",
    },
    "compensating": {
        "WorkflowRecovered",
        "StepFailed",
        "WorkflowFailed",
        "BackendUnavailable",
        "WorkflowSucceeded",
        "WorkflowCancelled",
    },
    "failed": {"WorkflowRecovered", "StepFailed", "WorkflowFailed", "WorkflowClosed"},
    "unavailable": {
        "WorkflowRecovered",
        "WorkerReclaimed",
        "BackendUnavailable",
        "WorkflowClosed",
    },
    "succeeded": {
        "WorkflowSucceeded",
        "EvidenceRecorded",
        "WorkflowVerified",
        "CompensationStarted",
        "WorkflowClosed",
    },
    "verified": {"PRMerged", "WorkflowClosed"},
    "merged": {"WorkflowClosed"},
    "cancelled": {"WorkflowClosed"},
    "closed": set(),
}

_WORKER_EVENTS = {
    "WorkerAcknowledged",
    "WorkerLeaseRenewed",
    "WorkerLeaseExpired",
    "WorkerReclaimed",
}
SCENE_BINDING_FIELDS = frozenset({"scene_id", "journey_id", "outcome_metric"})
_AGENT_WORKFLOW_IDENTITY_FIELDS = {
    "bet_id",
    "workflow_id",
    "correlation_id",
    "workflow_run_id",
    "packet_id",
    "packet_hash",
    "assignment_id",
    "dispatch_id",
    "actor_id",
    "delivery_attempt_id",
    "capability_requirements",
    "capability_requirements_digest",
}
EXACT_REQUEST_DISCRIMINATOR = "agent-workflow-exact/v1"
_EXACT_COORDINATOR_CAPABILITY = object()
_SHA256_REF_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class WorkflowMeshEventError(ValueError):
    """事件结构或状态转换不符合 Workflow Mesh 契约。"""


def _exact_coordinator_capability() -> object:
    """Return the module-private in-process exact coordinator capability."""
    return _EXACT_COORDINATOR_CAPABILITY


def _require_exact_coordinator_capability(capability: object, *, purpose: str) -> None:
    if capability is not _EXACT_COORDINATOR_CAPABILITY:
        raise WorkflowMeshEventError(f"exact {purpose} requires coordinator capability")


def _scene_binding(payload: Mapping[str, Any]) -> dict[str, str] | None:
    """Return the minimal business context carried by a workflow run.

    The Mesh does not own permissions or raw business data. It only preserves
    the stable identifiers that let product surfaces and evidence receipts
    relate an execution to a user journey and a measurable outcome.
    """
    value = payload.get("scene_binding")
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise WorkflowMeshEventError("scene_binding must be an object")
    missing = sorted(SCENE_BINDING_FIELDS - value.keys())
    if missing:
        raise WorkflowMeshEventError(f"scene_binding missing fields: {missing}")
    binding = {field: str(value[field]).strip() for field in SCENE_BINDING_FIELDS}
    empty = sorted(field for field, item in binding.items() if not item)
    if empty:
        raise WorkflowMeshEventError(f"scene_binding fields must be non-empty: {empty}")
    return binding


def _canonical_admission(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def worker_ack_origin_digest(origin_proof: str, payload: Mapping[str, Any]) -> str:
    """Bind a high-entropy one-time worker secret to one exact dispatch."""
    context = {
        key: payload.get(key)
        for key in (
            "ack_origin_nonce",
            "workflow_run_id",
            "dispatch_id",
            "worker_id",
            "step_run_id",
            "admission_id",
            "packet_id",
            "packet_hash",
            "instruction_binding",
            "result_digest",
        )
    }
    return (
        "sha256:"
        + hmac.new(
            origin_proof.encode("utf-8"),
            _canonical_admission(context),
            hashlib.sha256,
        ).hexdigest()
    )


def _validated_exact_request_identity(
    payload: Mapping[str, Any],
    workflow_run_id: str,
) -> dict[str, Any] | None:
    discriminator = payload.get("exact_request_discriminator")
    if discriminator is None:
        identity = payload.get("request_identity")
        if isinstance(identity, Mapping):
            legacy_fields = {"bet_id", "packet_id", "packet_hash"}
            if (
                set(identity) == legacy_fields
                and all(isinstance(identity.get(field), str) and identity.get(field) for field in legacy_fields)
                and _SHA256_REF_RE.fullmatch(str(identity["packet_hash"])) is not None
            ):
                return None
            raise WorkflowMeshEventError("exact Agent Workflow request discriminator is required")
        return None
    if discriminator != EXACT_REQUEST_DISCRIMINATOR:
        raise WorkflowMeshEventError("exact Agent Workflow request discriminator is invalid")
    identity = payload.get("request_identity")
    if not isinstance(identity, Mapping) or set(identity) != _AGENT_WORKFLOW_IDENTITY_FIELDS:
        raise WorkflowMeshEventError("exact Agent Workflow request identity shape is invalid")
    if (
        identity.get("bet_id") != payload.get("bet_id")
        or not isinstance(identity.get("bet_id"), str)
        or not identity.get("bet_id")
        or identity.get("workflow_id") != payload.get("workflow_id")
        or not isinstance(identity.get("workflow_id"), str)
        or not identity.get("workflow_id")
        or identity.get("workflow_run_id") != workflow_run_id
        or identity.get("correlation_id") != workflow_run_id
        or not isinstance(payload.get("workflow_id"), str)
        or not payload.get("workflow_id")
        or not isinstance(identity.get("packet_id"), str)
        or not identity["packet_id"]
        or not isinstance(identity.get("packet_hash"), str)
        or _SHA256_REF_RE.fullmatch(identity["packet_hash"]) is None
        or not isinstance(identity.get("assignment_id"), str)
        or not identity["assignment_id"]
        or not isinstance(identity.get("dispatch_id"), str)
        or not identity["dispatch_id"]
        or not isinstance(identity.get("actor_id"), str)
        or not identity["actor_id"]
        or not isinstance(identity.get("delivery_attempt_id"), str)
        or not identity["delivery_attempt_id"]
    ):
        raise WorkflowMeshEventError("exact Agent Workflow request delivery identity is invalid")
    requirements = identity.get("capability_requirements")
    try:
        from .orchestration_contract import validate_capability_requirements

        canonical_requirements = validate_capability_requirements(requirements)
    except (ImportError, ValueError):
        canonical_requirements = None
    if canonical_requirements is None or canonical_requirements != requirements:
        raise WorkflowMeshEventError("Agent Workflow request capability requirements are invalid")
    expected_digest = (
        "sha256:"
        + hashlib.sha256(
            _canonical_admission(canonical_requirements)  # type: ignore[arg-type]
        ).hexdigest()
    )
    if identity.get("capability_requirements_digest") != expected_digest:
        raise WorkflowMeshEventError("Agent Workflow request capability requirements digest mismatch")
    return dict(identity)


def _validate_admission_payload(
    payload: dict[str, Any],
    *,
    exact_request_identity: Mapping[str, Any] | None = None,
    requested_workflow_id: str | None = None,
) -> dict[str, Any]:
    admission = payload.get("admission") or payload
    if not isinstance(admission, dict):
        raise WorkflowMeshEventError("WorkflowAdmitted requires an admission grant")
    required = {
        "admission_id",
        "status",
        "workflow_run_id",
        "trace_id",
        "step_run_ids",
        "capabilities",
        "policy_digest",
        "issued_at",
        "expires_at",
        "proof",
    }
    missing = sorted(required - admission.keys())
    if missing:
        raise WorkflowMeshEventError(f"Admission grant missing fields: {missing}")
    if admission["status"] != "admitted":
        raise WorkflowMeshEventError("WorkflowAdmitted grant must be admitted")
    unsigned = {key: value for key, value in admission.items() if key != "proof"}
    expected_proof = hashlib.sha256(_canonical_admission(unsigned)).hexdigest()
    if admission["proof"] != expected_proof:
        raise WorkflowMeshEventError("Admission grant proof mismatch")
    if not isinstance(admission["step_run_ids"], list) or not admission["step_run_ids"]:
        raise WorkflowMeshEventError("Admission grant requires step_run_ids")
    try:
        issued_at = datetime.fromisoformat(str(admission["issued_at"]).replace("Z", "+00:00"))
        expires_at = datetime.fromisoformat(str(admission["expires_at"]).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise WorkflowMeshEventError("Admission grant time window is invalid") from exc
    if issued_at.tzinfo is None or expires_at.tzinfo is None or expires_at <= issued_at:
        raise WorkflowMeshEventError("Admission grant time window is invalid")
    if exact_request_identity is not None:
        identity = admission.get("request_identity")
        if not isinstance(identity, Mapping) or set(identity) != _AGENT_WORKFLOW_IDENTITY_FIELDS:
            raise WorkflowMeshEventError("Agent Workflow admission requires exact request_identity")
        if identity != exact_request_identity:
            raise WorkflowMeshEventError("Agent Workflow admission request identity mismatch")
        if (
            admission.get("exact_request_discriminator") != EXACT_REQUEST_DISCRIMINATOR
            or admission.get("bet_id") != identity.get("bet_id")
            or admission.get("workflow_id") != identity.get("workflow_id")
        ):
            raise WorkflowMeshEventError("Agent Workflow admission exact identity mismatch")
        run_id = admission["workflow_run_id"]
        if identity.get("workflow_run_id") != run_id or identity.get("correlation_id") != run_id:
            raise WorkflowMeshEventError("Agent Workflow admission run identity mismatch")
        if (
            not isinstance(identity.get("packet_id"), str)
            or not identity["packet_id"]
            or not isinstance(identity.get("packet_hash"), str)
            or _SHA256_REF_RE.fullmatch(identity["packet_hash"]) is None
            or not isinstance(identity.get("assignment_id"), str)
            or not identity["assignment_id"]
            or not isinstance(identity.get("dispatch_id"), str)
            or not identity["dispatch_id"]
            or not isinstance(identity.get("actor_id"), str)
            or not identity["actor_id"]
            or not isinstance(identity.get("delivery_attempt_id"), str)
            or not identity["delivery_attempt_id"]
        ):
            raise WorkflowMeshEventError("Agent Workflow admission delivery identity is invalid")
        requirements = identity.get("capability_requirements")
        try:
            from .orchestration_contract import validate_capability_requirements

            canonical_requirements = validate_capability_requirements(requirements)
        except (ImportError, ValueError):
            canonical_requirements = None
        if canonical_requirements is None or canonical_requirements != requirements:
            raise WorkflowMeshEventError("Agent Workflow admission capability requirements are invalid")
        expected_digest = (
            "sha256:"
            + hashlib.sha256(
                _canonical_admission(canonical_requirements)  # type: ignore[arg-type]
            ).hexdigest()
        )
        if identity.get("capability_requirements_digest") != expected_digest:
            raise WorkflowMeshEventError("Agent Workflow admission capability requirements digest mismatch")
        policy = payload.get("policy")
        expected_policy = {
            "exact_request_discriminator": EXACT_REQUEST_DISCRIMINATOR,
            "bet_id": identity["bet_id"],
            "workflow_id": requested_workflow_id,
            "workflow_run_id": run_id,
            "packet_id": identity["packet_id"],
            "packet_hash": identity["packet_hash"],
            "capability_requirements": identity["capability_requirements"],
            "capability_requirements_digest": identity["capability_requirements_digest"],
            "actor_id": identity["actor_id"],
            "delivery_attempt_id": identity["delivery_attempt_id"],
            "source_receipt_digests": policy.get("source_receipt_digests") if isinstance(policy, Mapping) else None,
            "requested_budget": 0.0,
        }
        source_receipt_digests = expected_policy["source_receipt_digests"]
        if (
            not isinstance(policy, Mapping)
            or set(policy) != set(expected_policy)
            or dict(policy) != expected_policy
            or not isinstance(source_receipt_digests, list)
            or len(source_receipt_digests) != len(identity["capability_requirements"])
            or any(
                not isinstance(digest, str) or _SHA256_REF_RE.fullmatch(digest) is None
                for digest in source_receipt_digests
            )
        ):
            raise WorkflowMeshEventError("Agent Workflow admission policy identity mismatch")
        expected_policy_digest = (
            hashlib.sha256(_canonical_admission(dict(policy))).hexdigest() if isinstance(policy, Mapping) else None
        )
        if (
            payload.get("exact_request_discriminator") != EXACT_REQUEST_DISCRIMINATOR
            or payload.get("bet_id") != identity.get("bet_id")
            or payload.get("workflow_id") != identity.get("workflow_id")
            or payload.get("request_identity") != identity
            or payload.get("policy_digest") != admission["policy_digest"]
            or expected_policy_digest != admission["policy_digest"]
            or payload.get("proof") != admission["proof"]
        ):
            raise WorkflowMeshEventError("Agent Workflow admission policy or proof mismatch")
    return admission


def _step_is_admitted(step_run_id: str, admission: dict[str, Any]) -> bool:
    return any(
        step_run_id == admitted or step_run_id.startswith(f"{admitted}:") for admitted in admission["step_run_ids"]
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _parsed_utc_timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise WorkflowMeshEventError("exact admission time window is invalid") from exc
    if parsed.tzinfo is None:
        raise WorkflowMeshEventError("exact admission time window is invalid")
    return parsed.astimezone(UTC)


def _require_live_exact_admission(snapshot: Mapping[str, Any], event: Mapping[str, Any]) -> None:
    if not isinstance(snapshot.get("exact_request_identity"), Mapping):
        return
    admission = snapshot.get("admission")
    if not isinstance(admission, Mapping):
        raise WorkflowMeshEventError("exact authenticated append requires persisted admission")
    issued_at = _parsed_utc_timestamp(admission.get("issued_at"))
    expires_at = _parsed_utc_timestamp(admission.get("expires_at"))
    event_at = _parsed_utc_timestamp(event.get("occurred_at"))
    current = datetime.now(UTC)
    if expires_at <= issued_at:
        raise WorkflowMeshEventError("exact admission time window is invalid")
    if event_at < issued_at or current < issued_at:
        raise WorkflowMeshEventError("exact admission is not yet valid")
    if event_at >= expires_at or current >= expires_at:
        raise WorkflowMeshEventError("exact admission is expired")


def _require_exact_worker_origin_proof(
    snapshot: Mapping[str, Any],
    workflow_run_id: str,
    origin_proof: str,
    *,
    purpose: str,
) -> None:
    if not origin_proof:
        raise WorkflowMeshEventError(f"exact {purpose} requires coordinator origin proof")
    worker = snapshot.get("worker")
    if not isinstance(worker, Mapping):
        raise WorkflowMeshEventError(f"exact {purpose} requires persisted worker context")
    context = {
        **worker,
        "workflow_run_id": workflow_run_id,
        "result_digest": None,
    }
    expected = worker_ack_origin_digest(origin_proof, context)
    if not hmac.compare_digest(expected, str(worker.get("ack_origin_proof_digest") or "")):
        raise WorkflowMeshEventError(f"exact {purpose} origin proof mismatch")


def new_workflow_event(
    event_type: str,
    workflow_run_id: str,
    *,
    trace_id: str | None = None,
    producer: str = "omo",
    payload: dict[str, Any] | None = None,
    scene_binding: Mapping[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    event_id = uuid4().hex
    event_payload = dict(payload or {})
    if scene_binding is not None:
        existing = event_payload.get("scene_binding")
        if existing is not None and existing != scene_binding:
            raise WorkflowMeshEventError("conflicting scene_binding payload")
        event_payload["scene_binding"] = dict(scene_binding)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "trace_id": trace_id or workflow_run_id,
        "workflow_run_id": workflow_run_id,
        "occurred_at": _utc_now(),
        "producer": producer,
        "schema_version": "workflow-mesh/v1",
        "idempotency_key": idempotency_key
        or f"{workflow_run_id}:{event_type}:{event_payload.get('step_run_id', 'workflow')}",
        "payload": event_payload,
    }


def validate_workflow_event(event: dict[str, Any]) -> dict[str, Any]:
    missing = REQUIRED_EVENT_FIELDS - event.keys()
    if missing:
        raise WorkflowMeshEventError(f"Workflow Mesh event missing fields: {sorted(missing)}")
    if event["schema_version"] != "workflow-mesh/v1":
        raise WorkflowMeshEventError("Unsupported Workflow Mesh event schema")
    if event["event_type"] not in EVENT_STATE:
        raise WorkflowMeshEventError(f"Unknown Workflow Mesh event: {event['event_type']}")
    if not isinstance(event["payload"], dict):
        raise WorkflowMeshEventError("Workflow Mesh event payload must be an object")
    _scene_binding(event["payload"])
    return event


def dispatch_backend(
    store: WorkflowMeshStore,
    backend_name: str,
    fn: Callable[[list[str]], int],
    args: list[str],
) -> int:
    """B 槽后端经 Mesh（S 槽收件箱）分发。

    发射 ``BackendDispatched`` 事件，让 Mesh 成为该次分发的观察收件箱（满足
    八律第 3 条：后端不拥有收件箱），随后原样调用 ``fn(args)`` 并返回其返回值。
    事件写入永不破坏后端：byte-for-byte 等价于直接调用 ``fn(args)``。
    """
    run_id = uuid4().hex
    try:
        store.append(
            new_workflow_event(
                "BackendDispatched",
                run_id,
                producer=f"omo.{backend_name}",
                payload={"backend": backend_name, "args": args},
            )
        )
    except Exception:  # noqa: BLE001 — 事件是观测性的，绝不能破坏后端
        pass
    return fn(args)


def record_handoff(
    store: WorkflowMeshStore,
    from_role: str,
    to_role: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """记录 Cell/Role 间交接事件（八律第3条纪律：交接入 Mesh 证据平面）。

    观测性事件：byte-for-byte 不改变交接双方行为；写入失败静默降级。
    """
    run_id = uuid4().hex
    body = {"from_role": from_role, "to_role": to_role}
    if payload:
        body["handoff"] = dict(payload)
    try:
        store.append(
            new_workflow_event(
                "HandoffRecorded",
                run_id,
                producer="omo.handoff",
                payload=body,
            )
        )
    except Exception:  # noqa: BLE001
        pass
    return {"handoff_id": run_id, "from_role": from_role, "to_role": to_role}


def project_workflow_run(events: list[dict[str, Any]], workflow_run_id: str) -> dict[str, Any]:
    """从事件重建一个运行快照，拒绝终态后的幽灵事件。"""
    # Append-only 文件顺序是状态机顺序；不能按调用方时间戳重排，否则迟到事件
    # 可能被插入历史位置，绕过终态和恢复检查。
    relevant = [event for event in events if event.get("workflow_run_id") == workflow_run_id]
    snapshot: dict[str, Any] = {
        "workflow_run_id": workflow_run_id,
        "trace_id": None,
        "state": "unknown",
        "event_count": 0,
        "step_states": {},
        "last_event_type": None,
        "last_event_at": None,
        "metadata": {},
        "step_runs": {},
        "checkpoints": [],
        "evidence": {},
        "approvals": {},
        "admission": None,
        "worker": None,
        "worker_events": [],
        "scene_binding": None,
        "exact_request_identity": None,
        "exact_request_discriminator": None,
        "requested_workflow_id": None,
        "worker_completion_receipt": None,
    }
    for event in relevant:
        validate_workflow_event(event)
        event_type = event["event_type"]
        next_state = EVENT_STATE[event_type]
        # Step-level progress can arrive out of order or in batches. It updates
        # the step projection without moving the workflow backwards.
        if snapshot["state"] == "running" and event_type in {
            "StepDispatched",
            "StepStarted",
            "StepHeartbeat",
            "CheckpointSaved",
        }:
            next_state = "running"
        elif snapshot["state"] == "dispatched" and event_type == "StepDispatched":
            next_state = "dispatched"
        elif event_type == "WorkerAcknowledged" and snapshot["state"] in {
            "dispatched",
            "running",
        }:
            next_state = snapshot["state"]
        if snapshot["state"] in TERMINAL_STATES:
            raise WorkflowMeshEventError(f"Workflow run is terminal; event is not allowed: {event['event_type']}")
        if event_type not in _ALLOWED_EVENTS.get(
            snapshot["state"], set()
        ) or next_state not in _ALLOWED_TRANSITIONS.get(snapshot["state"], set()):
            raise WorkflowMeshEventError(
                "Workflow run is terminal or requires recovery; "
                f"invalid transition {snapshot['state']} -> {next_state} "
                f"for {event_type}"
            )
        if event_type == "EvidenceRecorded" and not event["payload"].get("evidence_id"):
            raise WorkflowMeshEventError("EvidenceRecorded requires evidence_id")
        if event_type == "WorkflowAdmitted":
            admission = _validate_admission_payload(
                event["payload"],
                exact_request_identity=snapshot.get("exact_request_identity"),
                requested_workflow_id=snapshot.get("requested_workflow_id"),
            )
            if admission["workflow_run_id"] != workflow_run_id:
                raise WorkflowMeshEventError("Admission grant workflow_run_id mismatch")
            snapshot["admission"] = dict(admission)
        scene_binding = _scene_binding(event["payload"])
        if event_type == "WorkflowRequested":
            snapshot["scene_binding"] = scene_binding
            exact_identity = _validated_exact_request_identity(event["payload"], workflow_run_id)
            if exact_identity is not None:
                snapshot["exact_request_identity"] = exact_identity
                snapshot["exact_request_discriminator"] = event["payload"]["exact_request_discriminator"]
                snapshot["requested_workflow_id"] = event["payload"]["workflow_id"]
        elif scene_binding is not None:
            if snapshot["scene_binding"] is None:
                raise WorkflowMeshEventError("scene_binding requires a prior WorkflowRequested event")
            if scene_binding != snapshot["scene_binding"]:
                raise WorkflowMeshEventError("scene_binding cannot change within a run")
        if (
            event_type
            in {
                "StepDispatched",
                "StepStarted",
                "StepHeartbeat",
                "StepRetryScheduled",
                "CheckpointSaved",
                "ToolInvocationRecorded",
                "CompensationStarted",
                "StepFailed",
            }
            | _WORKER_EVENTS
        ):
            step_run_id = event["payload"].get("step_run_id")
            admission = snapshot.get("admission")
            if not step_run_id or not isinstance(admission, dict):
                raise WorkflowMeshEventError(f"{event_type} requires an admitted StepRun")
            if event["payload"].get("admission_id") != admission["admission_id"]:
                raise WorkflowMeshEventError(f"{event_type} admission_id mismatch")
            if not _step_is_admitted(step_run_id, admission):
                raise WorkflowMeshEventError(f"StepRun is not admitted: {step_run_id}")
            if (
                event_type == "StepDispatched"
                and isinstance(snapshot.get("exact_request_identity"), Mapping)
                and isinstance(admission.get("request_identity"), Mapping)
            ):
                exact_identity = snapshot["exact_request_identity"]
                requirements_digest = admission["request_identity"].get("capability_requirements_digest")
                if (
                    event["payload"].get("exact_request_discriminator") != EXACT_REQUEST_DISCRIMINATOR
                    or event["payload"].get("bet_id") != exact_identity.get("bet_id")
                    or event["payload"].get("workflow_id") != exact_identity.get("workflow_id")
                    or event["payload"].get("dispatch_id") != exact_identity.get("dispatch_id")
                    or event["payload"].get("packet_id") != exact_identity.get("packet_id")
                    or event["payload"].get("packet_hash") != exact_identity.get("packet_hash")
                    or event["payload"].get("policy_digest") != admission.get("policy_digest")
                    or requirements_digest is not None
                    and event["payload"].get("capability_requirements_digest") != requirements_digest
                ):
                    raise WorkflowMeshEventError("exact StepDispatched persisted admission binding mismatch")
            if event_type != "StepDispatched" and step_run_id not in snapshot["step_runs"]:
                raise WorkflowMeshEventError(f"{event_type} requires prior StepDispatched")
        if event_type in _WORKER_EVENTS:
            required_worker_fields = {
                "dispatch_id",
                "worker_id",
                "step_run_id",
                "admission_id",
            }
            missing_worker_fields = sorted(required_worker_fields - event["payload"].keys())
            if missing_worker_fields:
                raise WorkflowMeshEventError(f"{event_type} missing worker fields: {missing_worker_fields}")
            event_specific_fields = {
                "WorkerAcknowledged": {
                    "acknowledged_at",
                    "lease_expires_at",
                    "packet_id",
                    "packet_hash",
                    "instruction_binding",
                    "ack_decision",
                    "ack_origin_proof_digest",
                },
                "WorkerLeaseRenewed": {
                    "heartbeat_id",
                    "heartbeat_at",
                    "lease_expires_at",
                },
                "WorkerLeaseExpired": {
                    "expired_at",
                    "lease_expires_at",
                    "reason",
                },
                "WorkerReclaimed": {
                    "reclaimed_at",
                    "successor_worker_id",
                    "successor_dispatch_id",
                    "reason",
                },
            }[event_type]
            missing_specific_fields = sorted(event_specific_fields - event["payload"].keys())
            if missing_specific_fields:
                raise WorkflowMeshEventError(f"{event_type} missing fields: {missing_specific_fields}")
            current_worker = snapshot.get("worker")
            if not isinstance(current_worker, dict):
                raise WorkflowMeshEventError(f"{event_type} requires prior StepDispatched worker context")
            for key in ("dispatch_id", "worker_id", "step_run_id", "admission_id"):
                if current_worker.get(key) != event["payload"].get(key):
                    raise WorkflowMeshEventError(f"{event_type} worker context mismatch: {key}")
            worker_state = current_worker.get("state")
            if event_type == "WorkerAcknowledged" and worker_state not in {
                "dispatched",
                "acknowledged",
            }:
                raise WorkflowMeshEventError("WorkerAcknowledged requires a dispatched worker")
            if event_type == "WorkerAcknowledged":
                if event["payload"].get("ack_decision") not in {"proceed", "stop"}:
                    raise WorkflowMeshEventError("WorkerAcknowledged ack_decision must be proceed or stop")
                dispatched_identity = {
                    key: current_worker.get(key) for key in ("packet_id", "packet_hash", "instruction_binding")
                }
                acknowledged_identity = {
                    key: event["payload"].get(key) for key in ("packet_id", "packet_hash", "instruction_binding")
                }
                if dispatched_identity != acknowledged_identity:
                    raise WorkflowMeshEventError("WorkerAcknowledged delivery binding mismatch")
            if event_type == "WorkerLeaseRenewed" and worker_state not in {
                "acknowledged",
                "active",
            }:
                raise WorkflowMeshEventError("WorkerLeaseRenewed requires an acknowledged worker")
            if event_type == "WorkerLeaseExpired" and worker_state not in {
                "acknowledged",
                "active",
            }:
                raise WorkflowMeshEventError("WorkerLeaseExpired requires a live worker lease")
            if event_type == "WorkerReclaimed" and worker_state != "lease_expired":
                raise WorkflowMeshEventError("WorkerReclaimed requires an expired worker lease")
        if event_type == "ToolInvocationRecorded":
            required_tool_fields = {
                "invocation_id",
                "tool_id",
                "operation",
                "input_ref",
                "input_digest",
                "activation",
                "external_side_effects",
                "dispatch_id",
                "worker_id",
                "request_digest",
                "outcome",
            }
            missing_tool_fields = sorted(required_tool_fields - event["payload"].keys())
            if missing_tool_fields:
                raise WorkflowMeshEventError(f"ToolInvocationRecorded missing fields: {missing_tool_fields}")
            if event["payload"].get("activation") != "sandbox":
                raise WorkflowMeshEventError("ToolInvocationRecorded activation must be sandbox")
            if event["payload"].get("external_side_effects") != "disabled":
                raise WorkflowMeshEventError("ToolInvocationRecorded external_side_effects must be disabled")
            if event["payload"].get("outcome") not in TOOL_OUTCOMES:
                raise WorkflowMeshEventError("ToolInvocationRecorded outcome must be succeeded, failed, or unavailable")
        if event_type == "WorkflowVerified" and not snapshot["evidence"]:
            raise WorkflowMeshEventError("WorkflowVerified requires at least one EvidenceRecorded event")
        if event_type == "WorkflowSucceeded" and isinstance(snapshot.get("exact_request_identity"), Mapping):
            receipt = event["payload"].get("worker_completion_receipt")
            receipt_fields = {
                "status",
                "exact_request_discriminator",
                "bet_id",
                "workflow_id",
                "workflow_run_id",
                "admission_id",
                "step_run_id",
                "dispatch_id",
                "worker_id",
                "ack_origin_proof_digest",
                "completion_origin_commitment",
                "result_digest",
                "receipt_digest",
            }
            if not isinstance(receipt, Mapping) or set(receipt) != receipt_fields:
                raise WorkflowMeshEventError("exact WorkflowSucceeded requires worker completion receipt")
            unsigned_receipt = {key: value for key, value in receipt.items() if key != "receipt_digest"}
            expected_receipt_digest = "sha256:" + hashlib.sha256(_canonical_admission(unsigned_receipt)).hexdigest()
            admission = snapshot.get("admission")
            exact_identity = snapshot.get("exact_request_identity")
            worker = snapshot.get("worker")
            step = snapshot.get("step_runs", {}).get(receipt.get("step_run_id"))
            if (
                event.get("producer") != "worker"
                or receipt.get("status") != "succeeded"
                or receipt.get("exact_request_discriminator") != EXACT_REQUEST_DISCRIMINATOR
                or not isinstance(exact_identity, Mapping)
                or receipt.get("bet_id") != exact_identity.get("bet_id")
                or receipt.get("workflow_id") != exact_identity.get("workflow_id")
                or receipt.get("workflow_run_id") != workflow_run_id
                or not isinstance(admission, Mapping)
                or receipt.get("admission_id") != admission.get("admission_id")
                or not isinstance(step, Mapping)
                or step.get("state") != "running"
                or step.get("admission_id") != admission.get("admission_id")
                or not isinstance(worker, Mapping)
                or worker.get("state") not in {"acknowledged", "active"}
                or worker.get("ack_decision") != "proceed"
                or worker.get("ack_origin_proof_consumed") is not True
                or any(
                    receipt.get(field) != worker.get(field)
                    for field in ("dispatch_id", "worker_id", "step_run_id", "admission_id")
                )
                or receipt.get("ack_origin_proof_digest") != worker.get("ack_origin_proof_digest")
                or not isinstance(receipt.get("ack_origin_proof_digest"), str)
                or _SHA256_REF_RE.fullmatch(receipt["ack_origin_proof_digest"]) is None
                or not isinstance(receipt.get("completion_origin_commitment"), str)
                or _SHA256_REF_RE.fullmatch(receipt["completion_origin_commitment"]) is None
                or not isinstance(receipt.get("result_digest"), str)
                or _SHA256_REF_RE.fullmatch(receipt["result_digest"]) is None
                or receipt.get("receipt_digest") != expected_receipt_digest
            ):
                raise WorkflowMeshEventError("exact worker completion receipt binding mismatch")
            snapshot["worker_completion_receipt"] = dict(receipt)
        snapshot["trace_id"] = snapshot["trace_id"] or event["trace_id"]
        snapshot["state"] = next_state
        snapshot["event_count"] += 1
        snapshot["last_event_type"] = event["event_type"]
        snapshot["last_event_at"] = event["occurred_at"]
        for key in (
            "workflow",
            "workflow_definition_id",
            "intent_id",
            "task_id",
            "evidence_id",
            "pr_id",
            "pr_url",
        ):
            if key in event["payload"] and key not in snapshot["metadata"]:
                snapshot["metadata"][key] = event["payload"][key]
        step_run_id = event["payload"].get("step_run_id")
        if step_run_id:
            step_projection = snapshot["step_runs"].setdefault(
                step_run_id,
                {
                    "step_run_id": step_run_id,
                    "step_name": event["payload"].get("step_name"),
                    "state": "unknown",
                    "attempt": event["payload"].get("attempt", 1),
                    "last_event_type": None,
                    "checkpoint": None,
                    "admission_id": event["payload"].get("admission_id"),
                },
            )
            step_projection["step_name"] = step_projection.get("step_name") or event["payload"].get("step_name")
            step_projection["state"] = {
                "StepDispatched": "dispatched",
                "StepStarted": "running",
                "StepHeartbeat": "running",
                "StepRetryScheduled": "running",
                "CheckpointSaved": "running",
                "ToolInvocationRecorded": "running",
                "EvidenceRecorded": "succeeded",
                "CompensationStarted": "compensating",
                "StepFailed": "failed",
                "BackendUnavailable": "unavailable",
                "WorkerLeaseRenewed": "running",
                "WorkerLeaseExpired": "unavailable",
                "WorkerReclaimed": "running",
            }.get(event_type, step_projection["state"])
            if event_type == "ToolInvocationRecorded":
                step_projection["tool_outcome"] = event["payload"].get("outcome")
            step_projection["last_event_type"] = event_type
            step_projection["admission_id"] = event["payload"].get("admission_id", step_projection.get("admission_id"))
            if event_type == "CheckpointSaved":
                checkpoint = {
                    "checkpoint_id": event["payload"].get("checkpoint_id") or event["event_id"],
                    "step_run_id": step_run_id,
                    "attempt": event["payload"].get("attempt", 1),
                    "next_turn": event["payload"].get("next_turn"),
                    "checkpoint": event["payload"].get("checkpoint"),
                    "event_id": event["event_id"],
                }
                step_projection["checkpoint"] = checkpoint
                snapshot["checkpoints"].append(checkpoint)
            snapshot["step_states"][step_run_id] = {
                "state": step_projection["state"],
                "last_event_type": event["event_type"],
            }
        if event_type == "StepDispatched" and event["payload"].get("dispatch_id"):
            snapshot["worker"] = {
                "dispatch_id": event["payload"]["dispatch_id"],
                "worker_id": event["payload"].get("worker_id"),
                "step_run_id": event["payload"].get("step_run_id"),
                "admission_id": event["payload"].get("admission_id"),
                "packet_id": event["payload"].get("packet_id"),
                "packet_hash": event["payload"].get("packet_hash"),
                "instruction_binding": event["payload"].get("instruction_binding"),
                "ack_origin_nonce": event["payload"].get("ack_origin_nonce"),
                "ack_origin_commitment": event["payload"].get("ack_origin_commitment"),
                "state": "dispatched",
                "dispatched_at": event["occurred_at"],
            }
        if event_type in _WORKER_EVENTS:
            worker = snapshot["worker"]
            assert isinstance(worker, dict)
            payload = event["payload"]
            if event_type == "WorkerAcknowledged":
                worker.update(
                    {
                        "state": "acknowledged",
                        "acknowledged_at": payload["acknowledged_at"],
                        "lease_expires_at": payload["lease_expires_at"],
                        "packet_id": payload["packet_id"],
                        "packet_hash": payload["packet_hash"],
                        "instruction_binding": payload["instruction_binding"],
                        "ack_decision": payload["ack_decision"],
                        "ack_origin_proof_digest": payload["ack_origin_proof_digest"],
                        "ack_origin_proof_consumed": True,
                    }
                )
            elif event_type == "WorkerLeaseRenewed":
                worker.update(
                    {
                        "state": "active",
                        "heartbeat_id": payload["heartbeat_id"],
                        "heartbeat_at": payload["heartbeat_at"],
                        "lease_expires_at": payload["lease_expires_at"],
                    }
                )
            elif event_type == "WorkerLeaseExpired":
                worker.update(
                    {
                        "state": "lease_expired",
                        "expired_at": payload["expired_at"],
                        "lease_expires_at": payload["lease_expires_at"],
                        "reason": payload.get("reason"),
                    }
                )
            else:
                worker.update(
                    {
                        "state": "reclaimed",
                        "reclaimed_at": payload["reclaimed_at"],
                        "reason": payload.get("reason"),
                        "successor_worker_id": payload["successor_worker_id"],
                        "successor_dispatch_id": payload["successor_dispatch_id"],
                    }
                )
            snapshot["worker_events"].append(
                {
                    "event_id": event["event_id"],
                    "event_type": event_type,
                    "dispatch_id": payload["dispatch_id"],
                    "worker_id": payload["worker_id"],
                    "occurred_at": event["occurred_at"],
                }
            )
        if event_type == "EvidenceRecorded":
            evidence_id = event["payload"]["evidence_id"]
            evidence = {
                "evidence_id": evidence_id,
                "kind": event["payload"].get("kind"),
                "uri": event["payload"].get("uri"),
                "sha256": event["payload"].get("sha256"),
                "event_id": event["event_id"],
            }
            # Keep the external receipt's provenance and decision context
            # queryable without copying provider output into the projection.
            for key in (
                "evidence_schema",
                "receipt_id",
                "resource_id",
                "trace_id",
                "result_state",
                "observed_at",
                "provenance_ref",
                "policy_digest",
                "decision_factors",
                "step_run_id",
            ):
                if key in event["payload"]:
                    evidence[key] = event["payload"][key]
            snapshot["evidence"][evidence_id] = evidence
        if event_type in {"ApprovalRequested", "ApprovalGranted", "ApprovalTimeout"}:
            approval_id = event["payload"].get("approval_id") or "workflow"
            if event_type == "ApprovalRequested":
                snapshot["approvals"][approval_id] = {
                    "approval_id": approval_id,
                    "state": "requested",
                    "requested_at": event["payload"].get("requested_at"),
                    "timeout_at": event["payload"].get("timeout_at"),
                    "event_id": event["event_id"],
                }
            elif event_type == "ApprovalGranted":
                existing = snapshot["approvals"].get(approval_id, {})
                existing.update(
                    {
                        "approval_id": approval_id,
                        "state": "granted",
                        "event_id": event["event_id"],
                    }
                )
                snapshot["approvals"][approval_id] = existing
            else:
                existing = snapshot["approvals"].get(approval_id, {})
                existing.update(
                    {
                        "approval_id": approval_id,
                        "state": "timed_out",
                        "event_id": event["event_id"],
                    }
                )
                snapshot["approvals"][approval_id] = existing
    return snapshot


class WorkflowMeshStore:
    """OMO 中 Workflow Mesh 事件的 append-only 存储和投影入口。"""

    def __init__(self, omo_dir: Path | str) -> None:
        self.omo_dir = Path(omo_dir)
        self.log_path = self.omo_dir / WORKFLOW_MESH_LOG
        self._lock = fcntl_lock(self.log_path.with_suffix(self.log_path.suffix + ".lock"))
        self._log = AppendOnlyLog(
            self.log_path,
            lock=self._lock,
        )

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._log.read_all()

    def append(self, event: dict[str, Any]) -> dict[str, Any]:
        validate_workflow_event(event)
        if event["event_type"] == "WorkerAcknowledged" and (
            event["payload"].get("ack_decision") != "stop" or event["payload"].get("packet_id") is not None
        ):
            raise WorkflowMeshEventError("WorkerAcknowledged requires authenticated worker append")
        if event["event_type"] == "WorkflowSucceeded":
            with self._lock:
                current = self._log.read_all()
                snapshot = project_workflow_run(current, event["workflow_run_id"])
                if isinstance(snapshot.get("exact_request_identity"), Mapping):
                    raise WorkflowMeshEventError(
                        "exact WorkflowSucceeded requires authenticated worker completion append"
                    )
                return self._append_locked(event)
        if event["event_type"] in {
            "StepDispatched",
            "WorkerLeaseRenewed",
            "WorkerLeaseExpired",
            "WorkerReclaimed",
        }:
            with self._lock:
                current = self._log.read_all()
                snapshot = project_workflow_run(current, event["workflow_run_id"])
                if isinstance(snapshot.get("exact_request_identity"), Mapping):
                    if event["event_type"] == "StepDispatched":
                        raise WorkflowMeshEventError(
                            "exact StepDispatched requires authenticated exact dispatch append"
                        )
                    if event["event_type"] == "WorkerLeaseRenewed":
                        raise WorkflowMeshEventError(
                            "exact WorkerLeaseRenewed requires authenticated exact lease renewal append"
                        )
                    raise WorkflowMeshEventError(
                        f"exact {event['event_type']} requires authenticated coordinator append"
                    )
                return self._append_locked(event)
        with self._lock:
            return self._append_locked(event)

    def _append_locked(self, event: dict[str, Any]) -> dict[str, Any]:
        current = self._log.read_all()
        for existing in current:
            if (
                existing.get("event_id") != event["event_id"]
                and existing.get("idempotency_key") != event["idempotency_key"]
            ):
                continue
            if existing == event:
                return existing
            raise WorkflowMeshEventError(
                f"Conflicting duplicate Workflow Mesh event: {event['event_id']} / {event['idempotency_key']}"
            )
        run_id = event["workflow_run_id"]
        candidate = [*current, event]
        project_workflow_run(candidate, run_id)
        return self._log.append(event, sort_keys=True)

    def append_worker_ack(self, event: dict[str, Any], *, origin_proof: str) -> dict[str, Any]:
        """Atomically authenticate and consume a dispatch-scoped worker capability."""
        validate_workflow_event(event)
        if event["event_type"] != "WorkerAcknowledged":
            raise WorkflowMeshEventError("authenticated append only accepts WorkerAcknowledged")
        if not origin_proof:
            raise WorkflowMeshEventError("WorkerAcknowledged requires worker origin proof")
        with self._lock:
            current = self._log.read_all()
            for existing in current:
                if existing.get("idempotency_key") == event["idempotency_key"]:
                    raise WorkflowMeshEventError("worker ACK origin proof already consumed")
            snapshot = project_workflow_run(current, event["workflow_run_id"])
            _require_live_exact_admission(snapshot, event)
            worker = snapshot.get("worker")
            if not isinstance(worker, Mapping):
                raise WorkflowMeshEventError("WorkerAcknowledged requires prior StepDispatched worker context")
            context = {
                **worker,
                "workflow_run_id": event["workflow_run_id"],
            }
            expected = worker_ack_origin_digest(origin_proof, context)
            commitment = str(worker.get("ack_origin_commitment") or "")
            if not hmac.compare_digest(expected, commitment):
                raise WorkflowMeshEventError("worker ACK origin proof mismatch")
            if event["payload"].get("ack_origin_proof_digest") != commitment:
                raise WorkflowMeshEventError("worker ACK origin proof digest mismatch")
            return self._append_locked(event)

    def append_exact_step_dispatch(self, event: dict[str, Any], *, origin_proof: str) -> dict[str, Any]:
        """Atomically authenticate one exact coordinator-to-worker dispatch."""
        validate_workflow_event(event)
        if event["event_type"] != "StepDispatched" or not origin_proof:
            raise WorkflowMeshEventError("authenticated exact dispatch requires StepDispatched and origin proof")
        with self._lock:
            current = self._log.read_all()
            snapshot = project_workflow_run(current, event["workflow_run_id"])
            if not isinstance(snapshot.get("exact_request_identity"), Mapping):
                raise WorkflowMeshEventError("authenticated exact dispatch requires exact request")
            _require_live_exact_admission(snapshot, event)
            payload = event.get("payload", {})
            if not payload.get("ack_origin_nonce"):
                raise WorkflowMeshEventError("exact dispatch origin nonce is required")
            expected = worker_ack_origin_digest(
                origin_proof,
                {**payload, "workflow_run_id": event["workflow_run_id"]},
            )
            if not hmac.compare_digest(expected, str(payload.get("ack_origin_commitment") or "")):
                raise WorkflowMeshEventError("exact dispatch origin proof mismatch")
            return self._append_locked(event)

    def append_exact_worker_lease(self, event: dict[str, Any], *, origin_proof: str) -> dict[str, Any]:
        """Atomically validate, cap, and append one exact worker lease renewal."""
        validate_workflow_event(event)
        if event["event_type"] != "WorkerLeaseRenewed":
            raise WorkflowMeshEventError("exact lease renewal append requires WorkerLeaseRenewed")
        with self._lock:
            current = self._log.read_all()
            snapshot = project_workflow_run(current, event["workflow_run_id"])
            if not isinstance(snapshot.get("exact_request_identity"), Mapping):
                raise WorkflowMeshEventError("exact lease renewal requires exact request")
            _require_exact_worker_origin_proof(
                snapshot,
                event["workflow_run_id"],
                origin_proof,
                purpose="worker lease renewal",
            )
            _require_live_exact_admission(snapshot, event)
            admission = snapshot.get("admission")
            if not isinstance(admission, Mapping):
                raise WorkflowMeshEventError("exact lease renewal requires persisted admission")
            capped = dict(event)
            payload = dict(event["payload"])
            _require_live_exact_admission(
                snapshot,
                {**event, "occurred_at": payload.get("heartbeat_at")},
            )
            admission_expiry = _parsed_utc_timestamp(admission.get("expires_at"))
            requested_expiry = _parsed_utc_timestamp(payload.get("lease_expires_at"))
            if requested_expiry > admission_expiry:
                payload["lease_expires_at"] = admission_expiry.isoformat().replace("+00:00", "Z")
            capped["payload"] = payload
            for existing in current:
                if existing.get("idempotency_key") != capped["idempotency_key"]:
                    continue
                if existing.get("event_type") == "WorkerLeaseRenewed" and existing.get("payload") == payload:
                    return existing
                raise WorkflowMeshEventError("conflicting exact worker lease renewal")
            return self._append_locked(capped)

    def append_exact_worker_expiry(
        self,
        event: dict[str, Any],
        *,
        coordinator_capability: object,
    ) -> dict[str, Any]:
        """Atomically authenticate one exact worker lease expiry."""
        validate_workflow_event(event)
        if event["event_type"] != "WorkerLeaseExpired":
            raise WorkflowMeshEventError("exact expiry append requires WorkerLeaseExpired")
        with self._lock:
            _require_exact_coordinator_capability(
                coordinator_capability,
                purpose="worker lease expiry",
            )
            current = self._log.read_all()
            snapshot = project_workflow_run(current, event["workflow_run_id"])
            if not isinstance(snapshot.get("exact_request_identity"), Mapping):
                raise WorkflowMeshEventError("exact worker expiry requires exact request")
            worker = snapshot.get("worker")
            payload = event.get("payload", {})
            if not isinstance(worker, Mapping) or any(
                payload.get(field) != worker.get(field)
                for field in ("dispatch_id", "worker_id", "step_run_id", "admission_id")
            ):
                raise WorkflowMeshEventError("exact worker expiry context mismatch")
            for existing in current:
                if existing.get("idempotency_key") != event["idempotency_key"]:
                    continue
                if existing.get("event_type") == "WorkerLeaseExpired" and existing.get("payload") == event["payload"]:
                    return existing
                raise WorkflowMeshEventError("conflicting exact worker lease expiry")
            if worker.get("state") not in {"acknowledged", "active"}:
                raise WorkflowMeshEventError("exact worker expiry requires live worker lease")
            current_lease = str(worker.get("lease_expires_at") or "")
            if payload.get("lease_expires_at") != current_lease:
                raise WorkflowMeshEventError("exact worker expiry current lease mismatch")
            lease_expiry = _parsed_utc_timestamp(current_lease)
            expired_at = _parsed_utc_timestamp(payload.get("expired_at"))
            if expired_at < lease_expiry or datetime.now(UTC) < lease_expiry:
                raise WorkflowMeshEventError("exact worker lease has not expired")
            return self._append_locked(event)

    def append_exact_worker_reclaim(
        self,
        event: dict[str, Any],
        *,
        coordinator_capability: object,
    ) -> dict[str, Any]:
        """Atomically authenticate one exact coordinator reclaim."""
        validate_workflow_event(event)
        if event["event_type"] != "WorkerReclaimed":
            raise WorkflowMeshEventError("exact reclaim append requires WorkerReclaimed")
        with self._lock:
            _require_exact_coordinator_capability(
                coordinator_capability,
                purpose="worker reclaim",
            )
            current = self._log.read_all()
            snapshot = project_workflow_run(current, event["workflow_run_id"])
            if not isinstance(snapshot.get("exact_request_identity"), Mapping):
                raise WorkflowMeshEventError("exact worker reclaim requires exact request")
            worker = snapshot.get("worker")
            payload = event.get("payload", {})
            if not isinstance(worker, Mapping) or any(
                payload.get(field) != worker.get(field)
                for field in ("dispatch_id", "worker_id", "step_run_id", "admission_id")
            ):
                raise WorkflowMeshEventError("exact worker reclaim context mismatch")
            for existing in current:
                if existing.get("idempotency_key") != event["idempotency_key"]:
                    continue
                if existing.get("event_type") == "WorkerReclaimed" and existing.get("payload") == event["payload"]:
                    return existing
                raise WorkflowMeshEventError("conflicting exact worker reclaim")
            if worker.get("state") != "lease_expired":
                raise WorkflowMeshEventError("exact worker reclaim requires expired lease")
            return self._append_locked(event)

    def append_worker_completion(self, event: dict[str, Any], *, origin_proof: str) -> dict[str, Any]:
        """Authenticate one exact worker completion with its held origin proof."""
        validate_workflow_event(event)
        if event["event_type"] != "WorkflowSucceeded" or not origin_proof:
            raise WorkflowMeshEventError("authenticated completion requires WorkflowSucceeded and origin proof")
        with self._lock:
            current = self._log.read_all()
            snapshot = project_workflow_run(current, event["workflow_run_id"])
            if not isinstance(snapshot.get("exact_request_identity"), Mapping):
                raise WorkflowMeshEventError("authenticated completion requires an exact workflow request")
            _require_live_exact_admission(snapshot, event)
            worker = snapshot.get("worker")
            receipt = event.get("payload", {}).get("worker_completion_receipt")
            if not isinstance(worker, Mapping) or not isinstance(receipt, Mapping):
                raise WorkflowMeshEventError("authenticated completion requires worker context and receipt")
            ack_context = {
                **worker,
                "workflow_run_id": event["workflow_run_id"],
                "result_digest": None,
            }
            expected_ack = worker_ack_origin_digest(origin_proof, ack_context)
            if not hmac.compare_digest(expected_ack, str(worker.get("ack_origin_proof_digest") or "")):
                raise WorkflowMeshEventError("worker completion origin proof does not match durable ACK")
            completion_context = {
                **worker,
                "workflow_run_id": event["workflow_run_id"],
                "result_digest": receipt.get("result_digest"),
            }
            expected_completion = worker_ack_origin_digest(origin_proof, completion_context)
            if not hmac.compare_digest(
                expected_completion,
                str(receipt.get("completion_origin_commitment") or ""),
            ):
                raise WorkflowMeshEventError("worker completion origin commitment mismatch")
            for existing in current:
                if existing.get("idempotency_key") != event["idempotency_key"]:
                    continue
                if existing.get("event_type") == event["event_type"] and existing.get("payload") == event["payload"]:
                    return existing
                raise WorkflowMeshEventError("conflicting authenticated worker completion")
            return self._append_locked(event)

    def snapshot(self, workflow_run_id: str) -> dict[str, Any]:
        return project_workflow_run(self.events(), workflow_run_id)

    def step_snapshot(self, workflow_run_id: str, step_run_id: str) -> dict[str, Any] | None:
        """Return the projected StepRun owned by a WorkflowRun."""
        return self.snapshot(workflow_run_id)["step_runs"].get(step_run_id)

    def evidence_snapshot(self, workflow_run_id: str, evidence_id: str) -> dict[str, Any] | None:
        """Return one evidence projection owned by a WorkflowRun."""
        return self.snapshot(workflow_run_id)["evidence"].get(evidence_id)

    def worker_snapshot(self, workflow_run_id: str) -> dict[str, Any] | None:
        """Return the current durable worker lease projection for a run."""
        return self.snapshot(workflow_run_id).get("worker")

    def snapshots(self) -> list[dict[str, Any]]:
        """返回所有运行快照，顺序按事件日志中最后一次出现的顺序。"""
        events = self.events()
        run_ids = list(dict.fromkeys(event.get("workflow_run_id") for event in events if event.get("workflow_run_id")))
        last_indexes = {
            str(run_id): index
            for index, event in enumerate(events)
            for run_id in [event.get("workflow_run_id")]
            if run_id
        }
        snapshots = []
        for run_id in run_ids:
            snapshot = project_workflow_run(events, str(run_id))
            snapshot["event_sequence"] = last_indexes[str(run_id)]
            snapshots.append(snapshot)
        return sorted(snapshots, key=lambda snapshot: snapshot["event_sequence"])

    def sink(self) -> Callable[[dict[str, Any]], dict[str, Any]]:
        return self.append


__all__ = [
    "EVENT_STATE",
    "TERMINAL_STATES",
    "TOOL_OUTCOMES",
    "WORKFLOW_MESH_LOG",
    "WorkflowMeshEventError",
    "WorkflowMeshStore",
    "new_workflow_event",
    "project_workflow_run",
    "validate_workflow_event",
]
