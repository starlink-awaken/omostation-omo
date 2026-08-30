"""Durable worker acknowledgement and lease lifecycle for Workflow Mesh.

The dispatch YAML remains an operator-facing artifact. These functions make
the worker lifecycle durable by recording it in the same append-only event log
as admission, step progress, recovery, and evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .orchestration_contract import OrchestrationContractError, validate_capability_requirements
from .workflow_mesh import (
    EXACT_REQUEST_DISCRIMINATOR,
    WorkflowMeshEventError,
    WorkflowMeshStore,
    new_workflow_event,
    worker_ack_origin_digest,
)


class WorkerLifecycleError(ValueError):
    """A worker lifecycle transition failed its mesh contract."""


def new_worker_ack_origin_proof() -> str:
    """Return a high-entropy capability delivered only to the worker transport."""
    return secrets.token_urlsafe(32)


def _utc(value: str | None = None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _stamp(value: str | None = None) -> str:
    return _utc(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _store(omo_dir: Path | str) -> WorkflowMeshStore:
    return WorkflowMeshStore(omo_dir)


def _remaining_exact_admission_seconds(
    admission: Mapping[str, Any],
    *,
    now: str | None = None,
) -> float:
    try:
        issued_at = datetime.fromisoformat(str(admission.get("issued_at") or "").replace("Z", "+00:00"))
        expires_at = datetime.fromisoformat(str(admission.get("expires_at") or "").replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise WorkerLifecycleError("exact admission time window is invalid") from exc
    if issued_at.tzinfo is None or expires_at.tzinfo is None:
        raise WorkerLifecycleError("exact admission time window is invalid")
    issued_at = issued_at.astimezone(UTC)
    expires_at = expires_at.astimezone(UTC)
    observed_at = _utc(now)
    if expires_at <= issued_at:
        raise WorkerLifecycleError("exact admission time window is invalid")
    if observed_at < issued_at:
        raise WorkerLifecycleError("exact admission is not yet valid")
    if observed_at >= expires_at:
        raise WorkerLifecycleError("exact admission is expired")
    return (expires_at - observed_at).total_seconds()


def _validate_exact_admission_window(admission: Mapping[str, Any]) -> None:
    _remaining_exact_admission_seconds(admission)


def _existing(store: WorkflowMeshStore, idempotency_key: str) -> dict[str, Any] | None:
    for event in store.events():
        if event.get("idempotency_key") == idempotency_key:
            return event
    return None


def _append(
    store: WorkflowMeshStore,
    event_type: str,
    workflow_run_id: str,
    *,
    trace_id: str,
    producer: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    prior = _existing(store, idempotency_key)
    if prior is not None:
        if prior.get("event_type") != event_type or prior.get("payload") != payload:
            raise WorkerLifecycleError(f"conflicting worker lifecycle event: {idempotency_key}")
        return prior
    try:
        return store.append(
            new_workflow_event(
                event_type,
                workflow_run_id,
                trace_id=trace_id,
                producer=producer,
                idempotency_key=idempotency_key,
                payload=payload,
            )
        )
    except WorkflowMeshEventError as exc:
        raise WorkerLifecycleError(str(exc)) from exc


def _validate_context(
    store: WorkflowMeshStore,
    *,
    workflow_run_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
) -> dict[str, Any]:
    snapshot = store.snapshot(workflow_run_id)
    if snapshot.get("state") in {"unknown", "planned", "closed", "cancelled"}:
        raise WorkerLifecycleError(f"workflow is not dispatchable for worker lifecycle: {snapshot.get('state')}")
    admission = snapshot.get("admission")
    if not isinstance(admission, dict) or admission.get("admission_id") != admission_id:
        raise WorkerLifecycleError("worker lifecycle admission_id mismatch")
    step = snapshot.get("step_runs", {}).get(step_run_id)
    if not isinstance(step, dict):
        raise WorkerLifecycleError(f"unknown admitted StepRun: {step_run_id}")
    if step.get("admission_id") != admission_id:
        raise WorkerLifecycleError("worker lifecycle StepRun admission mismatch")
    if not dispatch_id or not worker_id:
        raise WorkerLifecycleError("dispatch_id and worker_id are required")
    return snapshot


def record_step_dispatch(
    omo_dir: Path | str,
    *,
    workflow_run_id: str,
    trace_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
    policy_digest: str,
    packet_id: str | None = None,
    packet_hash: str | None = None,
    instruction_binding: Mapping[str, Any] | None = None,
    ack_origin_proof: str | None = None,
    step_name: str = "execute",
) -> dict[str, Any]:
    """Persist the coordinator-to-worker dispatch edge exactly once."""
    store = _store(omo_dir)
    snapshot = store.snapshot(workflow_run_id)
    admission = snapshot.get("admission")
    request_identity = admission.get("request_identity") if isinstance(admission, Mapping) else None
    exact_request_identity = snapshot.get("exact_request_identity")
    exact_admission = isinstance(exact_request_identity, Mapping)
    try:
        requirements = validate_capability_requirements(
            request_identity.get("capability_requirements") if isinstance(request_identity, Mapping) else None
        )
    except OrchestrationContractError:
        requirements = []
    canonical_requirements = json.dumps(
        requirements,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    requirements_digest = "sha256:" + hashlib.sha256(canonical_requirements.encode()).hexdigest()
    validated_persisted_proof = (
        hashlib.sha256(
            json.dumps(
                {key: value for key, value in admission.items() if key != "proof"},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if isinstance(admission, Mapping)
        else None
    )
    admitted_steps = admission.get("step_run_ids") if isinstance(admission, Mapping) else None
    if (
        # Mesh-legal StepDispatched origins: admitted, dispatched (renewal
        # self-loop), running (WorkerReclaimed replay).
        snapshot.get("state") not in {"admitted", "dispatched", "running"}
        or not isinstance(admission, dict)
        or not isinstance(request_identity, Mapping)
        or admission.get("admission_id") != admission_id
        or admission.get("workflow_run_id") != workflow_run_id
        or admission.get("policy_digest") != policy_digest
        or request_identity.get("packet_id") != packet_id
        or request_identity.get("packet_hash") != packet_hash
        or exact_admission
        and (
            admission.get("workflow_run_id") != workflow_run_id
            or request_identity != exact_request_identity
            or admission.get("exact_request_discriminator") != EXACT_REQUEST_DISCRIMINATOR
            or request_identity.get("bet_id") != exact_request_identity.get("bet_id")
            or request_identity.get("workflow_id") != exact_request_identity.get("workflow_id")
            or not isinstance(request_identity.get("assignment_id"), str)
            or not request_identity.get("assignment_id")
            or not isinstance(request_identity.get("dispatch_id"), str)
            or not request_identity.get("dispatch_id")
            or dispatch_id != exact_request_identity.get("dispatch_id")
            or request_identity.get("workflow_run_id") != workflow_run_id
            or request_identity.get("correlation_id") != workflow_run_id
            or not isinstance(request_identity.get("actor_id"), str)
            or not request_identity.get("actor_id")
            or not isinstance(request_identity.get("delivery_attempt_id"), str)
            or not request_identity.get("delivery_attempt_id")
            or requirements != request_identity.get("capability_requirements")
            or request_identity.get("capability_requirements_digest") != requirements_digest
            or admission.get("proof") != validated_persisted_proof
            or not isinstance(admitted_steps, list)
            or not any(
                step_run_id == admitted_step or step_run_id.startswith(f"{admitted_step}:")
                for admitted_step in admitted_steps
            )
        )
    ):
        raise WorkerLifecycleError("admission binding mismatch")
    if exact_admission:
        _validate_exact_admission_window(admission)
    nonce = secrets.token_hex(16) if ack_origin_proof else None
    payload = {
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "step_run_id": step_run_id,
        "step_name": step_name,
        "admission_id": admission_id,
        "policy_digest": policy_digest,
        "packet_id": packet_id,
        "packet_hash": packet_hash,
        "instruction_binding": dict(instruction_binding) if instruction_binding is not None else None,
        "ack_origin_nonce": nonce,
    }
    if exact_admission:
        payload["exact_request_discriminator"] = EXACT_REQUEST_DISCRIMINATOR
        payload["bet_id"] = request_identity["bet_id"]
        payload["workflow_id"] = request_identity["workflow_id"]
        payload["capability_requirements_digest"] = requirements_digest
    if ack_origin_proof:
        payload["ack_origin_commitment"] = worker_ack_origin_digest(
            ack_origin_proof,
            {**payload, "workflow_run_id": workflow_run_id},
        )
    event = new_workflow_event(
        "StepDispatched",
        workflow_run_id,
        trace_id=trace_id,
        producer="omo.worker_lifecycle",
        idempotency_key=f"{workflow_run_id}:step-dispatched:{dispatch_id}",
        payload=payload,
    )
    if exact_admission:
        if not ack_origin_proof:
            raise WorkerLifecycleError("exact dispatch origin proof is required")
        try:
            return store.append_exact_step_dispatch(event, origin_proof=ack_origin_proof)
        except WorkflowMeshEventError as exc:
            raise WorkerLifecycleError(str(exc)) from exc
    prior = _existing(store, event["idempotency_key"])
    if prior is not None:
        if prior.get("event_type") != "StepDispatched" or prior.get("payload") != payload:
            raise WorkerLifecycleError(f"conflicting worker lifecycle event: {event['idempotency_key']}")
        return prior
    try:
        return store.append(event)
    except WorkflowMeshEventError as exc:
        raise WorkerLifecycleError(str(exc)) from exc


def acknowledge_worker(
    omo_dir: Path | str,
    *,
    workflow_run_id: str,
    trace_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
    packet_id: str | None = None,
    packet_hash: str | None = None,
    instruction_binding: Mapping[str, Any] | None = None,
    ack_decision: str = "stop",
    origin_proof: str | None = None,
    lease_seconds: int = 1200,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a worker ACK and establish its first durable lease."""
    if lease_seconds <= 0:
        raise WorkerLifecycleError("lease_seconds must be positive")
    legacy_observer_ack = packet_id is None and packet_hash is None and instruction_binding is None
    if not legacy_observer_ack and (
        not isinstance(packet_id, str)
        or not packet_id.startswith("WP-")
        or not isinstance(packet_hash, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", packet_hash)
    ):
        raise WorkerLifecycleError("worker ACK packet binding is invalid")
    instruction_fields = {
        "instruction_ref",
        "instruction_version",
        "content_digest",
        "instruction_profile",
    }
    if not legacy_observer_ack and (
        not isinstance(instruction_binding, Mapping) or set(instruction_binding) != instruction_fields
    ):
        raise WorkerLifecycleError("worker ACK instruction binding is invalid")
    instruction = (
        {key: str(instruction_binding.get(key) or "").strip() for key in instruction_fields}
        if isinstance(instruction_binding, Mapping)
        else None
    )
    if not legacy_observer_ack and (
        instruction is None
        or not all(instruction.values())
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", instruction["content_digest"])
        or instruction["instruction_profile"] != "executor"
    ):
        raise WorkerLifecycleError("worker ACK instruction binding is invalid")
    if legacy_observer_ack and ack_decision != "stop":
        raise WorkerLifecycleError("unbound worker ACK must stop")
    if ack_decision not in {"proceed", "stop"}:
        raise WorkerLifecycleError("worker ACK decision must be proceed or stop")
    store = _store(omo_dir)
    event_key = f"{workflow_run_id}:worker-ack:{dispatch_id}"
    prior = _existing(store, event_key)
    if prior is not None:
        raise WorkerLifecycleError("worker ACK origin proof already consumed")
    snapshot = _validate_context(
        store,
        workflow_run_id=workflow_run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=admission_id,
    )
    worker = snapshot.get("worker")
    admission = snapshot.get("admission")
    exact_ack = isinstance(snapshot.get("exact_request_identity"), Mapping)
    if exact_ack:
        if not isinstance(admission, Mapping):
            raise WorkerLifecycleError("worker ACK requires exact admission")
        _remaining_exact_admission_seconds(admission, now=now)
    if not isinstance(worker, Mapping):
        raise WorkerLifecycleError("worker ACK requires dispatch context")
    if not legacy_observer_ack and (
        worker.get("packet_id") != packet_id
        or worker.get("packet_hash") != packet_hash
        or worker.get("instruction_binding") != instruction
    ):
        raise WorkerLifecycleError("worker ACK delivery binding mismatch")
    if not legacy_observer_ack and not origin_proof:
        raise WorkerLifecycleError("worker ACK origin proof is required")
    origin_commitment = str(worker.get("ack_origin_commitment") or "")
    if not legacy_observer_ack and not origin_commitment:
        raise WorkerLifecycleError("worker ACK dispatch has no origin proof commitment")
    acknowledged_at_value = _utc(now)
    acknowledged_at = _stamp(acknowledged_at_value.isoformat())
    requested_lease_expiry = acknowledged_at_value + timedelta(seconds=lease_seconds)
    if isinstance(snapshot.get("exact_request_identity"), Mapping):
        admission_expiry = _utc(str(admission.get("expires_at")))
        requested_lease_expiry = min(requested_lease_expiry, admission_expiry)
    lease_expires_at = _stamp(requested_lease_expiry.isoformat())
    payload = {
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "step_run_id": step_run_id,
        "admission_id": admission_id,
        "acknowledged_at": acknowledged_at,
        "lease_expires_at": lease_expires_at,
        "packet_id": packet_id,
        "packet_hash": packet_hash,
        "instruction_binding": instruction,
        "ack_decision": ack_decision,
        "ack_origin_proof_digest": origin_commitment or None,
    }
    event = new_workflow_event(
        "WorkerAcknowledged",
        workflow_run_id,
        trace_id=trace_id,
        producer="worker",
        idempotency_key=event_key,
        payload=payload,
    )
    try:
        if legacy_observer_ack:
            return store.append(event)
        return store.append_worker_ack(event, origin_proof=origin_proof or "")
    except WorkflowMeshEventError as exc:
        raise WorkerLifecycleError(str(exc)) from exc


def record_worker_completion(
    omo_dir: Path | str,
    *,
    workflow_run_id: str,
    trace_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
    origin_proof: str,
    result_digest: str,
) -> dict[str, Any]:
    """Persist one successful completion receipt from an ACKed worker."""
    result_digest = str(result_digest or "").lower()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", result_digest):
        raise WorkerLifecycleError("worker completion result_digest is invalid")
    store = _store(omo_dir)
    snapshot = _validate_context(
        store,
        workflow_run_id=workflow_run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=admission_id,
    )
    if snapshot.get("state") != "running":
        raise WorkerLifecycleError("worker completion requires a running StepRun")
    admission = snapshot.get("admission")
    exact_identity = snapshot.get("exact_request_identity")
    if isinstance(exact_identity, Mapping):
        if not isinstance(admission, Mapping):
            raise WorkerLifecycleError("worker completion requires exact admission")
        _validate_exact_admission_window(admission)
    worker = snapshot.get("worker")
    if (
        not isinstance(worker, Mapping)
        or worker.get("state") not in {"acknowledged", "active"}
        or worker.get("ack_decision") != "proceed"
        or worker.get("ack_origin_proof_consumed") is not True
    ):
        raise WorkerLifecycleError("worker completion requires authenticated proceed ACK")
    for field, expected in (
        ("dispatch_id", dispatch_id),
        ("worker_id", worker_id),
        ("step_run_id", step_run_id),
        ("admission_id", admission_id),
    ):
        if worker.get(field) != expected:
            raise WorkerLifecycleError(f"worker completion context mismatch: {field}")
    if not origin_proof:
        raise WorkerLifecycleError("worker completion origin proof is required")
    ack_context = {
        **worker,
        "workflow_run_id": workflow_run_id,
        "result_digest": None,
    }
    expected_ack = worker_ack_origin_digest(origin_proof, ack_context)
    if not secrets.compare_digest(expected_ack, str(worker.get("ack_origin_proof_digest") or "")):
        raise WorkerLifecycleError("worker completion origin proof does not match durable ACK")
    completion_context = {
        **worker,
        "workflow_run_id": workflow_run_id,
        "result_digest": result_digest,
    }
    receipt = {
        "status": "succeeded",
        **(
            {
                "exact_request_discriminator": EXACT_REQUEST_DISCRIMINATOR,
                "bet_id": exact_identity["bet_id"],
                "workflow_id": exact_identity["workflow_id"],
            }
            if isinstance(exact_identity, Mapping)
            else {}
        ),
        "workflow_run_id": workflow_run_id,
        "admission_id": admission_id,
        "step_run_id": step_run_id,
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "ack_origin_proof_digest": str(worker.get("ack_origin_proof_digest") or ""),
        "completion_origin_commitment": worker_ack_origin_digest(origin_proof, completion_context),
        "result_digest": result_digest,
    }
    receipt["receipt_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    event = new_workflow_event(
        "WorkflowSucceeded",
        workflow_run_id,
        trace_id=trace_id,
        producer="worker",
        idempotency_key=f"{workflow_run_id}:worker-completed:{dispatch_id}",
        payload={"worker_completion_receipt": receipt},
    )
    try:
        return store.append_worker_completion(event, origin_proof=origin_proof)
    except WorkflowMeshEventError as exc:
        raise WorkerLifecycleError(str(exc)) from exc


def renew_worker_lease(
    omo_dir: Path | str,
    *,
    workflow_run_id: str,
    trace_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
    lease_seconds: int = 1200,
    now: str | None = None,
    heartbeat_id: str | None = None,
) -> dict[str, Any]:
    """Renew a live lease; repeated heartbeat IDs are idempotent."""
    if lease_seconds <= 0:
        raise WorkerLifecycleError("lease_seconds must be positive")
    store = _store(omo_dir)
    snapshot = _validate_context(
        store,
        workflow_run_id=workflow_run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=admission_id,
    )
    current = snapshot.get("worker")
    if not isinstance(current, dict) or current.get("state") not in {
        "acknowledged",
        "active",
    }:
        raise WorkerLifecycleError("worker must ACK before renewing its lease")
    if current.get("dispatch_id") != dispatch_id or current.get("worker_id") != worker_id:
        raise WorkerLifecycleError("worker lease owner mismatch")
    heartbeat_value = _utc(now)
    requested_expiry = heartbeat_value + timedelta(seconds=lease_seconds)
    exact_renewal = isinstance(snapshot.get("exact_request_identity"), Mapping)
    if exact_renewal:
        admission = snapshot.get("admission")
        if not isinstance(admission, Mapping):
            raise WorkerLifecycleError("exact worker renewal requires persisted admission")
        _remaining_exact_admission_seconds(admission)
        _remaining_exact_admission_seconds(admission, now=now)
        requested_expiry = min(requested_expiry, _utc(str(admission.get("expires_at"))))
    heartbeat_at = _stamp(heartbeat_value.isoformat())
    lease_expires_at = _stamp(requested_expiry.isoformat())
    event_key = heartbeat_id or lease_expires_at
    idempotency_key = f"{workflow_run_id}:worker-heartbeat:{dispatch_id}:{event_key}"
    payload = {
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "step_run_id": step_run_id,
        "admission_id": admission_id,
        "heartbeat_id": event_key,
        "heartbeat_at": heartbeat_at,
        "lease_expires_at": lease_expires_at,
    }
    event = new_workflow_event(
        "WorkerLeaseRenewed",
        workflow_run_id,
        trace_id=trace_id,
        producer="worker",
        idempotency_key=idempotency_key,
        payload=payload,
    )
    if exact_renewal:
        try:
            return store.append_exact_worker_lease(event)
        except WorkflowMeshEventError as exc:
            raise WorkerLifecycleError(str(exc)) from exc
    prior = _existing(store, idempotency_key)
    if prior is not None:
        return prior
    try:
        return store.append(event)
    except WorkflowMeshEventError as exc:
        raise WorkerLifecycleError(str(exc)) from exc


def expire_worker_lease(
    omo_dir: Path | str,
    *,
    workflow_run_id: str,
    trace_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
    now: str | None = None,
    reason: str = "lease_expired",
) -> dict[str, Any]:
    """Mark an unresponsive worker unavailable only after its lease expires."""
    store = _store(omo_dir)
    event_key = f"{workflow_run_id}:worker-expired:{dispatch_id}"
    prior = _existing(store, event_key)
    if prior is not None:
        return prior
    snapshot = _validate_context(
        store,
        workflow_run_id=workflow_run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=admission_id,
    )
    current = snapshot.get("worker")
    if not isinstance(current, dict) or current.get("state") not in {
        "acknowledged",
        "active",
    }:
        raise WorkerLifecycleError("worker has no live lease to expire")
    if current.get("dispatch_id") != dispatch_id or current.get("worker_id") != worker_id:
        raise WorkerLifecycleError("worker lease owner mismatch")
    observed_at = _stamp(now)
    lease_expires_at = str(current.get("lease_expires_at", ""))
    if not lease_expires_at or _utc(observed_at) < _utc(lease_expires_at):
        raise WorkerLifecycleError("worker lease has not expired")
    payload = {
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "step_run_id": step_run_id,
        "admission_id": admission_id,
        "lease_expires_at": lease_expires_at,
        "expired_at": observed_at,
        "reason": reason,
    }
    return _append(
        store,
        "WorkerLeaseExpired",
        workflow_run_id,
        trace_id=trace_id,
        producer="omo.worker_lifecycle",
        idempotency_key=event_key,
        payload=payload,
    )


def reclaim_worker(
    omo_dir: Path | str,
    *,
    workflow_run_id: str,
    trace_id: str,
    dispatch_id: str,
    worker_id: str,
    step_run_id: str,
    admission_id: str,
    successor_worker_id: str,
    successor_dispatch_id: str,
    now: str | None = None,
    reason: str = "lease_expired",
) -> dict[str, Any]:
    """Record coordinator reclaim and successor assignment after expiry."""
    if not successor_worker_id or not successor_dispatch_id:
        raise WorkerLifecycleError("successor_worker_id and successor_dispatch_id are required")
    store = _store(omo_dir)
    event_key = f"{workflow_run_id}:worker-reclaim:{dispatch_id}:{successor_dispatch_id}"
    prior = _existing(store, event_key)
    if prior is not None:
        return prior
    snapshot = _validate_context(
        store,
        workflow_run_id=workflow_run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=admission_id,
    )
    current = snapshot.get("worker")
    if not isinstance(current, dict) or current.get("state") != "lease_expired":
        raise WorkerLifecycleError("worker must be lease_expired before reclaim")
    payload = {
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "step_run_id": step_run_id,
        "admission_id": admission_id,
        "successor_worker_id": successor_worker_id,
        "successor_dispatch_id": successor_dispatch_id,
        "reclaimed_at": _stamp(now),
        "reason": reason,
    }
    return _append(
        store,
        "WorkerReclaimed",
        workflow_run_id,
        trace_id=trace_id,
        producer="omo.worker_lifecycle",
        idempotency_key=event_key,
        payload=payload,
    )


def scan_worker_leases(
    omo_dir: Path | str,
    *,
    now: str | None = None,
    apply: bool = False,
    reason: str = "lease_expired",
) -> dict[str, Any]:
    """Find expired Mesh leases and optionally persist expiry events.

    The default is deliberately read-only so an operator, cron job, or UI can
    inspect the result without changing workflow state. ``apply=True`` only
    appends ``WorkerLeaseExpired``; successor selection remains a separate
    coordinator decision through :func:`reclaim_worker`.
    """
    if not str(reason).strip():
        raise WorkerLifecycleError("watchdog expiry reason is required")

    store = _store(omo_dir)
    observed_at = _stamp(now)
    run_ids = sorted(
        {
            str(event.get("workflow_run_id"))
            for event in store.events()
            if str(event.get("workflow_run_id") or "").strip()
        }
    )
    due: list[dict[str, Any]] = []
    expired: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    worker_count = 0

    for workflow_run_id in run_ids:
        try:
            snapshot = store.snapshot(workflow_run_id)
        except (WorkflowMeshEventError, OSError, ValueError) as exc:
            errors.append({"workflow_run_id": workflow_run_id, "error": str(exc)})
            continue

        worker = snapshot.get("worker")
        if not isinstance(worker, dict):
            continue
        worker_count += 1
        state = str(worker.get("state") or "unknown")
        if state not in {"acknowledged", "active"}:
            continue

        lease_expires_at = str(worker.get("lease_expires_at") or "")
        if not lease_expires_at:
            errors.append(
                {
                    "workflow_run_id": workflow_run_id,
                    "error": "live worker has no lease_expires_at",
                }
            )
            continue
        try:
            expired_now = _utc(observed_at) >= _utc(lease_expires_at)
        except ValueError as exc:
            errors.append({"workflow_run_id": workflow_run_id, "error": str(exc)})
            continue
        if not expired_now:
            continue

        context = {
            "workflow_run_id": workflow_run_id,
            "trace_id": str(snapshot.get("trace_id") or workflow_run_id),
            "dispatch_id": str(worker.get("dispatch_id") or ""),
            "worker_id": str(worker.get("worker_id") or ""),
            "step_run_id": str(worker.get("step_run_id") or ""),
            "admission_id": str(worker.get("admission_id") or ""),
            "lease_expires_at": lease_expires_at,
            "observed_at": observed_at,
        }
        if not all(context[key] for key in ("dispatch_id", "worker_id", "step_run_id", "admission_id")):
            errors.append(
                {
                    "workflow_run_id": workflow_run_id,
                    "error": "expired worker context is incomplete",
                }
            )
            continue
        if not apply:
            due.append({**context, "action": "would_expire"})
            continue

        try:
            event = expire_worker_lease(
                Path(omo_dir),
                workflow_run_id=workflow_run_id,
                trace_id=context["trace_id"],
                dispatch_id=context["dispatch_id"],
                worker_id=context["worker_id"],
                step_run_id=context["step_run_id"],
                admission_id=context["admission_id"],
                now=observed_at,
                reason=reason,
            )
        except WorkerLifecycleError as exc:
            errors.append({"workflow_run_id": workflow_run_id, "error": str(exc)})
            continue
        expired.append(
            {
                **context,
                "action": "expired",
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
            }
        )

    return {
        "schema": "workflow-mesh-watchdog/v1",
        "mode": "apply" if apply else "dry_run",
        "observed_at": observed_at,
        "run_count": len(run_ids),
        "worker_count": worker_count,
        "due_count": len(due),
        "expired_count": len(expired),
        "due": due,
        "expired": expired,
        "errors": errors,
    }


__all__ = [
    "WorkerLifecycleError",
    "acknowledge_worker",
    "expire_worker_lease",
    "reclaim_worker",
    "record_worker_completion",
    "record_step_dispatch",
    "renew_worker_lease",
    "scan_worker_leases",
]
