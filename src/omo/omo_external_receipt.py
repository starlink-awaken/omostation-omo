"""受治理的外部调用 receipt 到 Workflow Mesh 证据回写 broker。

执行方负责产生 credential-free receipt；OMO 负责校验上下文、规范化最小证据
payload，并以幂等的 ``EvidenceRecorded`` 事件写入 Workflow Mesh。这个模块不
调用 provider，也不接触外部原文或凭据。
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .workflow_mesh import WorkflowMeshStore, new_workflow_event

RECEIPT_SCHEMA = "external-connection-receipt/v1"
EVIDENCE_KIND = "external_connection"
_REQUIRED_FIELDS = frozenset(
    {
        "receipt_id",
        "trace_id",
        "resource_id",
        "operation",
        "result_state",
        "observed_at",
        "provenance_ref",
        "policy_digest",
    }
)
_EVIDENCE_STATES = frozenset({"succeeded", "degraded"})
_NATIVE_EXECUTION_SCHEMA = "native-execution-receipt/v1"
_NATIVE_MATERIAL_SCHEMA = "native-execution-material/v1"
_FORBIDDEN_KEYS = frozenset(
    {
        "access_token",
        "content",
        "input_data",
        "output",
        "output_data",
        "password",
        "private_key",
        "raw_content",
        "raw_input",
        "raw_output",
        "refresh_token",
        "secret",
    }
)


class ExternalReceiptError(ValueError):
    """Receipt 不能安全地成为 Workflow Mesh 证据。"""


def _as_mapping(receipt: Mapping[str, Any] | Any) -> Mapping[str, Any]:
    if isinstance(receipt, Mapping):
        return receipt
    to_dict = getattr(receipt, "to_dict", None)
    if callable(to_dict):
        value = to_dict()
        if isinstance(value, Mapping):
            return value
    raise ExternalReceiptError("receipt must be a mapping or expose to_dict()")


def _reject_forbidden(value: Any, path: str = "receipt") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key).lower() in _FORBIDDEN_KEYS:
                raise ExternalReceiptError(f"forbidden raw or secret field: {path}.{key}")
            _reject_forbidden(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_forbidden(nested, f"{path}[{index}]")


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ExternalReceiptError(f"receipt missing required field: {field_name}")
    return text


def _validate_timestamp(value: Any) -> str:
    text = _required_text(value, "observed_at")
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExternalReceiptError(f"invalid receipt timestamp: {text}") from exc
    return text


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_digest(value: Any) -> str | None:
    if value in (None, ""):
        return None
    digest = str(value).strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ExternalReceiptError("output_digest must be a SHA-256 hex digest")
    return digest


def _normalise_receipt(receipt: Mapping[str, Any] | Any) -> dict[str, Any]:
    value = dict(_as_mapping(receipt))
    _reject_forbidden(value)
    missing = sorted(_REQUIRED_FIELDS - value.keys())
    if missing:
        raise ExternalReceiptError(f"receipt missing required fields: {missing}")
    result_state = _required_text(value["result_state"], "result_state").lower()
    if result_state not in _EVIDENCE_STATES:
        raise ExternalReceiptError(
            f"only succeeded/degraded receipts may become EvidenceRecorded; received {result_state!r}"
        )
    decision_factors = value.get("decision_factors", {})
    if not isinstance(decision_factors, Mapping):
        raise ExternalReceiptError("decision_factors must be an object")
    output_digest = _validate_digest(value.get("output_digest"))
    if result_state == "succeeded" and output_digest is None:
        raise ExternalReceiptError("succeeded receipt requires output_digest")
    return {
        "receipt_id": _required_text(value["receipt_id"], "receipt_id"),
        "trace_id": _required_text(value["trace_id"], "trace_id"),
        "resource_id": _required_text(value["resource_id"], "resource_id"),
        "operation": _required_text(value["operation"], "operation"),
        "result_state": result_state,
        "observed_at": _validate_timestamp(value["observed_at"]),
        "provenance_ref": _required_text(value["provenance_ref"], "provenance_ref"),
        "policy_digest": _required_text(value["policy_digest"], "policy_digest"),
        "decision_factors": dict(decision_factors),
        "output_digest": output_digest,
        "error_code": str(value.get("error_code") or "").strip() or None,
    }


def _event_id(workflow_run_id: str, receipt_id: str) -> str:
    material = f"{workflow_run_id}\n{receipt_id}".encode()
    return f"external-evidence:{hashlib.sha256(material).hexdigest()}"


def record_external_receipt(
    omo_dir: Path | str,
    receipt: Mapping[str, Any] | Any,
    *,
    workflow_run_id: str,
    step_run_id: str | None = None,
    producer: str = "external-connection-fabric",
) -> dict[str, Any]:
    """将成功/降级 receipt 幂等回写为一个 ``EvidenceRecorded`` 事件。

    只接受已经完成外部调用的最小 receipt。它不会替调用方执行 admission、
    provider 或补偿；这些仍由各自控制面负责。
    """
    run_id = _required_text(workflow_run_id, "workflow_run_id")
    step = str(step_run_id or "").strip() or None
    normalized = _normalise_receipt(receipt)
    evidence_id = f"external:{normalized['resource_id']}:{normalized['receipt_id']}"
    payload: dict[str, Any] = {
        "evidence_id": evidence_id,
        "evidence_schema": RECEIPT_SCHEMA,
        "kind": EVIDENCE_KIND,
        "uri": f"external://{normalized['resource_id']}/{normalized['operation']}",
        "sha256": normalized["output_digest"],
        "resource_id": normalized["resource_id"],
        "trace_id": normalized["trace_id"],
        "workflow_run_id": run_id,
        "result_state": normalized["result_state"],
        "observed_at": normalized["observed_at"],
        "provenance_ref": normalized["provenance_ref"],
        "policy_digest": normalized["policy_digest"],
        "receipt_id": normalized["receipt_id"],
        "decision_factors": normalized["decision_factors"],
    }
    if normalized["error_code"]:
        payload["error_code"] = normalized["error_code"]
    if step:
        payload["step_run_id"] = step
    event = new_workflow_event(
        "EvidenceRecorded",
        run_id,
        trace_id=normalized["trace_id"],
        producer=producer,
        payload=payload,
        idempotency_key=f"external-evidence:{run_id}:{normalized['receipt_id']}",
    )
    # Receipt identity and observed time are stable, so retries create the exact
    # same event and WorkflowMeshStore can return the existing append-only record.
    event["event_id"] = _event_id(run_id, normalized["receipt_id"])
    event["occurred_at"] = normalized["observed_at"]
    return WorkflowMeshStore(omo_dir).append(event)


def _native_digest(value: Any, field_name: str) -> str:
    text = _required_text(value, field_name).lower()
    if len(text) != 71 or not text.startswith("sha256:") or any(char not in "0123456789abcdef" for char in text[7:]):
        raise ExternalReceiptError(f"{field_name} must be a sha256 digest")
    return text


def _normalise_native_execution_receipt(
    receipt: Mapping[str, Any] | Any,
    *,
    workflow_run_id: str,
    step_run_id: str | None,
) -> dict[str, Any]:
    value = dict(_as_mapping(receipt))
    _reject_forbidden(value)
    if value.get("schema") != _NATIVE_EXECUTION_SCHEMA or value.get("status") != "completed":
        raise ExternalReceiptError("native receipt schema or status is invalid")
    if value.get("value_indicator_policy") is not False:
        raise ExternalReceiptError("native receipt value promotion is forbidden")
    if value.get("fallback") != {"used": False}:
        raise ExternalReceiptError("native receipt fallback is forbidden")
    if value.get("states") != {"invoked": True, "evidenced": False, "independently_verified": False}:
        raise ExternalReceiptError("native receipt states are not eligible")
    if value.get("transport_state") != "confirmed":
        raise ExternalReceiptError("only confirmed succeeded native receipts are consumable")

    material = value.get("material")
    if not isinstance(material, Mapping) or material.get("schema") != _NATIVE_MATERIAL_SCHEMA:
        raise ExternalReceiptError("native receipt material is invalid")
    binding = material.get("binding")
    if not isinstance(binding, Mapping):
        raise ExternalReceiptError("native receipt binding is invalid")
    bound_run_id = _required_text(binding.get("workflow_run_id"), "material.binding.workflow_run_id")
    if bound_run_id != workflow_run_id:
        raise ExternalReceiptError("native receipt workflow_run_id does not match consumer context")

    admission = material.get("admission")
    if not isinstance(admission, Mapping):
        raise ExternalReceiptError("native receipt admission is invalid")
    bound_step = _required_text(admission.get("step_run_id"), "material.admission.step_run_id")
    if step_run_id is not None and bound_step != step_run_id:
        raise ExternalReceiptError("native receipt step_run_id does not match consumer context")

    capability = material.get("capability")
    if not isinstance(capability, Mapping):
        raise ExternalReceiptError("native receipt capability is invalid")
    capability_id = _required_text(capability.get("id"), "material.capability.id")
    capability_kind = _required_text(capability.get("kind"), "material.capability.kind")
    operation_id = _required_text(material.get("operation_id"), "material.operation_id")
    invocation_id = _native_digest(value.get("invocation_id"), "invocation_id")
    receipt_digest = _native_digest(value.get("receipt_digest"), "receipt_digest")

    outcome = value.get("outcome")
    if (
        not isinstance(outcome, Mapping)
        or outcome.get("status") != "succeeded"
        or outcome.get("failure_code") is not None
    ):
        raise ExternalReceiptError("only confirmed succeeded native receipts are consumable")
    result_digest = _native_digest(outcome.get("result_digest"), "outcome.result_digest")
    return {
        "receipt_id": receipt_digest,
        "trace_id": bound_run_id,
        "resource_id": capability_id,
        "operation": operation_id,
        "result_state": "succeeded",
        "observed_at": _utc_now(),
        "provenance_ref": f"native://{capability_id}/{operation_id}",
        "policy_digest": _NATIVE_EXECUTION_SCHEMA,
        "decision_factors": {
            "native_receipt_schema": _NATIVE_EXECUTION_SCHEMA,
            "native_receipt_digest": receipt_digest,
            "invocation_id": invocation_id,
            "capability_kind": capability_kind,
            "operation_id": operation_id,
            "admission_id": _required_text(admission.get("admission_id"), "material.admission.admission_id"),
            "step_run_id": bound_step,
        },
        "output_digest": result_digest[7:],
    }


def record_native_execution_receipt(
    omo_dir: Path | str,
    receipt: Mapping[str, Any] | Any,
    *,
    workflow_run_id: str,
    step_run_id: str | None = None,
    producer: str = "native-execution-consumer",
) -> dict[str, Any]:
    """Consume one confirmed native execution receipt through the existing broker.

    The adapter only projects digest-only execution metadata.  It never copies
    material, provider output, or credentials into Workflow Mesh and delegates
    durable evidence/idempotency semantics to ``record_external_receipt``.
    """
    normalized = _normalise_native_execution_receipt(
        receipt,
        workflow_run_id=_required_text(workflow_run_id, "workflow_run_id"),
        step_run_id=step_run_id,
    )
    evidence_id = f"external:{normalized['resource_id']}:{normalized['receipt_id']}"
    existing = WorkflowMeshStore(omo_dir).evidence_snapshot(workflow_run_id, evidence_id)
    if isinstance(existing, Mapping) and existing.get("observed_at"):
        normalized["observed_at"] = existing["observed_at"]
    return record_external_receipt(
        omo_dir,
        normalized,
        workflow_run_id=workflow_run_id,
        step_run_id=step_run_id or normalized["decision_factors"]["step_run_id"],
        producer=producer,
    )


__all__ = [
    "EVIDENCE_KIND",
    "RECEIPT_SCHEMA",
    "ExternalReceiptError",
    "record_external_receipt",
    "record_native_execution_receipt",
]
