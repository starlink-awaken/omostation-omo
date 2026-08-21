"""Governed engineering-delivery metadata ingestion and human review.

Machine ingestion records supply-side metadata only.  A qualified decision
outcome is created exclusively by the human-review broker after it verifies an
existing WorkflowRun, receipt, and submitted feedback record.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .omo_belief import MOSBeliefManager
from .omo_external_receipt import RECEIPT_SCHEMA, record_external_receipt
from .omo_io import AppendOnlyLog, fcntl_lock
from .omo_shared import load_yaml_value
from .outcome_feedback import read_outcome_feedback, record_outcome_feedback
from .workflow_mesh import WorkflowMeshStore

CONSUMPTION_SCHEMA = "engineering-delivery-consumption/v1"
REVIEW_SCHEMA = "engineering-delivery-review/v1"
REVIEW_QUEUE_SCHEMA = "engineering-delivery-review-queue/v1"
QUALIFIED_DECISION_OUTCOME_SCHEMA = "qualified-decision-outcome/v1"
SHADOW_OBSERVER_SCHEMA = "engineering-delivery-shadow-observer/v1"
QUALIFIED_DECISION_OUTCOME_LOG = Path("_knowledge/workflow-mesh/engineering-delivery-decision-outcomes.jsonl")
MOS_PROJECTION_RECEIPT_LOG = Path("_knowledge/workflow-mesh/engineering-delivery-mos-projections.jsonl")

SCENE_BINDING = {
    "scene_id": "engineering-delivery",
    "journey_id": "intent-to-evidence",
    "outcome_metric": "verified_delivery_lead_time",
}
SCENE_POLICY = {
    "scene_id": "engineering-delivery",
    "tier": "shadow",
    "value_indicator_policy": False,
}
CONTROLS = {
    "proposal_only": True,
    "activation": "forbidden",
    "workflow_run_creation": False,
    "provider_invocation": False,
    "automatic_promotion": False,
    "personal_value_attribution": False,
}

_DELIVERY_FIELDS = frozenset(
    {
        "delivery_id",
        "repository_ref",
        "pr_url",
        "merge_sha",
        "requested_at",
        "merged_at",
        "evidence_refs",
    }
)
_REVIEW_FIELDS = frozenset({"delivery_id", "decision", "evidence_refs"})
_DECISIONS = frozenset({"reviewed", "adopted", "rejected"})
_FORBIDDEN_KEY_PARTS = frozenset(
    {
        "content",
        "credential",
        "document",
        "input",
        "output",
        "password",
        "path",
        "raw",
        "reviewdecision",
        "secret",
        "token",
        "verdict",
    }
)
_OPAQUE_REF_SCHEMES = frozenset({"evidence", "github", "ci", "workflow", "receipt"})
_HUMAN_ACTOR_SCHEMES = frozenset({"human", "operator", "principal"})
_PRINCIPAL_ASSERTION_SCHEMA = "cockpit-human-principal-assertion/v2"
_ENGINEERING_REVIEW_SIGNING_KEY_ENV = "COCKPIT_ENGINEERING_REVIEW_SIGNING_KEY"
_PRINCIPAL_ASSERTION_MAX_AGE = timedelta(minutes=5)
_NON_HUMAN_EVIDENCE = re.compile(
    r"(?:^|[/_.:-])(test|synthetic|user[-_]?provided)(?:$|[/_.:-])",
    re.IGNORECASE,
)


class EngineeringDeliveryConsumerError(ValueError):
    """The delivery or review envelope is unsafe or inconsistent."""


class EngineeringDeliveryProjectionError(OSError):
    """The primary human outcome is durable but its MOS projection degraded."""


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _required_text(value: Any, field: str, *, max_length: int = 500) -> str:
    text = str(value or "").strip()
    if not text:
        raise EngineeringDeliveryConsumerError(f"missing required field: {field}")
    if len(text) > max_length:
        raise EngineeringDeliveryConsumerError(f"field is too long: {field}")
    return text


def _timestamp(value: Any, field: str) -> str:
    text = _required_text(value, field, max_length=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EngineeringDeliveryConsumerError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise EngineeringDeliveryConsumerError(f"{field} must include a timezone")
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _reject_forbidden_keys(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = re.sub(r"[^a-z]", "", str(key).lower())
            if any(part in normalized for part in _FORBIDDEN_KEY_PARTS):
                raise EngineeringDeliveryConsumerError(f"forbidden raw, secret, or verdict field: {path}.{key}")
            _reject_forbidden_keys(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_forbidden_keys(nested, f"{path}[{index}]")


def _strict_envelope(payload: Mapping[str, Any], allowed: frozenset[str], name: str) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise EngineeringDeliveryConsumerError(f"{name} envelope must be an object")
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise EngineeringDeliveryConsumerError(f"unsupported {name} fields: {unknown}")
    _reject_forbidden_keys(payload)
    return dict(payload)


def _uri(value: Any, field: str, *, schemes: frozenset[str] | None = None) -> str:
    text = _required_text(value, field)
    parsed = urlparse(text)
    if not parsed.scheme or (schemes is not None and parsed.scheme.lower() not in schemes):
        raise EngineeringDeliveryConsumerError(f"{field} must be an opaque URI reference")
    if parsed.scheme.lower() == "file" or text.startswith(("/", "~")):
        raise EngineeringDeliveryConsumerError(f"{field} must not expose a filesystem path")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise EngineeringDeliveryConsumerError(f"{field} must be an opaque URI without credentials or query data")
    return text


def _opaque_id(value: Any, field: str, *, max_length: int = 160) -> str:
    text = _required_text(value, field, max_length=max_length)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", text):
        raise EngineeringDeliveryConsumerError(f"{field} must be an opaque identifier")
    return text


def _evidence_refs(value: Any, *, required: bool, human_review: bool = False) -> list[str]:
    if not isinstance(value, list):
        raise EngineeringDeliveryConsumerError("evidence_refs must be a list")
    if required and not value:
        raise EngineeringDeliveryConsumerError("evidence_refs must contain at least one reference")
    if len(value) > 20:
        raise EngineeringDeliveryConsumerError("evidence_refs must contain at most 20 references")
    refs = [_uri(item, "evidence_refs.item", schemes=_OPAQUE_REF_SCHEMES) for item in value]
    if human_review and any(_NON_HUMAN_EVIDENCE.search(ref) for ref in refs):
        raise EngineeringDeliveryConsumerError("human review evidence must not be test, synthetic, or user_provided")
    if len(set(refs)) != len(refs):
        raise EngineeringDeliveryConsumerError("evidence_refs must be unique")
    return refs


def _validate_delivery(payload: Mapping[str, Any]) -> dict[str, Any]:
    value = _strict_envelope(payload, _DELIVERY_FIELDS, "delivery")
    requested_at = _timestamp(value.get("requested_at"), "requested_at")
    merged_at = _timestamp(value.get("merged_at"), "merged_at")
    if merged_at < requested_at:
        raise EngineeringDeliveryConsumerError("merged_at must not precede requested_at")
    merge_sha = _required_text(value.get("merge_sha"), "merge_sha", max_length=64).lower()
    if not re.fullmatch(r"[0-9a-f]{40,64}", merge_sha):
        raise EngineeringDeliveryConsumerError("merge_sha must be a 40-64 character hexadecimal digest")
    repository_ref = _uri(value.get("repository_ref"), "repository_ref", schemes=frozenset({"github"}))
    repository_uri = urlparse(repository_ref)
    if not repository_uri.netloc or not re.fullmatch(r"/[^/]+", repository_uri.path):
        raise EngineeringDeliveryConsumerError("repository_ref must identify one GitHub owner/repository")
    pr_url = _uri(value.get("pr_url"), "pr_url", schemes=frozenset({"https"}))
    if urlparse(pr_url).hostname != "github.com" or not re.fullmatch(
        r"/[^/]+/[^/]+/pull/[1-9][0-9]*", urlparse(pr_url).path
    ):
        raise EngineeringDeliveryConsumerError("pr_url must identify a GitHub pull request")
    return {
        "delivery_id": _opaque_id(value.get("delivery_id"), "delivery_id"),
        "repository_ref": repository_ref,
        "pr_url": pr_url,
        "merge_sha": merge_sha,
        "requested_at": requested_at,
        "merged_at": merged_at,
        "evidence_refs": _evidence_refs(value.get("evidence_refs"), required=True),
    }


def normalize_engineering_delivery_review(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical review payload used for assertion binding."""
    value = _strict_envelope(payload, _REVIEW_FIELDS, "review")
    decision = _required_text(value.get("decision"), "decision", max_length=32).lower()
    if decision not in _DECISIONS:
        raise EngineeringDeliveryConsumerError(f"unsupported human decision: {decision}")
    return {
        "delivery_id": _opaque_id(value.get("delivery_id"), "delivery_id"),
        "decision": decision,
        "evidence_refs": _evidence_refs(value.get("evidence_refs"), required=True, human_review=True),
    }


def _human_actor(value: Any) -> str:
    try:
        actor = _uri(value, "actor", schemes=_HUMAN_ACTOR_SCHEMES)
    except EngineeringDeliveryConsumerError as exc:
        raise EngineeringDeliveryConsumerError(
            "human actor must use a human://, operator://, or principal:// reference"
        ) from exc
    lowered = actor.lower()
    if any(token in lowered for token in ("agent", "automation", "bot", "system")):
        raise EngineeringDeliveryConsumerError("human actor must not identify an agent or system")
    return actor


def _verify_principal_assertion(
    value: Any,
    binding: Mapping[str, Any],
    *,
    enforce_freshness: bool,
) -> tuple[str, str]:
    required = {
        "schema",
        "principal_ref",
        "source_class",
        "issued_at",
        "binding_digest",
        "signature",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise EngineeringDeliveryConsumerError("invalid human principal assertion shape")
    assertion = dict(value)
    if assertion.get("schema") != _PRINCIPAL_ASSERTION_SCHEMA:
        raise EngineeringDeliveryConsumerError("invalid human principal assertion schema")
    if assertion.get("source_class") != "real_human":
        raise EngineeringDeliveryConsumerError("human principal assertion source must be real_human")
    actor = _human_actor(assertion.get("principal_ref"))
    issued_at_text = _timestamp(assertion.get("issued_at"), "principal_assertion.issued_at")
    expected_binding_digest = hashlib.sha256(_canonical(binding).encode("utf-8")).hexdigest()
    if not hmac.compare_digest(str(assertion.get("binding_digest") or ""), expected_binding_digest):
        raise EngineeringDeliveryConsumerError("human principal assertion delivery binding mismatch")
    signing_key = os.environ.get(_ENGINEERING_REVIEW_SIGNING_KEY_ENV, "")
    if len(signing_key) < 32:
        raise EngineeringDeliveryConsumerError("human principal assertion verifier is unavailable")
    signed_body = {key: assertion[key] for key in required if key != "signature"}
    expected_signature = hmac.new(
        signing_key.encode("utf-8"),
        _canonical(signed_body).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    signature = str(assertion.get("signature") or "")
    if not hmac.compare_digest(signature, expected_signature):
        raise EngineeringDeliveryConsumerError("invalid human principal assertion signature")
    if enforce_freshness:
        issued_at = datetime.fromisoformat(issued_at_text.replace("Z", "+00:00"))
        now = datetime.fromisoformat(_utc_now().replace("Z", "+00:00"))
        if issued_at > now + timedelta(seconds=30) or now - issued_at > _PRINCIPAL_ASSERTION_MAX_AGE:
            raise EngineeringDeliveryConsumerError("human principal assertion is expired or from the future")
    receipt_id = f"human-adjudication:{_sha256(signed_body)}"
    return actor, receipt_id


def _workspace_root(omo_dir: Path) -> Path:
    return omo_dir.parent if omo_dir.name == ".omo" else omo_dir


def _qualified_log(omo_dir: Path) -> AppendOnlyLog:
    path = omo_dir / QUALIFIED_DECISION_OUTCOME_LOG
    return AppendOnlyLog(path, lock=fcntl_lock(path.with_suffix(path.suffix + ".lock")))


def _qualified_records(omo_dir: Path) -> list[dict[str, Any]]:
    records = _qualified_log(omo_dir).read_all()
    return _validate_primary_records(records)


def _validate_primary_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    validated = [_validate_qualified_record(record) for record in records]
    ids = [record["decision_outcome_id"] for record in validated]
    if len(set(ids)) != len(ids):
        raise EngineeringDeliveryConsumerError("duplicate qualified decision-outcome identity")
    return validated


def _validate_projection_receipt(record: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema",
        "projection_receipt_id",
        "decision_outcome_id",
        "status",
        "mos_decision_id",
        "error_code",
        "recorded_at",
    }
    if not isinstance(record, Mapping) or set(record) != required:
        raise EngineeringDeliveryConsumerError("invalid MOS projection receipt shape")
    if record.get("schema") != "engineering-delivery-mos-projection/v1":
        raise EngineeringDeliveryConsumerError("invalid MOS projection receipt schema")
    if record.get("status") not in {"pending", "projected", "degraded"}:
        raise EngineeringDeliveryConsumerError("invalid MOS projection receipt status")
    if record.get("status") == "projected" and not str(record.get("mos_decision_id") or "").strip():
        raise EngineeringDeliveryConsumerError("projected MOS receipt requires mos_decision_id")
    if record.get("status") == "degraded" and record.get("error_code") != "mos_projection_unavailable":
        raise EngineeringDeliveryConsumerError("degraded MOS receipt requires stable error_code")
    _required_text(record.get("projection_receipt_id"), "projection_receipt_id")
    _required_text(record.get("decision_outcome_id"), "decision_outcome_id")
    _timestamp(record.get("recorded_at"), "recorded_at")
    return dict(record)


def _projection_records(omo_dir: Path) -> list[dict[str, Any]]:
    records = AppendOnlyLog(omo_dir / MOS_PROJECTION_RECEIPT_LOG).read_all()
    return [_validate_projection_receipt(record) for record in records]


def _append_projection_status(
    log: AppendOnlyLog,
    existing: list[dict[str, Any]],
    *,
    decision_outcome_id: str,
    status: str,
    mos_decision_id: str | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    related = [item for item in existing if item.get("decision_outcome_id") == decision_outcome_id]
    if related and related[-1].get("status") == status:
        return related[-1]
    recorded_at = _utc_now()
    receipt = {
        "schema": "engineering-delivery-mos-projection/v1",
        "projection_receipt_id": (
            f"engineering-delivery-mos-projection:{_sha256({'decision_outcome_id': decision_outcome_id, 'status': status, 'recorded_at': recorded_at})}"
        ),
        "decision_outcome_id": decision_outcome_id,
        "status": status,
        "mos_decision_id": mos_decision_id,
        "error_code": error_code,
        "recorded_at": recorded_at,
    }
    _validate_projection_receipt(receipt)
    log.append(receipt, sort_keys=True)
    existing.append(receipt)
    return receipt


def _validate_qualified_record(record: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema",
        "decision_outcome_id",
        "workflow_run_id",
        "delivery_id",
        "scene_id",
        "tier",
        "value_indicator_policy",
        "source_class",
        "adjudication_receipt_id",
        "adjudication_assertion",
        "human_verdict",
        "human_actor_ref",
        "reviewed_at",
        "evidence_refs",
        "source_feedback_id",
        "source_receipt_id",
        "recorded_at",
    }
    if not isinstance(record, Mapping) or set(record) != required:
        raise EngineeringDeliveryConsumerError("invalid qualified decision-outcome record shape")
    if record.get("schema") != QUALIFIED_DECISION_OUTCOME_SCHEMA:
        raise EngineeringDeliveryConsumerError("invalid qualified decision-outcome schema")
    if record.get("scene_id") != "engineering-delivery" or record.get("tier") != "shadow":
        raise EngineeringDeliveryConsumerError("qualified decision-outcome scene policy mismatch")
    if record.get("value_indicator_policy") is not False:
        raise EngineeringDeliveryConsumerError("qualified decision-outcome must not count as personal value")
    if record.get("source_class") != "real_human":
        raise EngineeringDeliveryConsumerError("qualified decision-outcome source must be real_human")
    if record.get("human_verdict") not in _DECISIONS:
        raise EngineeringDeliveryConsumerError("invalid qualified decision-outcome verdict")
    _human_actor(record.get("human_actor_ref"))
    _timestamp(record.get("reviewed_at"), "reviewed_at")
    _timestamp(record.get("recorded_at"), "recorded_at")
    _evidence_refs(record.get("evidence_refs"), required=True, human_review=True)
    review = {
        "delivery_id": record.get("delivery_id"),
        "decision": record.get("human_verdict"),
        "evidence_refs": record.get("evidence_refs"),
    }
    asserted_actor, adjudication_receipt_id = _verify_principal_assertion(
        record.get("adjudication_assertion"),
        {
            "workflow_run_id": record.get("workflow_run_id"),
            "candidate_receipt_id": record.get("source_receipt_id"),
            "review": review,
        },
        enforce_freshness=False,
    )
    if asserted_actor != record.get("human_actor_ref"):
        raise EngineeringDeliveryConsumerError("qualified decision-outcome actor assertion mismatch")
    if adjudication_receipt_id != record.get("adjudication_receipt_id"):
        raise EngineeringDeliveryConsumerError("qualified decision-outcome adjudication receipt mismatch")
    for field in (
        "decision_outcome_id",
        "workflow_run_id",
        "delivery_id",
        "source_feedback_id",
        "source_receipt_id",
        "adjudication_receipt_id",
    ):
        _required_text(record.get(field), field)
    return dict(record)


def consume_engineering_delivery(
    omo_dir: Path | str,
    payload: Mapping[str, Any],
    *,
    workflow_run_id: str,
) -> dict[str, Any]:
    """Record a merged-delivery summary as receipt + submitted feedback only."""
    root = Path(omo_dir)
    run_id = _required_text(workflow_run_id, "workflow_run_id")
    delivery = _validate_delivery(payload)
    snapshot = WorkflowMeshStore(root).snapshot(run_id)
    if snapshot.get("state") == "unknown":
        raise EngineeringDeliveryConsumerError("workflow run does not exist")
    if snapshot.get("scene_binding") != SCENE_BINDING:
        raise EngineeringDeliveryConsumerError("workflow run scene binding is not engineering-delivery")
    if snapshot.get("state") not in {"succeeded", "verified", "merged", "closed"}:
        raise EngineeringDeliveryConsumerError("workflow run has no eligible merged delivery outcome")

    receipt_id = delivery["delivery_id"]
    evidence_id = f"external:engineering-delivery:{receipt_id}"
    existing = snapshot.get("evidence", {}).get(evidence_id)
    receipt = {
        "receipt_id": receipt_id,
        "trace_id": run_id,
        "resource_id": "engineering-delivery",
        "operation": "consume-merged-metadata",
        "result_state": "succeeded",
        "observed_at": delivery["merged_at"],
        "provenance_ref": delivery["pr_url"],
        "policy_digest": "engineering-delivery-shadow/v1",
        "decision_factors": {
            "repository_ref": delivery["repository_ref"],
            "merge_sha": delivery["merge_sha"],
            "requested_at": delivery["requested_at"],
            "merged_at": delivery["merged_at"],
        },
        "output_digest": _sha256(delivery),
    }
    if existing is None:
        record_external_receipt(root, receipt, workflow_run_id=run_id, producer="engineering-delivery-consumer")
    else:
        expected = {
            "receipt_id": receipt_id,
            "sha256": receipt["output_digest"],
            "evidence_schema": RECEIPT_SCHEMA,
            "resource_id": "engineering-delivery",
            "trace_id": run_id,
            "result_state": "succeeded",
            "observed_at": delivery["merged_at"],
            "provenance_ref": delivery["pr_url"],
            "policy_digest": "engineering-delivery-shadow/v1",
            "decision_factors": receipt["decision_factors"],
        }
        if any(existing.get(key) != value for key, value in expected.items()):
            raise EngineeringDeliveryConsumerError("conflicting delivery receipt replay")

    outcome_id = f"outcome:engineering-delivery:{delivery['delivery_id']}"
    feedback_result = record_outcome_feedback(
        root,
        {
            "workflow_run_id": run_id,
            "outcome_id": outcome_id,
            "scene_binding": SCENE_BINDING,
            "consumption_state": "submitted",
            "consumer_ref": "system://engineering-delivery-consumer",
            "result_ref": f"receipt://engineering-delivery/{delivery['delivery_id']}",
            "evidence_refs": delivery["evidence_refs"],
            "value": {},
            "observed_at": delivery["merged_at"],
        },
        actor="system://engineering-delivery-consumer",
    )
    return {
        "schema": CONSUMPTION_SCHEMA,
        "status": feedback_result["status"],
        "workflow_run_id": run_id,
        "delivery_id": delivery["delivery_id"],
        "outcome_id": outcome_id,
        "receipt_id": receipt_id,
        "feedback_id": feedback_result["feedback"]["feedback_id"],
        "scene": dict(SCENE_POLICY),
        "controls": dict(CONTROLS),
    }


def _project_to_mos(omo_dir: Path, record: Mapping[str, Any]) -> dict[str, str]:
    manager = MOSBeliefManager(root=_workspace_root(omo_dir))
    existing = next(
        (
            item
            for item in manager._load_state().get("decision_outcomes", [])
            if item.get("source_run_id") == record["decision_outcome_id"]
        ),
        None,
    )
    if existing is not None:
        return {"status": "deduplicated", "decision_id": str(existing["id"])}
    decision_id = manager.record_decision_outcome(
        decision_type="engineering-delivery:human-review",
        input_summary=f"delivery={record['delivery_id']} scene=engineering-delivery tier=shadow",
        expected_outcome="explicit human review of a submitted engineering delivery",
        actual_outcome=str(record["human_verdict"]),
        delta="value_indicator_policy=false",
        source_run_id=str(record["decision_outcome_id"]),
        metadata={
            "scene_id": "engineering-delivery",
            "tier": "shadow",
            "source_class": "real_human",
            "value_indicator_policy": False,
            "personal_value_attribution": False,
        },
    )
    return {"status": "projected", "decision_id": decision_id}


def record_engineering_delivery_review(
    omo_dir: Path | str,
    payload: Mapping[str, Any],
    *,
    workflow_run_id: str,
    principal_assertion: Mapping[str, Any],
) -> dict[str, Any]:
    """Record an explicit human verdict and project it to MOS fail-closed."""
    root = Path(omo_dir)
    run_id = _required_text(workflow_run_id, "workflow_run_id")
    review = normalize_engineering_delivery_review(payload)
    snapshot = WorkflowMeshStore(root).snapshot(run_id)
    if snapshot.get("state") == "unknown" or snapshot.get("scene_binding") != SCENE_BINDING:
        raise EngineeringDeliveryConsumerError("review requires an existing engineering-delivery workflow run")
    receipt_id = review["delivery_id"]
    evidence = snapshot.get("evidence", {}).get(f"external:engineering-delivery:{receipt_id}")
    if not isinstance(evidence, Mapping) or evidence.get("evidence_schema") != RECEIPT_SCHEMA:
        raise EngineeringDeliveryConsumerError("review requires an existing engineering-delivery receipt")
    human_actor, adjudication_receipt_id = _verify_principal_assertion(
        principal_assertion,
        {
            "workflow_run_id": run_id,
            "candidate_receipt_id": str(evidence["receipt_id"]),
            "review": review,
        },
        enforce_freshness=True,
    )

    outcome_id = f"outcome:engineering-delivery:{review['delivery_id']}"
    log_path = root / QUALIFIED_DECISION_OUTCOME_LOG
    lock = fcntl_lock(log_path.with_suffix(log_path.suffix + ".lock"))
    # The domain fcntl lock encloses feedback, primary outcome, outbox receipt,
    # and MOS projection.  The logs use their own in-process lock so append()
    # does not reacquire the same fcntl lock.
    log = AppendOnlyLog(log_path)
    projection_log = AppendOnlyLog(root / MOS_PROJECTION_RECEIPT_LOG)
    with lock:
        # Submitted machine feedback proves that the candidate exists. Human
        # review authority comes only from the qualified primary log; generic
        # outcome-feedback records can never promote or block this scene.
        all_feedback = read_outcome_feedback(root)
        submitted = [
            item
            for item in all_feedback
            if item.get("workflow_run_id") == run_id
            and item.get("outcome_id") == outcome_id
            and item.get("consumption_state") == "submitted"
        ]
        if not submitted:
            raise EngineeringDeliveryConsumerError("review requires existing submitted feedback")
        existing_records = _validate_primary_records(log.read_all())
        decision_outcome_id = f"engineering-delivery-outcome:{_sha256({'run': run_id, 'delivery': receipt_id})}"
        existing_record = next(
            (item for item in existing_records if item.get("decision_outcome_id") == decision_outcome_id),
            None,
        )
        if existing_record is not None:
            expected = {
                "human_verdict": review["decision"],
                "human_actor_ref": human_actor,
                "evidence_refs": review["evidence_refs"],
            }
            if any(existing_record.get(key) != value for key, value in expected.items()):
                raise EngineeringDeliveryConsumerError("conflicting qualified decision-outcome replay")
            qualified = existing_record
            feedback = next(
                (item for item in all_feedback if item.get("feedback_id") == existing_record["source_feedback_id"]),
                None,
            )
            if feedback is None:
                raise EngineeringDeliveryConsumerError("qualified review feedback is unavailable")
            feedback_result = {"status": "deduplicated", "feedback": feedback}
        else:
            reviewed_at = _utc_now()
            feedback_result = record_outcome_feedback(
                root,
                {
                    "workflow_run_id": run_id,
                    "outcome_id": outcome_id,
                    "scene_binding": SCENE_BINDING,
                    "consumption_state": review["decision"],
                    "consumer_ref": human_actor,
                    "result_ref": f"receipt://engineering-delivery/{receipt_id}",
                    "evidence_refs": review["evidence_refs"],
                    "value": {},
                    "observed_at": reviewed_at,
                },
                actor=human_actor,
            )
            qualified = {
                "schema": QUALIFIED_DECISION_OUTCOME_SCHEMA,
                "decision_outcome_id": decision_outcome_id,
                "workflow_run_id": run_id,
                "delivery_id": receipt_id,
                "scene_id": "engineering-delivery",
                "tier": "shadow",
                "value_indicator_policy": False,
                "source_class": "real_human",
                "adjudication_receipt_id": adjudication_receipt_id,
                "adjudication_assertion": dict(principal_assertion),
                "human_verdict": review["decision"],
                "human_actor_ref": human_actor,
                "reviewed_at": reviewed_at,
                "evidence_refs": review["evidence_refs"],
                "source_feedback_id": feedback_result["feedback"]["feedback_id"],
                "source_receipt_id": str(evidence["receipt_id"]),
                "recorded_at": reviewed_at,
            }
            _validate_qualified_record(qualified)
            log.append(qualified, sort_keys=True)

        projection_records = _projection_records(root)
        related_projections = [
            item for item in projection_records if item.get("decision_outcome_id") == decision_outcome_id
        ]
        latest_projection = related_projections[-1] if related_projections else None
        if latest_projection and latest_projection["status"] == "projected":
            mos_projection = {
                "status": "deduplicated",
                "decision_id": str(latest_projection["mos_decision_id"]),
            }
        else:
            _append_projection_status(
                projection_log,
                projection_records,
                decision_outcome_id=decision_outcome_id,
                status="pending",
            )
            try:
                mos_projection = _project_to_mos(root, qualified)
            except Exception as exc:
                _append_projection_status(
                    projection_log,
                    projection_records,
                    decision_outcome_id=decision_outcome_id,
                    status="degraded",
                    error_code="mos_projection_unavailable",
                )
                raise EngineeringDeliveryProjectionError(
                    f"MOS projection degraded for {decision_outcome_id}; retry is required"
                ) from exc
            _append_projection_status(
                projection_log,
                projection_records,
                decision_outcome_id=decision_outcome_id,
                status="projected",
                mos_decision_id=mos_projection["decision_id"],
            )

    return {
        "schema": REVIEW_SCHEMA,
        "status": "deduplicated" if existing_record is not None else "recorded",
        "workflow_run_id": run_id,
        "delivery_id": receipt_id,
        "decision": review["decision"],
        "reviewed_at": qualified["reviewed_at"],
        "outcome_id": outcome_id,
        "decision_outcome_id": decision_outcome_id,
        "value_indicator_policy": False,
        "feedback_id": feedback_result["feedback"]["feedback_id"],
        "qualified_decision_outcome": qualified,
        "mos_projection": mos_projection,
        "scene": dict(SCENE_POLICY),
        "controls": dict(CONTROLS),
    }


def build_engineering_delivery_review_queue(
    omo_dir: Path | str,
    *,
    workflow_run_id: str | None = None,
) -> dict[str, Any]:
    """Build the read-only Cockpit queue from receipts and feedback."""
    root = Path(omo_dir)
    feedback = read_outcome_feedback(root)
    qualified_records = _qualified_records(root)
    rows: list[dict[str, Any]] = []
    for snapshot in WorkflowMeshStore(root).snapshots():
        run_id = str(snapshot.get("workflow_run_id") or "")
        if workflow_run_id and run_id != workflow_run_id:
            continue
        if snapshot.get("scene_binding") != SCENE_BINDING:
            continue
        for evidence in snapshot.get("evidence", {}).values():
            if (
                evidence.get("resource_id") != "engineering-delivery"
                or evidence.get("evidence_schema") != RECEIPT_SCHEMA
            ):
                continue
            delivery_id = str(evidence.get("receipt_id") or "")
            outcome_id = f"outcome:engineering-delivery:{delivery_id}"
            related = [
                item
                for item in feedback
                if item.get("workflow_run_id") == run_id and item.get("outcome_id") == outcome_id
            ]
            human = [
                item
                for item in qualified_records
                if item.get("workflow_run_id") == run_id and item.get("delivery_id") == delivery_id
            ]
            factors = evidence.get("decision_factors") or {}
            duration_seconds: int | None = None
            try:
                requested = datetime.fromisoformat(str(factors["requested_at"]).replace("Z", "+00:00"))
                merged = datetime.fromisoformat(str(factors["merged_at"]).replace("Z", "+00:00"))
                duration_seconds = int((merged - requested).total_seconds())
            except (KeyError, TypeError, ValueError):
                pass
            rows.append(
                {
                    "delivery_id": delivery_id,
                    "workflow_run_id": run_id,
                    "workflow_state": snapshot.get("state"),
                    "scene_binding": dict(SCENE_BINDING),
                    "receipt_id": evidence.get("receipt_id"),
                    "review_status": "reviewed" if human else "pending",
                    "latest_decision": human[-1]["human_verdict"] if human else None,
                    "delivery_duration_seconds": duration_seconds,
                    "evidence_count": len(related[-1].get("evidence_refs", [])) if related else 0,
                }
            )
    rows.sort(key=lambda item: (item["workflow_run_id"], item["delivery_id"]))
    reviewed = sum(row["review_status"] == "reviewed" for row in rows)
    return {
        "schema": REVIEW_QUEUE_SCHEMA,
        "status": "live",
        "summary": {
            "row_count": len(rows),
            "pending_review_count": len(rows) - reviewed,
            "reviewed_count": reviewed,
        },
        "rows": rows,
        "scene": dict(SCENE_POLICY),
        "controls": {
            "read_only": True,
            "workflow_state_mutation": False,
            "provider_invocation": False,
            "automatic_promotion": False,
            "personal_value_attribution": False,
        },
    }


def build_engineering_delivery_shadow_observer(
    omo_dir: Path | str,
    *,
    as_of: datetime | str | None = None,
) -> dict[str, Any]:
    """Count qualified outcomes in the half-open rolling 7-day window."""
    if as_of is None:
        end = datetime.now(UTC).replace(microsecond=0)
    elif isinstance(as_of, datetime):
        if as_of.tzinfo is None:
            raise EngineeringDeliveryConsumerError("as_of must include a timezone")
        end = as_of.astimezone(UTC).replace(microsecond=0)
    else:
        end = datetime.fromisoformat(_timestamp(as_of, "as_of").replace("Z", "+00:00"))
    start = end - timedelta(days=7)
    result: dict[str, Any] = {
        "schema": SHADOW_OBSERVER_SCHEMA,
        "scene_id": "engineering-delivery",
        "tier": "shadow",
        "value_indicator_policy": False,
        "window": {
            "start": start.isoformat().replace("+00:00", "Z"),
            "end_exclusive": end.isoformat().replace("+00:00", "Z"),
        },
        "threshold": 20,
        "human_gate": "not_ready",
    }
    try:
        root = Path(omo_dir)
        primary_path = root / QUALIFIED_DECISION_OUTCOME_LOG
        lock = fcntl_lock(primary_path.with_suffix(primary_path.suffix + ".lock"))
        with lock:
            records = _validate_primary_records(AppendOnlyLog(primary_path).read_all())
            projection_records = _projection_records(root)
            feedback_records = read_outcome_feedback(root)
        latest_projection = {str(item["decision_outcome_id"]): item for item in projection_records}
        projected_receipts = [item for item in latest_projection.values() if item.get("status") == "projected"]
        if projected_receipts:
            mos_state_path = _workspace_root(root) / ".omo" / "state" / "agent-beliefs" / "index.yaml"
            if not mos_state_path.is_file():
                raise EngineeringDeliveryConsumerError("MOS projection state is unavailable")
            mos_state = load_yaml_value(mos_state_path) or {}
            mos_outcomes = mos_state.get("decision_outcomes")
            if not isinstance(mos_outcomes, list):
                raise EngineeringDeliveryConsumerError("MOS decision outcomes are unreadable")
            mos_by_source = {str(item.get("source_run_id")): item for item in mos_outcomes if isinstance(item, Mapping)}
            for receipt in projected_receipts:
                mos_outcome = mos_by_source.get(str(receipt["decision_outcome_id"]))
                if not mos_outcome or str(mos_outcome.get("id")) != str(receipt["mos_decision_id"]):
                    raise EngineeringDeliveryConsumerError("MOS projection receipt does not match MOS state")
                if mos_outcome.get("metadata") != {
                    "scene_id": "engineering-delivery",
                    "tier": "shadow",
                    "source_class": "real_human",
                    "value_indicator_policy": False,
                    "personal_value_attribution": False,
                }:
                    raise EngineeringDeliveryConsumerError("MOS projection is missing structured value isolation")
        qualifying = []
        for record in records:
            snapshot = WorkflowMeshStore(root).snapshot(str(record["workflow_run_id"]))
            evidence = snapshot.get("evidence", {}).get(f"external:engineering-delivery:{record['delivery_id']}")
            source_feedback = next(
                (item for item in feedback_records if item.get("feedback_id") == record["source_feedback_id"]),
                None,
            )
            if (
                snapshot.get("scene_binding") != SCENE_BINDING
                or not isinstance(evidence, Mapping)
                or evidence.get("receipt_id") != record["source_receipt_id"]
                or source_feedback is None
                or source_feedback.get("workflow_run_id") != record["workflow_run_id"]
                or source_feedback.get("outcome_id") != f"outcome:engineering-delivery:{record['delivery_id']}"
                or source_feedback.get("consumption_state") != record["human_verdict"]
                or source_feedback.get("actor") != record["human_actor_ref"]
                or source_feedback.get("evidence_refs") != record["evidence_refs"]
            ):
                raise EngineeringDeliveryConsumerError("qualified decision-outcome provenance is inconsistent")
            reviewed_at = datetime.fromisoformat(str(record["reviewed_at"]).replace("Z", "+00:00"))
            projection = latest_projection.get(str(record["decision_outcome_id"]))
            if start <= reviewed_at < end and projection and projection.get("status") == "projected":
                qualifying.append(record)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return {
            **result,
            "status": "unprovable",
            "verdict": "UNPROVABLE",
            "qualifying_decision_outcomes": None,
            "error": "qualified_decision_outcomes_unreadable",
        }
    count = len(qualifying)
    if count >= 20:
        status = "ready_for_human_review"
        human_gate = "not_decided"
    else:
        status = "collecting"
        human_gate = "not_ready"
    return {
        **result,
        "status": status,
        "verdict": "PASS" if count >= 20 else "FAIL",
        "qualifying_decision_outcomes": count,
        "human_gate": human_gate,
    }


__all__ = [
    "CONSUMPTION_SCHEMA",
    "EngineeringDeliveryConsumerError",
    "EngineeringDeliveryProjectionError",
    "MOS_PROJECTION_RECEIPT_LOG",
    "QUALIFIED_DECISION_OUTCOME_LOG",
    "REVIEW_QUEUE_SCHEMA",
    "REVIEW_SCHEMA",
    "build_engineering_delivery_review_queue",
    "build_engineering_delivery_shadow_observer",
    "consume_engineering_delivery",
    "normalize_engineering_delivery_review",
    "record_engineering_delivery_review",
]
