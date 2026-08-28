"""Projection and record helpers for engineering delivery consumer."""

from __future__ import annotations

import os
import stat
from datetime import UTC
from pathlib import Path
from typing import Any, Mapping

from .engineering_delivery_consumer_constants import (
    MOS_PROJECTION_RECEIPT_LOG,
    QUALIFIED_DECISION_OUTCOME_LOG,
    QUALIFIED_DECISION_OUTCOME_SCHEMA,
    SCENE_BINDING,
    _DECISIONS,
)
from .engineering_delivery_consumer_validators import (
    EngineeringDeliveryConsumerError,
    _evidence_refs,
    _human_actor,
    _required_text,
    _sha256,
    _timestamp,
)
from .omo_io import AppendOnlyLog, fcntl_lock
from .omo_shared import load_yaml_value_docs


def _utc_now() -> str:
    from datetime import datetime

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _workspace_root(omo_dir: Path) -> Path:
    return omo_dir.parent if omo_dir.name == ".omo" else omo_dir


def _qualified_log(omo_dir: Path) -> AppendOnlyLog:
    return AppendOnlyLog(omo_dir / QUALIFIED_DECISION_OUTCOME_LOG)


def _qualified_records(omo_dir: Path) -> list[dict[str, Any]]:
    return _qualified_log(omo_dir).read_all()


def _validate_primary_records(records: list[dict[str, Any]], root: Path) -> list[dict[str, Any]]:
    validated = [_validate_qualified_record(record, root) for record in records]
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
    log = AppendOnlyLog(omo_dir / MOS_PROJECTION_RECEIPT_LOG)
    return [record for record in log.read_all() if isinstance(record, Mapping)]


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


def _validate_qualified_record(record: Mapping[str, Any], root: Path) -> dict[str, Any]:
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
    return dict(record)


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
        actual_outcome=str(record.get("human_verdict")),
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
