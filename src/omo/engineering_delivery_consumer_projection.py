"""Projection and record helpers for engineering delivery consumer."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any, Mapping

from .engineering_delivery_consumer_constants import (
    MOS_PROJECTION_RECEIPT_LOG,
    QUALIFIED_DECISION_OUTCOME_LOG,
    QUALIFIED_DECISION_OUTCOME_SCHEMA,
    SCENE_BINDING,
)
from .engineering_delivery_consumer_validators import EngineeringDeliveryConsumerError, _utc_now
from .omo_io import AppendOnlyLog, fcntl_lock
from .omo_shared import load_yaml_value_docs


def _workspace_root(omo_dir: Path) -> Path:
    return omo_dir.resolve().parent


def _qualified_log(omo_dir: Path) -> AppendOnlyLog:
    return AppendOnlyLog(omo_dir / QUALIFIED_DECISION_OUTCOME_LOG)


def _qualified_records(omo_dir: Path) -> list[dict[str, Any]]:
    return _qualified_log(omo_dir).read_all()


def _validate_primary_records(records: list[dict[str, Any]], root: Path) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        if record.get("schema") != QUALIFIED_DECISION_OUTCOME_SCHEMA:
            continue
        if record.get("scene_binding") != SCENE_BINDING:
            continue
        valid.append(dict(record))
    return valid


def _validate_projection_receipt(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise EngineeringDeliveryConsumerError("projection receipt must be a mapping")
    if record.get("schema") != MOS_PROJECTION_RECEIPT_LOG.name:
        raise EngineeringDeliveryConsumerError("projection receipt schema mismatch")
    return dict(record)


def _projection_records(omo_dir: Path) -> list[dict[str, Any]]:
    log = AppendOnlyLog(omo_dir / MOS_PROJECTION_RECEIPT_LOG)
    return [record for record in log.read_all() if isinstance(record, Mapping)]


def _append_projection_status(
    omo_dir: Path,
    *,
    status: str,
    decision_outcome_id: str,
    detail: str | None = None,
) -> dict[str, Any]:
    log = AppendOnlyLog(omo_dir / MOS_PROJECTION_RECEIPT_LOG)
    entry: dict[str, Any] = {
        "schema": MOS_PROJECTION_RECEIPT_LOG.name,
        "status": status,
        "decision_outcome_id": decision_outcome_id,
        "detail": detail,
        "recorded_at": _utc_now(),
    }
    log.append(entry)
    return entry


def _validate_qualified_record(record: Mapping[str, Any], root: Path) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise EngineeringDeliveryConsumerError("qualified record must be a mapping")
    if record.get("schema") != QUALIFIED_DECISION_OUTCOME_SCHEMA:
        raise EngineeringDeliveryConsumerError("qualified record schema mismatch")
    if record.get("scene_binding") != SCENE_BINDING:
        raise EngineeringDeliveryConsumerError("qualified record scene binding mismatch")
    return dict(record)


from .omo_belief import MOSBeliefManager


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
        outcome={"decision": record.get("decision"), "delivery_id": record.get("delivery_id")},
        source_run_id=record["decision_outcome_id"],
    )
    return {"status": "projected", "decision_id": decision_id}
