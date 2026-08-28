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
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from .omo_belief import MOSBeliefManager
from .omo_external_receipt import RECEIPT_SCHEMA, record_external_receipt
from .omo_io import AppendOnlyLog, fcntl_lock
from .omo_shared import load_yaml_value_docs
from .outcome_feedback import (
    OUTCOME_FEEDBACK_LOG,
    read_outcome_feedback,
    record_outcome_feedback,
    validate_outcome_feedback,
)
from .workflow_mesh import WORKFLOW_MESH_LOG, WorkflowMeshStore, project_workflow_run

CONSUMPTION_SCHEMA = "engineering-delivery-consumption/v1"
REVIEW_SCHEMA = "engineering-delivery-review/v1"
REVIEW_QUEUE_SCHEMA = "engineering-delivery-review-queue/v1"
QUALIFIED_DECISION_OUTCOME_SCHEMA = "qualified-decision-outcome/v1"
SHADOW_OBSERVER_SCHEMA = "engineering-delivery-shadow-observer/v1"
QUALIFIED_DECISION_OUTCOME_LOG = Path("_knowledge/workflow-mesh/engineering-delivery-decision-outcomes.jsonl")
MOS_PROJECTION_RECEIPT_LOG = Path("_knowledge/workflow-mesh/engineering-delivery-mos-projections.jsonl")
_SHADOW_OBSERVER_INPUT_MAX_BYTES = 64 * 1024 * 1024
_SHADOW_OBSERVER_TOTAL_MAX_BYTES = 128 * 1024 * 1024

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
# Server-owned persistent key file (gitignored); used as a fallback when the
# environment variable is not set so the observer can verify recorded
# assertions in a later process without re-exporting the key.
_ENGINEERING_REVIEW_SIGNING_KEY_FILE = "_knowledge/workflow-mesh/engineering-review-signing.key"
_PRINCIPAL_ASSERTION_MAX_AGE = timedelta(minutes=5)


from .engineering_delivery_consumer_constants import (
    CONSUMPTION_SCHEMA,
    CONTROLS,
    MOS_PROJECTION_RECEIPT_LOG,
    QUALIFIED_DECISION_OUTCOME_LOG,
    QUALIFIED_DECISION_OUTCOME_SCHEMA,
    REVIEW_QUEUE_SCHEMA,
    REVIEW_SCHEMA,
    SCENE_BINDING,
    SCENE_POLICY,
    SHADOW_OBSERVER_SCHEMA,
)
from .engineering_delivery_consumer_projection import (
    _append_projection_status,
    _project_to_mos,
    _projection_records,
    _qualified_log,
    _qualified_records,
    _validate_primary_records,
    _validate_projection_receipt,
    _validate_qualified_record,
    _workspace_root,
)
from .engineering_delivery_consumer_shadow import (
    _close_shadow_observer_inputs,
    _digest_shadow_observer_bytes,
    _open_shadow_observer_leaf,
    _open_shadow_observer_workspace,
    _read_shadow_observer_bytes,
    _read_shadow_observer_input,
    _read_shadow_observer_inputs,
    _read_shadow_observer_jsonl,
    _shadow_observer_directory_flags,
    _shadow_observer_identity,
    _shadow_observer_input_paths,
    _shadow_observer_relative_parts,
    _ShadowObserverInputChangedError,
    _ShadowObserverInputError,
    _ShadowObserverInputSnapshot,
    _verify_shadow_observer_input,
    _verify_shadow_observer_inputs,
)
from .engineering_delivery_consumer_validators import (
    EngineeringDeliveryConsumerError,
    EngineeringDeliveryProjectionError,
    MOSBeliefManager,
    _canonical,
    _evidence_refs,
    _human_actor,
    _opaque_id,
    _reject_forbidden_keys,
    _required_text,
    _sha256,
    _signing_key,
    _strict_envelope,
    _timestamp,
    _uri,
    _validate_delivery,
    normalize_engineering_delivery_review,
)


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _verify_principal_assertion(
    value: Any,
    binding: Mapping[str, Any],
    *,
    enforce_freshness: bool,
    root: Path | None = None,
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
    signing_key = _signing_key(root)
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


def _shadow_observer_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode), info.st_size, info.st_mtime_ns)


def _shadow_observer_relative_parts(path: Path, *, workspace_root: Path) -> tuple[str, ...]:
    try:
        relative = path.absolute().relative_to(workspace_root.absolute())
    except ValueError as exc:
        raise _ShadowObserverInputError("shadow observer input escapes its workspace") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise _ShadowObserverInputError("shadow observer input path is invalid")
    return relative.parts


def _shadow_observer_directory_flags() -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise _ShadowObserverInputError("secure directory traversal is unavailable")
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


def _open_shadow_observer_workspace(workspace_root: Path) -> int | None:
    try:
        return os.open(workspace_root, _shadow_observer_directory_flags())
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _ShadowObserverInputError("shadow observer workspace cannot be opened safely") from exc


def _open_shadow_observer_leaf(workspace_fd: int, parts: tuple[str, ...]) -> int | None:
    parent_fd = os.dup(workspace_fd)
    try:
        for part in parts[:-1]:
            try:
                child_fd = os.open(part, _shadow_observer_directory_flags(), dir_fd=parent_fd)
            except FileNotFoundError:
                return None
            except OSError as exc:
                raise _ShadowObserverInputError("shadow observer input traversal is unsafe") from exc
            os.close(parent_fd)
            parent_fd = child_fd
        flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            return os.open(parts[-1], flags, dir_fd=parent_fd)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise _ShadowObserverInputError("shadow observer input cannot be opened safely") from exc
    finally:
        os.close(parent_fd)


def _read_shadow_observer_bytes(fd: int, *, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = os.read(fd, 65_536)
        if not chunk:
            return b"".join(chunks)
        size += len(chunk)
        if size > max_bytes:
            raise _ShadowObserverInputError("shadow observer input exceeds the byte limit")
        chunks.append(chunk)


def _digest_shadow_observer_bytes(fd: int, *, max_bytes: int) -> str:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = os.read(fd, 65_536)
        if not chunk:
            return digest.hexdigest()
        size += len(chunk)
        if size > max_bytes:
            raise _ShadowObserverInputError("shadow observer input exceeds the byte limit")
        digest.update(chunk)


def _read_shadow_observer_input(
    path: Path,
    *,
    workspace_root: Path,
    workspace_fd: int,
    max_bytes: int,
) -> _ShadowObserverInputSnapshot:
    """Capture one regular file through exactly one read-only descriptor."""
    fd = _open_shadow_observer_leaf(
        workspace_fd,
        _shadow_observer_relative_parts(path, workspace_root=workspace_root),
    )
    if fd is None:
        return _ShadowObserverInputSnapshot(path, None, None, None, None)
    keep_open = False
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise _ShadowObserverInputError("shadow observer input is not a regular file")
        if before.st_size > max_bytes:
            raise _ShadowObserverInputError("shadow observer input exceeds the byte limit")
        payload = _read_shadow_observer_bytes(fd, max_bytes=max_bytes)
        after = os.fstat(fd)
        if _shadow_observer_identity(before) != _shadow_observer_identity(after):
            raise _ShadowObserverInputChangedError("shadow observer input changed during capture")
        snapshot = _ShadowObserverInputSnapshot(
            path,
            fd,
            _shadow_observer_identity(after),
            hashlib.sha256(payload).hexdigest(),
            payload,
        )
        keep_open = True
        return snapshot
    finally:
        if not keep_open:
            os.close(fd)


def _read_shadow_observer_inputs(
    omo_dir: Path,
) -> tuple[dict[Path, _ShadowObserverInputSnapshot], tuple[int, int, int, int, int] | None]:
    snapshots: dict[Path, _ShadowObserverInputSnapshot] = {}
    workspace_root = _workspace_root(omo_dir)
    workspace_fd = _open_shadow_observer_workspace(workspace_root)
    if workspace_fd is None:
        return (
            {
                path: _ShadowObserverInputSnapshot(path, None, None, None, None)
                for path in _shadow_observer_input_paths(omo_dir)
            },
            None,
        )
    workspace_identity = _shadow_observer_identity(os.fstat(workspace_fd))
    total_bytes = 0
    try:
        for path in _shadow_observer_input_paths(omo_dir):
            remaining_bytes = _SHADOW_OBSERVER_TOTAL_MAX_BYTES - total_bytes
            if remaining_bytes <= 0:
                raise _ShadowObserverInputError("shadow observer inputs exceed the total byte limit")
            snapshots[path] = _read_shadow_observer_input(
                path,
                workspace_root=workspace_root,
                workspace_fd=workspace_fd,
                max_bytes=min(_SHADOW_OBSERVER_INPUT_MAX_BYTES, remaining_bytes),
            )
            total_bytes += len(snapshots[path].payload or b"")
    except Exception:
        _close_shadow_observer_inputs(snapshots)
        raise
    finally:
        os.close(workspace_fd)
    return snapshots, workspace_identity


def _read_shadow_observer_jsonl(snapshot: _ShadowObserverInputSnapshot) -> list[dict[str, Any]]:
    if snapshot.payload is None:
        return []
    records: list[dict[str, Any]] = []
    for line in snapshot.payload.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            records.append({"raw": line[:200]})
    return records


def _verify_shadow_observer_input(
    snapshot: _ShadowObserverInputSnapshot,
    *,
    workspace_root: Path,
    workspace_fd: int,
) -> None:
    """Fail closed if a captured input changed, appeared, or was replaced."""
    current_fd = _open_shadow_observer_leaf(
        workspace_fd,
        _shadow_observer_relative_parts(snapshot.path, workspace_root=workspace_root),
    )
    if snapshot.fd is None:
        if current_fd is None:
            return
        os.close(current_fd)
        raise _ShadowObserverInputChangedError("shadow observer input appeared after capture")
    if current_fd is None:
        raise _ShadowObserverInputChangedError("shadow observer input disappeared after capture")
    try:
        current = os.fstat(current_fd)
    finally:
        os.close(current_fd)
    os.lseek(snapshot.fd, 0, os.SEEK_SET)
    captured_again_digest = _digest_shadow_observer_bytes(
        snapshot.fd,
        max_bytes=_SHADOW_OBSERVER_INPUT_MAX_BYTES,
    )
    descriptor = os.fstat(snapshot.fd)
    if (
        not stat.S_ISREG(current.st_mode)
        or snapshot.identity != _shadow_observer_identity(current)
        or snapshot.identity != _shadow_observer_identity(descriptor)
        or snapshot.digest != captured_again_digest
    ):
        raise _ShadowObserverInputChangedError("shadow observer input changed after capture")


def _verify_shadow_observer_inputs(
    snapshots: Mapping[Path, _ShadowObserverInputSnapshot],
    *,
    workspace_root: Path,
    workspace_identity: tuple[int, int, int, int, int] | None,
) -> None:
    workspace_fd = _open_shadow_observer_workspace(workspace_root)
    if workspace_fd is None:
        if workspace_identity is None:
            return
        raise _ShadowObserverInputChangedError("shadow observer workspace disappeared after capture")
    try:
        if workspace_identity is None or _shadow_observer_identity(os.fstat(workspace_fd)) != workspace_identity:
            raise _ShadowObserverInputChangedError("shadow observer workspace changed after capture")
        for snapshot in snapshots.values():
            _verify_shadow_observer_input(
                snapshot,
                workspace_root=workspace_root,
                workspace_fd=workspace_fd,
            )
    finally:
        os.close(workspace_fd)


def _close_shadow_observer_inputs(snapshots: Mapping[Path, _ShadowObserverInputSnapshot]) -> None:
    for snapshot in snapshots.values():
        if snapshot.fd is not None:
            os.close(snapshot.fd)


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
        root=root,
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


def build_principal_assertion(
    *,
    principal_ref: str,
    workflow_run_id: str,
    candidate_receipt_id: str,
    review: Mapping[str, Any],
    issued_at: str | None = None,
) -> dict[str, str]:
    """Build a server-signed human principal assertion for a delivery review.

    The caller (a human or a human-driven tool) supplies the principal and the
    binding fields; this function HMAC-signs the canonical assertion with the
    server-owned key (COCKPIT_ENGINEERING_REVIEW_SIGNING_KEY).  A client can
    never forge the signature because it does not hold the key — the same
    property enforced by ``_verify_principal_assertion`` on read.
    """
    signing_key = _signing_key()
    if len(signing_key) < 32:
        raise EngineeringDeliveryConsumerError(
            f"human principal assertion verifier is unavailable: set {_ENGINEERING_REVIEW_SIGNING_KEY_ENV}"
        )
    binding = {
        "workflow_run_id": workflow_run_id,
        "candidate_receipt_id": candidate_receipt_id,
        "review": dict(review),
    }
    body = {
        "schema": _PRINCIPAL_ASSERTION_SCHEMA,
        "principal_ref": principal_ref,
        "source_class": "real_human",
        "issued_at": issued_at or _utc_now(),
        "binding_digest": hashlib.sha256(_canonical(binding).encode("utf-8")).hexdigest(),
    }
    signature = hmac.new(
        signing_key.encode("utf-8"),
        _canonical(body).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {**body, "signature": signature}


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
        root=root,
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
        existing_records = _validate_primary_records(log.read_all(), root)
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
            _validate_qualified_record(qualified, root)
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
    query_only: bool = True,
) -> dict[str, Any]:
    """Count qualified outcomes in the half-open rolling 7-day window without writes."""
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
    snapshots: dict[Path, _ShadowObserverInputSnapshot] = {}
    workspace_identity: tuple[int, int, int, int, int] | None = None
    try:
        root = Path(omo_dir)
        if not query_only:
            raise _ShadowObserverInputError("shadow observer only supports query-only reads")
        snapshots, workspace_identity = _read_shadow_observer_inputs(root)
        records = _validate_primary_records(
            _read_shadow_observer_jsonl(snapshots[root / QUALIFIED_DECISION_OUTCOME_LOG]),
            root,
        )
        projection_records = [
            _validate_projection_receipt(record)
            for record in _read_shadow_observer_jsonl(snapshots[root / MOS_PROJECTION_RECEIPT_LOG])
        ]
        feedback_records = [
            validate_outcome_feedback(record)
            for record in _read_shadow_observer_jsonl(snapshots[root / OUTCOME_FEEDBACK_LOG])
        ]
        workflow_events = _read_shadow_observer_jsonl(snapshots[root / WORKFLOW_MESH_LOG])
        latest_projection = {str(item["decision_outcome_id"]): item for item in projection_records}
        projected_receipts = [item for item in latest_projection.values() if item.get("status") == "projected"]
        if projected_receipts:
            mos_state_path = _workspace_root(root) / ".omo" / "state" / "agent-beliefs" / "index.yaml"
            mos_snapshot = snapshots[mos_state_path]
            if mos_snapshot.payload is None:
                raise EngineeringDeliveryConsumerError("MOS projection state is unavailable")
            mos_state = load_yaml_value_docs(mos_snapshot.payload.decode("utf-8")) or {}
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
            snapshot = project_workflow_run(workflow_events, str(record["workflow_run_id"]))
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
        _verify_shadow_observer_inputs(
            snapshots,
            workspace_root=_workspace_root(root),
            workspace_identity=workspace_identity,
        )
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, yaml.YAMLError) as exc:
        return {
            **result,
            "status": "unprovable",
            "verdict": "UNPROVABLE",
            "qualifying_decision_outcomes": None,
            "error": (
                "qualified_decision_outcomes_changed_during_read"
                if isinstance(exc, _ShadowObserverInputChangedError)
                else "qualified_decision_outcomes_unreadable"
            ),
        }
    finally:
        _close_shadow_observer_inputs(snapshots)
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
    "CONTROLS",
    "EngineeringDeliveryConsumerError",
    "EngineeringDeliveryProjectionError",
    "MOS_PROJECTION_RECEIPT_LOG",
    "OUTCOME_FEEDBACK_LOG",
    "QUALIFIED_DECISION_OUTCOME_LOG",
    "QUALIFIED_DECISION_OUTCOME_SCHEMA",
    "REVIEW_QUEUE_SCHEMA",
    "REVIEW_SCHEMA",
    "SCENE_BINDING",
    "SCENE_POLICY",
    "SHADOW_OBSERVER_SCHEMA",
    "_DECISIONS",
    "_DELIVERY_FIELDS",
    "_ENGINEERING_REVIEW_SIGNING_KEY_ENV",
    "_ENGINEERING_REVIEW_SIGNING_KEY_FILE",
    "_FORBIDDEN_KEY_PARTS",
    "_HUMAN_ACTOR_SCHEMES",
    "_NON_HUMAN_EVIDENCE",
    "_OPAQUE_REF_SCHEMES",
    "_PRINCIPAL_ASSERTION_MAX_AGE",
    "_PRINCIPAL_ASSERTION_SCHEMA",
    "_REVIEW_FIELDS",
    "_SHADOW_OBSERVER_INPUT_MAX_BYTES",
    "_SHADOW_OBSERVER_TOTAL_MAX_BYTES",
    "_ShadowObserverInputChangedError",
    "_ShadowObserverInputError",
    "_ShadowObserverInputSnapshot",
    "_append_projection_status",
    "_canonical",
    "_close_shadow_observer_inputs",
    "_digest_shadow_observer_bytes",
    "_evidence_refs",
    "_human_actor",
    "_opaque_id",
    "_open_shadow_observer_leaf",
    "_open_shadow_observer_workspace",
    "_project_to_mos",
    "_projection_records",
    "_qualified_log",
    "_qualified_records",
    "_read_shadow_observer_bytes",
    "_read_shadow_observer_input",
    "_read_shadow_observer_inputs",
    "_read_shadow_observer_jsonl",
    "_reject_forbidden_keys",
    "_required_text",
    "_sha256",
    "_shadow_observer_directory_flags",
    "_shadow_observer_identity",
    "_shadow_observer_input_paths",
    "_shadow_observer_relative_parts",
    "_signing_key",
    "_strict_envelope",
    "_timestamp",
    "_uri",
    "_utc_now",
    "_validate_delivery",
    "_validate_primary_records",
    "_validate_projection_receipt",
    "_validate_qualified_record",
    "_verify_principal_assertion",
    "_verify_shadow_observer_input",
    "_verify_shadow_observer_inputs",
    "_workspace_root",
    "build_engineering_delivery_review_queue",
    "build_engineering_delivery_shadow_observer",
    "build_principal_assertion",
    "consume_engineering_delivery",
    "normalize_engineering_delivery_review",
    "record_engineering_delivery_review",
    "MOSBeliefManager",
]
