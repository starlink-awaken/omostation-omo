"""Validation and normalization helpers for engineering delivery consumer."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from .engineering_delivery_consumer_constants import (
    _DECISIONS,
    _DELIVERY_FIELDS,
    _ENGINEERING_REVIEW_SIGNING_KEY_ENV,
    _ENGINEERING_REVIEW_SIGNING_KEY_FILE,
    _FORBIDDEN_KEY_PARTS,
    _HUMAN_ACTOR_SCHEMES,
    _NON_HUMAN_EVIDENCE,
    _OPAQUE_REF_SCHEMES,
    _PRINCIPAL_ASSERTION_MAX_AGE,
    _PRINCIPAL_ASSERTION_SCHEMA,
    _REVIEW_FIELDS,
)
from .omo_belief import MOSBeliefManager
from .omo_external_receipt import RECEIPT_SCHEMA, record_external_receipt
from .omo_io import AppendOnlyLog, fcntl_lock
from .outcome_feedback import (
    OUTCOME_FEEDBACK_LOG,
    read_outcome_feedback,
    record_outcome_feedback,
    validate_outcome_feedback,
)


class EngineeringDeliveryConsumerError(ValueError):
    """Invalid or missing engineering delivery payload."""


class EngineeringDeliveryProjectionError(OSError):
    """MOS projection or qualified-record write failure."""


def _signing_key(root: Path | None = None) -> str:
    """Resolve the server-owned review signing key (env wins, file fallback)."""
    from_env = os.environ.get(_ENGINEERING_REVIEW_SIGNING_KEY_ENV, "")
    if len(from_env) >= 32:
        return from_env
    key_path = (root if root is not None else Path(".omo")) / _ENGINEERING_REVIEW_SIGNING_KEY_FILE
    try:
        return key_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise EngineeringDeliveryConsumerError("verifier is unavailable") from exc


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _required_text(value: Any, field: str, *, max_length: int = 500) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EngineeringDeliveryConsumerError(f"{field} is required")
    text = value.strip()
    if len(text) > max_length:
        raise EngineeringDeliveryConsumerError(f"{field} exceeds max length {max_length}")
    return text


def _timestamp(value: Any, field: str) -> str:
    text = _required_text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EngineeringDeliveryConsumerError(f"{field} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise EngineeringDeliveryConsumerError(f"{field} requires timezone information")
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _reject_forbidden_keys(value: Any, path: str = "payload") -> None:
    if not isinstance(value, Mapping):
        return
    for key in value.keys():
        if not isinstance(key, str):
            continue
        normalized = key.lower().replace("_", "").replace("-", "")
        if any(part in normalized for part in _FORBIDDEN_KEY_PARTS):
            raise EngineeringDeliveryConsumerError(f"forbidden key {key!r} in {path}")


def _strict_envelope(payload: Mapping[str, Any], allowed: frozenset[str], name: str) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise EngineeringDeliveryConsumerError(f"{name} envelope must be an object")
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise EngineeringDeliveryConsumerError(f"unsupported {name} fields: {unknown}")
    _reject_forbidden_keys(payload)
    return dict(payload)


def _uri(value: Any, field: str, *, schemes: frozenset[str] | None = None) -> str:
    text = _required_text(value, field, max_length=2048)
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
    envelope = _strict_envelope(payload, _DELIVERY_FIELDS, "delivery")
    delivery_id = _required_text(envelope.get("delivery_id"), "delivery_id")
    repository_ref = _uri(envelope.get("repository_ref"), "repository_ref")
    pr_url = _uri(envelope.get("pr_url"), "pr_url")
    merge_sha = _required_text(envelope.get("merge_sha"), "merge_sha")
    requested_at = _timestamp(envelope.get("requested_at"), "requested_at")
    merged_at = _timestamp(envelope.get("merged_at"), "merged_at")
    evidence_refs = _evidence_refs(envelope.get("evidence_refs"), required=True, human_review=True)
    return {
        "delivery_id": delivery_id,
        "repository_ref": repository_ref,
        "pr_url": pr_url,
        "merge_sha": merge_sha,
        "requested_at": requested_at,
        "merged_at": merged_at,
        "evidence_refs": evidence_refs,
    }


def normalize_engineering_delivery_review(payload: Mapping[str, Any]) -> dict[str, Any]:
    envelope = _strict_envelope(payload, _REVIEW_FIELDS, "review")
    decision = _required_text(envelope.get("decision"), "decision", max_length=32).lower()
    if decision not in _DECISIONS:
        raise EngineeringDeliveryConsumerError(f"unsupported human decision: {decision}")
    return {
        "delivery_id": _opaque_id(envelope.get("delivery_id"), "delivery_id"),
        "decision": decision,
        "evidence_refs": _evidence_refs(envelope.get("evidence_refs"), required=True, human_review=True),
    }


def _human_actor(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EngineeringDeliveryConsumerError("human actor is required")
    text = value.strip()
    scheme = text.split(":", 1)[0] if ":" in text else ""
    if scheme not in _HUMAN_ACTOR_SCHEMES:
        raise EngineeringDeliveryConsumerError(f"unsupported human actor scheme {scheme!r}")
    return text


