"""Production verifier interfaces for auxiliary R0 operator evidence (Wave E)."""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .claims_authority import AuthorityError, canonical_digest, canonical_json

_OPERATOR_AUTHORIZATION_SCHEMA = "claims-operator-authorization/v1"
_STOPPED_PROCESS_PROOF_SCHEMA = "claims-stopped-process-proof/v1"
_OPERATOR_AUTHORIZATION_FIELDS = frozenset(
    {
        "schema",
        "authority_id",
        "security_level",
        "principal_id",
        "principal_authority_ref",
        "principal_receipt_digest",
        "decision_ref",
        "target_kind",
        "target_id",
        "unknown_receipt_digest",
        "resolver_operation",
        "authorized_outcome",
        "process_identity_digest",
        "issued_at",
        "expires_at",
        "digest",
    }
)
_STOPPED_PROCESS_PROOF_FIELDS = frozenset(
    {
        "schema",
        "authority_id",
        "security_level",
        "observer_kind",
        "observer_receipt_digest",
        "target_kind",
        "target_id",
        "unknown_receipt_digest",
        "authorization_digest",
        "process_identity_digest",
        "status",
        "observed_at",
        "digest",
    }
)
_TARGET_KINDS = frozenset({"claim_mutation", "legacy_fence"})
_MAX_EVIDENCE_BYTES = 16_384
_MAX_AUTHORIZATION_LIFETIME_SECONDS = 300
_SHA256_RE_PREFIX = "sha256:"


def _parse_utc(value: object, *, code: str, detail: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise AuthorityError(code, detail) from exc
    if parsed.tzinfo is None:
        raise AuthorityError(code, detail)
    return parsed.astimezone(UTC)


def _require_mapping(obj: object, *, code: str, detail: str) -> dict[str, Any]:
    if not isinstance(obj, Mapping):
        raise AuthorityError(code, detail)
    return dict(obj)


def _require_digest_field(value: object, *, code: str, detail: str) -> str:
    text = str(value or "")
    if not text.startswith(_SHA256_RE_PREFIX) or len(text) != 71:
        raise AuthorityError(code, detail)
    hex_part = text.removeprefix(_SHA256_RE_PREFIX)
    if any(ch not in "0123456789abcdef" for ch in hex_part):
        raise AuthorityError(code, detail)
    return text


def _assert_canonical_size_and_digest(payload: Mapping[str, Any], *, code: str) -> str:
    encoded = canonical_json(payload).encode("utf-8")
    if len(encoded) > _MAX_EVIDENCE_BYTES:
        raise AuthorityError(code, "evidence_too_large")
    digest = payload.get("digest")
    if not isinstance(digest, str) or digest != canonical_digest(payload):
        raise AuthorityError(code, "evidence_digest")
    return digest


def _symbol_digest(function: Any) -> str:
    source = inspect.getsource(function)
    identity = {
        "module": "omo.workflow.claims_verifiers",
        "symbol": function.__name__,
        "source": source,
    }
    payload = canonical_json(identity).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def operator_authorization_verifier_digest() -> str:
    return _symbol_digest(verify_operator_authorization)


def stopped_process_verifier_digest() -> str:
    return _symbol_digest(verify_stopped_process_proof)


def production_verifier_dependency_entries() -> list[dict[str, str]]:
    entries = [
        {
            "path": "omo.workflow.claims_verifiers:verify_operator_authorization",
            "kind": "blob",
            "object_oid_or_digest": operator_authorization_verifier_digest(),
        },
        {
            "path": "omo.workflow.claims_verifiers:verify_stopped_process_proof",
            "kind": "blob",
            "object_oid_or_digest": stopped_process_verifier_digest(),
        },
    ]
    return sorted(entries, key=lambda item: item["path"])


def production_verifier_closure_digest() -> str:
    return canonical_digest({"critical_dependency_entries": production_verifier_dependency_entries()})


def verify_operator_authorization(obj: object) -> str:
    payload = _require_mapping(obj, code="OPERATOR_AUTHORIZATION_REQUIRED", detail="authorization_type")
    if set(payload) != _OPERATOR_AUTHORIZATION_FIELDS:
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_fields")
    if payload.get("schema") != _OPERATOR_AUTHORIZATION_SCHEMA:
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_schema")
    if payload.get("security_level") != "R0_COOPERATIVE":
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_security_level")
    if payload.get("target_kind") not in _TARGET_KINDS:
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_target_kind")
    for field in (
        "authority_id",
        "principal_id",
        "principal_authority_ref",
        "decision_ref",
        "target_id",
        "resolver_operation",
        "authorized_outcome",
    ):
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", f"authorization_{field}")
    for field in (
        "principal_receipt_digest",
        "unknown_receipt_digest",
        "process_identity_digest",
    ):
        _require_digest_field(
            payload.get(field),
            code="OPERATOR_AUTHORIZATION_REQUIRED",
            detail=f"authorization_{field}",
        )
    issued_at = _parse_utc(
        payload.get("issued_at"),
        code="OPERATOR_AUTHORIZATION_REQUIRED",
        detail="authorization_issued_at",
    )
    expires_at = _parse_utc(
        payload.get("expires_at"),
        code="OPERATOR_AUTHORIZATION_REQUIRED",
        detail="authorization_expires_at",
    )
    lifetime = (expires_at - issued_at).total_seconds()
    if lifetime <= 0 or lifetime > _MAX_AUTHORIZATION_LIFETIME_SECONDS:
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_lifetime")
    return _assert_canonical_size_and_digest(payload, code="OPERATOR_AUTHORIZATION_REQUIRED")


def verify_stopped_process_proof(
    obj: object,
    *,
    authorization: Mapping[str, Any] | None = None,
    authorization_issued_at: object | None = None,
) -> str:
    payload = _require_mapping(obj, code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID", detail="stopped_process_type")
    if set(payload) != _STOPPED_PROCESS_PROOF_FIELDS:
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_fields")
    if payload.get("schema") != _STOPPED_PROCESS_PROOF_SCHEMA:
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_schema")
    if payload.get("security_level") != "R0_COOPERATIVE":
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_security_level")
    if payload.get("target_kind") not in _TARGET_KINDS:
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_target_kind")
    if payload.get("status") != "stopped":
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_status")
    for field in ("authority_id", "observer_kind", "target_id"):
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", f"stopped_process_{field}")
    for field in (
        "observer_receipt_digest",
        "unknown_receipt_digest",
        "authorization_digest",
        "process_identity_digest",
    ):
        _require_digest_field(
            payload.get(field),
            code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID",
            detail=f"stopped_process_{field}",
        )
    observed_at = _parse_utc(
        payload.get("observed_at"),
        code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID",
        detail="stopped_process_observed_at",
    )
    issued_at_value: object | None = authorization_issued_at
    if authorization is not None:
        auth_payload = _require_mapping(
            authorization,
            code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID",
            detail="authorization_context",
        )
        issued_at_value = auth_payload.get("issued_at")
        auth_digest = auth_payload.get("digest")
        if isinstance(auth_digest, str) and auth_digest and payload.get("authorization_digest") != auth_digest:
            raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_authorization_digest")
    if issued_at_value is not None:
        issued_at = _parse_utc(
            issued_at_value,
            code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID",
            detail="authorization_issued_at",
        )
        if observed_at < issued_at:
            raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_before_authorization")
    return _assert_canonical_size_and_digest(payload, code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID")
