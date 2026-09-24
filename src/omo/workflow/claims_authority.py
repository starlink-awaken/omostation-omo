"""Canonical R0 claims authority primitives for WP1 shadow operation.

This module is evidence-only during WP1.  It never grants publication authority
and never performs Git, GitHub, service-control, or host-recovery effects.
"""

from __future__ import annotations

import hashlib
import json
import os
import pwd
import re
import sqlite3
import stat
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml

from ..event_ledger.schema import wal_allowed_for_current

_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_OID_RE = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_CANONICAL_REPOSITORY = "starlink-awaken/omostation"
_PRODUCTION_AUTHORITY_ID = "omo-claims-authority-r0"
_OPERATOR_AUTHORIZATION_DIR = "operator-authorizations"
_STOPPED_PROCESS_PROOF_DIR = "stopped-process-proofs"
_OPERATOR_EVIDENCE_MAX_BYTES = 16_384
_ENVELOPE_IDENTITY_FIELDS = {
    "actor_id",
    "delivery_attempt_id",
    "repository",
    "clone_root_digest",
    "branch",
    "clone_identity_digest",
    "manifest_digest",
    "readiness_digest",
    "frozen_base",
    "head_oid",
    "bet_id",
    "work_packet_id",
    "work_packet_digest",
    "spec_ref",
    "spec_digest",
    "run_id",
    "requested_paths_digest",
    "affected_graph_digest",
    "clone_identity_schema",
}

__all__ = (
    "AuthorityError",
    "AuthorityPaths",
    "V1Decision",
    "ShadowDecision",
    "canonical_json",
    "canonical_digest",
    "resolve_authority_paths",
    "dispatch_request",
    "observe_claim",
    "begin_claim_mutation",
    "settle_claim_mutation",
    "mark_claim_mutation_operator_required",
    "resolve_claim_mutation_unknown",
    "activate_shadow",
    "issue_legacy_fence",
    "enter_legacy_publishing",
    "settle_legacy_publication",
    "mark_legacy_operator_required",
    "resolve_legacy_unknown",
    "authority_status",
    "evaluate_graduation",
    "cli_main",
)


class AuthorityError(RuntimeError):
    """Stable, redacted authority failure exposed to callers."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


@dataclass(frozen=True)
class AuthorityPaths:
    account_home: Path
    integration_root: Path
    authority_dir: Path
    store: Path
    high_water: Path
    backups: Path
    activation_witness: Path


@dataclass(frozen=True)
class V1Decision:
    decision: Literal["allow", "deny"]
    code: str

    def __post_init__(self) -> None:
        if self.decision not in {"allow", "deny"} or not self.code:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "v1_decision")


@dataclass(frozen=True)
class ShadowDecision:
    decision: Literal["would_allow", "would_deny", "unprovable"]
    code: str

    def __post_init__(self) -> None:
        if self.decision not in {"would_allow", "would_deny", "unprovable"} or not self.code:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "shadow_decision")


def _digest_preimage(value: Any) -> Any:
    """Remove only an authority object's own top-level digest fields."""
    if not isinstance(value, Mapping):
        return value
    return {key: item for key, item in value.items() if key not in {"digest", "signature"}}


def canonical_json(value: Any) -> str:
    """Encode one authority object using its frozen deterministic JSON form."""
    return json.dumps(
        _digest_preimage(value),
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_digest(value: Any) -> str:
    """Return the frozen SHA-256 identifier for one authority object."""
    payload = canonical_json(value).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def compare_decisions(v1: V1Decision, shadow: ShadowDecision) -> dict[str, Any]:
    if (
        v1.decision == "deny"
        and v1.code == "claims_authority_mismatch"
        and shadow.decision == "would_allow"
        and shadow.code == "valid_managed_clone"
    ):
        classification = "expected_managed_clone_difference"
    elif (v1.decision == "allow" and shadow.decision == "would_allow") or (
        v1.decision == "deny" and shadow.decision == "would_deny"
    ):
        classification = "equivalent"
    else:
        classification = "unexplained"
    return {
        "effective_v1": {"decision": v1.decision, "code": v1.code},
        "shadow_v2": {"decision": shadow.decision, "code": shadow.code},
        "classification": classification,
        "effective_claim_authority": "v1",
        "publication_effect_fence": "legacy-v2-required-after-v1-allow",
        "instruction_capable": False,
    }


def _clock_now() -> datetime:
    return datetime.now(UTC)


def _utc_now() -> str:
    return _clock_now().isoformat().replace("+00:00", "Z")


def _lease_expiry(minutes: int = 15) -> str:
    from datetime import timedelta

    return (_clock_now() + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


def _canonical_json_full(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = _canonical_json_full(payload).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _activation_checkpoint(_stage: str) -> None:
    """Private deterministic crash seam; production execution is a no-op."""


def _witness_payload(
    authority_id: str,
    *,
    state: str,
    sequence: int,
    descriptor_digest: str | None,
    activation_receipt_digest: str | None,
    request_digest: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "claims-activation-witness/v1",
        "authority_id": authority_id,
        "state": state,
        "sequence": sequence,
        "descriptor_digest": descriptor_digest,
        "activation_receipt_digest": activation_receipt_digest,
        "request_digest": request_digest,
    }
    payload["digest"] = canonical_digest(payload)
    return payload


def _high_water_payload(
    authority_id: str,
    *,
    sequence: int,
    receipt_digest: str | None,
    descriptor_digest: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "claims-authority-high-water/v1",
        "authority_id": authority_id,
        "sequence": sequence,
        "receipt_digest": receipt_digest,
        "descriptor_digest": descriptor_digest,
    }
    payload["digest"] = canonical_digest(payload)
    return payload


def _assert_safe_existing_path(path: Path) -> None:
    if not os.path.lexists(path):
        return
    try:
        info = path.lstat()
    except OSError as exc:
        raise AuthorityError("AUTHORITY_STORE_UNSAFE", "path_unreadable") from exc
    if stat.S_ISLNK(info.st_mode):
        raise AuthorityError("AUTHORITY_STORE_UNSAFE", "symlink")
    if info.st_uid != os.getuid():
        raise AuthorityError("AUTHORITY_STORE_UNSAFE", "owner")
    if stat.S_IMODE(info.st_mode) & 0o022:
        raise AuthorityError("AUTHORITY_STORE_UNSAFE", "mode")


def resolve_authority_paths() -> AuthorityPaths:
    """Resolve production authority from the OS account, never caller state."""
    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    integration_root = account_home / "Workspace"
    authority_dir = account_home / "agents/_shared/runtime/omo-claims-authority-r0"
    paths = AuthorityPaths(
        account_home=account_home,
        integration_root=integration_root,
        authority_dir=authority_dir,
        store=authority_dir / "store.sqlite3",
        high_water=authority_dir / "high-water.json",
        backups=authority_dir / "backups",
        activation_witness=authority_dir / "activation-witness.json",
    )
    for path in (
        account_home,
        integration_root,
        account_home / "agents",
        account_home / "agents/_shared",
        account_home / "agents/_shared/runtime",
        authority_dir,
        paths.store,
        paths.high_water,
        paths.backups,
        paths.activation_witness,
    ):
        _assert_safe_existing_path(path)
    return paths


def _read_json_mapping(path: Path, *, code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AuthorityError(code, "malformed") from exc
    if not isinstance(payload, dict):
        raise AuthorityError(code, "malformed")
    return payload


def _read_trusted_operator_evidence(
    paths: AuthorityPaths,
    digest: object,
    *,
    directory_name: str,
    code: str,
) -> dict[str, Any]:
    evidence_digest = _require_digest(digest, code=code, detail="digest")
    evidence_dir = paths.authority_dir / directory_name
    evidence_path = evidence_dir / f"{evidence_digest.removeprefix('sha256:')}.json"
    for path in (paths.authority_dir, evidence_dir, evidence_path):
        _assert_safe_existing_path(path)
    if not evidence_dir.is_dir() or not evidence_path.is_file():
        raise AuthorityError(code, "canonical_evidence_missing")
    try:
        info = evidence_path.lstat()
        raw = evidence_path.read_bytes()
    except OSError as exc:
        raise AuthorityError(code, "canonical_evidence_unreadable") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or len(raw) > _OPERATOR_EVIDENCE_MAX_BYTES:
        raise AuthorityError(code, "canonical_evidence_unsafe")
    try:
        text = raw.decode("utf-8")
        payload = json.loads(text)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AuthorityError(code, "canonical_evidence_malformed") from exc
    if not isinstance(payload, dict) or text != _canonical_json_full(payload):
        raise AuthorityError(code, "canonical_evidence_malformed")
    if payload.get("digest") != evidence_digest or canonical_digest(payload) != evidence_digest:
        raise AuthorityError(code, "canonical_evidence_digest")
    return payload


def _operator_evidence_time(value: object, *, code: str, detail: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise AuthorityError(code, detail) from exc
    if parsed.tzinfo is None:
        raise AuthorityError(code, detail)
    return parsed.astimezone(UTC)


def _broker_unavailable_policy(paths: AuthorityPaths) -> dict[str, Any]:
    """Apply the fixed deny-only witness rule when canonical stdio is absent."""
    for path in (paths.authority_dir, paths.store, paths.high_water, paths.backups, paths.activation_witness):
        _assert_safe_existing_path(path)

    witness_exists = paths.activation_witness.is_file()
    initialized = paths.store.exists() or paths.high_water.exists() or paths.backups.exists()
    if not witness_exists:
        if initialized or os.path.lexists(paths.activation_witness):
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "missing_after_initialization")
        return {"allow_v1": True, "code": "not_activated", "witness_state": "pristine"}

    witness = _read_json_mapping(paths.activation_witness, code="AUTHORITY_ACTIVATION_WITNESS_INVALID")
    required = {
        "schema": "claims-activation-witness/v1",
        "authority_id": "omo-claims-authority-r0",
    }
    if any(witness.get(key) != value for key, value in required.items()):
        raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "identity")
    if witness.get("digest") != canonical_digest(witness):
        raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "digest")
    sequence = witness.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "sequence")

    state = witness.get("state")
    if state == "unactivated":
        if (
            sequence != 0
            or witness.get("descriptor_digest") is not None
            or witness.get("activation_receipt_digest") is not None
        ):
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "rollback")
        if paths.high_water.exists():
            high_water = _read_json_mapping(paths.high_water, code="AUTHORITY_ACTIVATION_WITNESS_INVALID")
            persisted_sequence = high_water.get("sequence")
            if (
                high_water.get("schema") != "claims-authority-high-water/v1"
                or high_water.get("authority_id") != "omo-claims-authority-r0"
                or high_water.get("digest") != canonical_digest(high_water)
                or not isinstance(persisted_sequence, int)
                or isinstance(persisted_sequence, bool)
                or persisted_sequence != 0
                or high_water.get("receipt_digest") is not None
                or high_water.get("descriptor_digest") is not None
            ):
                raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "rollback")
        return {"allow_v1": True, "code": "not_activated", "witness_state": "unactivated"}

    if state in {"prepared", "shadow-active"}:
        raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", state)
    raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "state")


def _uuid4(value: object, *, code: str = "REQUEST_SCHEMA_INVALID") -> str:
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise AuthorityError(code, "request_id") from exc
    if parsed.version != 4 or str(parsed) != str(value).lower():
        raise AuthorityError(code, "request_id")
    return str(parsed)


def _new_request_id() -> str:
    return str(uuid.uuid4())


def _require_digest(value: object, *, code: str, detail: str) -> str:
    candidate = str(value or "")
    if not _SHA256_RE.fullmatch(candidate):
        raise AuthorityError(code, detail)
    return candidate


def _reject_unknown_fields(request: Mapping[str, Any], allowed: set[str], *, detail: str) -> None:
    unknown = sorted(set(request) - allowed)
    if unknown:
        raise AuthorityError("REQUEST_SCHEMA_INVALID", f"{detail}_unknown_fields")



def _validate_publication_scoped_allow(request: Mapping[str, Any]) -> None:
    """ADR-0455 option A — v2 managed-clone allow only when publication-scoped.

    General allow remains forbidden. Scope must bind exact changed_paths and a
    one-shot legacy fence effect ceiling; paths digest must match the observe
    request's requested_paths_digest.
    """
    scope = request.get("publication_scope")
    if not isinstance(scope, Mapping):
        raise AuthorityError("V1_AUTHORITY_FORBIDDEN", "managed_clone")
    if scope.get("schema") != "claims-publication-scope/v1":
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "publication_scope_schema")
    if scope.get("kind") != "legacy-publication":
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "publication_scope_kind")
    if scope.get("effect_ceiling") != "one-legacy-fence":
        raise AuthorityError("V1_AUTHORITY_FORBIDDEN", "publication_scope_ceiling")
    paths = scope.get("changed_paths")
    if not isinstance(paths, list) or not paths:
        raise AuthorityError("V1_AUTHORITY_FORBIDDEN", "publication_scope_paths")
    if not all(isinstance(p, str) and p and not p.startswith("/") for p in paths):
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "publication_scope_path_value")
    if len(set(paths)) != len(paths):
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "publication_scope_path_dup")
    if scope.get("paths_digest") != canonical_digest(paths):
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "publication_scope_paths_digest")
    if request.get("requested_paths_digest") != scope.get("paths_digest"):
        raise AuthorityError("CLAIM_SCOPE_VIOLATION", "publication_scope_bound")


def _validate_observe_request(request: Mapping[str, Any]) -> None:
    _reject_unknown_fields(
        request,
        {
            "schema",
            "operation",
            "request_id",
            "authority_id",
            "actor_id",
            "delivery_attempt_id",
            "repository",
            "clone_root_digest",
            "branch",
            "clone_identity_digest",
            "manifest_digest",
            "readiness_digest",
            "frozen_base",
            "head_oid",
            "bet_id",
            "work_packet_id",
            "work_packet_digest",
            "spec_ref",
            "spec_digest",
            "affected_graph_digest",
            "requested_paths_digest",
            "expected_claim_version",
            "run_id",
            "claim_ordinal",
            "v1_claim_digest",
            "v1_run_digest",
            "v1_lock_set_digest",
            "v1_decision",
            "authority_mode",
            "clone_identity_schema",
            "publication_scope",
        },
        detail="observe_claim",
    )
    if "promoted_v1_receipt" in request:
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "v1_promotion")
    actor_id = str(request.get("actor_id") or "")
    attempt_id = str(request.get("delivery_attempt_id") or "")
    if not _IDENTIFIER_RE.fullmatch(actor_id) or not _IDENTIFIER_RE.fullmatch(attempt_id):
        raise AuthorityError("IDENTITY_MISMATCH", "actor_attempt")
    if request.get("repository") != _CANONICAL_REPOSITORY:
        raise AuthorityError("IDENTITY_MISMATCH", "repository")
    if request.get("branch") != f"agent/{actor_id}--{attempt_id}":
        raise AuthorityError("IDENTITY_MISMATCH", "branch")
    for field in ("frozen_base", "head_oid"):
        if not _OID_RE.fullmatch(str(request.get(field) or "")):
            raise AuthorityError("IDENTITY_MISMATCH", field)
    for field in ("clone_root_digest", "clone_identity_digest", "manifest_digest", "readiness_digest"):
        _require_digest(request.get(field), code="IDENTITY_MISMATCH", detail=field)
    for field in ("run_id",):
        if not str(request.get(field) or ""):
            raise AuthorityError("IDENTITY_MISMATCH", field)
    claim_ordinal = request.get("claim_ordinal")
    if not isinstance(claim_ordinal, int) or isinstance(claim_ordinal, bool) or claim_ordinal < 0:
        raise AuthorityError("IDENTITY_MISMATCH", "claim_ordinal")
    _require_digest(request.get("v1_claim_digest"), code="IDENTITY_MISMATCH", detail="v1_claim_digest")
    _require_digest(request.get("v1_run_digest"), code="IDENTITY_MISMATCH", detail="v1_run_digest")
    _require_digest(
        request.get("v1_lock_set_digest"),
        code="IDENTITY_MISMATCH",
        detail="v1_lock_set_digest",
    )

    bet_id = str(request.get("bet_id") or "")
    if not bet_id.startswith("BET-") or request.get("work_packet_id") != f"WP-{bet_id}":
        raise AuthorityError("WORK_PACKET_UNBOUND", "packet_identity")
    _require_digest(request.get("work_packet_digest"), code="WORK_PACKET_UNBOUND", detail="work_packet_digest")
    if not str(request.get("spec_ref") or "").startswith("repo://"):
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_ref")
    _require_digest(request.get("spec_digest"), code="WORK_PACKET_UNBOUND", detail="spec_digest")
    _require_digest(
        request.get("requested_paths_digest"),
        code="CLAIM_SCOPE_VIOLATION",
        detail="requested_paths_digest",
    )
    _require_digest(
        request.get("affected_graph_digest"),
        code="AFFECTED_GRAPH_MISMATCH",
        detail="affected_graph_digest",
    )
    expected_version = request.get("expected_claim_version")
    if not isinstance(expected_version, int) or isinstance(expected_version, bool) or expected_version < 0:
        raise AuthorityError("CLAIM_VERSION_STALE", "expected_claim_version")

    v1_decision = request.get("v1_decision")
    if not isinstance(v1_decision, Mapping) or v1_decision.get("decision") not in {"allow", "deny"}:
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "v1_decision")
    if not str(v1_decision.get("code") or ""):
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "v1_code")
    if request.get("authority_mode") == "cutover" and v1_decision.get("decision") == "allow":
        raise AuthorityError("V1_AUTHORITY_FORBIDDEN", "post_cutover")
    if request.get("clone_identity_schema") == "agent-clone-identity/v2" and v1_decision.get("decision") == "allow":
        # ADR-0455 方案 A: 仅允许绑定 exact changed_paths + 单次 fence 效果上限的 allow
        _validate_publication_scoped_allow(request)


def _read_head_oid(clone_root: Path, identity: Mapping[str, Any]) -> str:
    try:
        head = (clone_root / ".git/HEAD").read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise AuthorityError("IDENTITY_MISMATCH", "head") from exc
    if _OID_RE.fullmatch(head):
        return head
    if head.startswith("ref: "):
        ref = head.removeprefix("ref: ")
        ref_path = clone_root / ".git" / ref
        if ref_path.is_file():
            candidate = ref_path.read_text(encoding="utf-8").strip()
            if _OID_RE.fullmatch(candidate):
                return candidate
        packed = clone_root / ".git/packed-refs"
        if packed.is_file():
            for line in packed.read_text(encoding="utf-8").splitlines():
                parts = line.split(" ", 1)
                if len(parts) == 2 and parts[1] == ref and _OID_RE.fullmatch(parts[0]):
                    return parts[0]
    fallback = str(identity.get("frozen_root_sha") or "")
    if _OID_RE.fullmatch(fallback):
        return fallback
    raise AuthorityError("IDENTITY_MISMATCH", "head")


def _safe_repo_relative(root: Path, raw: str, *, code: str) -> Path:
    candidate = Path(raw)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise AuthorityError(code, "path")
    resolved = (root / candidate).resolve(strict=False)
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise AuthorityError(code, "path") from exc
    return resolved


_REMOTE_OBSERVATION_FIELDS = {
    "schema",
    "repository_identity_digest",
    "remote_ref_digest",
    "observed_remote_oid",
    "monotonic_ns",
    "broker_observed_at",
    "git_executable_digest",
    "effect_process_identity_digest",
    "command_digest",
    "descriptor_digest",
}


def _validate_remote_observation_pair(
    request: Mapping[str, Any],
    *,
    descriptor_digest: str,
    remote_ref: str,
    observed_remote_oid: str,
    effect_process_identity_digest: str | None = None,
) -> str:
    first = request.get("first_remote_observation")
    second = request.get("second_remote_observation")
    if not isinstance(first, Mapping) or not isinstance(second, Mapping):
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "remote_observation_pair")
    if set(first) != _REMOTE_OBSERVATION_FIELDS or set(second) != _REMOTE_OBSERVATION_FIELDS:
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "remote_observation_fields")
    stable_fields = _REMOTE_OBSERVATION_FIELDS - {"monotonic_ns", "broker_observed_at"}
    for observation in (first, second):
        if observation.get("schema") != "claims-remote-observation/v2":
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "remote_observation_schema")
        for field in (
            "repository_identity_digest",
            "remote_ref_digest",
            "git_executable_digest",
            "effect_process_identity_digest",
            "command_digest",
            "descriptor_digest",
        ):
            _require_digest(observation.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
        monotonic_ns = observation.get("monotonic_ns")
        if not isinstance(monotonic_ns, int) or isinstance(monotonic_ns, bool) or monotonic_ns < 0:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "monotonic_ns")
        try:
            observed_at = datetime.fromisoformat(
                str(observation.get("broker_observed_at") or "").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "broker_observed_at") from exc
        age = (_clock_now() - observed_at).total_seconds()
        if not -5 <= age <= 120:
            raise AuthorityError("PUBLISH_INTENT_EXPIRED", "remote_observation")
    if int(second["monotonic_ns"]) < int(first["monotonic_ns"]):
        raise AuthorityError("REMOTE_OID_DRIFT", "monotonic_order")
    if datetime.fromisoformat(str(second["broker_observed_at"]).replace("Z", "+00:00")) < datetime.fromisoformat(
        str(first["broker_observed_at"]).replace("Z", "+00:00")
    ):
        raise AuthorityError("REMOTE_OID_DRIFT", "observation_order")
    for field in stable_fields:
        if first.get(field) != second.get(field):
            raise AuthorityError("REMOTE_OID_DRIFT", field)
    if first.get("descriptor_digest") != descriptor_digest:
        raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "remote_observation")
    if first.get("remote_ref_digest") != canonical_digest({"remote_ref": remote_ref}):
        raise AuthorityError("REMOTE_OID_DRIFT", "remote_ref")
    if first.get("observed_remote_oid") != observed_remote_oid or not _OID_RE.fullmatch(observed_remote_oid):
        raise AuthorityError("REMOTE_OID_DRIFT", "observed_remote_oid")
    if (
        effect_process_identity_digest is not None
        and first.get("effect_process_identity_digest") != effect_process_identity_digest
    ):
        raise AuthorityError("IDENTITY_MISMATCH", "effect_process")
    return canonical_digest([dict(first), dict(second)])


def _recompute_lock_set_digest(
    clone_root: Path,
    run_id: str,
    run: Mapping[str, Any],
) -> str:
    lock_dir = clone_root / ".omo/_delivery/agent-workflows/locks"
    raw_locks = run.get("locks")
    if raw_locks is None:
        raw_locks = []
    if not isinstance(raw_locks, list) or not all(isinstance(item, str) and item for item in raw_locks):
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "lock_set")
    selected = {str(item) for item in raw_locks}
    if lock_dir.exists():
        for candidate in lock_dir.glob("*.lock.yaml"):
            try:
                lock = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
            except (OSError, UnicodeError, yaml.YAMLError):
                lock = {}
            if isinstance(lock, dict) and lock.get("run_id") == run_id:
                selected.add(str(candidate))
    resolved_locks: dict[str, Path] = {}
    for raw in selected:
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = clone_root / candidate
        resolved = candidate.resolve(strict=False)
        try:
            resolved.relative_to(lock_dir.resolve())
        except ValueError as exc:
            raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "lock_path") from exc
        resolved_locks[str(resolved)] = resolved
    lock_records = []
    for resolved in sorted(resolved_locks.values(), key=str):
        exists = resolved.is_file()
        lock_records.append(
            {
                "path": str(resolved.relative_to(clone_root.resolve())),
                "exists": exists,
                "content_digest": (f"sha256:{hashlib.sha256(resolved.read_bytes()).hexdigest()}" if exists else None),
            }
        )
    return canonical_digest(lock_records)


def _verify_production_observe_request(
    request: Mapping[str, Any],
    paths: AuthorityPaths,
) -> dict[str, Any]:
    """Reread every caller identity from the account-managed attempt tree."""
    _validate_observe_request(request)
    if request.get("authority_id") != _PRODUCTION_AUTHORITY_ID:
        raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
    actor = str(request["actor_id"])
    attempt = str(request["delivery_attempt_id"])
    clone_root = paths.account_home / "agents" / actor / "attempts" / attempt / "ws"
    for candidate in (
        clone_root,
        clone_root / ".git",
        clone_root / ".git/agent-clone-identity.json",
        clone_root / ".git/agent-clone-provenance.json",
        clone_root / ".git/agent-clone-readiness.json",
        clone_root / ".git/HEAD",
    ):
        _assert_safe_existing_path(candidate)
    identity = _read_json_mapping(
        clone_root / ".git/agent-clone-identity.json",
        code="IDENTITY_MISMATCH",
    )
    provenance = _read_json_mapping(
        clone_root / ".git/agent-clone-provenance.json",
        code="IDENTITY_MISMATCH",
    )
    readiness = _read_json_mapping(
        clone_root / ".git/agent-clone-readiness.json",
        code="IDENTITY_MISMATCH",
    )
    repository = str(provenance.get("repository", {}).get("canonical_repository") or "")
    repository = repository.removeprefix("github.com/")
    identity_checks = {
        "actor_id": identity.get("actor_id"),
        "delivery_attempt_id": identity.get("delivery_attempt_id"),
        "repository": repository,
        "clone_root_digest": canonical_digest({"clone_root": str(clone_root.resolve())}),
        "branch": provenance.get("working_branch"),
        "clone_identity_digest": canonical_digest(identity),
        "manifest_digest": canonical_digest(identity.get("transport", {})),
        "readiness_digest": f"sha256:{readiness.get('receipt_digest')}",
        "frozen_base": identity.get("frozen_root_sha"),
        "head_oid": _read_head_oid(clone_root, identity),
        "clone_identity_schema": identity.get("schema"),
    }
    for field, actual in identity_checks.items():
        if request.get(field) != actual:
            raise AuthorityError("IDENTITY_MISMATCH", field)
    if Path(str(identity.get("canonical_root") or "")).resolve() != clone_root.resolve():
        raise AuthorityError("IDENTITY_MISMATCH", "canonical_root")

    run_id = str(request.get("run_id") or "")
    if not _IDENTIFIER_RE.fullmatch(run_id):
        raise AuthorityError("IDENTITY_MISMATCH", "run_id")
    run_path = clone_root / f".omo/_delivery/agent-workflows/runs/{run_id}.yaml"
    _assert_safe_existing_path(run_path)
    try:
        run_bytes = run_path.read_bytes()
        run = yaml.safe_load(run_bytes) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise AuthorityError("IDENTITY_MISMATCH", "run") from exc
    if not isinstance(run, dict) or run.get("run_id") != run_id or run.get("actor") != actor:
        raise AuthorityError("IDENTITY_MISMATCH", "run")
    if request.get("v1_run_digest") != f"sha256:{hashlib.sha256(run_bytes).hexdigest()}":
        raise AuthorityError("IDENTITY_MISMATCH", "v1_run_digest")
    if request.get("v1_lock_set_digest") != _recompute_lock_set_digest(clone_root, run_id, run):
        raise AuthorityError("IDENTITY_MISMATCH", "v1_lock_set_digest")
    if run.get("bet_id") != request.get("bet_id"):
        raise AuthorityError("WORK_PACKET_UNBOUND", "bet_id")
    work_packet = run.get("work_packet")
    spec_binding = run.get("spec_binding")
    claims = run.get("claims")
    if not isinstance(work_packet, dict) or not isinstance(spec_binding, dict) or not isinstance(claims, list):
        raise AuthorityError("WORK_PACKET_UNBOUND", "run_binding")
    if (
        request.get("work_packet_id") != work_packet.get("packet_id")
        or request.get("work_packet_digest") != canonical_digest(work_packet)
        or run.get("work_packet_hash") != request.get("work_packet_digest")
        or request.get("spec_ref") != spec_binding.get("spec_ref")
        or request.get("spec_digest") != spec_binding.get("content_digest")
    ):
        raise AuthorityError("WORK_PACKET_UNBOUND", "binding")
    spec_ref = str(request["spec_ref"])
    if not spec_ref.startswith("repo://"):
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_ref")
    spec_path = _safe_repo_relative(
        paths.integration_root,
        spec_ref.removeprefix("repo://"),
        code="WORK_PACKET_UNBOUND",
    )
    _assert_safe_existing_path(spec_path)
    try:
        actual_spec_digest = f"sha256:{hashlib.sha256(spec_path.read_bytes()).hexdigest()}"
    except OSError as exc:
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_unreadable") from exc
    if actual_spec_digest != request.get("spec_digest"):
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_digest")

    ordinal = request.get("claim_ordinal")
    if not isinstance(ordinal, int) or isinstance(ordinal, bool) or not 0 <= ordinal < len(claims):
        raise AuthorityError("CLAIM_SCOPE_VIOLATION", "claim_ordinal")
    claim = claims[ordinal]
    if not isinstance(claim, dict) or request.get("v1_claim_digest") != canonical_digest(claim):
        raise AuthorityError("IDENTITY_MISMATCH", "v1_claim_digest")
    requested_paths = {
        "paths": sorted(str(item) for item in claim.get("paths", [])),
        "surfaces": sorted(str(item) for item in claim.get("surfaces", [])),
    }
    if request.get("requested_paths_digest") != canonical_digest(requested_paths):
        raise AuthorityError("CLAIM_SCOPE_VIOLATION", "requested_paths_digest")
    scope = work_packet.get("scope")
    write_surfaces = scope.get("write_surfaces") if isinstance(scope, dict) else None
    if not isinstance(write_surfaces, list) or not set(requested_paths["paths"]).issubset(
        {str(item) for item in write_surfaces}
    ):
        raise AuthorityError("CLAIM_SCOPE_VIOLATION", "work_packet_scope")
    affected = claim.get("affected_graph")
    if not isinstance(affected, dict):
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "claim_receipt")
    affected_hash = str(affected.get("receipt_hash") or "")
    if request.get("affected_graph_digest") != f"sha256:{affected_hash}":
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "receipt_hash")
    receipt_ref = str(affected.get("receipt_ref") or "")
    receipt_path = _safe_repo_relative(clone_root, receipt_ref, code="AFFECTED_GRAPH_MISMATCH")
    _assert_safe_existing_path(receipt_path)
    receipt = _read_json_mapping(receipt_path, code="AFFECTED_GRAPH_MISMATCH")
    receipt_preimage = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    if (
        receipt.get("receipt_hash") != affected_hash
        or hashlib.sha256(canonical_json(receipt_preimage).encode("utf-8")).hexdigest() != affected_hash
    ):
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "receipt_digest")
    return {"run_id": run_id, "claim_ordinal": ordinal, "clone_root_digest": request["clone_root_digest"]}


def _verify_production_mutation_request(
    request: Mapping[str, Any],
    paths: AuthorityPaths,
    *,
    phase: Literal["before", "after"],
) -> dict[str, Any]:
    actor = str(request.get("actor_id") or "")
    attempt = str(request.get("delivery_attempt_id") or "")
    if not _IDENTIFIER_RE.fullmatch(actor) or not _IDENTIFIER_RE.fullmatch(attempt):
        raise AuthorityError("IDENTITY_MISMATCH", "actor_attempt")
    clone_root = paths.account_home / "agents" / actor / "attempts" / attempt / "ws"
    identity = _read_json_mapping(
        clone_root / ".git/agent-clone-identity.json",
        code="IDENTITY_MISMATCH",
    )
    provenance = _read_json_mapping(
        clone_root / ".git/agent-clone-provenance.json",
        code="IDENTITY_MISMATCH",
    )
    readiness = _read_json_mapping(
        clone_root / ".git/agent-clone-readiness.json",
        code="IDENTITY_MISMATCH",
    )
    repository = str(provenance.get("repository", {}).get("canonical_repository") or "")
    repository = repository.removeprefix("github.com/")
    identity_checks = {
        "actor_id": identity.get("actor_id"),
        "delivery_attempt_id": identity.get("delivery_attempt_id"),
        "repository": repository,
        "clone_root_digest": canonical_digest({"clone_root": str(clone_root.resolve())}),
        "branch": provenance.get("working_branch"),
        "clone_identity_digest": canonical_digest(identity),
        "manifest_digest": canonical_digest(identity.get("transport", {})),
        "readiness_digest": f"sha256:{readiness.get('receipt_digest')}",
        "frozen_base": identity.get("frozen_root_sha"),
        "head_oid": _read_head_oid(clone_root, identity),
        "clone_identity_schema": identity.get("schema"),
    }
    for field, actual in identity_checks.items():
        if request.get(field) != actual:
            raise AuthorityError("IDENTITY_MISMATCH", field)
    if Path(str(identity.get("canonical_root") or "")).resolve() != clone_root.resolve():
        raise AuthorityError("IDENTITY_MISMATCH", "canonical_root")

    run_id = str(request.get("run_id") or "")
    if not _IDENTIFIER_RE.fullmatch(run_id):
        raise AuthorityError("IDENTITY_MISMATCH", "run_id")
    run_path = clone_root / f".omo/_delivery/agent-workflows/runs/{run_id}.yaml"
    try:
        run_bytes = run_path.read_bytes()
        run = yaml.safe_load(run_bytes) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise AuthorityError("IDENTITY_MISMATCH", "run") from exc
    if not isinstance(run, dict) or run.get("run_id") != run_id or run.get("actor") != actor:
        raise AuthorityError("IDENTITY_MISMATCH", "run")
    work_packet = run.get("work_packet")
    spec_binding = run.get("spec_binding")
    if not isinstance(work_packet, dict) or not isinstance(spec_binding, dict):
        raise AuthorityError("WORK_PACKET_UNBOUND", "run_binding")
    if (
        request.get("bet_id") != run.get("bet_id")
        or request.get("work_packet_id") != work_packet.get("packet_id")
        or request.get("work_packet_digest") != canonical_digest(work_packet)
        or run.get("work_packet_hash") != request.get("work_packet_digest")
        or request.get("spec_ref") != spec_binding.get("spec_ref")
        or request.get("spec_digest") != spec_binding.get("content_digest")
    ):
        raise AuthorityError("WORK_PACKET_UNBOUND", "binding")
    spec_ref = str(request.get("spec_ref") or "")
    if not spec_ref.startswith("repo://"):
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_ref")
    spec_path = _safe_repo_relative(
        paths.integration_root,
        spec_ref.removeprefix("repo://"),
        code="WORK_PACKET_UNBOUND",
    )
    try:
        spec_digest = f"sha256:{hashlib.sha256(spec_path.read_bytes()).hexdigest()}"
    except OSError as exc:
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_unreadable") from exc
    if spec_digest != request.get("spec_digest"):
        raise AuthorityError("WORK_PACKET_UNBOUND", "spec_digest")

    claims = run.get("claims")
    if not isinstance(claims, list) or not all(isinstance(claim, dict) for claim in claims):
        raise AuthorityError("CLAIM_SCOPE_VIOLATION", "claims")
    requested = [
        {
            "paths": sorted(str(item) for item in claim.get("paths", [])),
            "surfaces": sorted(str(item) for item in claim.get("surfaces", [])),
        }
        for claim in claims
    ]
    affected_hashes = []
    for claim in claims:
        affected = claim.get("affected_graph")
        raw_hash = affected.get("receipt_hash") if isinstance(affected, dict) else None
        if not re.fullmatch(r"[0-9a-f]{64}", str(raw_hash or "")):
            raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "receipt_hash")
        affected_hashes.append(f"sha256:{raw_hash}")
    if request.get("requested_paths_digest") != canonical_digest(requested):
        raise AuthorityError("CLAIM_SCOPE_VIOLATION", "requested_paths_digest")
    if request.get("affected_graph_digest") != canonical_digest(affected_hashes):
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "affected_graph_digest")

    lock_dir = clone_root / ".omo/_delivery/agent-workflows/locks"
    raw_locks = run.get("locks")
    if raw_locks is None:
        raw_locks = []
    if not isinstance(raw_locks, list) or not all(isinstance(item, str) and item for item in raw_locks):
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "lock_set")
    selected = {str(item) for item in raw_locks}
    if lock_dir.exists():
        for candidate in lock_dir.glob("*.lock.yaml"):
            try:
                lock = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
            except (OSError, UnicodeError, yaml.YAMLError):
                lock = {}
            if isinstance(lock, dict) and lock.get("run_id") == run_id:
                selected.add(str(candidate))
    lock_records = []
    for raw in sorted(selected):
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = clone_root / candidate
        resolved = candidate.resolve(strict=False)
        try:
            resolved.relative_to(lock_dir.resolve())
        except ValueError as exc:
            raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "lock_path") from exc
        exists = resolved.is_file()
        lock_records.append(
            {
                "path": str(resolved.relative_to(clone_root.resolve())),
                "exists": exists,
                "content_digest": (f"sha256:{hashlib.sha256(resolved.read_bytes()).hexdigest()}" if exists else None),
            }
        )
    expected_run_field = "run_digest" if phase == "before" else "resulting_run_digest"
    expected_lock_field = "lock_set_digest" if phase == "before" else "resulting_lock_set_digest"
    run_digest = f"sha256:{hashlib.sha256(run_bytes).hexdigest()}"
    lock_set_digest = canonical_digest(lock_records)
    if request.get(expected_run_field) != run_digest or request.get(expected_lock_field) != lock_set_digest:
        raise AuthorityError("AFFECTED_GRAPH_MISMATCH", f"{phase}_snapshot")
    return {"run_id": run_id, "run_digest": run_digest, "lock_set_digest": lock_set_digest}


class _AuthorityStore:
    """Canonical SQLite store; direct construction is test-only in Wave A."""

    def __init__(self, connection: sqlite3.Connection, paths: AuthorityPaths, authority_id: str) -> None:
        self._connection = connection
        self.test_paths = paths
        self.authority_id = authority_id

    @classmethod
    def connect_for_test(cls, path: Path, authority_id: str) -> _AuthorityStore:
        return cls._connect_new(path, authority_id, test_only=True)

    @classmethod
    def _connect_new(
        cls,
        path: Path,
        authority_id: str,
        *,
        test_only: bool,
    ) -> _AuthorityStore:
        if test_only:
            if not authority_id.startswith("test:"):
                raise AuthorityError("IDENTITY_MISMATCH", "test_authority_required")
            _uuid4(authority_id.removeprefix("test:"), code="IDENTITY_MISMATCH")
        elif authority_id != _PRODUCTION_AUTHORITY_ID:
            raise AuthorityError("IDENTITY_MISMATCH", "production_authority")
        if not wal_allowed_for_current():
            raise AuthorityError("AUTHORITY_STORE_UNSAFE", "unsafe_sqlite")
        authority_dir = Path(path)
        authority_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        paths = AuthorityPaths(
            account_home=authority_dir.parent,
            integration_root=authority_dir.parent / "Workspace",
            authority_dir=authority_dir,
            store=authority_dir / "store.sqlite3",
            high_water=authority_dir / "high-water.json",
            backups=authority_dir / "backups",
            activation_witness=authority_dir / "activation-witness.json",
        )
        connection = sqlite3.connect(paths.store, timeout=5.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        mode = str(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
        if mode != "wal":
            connection.close()
            raise AuthorityError("AUTHORITY_STORE_UNSAFE", "wal_unavailable")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE authority_meta (
                authority_id TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL,
                operating_mode TEXT NOT NULL CHECK (operating_mode IN ('shadow', 'enforce-r0', 'human-degraded-only')),
                descriptor_digest TEXT,
                last_sequence INTEGER NOT NULL DEFAULT 0,
                last_receipt_digest TEXT,
                last_broker_time TEXT NOT NULL
            );
            CREATE TABLE requests (
                authority_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                request_digest TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (authority_id, request_id)
            );
            CREATE TABLE receipts (
                authority_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                receipt_id TEXT NOT NULL UNIQUE,
                previous_receipt_digest TEXT,
                receipt_json TEXT NOT NULL,
                receipt_digest TEXT NOT NULL UNIQUE,
                recorded_at TEXT NOT NULL,
                PRIMARY KEY (authority_id, sequence)
            );
            CREATE TABLE claims (
                claim_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                authority_claim_version INTEGER NOT NULL,
                authority_lease_epoch INTEGER NOT NULL,
                state TEXT NOT NULL,
                intent_or_fence_id TEXT,
                expires_at TEXT NOT NULL,
                identity_digest TEXT NOT NULL,
                v1_snapshot_digest TEXT NOT NULL
            );
            CREATE TABLE claim_mutation_batches (
                mutation_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                operation TEXT NOT NULL CHECK (operation IN ('claim', 'heartbeat', 'close', 'takeover', 'expire')),
                settlement_request_id TEXT NOT NULL UNIQUE,
                expected_v1_run_digest TEXT NOT NULL,
                expected_v1_lockset_digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('reserved', 'settled', 'unknown')),
                result_v1_run_digest TEXT,
                result_v1_lockset_digest TEXT,
                outcome TEXT,
                authorization_digest TEXT,
                operator_required INTEGER NOT NULL DEFAULT 0 CHECK (operator_required IN (0, 1)),
                operator_required_at TEXT,
                mutation_process_proof_digest TEXT,
                observed_v1_state_digest TEXT,
                created_at TEXT NOT NULL,
                settled_at TEXT
            );
            CREATE TABLE claim_mutation_members (
                mutation_id TEXT NOT NULL REFERENCES claim_mutation_batches(mutation_id),
                claim_id TEXT NOT NULL REFERENCES claims(claim_id),
                expected_authority_claim_version INTEGER NOT NULL,
                expected_authority_lease_epoch INTEGER NOT NULL,
                PRIMARY KEY (mutation_id, claim_id)
            );
            CREATE UNIQUE INDEX one_unresolved_batch_per_run
                ON claim_mutation_batches(run_id)
                WHERE state IN ('reserved', 'unknown');
            CREATE TABLE legacy_fences (
                fence_id TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL,
                claim_id TEXT NOT NULL REFERENCES claims(claim_id),
                request_digest TEXT NOT NULL,
                settlement_request_id TEXT UNIQUE,
                state TEXT NOT NULL,
                expected_remote_oid TEXT NOT NULL,
                remote_ref_digest TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                outcome TEXT,
                observed_remote_oid TEXT,
                settlement_digest TEXT,
                operator_required INTEGER NOT NULL DEFAULT 0 CHECK (operator_required IN (0, 1)),
                operator_required_at TEXT,
                operator_authorization_digest TEXT,
                effect_process_proof_digest TEXT
            );
            CREATE TABLE activation (
                authority_id TEXT PRIMARY KEY,
                operating_mode TEXT NOT NULL CHECK (operating_mode = 'shadow'),
                activation_state TEXT NOT NULL CHECK (activation_state IN ('unactivated', 'shadow-active')),
                descriptor_digest TEXT NOT NULL,
                activated_at TEXT NOT NULL,
                activation_receipt_digest TEXT NOT NULL
            );
            CREATE INDEX receipts_request_order ON receipts(authority_id, recorded_at);
            CREATE INDEX fences_state ON legacy_fences(epoch, state);
            PRAGMA user_version = 1;
            """
        )
        connection.execute(
            "INSERT INTO authority_meta(authority_id, epoch, operating_mode, descriptor_digest, last_sequence, last_receipt_digest, last_broker_time) VALUES(?, 0, 'shadow', NULL, 0, NULL, ?)",
            (authority_id, _utc_now()),
        )
        os.chmod(paths.store, 0o600)
        _atomic_write_json(
            paths.high_water,
            _high_water_payload(
                authority_id,
                sequence=0,
                receipt_digest=None,
                descriptor_digest=None,
            ),
        )
        _atomic_write_json(
            paths.activation_witness,
            _witness_payload(
                authority_id,
                state="unactivated",
                sequence=0,
                descriptor_digest=None,
                activation_receipt_digest=None,
            ),
        )
        return cls(connection, paths, authority_id)

    @classmethod
    def _connect_existing(cls, paths: AuthorityPaths, authority_id: str) -> _AuthorityStore:
        if authority_id != _PRODUCTION_AUTHORITY_ID:
            raise AuthorityError("IDENTITY_MISMATCH", "production_authority")
        if not wal_allowed_for_current():
            raise AuthorityError("AUTHORITY_STORE_UNSAFE", "unsafe_sqlite")
        for path in (paths.authority_dir, paths.store, paths.high_water, paths.activation_witness):
            _assert_safe_existing_path(path)
        if not paths.store.is_file():
            raise AuthorityError("AUTHORITY_UNAVAILABLE", "store_missing")
        connection = sqlite3.connect(paths.store, timeout=5.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            if str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "wal":
                raise AuthorityError("AUTHORITY_STORE_UNSAFE", "wal_unavailable")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA foreign_keys = ON")
            if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 1:
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "user_version")
            if str(connection.execute("PRAGMA integrity_check").fetchone()[0]).lower() != "ok":
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "integrity_check")
            store = cls(connection, paths, authority_id)
            activation_state, _, _ = store._activation_meta()
            witness = _read_json_mapping(
                paths.activation_witness,
                code="AUTHORITY_ACTIVATION_WITNESS_INVALID",
            )
            if activation_state == "shadow-active" and witness.get("state") == "prepared":
                store.reconcile_activation_witness()
            else:
                store.verify_high_water()
                if activation_state == "shadow-active":
                    store.reconcile_activation_witness()
                elif activation_state == "unactivated":
                    _broker_unavailable_policy(paths)
                else:
                    raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "activation_state")
            return store
        except BaseException:
            connection.close()
            raise

    def scalar(self, statement: str) -> Any:
        row = self._connection.execute(statement).fetchone()
        return row[0] if row is not None else None

    def _database_tip(self) -> tuple[int, str | None]:
        row = self._connection.execute(
            "SELECT last_sequence, last_receipt_digest FROM authority_meta WHERE authority_id=?",
            (self.authority_id,),
        ).fetchone()
        if row is None:
            raise AuthorityError("AUTHORITY_STORE_CORRUPT", "authority_meta")
        digest = str(row["last_receipt_digest"]) if row["last_receipt_digest"] is not None else None
        return int(row["last_sequence"]), digest

    def _activation_meta(self) -> tuple[str, int, str | None]:
        meta = self._connection.execute(
            "SELECT epoch, descriptor_digest FROM authority_meta WHERE authority_id=?",
            (self.authority_id,),
        ).fetchone()
        if meta is None:
            raise AuthorityError("AUTHORITY_STORE_CORRUPT", "authority_meta")
        activation = self._connection.execute(
            "SELECT activation_state, descriptor_digest FROM activation WHERE authority_id=?",
            (self.authority_id,),
        ).fetchone()
        state = str(activation["activation_state"]) if activation is not None else "unactivated"
        descriptor_value = activation["descriptor_digest"] if activation is not None else meta["descriptor_digest"]
        descriptor = str(descriptor_value) if descriptor_value else None
        return state, int(meta["epoch"]), descriptor

    def _claim_id_for_request(self, request: Mapping[str, Any]) -> str:
        return canonical_digest(
            {
                "authority_id": self.authority_id,
                "run_id": str(request.get("run_id") or ""),
                "claim_ordinal": int(request.get("claim_ordinal") or 0),
                "v1_claim_digest": str(request.get("v1_claim_digest") or ""),
            }
        )

    def _claim_rows(self, run_id: str) -> list[sqlite3.Row]:
        return list(
            self._connection.execute(
                "SELECT * FROM claims WHERE run_id=? ORDER BY claim_id",
                (run_id,),
            )
        )

    def _mutation_members(self, mutation_id: str) -> list[dict[str, Any]]:
        return [
            {
                "claim_id": str(row["claim_id"]),
                "claim_version": int(row["expected_authority_claim_version"]),
                "lease_epoch": int(row["expected_authority_lease_epoch"]),
            }
            for row in self._connection.execute(
                "SELECT claim_id, expected_authority_claim_version, expected_authority_lease_epoch FROM claim_mutation_members WHERE mutation_id=? ORDER BY claim_id",
                (mutation_id,),
            )
        ]

    @staticmethod
    def _member_from_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "claim_id": str(row["claim_id"]),
            "claim_version": int(row["authority_claim_version"]),
            "lease_epoch": int(row["authority_lease_epoch"]),
        }

    def _fence_issue_receipt(self, fence_id: str) -> dict[str, Any]:
        for row in self._connection.execute(
            "SELECT receipt_json FROM receipts WHERE authority_id=? ORDER BY sequence",
            (self.authority_id,),
        ):
            receipt = json.loads(row["receipt_json"])
            if receipt.get("operation") == "issue-legacy-fence" and receipt.get("fence_id") == fence_id:
                return receipt
        raise AuthorityError("AUTHORITY_STORE_CORRUPT", "fence_issue_receipt")

    def _receipt_by_digest(self, receipt_digest: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT receipt_json FROM receipts WHERE authority_id=? AND receipt_digest=?",
            (self.authority_id, receipt_digest),
        ).fetchone()
        if row is None:
            return None
        receipt = json.loads(row["receipt_json"])
        if not isinstance(receipt, dict):
            raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_json")
        return receipt

    def _fence_unknown_settlement_receipt(self, fence_id: str) -> dict[str, Any]:
        for row in self._connection.execute(
            "SELECT receipt_json FROM receipts WHERE authority_id=? ORDER BY sequence DESC",
            (self.authority_id,),
        ):
            receipt = json.loads(row["receipt_json"])
            if (
                receipt.get("operation") == "settle-legacy-publication"
                and receipt.get("fence_id") == fence_id
                and receipt.get("state") == "unknown"
            ):
                return receipt
        raise AuthorityError("AUTHORITY_STORE_CORRUPT", "fence_unknown_receipt")

    def _idempotent_response(self, request_id: str, request_digest: str) -> dict[str, Any] | None:
        existing = self._connection.execute(
            "SELECT request_digest, response_json FROM requests WHERE authority_id=? AND request_id=?",
            (self.authority_id, request_id),
        ).fetchone()
        if existing is None:
            return None
        if existing["request_digest"] != request_digest:
            raise AuthorityError("REQUEST_ID_REUSE_MISMATCH")
        return json.loads(existing["response_json"])

    def _assert_broker_clock(self) -> str:
        row = self._connection.execute(
            "SELECT last_broker_time FROM authority_meta WHERE authority_id=?",
            (self.authority_id,),
        ).fetchone()
        now = _clock_now()
        if row is not None:
            try:
                previous = datetime.fromisoformat(str(row["last_broker_time"]).replace("Z", "+00:00"))
            except ValueError as exc:
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "last_broker_time") from exc
            if (previous - now).total_seconds() > 30:
                raise AuthorityError("AUTHORITY_CLOCK_ROLLBACK")
        return now.isoformat().replace("+00:00", "Z")

    def _new_receipt(self, fields: Mapping[str, Any]) -> dict[str, Any]:
        self.reconcile_crash_tail()
        sequence, previous_digest = self._database_tip()
        issued_at = self._assert_broker_clock()
        receipt: dict[str, Any] = {
            "schema": "claims-authority-receipt/v2",
            "authority_id": self.authority_id,
            "security_level": "R0_COOPERATIVE",
            "publishable": False,
            "sequence": sequence + 1,
            "previous_receipt_digest": previous_digest,
            "issued_at": issued_at,
            **fields,
        }
        receipt["receipt_id"] = canonical_digest(
            {
                "authority_id": self.authority_id,
                "sequence": receipt["sequence"],
                "request_id": receipt.get("request_id"),
                "operation": receipt.get("operation"),
            }
        )
        receipt["receipt_digest"] = canonical_digest(receipt)
        return receipt

    def _insert_receipt_and_idempotency(
        self,
        receipt: Mapping[str, Any],
        *,
        request_id: str,
        request_digest: str,
    ) -> None:
        response_json = canonical_json(receipt)
        self._connection.execute(
            "INSERT INTO receipts(authority_id, sequence, receipt_id, previous_receipt_digest, receipt_json, receipt_digest, recorded_at) VALUES(?,?,?,?,?,?,?)",
            (
                self.authority_id,
                receipt["sequence"],
                receipt["receipt_id"],
                receipt["previous_receipt_digest"],
                response_json,
                receipt["receipt_digest"],
                receipt["issued_at"],
            ),
        )
        self._connection.execute(
            "INSERT INTO requests(authority_id, request_id, request_digest, response_json, created_at) VALUES(?,?,?,?,?)",
            (self.authority_id, request_id, request_digest, response_json, receipt["issued_at"]),
        )
        self._connection.execute(
            "UPDATE authority_meta SET last_sequence=?, last_receipt_digest=?, last_broker_time=? WHERE authority_id=?",
            (
                receipt["sequence"],
                receipt["receipt_digest"],
                receipt["issued_at"],
                self.authority_id,
            ),
        )

    def _settle_high_water_for_receipt(self, receipt: Mapping[str, Any]) -> None:
        _, _, descriptor_digest = self._activation_meta()
        self._write_high_water(
            sequence=int(receipt["sequence"]),
            receipt_digest=str(receipt["receipt_digest"]),
            descriptor_digest=descriptor_digest,
        )

    def _write_high_water(self, *, sequence: int, receipt_digest: str | None, descriptor_digest: str | None) -> None:
        _atomic_write_json(
            self.test_paths.high_water,
            _high_water_payload(
                self.authority_id,
                sequence=sequence,
                receipt_digest=receipt_digest,
                descriptor_digest=descriptor_digest,
            ),
        )

    def _verify_receipt_chain(self) -> tuple[int, str | None]:
        expected_sequence = 1
        previous_digest: str | None = None
        for row in self._connection.execute(
            "SELECT authority_id, sequence, receipt_id, previous_receipt_digest, receipt_json, receipt_digest, recorded_at FROM receipts WHERE authority_id=? ORDER BY sequence",
            (self.authority_id,),
        ):
            if int(row["sequence"]) != expected_sequence:
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_sequence")
            if row["authority_id"] != self.authority_id or row["previous_receipt_digest"] != previous_digest:
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_chain")
            try:
                receipt = json.loads(row["receipt_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_json") from exc
            if not isinstance(receipt, dict):
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_json")
            digest_preimage = {key: value for key, value in receipt.items() if key != "receipt_digest"}
            computed_digest = canonical_digest(digest_preimage)
            if (
                receipt.get("authority_id") != self.authority_id
                or receipt.get("sequence") != expected_sequence
                or receipt.get("receipt_id") != row["receipt_id"]
                or receipt.get("previous_receipt_digest") != previous_digest
                or receipt.get("issued_at") != row["recorded_at"]
                or receipt.get("receipt_digest") != row["receipt_digest"]
                or row["receipt_digest"] != computed_digest
                or canonical_json(receipt) != row["receipt_json"]
            ):
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_digest")
            previous_digest = str(row["receipt_digest"])
            expected_sequence += 1
        sequence = expected_sequence - 1
        meta_sequence, meta_digest = self._database_tip()
        if sequence != meta_sequence or previous_digest != meta_digest:
            raise AuthorityError("AUTHORITY_STORE_CORRUPT", "authority_meta_tip")
        return sequence, previous_digest

    def verify_high_water(self) -> dict[str, Any]:
        self._verify_receipt_chain()
        high_water = _read_json_mapping(
            self.test_paths.high_water,
            code="AUTHORITY_HIGHWATER_GAP",
        )
        _, _, descriptor_digest = self._activation_meta()
        if (
            high_water.get("schema") != "claims-authority-high-water/v1"
            or high_water.get("authority_id") != self.authority_id
            or high_water.get("digest") != canonical_digest(high_water)
            or high_water.get("descriptor_digest") != descriptor_digest
        ):
            raise AuthorityError("AUTHORITY_HIGHWATER_ROLLBACK", "identity_or_digest")
        database_sequence, database_digest = self._database_tip()
        persisted_sequence = high_water.get("sequence")
        if not isinstance(persisted_sequence, int) or isinstance(persisted_sequence, bool):
            raise AuthorityError("AUTHORITY_HIGHWATER_GAP", "sequence")
        if persisted_sequence > database_sequence:
            raise AuthorityError("AUTHORITY_HIGHWATER_ROLLBACK", "high_water_ahead")
        if database_sequence - persisted_sequence > 1:
            raise AuthorityError("AUTHORITY_HIGHWATER_GAP", "multiple_tails")
        if database_sequence == persisted_sequence and high_water.get("receipt_digest") != database_digest:
            raise AuthorityError("AUTHORITY_HIGHWATER_ROLLBACK", "digest")
        return {
            "database_sequence": database_sequence,
            "high_water_sequence": persisted_sequence,
            "receipt_digest": database_digest,
        }

    def reconcile_crash_tail(self) -> dict[str, Any]:
        report = self.verify_high_water()
        database_sequence = int(report["database_sequence"])
        high_water_sequence = int(report["high_water_sequence"])
        gap = database_sequence - high_water_sequence
        if gap == 0:
            return {"reconciled": False, **report}
        if gap != 1:
            raise AuthorityError("AUTHORITY_HIGHWATER_GAP", "multiple_tails")
        high_water = _read_json_mapping(
            self.test_paths.high_water,
            code="AUTHORITY_HIGHWATER_GAP",
        )
        tail = self._connection.execute(
            "SELECT receipt_digest, previous_receipt_digest FROM receipts WHERE sequence=?",
            (database_sequence,),
        ).fetchone()
        if tail is None or tail["previous_receipt_digest"] != high_water.get("receipt_digest"):
            raise AuthorityError("AUTHORITY_HIGHWATER_GAP", "tail_chain")
        _, _, descriptor_digest = self._activation_meta()
        self._write_high_water(
            sequence=database_sequence,
            receipt_digest=str(tail["receipt_digest"]),
            descriptor_digest=descriptor_digest,
        )
        return {
            "reconciled": True,
            "database_sequence": database_sequence,
            "high_water_sequence": database_sequence,
            "receipt_digest": str(tail["receipt_digest"]),
        }

    def activate_shadow(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "operation",
                "request_id",
                "authority_id",
                "expected_authority_epoch",
                "expected_state",
                "descriptor",
            },
            detail="activate_shadow",
        )
        request_id = _uuid4(request.get("request_id"))
        if request.get("schema") != "claim-mutation-envelope/v2" or request.get("operation") != "activate-shadow":
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "activate_shadow")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        descriptor = request.get("descriptor")
        if not isinstance(descriptor, Mapping):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "descriptor")
        descriptor_payload = dict(descriptor)
        production_activation = self.authority_id == _PRODUCTION_AUTHORITY_ID
        base_descriptor_fields = {
            "schema",
            "authority_id",
            "security_level",
            "operating_mode",
            "repository",
            "broker_transport",
            "store_identity",
            "critical_dependency_closure_digest",
            "accepted_clone_identity_schemas",
            "v1_compatibility",
            "digest",
        }
        expected_fields = set(base_descriptor_fields)
        if production_activation:
            expected_fields |= {
                "operator_authorization_verifier_digest",
                "stopped_process_verifier_digest",
            }
        if set(descriptor_payload) != expected_fields:
            raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "fields")
        descriptor_digest = descriptor_payload.get("digest")
        if not isinstance(descriptor_digest, str) or descriptor_digest != canonical_digest(descriptor_payload):
            raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "digest")
        if descriptor_payload.get("authority_id") != self.authority_id:
            raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "authority_id")
        if (
            descriptor_payload.get("schema") != "claims-authority-descriptor/v2"
            or descriptor_payload.get("security_level") != "cooperative-r0"
            or descriptor_payload.get("operating_mode") != "shadow"
            or descriptor_payload.get("repository") != _CANONICAL_REPOSITORY
            or descriptor_payload.get("broker_transport") != "stdio"
            or not _IDENTIFIER_RE.fullmatch(str(descriptor_payload.get("store_identity") or ""))
            or not _SHA256_RE.fullmatch(str(descriptor_payload.get("critical_dependency_closure_digest") or ""))
            or descriptor_payload.get("accepted_clone_identity_schemas") != ["agent-clone-identity/v2"]
            or descriptor_payload.get("v1_compatibility") != "legacy-effective-shadow"
        ):
            raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "mode")
        if production_activation:
            from . import claims_verifiers

            expected_operator = claims_verifiers.operator_authorization_verifier_digest()
            expected_stopped = claims_verifiers.stopped_process_verifier_digest()
            expected_closure = claims_verifiers.production_verifier_closure_digest()
            operator_digest = descriptor_payload.get("operator_authorization_verifier_digest")
            stopped_digest = descriptor_payload.get("stopped_process_verifier_digest")
            closure_digest = descriptor_payload.get("critical_dependency_closure_digest")
            if operator_digest != expected_operator:
                raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "unbound_verifier")
            if stopped_digest != expected_stopped:
                raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "unbound_verifier")
            if closure_digest != expected_closure:
                raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "verifier_closure")
        request_digest = canonical_digest(request)
        existing = self._idempotent_response(request_id, request_digest)
        if existing is not None:
            self.reconcile_activation_witness()
            return existing

        activation_state, authority_epoch, current_descriptor = self._activation_meta()
        if (
            activation_state != request.get("expected_state")
            or authority_epoch != request.get("expected_authority_epoch")
            or current_descriptor is not None
        ):
            raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "activation_cas")
        self.verify_high_water()
        sequence, previous_digest = self._database_tip()
        next_sequence = sequence + 1
        prepared = _witness_payload(
            self.authority_id,
            state="prepared",
            sequence=next_sequence,
            descriptor_digest=descriptor_digest,
            activation_receipt_digest=None,
            request_digest=request_digest,
        )
        _atomic_write_json(self.test_paths.activation_witness, prepared)
        _activation_checkpoint("prepared")

        receipt: dict[str, Any] = {
            "schema": "claims-authority-receipt/v2",
            "authority_id": self.authority_id,
            "security_level": "R0_COOPERATIVE",
            "publishable": False,
            "operation": "activate-shadow",
            "request_id": request_id,
            "request_digest": request_digest,
            "sequence": next_sequence,
            "previous_receipt_digest": previous_digest,
            "authority_epoch": authority_epoch + 1,
            "descriptor_digest": descriptor_digest,
            "issued_at": self._assert_broker_clock(),
        }
        receipt["receipt_id"] = canonical_digest(
            {
                "authority_id": self.authority_id,
                "sequence": next_sequence,
                "request_id": request_id,
                "operation": "activate-shadow",
            }
        )
        receipt["receipt_digest"] = canonical_digest(receipt)
        response_json = canonical_json(receipt)
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            state_now, epoch_now, descriptor_now = self._activation_meta()
            if state_now != activation_state or epoch_now != authority_epoch or descriptor_now is not None:
                raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "activation_cas")
            connection.execute(
                "INSERT INTO receipts(authority_id, sequence, receipt_id, previous_receipt_digest, receipt_json, receipt_digest, recorded_at) VALUES(?,?,?,?,?,?,?)",
                (
                    self.authority_id,
                    next_sequence,
                    receipt["receipt_id"],
                    previous_digest,
                    response_json,
                    receipt["receipt_digest"],
                    receipt["issued_at"],
                ),
            )
            connection.execute(
                "INSERT INTO requests(authority_id, request_id, request_digest, response_json, created_at) VALUES(?,?,?,?,?)",
                (self.authority_id, request_id, request_digest, response_json, receipt["issued_at"]),
            )
            connection.execute(
                "UPDATE authority_meta SET epoch=?, descriptor_digest=?, last_sequence=?, last_receipt_digest=?, last_broker_time=? WHERE authority_id=?",
                (
                    authority_epoch + 1,
                    descriptor_digest,
                    next_sequence,
                    receipt["receipt_digest"],
                    receipt["issued_at"],
                    self.authority_id,
                ),
            )
            connection.execute(
                "INSERT INTO activation(authority_id, operating_mode, activation_state, descriptor_digest, activated_at, activation_receipt_digest) VALUES(?, 'shadow', 'shadow-active', ?, ?, ?)",
                (
                    self.authority_id,
                    descriptor_digest,
                    receipt["issued_at"],
                    receipt["receipt_digest"],
                ),
            )
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        _activation_checkpoint("db_commit")

        self._write_high_water(
            sequence=next_sequence,
            receipt_digest=str(receipt["receipt_digest"]),
            descriptor_digest=str(descriptor_digest),
        )
        _activation_checkpoint("high_water")
        active = _witness_payload(
            self.authority_id,
            state="shadow-active",
            sequence=next_sequence,
            descriptor_digest=str(descriptor_digest),
            activation_receipt_digest=str(receipt["receipt_digest"]),
            request_digest=request_digest,
        )
        _atomic_write_json(self.test_paths.activation_witness, active)
        _activation_checkpoint("active")
        self._ensure_daily_backup()
        return receipt

    def reconcile_activation_witness(self) -> dict[str, Any]:
        witness = _read_json_mapping(
            self.test_paths.activation_witness,
            code="AUTHORITY_ACTIVATION_WITNESS_INVALID",
        )
        if witness.get("digest") != canonical_digest(witness):
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "digest")
        state, epoch, descriptor_digest = self._activation_meta()
        sequence, receipt_digest = self._database_tip()
        if state != "shadow-active" or epoch < 1 or descriptor_digest is None or receipt_digest is None:
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "activation_uncommitted")
        if witness.get("state") == "shadow-active":
            activation = self._connection.execute(
                "SELECT activation_receipt_digest FROM activation WHERE authority_id=?",
                (self.authority_id,),
            ).fetchone()
            activation_receipt_digest = str(activation["activation_receipt_digest"]) if activation is not None else ""
            activation_receipt = self._connection.execute(
                "SELECT sequence, receipt_json FROM receipts WHERE authority_id=? AND receipt_digest=?",
                (self.authority_id, activation_receipt_digest),
            ).fetchone()
            if activation_receipt is None:
                raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "activation_receipt_missing")
            try:
                activation_payload = json.loads(activation_receipt["receipt_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "activation_receipt_malformed") from exc
            if (
                witness.get("sequence") != int(activation_receipt["sequence"])
                or witness.get("descriptor_digest") != descriptor_digest
                or witness.get("activation_receipt_digest") != activation_receipt_digest
                or activation_payload.get("operation") != "activate-shadow"
                or activation_payload.get("descriptor_digest") != descriptor_digest
                or activation_payload.get("request_digest") != witness.get("request_digest")
            ):
                raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "active_mismatch")
            return witness
        if witness.get("state") != "prepared" or witness.get("sequence") != sequence:
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "prepared_mismatch")
        row = self._connection.execute(
            "SELECT receipt_json FROM receipts WHERE authority_id=? AND sequence=? AND receipt_digest=?",
            (self.authority_id, sequence, receipt_digest),
        ).fetchone()
        if row is None:
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "receipt_missing")
        receipt = json.loads(row["receipt_json"])
        if (
            receipt.get("operation") != "activate-shadow"
            or receipt.get("request_digest") != witness.get("request_digest")
            or receipt.get("descriptor_digest") != descriptor_digest
        ):
            raise AuthorityError("AUTHORITY_ACTIVATION_WITNESS_INVALID", "receipt_mismatch")
        self._write_high_water(
            sequence=sequence,
            receipt_digest=receipt_digest,
            descriptor_digest=descriptor_digest,
        )
        active = _witness_payload(
            self.authority_id,
            state="shadow-active",
            sequence=sequence,
            descriptor_digest=descriptor_digest,
            activation_receipt_digest=receipt_digest,
            request_digest=str(witness.get("request_digest") or ""),
        )
        _atomic_write_json(self.test_paths.activation_witness, active)
        return active

    def backup_now(self, *, timestamp: str) -> dict[str, str]:
        self.test_paths.backups.mkdir(mode=0o700, parents=True, exist_ok=True)
        stamp = "".join(character for character in timestamp if character.isdigit())[:14] + "Z"
        if len(stamp) != 15:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "backup_timestamp")
        database_path = self.test_paths.backups / f"backup-{stamp}.sqlite3"
        manifest_path = self.test_paths.backups / f"backup-{stamp}.manifest.json"
        if database_path.exists() or manifest_path.exists():
            raise AuthorityError("REQUEST_ID_REUSE_MISMATCH", "backup_timestamp")
        destination = sqlite3.connect(database_path)
        try:
            self._connection.backup(destination)
        finally:
            destination.close()
        os.chmod(database_path, 0o600)
        sequence, receipt_digest = self._database_tip()
        _, _, descriptor_digest = self._activation_meta()
        if descriptor_digest is None and self.test_paths.activation_witness.is_file():
            witness = _read_json_mapping(
                self.test_paths.activation_witness,
                code="AUTHORITY_STORE_CORRUPT",
            )
            if witness.get("state") == "shadow-active":
                descriptor_digest = str(witness["descriptor_digest"])
        if descriptor_digest is None:
            activation_row = self._connection.execute(
                """
                SELECT receipt_json FROM receipts
                WHERE authority_id=? AND sequence=1
                ORDER BY sequence DESC LIMIT 1
                """,
                (self.authority_id,),
            ).fetchone()
            if activation_row is not None:
                activation_receipt = json.loads(activation_row["receipt_json"])
                if activation_receipt.get("operation") == "activate-shadow" and isinstance(
                    activation_receipt.get("descriptor_digest"), str
                ):
                    descriptor_digest = str(activation_receipt["descriptor_digest"])
        manifest: dict[str, Any] = {
            "schema": "claims-authority-backup-manifest/v1",
            "authority_id": self.authority_id,
            "created_at": timestamp,
            "database": database_path.name,
            "store_digest": f"sha256:{hashlib.sha256(database_path.read_bytes()).hexdigest()}",
            "sequence": sequence,
            "previous_receipt_digest": receipt_digest,
            "descriptor_digest": descriptor_digest,
        }
        manifest["digest"] = canonical_digest(manifest)
        _atomic_write_json(manifest_path, manifest)

        valid_pairs: list[tuple[Path, Path]] = []
        for candidate_manifest in sorted(self.test_paths.backups.glob("backup-*.manifest.json"), reverse=True):
            try:
                candidate = _read_json_mapping(candidate_manifest, code="AUTHORITY_STORE_CORRUPT")
                candidate_database = self.test_paths.backups / str(candidate["database"])
                expected_digest = f"sha256:{hashlib.sha256(candidate_database.read_bytes()).hexdigest()}"
                if (
                    candidate.get("digest") != canonical_digest(candidate)
                    or candidate.get("store_digest") != expected_digest
                ):
                    continue
            except (AuthorityError, KeyError, OSError):
                continue
            valid_pairs.append((candidate_manifest, candidate_database))
        for old_manifest, old_database in valid_pairs[3:]:
            old_manifest.unlink()
            old_database.unlink()
        return {"database": str(database_path), "manifest": str(manifest_path)}

    def _ensure_daily_backup(self) -> None:
        self.reconcile_crash_tail()
        self._assert_broker_clock()
        today = _clock_now().date().isoformat()
        if self.test_paths.backups.exists():
            for manifest_path in self.test_paths.backups.glob("backup-*.manifest.json"):
                try:
                    manifest = _read_json_mapping(manifest_path, code="AUTHORITY_STORE_CORRUPT")
                    database = self.test_paths.backups / str(manifest["database"])
                    expected_store_digest = f"sha256:{hashlib.sha256(database.read_bytes()).hexdigest()}"
                    if (
                        manifest.get("digest") == canonical_digest(manifest)
                        and manifest.get("store_digest") == expected_store_digest
                        and str(manifest.get("created_at") or "")[:10] == today
                    ):
                        return
                except (AuthorityError, KeyError, OSError):
                    continue
        self.backup_now(timestamp=_utc_now())

    def authority_status(self, *, now: datetime | None = None) -> dict[str, Any]:
        observed_now = now or datetime.now(UTC)
        sequence, receipt_digest = self._database_tip()
        row = self._connection.execute(
            "SELECT receipt_json FROM receipts WHERE authority_id=? ORDER BY sequence DESC LIMIT 1",
            (self.authority_id,),
        ).fetchone()
        observed_at: str | None = None
        fresh = False
        if row is not None:
            payload = json.loads(row["receipt_json"])
            observed_at = payload.get("issued_at")
            if isinstance(observed_at, str):
                try:
                    issued = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
                    age = (observed_now - issued).total_seconds()
                    fresh = 0 <= age <= 120
                except ValueError:
                    fresh = False
        activation_state, authority_epoch, descriptor_digest = self._activation_meta()
        return {
            "schema": "claims-authority-status/v2",
            "authority_id": self.authority_id,
            "security_level": "R0_COOPERATIVE",
            "activation_state": activation_state,
            "authority_epoch": authority_epoch,
            "descriptor_digest": descriptor_digest,
            "sequence": sequence,
            "last_receipt_digest": receipt_digest,
            "observed_at": observed_at,
            "fresh": fresh,
            "effective_claim_authority": "v1",
            "instruction_capable": False,
        }

    def observe_claim(self, request: Mapping[str, Any]) -> dict[str, Any]:
        request_id = _uuid4(request.get("request_id"))
        if request.get("schema") != "claim-mutation-envelope/v2" or request.get("operation") != "observe-claim":
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "observe_claim")
        _validate_observe_request(request)
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing

            run_id = str(request.get("run_id") or "")
            ordinal = int(request.get("claim_ordinal") or 0)
            claim_digest = str(request.get("v1_claim_digest") or "")
            run_snapshot_digest = str(request.get("v1_run_digest") or "")
            claim_id = self._claim_id_for_request(request)
            claim = connection.execute(
                "SELECT * FROM claims WHERE claim_id=?",
                (claim_id,),
            ).fetchone()
            identity_digest = canonical_digest(
                {
                    "actor_id": request["actor_id"],
                    "delivery_attempt_id": request["delivery_attempt_id"],
                    "repository": request["repository"],
                    "clone_root_digest": request["clone_root_digest"],
                    "branch": request["branch"],
                    "clone_identity_digest": request["clone_identity_digest"],
                    "manifest_digest": request["manifest_digest"],
                    "readiness_digest": request["readiness_digest"],
                    "frozen_base": request["frozen_base"],
                    "bet_id": request["bet_id"],
                    "work_packet_id": request["work_packet_id"],
                    "work_packet_digest": request["work_packet_digest"],
                    "spec_ref": request["spec_ref"],
                    "spec_digest": request["spec_digest"],
                }
            )
            if claim is None:
                existing_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM claims WHERE run_id=?",
                        (run_id,),
                    ).fetchone()[0]
                )
                if ordinal != existing_count:
                    raise AuthorityError("IDENTITY_MISMATCH", "claim_history")
                claim_version = 1
                lease_epoch = 1
                connection.execute(
                    "INSERT INTO claims(claim_id, run_id, authority_claim_version, authority_lease_epoch, state, intent_or_fence_id, expires_at, identity_digest, v1_snapshot_digest) VALUES(?,?,?,?, 'active', NULL, ?, ?, ?)",
                    (
                        claim_id,
                        run_id,
                        claim_version,
                        lease_epoch,
                        _lease_expiry(),
                        identity_digest,
                        run_snapshot_digest,
                    ),
                )
            else:
                if claim["run_id"] != run_id or claim["identity_digest"] != identity_digest:
                    raise AuthorityError("IDENTITY_MISMATCH", "claim_history")
                connection.execute(
                    "UPDATE claims SET v1_snapshot_digest=? WHERE claim_id=?",
                    (run_snapshot_digest, claim_id),
                )
                claim_version = int(claim["authority_claim_version"])
                lease_epoch = int(claim["authority_lease_epoch"])
            v1_payload = request["v1_decision"]
            v1 = V1Decision(
                str(v1_payload["decision"]),  # type: ignore[arg-type]
                str(v1_payload["code"]),
            )
            if v1.decision == "deny" and v1.code == "claims_authority_mismatch":
                shadow = ShadowDecision("would_allow", "valid_managed_clone")
            elif v1.decision == "allow":
                shadow = ShadowDecision("would_allow", "v1_equivalent")
            else:
                shadow = ShadowDecision("would_deny", v1.code)
            comparison = compare_decisions(v1, shadow)
            receipt = self._new_receipt(
                {
                    "operation": "observe-claim",
                    "request_id": request_id,
                    "run_id": run_id,
                    "claim_id": claim_id,
                    "claim_version": claim_version,
                    "lease_epoch": lease_epoch,
                    "actor_id": str(request.get("actor_id") or ""),
                    "delivery_attempt_id": str(request.get("delivery_attempt_id") or ""),
                    "repository": str(request.get("repository") or ""),
                    "frozen_base": str(request.get("frozen_base") or ""),
                    "head_oid": str(request.get("head_oid") or ""),
                    "work_packet_digest": str(request.get("work_packet_digest") or ""),
                    "affected_graph_digest": str(request.get("affected_graph_digest") or ""),
                    "requested_paths_digest": str(request.get("requested_paths_digest") or ""),
                    "publication_scope": (
                        dict(request["publication_scope"])
                        if isinstance(request.get("publication_scope"), Mapping)
                        else None
                    ),
                    "v1_claim_digest": claim_digest,
                    "v1_run_digest": run_snapshot_digest,
                    "v1_lock_set_digest": str(request.get("v1_lock_set_digest") or ""),
                    "v1_snapshot_digest": run_snapshot_digest,
                    "comparison": comparison,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def begin_claim_mutation(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "run_id",
                "run_digest",
                "lock_set_digest",
                "members",
                "mutation_process_identity_digest",
            }
            | _ENVELOPE_IDENTITY_FIELDS,
            detail="begin_claim_mutation",
        )
        request_id = _uuid4(request.get("request_id"))
        operation = str(request.get("operation") or "")
        if request.get("schema") != "claim-mutation-envelope/v2" or operation not in {
            "claim",
            "heartbeat",
            "close",
            "takeover",
            "expire",
        }:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "claim_mutation")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        run_id = str(request.get("run_id") or "")
        if not run_id:
            raise AuthorityError("IDENTITY_MISMATCH", "run_id")
        pre_run_digest = _require_digest(
            request.get("run_digest"),
            code="AFFECTED_GRAPH_MISMATCH",
            detail="run_digest",
        )
        pre_lock_digest = _require_digest(
            request.get("lock_set_digest"),
            code="AFFECTED_GRAPH_MISMATCH",
            detail="lock_set_digest",
        )
        supplied_members = request.get("members")
        if not isinstance(supplied_members, list):
            raise AuthorityError("CLAIM_SCOPE_VIOLATION", "members")
        mutation_process_identity = _require_digest(
            request.get("mutation_process_identity_digest"),
            code="REQUEST_SCHEMA_INVALID",
            detail="mutation_process_identity_digest",
        )
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            unresolved = connection.execute(
                "SELECT mutation_id FROM claim_mutation_batches WHERE run_id=? AND state IN ('reserved','unknown')",
                (run_id,),
            ).fetchone()
            if unresolved is not None:
                raise AuthorityError("CLAIM_VERSION_STALE", "batch_unresolved")
            rows = self._claim_rows(run_id)
            actual_members = [self._member_from_row(row) for row in rows]
            if not rows and operation == "claim" and supplied_members == []:
                pass
            elif not rows or supplied_members != actual_members:
                if len(supplied_members) == len(actual_members):
                    raise AuthorityError("CLAIM_VERSION_STALE", "member_version")
                raise AuthorityError("CLAIM_SCOPE_VIOLATION", "complete_run_members")
            now = _utc_now()
            for row in rows:
                if row["state"] != "active" or row["intent_or_fence_id"] is not None:
                    raise AuthorityError("LEGACY_DRAIN_INCOMPLETE", "claim_frozen")
                if str(row["expires_at"]) <= now and operation != "expire":
                    raise AuthorityError("CLAIM_LEASE_EXPIRED")
            mutation_batch_id = canonical_digest(
                {
                    "authority_id": self.authority_id,
                    "request_id": request_id,
                    "run_id": run_id,
                    "operation": operation,
                    "members": actual_members,
                    "run_digest": pre_run_digest,
                    "lock_set_digest": pre_lock_digest,
                }
            )
            settlement_request_id = _new_request_id()
            connection.execute(
                "INSERT INTO claim_mutation_batches(mutation_id, run_id, operation, settlement_request_id, expected_v1_run_digest, expected_v1_lockset_digest, state, mutation_process_proof_digest, created_at) VALUES(?,?,?,?,?,?,'reserved',?,?)",
                (
                    mutation_batch_id,
                    run_id,
                    operation,
                    settlement_request_id,
                    pre_run_digest,
                    pre_lock_digest,
                    mutation_process_identity,
                    self._assert_broker_clock(),
                ),
            )
            connection.executemany(
                "INSERT INTO claim_mutation_members(mutation_id, claim_id, expected_authority_claim_version, expected_authority_lease_epoch) VALUES(?,?,?,?)",
                [
                    (
                        mutation_batch_id,
                        member["claim_id"],
                        member["claim_version"],
                        member["lease_epoch"],
                    )
                    for member in actual_members
                ],
            )
            receipt = self._new_receipt(
                {
                    "operation": "begin-claim-mutation",
                    "request_id": request_id,
                    "mutation_operation": operation,
                    "mutation_batch_id": mutation_batch_id,
                    "settlement_request_id": settlement_request_id,
                    "run_id": run_id,
                    "state": "reserved",
                    "members": actual_members,
                    "pre_run_digest": pre_run_digest,
                    "pre_lock_set_digest": pre_lock_digest,
                    "mutation_process_identity_digest": mutation_process_identity,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def settle_claim_mutation(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "mutation_batch_id",
                "outcome",
                "resulting_run_digest",
                "resulting_lock_set_digest",
                "members",
                "mutation_process_identity_digest",
            }
            | _ENVELOPE_IDENTITY_FIELDS,
            detail="settle_claim_mutation",
        )
        request_id = _uuid4(request.get("request_id"))
        if request.get("schema") != "claim-mutation-envelope/v2" or request.get("operation") != "settle-claim-mutation":
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "settle_claim_mutation")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        outcome = str(request.get("outcome") or "")
        if outcome not in {"applied", "rejected", "unknown"}:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "outcome")
        resulting_run_digest = _require_digest(
            request.get("resulting_run_digest"),
            code="AFFECTED_GRAPH_MISMATCH",
            detail="resulting_run_digest",
        )
        resulting_lock_digest = _require_digest(
            request.get("resulting_lock_set_digest"),
            code="AFFECTED_GRAPH_MISMATCH",
            detail="resulting_lock_set_digest",
        )
        mutation_batch_id = str(request.get("mutation_batch_id") or "")
        mutation_process_identity = _require_digest(
            request.get("mutation_process_identity_digest"),
            code="REQUEST_SCHEMA_INVALID",
            detail="mutation_process_identity_digest",
        )
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            batch = connection.execute(
                "SELECT * FROM claim_mutation_batches WHERE mutation_id=?",
                (mutation_batch_id,),
            ).fetchone()
            if batch is None or batch["state"] != "reserved":
                raise AuthorityError("CLAIM_VERSION_STALE", "batch_state")
            if str(batch["settlement_request_id"]) != request_id:
                raise AuthorityError("REQUEST_SCHEMA_INVALID", "settlement_request_id")
            if batch["mutation_process_proof_digest"] != mutation_process_identity:
                raise AuthorityError("IDENTITY_MISMATCH", "mutation_process")
            members = self._mutation_members(mutation_batch_id)
            if request.get("members") != members:
                raise AuthorityError("CLAIM_SCOPE_VIOLATION", "batch_members")
            operation = str(batch["operation"])
            unchanged = (
                resulting_run_digest == batch["expected_v1_run_digest"]
                and resulting_lock_digest == batch["expected_v1_lockset_digest"]
            )
            if outcome == "applied" and unchanged:
                raise AuthorityError("AFFECTED_GRAPH_MISMATCH", f"{operation}_no_change")
            if outcome == "rejected" and not unchanged:
                raise AuthorityError("AFFECTED_GRAPH_MISMATCH", f"{operation}_changed")

            final_members = members
            state = "unknown" if outcome == "unknown" else "settled"
            if outcome == "applied":
                if operation == "heartbeat":
                    connection.execute(
                        "UPDATE claims SET authority_lease_epoch=authority_lease_epoch+1, expires_at=? WHERE run_id=?",
                        (_lease_expiry(), batch["run_id"]),
                    )
                elif operation == "claim":
                    connection.executemany(
                        "UPDATE claims SET authority_claim_version=authority_claim_version+1 WHERE claim_id=?",
                        [(member["claim_id"],) for member in members],
                    )
                else:
                    target_state = {
                        "claim": "active",
                        "close": "closed",
                        "takeover": "taken_over",
                        "expire": "expired",
                    }[operation]
                    connection.execute(
                        "UPDATE claims SET state=?, authority_claim_version=authority_claim_version+1 WHERE run_id=?",
                        (target_state, batch["run_id"]),
                    )
                final_members = [self._member_from_row(row) for row in self._claim_rows(str(batch["run_id"]))]
            connection.execute(
                "UPDATE claim_mutation_batches SET state=?, result_v1_run_digest=?, result_v1_lockset_digest=?, outcome=?, settled_at=? WHERE mutation_id=?",
                (
                    state,
                    resulting_run_digest,
                    resulting_lock_digest,
                    outcome,
                    self._assert_broker_clock(),
                    mutation_batch_id,
                ),
            )
            receipt = self._new_receipt(
                {
                    "operation": "settle-claim-mutation",
                    "request_id": request_id,
                    "settlement_request_id": str(batch["settlement_request_id"]),
                    "mutation_operation": operation,
                    "mutation_batch_id": mutation_batch_id,
                    "run_id": str(batch["run_id"]),
                    "state": state,
                    "outcome": outcome,
                    "members": final_members,
                    "resulting_run_digest": resulting_run_digest,
                    "resulting_lock_set_digest": resulting_lock_digest,
                    "mutation_process_identity_digest": mutation_process_identity,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def mark_claim_mutation_operator_required(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "mutation_batch_id",
                "reason_code",
                "mutation_process_identity_digest",
            },
            detail="mark_claim_mutation_operator_required",
        )
        request_id = _uuid4(request.get("request_id"))
        if (
            request.get("schema") != "claim-mutation-envelope/v2"
            or request.get("operation") != "mark-claim-mutation-operator-required"
        ):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "mark_claim_mutation_operator_required")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        mutation_batch_id = str(request.get("mutation_batch_id") or "")
        reason_code = str(request.get("reason_code") or "")
        if not _IDENTIFIER_RE.fullmatch(reason_code):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "reason_code")
        process_identity = _require_digest(
            request.get("mutation_process_identity_digest"),
            code="REQUEST_SCHEMA_INVALID",
            detail="mutation_process_identity_digest",
        )
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            batch = connection.execute(
                "SELECT * FROM claim_mutation_batches WHERE mutation_id=?",
                (mutation_batch_id,),
            ).fetchone()
            if batch is None or batch["state"] != "unknown":
                raise AuthorityError("CLAIM_VERSION_STALE", "batch_state")
            if int(batch["operator_required"]):
                raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "already_marked")
            if batch["mutation_process_proof_digest"] != process_identity:
                raise AuthorityError("IDENTITY_MISMATCH", "mutation_process")
            marked_at = self._assert_broker_clock()
            connection.execute(
                "UPDATE claim_mutation_batches SET operator_required=1, operator_required_at=?, mutation_process_proof_digest=? WHERE mutation_id=?",
                (marked_at, process_identity, mutation_batch_id),
            )
            receipt = self._new_receipt(
                {
                    "operation": "mark-claim-mutation-operator-required",
                    "request_id": request_id,
                    "mutation_batch_id": mutation_batch_id,
                    "run_id": str(batch["run_id"]),
                    "state": "unknown",
                    "operator_required": True,
                    "reason_code": reason_code,
                    "mutation_process_identity_digest": process_identity,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def resolve_claim_mutation_unknown(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "mutation_batch_id",
                "authorization_digest",
                "stopped_process_digest",
                "first_complete_read",
                "second_complete_read",
                "outcome",
            },
            detail="resolve_claim_mutation_unknown",
        )
        request_id = _uuid4(request.get("request_id"))
        if (
            request.get("schema") != "claim-mutation-envelope/v2"
            or request.get("operation") != "resolve-claim-mutation-unknown"
        ):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "resolve_claim_mutation_unknown")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        for field in ("authorization_digest", "stopped_process_digest"):
            _require_digest(request.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
        first_read = request.get("first_complete_read")
        second_read = request.get("second_complete_read")
        if not isinstance(first_read, Mapping) or not isinstance(second_read, Mapping):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "complete_reads")
        expected_read_keys = {"run_digest", "lock_set_digest", "members"}
        if set(first_read) != expected_read_keys or set(second_read) != expected_read_keys:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "complete_reads")
        for field in ("run_digest", "lock_set_digest"):
            _require_digest(first_read.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
            _require_digest(second_read.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
        if canonical_json(first_read) != canonical_json(second_read):
            raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "operator_reads")
        outcome = str(request.get("outcome") or "")
        if outcome not in {"applied", "rejected"}:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "outcome")
        mutation_batch_id = str(request.get("mutation_batch_id") or "")
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            batch = connection.execute(
                "SELECT * FROM claim_mutation_batches WHERE mutation_id=?",
                (mutation_batch_id,),
            ).fetchone()
            if batch is None or batch["state"] != "unknown":
                raise AuthorityError("CLAIM_VERSION_STALE", "batch_state")
            if not int(batch["operator_required"]):
                raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "marker_missing")
            expected_members = self._mutation_members(mutation_batch_id)
            actual_members = [self._member_from_row(row) for row in self._claim_rows(str(batch["run_id"]))]
            if first_read.get("members") != actual_members:
                raise AuthorityError("CLAIM_VERSION_STALE", "operator_members")
            observed_pair = (str(first_read["run_digest"]), str(first_read["lock_set_digest"]))
            pre_pair = (
                str(batch["expected_v1_run_digest"]),
                str(batch["expected_v1_lockset_digest"]),
            )
            result_pair = (
                str(batch["result_v1_run_digest"] or ""),
                str(batch["result_v1_lockset_digest"] or ""),
            )
            if outcome == "rejected" and observed_pair != pre_pair:
                raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "rejected_state")
            if outcome == "applied" and observed_pair != result_pair:
                raise AuthorityError("AFFECTED_GRAPH_MISMATCH", "applied_state")

            final_members = actual_members
            if outcome == "applied":
                operation = str(batch["operation"])
                if operation == "heartbeat":
                    connection.execute(
                        "UPDATE claims SET authority_lease_epoch=authority_lease_epoch+1, expires_at=? WHERE run_id=?",
                        (_lease_expiry(), batch["run_id"]),
                    )
                elif operation == "claim":
                    connection.executemany(
                        "UPDATE claims SET authority_claim_version=authority_claim_version+1 WHERE claim_id=?",
                        [(member["claim_id"],) for member in expected_members],
                    )
                else:
                    target_state = {
                        "claim": "active",
                        "close": "closed",
                        "takeover": "taken_over",
                        "expire": "expired",
                    }[operation]
                    connection.execute(
                        "UPDATE claims SET state=?, authority_claim_version=authority_claim_version+1 WHERE run_id=?",
                        (target_state, batch["run_id"]),
                    )
                final_members = [self._member_from_row(row) for row in self._claim_rows(str(batch["run_id"]))]
            settled_at = self._assert_broker_clock()
            observed_digest = canonical_digest(first_read)
            connection.execute(
                "UPDATE claim_mutation_batches SET state='settled', outcome=?, operator_required=0, authorization_digest=?, mutation_process_proof_digest=?, observed_v1_state_digest=?, settled_at=? WHERE mutation_id=?",
                (
                    outcome,
                    request["authorization_digest"],
                    request["stopped_process_digest"],
                    observed_digest,
                    settled_at,
                    mutation_batch_id,
                ),
            )
            receipt = self._new_receipt(
                {
                    "operation": "resolve-claim-mutation-unknown",
                    "request_id": request_id,
                    "mutation_batch_id": mutation_batch_id,
                    "run_id": str(batch["run_id"]),
                    "state": "settled",
                    "outcome": outcome,
                    "members": final_members,
                    "authorization_digest": str(request["authorization_digest"]),
                    "stopped_process_digest": str(request["stopped_process_digest"]),
                    "observed_complete_read_digest": observed_digest,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def issue_legacy_fence(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "claim_id",
                "claim_version",
                "lease_epoch",
                "v1_allow_receipt_digest",
                "v1_snapshot_digest",
                "changeset_digest",
                "path_digest",
                "head_oid",
                "descriptor_digest",
                "remote_ref",
                "expected_remote_oid",
                "first_remote_observation",
                "second_remote_observation",
            },
            detail="issue_legacy_fence",
        )
        request_id = _uuid4(request.get("request_id"))
        if request.get("schema") != "claim-mutation-envelope/v2" or request.get("operation") != "issue-legacy-fence":
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "issue_legacy_fence")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        for field in (
            "v1_allow_receipt_digest",
            "v1_snapshot_digest",
            "changeset_digest",
            "path_digest",
            "descriptor_digest",
        ):
            _require_digest(request.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
        if not _OID_RE.fullmatch(str(request.get("head_oid") or "")) or not _OID_RE.fullmatch(
            str(request.get("expected_remote_oid") or "")
        ):
            raise AuthorityError("IDENTITY_MISMATCH", "oid")
        remote_ref = str(request.get("remote_ref") or "")
        if not remote_ref.startswith("refs/heads/"):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "remote_ref")
        state, _, descriptor_digest = self._activation_meta()
        if state != "shadow-active" or request.get("descriptor_digest") != descriptor_digest:
            raise AuthorityError("AUTHORITY_DESCRIPTOR_MISMATCH", "activation")
        remote_observation_digest = _validate_remote_observation_pair(
            request,
            descriptor_digest=str(descriptor_digest),
            remote_ref=remote_ref,
            observed_remote_oid=str(request["expected_remote_oid"]),
        )
        claim_id = str(request.get("claim_id") or "")
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            claim = connection.execute("SELECT * FROM claims WHERE claim_id=?", (claim_id,)).fetchone()
            if claim is None:
                raise AuthorityError("IDENTITY_MISMATCH", "claim_id")
            v1_receipt = self._receipt_by_digest(str(request["v1_allow_receipt_digest"]))
            effective_v1 = (
                v1_receipt.get("comparison", {}).get("effective_v1") if isinstance(v1_receipt, dict) else None
            )
            receipt_scope = (
                v1_receipt.get("publication_scope") if isinstance(v1_receipt, dict) else None
            )
            if (
                not isinstance(v1_receipt, dict)
                or v1_receipt.get("operation") != "observe-claim"
                or v1_receipt.get("claim_id") != claim_id
                or not isinstance(effective_v1, dict)
                or effective_v1.get("decision") != "allow"
                or request.get("v1_snapshot_digest") != claim["v1_snapshot_digest"]
                or v1_receipt.get("v1_run_digest") != claim["v1_snapshot_digest"]
            ):
                raise AuthorityError("V1_AUTHORITY_FORBIDDEN", "allow_receipt")
            if isinstance(receipt_scope, Mapping):
                if (
                    receipt_scope.get("effect_ceiling") != "one-legacy-fence"
                    or request.get("path_digest") != receipt_scope.get("paths_digest")
                ):
                    raise AuthorityError("V1_AUTHORITY_FORBIDDEN", "allow_receipt_scope")
            unresolved_batch = connection.execute(
                "SELECT mutation_id FROM claim_mutation_batches WHERE run_id=? AND state IN ('reserved','unknown')",
                (claim["run_id"],),
            ).fetchone()
            if unresolved_batch is not None:
                raise AuthorityError("LEGACY_DRAIN_INCOMPLETE", "mutation_batch")
            if claim["intent_or_fence_id"] is not None:
                raise AuthorityError("LEGACY_FENCE_REPLAY")
            if claim["state"] != "active":
                raise AuthorityError("LEGACY_DRAIN_INCOMPLETE", "claim_frozen")
            if int(request.get("claim_version", -1)) != int(claim["authority_claim_version"]) or int(
                request.get("lease_epoch", -1)
            ) != int(claim["authority_lease_epoch"]):
                raise AuthorityError("CLAIM_VERSION_STALE")
            if str(claim["expires_at"]) <= _utc_now():
                raise AuthorityError("CLAIM_LEASE_EXPIRED")
            fence_id = canonical_digest(
                {
                    "authority_id": self.authority_id,
                    "request_id": request_id,
                    "claim_id": claim_id,
                    "claim_version": int(claim["authority_claim_version"]),
                    "lease_epoch": int(claim["authority_lease_epoch"]),
                    "changeset_digest": request["changeset_digest"],
                    "remote_ref": remote_ref,
                    "expected_remote_oid": request["expected_remote_oid"],
                }
            )
            expires_at = _lease_expiry(minutes=5)
            remote_ref_digest = canonical_digest({"remote_ref": remote_ref})
            _, authority_epoch, _ = self._activation_meta()
            connection.execute(
                "INSERT INTO legacy_fences(fence_id, epoch, claim_id, request_digest, state, expected_remote_oid, remote_ref_digest, expires_at) VALUES(?,?,?,?, 'issued', ?, ?, ?)",
                (
                    fence_id,
                    authority_epoch,
                    claim_id,
                    request_digest,
                    request["expected_remote_oid"],
                    remote_ref_digest,
                    expires_at,
                ),
            )
            connection.execute(
                "UPDATE claims SET intent_or_fence_id=? WHERE claim_id=?",
                (fence_id, claim_id),
            )
            receipt = self._new_receipt(
                {
                    "operation": "issue-legacy-fence",
                    "request_id": request_id,
                    "fence_id": fence_id,
                    "claim_id": claim_id,
                    "state": "issued",
                    "expires_at": expires_at,
                    "remote_ref": remote_ref,
                    "remote_ref_digest": remote_ref_digest,
                    "expected_remote_oid": str(request["expected_remote_oid"]),
                    "head_oid": str(request["head_oid"]),
                    "v1_snapshot_digest": str(request["v1_snapshot_digest"]),
                    "claim_version": int(request["claim_version"]),
                    "lease_epoch": int(request["lease_epoch"]),
                    "descriptor_digest": str(descriptor_digest),
                    "remote_observation_digest": remote_observation_digest,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def enter_legacy_publishing(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "fence_id",
                "v1_snapshot_digest",
                "claim_version",
                "lease_epoch",
                "remote_ref",
                "expected_remote_oid",
                "first_remote_observation",
                "second_remote_observation",
            },
            detail="enter_legacy_publishing",
        )
        request_id = _uuid4(request.get("request_id"))
        if (
            request.get("schema") != "claim-mutation-envelope/v2"
            or request.get("operation") != "enter-legacy-publishing"
        ):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "enter_legacy_publishing")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        request_digest = canonical_digest(request)
        fence_id = str(request.get("fence_id") or "")
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            fence = connection.execute("SELECT * FROM legacy_fences WHERE fence_id=?", (fence_id,)).fetchone()
            if fence is None or fence["state"] != "issued":
                raise AuthorityError("LEGACY_FENCE_REPLAY")
            if str(fence["expires_at"]) <= _utc_now():
                raise AuthorityError("PUBLISH_INTENT_EXPIRED")
            issue_receipt = self._fence_issue_receipt(fence_id)
            remote_ref = str(request.get("remote_ref") or "")
            if (
                canonical_digest({"remote_ref": remote_ref}) != fence["remote_ref_digest"]
                or request.get("expected_remote_oid") != fence["expected_remote_oid"]
            ):
                raise AuthorityError("REMOTE_OID_DRIFT")
            claim = connection.execute(
                "SELECT * FROM claims WHERE claim_id=?",
                (fence["claim_id"],),
            ).fetchone()
            if claim is None:
                raise AuthorityError("AUTHORITY_STORE_CORRUPT", "fence_claim")
            if (
                request.get("v1_snapshot_digest") != issue_receipt.get("v1_snapshot_digest")
                or request.get("claim_version") != int(claim["authority_claim_version"])
                or request.get("lease_epoch") != int(claim["authority_lease_epoch"])
            ):
                raise AuthorityError("CLAIM_VERSION_STALE", "fence_entry")
            remote_observation_digest = _validate_remote_observation_pair(
                request,
                descriptor_digest=str(issue_receipt["descriptor_digest"]),
                remote_ref=remote_ref,
                observed_remote_oid=str(fence["expected_remote_oid"]),
            )
            settlement_request_id = _new_request_id()
            connection.execute(
                "UPDATE legacy_fences SET state='publishing', settlement_request_id=? WHERE fence_id=?",
                (settlement_request_id, fence_id),
            )
            connection.execute(
                "UPDATE claims SET state='publishing' WHERE claim_id=?",
                (fence["claim_id"],),
            )
            receipt = self._new_receipt(
                {
                    "operation": "enter-legacy-publishing",
                    "request_id": request_id,
                    "fence_id": fence_id,
                    "claim_id": str(fence["claim_id"]),
                    "settlement_request_id": settlement_request_id,
                    "state": "publishing",
                    "remote_ref": remote_ref,
                    "remote_ref_digest": str(fence["remote_ref_digest"]),
                    "expected_remote_oid": str(fence["expected_remote_oid"]),
                    "remote_observation_digest": remote_observation_digest,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def settle_legacy_publication(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "fence_id",
                "outcome",
                "remote_ref",
                "observed_remote_oid",
                "effect_process_identity_digest",
                "first_remote_observation",
                "second_remote_observation",
            },
            detail="settle_legacy_publication",
        )
        request_id = _uuid4(request.get("request_id"))
        if (
            request.get("schema") != "claim-mutation-envelope/v2"
            or request.get("operation") != "settle-legacy-publication"
        ):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "settle_legacy_publication")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        outcome = str(request.get("outcome") or "")
        if outcome not in {"success", "rejected", "unknown"}:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "outcome")
        effect_process_identity_digest = _require_digest(
            request.get("effect_process_identity_digest"),
            code="REQUEST_SCHEMA_INVALID",
            detail="effect_process_identity_digest",
        )
        if not _OID_RE.fullmatch(str(request.get("observed_remote_oid") or "")):
            raise AuthorityError("REMOTE_OID_DRIFT")
        request_digest = canonical_digest(request)
        fence_id = str(request.get("fence_id") or "")
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            fence = connection.execute("SELECT * FROM legacy_fences WHERE fence_id=?", (fence_id,)).fetchone()
            if fence is None or fence["state"] != "publishing":
                raise AuthorityError("LEGACY_FENCE_REPLAY")
            if not fence["settlement_request_id"] or str(fence["settlement_request_id"]) != request_id:
                raise AuthorityError("REQUEST_SCHEMA_INVALID", "settlement_request_id")
            remote_ref = str(request.get("remote_ref") or "")
            if canonical_digest({"remote_ref": remote_ref}) != fence["remote_ref_digest"]:
                raise AuthorityError("REMOTE_OID_DRIFT")
            issue_receipt = self._fence_issue_receipt(fence_id)
            observation_digest = _validate_remote_observation_pair(
                request,
                descriptor_digest=str(issue_receipt["descriptor_digest"]),
                remote_ref=remote_ref,
                observed_remote_oid=str(request["observed_remote_oid"]),
                effect_process_identity_digest=effect_process_identity_digest,
            )
            if outcome == "success" and request["observed_remote_oid"] != issue_receipt.get("head_oid"):
                raise AuthorityError("REMOTE_OID_DRIFT", "success_oid")
            if outcome == "rejected" and request["observed_remote_oid"] != fence["expected_remote_oid"]:
                raise AuthorityError("REMOTE_OID_DRIFT", "rejected_oid")
            state = "unknown" if outcome == "unknown" else "settled"
            connection.execute(
                "UPDATE legacy_fences SET state=?, outcome=?, observed_remote_oid=? WHERE fence_id=?",
                (state, outcome, request["observed_remote_oid"], fence_id),
            )
            if state == "settled":
                connection.execute(
                    "UPDATE claims SET state='active', intent_or_fence_id=NULL WHERE claim_id=?",
                    (fence["claim_id"],),
                )
            receipt = self._new_receipt(
                {
                    "operation": "settle-legacy-publication",
                    "request_id": request_id,
                    "settlement_request_id": str(fence["settlement_request_id"]),
                    "fence_id": fence_id,
                    "claim_id": str(fence["claim_id"]),
                    "state": state,
                    "outcome": outcome,
                    "remote_ref": remote_ref,
                    "remote_ref_digest": str(fence["remote_ref_digest"]),
                    "observed_remote_oid": str(request["observed_remote_oid"]),
                    "observation_digest": observation_digest,
                    "effect_process_identity_digest": effect_process_identity_digest,
                    "descriptor_digest": str(issue_receipt["descriptor_digest"]),
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def mark_legacy_operator_required(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "fence_id",
                "reason_code",
                "effect_process_identity_digest",
            },
            detail="mark_legacy_operator_required",
        )
        request_id = _uuid4(request.get("request_id"))
        if (
            request.get("schema") != "claim-mutation-envelope/v2"
            or request.get("operation") != "mark-legacy-operator-required"
        ):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "mark_legacy_operator_required")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        fence_id = str(request.get("fence_id") or "")
        reason_code = str(request.get("reason_code") or "")
        if not _IDENTIFIER_RE.fullmatch(reason_code):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "reason_code")
        effect_identity = _require_digest(
            request.get("effect_process_identity_digest"),
            code="REQUEST_SCHEMA_INVALID",
            detail="effect_process_identity_digest",
        )
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            fence = connection.execute(
                "SELECT * FROM legacy_fences WHERE fence_id=?",
                (fence_id,),
            ).fetchone()
            if fence is None or fence["state"] != "unknown":
                raise AuthorityError("LEGACY_FENCE_REPLAY", "fence_state")
            if int(fence["operator_required"]):
                raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "already_marked")
            unknown_receipt = self._fence_unknown_settlement_receipt(fence_id)
            if unknown_receipt.get("effect_process_identity_digest") != effect_identity:
                raise AuthorityError("IDENTITY_MISMATCH", "effect_process")
            marked_at = self._assert_broker_clock()
            connection.execute(
                "UPDATE legacy_fences SET operator_required=1, operator_required_at=?, effect_process_proof_digest=? WHERE fence_id=?",
                (marked_at, effect_identity, fence_id),
            )
            receipt = self._new_receipt(
                {
                    "operation": "mark-legacy-operator-required",
                    "request_id": request_id,
                    "fence_id": fence_id,
                    "claim_id": str(fence["claim_id"]),
                    "state": "unknown",
                    "operator_required": True,
                    "reason_code": reason_code,
                    "effect_process_identity_digest": effect_identity,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def resolve_legacy_unknown(self, request: Mapping[str, Any]) -> dict[str, Any]:
        _reject_unknown_fields(
            request,
            {
                "schema",
                "request_id",
                "authority_id",
                "operation",
                "fence_id",
                "authorization_digest",
                "stopped_process_digest",
                "remote_ref",
                "first_remote_observation",
                "second_remote_observation",
                "outcome",
            },
            detail="resolve_legacy_unknown",
        )
        request_id = _uuid4(request.get("request_id"))
        if (
            request.get("schema") != "claim-mutation-envelope/v2"
            or request.get("operation") != "resolve-legacy-unknown"
        ):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "resolve_legacy_unknown")
        if request.get("authority_id") != self.authority_id:
            raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
        for field in ("authorization_digest", "stopped_process_digest"):
            _require_digest(request.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
        first_observation = request.get("first_remote_observation")
        if not isinstance(first_observation, Mapping):
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "remote_observations")
        observed_remote_oid = str(first_observation.get("observed_remote_oid") or "")
        outcome = str(request.get("outcome") or "")
        if outcome not in {"success", "rejected"}:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "outcome")
        fence_id = str(request.get("fence_id") or "")
        request_digest = canonical_digest(request)
        self._ensure_daily_backup()
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._idempotent_response(request_id, request_digest)
            if existing is not None:
                connection.execute("COMMIT")
                return existing
            fence = connection.execute(
                "SELECT * FROM legacy_fences WHERE fence_id=?",
                (fence_id,),
            ).fetchone()
            if fence is None or fence["state"] != "unknown":
                raise AuthorityError("LEGACY_FENCE_REPLAY", "fence_state")
            if not int(fence["operator_required"]):
                raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "marker_missing")
            remote_ref = str(request.get("remote_ref") or "")
            if canonical_digest({"remote_ref": remote_ref}) != fence["remote_ref_digest"]:
                raise AuthorityError("REMOTE_OID_DRIFT", "remote_ref")
            issue_receipt = self._fence_issue_receipt(fence_id)
            settlement_digest = _validate_remote_observation_pair(
                request,
                descriptor_digest=str(issue_receipt["descriptor_digest"]),
                remote_ref=remote_ref,
                observed_remote_oid=observed_remote_oid,
                effect_process_identity_digest=str(fence["effect_process_proof_digest"]),
            )
            if outcome == "success" and observed_remote_oid != issue_receipt.get("head_oid"):
                raise AuthorityError("REMOTE_OID_DRIFT", "success_oid")
            if outcome == "rejected" and observed_remote_oid != fence["expected_remote_oid"]:
                raise AuthorityError("REMOTE_OID_DRIFT", "rejected_oid")
            settled_at = self._assert_broker_clock()
            connection.execute(
                "UPDATE legacy_fences SET state='settled', outcome=?, observed_remote_oid=?, operator_required=0, operator_authorization_digest=?, effect_process_proof_digest=?, settlement_digest=? WHERE fence_id=?",
                (
                    outcome,
                    observed_remote_oid,
                    request["authorization_digest"],
                    request["stopped_process_digest"],
                    settlement_digest,
                    fence_id,
                ),
            )
            connection.execute(
                "UPDATE claims SET state='active', intent_or_fence_id=NULL WHERE claim_id=?",
                (fence["claim_id"],),
            )
            receipt = self._new_receipt(
                {
                    "operation": "resolve-legacy-unknown",
                    "request_id": request_id,
                    "fence_id": fence_id,
                    "claim_id": str(fence["claim_id"]),
                    "state": "settled",
                    "outcome": outcome,
                    "observed_remote_oid": observed_remote_oid,
                    "authorization_digest": str(request["authorization_digest"]),
                    "stopped_process_digest": str(request["stopped_process_digest"]),
                    "settlement_digest": settlement_digest,
                    "settled_at": settled_at,
                }
            )
            self._insert_receipt_and_idempotency(
                receipt,
                request_id=request_id,
                request_digest=request_digest,
            )
            connection.execute("COMMIT")
            self._settle_high_water_for_receipt(receipt)
            return receipt
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def evaluate_graduation(self) -> dict[str, Any]:
        unresolved_fences = int(
            self.scalar(
                "SELECT COUNT(*) FROM legacy_fences WHERE state IN ('issued','publishing','unknown') OR operator_required=1"
            )
            or 0
        )
        unresolved_batches = int(
            self.scalar(
                "SELECT COUNT(*) FROM claim_mutation_batches WHERE state IN ('reserved','unknown') OR operator_required=1"
            )
            or 0
        )
        if unresolved_fences or unresolved_batches:
            raise AuthorityError("LEGACY_DRAIN_INCOMPLETE")
        return {
            "ready": False,
            "reason": "observation_window_unproven",
            "instruction_capable": False,
        }


def _unknown_receipt_digest(
    store: _AuthorityStore,
    *,
    operation: str,
    target_field: str,
    target_id: str,
) -> str:
    matches: list[str] = []
    for row in store._connection.execute(
        "SELECT receipt_json, receipt_digest FROM receipts WHERE authority_id=? ORDER BY sequence",
        (store.authority_id,),
    ):
        try:
            payload = json.loads(row["receipt_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise AuthorityError("AUTHORITY_STORE_CORRUPT", "receipt_json") from exc
        if (
            isinstance(payload, dict)
            and payload.get("operation") == operation
            and payload.get(target_field) == target_id
            and payload.get("state") == "unknown"
            and payload.get("outcome") == "unknown"
        ):
            matches.append(
                _require_digest(
                    row["receipt_digest"],
                    code="AUTHORITY_STORE_CORRUPT",
                    detail="unknown_receipt_digest",
                )
            )
    if len(matches) != 1:
        raise AuthorityError("AUTHORITY_STORE_CORRUPT", "unknown_receipt_identity")
    return matches[0]


def _verify_production_operator_resolution(
    store: _AuthorityStore,
    paths: AuthorityPaths,
    method_name: str,
    request: Mapping[str, Any],
) -> None:
    if method_name == "resolve_claim_mutation_unknown":
        target_kind = "claim_mutation"
        target_field = "mutation_batch_id"
        target_id = str(request.get(target_field) or "")
        resolver_operation = "resolve-claim-mutation-unknown"
        unknown_operation = "settle-claim-mutation"
        row = store._connection.execute(
            "SELECT state, operator_required, operator_required_at, mutation_process_proof_digest FROM claim_mutation_batches WHERE mutation_id=?",
            (target_id,),
        ).fetchone()
        if row is None or row["state"] != "unknown":
            raise AuthorityError("CLAIM_VERSION_STALE", "batch_state")
        process_identity_digest = row["mutation_process_proof_digest"]
    elif method_name == "resolve_legacy_unknown":
        target_kind = "legacy_fence"
        target_field = "fence_id"
        target_id = str(request.get(target_field) or "")
        resolver_operation = "resolve-legacy-unknown"
        unknown_operation = "settle-legacy-publication"
        row = store._connection.execute(
            "SELECT state, operator_required, operator_required_at, effect_process_proof_digest FROM legacy_fences WHERE fence_id=?",
            (target_id,),
        ).fetchone()
        if row is None or row["state"] != "unknown":
            raise AuthorityError("LEGACY_FENCE_REPLAY", "fence_state")
        process_identity_digest = row["effect_process_proof_digest"]
    else:
        return

    if not int(row["operator_required"]):
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "marker_missing")
    expected_process_digest = _require_digest(
        process_identity_digest,
        code="IDENTITY_MISMATCH",
        detail="recorded_process_identity",
    )
    unknown_receipt_digest = _unknown_receipt_digest(
        store,
        operation=unknown_operation,
        target_field=target_field,
        target_id=target_id,
    )
    outcome = str(request.get("outcome") or "")
    from . import claims_verifiers

    authorization = _read_trusted_operator_evidence(
        paths,
        request.get("authorization_digest"),
        directory_name=_OPERATOR_AUTHORIZATION_DIR,
        code="OPERATOR_AUTHORIZATION_REQUIRED",
    )
    claims_verifiers.verify_operator_authorization(authorization)
    if any(
        (
            authorization.get("authority_id") != store.authority_id,
            authorization.get("target_kind") != target_kind,
            authorization.get("target_id") != target_id,
            authorization.get("unknown_receipt_digest") != unknown_receipt_digest,
            authorization.get("resolver_operation") != resolver_operation,
            authorization.get("authorized_outcome") != outcome,
            authorization.get("process_identity_digest") != expected_process_digest,
        )
    ):
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_binding")
    issued_at = _operator_evidence_time(
        authorization.get("issued_at"),
        code="OPERATOR_AUTHORIZATION_REQUIRED",
        detail="authorization_issued_at",
    )
    expires_at = _operator_evidence_time(
        authorization.get("expires_at"),
        code="OPERATOR_AUTHORIZATION_REQUIRED",
        detail="authorization_expires_at",
    )
    marker_at = _operator_evidence_time(
        row["operator_required_at"],
        code="OPERATOR_AUTHORIZATION_REQUIRED",
        detail="operator_required_at",
    )
    now = _clock_now()
    if issued_at < marker_at or issued_at > now or expires_at <= issued_at or now > expires_at:
        raise AuthorityError("OPERATOR_AUTHORIZATION_REQUIRED", "authorization_time")

    stopped_proof = _read_trusted_operator_evidence(
        paths,
        request.get("stopped_process_digest"),
        directory_name=_STOPPED_PROCESS_PROOF_DIR,
        code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID",
    )
    claims_verifiers.verify_stopped_process_proof(stopped_proof, authorization=authorization)
    if any(
        (
            stopped_proof.get("authority_id") != store.authority_id,
            stopped_proof.get("target_kind") != target_kind,
            stopped_proof.get("target_id") != target_id,
            stopped_proof.get("unknown_receipt_digest") != unknown_receipt_digest,
            stopped_proof.get("authorization_digest") != request.get("authorization_digest"),
            stopped_proof.get("process_identity_digest") != expected_process_digest,
        )
    ):
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_binding")
    observed_at = _operator_evidence_time(
        stopped_proof.get("observed_at"),
        code="OPERATOR_STOPPED_PROCESS_PROOF_INVALID",
        detail="stopped_process_observed_at",
    )
    if observed_at < marker_at or observed_at > now:
        raise AuthorityError("OPERATOR_STOPPED_PROCESS_PROOF_INVALID", "stopped_process_time")


def _open_production_store(
    *,
    create_for_activation: bool = False,
    resolved_paths: AuthorityPaths | None = None,
) -> _AuthorityStore:
    paths = resolved_paths or resolve_authority_paths()
    if paths.store.exists():
        return _AuthorityStore._connect_existing(paths, _PRODUCTION_AUTHORITY_ID)
    if not create_for_activation:
        raise AuthorityError("AUTHORITY_UNAVAILABLE", "store_missing")
    if os.path.lexists(paths.authority_dir):
        raise AuthorityError("AUTHORITY_STORE_UNSAFE", "non_pristine_authority_dir")
    paths.authority_dir.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    store = _AuthorityStore._connect_new(
        paths.authority_dir,
        _PRODUCTION_AUTHORITY_ID,
        test_only=False,
    )
    store.test_paths = paths
    return store


def _invoke_production(method_name: str, request: Mapping[str, Any]) -> dict[str, Any]:
    if request.get("authority_id") != _PRODUCTION_AUTHORITY_ID:
        raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
    paths = resolve_authority_paths()
    store = _open_production_store(
        create_for_activation=method_name == "activate_shadow",
        resolved_paths=paths,
    )
    try:
        if method_name != "activate_shadow" and store._activation_meta()[0] != "shadow-active":
            raise AuthorityError("AUTHORITY_UNAVAILABLE", "not_activated")
        if method_name in {"resolve_claim_mutation_unknown", "resolve_legacy_unknown"}:
            request_id = _uuid4(request.get("request_id"))
            existing = store._idempotent_response(request_id, canonical_digest(request))
            if existing is None:
                _verify_production_operator_resolution(store, paths, method_name, request)
        method = getattr(store, method_name)
        result = method(request)
        if not isinstance(result, dict):
            raise AuthorityError("AUTHORITY_STORE_CORRUPT", "response_type")
        return result
    finally:
        store._connection.close()


def observe_claim(request: Mapping[str, Any]) -> dict[str, Any]:
    _verify_production_observe_request(request, resolve_authority_paths())
    return _invoke_production("observe_claim", request)


def begin_claim_mutation(request: Mapping[str, Any]) -> dict[str, Any]:
    _verify_production_mutation_request(request, resolve_authority_paths(), phase="before")
    return _invoke_production("begin_claim_mutation", request)


def settle_claim_mutation(request: Mapping[str, Any]) -> dict[str, Any]:
    _verify_production_mutation_request(request, resolve_authority_paths(), phase="after")
    return _invoke_production("settle_claim_mutation", request)


def mark_claim_mutation_operator_required(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("mark_claim_mutation_operator_required", request)


def resolve_claim_mutation_unknown(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("resolve_claim_mutation_unknown", request)


def activate_shadow(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("activate_shadow", request)


def issue_legacy_fence(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("issue_legacy_fence", request)


def enter_legacy_publishing(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("enter_legacy_publishing", request)


def settle_legacy_publication(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("settle_legacy_publication", request)


def mark_legacy_operator_required(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("mark_legacy_operator_required", request)


def resolve_legacy_unknown(request: Mapping[str, Any]) -> dict[str, Any]:
    return _invoke_production("resolve_legacy_unknown", request)


def authority_status() -> dict[str, Any]:
    paths = resolve_authority_paths()
    if not paths.store.exists():
        policy = _broker_unavailable_policy(paths)
        return {
            "schema": "claims-authority-status/v2",
            "authority_id": _PRODUCTION_AUTHORITY_ID,
            "security_level": "R0_COOPERATIVE",
            "activation_state": "unactivated",
            "authority_epoch": 0,
            "descriptor_digest": None,
            "sequence": 0,
            "last_receipt_digest": None,
            "observed_at": None,
            "fresh": False,
            "effective_claim_authority": "v1",
            "instruction_capable": False,
            "code": policy["code"],
        }
    store = _AuthorityStore._connect_existing(paths, _PRODUCTION_AUTHORITY_ID)
    try:
        return store.authority_status()
    finally:
        store._connection.close()


def evaluate_graduation(request: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema",
        "operation",
        "request_id",
        "authority_id",
        "activation_receipt_digest",
        "observation_window_digest",
        "lifecycle_receipt_ids_digest",
        "red_report_digest",
        "publication_inventory_digest",
    }
    if (
        set(request) != required
        or request.get("schema") != "claim-mutation-envelope/v2"
        or request.get("operation") != "evaluate-graduation"
    ):
        raise AuthorityError("REQUEST_SCHEMA_INVALID", "evaluate_graduation")
    _uuid4(request.get("request_id"))
    if request.get("authority_id") != _PRODUCTION_AUTHORITY_ID:
        raise AuthorityError("IDENTITY_MISMATCH", "authority_id")
    for field in required - {"schema", "operation", "request_id", "authority_id"}:
        _require_digest(request.get(field), code="REQUEST_SCHEMA_INVALID", detail=field)
    store = _open_production_store()
    try:
        return store.evaluate_graduation()
    finally:
        store._connection.close()


_DISPATCH_METHODS = {
    "observe-claim": "observe_claim",
    "begin-claim-mutation": "begin_claim_mutation",
    "settle-claim-mutation": "settle_claim_mutation",
    "mark-claim-mutation-operator-required": "mark_claim_mutation_operator_required",
    "resolve-claim-mutation-unknown": "resolve_claim_mutation_unknown",
    "activate-shadow": "activate_shadow",
    "issue-legacy-fence": "issue_legacy_fence",
    "enter-legacy-publishing": "enter_legacy_publishing",
    "settle-legacy-publication": "settle_legacy_publication",
    "mark-legacy-operator-required": "mark_legacy_operator_required",
    "resolve-legacy-unknown": "resolve_legacy_unknown",
    "evaluate-graduation": "evaluate_graduation",
}


def dispatch_request(verb: str, request: Mapping[str, Any] | None) -> dict[str, Any]:
    if verb == "status":
        if request is not None:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "status_body")
        result = authority_status()
    else:
        method_name = _DISPATCH_METHODS.get(verb)
        if method_name is None or request is None:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "verb_or_body")
        result = globals()[method_name](request)
    sequence = result.get("sequence", 0)
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise AuthorityError("AUTHORITY_STORE_CORRUPT", "response_sequence")
    return {
        "ok": True,
        "schema": "claims-authority-response/v2",
        "authority_id": _PRODUCTION_AUTHORITY_ID,
        "sequence": sequence,
        "result": result,
        "error": None,
    }


def cli_main(argv: Sequence[str]) -> int:
    try:
        args = list(argv)
        if args == ["status", "--json"]:
            request: Mapping[str, Any] | None = None
            verb = "status"
        elif len(args) == 3 and args[1:] == ["--request-json", "-"]:
            verb = args[0]
            raw = sys.stdin.read().strip()
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise AuthorityError("REQUEST_SCHEMA_INVALID", "json") from exc
            if not isinstance(decoded, dict) or canonical_json(decoded) != raw:
                raise AuthorityError("REQUEST_SCHEMA_INVALID", "canonical_json")
            request = decoded
        else:
            raise AuthorityError("REQUEST_SCHEMA_INVALID", "argv")
        response = dispatch_request(verb, request)
        sys.stdout.write(_canonical_json_full(response) + "\n")
        return 0
    except AuthorityError as exc:
        response = {
            "ok": False,
            "schema": "claims-authority-response/v2",
            "authority_id": _PRODUCTION_AUTHORITY_ID,
            "sequence": 0,
            "result": None,
            "error": {"code": exc.code},
        }
        sys.stdout.write(_canonical_json_full(response) + "\n")
        return 2
