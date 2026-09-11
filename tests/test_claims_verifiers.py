from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from omo.workflow.claims_authority import AuthorityError, canonical_digest
from omo.workflow.claims_verifiers import (
    operator_authorization_verifier_digest,
    production_verifier_closure_digest,
    production_verifier_dependency_entries,
    stopped_process_verifier_digest,
    verify_operator_authorization,
    verify_stopped_process_proof,
)


def _digest(seed: str) -> str:
    return f"sha256:{seed * 64}"


def _valid_authorization(*, lifetime_seconds: int = 300) -> dict[str, object]:
    now = datetime.now(UTC)
    payload: dict[str, object] = {
        "schema": "claims-operator-authorization/v1",
        "authority_id": "omo-claims-authority-r0",
        "security_level": "R0_COOPERATIVE",
        "principal_id": "principal-a",
        "principal_authority_ref": "decision://accepted/principal-a",
        "principal_receipt_digest": _digest("1"),
        "decision_ref": "decision://accepted/op-1",
        "target_kind": "claim_mutation",
        "target_id": "batch-1",
        "unknown_receipt_digest": _digest("2"),
        "resolver_operation": "resolve-claim-mutation-unknown",
        "authorized_outcome": "applied",
        "process_identity_digest": _digest("3"),
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(seconds=lifetime_seconds)).isoformat().replace("+00:00", "Z"),
    }
    payload["digest"] = canonical_digest(payload)
    return payload


def _valid_stopped_proof(authorization: dict[str, object], *, observed_offset_seconds: int = 0) -> dict[str, object]:
    issued_at = datetime.fromisoformat(str(authorization["issued_at"]).replace("Z", "+00:00"))
    observed_at = issued_at + timedelta(seconds=observed_offset_seconds)
    payload: dict[str, object] = {
        "schema": "claims-stopped-process-proof/v1",
        "authority_id": "omo-claims-authority-r0",
        "security_level": "R0_COOPERATIVE",
        "observer_kind": "independent-process-observer",
        "observer_receipt_digest": _digest("4"),
        "target_kind": authorization["target_kind"],
        "target_id": authorization["target_id"],
        "unknown_receipt_digest": authorization["unknown_receipt_digest"],
        "authorization_digest": authorization["digest"],
        "process_identity_digest": authorization["process_identity_digest"],
        "status": "stopped",
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
    }
    payload["digest"] = canonical_digest(payload)
    return payload


def test_green_operator_authorization_verifier_accepts_spec_object() -> None:
    payload = _valid_authorization()
    assert verify_operator_authorization(payload) == payload["digest"]


def test_green_stopped_process_verifier_accepts_spec_object_with_authorization_bound() -> None:
    authorization = _valid_authorization()
    stopped = _valid_stopped_proof(authorization)
    assert verify_stopped_process_proof(stopped, authorization=authorization) == stopped["digest"]


def test_red_operator_authorization_rejects_wrong_fields_security_lifetime_and_digest() -> None:
    payload = _valid_authorization()
    payload["extra"] = "nope"
    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        verify_operator_authorization(payload)

    bad_security = _valid_authorization()
    bad_security["security_level"] = "ADVERSARIAL_ENFORCED"
    bad_security["digest"] = canonical_digest(bad_security)
    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        verify_operator_authorization(bad_security)

    too_long = _valid_authorization(lifetime_seconds=301)
    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        verify_operator_authorization(too_long)

    tampered = _valid_authorization()
    tampered["digest"] = _digest("9")
    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        verify_operator_authorization(tampered)


def test_red_stopped_process_rejects_live_status_and_pre_authorization_observation() -> None:
    authorization = _valid_authorization()
    live = _valid_stopped_proof(authorization)
    live["status"] = "running"
    live["digest"] = canonical_digest(live)
    with pytest.raises(AuthorityError, match="OPERATOR_STOPPED_PROCESS_PROOF_INVALID"):
        verify_stopped_process_proof(live, authorization=authorization)

    early = _valid_stopped_proof(authorization, observed_offset_seconds=-1)
    with pytest.raises(AuthorityError, match="OPERATOR_STOPPED_PROCESS_PROOF_INVALID"):
        verify_stopped_process_proof(early, authorization=authorization)


def test_red_oversized_operator_authorization_is_rejected() -> None:
    payload = _valid_authorization()
    payload["decision_ref"] = "x" * 20_000
    payload["digest"] = canonical_digest(payload)
    with pytest.raises(AuthorityError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        verify_operator_authorization(payload)


def test_green_verifier_digests_are_distinct_and_closure_bound() -> None:
    operator_digest = operator_authorization_verifier_digest()
    stopped_digest = stopped_process_verifier_digest()
    assert operator_digest.startswith("sha256:")
    assert stopped_digest.startswith("sha256:")
    assert operator_digest != stopped_digest
    entries = production_verifier_dependency_entries()
    assert [entry["path"] for entry in entries] == sorted(entry["path"] for entry in entries)
    assert {entry["object_oid_or_digest"] for entry in entries} == {operator_digest, stopped_digest}
    assert production_verifier_closure_digest() == canonical_digest(
        {"critical_dependency_entries": entries}
    )
