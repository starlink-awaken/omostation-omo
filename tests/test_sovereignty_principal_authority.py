"""Tests for omo.sovereignty.principal_authority (BET-Y1Q3-T4-04 WP4)."""

from __future__ import annotations

import pytest

from omo.sovereignty.principal_authority import (
    REASON_AUTHORITY_CREDENTIAL_MISMATCH,
    REASON_AUTHORITY_UNKNOWN,
    REASON_AUTHORITY_VERSION_ROLLBACK,
    AuthorityVerificationError,
    DefaultPrincipalAuthority,
    PrincipalAuthorityReceipt,
    digest_receipt,
)

VALID_CREDENTIAL = "credential:key:1:sha256:a3bba3adae0ebc76d0c42035e9f2c45172edaf945683ccdb9b4d9e40ccaf47ed"
NOW = "2026-08-29T00:00:00+00:00"


def _auth(**kwargs) -> DefaultPrincipalAuthority:
    return DefaultPrincipalAuthority(**kwargs)


def test_verify_returns_valid_receipt() -> None:
    auth = _auth()
    receipt = auth.verify("principal:xiamingxing", VALID_CREDENTIAL, now=NOW)
    assert isinstance(receipt, PrincipalAuthorityReceipt)
    assert receipt.principal_id == "principal:xiamingxing"
    assert receipt.authority_ref == "authority:omo:v1:principal:xiamingxing"
    assert receipt.membership_version == 1
    assert receipt.verified_at == NOW
    assert receipt.expires_at > NOW


def test_missing_credential_ref_rejected() -> None:
    auth = _auth()
    with pytest.raises(AuthorityVerificationError) as exc:
        auth.verify("principal:xiamingxing", "", now=NOW)
    assert exc.value.reason == REASON_AUTHORITY_CREDENTIAL_MISMATCH


def test_malformed_credential_ref_rejected() -> None:
    auth = _auth()
    with pytest.raises(AuthorityVerificationError) as exc:
        auth.verify("principal:xiamingxing", "not-a-credential", now=NOW)
    assert exc.value.reason == REASON_AUTHORITY_CREDENTIAL_MISMATCH


def test_unknown_principal_rejected() -> None:
    auth = _auth()
    with pytest.raises(AuthorityVerificationError) as exc:
        auth.verify("principal:nobody", VALID_CREDENTIAL, now=NOW)
    assert exc.value.reason == REASON_AUTHORITY_UNKNOWN


def test_fixture_only_rejected_in_production() -> None:
    auth = _auth(production=True)
    with pytest.raises(AuthorityVerificationError) as exc:
        auth.verify("principal:alice", VALID_CREDENTIAL, now=NOW)
    assert exc.value.reason == REASON_AUTHORITY_UNKNOWN


def test_fixture_allowed_in_default_mode() -> None:
    # Default (non-production) local authority: fixture principal with matching
    # membership is acceptable — production flag is the gate.
    auth = _auth(production=False)
    operator_credential = "credential:key:1:sha256:9a9c1b0fc191e5c01d73196ebc15e077b5d90b6b03449c92fd630fe6618daab7"
    receipt = auth.verify("principal:operator", operator_credential, now=NOW)
    assert receipt.principal_id == "principal:operator"


def test_credential_digest_mismatch_rejected() -> None:
    auth = _auth()
    bad = "credential:key:1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    with pytest.raises(AuthorityVerificationError) as exc:
        auth.verify("principal:xiamingxing", bad, now=NOW)
    assert exc.value.reason == REASON_AUTHORITY_CREDENTIAL_MISMATCH


def test_version_rollback_rejected() -> None:
    auth = _auth()
    # Version 0 < current membership_version 1 -> rollback
    rollback_credential = "credential:key:0:sha256:a3bba3adae0ebc76d0c42035e9f2c45172edaf945683ccdb9b4d9e40ccaf47ed"
    with pytest.raises(AuthorityVerificationError) as exc:
        auth.verify("principal:xiamingxing", rollback_credential, now=NOW)
    assert exc.value.reason == REASON_AUTHORITY_VERSION_ROLLBACK


def test_digest_receipt_deterministic() -> None:
    auth = _auth()
    r1 = auth.verify("principal:xiamingxing", VALID_CREDENTIAL, now=NOW)
    r2 = auth.verify("principal:xiamingxing", VALID_CREDENTIAL, now=NOW)
    assert digest_receipt(r1) == digest_receipt(r2)
    assert digest_receipt(r1).startswith("sha256:")
    assert len(digest_receipt(r1)) == 7 + 64


def test_receipt_contains_no_credential_secret() -> None:
    auth = _auth()
    receipt = auth.verify("principal:xiamingxing", VALID_CREDENTIAL, now=NOW)
    text = str(receipt)
    # The credential reference (kind+version) is never stored on the receipt.
    assert "credential:key:" not in text
    # The full credential string (kind:version:digest) is never stored either —
    # only the digest itself is present by design.
    assert VALID_CREDENTIAL not in text
    assert "credential_ref" not in text
