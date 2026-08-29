"""Principal authority adapter — verify a principal before effect admission.

BET-Y1Q3-T4-04 (Product P0 WP4): upgrade ``principal_id`` from a format-only
string into an identity verified by a single authority before any provider /
router / tool / ledger effect.  OMO is the only principal authority verifier
and admission authority; Cockpit only delegates; Agora only forwards the
already-verified digest.

Design notes:
- The receipt never stores a credential secret; it stores the authority
  reference, a canonical credential digest, membership version and expiry.
- ``digest_receipt`` is a deterministic cross-repo canonical digest so OMO /
  Cockpit / Agora can replay the exact same digest.
- All denial paths raise :class:`AuthorityVerificationError` carrying a
  machine-readable reason key that the enforcement layer maps onto
  ``policy_denied`` (never allow/succeeded).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol, runtime_checkable

from omo.sovereignty.enforcement import (
    PolicyEnforcementError,
    REASON_PDP_UNAVAILABLE,
)

# ---------------------------------------------------------------------------
# Stable authority reason vocabulary (all map onto policy_denied)
# ---------------------------------------------------------------------------

REASON_AUTHORITY_REQUIRED = "authority_required"
REASON_AUTHORITY_PRINCIPAL_MISMATCH = "authority_principal_mismatch"
REASON_AUTHORITY_CREDENTIAL_MISMATCH = "authority_credential_mismatch"
REASON_AUTHORITY_EXPIRED = "authority_expired"
REASON_AUTHORITY_VERSION_ROLLBACK = "authority_version_rollback"
REASON_AUTHORITY_UNKNOWN = "authority_unknown"
REASON_AUTHORITY_REPLAY = "authority_replay"
REASON_AUTHORITY_DIGEST_UNVERIFIED = "authority_digest_unverified"

AUTHORITY_REASONS: tuple[str, ...] = (
    REASON_AUTHORITY_REQUIRED,
    REASON_AUTHORITY_PRINCIPAL_MISMATCH,
    REASON_AUTHORITY_CREDENTIAL_MISMATCH,
    REASON_AUTHORITY_EXPIRED,
    REASON_AUTHORITY_VERSION_ROLLBACK,
    REASON_AUTHORITY_UNKNOWN,
    REASON_AUTHORITY_REPLAY,
    REASON_AUTHORITY_DIGEST_UNVERIFIED,
)

# ---------------------------------------------------------------------------
# Default local authority membership registry (single-user local authority)
# ---------------------------------------------------------------------------

# Format: principal_id -> (credential_kind, credential_digest, membership_version)
# The credential digest here is the *expected* digest for a principal, not a secret.
_DEFAULT_MEMBERS = {
    "principal:xiamingxing": ("key", "sha256:a3bba3adae0ebc76d0c42035e9f2c45172edaf945683ccdb9b4d9e40ccaf47ed", 1),
    "principal:operator": ("key", "sha256:9a9c1b0fc191e5c01d73196ebc15e077b5d90b6b03449c92fd630fe6618daab7", 1),
}

# Fixture-only identities that must never reach the production path.
_FIXTURE_ONLY_PRINCIPALS = frozenset({"principal:alice", "principal:bob"})

# Credential reference format: credential:<kind>:<version>:<digest>
_CREDENTIAL_REF_RE = re.compile(r"^credential:[A-Za-z0-9_-]+:[0-9]+:[A-Za-z0-9_:.-]+$")

_CREDENTIAL_DIGEST_RE = re.compile(r"^sha256:[a-f0-9]{64}$")
_AUTHORITY_REF_RE = re.compile(r"^authority:[A-Za-z0-9_.:/-]+$")

_DEFAULT_TTL_SECONDS = 3600


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Receipt + Protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PrincipalAuthorityReceipt:
    """Canonical authority verification receipt (spec §3)."""

    principal_id: str
    authority_ref: str
    credential_digest: str
    membership_version: int
    verified_at: str
    expires_at: str


class AuthorityVerificationError(PolicyEnforcementError):
    """Raised when principal authority verification fails.

    ``reason`` carries a stable authority reason key (see AUTHORITY_REASONS);
    the enforcement layer maps it onto ``policy_denied``.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@runtime_checkable
class PrincipalAuthority(Protocol):
    """Protocol for the single principal authority verifier."""

    def verify(
        self,
        principal_id: str,
        credential_ref: str,
        *,
        now: str,
    ) -> PrincipalAuthorityReceipt: ...


# ---------------------------------------------------------------------------
# Deterministic receipt digest (cross-repo replay)
# ---------------------------------------------------------------------------


def digest_receipt(receipt: PrincipalAuthorityReceipt) -> str:
    """Canonical digest of a receipt for OMO/Cockpit/Agora full-chain replay.

    Only deterministic fields are hashed (principal, authority ref, credential
    digest, membership version) so the digest is stable for the same identity
    regardless of the verification timestamp; expiry is checked at admission
    time against the receipt timestamps rather than baked into the digest.
    """
    payload = json.dumps(
        {
            "principal_id": receipt.principal_id,
            "authority_ref": receipt.authority_ref,
            "credential_digest": receipt.credential_digest,
            "membership_version": receipt.membership_version,
        },
        sort_keys=True,
        allow_nan=False,
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Default implementation (single-user local authority)
# ---------------------------------------------------------------------------


class DefaultPrincipalAuthority:
    """Local, single-user principal authority.

    ``production=False`` (default) treats any registered local principal as
    valid and mints receipts for them; ``production=True`` rejects fixture-only
    identities (``principal:alice`` etc.) so they can never reach the
    production path.
    """

    def __init__(
        self,
        *,
        members: dict[str, tuple[str, str, int]] | None = None,
        fixture_only: frozenset[str] | None = None,
        production: bool = False,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    ) -> None:
        self._members = dict(members if members is not None else _DEFAULT_MEMBERS)
        self._fixture_only = (
            frozenset(fixture_only) if fixture_only is not None else _FIXTURE_ONLY_PRINCIPALS
        )
        self._production = production
        self._ttl_seconds = ttl_seconds

    def verify(
        self,
        principal_id: str,
        credential_ref: str,
        *,
        now: str,
    ) -> PrincipalAuthorityReceipt:
        if not isinstance(credential_ref, str) or not credential_ref:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_CREDENTIAL_MISMATCH,
                "credential_ref is required",
            )
        if _CREDENTIAL_REF_RE.match(credential_ref) is None:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_CREDENTIAL_MISMATCH,
                f"malformed credential_ref: {credential_ref!r}",
            )
        parts = credential_ref.split(":", 3)
        # parts = ["credential", kind, version, "<digest>"] where <digest> may itself contain ':'
        credential_kind, version_str, digest = parts[1], parts[2], parts[3]
        if _CREDENTIAL_DIGEST_RE.match(digest) is None:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_CREDENTIAL_MISMATCH,
                f"malformed credential digest: {digest!r}",
            )
        try:
            credential_version = int(version_str)
        except ValueError as exc:  # pragma: no cover - regex guards this
            raise AuthorityVerificationError(
                REASON_AUTHORITY_CREDENTIAL_MISMATCH,
                f"malformed credential version: {version_str!r}",
            ) from exc

        if self._production and principal_id in self._fixture_only:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_UNKNOWN,
                f"fixture-only principal {principal_id!r} rejected on production path",
            )

        member = self._members.get(principal_id)
        if member is None:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_UNKNOWN,
                f"unknown principal {principal_id!r}",
            )
        expected_kind, expected_digest, current_membership_version = member
        if credential_kind != expected_kind:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_CREDENTIAL_MISMATCH,
                f"credential kind mismatch for {principal_id!r}",
            )
        if credential_version < current_membership_version:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_VERSION_ROLLBACK,
                f"credential version rollback for {principal_id!r}",
            )
        if digest != expected_digest:
            raise AuthorityVerificationError(
                REASON_AUTHORITY_CREDENTIAL_MISMATCH,
                f"credential digest mismatch for {principal_id!r}",
            )

        authority_ref = f"authority:omo:v1:{principal_id}"
        now_dt = datetime.fromisoformat(now)
        expires_dt = now_dt + timedelta(seconds=self._ttl_seconds)
        receipt = PrincipalAuthorityReceipt(
            principal_id=principal_id,
            authority_ref=authority_ref,
            credential_digest=digest,
            membership_version=current_membership_version,
            verified_at=now,
            expires_at=expires_dt.isoformat(),
        )
        return receipt

    def is_receipt_expired(self, receipt: PrincipalAuthorityReceipt, *, now: str) -> bool:
        try:
            return datetime.fromisoformat(now) > datetime.fromisoformat(receipt.expires_at)
        except ValueError:  # pragma: no cover - malformed timestamp
            return True
