#!/usr/bin/env python3
from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

from .event_ledger.surface import EventLedgerSurface

# T10-58 reconciliation: the 6fe958c6 extraction committed this module with a
# truncated import block and no wiring back into omo_ledger's dispatcher.
# _emit_error/_emit_receipt stayed in omo_ledger; importing them here is safe
# because omo_ledger imports this module lazily (inside _subcommand_main).
from .omo_ledger import _emit_error, _emit_receipt


def _cmd_mandate_grant(surface: EventLedgerSurface, params: dict[str, Any], is_json: bool) -> int:
    """Grant a DelegationMandate. Local only, writes via broker."""
    from datetime import UTC, datetime, timedelta

    from ecos.ssot.mof.generated.control.mof_control_models import DelegationMandate

    from omo.sovereignty import (
        MandateError,
        MandateManager,
        SovereigntyService,
    )

    broker = surface.broker
    svc = SovereigntyService(broker)
    mgr = MandateManager(broker)

    principal_id = params["principal_id"]
    role_context_id = params["role_context_id"]

    # Get current assignment to snapshot versions
    assignment = svc.current_assignment(principal_id, role_context_id)
    if assignment is None or assignment.status != "active":
        _emit_error(
            {
                "ok": False,
                "error": f"role {role_context_id} not active for {principal_id}",
                "reason": "role_context_stale",
            },
            is_agora=False,
            is_json=is_json,
        )
        return 1

    resp = next(
        (r for r in assignment.responsibilities if r.resp_id == params["responsibility_id"]),
        None,
    )
    if resp is None:
        _emit_error(
            {
                "ok": False,
                "error": f"responsibility {params['responsibility_id']} not in assignment",
                "reason": "responsibility_stale",
            },
            is_agora=False,
            is_json=is_json,
        )
        return 1

    now = datetime.now(UTC)
    now_iso = now.isoformat()
    valid_from = params.get("valid_from") or now_iso
    expires_at = params.get("expires_at") or (now + timedelta(days=365)).isoformat()

    # Generate trace_id before Pydantic construction (can't pass "")
    import uuid as _uuid

    trace_id = _uuid.uuid4().hex[:24]

    from pydantic import ValidationError as PydanticValidationError

    try:
        mandate = DelegationMandate(
            mandate_id=params["mandate_id"],
            schema_version="delegation-mandate/v1",
            principal_id=principal_id,
            executor_id=params["executor_id"],
            episode_id=params["episode_id"],
            role_context_id=role_context_id,
            role_assignment_id=assignment.assignment_id,
            role_assignment_version=assignment.version,
            responsibility_id=params["responsibility_id"],
            responsibility_version=resp.version,
            purpose=params.get("purpose", "Granted via CLI"),
            capability_scope=params.get("capability") or [],
            autonomy_level=params["autonomy_level"],
            risk_ceiling=params["risk_ceiling"],
            approval_mode=params["approval_mode"],
            disclosure_policy=params["disclosure_policy"],
            valid_from=valid_from,
            expires_at=expires_at,
            budget_limit=float(params["budget_limit"]),
            budget_unit=params["budget_unit"],
            revocable=bool(params.get("revocable", False)),
            trace_id=trace_id,
            mandate_version=1,
            status="active",
        )
    except PydanticValidationError as exc:
        _emit_error(
            {"ok": False, "error": str(exc), "reason": "invalid_mandate_payload"},
            is_agora=False,
            is_json=is_json,
        )
        return 1

    try:
        granted = mgr.grant(mandate)
    except MandateError as exc:
        _emit_error(
            {"ok": False, "error": exc.message, "reason": exc.reason},
            is_agora=False,
            is_json=is_json,
        )
        return 1

    _emit_receipt(
        {
            "ok": True,
            "mandate_id": granted.mandate_id,
            "status": granted.status,
            "mandate_version": granted.mandate_version,
            "trace_id": granted.trace_id,
            "db_path": str(surface.db_path),
        },
        is_json,
        False,
    )
    return 0


def _cmd_mandate_revoke(surface: EventLedgerSurface, params: dict[str, Any], is_json: bool) -> int:
    """Revoke an active DelegationMandate. Local only, writes via broker."""
    from omo.sovereignty import MandateError, MandateManager

    mgr = MandateManager(surface.broker)

    # expected_version is required (no silent default)
    expected_version = params.get("expected_version")
    if expected_version is None:
        _emit_error(
            {
                "ok": False,
                "error": "--expected-version is required for mandate-revoke",
                "reason": "missing_expected_version",
            },
            is_agora=False,
            is_json=is_json,
        )
        return 1

    try:
        revoked = mgr.revoke(
            mandate_id=params["mandate_id"],
            principal_id=params["principal_id"],
            expected_version=int(expected_version),
        )
    except MandateError as exc:
        _emit_error(
            {"ok": False, "error": exc.message, "reason": exc.reason},
            is_agora=False,
            is_json=is_json,
        )
        return 1

    _emit_receipt(
        {
            "ok": True,
            "mandate_id": revoked.mandate_id,
            "status": revoked.status,
            "mandate_version": revoked.mandate_version,
            "trace_id": revoked.trace_id,
            "db_path": str(surface.db_path),
        },
        is_json,
        False,
    )
    return 0


def _cmd_mandate_admit(surface: EventLedgerSurface, params: dict[str, Any], is_json: bool) -> int:
    """Pure admission decision. Local only, read-only — exit 0 only for allow."""
    from omo.sovereignty import MandateError, MandateManager, MandateReplayError

    mgr = MandateManager(surface.broker)

    requested_budget = float(params.get("requested_budget", 0))

    # Non-finite (NaN/±inf) and negative requested budgets: default-deny
    # as budget_exceeded (H4).
    if not math.isfinite(requested_budget) or requested_budget < 0:
        _emit_error(
            {
                "ok": True,
                "allowed": False,
                "reason": "budget_exceeded",
            },
            is_agora=False,
            is_json=is_json,
        )
        return 1

    try:
        result = mgr.admit(
            mandate_id=params["mandate_id"],
            principal_id=params["principal_id"],
            executor_id=params["executor_id"],
            episode_id=params["episode_id"],
            role_context_id=params["role_context_id"],
            responsibility_id=params["responsibility_id"],
            capability=params["capability"],
            risk_level=params["risk_level"],
            requested_budget=requested_budget,
            budget_unit=params["budget_unit"],
            disclosure_policy=params["disclosure_policy"],
        )
    except MandateReplayError:
        # Corrupted replay → stable malformed_mandate_replay, never masquerade.
        _emit_error(
            {"ok": True, "allowed": False, "reason": "malformed_mandate_replay"},
            is_agora=False,
            is_json=is_json,
        )
        return 1
    except MandateError as exc:
        # Expected mandate error → report the real stable reason + message,
        # never masquerade as mandate_not_found.
        _emit_error(
            {
                "ok": True,
                "allowed": False,
                "reason": exc.reason,
                "error": exc.message,
            },
            is_agora=False,
            is_json=is_json,
        )
        return 1

    receipt = {
        "ok": True,
        "allowed": result.allowed,
        "reason": result.reason,
    }
    if result.mandate:
        receipt["mandate_id"] = result.mandate.mandate_id

    if result.allowed:
        _emit_receipt(receipt, is_json, False)
        return 0
    else:
        _emit_error(receipt, is_agora=False, is_json=is_json)
        return 1


# ---------------------------------------------------------------------------
# Receipt emission (CLI policy: maps ok/stderr/stdout)
# ---------------------------------------------------------------------------
