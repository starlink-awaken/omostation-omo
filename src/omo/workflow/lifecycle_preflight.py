"""Preflight and delivery-identity validation helpers for workflow lifecycle."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any

from .core import WORKSPACE, WorkflowError, registry_workspace_root, substitute, utc_now

_LEGACY_DELIVERY_IDENTITY_KEYS = ("spec_binding", "work_packet", "work_packet_hash")
_DELIVERY_IDENTITY_KEYS = (
    *_LEGACY_DELIVERY_IDENTITY_KEYS,
    "capability_requirements_digest",
    "capability_preflight",
)
_CAPABILITY_PREFLIGHT_OPTIONAL_KEYS = (
    "capability_requirements_digest",
    "capability_preflight",
)
_SHA256_REF_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PREFLIGHT_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:@/-]{1,256}$")
_PREFLIGHT_BINDING_KEYS = (
    "correlation_id",
    "workflow_run_id",
    "packet_id",
    "packet_hash",
    "assignment_id",
    "dispatch_id",
    "actor_id",
    "delivery_attempt_id",
)


_LEGACY_DELIVERY_IDENTITY_KEYS = ("spec_binding", "work_packet", "work_packet_hash")
_DELIVERY_IDENTITY_KEYS = (
    *_LEGACY_DELIVERY_IDENTITY_KEYS,
    "capability_requirements_digest",
    "capability_preflight",
)
_CAPABILITY_PREFLIGHT_OPTIONAL_KEYS = (
    "capability_requirements_digest",
    "capability_preflight",
)
_SHA256_REF_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PREFLIGHT_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:@/-]{1,256}$")
_PREFLIGHT_BINDING_KEYS = (
    "correlation_id",
    "workflow_run_id",
    "packet_id",
    "packet_hash",
    "assignment_id",
    "dispatch_id",
    "actor_id",
    "delivery_attempt_id",
)


def _load_spec_binding_contract() -> ModuleType:
    from .lifecycle import _load_spec_binding_contract as _load

    return _load()


def _prepare_bet_execution(bet_id: str) -> dict[str, Any]:
    contract = _load_spec_binding_contract()
    try:
        return contract.prepare_bet_execution(bet_id, workspace=WORKSPACE)
    except contract.SpecBindingContractError as exc:
        raise WorkflowError(str(exc)) from exc


def _validate_work_packet_claim(
    payload: dict[str, Any],
    paths: list[str],
    surfaces: list[str],
) -> None:
    if not payload.get("bet_id") and payload.get("work_packet") is None:
        return
    contract = _load_spec_binding_contract()
    try:
        contract.validate_work_packet_run(
            payload,
            paths,
            claimed_surfaces=surfaces,
            workspace=WORKSPACE,
        )
    except contract.SpecBindingContractError as exc:
        raise WorkflowError(str(exc)) from exc


def _validate_inherited_delivery_identity(
    bet_id: str,
    identity: dict[str, Any],
    *,
    parent_run_id: str,
) -> dict[str, Any]:
    identity_keys = set(identity)
    if not bet_id or identity_keys not in (
        set(_LEGACY_DELIVERY_IDENTITY_KEYS),
        set(_DELIVERY_IDENTITY_KEYS),
    ):
        raise WorkflowError(
            "WORK_PACKET_PARENT_BINDING_INCOMPLETE: parent must provide exact "
            "bet_id/spec_binding/work_packet/work_packet_hash, or the complete "
            "capability preflight identity"
        )
    packet = identity.get("work_packet")
    if not isinstance(packet, dict) or packet.get("spec_binding") != identity.get("spec_binding"):
        raise WorkflowError("WORK_PACKET_PARENT_BINDING_MISMATCH: parent spec_binding differs from work_packet")
    inherited = deepcopy(identity)
    _validate_work_packet_claim(
        {
            "run_id": parent_run_id,
            "bet_id": bet_id,
            **inherited,
        },
        [],
        [],
    )
    if identity_keys == set(_DELIVERY_IDENTITY_KEYS):
        _validate_capability_preflight(identity, parent_run_id)
    return inherited


def _delivery_identity_from_parent(parent_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    binding_keys = ("bet_id", *_DELIVERY_IDENTITY_KEYS)
    present = [key for key in binding_keys if key in parent_payload]
    if not present:
        raise WorkflowError("WORK_PACKET_PARENT_BINDING_REQUIRED: legacy unbound parent runs cannot create child runs")
    missing = [key for key in ("bet_id", *_LEGACY_DELIVERY_IDENTITY_KEYS) if key not in parent_payload]
    if missing:
        raise WorkflowError(
            f"WORK_PACKET_PARENT_BINDING_INCOMPLETE: parent run {parent_payload.get('run_id', '')} missing {missing}"
        )
    optional_present = [key for key in _CAPABILITY_PREFLIGHT_OPTIONAL_KEYS if key in parent_payload]
    if optional_present and len(optional_present) != len(_CAPABILITY_PREFLIGHT_OPTIONAL_KEYS):
        missing_optional = [key for key in _CAPABILITY_PREFLIGHT_OPTIONAL_KEYS if key not in parent_payload]
        raise WorkflowError(
            f"WORK_PACKET_PARENT_BINDING_INCOMPLETE: parent run {parent_payload.get('run_id', '')} missing {missing_optional}"
        )
    bet_id = parent_payload.get("bet_id")
    if not isinstance(bet_id, str) or not bet_id:
        raise WorkflowError("WORK_PACKET_PARENT_BINDING_INCOMPLETE: parent bet_id is required")
    identity_keys = _DELIVERY_IDENTITY_KEYS if optional_present else _LEGACY_DELIVERY_IDENTITY_KEYS
    identity = {key: parent_payload[key] for key in identity_keys}
    return bet_id, _validate_inherited_delivery_identity(
        bet_id,
        identity,
        parent_run_id=str(parent_payload.get("run_id") or ""),
    )


def resolve_parent_delivery_identity(
    registry: dict[str, Any],
    parent_run_id: str,
    requested_bet_id: str = "",
) -> tuple[str, dict[str, Any], str]:
    """Resolve one immutable parent identity before any child-side mutation."""
    _, parent_payload = _read_run(registry, parent_run_id)
    parent_bet_id, identity = _delivery_identity_from_parent(parent_payload)
    if requested_bet_id and requested_bet_id != parent_bet_id:
        raise WorkflowError(
            "WORK_PACKET_PARENT_BET_CONFLICT: requested "
            f"{requested_bet_id} but parent {parent_run_id} is bound to {parent_bet_id}"
        )
    return parent_bet_id, identity, str(parent_payload.get("agent_profile") or "")


def _preflight_error(reason: str) -> WorkflowError:
    return WorkflowError(f"CAPABILITY_PREFLIGHT_{reason}")


def _required_preflight_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or any(ord(char) < 32 for char in value):
        raise _preflight_error(f"{field.upper()}_INVALID")
    return value


def _required_preflight_identifier(value: Any, field: str) -> str:
    text = _required_preflight_text(value, field)
    if (
        _PREFLIGHT_IDENTIFIER_RE.fullmatch(text) is None
        or ".." in text
        or text.startswith(("/", "~", "\\"))
        or re.match(r"^[A-Za-z]:[\\/]", text)
    ):
        raise _preflight_error(f"{field.upper()}_INVALID")
    return text


def _validate_capability_preflight(identity: Mapping[str, Any], run_id: str) -> dict[str, Any]:
    """Validate the provider's privacy-safe, non-executing start receipt."""
    digest = identity.get("capability_requirements_digest")
    if not isinstance(digest, str) or _SHA256_REF_RE.fullmatch(digest) is None:
        raise _preflight_error("REQUIREMENTS_DIGEST_INVALID")
    packet = identity.get("work_packet")
    if not isinstance(packet, Mapping):
        raise _preflight_error("WORK_PACKET_INVALID")
    requirements = packet.get("capability_requirements")
    if not isinstance(requirements, list):
        raise _preflight_error("REQUIREMENTS_INVALID")
    requirement_ids: list[str] = []
    for requirement in requirements:
        if not isinstance(requirement, Mapping) or set(requirement) != {"capability_id", "operation", "effect"}:
            raise _preflight_error("REQUIREMENTS_INVALID")
        capability_id = _required_preflight_text(requirement.get("capability_id"), "capability_id")
        _required_preflight_text(requirement.get("operation"), "operation")
        _required_preflight_text(requirement.get("effect"), "effect")
        if capability_id in requirement_ids:
            raise _preflight_error("REQUIREMENTS_INVALID")
        requirement_ids.append(capability_id)

    preflight = identity.get("capability_preflight")
    if not isinstance(preflight, Mapping):
        raise _preflight_error("RESULT_INVALID")
    if set(preflight) != {
        "requirements_digest",
        "binding",
        "receipts",
        "invoked",
        "value_indicator_policy",
    }:
        raise _preflight_error("RESULT_INVALID")
    if preflight["requirements_digest"] != digest:
        raise _preflight_error("REQUIREMENTS_DIGEST_MISMATCH")
    if preflight["invoked"] is not False or preflight["value_indicator_policy"] is not False:
        raise _preflight_error("EXECUTION_OR_VALUE_POLICY")

    binding = preflight["binding"]
    if not isinstance(binding, Mapping) or set(binding) != set(_PREFLIGHT_BINDING_KEYS):
        raise _preflight_error("BINDING_INVALID")
    for field in _PREFLIGHT_BINDING_KEYS:
        if field == "packet_hash":
            _required_preflight_text(binding.get(field), field)
        else:
            _required_preflight_identifier(binding.get(field), field)
    if binding["correlation_id"] != run_id or binding["workflow_run_id"] != run_id:
        raise _preflight_error("BINDING_MISMATCH")
    if binding["packet_id"] != packet.get("packet_id"):
        raise _preflight_error("BINDING_MISMATCH")
    if binding["packet_hash"] != identity.get("work_packet_hash"):
        raise _preflight_error("BINDING_MISMATCH")
    if binding["assignment_id"] != f"preflight:{run_id}:assignment":
        raise _preflight_error("BINDING_MISMATCH")
    if binding["dispatch_id"] != f"preflight:{run_id}:dispatch":
        raise _preflight_error("BINDING_MISMATCH")
    if _SHA256_REF_RE.fullmatch(binding["packet_hash"]) is None:
        raise _preflight_error("BINDING_PACKET_HASH_INVALID")

    receipts = preflight["receipts"]
    if not isinstance(receipts, list) or len(receipts) != len(requirement_ids):
        raise _preflight_error("RECEIPTS_INVALID")
    redacted_receipts: list[dict[str, str]] = []
    for expected_id, receipt in zip(requirement_ids, receipts):
        if not isinstance(receipt, Mapping) or set(receipt) != {"capability_id", "source_digest", "receipt_digest"}:
            raise _preflight_error("RECEIPTS_INVALID")
        if receipt["capability_id"] != expected_id:
            raise _preflight_error("RECEIPTS_ORDER_INVALID")
        source_digest = receipt["source_digest"]
        receipt_digest = receipt["receipt_digest"]
        if (
            not isinstance(source_digest, str)
            or not isinstance(receipt_digest, str)
            or _SHA256_REF_RE.fullmatch(source_digest) is None
            or _SHA256_REF_RE.fullmatch(receipt_digest) is None
        ):
            raise _preflight_error("RECEIPTS_DIGEST_INVALID")
        redacted_receipts.append(
            {
                "capability_id": expected_id,
                "source_digest": str(source_digest),
                "receipt_digest": str(receipt_digest),
            }
        )
    return {
        "requirements_digest": digest,
        "binding": {field: binding[field] for field in _PREFLIGHT_BINDING_KEYS},
        "receipts": redacted_receipts,
        "invoked": False,
        "value_indicator_policy": False,
    }


def _complete_fresh_delivery_identity(
    identity: dict[str, Any],
    run_id: str,
    start_preflight: Callable[[str, dict[str, Any]], Mapping[str, Any]] | None,
) -> dict[str, Any]:
    identity_keys = set(identity) & set(_DELIVERY_IDENTITY_KEYS)
    unexpected_keys = set(identity) - set(_DELIVERY_IDENTITY_KEYS) - {"instruction_binding"}
    if unexpected_keys:
        raise _preflight_error("IDENTITY_INVALID")
    legacy_keys = set(_LEGACY_DELIVERY_IDENTITY_KEYS)
    exact_keys = set(_DELIVERY_IDENTITY_KEYS)
    prepared_keys = legacy_keys | {"capability_requirements_digest"}
    if identity_keys == legacy_keys:
        return identity
    if identity_keys not in (prepared_keys, exact_keys):
        raise _preflight_error("IDENTITY_INVALID")
    if start_preflight is None:
        raise _preflight_error("PROVIDER_UNAVAILABLE")
    try:
        result = start_preflight(run_id, deepcopy(identity))
    except Exception as exc:  # noqa: BLE001 - injected provider is a mandatory gate.
        raise _preflight_error("PROVIDER_FAILED") from exc
    completed = deepcopy(identity)
    completed["capability_preflight"] = dict(result) if isinstance(result, Mapping) else result
    validated = _validate_capability_preflight(completed, run_id)
    completed["capability_preflight"] = validated
    return completed





def _read_run(registry: dict[str, Any], run_id: str) -> tuple[Path, dict[str, Any]]:
    from .lifecycle import read_run

    return read_run(registry, run_id)
