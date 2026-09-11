from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import pwd
import re
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

try:
    from .mesh_agent_events import emit_workflow_mesh_event
except ImportError:  # graceful degradation during refactoring

    def emit_workflow_mesh_event(
        event_type: str,
        run_id: str,
        payload: dict[str, Any] | None = None,
        *,
        workspace: Any = None,
        scene_binding: dict[str, str] | None = None,
    ) -> bool:
        return False


try:
    from .scene_bridge import extract_scene_binding
except ImportError:

    def extract_scene_binding(
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, str] | None:
        return None


from ..omo_io import write_yaml_atomic
from .affected_graph_receipt import validate_affected_graph_receipt
from .core import (
    CLAIM_POLICY_MODES,
    RUN_UPDATE_LOCK_TIMEOUT_SECONDS,
    WORKSPACE,
    WorkflowError,
    command_display,
    display_path,
    ledger_path,
    lock_state_dir,
    normalize_repo_path,
    path_matches,
    registry_workspace_root,
    run_state_dir,
    substitute,
    utc_now,
    validate_agent_profile,
    workflow_by_id,
)

_LOCK_FILENAME_MAX_LEN = 255
_RUN_UPDATE_LOCK_NAME_MAX_LEN = _LOCK_FILENAME_MAX_LEN - len("run_.update.lock")
_PATH_LOCK_NAME_MAX_LEN = _LOCK_FILENAME_MAX_LEN - len(".lock.yaml")
_SPEC_BINDING_CONTRACT: ModuleType | None = None
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

_CLAIMS_AUTHORITY_TIMEOUT_SECONDS = 5.0
_CLAIMS_AUTHORITY_PROCESS_NONCE = uuid.uuid4().hex


def _call_claims_authority(verb: str, request: Mapping[str, Any] | None) -> dict[str, Any]:
    """Call only the OS-account integration-root stdio authority entry."""
    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    integration_root = account_home / "Workspace"
    managed_python = integration_root / "bin/gac/managed-python"
    runner = integration_root / "bin/agent-workflow.py"
    command = [
        str(managed_python),
        "run",
        "--profile",
        "stdlib",
        "--",
        str(runner),
        "claims-authority",
        verb,
    ]
    encoded: str | None
    if verb == "status":
        if request is not None:
            raise WorkflowError("REQUEST_SCHEMA_INVALID: status accepts no request body")
        command.append("--json")
        encoded = None
    else:
        if request is None:
            raise WorkflowError("REQUEST_SCHEMA_INVALID: mutation request is required")
        command.extend(("--request-json", "-"))
        encoded = json.dumps(
            request,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    try:
        completed = subprocess.run(
            command,
            cwd=integration_root,
            input=encoded,
            text=True,
            capture_output=True,
            check=False,
            timeout=_CLAIMS_AUTHORITY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkflowError("AUTHORITY_UNAVAILABLE") from exc
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if completed.returncode != 0 or len(lines) != 1:
        raise WorkflowError("AUTHORITY_UNAVAILABLE")
    try:
        response = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise WorkflowError("AUTHORITY_UNAVAILABLE") from exc
    if not isinstance(response, dict):
        raise WorkflowError("AUTHORITY_UNAVAILABLE")
    if response.get("schema") == "claims-authority-response/v2":
        if response.get("ok") is not True or not isinstance(response.get("result"), dict):
            error = response.get("error")
            code = error.get("code") if isinstance(error, Mapping) else "AUTHORITY_UNAVAILABLE"
            raise WorkflowError(str(code or "AUTHORITY_UNAVAILABLE"))
        return dict(response["result"])
    return response


def _authority_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _authority_digest(value: Any) -> str:
    preimage = (
        {key: item for key, item in value.items() if key not in {"digest", "signature"}}
        if isinstance(value, Mapping)
        else value
    )
    return f"sha256:{hashlib.sha256(_authority_json(preimage).encode('utf-8')).hexdigest()}"


def _authority_file_digest(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def _authority_fixed_paths() -> dict[str, Path]:
    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    authority_dir = account_home / "agents/_shared/runtime/omo-claims-authority-r0"
    return {
        "account_home": account_home,
        "authority_dir": authority_dir,
        "store": authority_dir / "store.sqlite3",
        "high_water": authority_dir / "high-water.json",
        "backups": authority_dir / "backups",
        "witness": authority_dir / "activation-witness.json",
    }


def _authority_safe_existing(path: Path) -> None:
    if not os.path.lexists(path):
        return
    try:
        info = path.lstat()
    except OSError as exc:
        raise WorkflowError("AUTHORITY_STORE_UNSAFE") from exc
    if stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o022:
        raise WorkflowError("AUTHORITY_STORE_UNSAFE")


def _authority_witness_state() -> dict[str, Any]:
    paths = _authority_fixed_paths()
    for key in ("authority_dir", "store", "high_water", "backups", "witness"):
        _authority_safe_existing(paths[key])
    witness_path = paths["witness"]
    initialized = paths["store"].exists() or paths["high_water"].exists() or paths["backups"].exists()
    if not witness_path.is_file():
        if initialized or os.path.lexists(witness_path):
            raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")
        return {"activation_state": "unactivated", "code": "not_activated", "witness_state": "pristine"}
    try:
        witness = json.loads(witness_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID") from exc
    if not isinstance(witness, dict):
        raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")
    expected_digest = _authority_digest(
        {key: value for key, value in witness.items() if key not in {"digest", "signature"}}
    )
    if (
        witness.get("schema") != "claims-activation-witness/v1"
        or witness.get("authority_id") != "omo-claims-authority-r0"
        or witness.get("digest") != expected_digest
    ):
        raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")
    sequence = witness.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")
    state = witness.get("state")
    if state == "unactivated":
        if (
            sequence != 0
            or witness.get("descriptor_digest") is not None
            or witness.get("activation_receipt_digest") is not None
        ):
            raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")
        if paths["high_water"].exists():
            try:
                high_water = json.loads(paths["high_water"].read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID") from exc
            if (
                not isinstance(high_water, dict)
                or high_water.get("schema") != "claims-authority-high-water/v1"
                or high_water.get("authority_id") != "omo-claims-authority-r0"
                or high_water.get("digest") != _authority_digest(high_water)
                or high_water.get("sequence") != 0
                or high_water.get("receipt_digest") is not None
                or high_water.get("descriptor_digest") is not None
            ):
                raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")
        return {"activation_state": "unactivated", "code": "not_activated", "witness_state": state}
    if state in {"prepared", "shadow-active"}:
        return {"activation_state": state, "code": "witness_requires_broker", "witness_state": state}
    raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")


def _authority_run_is_eligible(payload: Mapping[str, Any]) -> bool:
    return (
        bool(payload.get("run_id"))
        and bool(payload.get("actor"))
        and str(payload.get("bet_id") or "").startswith("BET-")
        and isinstance(payload.get("spec_binding"), Mapping)
        and isinstance(payload.get("work_packet"), Mapping)
        and isinstance(payload.get("claims"), list)
        and bool(payload.get("work_packet_hash"))
    )


def _authority_mode(payload: Mapping[str, Any]) -> str:
    if not _authority_run_is_eligible(payload):
        return "ineligible"
    try:
        status = _call_claims_authority("status", None)
    except WorkflowError:
        witness = _authority_witness_state()
        if witness["activation_state"] == "unactivated":
            return "unactivated"
        raise WorkflowError("AUTHORITY_UNAVAILABLE")
    witness = _authority_witness_state()
    if status.get("activation_state") == "unactivated" and witness["activation_state"] == "unactivated":
        return "unactivated"
    if status.get("activation_state") == "shadow-active" and witness["activation_state"] == "shadow-active":
        return "shadow-active"
    raise WorkflowError("AUTHORITY_ACTIVATION_WITNESS_INVALID")


def _record_authority_shadow_event(
    registry: dict[str, Any],
    run_id: str,
    operation: str,
    event: str,
    *,
    code: str,
    receipt: Mapping[str, Any] | None = None,
) -> None:
    # Wave A observation channel is the ledger event (shadow_observed /
    # shadow_unprovable). Spec §6.4 return sibling claims_authority_shadow is
    # owned by root agent-clone (Wave B); lifecycle keeps v1 return bytes intact.
    payload: dict[str, Any] = {
        "event": event,
        "run_id": run_id,
        "operation": operation,
        "code": code,
        "effective_claim_authority": "v1",
        "instruction_capable": False,
    }
    if receipt is not None:
        payload.update(
            {
                "authority_sequence": receipt.get("sequence"),
                "authority_receipt_digest": receipt.get("receipt_digest"),
                "mutation_batch_id": receipt.get("mutation_batch_id"),
            }
        )
    append_ledger_event(registry, payload)


def _authority_resolve_lock_path(registry: dict[str, Any], raw: str) -> Path:
    workspace = registry_workspace_root(registry)
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = workspace / candidate
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(lock_state_dir(registry).resolve())
    except ValueError as exc:
        raise WorkflowError(f"lock path escapes lock directory: {raw}") from exc
    return resolved


def _authority_snapshot(registry: dict[str, Any], run_id: str) -> dict[str, Any]:
    run_path, payload = read_run(registry, run_id)
    run_digest = _authority_file_digest(run_path)
    raw_locks = payload.get("locks")
    if raw_locks is None:
        raw_locks = []
    if not isinstance(raw_locks, list):
        raise WorkflowError(f"run {run_id} locks must be a list, got {type(raw_locks).__name__}")
    for item in raw_locks:
        if not isinstance(item, str) or not item:
            raise WorkflowError(f"run {run_id} locks contains invalid entry: {item!r}")
    requested_locks = {str(item) for item in raw_locks if isinstance(item, str) and item}
    lock_dir = lock_state_dir(registry)
    if lock_dir.exists():
        for candidate in lock_dir.glob("*.lock.yaml"):
            try:
                parsed = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
            except (OSError, UnicodeError, yaml.YAMLError):
                parsed = {}
            if isinstance(parsed, dict) and parsed.get("run_id") == run_id:
                requested_locks.add(display_path(candidate))
    lock_records: list[dict[str, Any]] = []
    resolved_locks = {
        str(_authority_resolve_lock_path(registry, raw)): _authority_resolve_lock_path(registry, raw)
        for raw in requested_locks
    }
    for lock_path in sorted(resolved_locks.values(), key=str):
        exists = lock_path.is_file()
        lock_records.append(
            {
                "path": display_path(lock_path),
                "exists": exists,
                "content_digest": _authority_file_digest(lock_path) if exists else None,
            }
        )
    return {
        "payload": payload,
        "run_digest": run_digest,
        "lock_set_digest": _authority_digest(lock_records),
        "lock_records": lock_records,
    }


def _authority_read_json(path: Path, *, code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkflowError(code) from exc
    if not isinstance(payload, dict):
        raise WorkflowError(code)
    return payload


def _authority_head_oid(workspace: Path, identity: Mapping[str, Any]) -> str:
    head = workspace / ".git/HEAD"
    try:
        value = head.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise WorkflowError("IDENTITY_MISMATCH") from exc
    if re.fullmatch(r"[0-9a-f]{40}", value):
        return value
    if not value.startswith("ref: "):
        raise WorkflowError("IDENTITY_MISMATCH")
    ref = value.removeprefix("ref: ")
    ref_path = workspace / ".git" / ref
    if ref_path.is_file():
        candidate = ref_path.read_text(encoding="utf-8").strip()
        if re.fullmatch(r"[0-9a-f]{40}", candidate):
            return candidate
    packed = workspace / ".git/packed-refs"
    if packed.is_file():
        for line in packed.read_text(encoding="utf-8").splitlines():
            parts = line.split(" ", 1)
            if len(parts) == 2 and parts[1] == ref and re.fullmatch(r"[0-9a-f]{40}", parts[0]):
                return parts[0]
    fallback = str(identity.get("frozen_root_sha") or "")
    if re.fullmatch(r"[0-9a-f]{40}", fallback):
        return fallback
    raise WorkflowError("IDENTITY_MISMATCH")


def _authority_sha_ref(value: Any, *, code: str) -> str:
    candidate = str(value or "")
    if candidate.startswith("sha256:") and re.fullmatch(r"sha256:[0-9a-f]{64}", candidate):
        return candidate
    if re.fullmatch(r"[0-9a-f]{64}", candidate):
        return f"sha256:{candidate}"
    raise WorkflowError(code)


def _authority_observe_members(
    registry: dict[str, Any],
    snapshot: Mapping[str, Any],
) -> list[dict[str, Any]]:
    base = _authority_envelope_identity(registry, snapshot)
    payload = snapshot["payload"]
    members: list[dict[str, Any]] = []
    claims = payload.get("claims")
    if not isinstance(claims, list):
        raise WorkflowError("CLAIM_SCOPE_VIOLATION")
    for ordinal, claim in enumerate(claims):
        if not isinstance(claim, Mapping):
            raise WorkflowError("CLAIM_SCOPE_VIOLATION")
        affected = claim.get("affected_graph")
        affected_hash = affected.get("receipt_hash") if isinstance(affected, Mapping) else None
        request = {
            **base,
            "operation": "observe-claim",
            "request_id": str(uuid.uuid4()),
            "expected_claim_version": 0,
            "v1_decision": {"decision": "deny", "code": "claims_authority_mismatch"},
            "authority_mode": "shadow",
            "clone_identity_schema": str(base["clone_identity_schema"]),
            "claim_ordinal": ordinal,
            "v1_claim_digest": _authority_digest(claim),
            "v1_run_digest": snapshot["run_digest"],
            "v1_lock_set_digest": snapshot["lock_set_digest"],
            "requested_paths_digest": _authority_digest(
                {
                    "paths": sorted(str(item) for item in claim.get("paths", [])),
                    "surfaces": sorted(str(item) for item in claim.get("surfaces", [])),
                }
            ),
            "affected_graph_digest": _authority_sha_ref(
                affected_hash,
                code="AFFECTED_GRAPH_MISMATCH",
            ),
        }
        receipt = _call_claims_authority("observe-claim", request)
        members.append(
            {
                "claim_id": str(receipt.get("claim_id") or ""),
                "claim_version": int(receipt.get("claim_version", -1)),
                "lease_epoch": int(receipt.get("lease_epoch", -1)),
            }
        )
    return sorted(members, key=lambda item: item["claim_id"])


def _authority_envelope_identity(
    registry: dict[str, Any],
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    payload = snapshot["payload"]
    workspace = registry_workspace_root(registry)
    identity = _authority_read_json(workspace / ".git/agent-clone-identity.json", code="IDENTITY_MISMATCH")
    readiness = _authority_read_json(workspace / ".git/agent-clone-readiness.json", code="IDENTITY_MISMATCH")
    provenance = _authority_read_json(workspace / ".git/agent-clone-provenance.json", code="IDENTITY_MISMATCH")
    if Path(str(identity.get("canonical_root") or "")).resolve() != workspace.resolve():
        raise WorkflowError("IDENTITY_MISMATCH")
    repository = str(provenance.get("repository", {}).get("canonical_repository") or "")
    repository = repository.removeprefix("github.com/")
    spec_binding = payload["spec_binding"]
    work_packet = payload["work_packet"]
    claims = payload.get("claims")
    if not isinstance(claims, list):
        raise WorkflowError("CLAIM_SCOPE_VIOLATION")
    affected_hashes = []
    requested = []
    for claim in claims:
        if not isinstance(claim, Mapping):
            raise WorkflowError("CLAIM_SCOPE_VIOLATION")
        affected = claim.get("affected_graph")
        affected_hashes.append(
            _authority_sha_ref(
                affected.get("receipt_hash") if isinstance(affected, Mapping) else None,
                code="AFFECTED_GRAPH_MISMATCH",
            )
        )
        requested.append(
            {
                "paths": sorted(str(item) for item in claim.get("paths", [])),
                "surfaces": sorted(str(item) for item in claim.get("surfaces", [])),
            }
        )
    return {
        "schema": "claim-mutation-envelope/v2",
        "authority_id": "omo-claims-authority-r0",
        "actor_id": str(identity.get("actor_id") or ""),
        "delivery_attempt_id": str(identity.get("delivery_attempt_id") or ""),
        "repository": repository,
        "clone_root_digest": _authority_digest({"clone_root": str(workspace.resolve())}),
        "branch": str(provenance.get("working_branch") or ""),
        "clone_identity_digest": _authority_digest(identity),
        "manifest_digest": _authority_digest(identity.get("transport", {})),
        "readiness_digest": _authority_sha_ref(readiness.get("receipt_digest"), code="IDENTITY_MISMATCH"),
        "frozen_base": str(identity.get("frozen_root_sha") or ""),
        "head_oid": _authority_head_oid(workspace, identity),
        "bet_id": str(payload.get("bet_id") or ""),
        "work_packet_id": str(work_packet.get("packet_id") or ""),
        "work_packet_digest": _authority_sha_ref(payload.get("work_packet_hash"), code="WORK_PACKET_UNBOUND"),
        "spec_ref": str(spec_binding.get("spec_ref") or ""),
        "spec_digest": _authority_sha_ref(spec_binding.get("content_digest"), code="WORK_PACKET_UNBOUND"),
        "run_id": str(payload.get("run_id") or ""),
        "requested_paths_digest": _authority_digest(requested),
        "affected_graph_digest": _authority_digest(affected_hashes),
        "clone_identity_schema": str(identity.get("schema") or ""),
    }


def _authority_begin_mutation(
    registry: dict[str, Any],
    run_id: str,
    operation: str,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    members = _authority_observe_members(registry, snapshot)
    process_identity = _authority_digest(
        {
            "process_nonce": _CLAIMS_AUTHORITY_PROCESS_NONCE,
            "run_id": run_id,
            "operation": operation,
        }
    )
    request = {
        **_authority_envelope_identity(registry, snapshot),
        "request_id": str(uuid.uuid4()),
        "operation": operation,
        "run_digest": snapshot["run_digest"],
        "lock_set_digest": snapshot["lock_set_digest"],
        "members": members,
        "mutation_process_identity_digest": process_identity,
    }
    return _call_claims_authority("begin-claim-mutation", request)


def _authority_settle_mutation(
    registry: dict[str, Any],
    run_id: str,
    operation: str,
    begin: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    outcome: str,
) -> dict[str, Any]:
    if operation == "claim" and outcome == "applied":
        _authority_observe_members(registry, snapshot)
    request = {
        **_authority_envelope_identity(registry, snapshot),
        "request_id": str(begin["settlement_request_id"]),
        "operation": "settle-claim-mutation",
        "mutation_batch_id": begin["mutation_batch_id"],
        "outcome": outcome,
        "resulting_run_digest": snapshot["run_digest"],
        "resulting_lock_set_digest": snapshot["lock_set_digest"],
        "members": begin["members"],
        "mutation_process_identity_digest": begin["mutation_process_identity_digest"],
    }
    return _call_claims_authority("settle-claim-mutation", request)


def _authority_mark_mutation_operator_required(
    begin: Mapping[str, Any],
    *,
    reason_code: str,
) -> dict[str, Any]:
    request = {
        "schema": "claim-mutation-envelope/v2",
        "request_id": str(uuid.uuid4()),
        "authority_id": "omo-claims-authority-r0",
        "operation": "mark-claim-mutation-operator-required",
        "mutation_batch_id": begin["mutation_batch_id"],
        "reason_code": reason_code,
        "mutation_process_identity_digest": begin["mutation_process_identity_digest"],
    }
    return _call_claims_authority("mark-claim-mutation-operator-required", request)


@contextmanager
def _authority_mutation_locked(
    registry: dict[str, Any],
    run_id: str,
    operation: str,
    *,
    force_requested: bool = False,
):
    before = _authority_snapshot(registry, run_id)
    mode = _authority_mode(before["payload"])
    if mode in {"legacy", "ineligible"}:
        yield
        return
    if mode == "unactivated":
        yield
        _record_authority_shadow_event(
            registry,
            run_id,
            operation,
            "shadow_unprovable",
            code="not_activated",
        )
        return
    if force_requested:
        raise WorkflowError("CLAIM_VERSION_STALE: force is forbidden after authority activation")
    begin = _authority_begin_mutation(registry, run_id, operation, before)
    revalidated = _authority_snapshot(registry, run_id)
    if revalidated["run_digest"] != before["run_digest"] or revalidated["lock_set_digest"] != before["lock_set_digest"]:
        settlement = _authority_settle_mutation(
            registry,
            run_id,
            operation,
            begin,
            revalidated,
            outcome="rejected",
        )
        raise WorkflowError("CLAIM_VERSION_STALE")
    try:
        yield
    except BaseException as original_error:
        after_error = _authority_snapshot(registry, run_id)
        unchanged = (
            after_error["run_digest"] == before["run_digest"]
            and after_error["lock_set_digest"] == before["lock_set_digest"]
        )
        try:
            settlement = _authority_settle_mutation(
                registry,
                run_id,
                operation,
                begin,
                after_error,
                outcome="rejected" if unchanged else "unknown",
            )
        except BaseException:
            marker = None
            if not unchanged:
                try:
                    marker = _authority_mark_mutation_operator_required(
                        begin,
                        reason_code="SETTLEMENT_RESULT_UNKNOWN",
                    )
                except WorkflowError:
                    pass
            try:
                _record_authority_shadow_event(
                    registry,
                    run_id,
                    operation,
                    "shadow_unprovable",
                    code="settlement_result_unknown",
                    receipt=marker,
                )
            except Exception:
                pass
            raise original_error
        if not unchanged:
            try:
                _authority_mark_mutation_operator_required(
                    begin,
                    reason_code="MUTATION_PROCESS_RESULT_UNKNOWN",
                )
            except WorkflowError:
                pass
        _record_authority_shadow_event(
            registry,
            run_id,
            operation,
            "shadow_unprovable" if not unchanged else "shadow_observed",
            code="mutation_unknown" if not unchanged else "v1_rejected",
            receipt=settlement,
        )
        raise
    after = _authority_snapshot(registry, run_id)
    try:
        settlement = _authority_settle_mutation(
            registry,
            run_id,
            operation,
            begin,
            after,
            outcome="applied",
        )
    except BaseException:
        marker = None
        try:
            marker = _authority_mark_mutation_operator_required(
                begin,
                reason_code="SETTLEMENT_RESULT_UNKNOWN",
            )
        except WorkflowError:
            pass
        try:
            _record_authority_shadow_event(
                registry,
                run_id,
                operation,
                "shadow_unprovable",
                code="settlement_result_unknown",
                receipt=marker,
            )
        except Exception:
            pass
        raise
    _record_authority_shadow_event(
        registry,
        run_id,
        operation,
        "shadow_observed",
        code="v1_applied",
        receipt=settlement,
    )


def admit_agent_workflow_start(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Load the exact admission bridge only for a qualifying root start."""
    from ..workflow_dispatch import admit_agent_workflow_start as _admit

    return _admit(*args, **kwargs)


def close_agent_workflow_run(*args: Any, **kwargs: Any) -> bool:
    """Load the exact closeout bridge only when lifecycle closeout needs it."""
    from ..workflow_dispatch import close_agent_workflow_run as _close

    return _close(*args, **kwargs)


def _fail_exact_agent_workflow_start(
    registry: dict[str, Any],
    record: dict[str, Any],
    error: BaseException,
    *,
    request_persisted: bool,
) -> None:
    run_id = str(record["run_id"])
    evidence = f"WORKFLOW_MESH_ADMISSION_FAILED: {type(error).__name__}"
    workspace = registry_workspace_root(registry)
    durable_request = False
    try:
        from ..workflow_mesh import WorkflowMeshStore

        durable_request = any(
            event.get("workflow_run_id") == run_id and event.get("event_type") == "WorkflowRequested"
            for event in WorkflowMeshStore(workspace / ".omo").events()
        )
    except Exception:
        durable_request = request_persisted
    close_run(registry, run_id, "failed", [evidence], True, emit_mesh=False)
    if durable_request:
        cancelled = emit_workflow_mesh_event(
            "WorkflowCancelled",
            run_id,
            {"status": "failed", "ok": False, "error": evidence},
            workspace=workspace,
        )
        closed = emit_workflow_mesh_event(
            "WorkflowClosed",
            run_id,
            {"status": "failed", "ok": False, "error": evidence},
            workspace=workspace,
        )
        if not cancelled or not closed:
            raise WorkflowError("WORKFLOW_MESH_ADMISSION_CLEANUP_FAILED: exact request was not closed") from error


def _is_exact_agent_workflow_request(root: Path, workflow_run_id: str, *, omo_dir: str = ".omo") -> bool:
    """Inspect the lightweight persisted request before importing the exact closer."""
    from ..workflow_mesh import EXACT_REQUEST_DISCRIMINATOR, WorkflowMeshStore

    for event in WorkflowMeshStore(root / omo_dir).events():
        if event.get("workflow_run_id") == workflow_run_id and event.get("event_type") == "WorkflowRequested":
            return event.get("payload", {}).get("exact_request_discriminator") == EXACT_REQUEST_DISCRIMINATOR
    return False


def _load_spec_binding_contract() -> ModuleType:
    """Load the Workspace-owned BET/WorkPacket boundary or fail closed."""
    global _SPEC_BINDING_CONTRACT
    if _SPEC_BINDING_CONTRACT is not None:
        return _SPEC_BINDING_CONTRACT
    path = WORKSPACE / "bin/plan/bet-ledger.py"
    spec = importlib.util.spec_from_file_location("_omo_spec_binding_contract", path)
    if spec is None or spec.loader is None:
        raise WorkflowError(f"SPEC_BINDING_UNAVAILABLE: cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - a broken mandatory gate must halt.
        sys.modules.pop(spec.name, None)
        raise WorkflowError(f"SPEC_BINDING_UNAVAILABLE: cannot load {path}: {exc}") from exc
    _SPEC_BINDING_CONTRACT = module
    return module


from .lifecycle_preflight import (
    _complete_fresh_delivery_identity,
    _delivery_identity_from_parent,
    _preflight_error,
    _prepare_bet_execution,
    _required_preflight_identifier,
    _required_preflight_text,
    _validate_capability_preflight,
    _validate_inherited_delivery_identity,
    _validate_work_packet_claim,
    resolve_parent_delivery_identity,
)


def workflow_plan(workflow: dict[str, Any], context: dict[str, str]) -> dict[str, Any]:
    resolved = substitute(workflow, context)
    return {
        "id": resolved["id"],
        "title": resolved.get("title", ""),
        "purpose": resolved.get("purpose", ""),
        "agents": resolved.get("agents", {}),
        "allowed_lanes": resolved.get("allowed_lanes", []),
        "lock_scopes": resolved.get("lock_scopes", []),
        "phases": resolved.get("phases", {}),
    }


def print_plan(plan: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return
    print(f"{plan['id']} — {plan['title']}")
    print(plan["purpose"])
    roles = plan.get("agents", {}).get("roles") or []
    if roles:
        print(f"agents: {', '.join(roles)}")
    print(f"lanes: {', '.join(plan['allowed_lanes'])}")
    print(f"locks: {', '.join(plan['lock_scopes'])}")
    for phase, entries in plan["phases"].items():
        print(f"\n[{phase}]")
        for item in entries:
            mode = item.get("mode", "?")
            cwd = item.get("cwd")
            prefix = f"({mode})"
            if cwd:
                prefix += f" cwd={cwd}"
            print(f"  {item.get('id')}: {prefix} {command_display(item['command'])}")


def run_stage(
    workflow: dict[str, Any],
    stage: str,
    context: dict[str, str],
    execute: bool,
    as_json: bool,
) -> int:
    plan = workflow_plan(workflow, context)
    entries = plan["phases"].get(stage)
    if not entries:
        raise WorkflowError(f"{plan['id']} has no stage: {stage}")

    results: list[dict[str, Any]] = []
    for item in entries:
        mode = item.get("mode")
        command = item["command"]
        cwd = WORKSPACE / item.get("cwd", ".")
        skipped = mode == "manual" or not execute
        result: dict[str, Any] = {
            "id": item.get("id"),
            "mode": mode,
            "command": command_display(command),
            "cwd": str(cwd.relative_to(WORKSPACE)) if cwd.is_relative_to(WORKSPACE) else str(cwd),
            "skipped": skipped,
            "ok": True,
        }
        if not skipped:
            completed = subprocess.run(command, cwd=cwd, check=False)
            result["returncode"] = completed.returncode
            result["ok"] = completed.returncode == 0 or mode == "advisory"
        results.append(result)

    report = {
        "workflow": plan["id"],
        "stage": stage,
        "execute": execute,
        "results": results,
    }
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for result in results:
            status = "SKIP" if result["skipped"] else ("PASS" if result["ok"] else "FAIL")
            print(f"[{status}] {result['id']} :: {result['command']}")
    return 0 if all(item["ok"] for item in results) else 1


def heartbeat_run(registry: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Serialize and renew every lock owned by one active run."""
    with run_update_lock(registry, run_id):
        with _authority_mutation_locked(registry, run_id, "heartbeat"):
            return _heartbeat_run_locked(registry, run_id)


def _heartbeat_run_locked(registry: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Renew ``last_heartbeat`` on every lock owned by an active run.

    Prevalidates all locks before writing any:
      - lock file must exist
      - YAML payload must be a mapping
      - payload ``run_id`` must exactly match
      - resolved path must be within the configured lock directory

    On validation failure no lock is modified.
    Returns ``{"run_id", "heartbeat_at", "renewed", "count"}``.
    """
    _, payload = read_run(registry, run_id)
    if payload.get("status") != "active":
        raise WorkflowError(f"cannot heartbeat non-active run {run_id} (status={payload.get('status', 'unknown')})")

    lock_dir = lock_state_dir(registry).resolve()
    raw_locks = payload.get("locks")
    if raw_locks is None:
        raw_locks = []
    if not isinstance(raw_locks, list):
        raise WorkflowError(f"run {run_id} locks must be a list, got {type(raw_locks).__name__}")
    for entry in raw_locks:
        if not isinstance(entry, str) or not entry:
            raise WorkflowError(f"run {run_id} locks contains invalid entry: {entry!r}")

    # Phase 1 — prevalidate every lock (no writes yet)
    validated: list[tuple[Path, dict[str, Any], str]] = []
    for lock_display in raw_locks:
        lock_path_raw = Path(lock_display)
        if not lock_path_raw.is_absolute():
            lock_path_raw = registry_workspace_root(registry) / lock_display
        try:
            lock_path = lock_path_raw.resolve()
        except OSError:
            raise WorkflowError(f"cannot resolve lock path: {lock_display}")

        try:
            lock_path.relative_to(lock_dir)
        except ValueError:
            raise WorkflowError(f"lock path escapes lock directory: {lock_display}")

        if not lock_path.exists():
            raise WorkflowError(f"missing lock file: {lock_display}")

        try:
            lock_data = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise WorkflowError(f"malformed YAML in lock {lock_display}: {exc}")
        except OSError as exc:
            raise WorkflowError(f"unreadable lock {lock_display}: {exc}")
        except UnicodeError as exc:
            raise WorkflowError(f"malformed lock (encoding error) {lock_display}: {exc}")
        if not isinstance(lock_data, dict):
            raise WorkflowError(f"malformed lock (not a mapping): {lock_display}")

        if lock_data.get("run_id") != run_id:
            raise WorkflowError(
                f"lock run_id mismatch in {lock_display}: expected {run_id}, found {lock_data.get('run_id')}"
            )

        validated.append((lock_path, lock_data, lock_display))

    # Phase 2 — write: only last_heartbeat changes (atomic per lock)
    heartbeat_at = utc_now()
    renewed: list[str] = []
    for lock_path, lock_data, lock_display in validated:
        lock_data["last_heartbeat"] = heartbeat_at
        write_yaml_atomic(lock_path, lock_data)
        renewed.append(lock_display)

    return {
        "run_id": run_id,
        "heartbeat_at": heartbeat_at,
        "renewed": renewed,
        "count": len(renewed),
    }


def run_file_for(registry: dict[str, Any], run_id: str) -> Path:
    run_dir = run_state_dir(registry)
    direct = run_dir / f"{run_id}.yaml"
    if direct.exists():
        return direct
    matches = list(run_dir.glob(f"*{run_id}*.yaml")) if run_dir.exists() else []
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise WorkflowError(f"ambiguous run id {run_id}: {', '.join(str(p) for p in matches)}")
    raise WorkflowError(f"run not found: {run_id}")


def start_run(
    registry: dict[str, Any],
    workflow: dict[str, Any],
    context: dict[str, str],
    objective: str,
    dry_run: bool,
    force_lock: bool,
    *,
    parent_run_id: str = "",
    parent_agent: str = "",
    bet_id: str = "",
    inherited_delivery_identity: dict[str, Any] | None = None,
    start_preflight: Callable[[str, dict[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    validate_agent_profile(registry, workflow, context.get("profile", ""), require=True)
    if parent_run_id:
        parent_bet_id, parent_identity, resolved_parent_agent = resolve_parent_delivery_identity(
            registry,
            parent_run_id,
            bet_id,
        )
        if inherited_delivery_identity is not None and inherited_delivery_identity != parent_identity:
            raise WorkflowError("WORK_PACKET_PARENT_BINDING_MISMATCH: supplied child identity differs from parent")
        bet_id = parent_bet_id
        inherited_delivery_identity = parent_identity
        parent_agent = resolved_parent_agent
    elif inherited_delivery_identity is not None:
        raise WorkflowError("WORK_PACKET_PARENT_BINDING_INCOMPLETE: inherited identity requires parent_run_id")
    if inherited_delivery_identity is not None:
        delivery_identity = _validate_inherited_delivery_identity(
            bet_id,
            inherited_delivery_identity,
            parent_run_id=parent_run_id,
        )
    else:
        delivery_identity = _prepare_bet_execution(bet_id) if bet_id else None
    if bet_id:
        context = {**context, "bet_id": bet_id}
    plan = workflow_plan(workflow, context)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{stamp}-{plan['id']}-{uuid.uuid4().hex[:8]}"
    context = {**context, "run_id": run_id}
    plan = workflow_plan(workflow, context)
    if inherited_delivery_identity is None and delivery_identity is not None:
        delivery_identity = _complete_fresh_delivery_identity(delivery_identity, run_id, start_preflight)
    record: dict[str, Any] = {
        "run_id": run_id,
        "workflow_id": plan["id"],
        "status": "active",
        "actor": context["actor"],
        "agent_profile": context.get("profile", ""),
        "objective": objective,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "context": context,
        "locks": [],
        "plan": plan,
        "evidence": [],
    }
    if parent_run_id:
        record["parent_run_id"] = parent_run_id
    if parent_agent:
        record["parent_agent"] = parent_agent
    if bet_id:
        record["bet_id"] = bet_id
        if delivery_identity is None:  # Defensive invariant; preparation is fail-closed.
            raise WorkflowError(f"WORK_PACKET_MISSING: no prepared identity for {bet_id}")
        record.update(delivery_identity)
    if dry_run:
        return record
    record["locks"] = acquire_locks(registry, plan["lock_scopes"], run_id, context["actor"], force_lock)
    run_dir = run_state_dir(registry)
    run_dir.mkdir(parents=True, exist_ok=True)
    run_path = run_dir / f"{run_id}.yaml"
    run_path.write_text(
        yaml.safe_dump(record, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )  # audit-exempt: non-atomic-write — run state single-writer under run_update_lock
    record["path"] = display_path(run_path)
    start_event: dict[str, Any] = {
        "event": "agent_workflow_start",
        "run_id": run_id,
        "workflow_id": plan["id"],
        "actor": context["actor"],
        "agent_profile": context.get("profile", ""),
        "objective": objective,
        "path": record["path"],
        "locks": record["locks"],
    }
    if parent_run_id:
        start_event["parent_run_id"] = parent_run_id
    if parent_agent:
        start_event["parent_agent"] = parent_agent
    append_ledger_event(registry, start_event)
    # Phase 1b/4: Bridge to Workflow Mesh with scene_binding
    _scene_binding = extract_scene_binding(context=context, workflow=workflow)
    mesh_payload: dict[str, Any] = {
        "workflow_id": plan["id"],
        "agent_profile": context.get("profile", ""),
        "objective": objective,
        "actor": context["actor"],
    }
    exact_start = not parent_run_id and ("capability_preflight" in record or "capability_requirements_digest" in record)
    if bet_id and isinstance(record.get("work_packet"), dict):
        # Canonical WorkPacket identity bridges into the Mesh so native-execution
        # verification can reconcile the binding against the persisted admission.
        work_packet = record["work_packet"]
        request_identity: dict[str, Any] = {
            "bet_id": bet_id,
            "packet_id": str(work_packet.get("packet_id") or ""),
            "packet_hash": str(record.get("work_packet_hash") or ""),
        }
        requirements = work_packet.get("capability_requirements") if isinstance(work_packet, Mapping) else None
        preflight = record.get("capability_preflight")
        binding = preflight.get("binding") if isinstance(preflight, Mapping) else None
        if isinstance(requirements, list):
            mesh_payload["capabilities"] = [
                str(requirement.get("capability_id"))
                for requirement in requirements
                if isinstance(requirement, Mapping) and requirement.get("capability_id")
            ]
        if isinstance(binding, Mapping) and not parent_run_id:
            request_identity = {
                "bet_id": bet_id,
                "workflow_id": plan["id"],
                **{key: binding.get(key) for key in _PREFLIGHT_BINDING_KEYS},
                "capability_requirements": requirements,
                "capability_requirements_digest": record.get("capability_requirements_digest"),
            }
        mesh_payload["request_identity"] = request_identity
    if exact_start:
        mesh_payload.update(
            {
                "exact_request_discriminator": "agent-workflow-exact/v1",
                "bet_id": str(bet_id or ""),
                "workflow_id": plan["id"],
            }
        )
    request_persisted = False
    try:
        request_persisted = emit_workflow_mesh_event(
            "AgentWorkflowStarted",
            run_id,
            mesh_payload,
            workspace=registry_workspace_root(registry),
        )
        if exact_start:
            if not request_persisted:
                raise WorkflowError("WORKFLOW_MESH_REQUEST_FAILED: exact Agent Workflow request was not persisted")
            capability_preflight = record.get("capability_preflight")
            binding = capability_preflight.get("binding") if isinstance(capability_preflight, Mapping) else None
            if not isinstance(binding, Mapping):
                raise WorkflowError("WORKFLOW_MESH_ADMISSION_FAILED: exact preflight binding is unavailable")
            admission = admit_agent_workflow_start(
                registry_workspace_root(registry),
                workflow_run_id=run_id,
                workflow_id=record["workflow_id"],
                bet_id=record["bet_id"],
                actor_id=str(binding.get("actor_id") or ""),
                work_packet=record["work_packet"],
                work_packet_hash=record["work_packet_hash"],
                capability_requirements_digest=record["capability_requirements_digest"],
                capability_preflight=capability_preflight,
            )
            if admission.get("worker_launch") is not False or admission.get("external_side_effects") != "disabled":
                raise WorkflowError("WORKFLOW_MESH_ADMISSION_UNSAFE: exact admission enabled execution")
    except BaseException as exc:
        if exact_start:
            _fail_exact_agent_workflow_start(
                registry,
                record,
                exc,
                request_persisted=request_persisted,
            )
        raise
    return record


def spawn_run(
    registry: dict[str, Any],
    parent_run_id: str,
    workflow: dict[str, Any],
    context: dict[str, str],
    objective: str,
    dry_run: bool = False,
    force_lock: bool = False,
    *,
    start_preflight: Callable[[str, dict[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    return start_run(
        registry,
        workflow,
        context,
        objective,
        dry_run,
        force_lock,
        parent_run_id=parent_run_id,
        start_preflight=start_preflight,
    )


def trace_attribution(registry: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
    chain: list[dict[str, Any]] = []
    visited: set[str] = set()
    current_id: str | None = run_id
    while current_id and current_id not in visited:
        visited.add(current_id)
        try:
            _, payload = read_run(registry, current_id)
        except (WorkflowError, FileNotFoundError):
            chain.append({"run_id": current_id, "status": "missing"})
            break
        entry = {
            "run_id": payload.get("run_id", current_id),
            "actor": payload.get("actor", ""),
            "agent_profile": payload.get("agent_profile", ""),
            "workflow_id": payload.get("workflow_id", ""),
            "status": payload.get("status", ""),
            "objective": payload.get("objective", ""),
        }
        chain.append(entry)
        current_id = payload.get("parent_run_id")
    chain.reverse()
    return chain


def read_run(registry: dict[str, Any], run_id: str) -> tuple[Path, dict[str, Any]]:
    path = run_file_for(registry, run_id)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict) or not payload.get("run_id"):
        raise WorkflowError(f"invalid run file: {path}")
    return path, payload


def write_run(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = utc_now()
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )  # audit-exempt: non-atomic-write — under run_update_lock


def claim_run(
    registry: dict[str, Any],
    run_id: str,
    actor: str,
    paths: list[str],
    surfaces: list[str],
    force_lock: bool,
    affected_hash: str | None = None,
    affected_receipt: str | None = None,
) -> dict[str, Any]:
    _, guard_payload = read_run(registry, run_id)
    if guard_payload.get("status") != "active":
        raise WorkflowError(f"cannot claim against non-active run: {run_id}")
    _validate_work_packet_claim(guard_payload, list(paths or []), list(surfaces or []))
    if force_lock and _authority_mode(guard_payload) == "shadow-active":
        raise WorkflowError("CLAIM_VERSION_STALE: force is forbidden after authority activation")
    heartbeat_run(registry, run_id)  # SR-01: renew before claim
    receipt_reference = affected_hash or affected_receipt
    if not receipt_reference:
        raise WorkflowError("Missing or invalid affected-hash. You must run affected-graph.py first.")
    if not paths and not surfaces:
        raise WorkflowError("claim requires at least one --path or --surface")
    with run_update_lock(registry, run_id):
        with _authority_mutation_locked(
            registry,
            run_id,
            "takeover" if force_lock else "claim",
            force_requested=force_lock,
        ):
            path, payload = read_run(registry, run_id)
            if payload.get("status") != "active":
                raise WorkflowError(f"cannot claim against non-active run: {run_id}")
            normalized_paths = sorted({normalize_repo_path(item) for item in paths})
            normalized_surfaces = sorted({item.strip() for item in surfaces if item.strip()})
            affected_graph = validate_affected_graph_receipt(
                receipt_reference,
                normalized_paths,
                WORKSPACE,
                normalized_surfaces,
            )

            # Phase 3 A2A Path Locks (Logical Isolation)
            # Check for path hierarchy overlap with other active runs
            run_dir = run_state_dir(registry)
            if run_dir.exists():
                for other_run_file in run_dir.glob("*.yaml"):
                    if other_run_file.name == f"{run_id}.yaml":
                        continue
                    try:
                        other_payload = yaml.safe_load(other_run_file.read_text(encoding="utf-8")) or {}
                    except Exception:
                        continue
                    if not isinstance(other_payload, dict):
                        continue
                    if other_payload.get("status") != "active":
                        continue

                    other_paths = []
                    for claim_item in other_payload.get("claims", []):
                        if isinstance(claim_item, dict):
                            other_paths.extend(claim_item.get("paths", []))

                    for p in normalized_paths:
                        for op in other_paths:
                            p_norm = p.rstrip("/")
                            op_norm = op.rstrip("/")
                            if (
                                p_norm == op_norm
                                or p_norm.startswith(op_norm + "/")
                                or op_norm.startswith(p_norm + "/")
                            ):
                                raise WorkflowError(
                                    f"A2A Path Lock Collision: path '{p}' overlaps with active claim '{op}' in run {other_payload.get('run_id', 'unknown')}"
                                )

            scopes = [f"path:{item}" for item in normalized_paths] + [f"surface:{item}" for item in normalized_surfaces]

            # Phase L0 MOF Enforce: Trigger pre-check for any projects being claimed
            mof_enforce_script = WORKSPACE / "bin/mof/mof-enforce"
            if mof_enforce_script.exists():
                for p in normalized_paths:
                    if p.startswith("projects/"):
                        parts = p.split("/")
                        if len(parts) >= 2:
                            node_id = parts[1]
                            try:
                                subprocess.run(
                                    ["bash", str(mof_enforce_script), "pre-check", node_id],
                                    cwd=str(WORKSPACE),
                                    capture_output=True,
                                    check=False,
                                )
                            except Exception:
                                pass

            lock_paths = acquire_locks(registry, scopes, run_id, actor, force_lock)
            try:
                payload.setdefault("locks", [])
                for lock_path in lock_paths:
                    if lock_path not in payload["locks"]:
                        payload["locks"].append(lock_path)
                claim = {
                    "claimed_at": utc_now(),
                    "actor": actor,
                    "paths": normalized_paths,
                    "surfaces": normalized_surfaces,
                    "scopes": scopes,
                    "locks": lock_paths,
                    "affected_graph": affected_graph,
                }
                payload.setdefault("claims", []).append(claim)
                write_run(path, payload)
            except Exception:
                for lock_path in lock_paths:
                    lock_file = Path(lock_path)
                    if not lock_file.is_absolute():
                        lock_file = WORKSPACE / lock_file
                    lock_file.unlink(missing_ok=True)
                raise
            append_ledger_event(
                registry,
                {
                    "event": "agent_workflow_claim",
                    "run_id": run_id,
                    "actor": actor,
                    "paths": normalized_paths,
                    "surfaces": normalized_surfaces,
                    "locks": lock_paths,
                },
            )
            return {**claim, "run_id": run_id}


def close_run(
    registry: dict[str, Any],
    run_id: str,
    status: str,
    evidence: list[str],
    release: bool,
    *,
    emit_mesh: bool = True,
) -> dict[str, Any]:
    with run_update_lock(registry, run_id):
        with _authority_mutation_locked(registry, run_id, "close"):
            return _close_run_locked(
                registry,
                run_id,
                status,
                evidence,
                release,
                emit_mesh=emit_mesh,
            )


def _close_run_locked(
    registry: dict[str, Any],
    run_id: str,
    status: str,
    evidence: list[str],
    release: bool,
    *,
    emit_mesh: bool = True,
) -> dict[str, Any]:
    path, payload = read_run(registry, run_id)
    direct_close_payload = {
        "status": status,
        "ok": status == "ok",
        "error": payload.get("error") or payload.get("failure_reason") or "",
        "evidence_count": len(evidence),
    }
    exact_closed = False
    workspace = registry_workspace_root(registry)
    if emit_mesh and _is_exact_agent_workflow_request(workspace, run_id):
        exact_closed = close_agent_workflow_run(
            workspace,
            workflow_run_id=run_id,
            status=status,
            payload=direct_close_payload,
        )
    payload["status"] = status
    payload["updated_at"] = utc_now()
    payload["closed_at"] = utc_now()
    payload.setdefault("evidence", [])
    payload["evidence"].extend(evidence)
    if release:
        payload["released_locks"] = release_locks(registry, payload["run_id"])
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )  # audit-exempt: non-atomic-write — under run_update_lock
    payload["path"] = display_path(path)
    append_ledger_event(
        registry,
        {
            "event": "agent_workflow_close",
            "run_id": payload["run_id"],
            "workflow_id": payload.get("workflow_id"),
            "status": status,
            "evidence": evidence,
            "released_locks": payload.get("released_locks", []),
        },
    )
    # Direct `close` owns its Mesh terminal event. `closeout` suppresses this
    # narrow payload and emits one richer terminal event after verify/observe.
    if emit_mesh and not exact_closed:
        emit_workflow_mesh_event(
            "AgentWorkflowClosed",
            payload["run_id"],
            direct_close_payload,
            workspace=registry_workspace_root(registry),
        )
    return payload


def _run_closeout_side_effects(
    registry: dict[str, Any],
    payload: dict[str, Any],
    run_id: str,
) -> None:
    """Run best-effort closeout integrations under one registry-owned root."""
    workspace = registry_workspace_root(registry)

    def run_silently(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
        try:
            subprocess.run(
                command,
                cwd=cwd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass

    smoke_script = workspace / "bin/gac/evidence-smoke.py"
    if smoke_script.is_file():
        run_silently([sys.executable, str(smoke_script), "--quiet"], cwd=workspace)

    omo_project = workspace / "projects/omo"
    if omo_project.is_dir():
        env = os.environ.copy()
        env["WORKSPACE_ROOT"] = str(workspace)
        env["PYTHONPATH"] = str(omo_project / "src")
        run_silently(
            [sys.executable, "-m", "omo.cli", "state", "sync"],
            cwd=omo_project,
            env=env,
        )

    kos_cli_path = workspace / "projects/kairon/packages/kos/kos-cli.py"
    env_kos = os.environ.copy()
    env_kos["WORKSPACE_ROOT"] = str(workspace)
    env_kos["KOS_HOME"] = str(workspace / "kos")
    env_kos["PYTHONPATH"] = str(workspace / "projects/kairon/packages/kos/src")
    if kos_cli_path.is_file():
        run_silently(
            [sys.executable, str(kos_cli_path), "ingress", "--snapshot", "latest", "--rebuild-ontology"],
            cwd=workspace,
            env=env_kos,
        )

    try:
        from omo.omo_belief import MOSBeliefManager

        belief_mgr = MOSBeliefManager(root=workspace)
        obj_text = payload.get("objective") or "agent-workflow closeout"
        wf_id = payload.get("workflow_id") or "general"
        belief_mgr.record_belief(
            topic=f"workflow:{wf_id}",
            belief_text=f"Workflow run {run_id} achieved objective: {obj_text}",
            pitfall="Unverified workflow closeout",
            solution="Executed agent-workflow verify & observe pass",
            scope_path=payload.get("path") or "*",
            source_run_id=run_id,
        )
    except Exception:
        # Preserve the historical recovery path without allowing it to escape
        # the registry-owned root or fail when KOS is unavailable.
        if kos_cli_path.is_file():
            run_silently([sys.executable, str(kos_cli_path), "onto", "rebuild"], cwd=workspace, env=env_kos)
            run_silently([sys.executable, str(kos_cli_path), "onto", "infer"], cwd=workspace, env=env_kos)
            for script_name in ("gac-kos-sync.py", "gac-consensus-inject.py"):
                script = workspace / "bin" / script_name
                if script.is_file():
                    run_silently([sys.executable, str(script)], cwd=workspace)


def closeout_run(
    registry: dict[str, Any],
    run_id: str,
    status: str,
    evidence: list[str],
    files: list[str],
    from_diff: bool,
    include_untracked: bool,
    all_checks: bool,
    keep_locks: bool,
) -> dict[str, Any]:
    if status == "ok":
        heartbeat_run(registry, run_id)  # SR-01: renew before successful closeout
    from .diagnostics import build_observe_report, build_verify_report

    verify_report = build_verify_report(
        registry,
        run_id,
        files,
        from_diff,
        include_untracked,
        all_checks,
        execute=True,
    )
    observe_report = build_observe_report(registry, run_id)
    if status == "ok" and not verify_report["ok"]:
        raise WorkflowError("closeout blocked: verify failed")
    if status == "ok" and not observe_report["ok"]:
        raise WorkflowError(f"closeout blocked: observe decision={observe_report['decision']}")
    closeout_evidence = [
        *evidence,
        f"agent-workflow verify: {verify_report['check_count']} checks ok={verify_report['ok']}",
        f"agent-workflow observe: {observe_report['decision']}",
    ]
    closeout_payload = {
        "status": status,
        "ok": status == "ok",
        "error": verify_report.get("reason") or "",
        "verify_ok": verify_report["ok"],
        "observe_decision": observe_report["decision"],
        "evidence_count": len(closeout_evidence),
    }
    workspace = registry_workspace_root(registry)
    exact_closed = False
    if _is_exact_agent_workflow_request(workspace, run_id):
        exact_closed = close_agent_workflow_run(
            workspace,
            workflow_run_id=run_id,
            status=status,
            payload=closeout_payload,
        )
    payload = close_run(
        registry,
        run_id,
        status,
        closeout_evidence,
        not keep_locks,
        emit_mesh=False,
    )
    report = {
        "ok": status == "ok",
        "run": payload,
        "verify": verify_report,
        "observe": observe_report,
    }
    append_ledger_event(
        registry,
        {
            "event": "agent_workflow_closeout",
            "run_id": run_id,
            "status": status,
            "ok": report["ok"],
            "verify_ok": verify_report["ok"],
            "observe_decision": observe_report["decision"],
        },
    )
    if status == "ok":
        _run_closeout_side_effects(registry, payload, run_id)
    # Phase 1b/5: Bridge to Workflow Mesh with event chain closure
    _closeout_scene = None
    try:
        _, _run_record = read_run(registry, run_id)
        _closeout_scene = extract_scene_binding(
            context=_run_record.get("context", {}),
            workflow=_run_record.get("plan", {}),
        )
    except Exception:
        pass
    if not exact_closed:
        emit_workflow_mesh_event(
            "AgentWorkflowClosed",
            run_id,
            closeout_payload,
            workspace=registry_workspace_root(registry),
        )
    return report


def load_run_records(
    registry: dict[str, Any],
) -> dict[str, tuple[Path, dict[str, Any]]]:
    run_dir = run_state_dir(registry)
    records: dict[str, tuple[Path, dict[str, Any]]] = {}
    if not run_dir.exists():
        return records
    for path in sorted(run_dir.glob("*.yaml")):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            payload = {}
        run_id = payload.get("run_id") if isinstance(payload, dict) else None
        if run_id:
            records[str(run_id)] = (path, payload)
    return records


def load_lock_records(registry: dict[str, Any]) -> list[tuple[Path, dict[str, Any]]]:
    lock_dir = lock_state_dir(registry)
    records: list[tuple[Path, dict[str, Any]]] = []
    if not lock_dir.exists():
        return records
    for path in sorted(lock_dir.glob("*.lock.yaml")):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            payload = {"run_id": None, "parse_error": True}
        records.append(
            (
                path,
                payload if isinstance(payload, dict) else {"run_id": None, "parse_error": True},
            )
        )
    return records


# 2026-08-26: claim 工具组拆至 lifecycle_claims.py (SRP 行数门), 此处 re-export 保持接口不变
from .lifecycle_claims import (  # noqa: F401 -- re-export
    claim_covers_path,
    claim_policy,
    claimed_paths,
    is_read_only_workflow,
    normalize_claim_mode,
)
from .lifecycle_ledger import (  # noqa: F401 -- re-export
    _extract_run_timestamp,
    append_ledger_event,
    heal_ledger_for_run,
    ledger_mentions_run,
)

# 2026-08-28: lock/ledger 工具组拆至 lifecycle_locks.py / lifecycle_ledger.py (SRP 行数门), 此处 re-export 保持接口不变
from .lifecycle_locks import (  # noqa: F401 -- re-export
    _HEARTBEAT_STALE_SECONDS,
    _bounded_lock_name,
    _classify_existing_lock,
    acquire_locks,
    heartbeat_lock,
    release_locks,
    run_update_lock,
    sanitize_lock_name,
    scan_locks,
)
from .lifecycle_locks import prune_stale_locks as _legacy_prune_stale_locks


def prune_stale_locks(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Delete only one frozen, revalidated stale candidate set.

    The discovery-style legacy helper is intentionally not called: candidates
    created or changed after the initial scan remain for a future explicit run.
    """
    frozen: list[dict[str, Any]] = []
    for entry in scan_locks(registry):
        if entry.get("kind") not in {"zombie_expired", "zombie_stale_heartbeat"}:
            continue
        run_id = str(entry.get("run_id") or "")
        raw_path = str(entry.get("path") or "")
        if not run_id or not raw_path:
            continue
        try:
            path = _authority_resolve_lock_path(registry, raw_path)
        except WorkflowError:
            continue
        if not path.is_file():
            continue
        frozen.append(
            {
                **entry,
                "run_id": run_id,
                "path": display_path(path),
                "content_digest": _authority_file_digest(path),
            }
        )
    if not frozen:
        return []

    run_ids = sorted({str(entry["run_id"]) for entry in frozen})
    with ExitStack() as stack:
        for run_id in run_ids:
            stack.enter_context(run_update_lock(registry, run_id))

        authority_batches: dict[str, tuple[dict[str, Any], dict[str, Any], str]] = {}
        authority_modes: dict[str, str] = {}
        for run_id in run_ids:
            try:
                before = _authority_snapshot(registry, run_id)
            except WorkflowError:
                continue
            mode = _authority_mode(before["payload"])
            authority_modes[run_id] = mode
            if mode == "shadow-active":
                begin = _authority_begin_mutation(registry, run_id, "expire", before)
                revalidated = _authority_snapshot(registry, run_id)
                if (
                    revalidated["run_digest"] != before["run_digest"]
                    or revalidated["lock_set_digest"] != before["lock_set_digest"]
                ):
                    _authority_settle_mutation(
                        registry,
                        run_id,
                        "expire",
                        begin,
                        revalidated,
                        outcome="rejected",
                    )
                    raise WorkflowError("CLAIM_VERSION_STALE")
                authority_batches[run_id] = (begin, before, mode)

        deleted: list[dict[str, Any]] = []
        deleted_by_run: set[str] = set()
        for entry in frozen:
            path = _authority_resolve_lock_path(registry, str(entry["path"]))
            if not path.is_file():
                continue
            try:
                if _authority_file_digest(path) != entry["content_digest"]:
                    continue
                classification = _classify_existing_lock(path)
            except OSError:
                continue
            if classification.get("kind") != entry.get("kind"):
                continue
            payload = classification.get("payload")
            if not isinstance(payload, Mapping) or payload.get("run_id") != entry["run_id"]:
                continue
            path.unlink()
            deleted.append({key: value for key, value in entry.items() if key != "content_digest"})
            deleted_by_run.add(str(entry["run_id"]))

        for run_id, (begin, before, _mode) in authority_batches.items():
            after = _authority_snapshot(registry, run_id)
            changed = (
                after["run_digest"] != before["run_digest"] or after["lock_set_digest"] != before["lock_set_digest"]
            )
            _authority_settle_mutation(
                registry,
                run_id,
                "expire",
                begin,
                after,
                outcome="applied" if run_id in deleted_by_run and changed else "rejected",
            )
        for run_id in sorted(deleted_by_run):
            if authority_modes.get(run_id) == "unactivated":
                _record_authority_shadow_event(
                    registry,
                    run_id,
                    "expire",
                    "shadow_unprovable",
                    code="not_activated",
                )
        return deleted
