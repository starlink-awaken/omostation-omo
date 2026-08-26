from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager
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
    _, parent_payload = read_run(registry, parent_run_id)
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


def sanitize_lock_name(scope: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", scope).strip("_") or "workspace"


def _bounded_lock_name(scope: str, max_len: int) -> str:
    """锁文件名上限保护: 超长时截断 + 内容 hash 后缀降低碰撞风险.

    macOS filename 上限 255 bytes; `verify` 不带 run_id 时 lifecycle 会把
    argv 串拼进 run_id → 锁名可达数千字节 → Errno 63 崩溃 (T1-05A 修复轮实测).
    """
    import hashlib

    name = sanitize_lock_name(scope)
    if len(name) <= max_len:
        return name
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    return f"{name[: max_len - len(digest) - 1]}-{digest}"


@contextmanager
def run_update_lock(registry: dict[str, Any], run_id: str):
    lock_dir = lock_state_dir(registry)
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_name = _bounded_lock_name(run_id, _RUN_UPDATE_LOCK_NAME_MAX_LEN)
    lock_path = lock_dir / f"run_{lock_name}.update.lock"
    deadline = time.monotonic() + RUN_UPDATE_LOCK_TIMEOUT_SECONDS
    acquired = False
    while not acquired:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(f"run_id: {run_id}\ncreated_at: {utc_now()}\n")
            acquired = True
        except FileExistsError:
            try:
                if time.time() - lock_path.stat().st_mtime > RUN_UPDATE_LOCK_TIMEOUT_SECONDS:
                    lock_path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise WorkflowError(f"timed out waiting for run update lock: {display_path(lock_path)}")
            time.sleep(0.05)
    try:
        yield
    finally:
        if acquired:
            lock_path.unlink(missing_ok=True)


def append_ledger_event(registry: dict[str, Any], event: dict[str, Any]) -> None:
    path = ledger_path(registry)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ts": utc_now(), **event}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _extract_run_timestamp(run_id: str) -> str | None:
    """Parse the timestamp embedded in a run_id like 20260723T062855Z-..."""
    if len(run_id) < 16 or not run_id[:4].isdigit():
        return None
    return f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}T{run_id[9:11]}:{run_id[11:13]}:{run_id[13:15]}Z"


def ledger_mentions_run(registry: dict[str, Any], run_id: str) -> bool:
    path = ledger_path(registry)
    if not path.exists() or path.stat().st_size == 0:
        return False
    needle = f'"run_id": "{run_id}"'
    # also match compact JSON without space after colon
    needle_alt = f'"run_id":"{run_id}"'
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return needle in text or needle_alt in text


def heal_ledger_for_run(
    registry: dict[str, Any],
    run_id: str,
    payload: dict[str, Any],
) -> bool:
    """ADR-0209 A2: if ledger has no event for a known run, replay from run yaml.

    Reconstructs a minimal start (and close if terminal) event so observe/compliance
    do not warn forever after events.jsonl was trimmed externally.
    Returns True when a heal write happened.
    """
    if ledger_mentions_run(registry, run_id):
        return False
    original_ts = payload.get("created_at") or _extract_run_timestamp(run_id)
    append_ledger_event(
        registry,
        {
            "event": "agent_workflow_start",
            "run_id": run_id,
            "workflow_id": payload.get("workflow_id"),
            "actor": payload.get("actor"),
            "agent_profile": payload.get("agent_profile"),
            "objective": payload.get("objective"),
            "path": payload.get("path"),
            "locks": payload.get("locks") or [],
            "healed": True,
            "heal_reason": "ledger_missing_run_replay_from_run_yaml",
            "ts": original_ts or utc_now(),
        },
    )
    status = str(payload.get("status") or "")
    if status in {"ok", "failed", "blocked"}:
        close_ts = payload.get("closed_at") or payload.get("updated_at") or original_ts
        append_ledger_event(
            registry,
            {
                "event": "agent_workflow_close",
                "run_id": run_id,
                "workflow_id": payload.get("workflow_id"),
                "status": status,
                "evidence": payload.get("evidence") or [],
                "healed": True,
                "heal_reason": "ledger_missing_run_replay_from_run_yaml",
                "ts": close_ts or utc_now(),
            },
        )
    return True


_HEARTBEAT_STALE_SECONDS = 3600


def _classify_existing_lock(lock_path: Path) -> dict[str, Any]:
    """Classify an existing lock as live, zombie_expired, or zombie_stale_heartbeat."""
    try:
        payload = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
    except (yaml.YAMLError, OSError):
        return {"kind": "zombie_stale_heartbeat", "detail": "unreadable lock file"}
    expires = payload.get("expires_at", "")
    if expires:
        try:
            exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
            if datetime.now(UTC) > exp_dt:
                return {
                    "kind": "zombie_expired",
                    "detail": f"expired at {expires}",
                    "payload": payload,
                }
        except ValueError:
            pass
    heartbeat = payload.get("last_heartbeat", "")
    if heartbeat:
        try:
            hb_dt = datetime.fromisoformat(heartbeat.replace("Z", "+00:00"))
            age = (datetime.now(UTC) - hb_dt).total_seconds()
            if age > _HEARTBEAT_STALE_SECONDS:
                return {
                    "kind": "zombie_stale_heartbeat",
                    "detail": f"heartbeat {age:.0f}s ago (> {_HEARTBEAT_STALE_SECONDS}s)",
                    "payload": payload,
                }
        except ValueError:
            pass
    return {"kind": "live", "detail": "holder active", "payload": payload}


def heartbeat_lock(lock_path: Path) -> None:
    """Update last_heartbeat on a lock file to signal liveness."""
    try:
        payload = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
        payload["last_heartbeat"] = utc_now()
        write_yaml_atomic(lock_path, payload)
    except OSError:
        pass


def heartbeat_run(registry: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Serialize and renew every lock owned by one active run."""
    with run_update_lock(registry, run_id):
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


def acquire_locks(
    registry: dict[str, Any],
    scopes: list[str],
    run_id: str,
    actor: str,
    force: bool,
) -> list[str]:
    lock_dir = lock_state_dir(registry)
    lock_dir.mkdir(parents=True, exist_ok=True)
    acquired: list[str] = []
    acquired_paths: list[Path] = []
    ttl_hours = float(registry.get("runner", {}).get("lock_ttl_hours", 24))
    expires_at = (datetime.now(UTC) + timedelta(hours=ttl_hours)).replace(microsecond=0)
    try:
        for scope in scopes:
            lock_name = _bounded_lock_name(scope, _PATH_LOCK_NAME_MAX_LEN)
            lock_path = lock_dir / f"{lock_name}.lock.yaml"
            now_ts = utc_now()
            payload = {
                "run_id": run_id,
                "actor": actor,
                "scope": scope,
                "created_at": now_ts,
                "last_heartbeat": now_ts,
                "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
            }
            if lock_path.exists() and not force:
                classification = _classify_existing_lock(lock_path)
                existing = lock_path.read_text(encoding="utf-8").strip()
                if classification["kind"] == "live":
                    raise WorkflowError(
                        f"lock HELD (live) for {scope}: {lock_path}\n"
                        f"  holder is active — {classification['detail']}\n"
                        f"{existing}"
                    )
                lock_path.unlink(missing_ok=True)
            with lock_path.open("w" if force else "x", encoding="utf-8") as handle:
                yaml.safe_dump(payload, handle, allow_unicode=True, sort_keys=False)
            acquired_paths.append(lock_path)
            acquired.append(display_path(lock_path))
    except WorkflowError:
        raise
    except Exception:
        for path in acquired_paths:
            path.unlink(missing_ok=True)
        raise
    return acquired


def release_locks(registry: dict[str, Any], run_id: str) -> list[str]:
    lock_dir = lock_state_dir(registry)
    released: list[str] = []
    if not lock_dir.exists():
        return released
    for lock_path in lock_dir.glob("*.lock.yaml"):
        try:
            payload = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            continue
        if payload.get("run_id") == run_id:
            lock_path.unlink()
            released.append(display_path(lock_path))
    return released


def scan_locks(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a report of all path locks with live/zombie classification."""
    lock_dir = lock_state_dir(registry)
    results: list[dict[str, Any]] = []
    if not lock_dir.exists():
        return results
    for lock_path in sorted(lock_dir.glob("*.lock.yaml")):
        classification = _classify_existing_lock(lock_path)
        entry: dict[str, Any] = {
            "path": display_path(lock_path),
            "kind": classification["kind"],
            "detail": classification["detail"],
        }
        payload = classification.get("payload") or {}
        if payload:
            entry["run_id"] = payload.get("run_id", "")
            entry["actor"] = payload.get("actor", "")
            entry["scope"] = payload.get("scope", "")
            entry["created_at"] = payload.get("created_at", "")
            entry["last_heartbeat"] = payload.get("last_heartbeat", "")
            entry["expires_at"] = payload.get("expires_at", "")
        results.append(entry)
    return results


def prune_stale_locks(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Remove zombie locks (expired or stale heartbeat). Return pruned entries."""
    pruned: list[dict[str, Any]] = []
    for entry in scan_locks(registry):
        if entry["kind"] in ("zombie_expired", "zombie_stale_heartbeat"):
            lock_file = Path(entry["path"])
            if not lock_file.is_absolute():
                lock_file = WORKSPACE / lock_file
            lock_file.unlink(missing_ok=True)
            pruned.append(entry)
    return pruned


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
    emit_workflow_mesh_event(
        "AgentWorkflowStarted",
        run_id,
        {
            "workflow_id": plan["id"],
            "agent_profile": context.get("profile", ""),
            "objective": objective,
            "actor": context["actor"],
        },
        workspace=registry_workspace_root(registry),
    )
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
    heartbeat_run(registry, run_id)  # SR-01: renew before claim
    receipt_reference = affected_hash or affected_receipt
    if not receipt_reference:
        raise WorkflowError("Missing or invalid affected-hash. You must run affected-graph.py first.")
    if not paths and not surfaces:
        raise WorkflowError("claim requires at least one --path or --surface")
    with run_update_lock(registry, run_id):
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
                        if p_norm == op_norm or p_norm.startswith(op_norm + "/") or op_norm.startswith(p_norm + "/"):
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
    path, payload = read_run(registry, run_id)
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
    if emit_mesh:
        emit_workflow_mesh_event(
            "AgentWorkflowClosed",
            payload["run_id"],
            {
                "status": status,
                "ok": status == "ok",
                "evidence_count": len(evidence),
            },
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
    emit_workflow_mesh_event(
        "AgentWorkflowClosed",
        run_id,
        {
            "status": status,
            "ok": report["ok"],
            "verify_ok": verify_report["ok"],
            "observe_decision": observe_report["decision"],
            "evidence_count": len(closeout_evidence),
        },
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

# 2026-08-27: report 函数组拆至 lifecycle_report.py (SRP 行数门)
from .lifecycle_report import (  # noqa: F401 -- re-export
    claim_coverage_report,
    recommended_next,
    staged_lane_report,
)
