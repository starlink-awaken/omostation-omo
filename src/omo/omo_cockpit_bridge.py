from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Final

from .omo_io import AppendOnlyLog, fcntl_lock, write_text_atomic, write_yaml_atomic
from .omo_redaction import redact_sensitive_text
from .omo_shared import load_yaml

REQUIRED_PROPOSAL_FIELDS: Final = frozenset(
    {
        "id",
        "type",
        "debt_id",
        "source",
        "target",
        "expected_change",
        "operation_level",
        "approval_required",
        "rollback",
        "verification",
        "auto_apply",
        "proposal_digest",
    }
)
FORBIDDEN_RAW_FIELDS: Final = frozenset({"body", "content", "payload", "raw_content", "secret", "password", "token"})
PROPOSAL_ID_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")


def _contains_secret_like_value(value: object) -> bool:
    if isinstance(value, str):
        return redact_sensitive_text(value) != value
    if isinstance(value, dict):
        if any(str(key).lower() in FORBIDDEN_RAW_FIELDS for key in value):
            return True
        return any(_contains_secret_like_value(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_secret_like_value(item) for item in value)
    return False


def _proposal_digest(proposal: dict[str, Any]) -> str:
    canonical = {key: value for key, value in proposal.items() if key != "proposal_digest"}
    encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _proposal_path(omo_dir: Path, proposal_id: str) -> Path:
    return omo_dir / "state" / "proposals" / f"{proposal_id}.yaml"


def _terminal_path(omo_dir: Path, proposal_id: str) -> Path:
    return omo_dir / "_delivery" / "hitl" / "family-dashboard" / f"{proposal_id}.yaml"


def _valid_proposal_id(proposal_id: str) -> bool:
    return PROPOSAL_ID_PATTERN.fullmatch(proposal_id) is not None


def write_console_run_record(workspace_root: Path, run_id: str, record: dict[str, Any]) -> Path:
    """Atomically persist one Cockpit Console run projection through OMO.

    Cockpit is an L3 caller and must not construct or mutate ``.omo`` paths. The
    run id and envelope identity are checked here so a caller cannot turn a run
    record into an arbitrary file write.
    """
    if not _valid_proposal_id(run_id):
        raise ValueError("console run id invalid")
    if record.get("run_id") != run_id:
        raise ValueError("console run id mismatch")
    if _contains_secret_like_value(record):
        raise ValueError("console run record contains secret-like raw values")
    target = Path(workspace_root) / ".omo" / "_delivery" / "console" / "runs" / f"{run_id}.json"
    write_text_atomic(target, json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return target


def _valid_runtime_receipt(ref: object, digest: object) -> bool:
    if not isinstance(ref, str) or not isinstance(digest, str):
        return False
    path = Path(ref)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != ref:
        return False
    return re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is not None


def record_hitl_proposal(
    omo_dir: Path,
    proposal: dict[str, Any],
    *,
    requested_by: str,
    now: str,
) -> dict[str, Any]:
    missing = sorted(REQUIRED_PROPOSAL_FIELDS - proposal.keys())
    if missing or proposal.get("approval_required") is not True or proposal.get("auto_apply") != "disabled":
        raise ValueError("proposal envelope invalid")
    if proposal.get("operation_level") != "L3" or not str(proposal.get("proposal_digest", "")).startswith("sha256:"):
        raise ValueError("proposal envelope invalid")
    if not hmac.compare_digest(str(proposal["proposal_digest"]), _proposal_digest(proposal)):
        raise ValueError("proposal digest mismatch")
    if _contains_secret_like_value(proposal):
        raise ValueError("proposal contains secret-like raw values")
    proposal_id = str(proposal.get("id", ""))
    if not _valid_proposal_id(proposal_id):
        raise ValueError("proposal id invalid")
    payload = {**proposal, "status": "pending", "requested_by": requested_by, "created_at": now}
    path = _proposal_path(omo_dir, proposal_id)
    with fcntl_lock(path.with_suffix(".lock")):
        if path.exists():
            current = load_yaml(path)
            if current.get("proposal_digest") != proposal["proposal_digest"]:
                raise ValueError("proposal id collision")
            return current
        write_yaml_atomic(path, payload)
    return payload


def list_hitl_proposals(omo_dir: Path) -> list[dict[str, Any]]:
    directory = omo_dir / "state" / "proposals"
    rows = [load_yaml(path) for path in sorted(directory.glob("*.yaml"))] if directory.exists() else []
    return sorted(
        (row for row in rows if isinstance(row, dict)), key=lambda row: str(row.get("created_at", "")), reverse=True
    )


async def approve_hitl_proposal_async(
    omo_dir: Path,
    proposal_id: str,
    *,
    principal_ref: str,
    approved_at: str,
    execute_mutation: Callable[[dict[str, Any]], dict[str, Any] | Awaitable[dict[str, Any]]],
) -> tuple[bool, str | None, dict[str, Any] | None]:
    if not _valid_proposal_id(proposal_id):
        return False, "proposal id invalid", None
    if not principal_ref.startswith("operator://cockpit-api/"):
        return False, "verified principal required", None
    pending = _proposal_path(omo_dir, proposal_id)
    processing = pending.with_suffix(".processing")
    terminal = _terminal_path(omo_dir, proposal_id)
    if terminal.exists():
        receipt = load_yaml(terminal)
        return (receipt.get("status") == "verified", None, receipt)
    if not pending.exists():
        return False, f"Proposal {proposal_id} not found", None
    try:
        pending.rename(processing)
    except OSError:
        return False, f"Proposal {proposal_id} is already being processed or locked.", None
    try:
        proposal = load_yaml(processing)
        proposal = {**proposal, "status": "approved", "approved_by": principal_ref, "approved_at": approved_at}
        write_yaml_atomic(processing, proposal)
        outcome = execute_mutation(proposal)
        if inspect.isawaitable(outcome):
            outcome = await outcome
        if not isinstance(outcome, dict) or outcome.get("status") != "verified":
            raise ValueError("mutation did not return verified receipt")
        if not _valid_runtime_receipt(outcome.get("verify_receipt_ref"), outcome.get("verify_receipt_sha256")):
            raise ValueError("runtime receipt invalid")
        if outcome.get("canary_rolled_back") is True and not _valid_runtime_receipt(
            outcome.get("rollback_receipt_ref"), outcome.get("rollback_receipt_sha256")
        ):
            raise ValueError("runtime receipt invalid")
        receipt: dict[str, Any] = {
            "schema": "omo-hitl-execution-receipt/v1",
            "proposal_id": proposal_id,
            "proposal_digest": proposal["proposal_digest"],
            "status": "verified",
            "approved_by": principal_ref,
            "approved_at": approved_at,
            "bos_uri": "bos://governance/hitl/execute/family_dashboard_document_write",
            "runtime_receipt_ref": outcome["verify_receipt_ref"],
            "runtime_receipt_sha256": outcome["verify_receipt_sha256"],
        }
        if outcome.get("canary_rolled_back") is True:
            receipt.update(
                {
                    "canary_rolled_back": True,
                    "rollback_receipt_ref": outcome["rollback_receipt_ref"],
                    "rollback_receipt_sha256": outcome["rollback_receipt_sha256"],
                }
            )
        if terminal.exists() and load_yaml(terminal) != receipt:
            raise ValueError("terminal receipt collision")
        write_yaml_atomic(terminal, receipt)
        processing.unlink()
        return True, None, receipt
    except Exception as exc:
        if processing.exists():
            processing.rename(pending)
        return False, str(exc), None


def reject_hitl_proposal(
    omo_dir: Path,
    proposal_id: str,
    *,
    principal_ref: str,
    rejected_at: str,
) -> dict[str, Any]:
    if not _valid_proposal_id(proposal_id):
        raise ValueError("proposal id invalid")
    if not principal_ref.startswith("operator://cockpit-api/"):
        raise ValueError("verified principal required")
    pending = _proposal_path(omo_dir, proposal_id)
    proposal = load_yaml(pending)
    if not proposal:
        raise ValueError(f"Proposal {proposal_id} not found")
    receipt = {
        "schema": "omo-hitl-execution-receipt/v1",
        "proposal_id": proposal_id,
        "proposal_digest": proposal["proposal_digest"],
        "status": "rejected",
        "rejected_by": principal_ref,
        "rejected_at": rejected_at,
    }
    terminal = _terminal_path(omo_dir, proposal_id)
    if terminal.exists() and load_yaml(terminal) != receipt:
        raise ValueError("terminal receipt collision")
    write_yaml_atomic(terminal, receipt)
    pending.unlink()
    return receipt


def append_hitl_override(omo_dir: Path, stream_name: str, record: dict[str, Any]) -> str:
    if Path(stream_name).name != stream_name or not stream_name.endswith(".jsonl"):
        raise ValueError("override stream invalid")
    proposal_id = str(record.get("proposal_id", ""))
    if not _valid_proposal_id(proposal_id):
        raise ValueError("proposal id invalid")
    stream_path = omo_dir / "state" / stream_name
    receipt_path = omo_dir / "_delivery" / "hitl" / "overrides" / f"{proposal_id}.json"
    encoded_record = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    receipt = {
        "schema": "omo-hitl-override-receipt/v1",
        "proposal_id": proposal_id,
        "status": "applied",
        "stream_ref": stream_path.relative_to(omo_dir).as_posix(),
        "record_sha256": "sha256:" + hashlib.sha256(encoded_record).hexdigest(),
    }
    with fcntl_lock(receipt_path.with_suffix(".lock")):
        if receipt_path.exists():
            current = json.loads(receipt_path.read_text(encoding="utf-8"))
            if current != receipt:
                raise ValueError("override receipt collision")
            return str(receipt_path)
        stream_lock = fcntl_lock(stream_path.with_suffix(stream_path.suffix + ".lock"))
        AppendOnlyLog(stream_path, lock=stream_lock).append(record, sort_keys=False)
        write_text_atomic(receipt_path, json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    return str(receipt_path)
