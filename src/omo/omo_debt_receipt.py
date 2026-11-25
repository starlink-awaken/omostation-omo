"""SHA-256 bound local broker receipts for canonical `.omo/debt/` writes."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

from .omo_io import AppendOnlyLog, fcntl_lock
from .omo_paths import state_root

OMO_DEBT_RECEIPT_LOG_ENV = "OMO_DEBT_RECEIPT_LOG"
RECEIPT_RELATIVE_PATH = Path(".omo") / "_delivery" / "omo-debt-writes.jsonl"


def _is_repo_checkout(repo_root: Path) -> bool:
    return (repo_root / "bin" / "lib" / "repo_root.py").is_file()


def resolve_receipt_log_path(omo_dir: Path) -> Path:
    """Resolve JSONL receipt path while keeping isolated test directories hermetic."""
    declared = os.environ.get(OMO_DEBT_RECEIPT_LOG_ENV)
    if declared:
        return Path(declared).expanduser().absolute()
    repo_root = omo_dir.resolve().parent
    if not _is_repo_checkout(repo_root):
        return omo_dir / "_delivery" / "omo-debt-writes.jsonl"
    return state_root() / RECEIPT_RELATIVE_PATH


def _relative_posix(omo_dir: Path, written_path: Path) -> str:
    repo_root = omo_dir.resolve().parent
    resolved_written = written_path.resolve()
    try:
        return resolved_written.relative_to(repo_root).as_posix()
    except ValueError:
        return resolved_written.as_posix()


def record_broker_write(
    omo_dir: Path,
    written_path: Path,
    *,
    command: str,
    debt_id: str = "",
    actor: str = "",
) -> Path:
    """Append a SHA-256 bound JSONL receipt for a broker-written file."""
    digest = hashlib.sha256(written_path.read_bytes()).hexdigest()
    log_path = resolve_receipt_log_path(omo_dir)
    entry = {
        "at": datetime.now(UTC).isoformat(),
        "path": _relative_posix(omo_dir, written_path),
        "sha256": digest,
        "command": command,
        "debt_id": debt_id,
        "actor": actor,
        "run_id": os.environ.get("AGENT_WORKFLOW_RUN_ID", ""),
    }
    log = AppendOnlyLog(log_path, lock=fcntl_lock(log_path.with_suffix(".lock")))
    log.append(entry)
    return log_path
