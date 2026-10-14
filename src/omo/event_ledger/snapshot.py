"""Verified online snapshot of the event ledger into a profile state root.

ADR-0456 B4b batch 1 (BET-Y2Q4-T10-208).  A snapshot is a *copy that proves
itself*, not a move: the source keeps writing, and every artifact created here
carries ``authoritative=false`` so a plausible-looking file at a new path can
never be mistaken for — or quietly promoted to — the authority.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .broker import LedgerBroker, LedgerError
from .schema import schema_fingerprint, table_names

#: Written beside every copy as ``<dest>.provenance.json``.
SIDECAR_SUFFIX = ".provenance.json"

_AUTHORITY_BASE = (
    'Not the authority. The authoritative ledger is the file named in "source"; '
    "no running service writes here yet (ADR-0456 B4b batch 1)."
)
_AUTHORITY_NOTE_SNAPSHOT = (
    f"{_AUTHORITY_BASE} Promoting this copy requires the batch that re-points "
    "launchd/cron and injects OMO_EVENT_LEDGER_DB."
)
_AUTHORITY_NOTE_BOOTSTRAP = (
    f"{_AUTHORITY_BASE} This store is the dev profile's own empty ledger, so dev "
    "verification never writes beside production."
)

_REQUIRED_TABLES = frozenset({"event_log", "schema_migration"})


class SnapshotError(Exception):
    """Fail-closed snapshot refusal with a stable machine-readable ``reason``."""

    def __init__(self, reason: str, message: str) -> None:
        self.reason = reason
        self.message = message
        super().__init__(message)


def provenance_path(dest: Path) -> Path:
    return dest.with_name(f"{dest.name}{SIDECAR_SUFFIX}")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _open_readonly(path: Path) -> sqlite3.Connection:
    """Open a database without writing it.

    A WAL database with no readable ``-shm`` index cannot be opened ``mode=ro``;
    the fallback keeps the file safe by restricting statements instead
    (``query_only``), which is what actually blocks the write PRAGMAs this
    contract forbids against a live source.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return conn
    except sqlite3.Error:
        conn.close()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    try:
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
    except sqlite3.Error as exc:
        conn.close()
        raise SnapshotError("source_unreadable", f"cannot read {path}: {exc}") from exc
    return conn


def _require_ledger_tables(conn: sqlite3.Connection, path: Path) -> None:
    missing = _REQUIRED_TABLES - table_names(conn)
    if missing:
        raise SnapshotError(
            "source_not_a_ledger",
            f"{path} is not an event ledger (missing {sorted(missing)})",
        )


def _head_sequence(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(sequence) FROM event_log").fetchone()
    value = row[0]
    return int(value) if value is not None else 0


def _stored_hashes(conn: sqlite3.Connection, upper: int) -> dict[int, str]:
    """Stored ``event_hash`` values keyed by sequence, never recomputed."""
    rows = conn.execute(
        "SELECT sequence, event_hash FROM event_log WHERE sequence <= ? ORDER BY sequence ASC",
        (upper,),
    ).fetchall()
    return {int(row["sequence"]): str(row["event_hash"]) for row in rows}


def _integrity_check(conn: sqlite3.Connection) -> str:
    row = conn.execute("PRAGMA integrity_check").fetchone()
    return str(row[0]) if row is not None else "no result"


def _discard(paths: list[Path]) -> None:
    for path in paths:
        for candidate in (path, path.with_name(f"{path.name}-wal"), path.with_name(f"{path.name}-shm")):
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                pass


def _write_provenance(dest: Path, payload: dict[str, Any]) -> Path:
    sidecar = provenance_path(dest)
    sidecar.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return sidecar


def snapshot_ledger(source: Path | str, dest: Path | str) -> dict[str, Any]:
    """Copy a live ledger to ``dest`` and prove the copy is a gap-free prefix of it."""
    src_path = Path(source).expanduser().resolve()
    dst_path = Path(dest).expanduser().resolve()

    if not src_path.is_file():
        raise SnapshotError("source_missing", f"source ledger not found: {src_path}")
    if src_path == dst_path:
        raise SnapshotError("source_equals_dest", "refusing to snapshot a ledger onto itself")
    _require_absent_destination(dst_path)

    created: list[Path] = [dst_path]
    try:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        src_conn = _open_readonly(src_path)
        try:
            _require_ledger_tables(src_conn, src_path)
            # The only copy primitive that is safe against an un-checkpointed WAL.
            dst_conn = sqlite3.connect(str(dst_path))
            try:
                src_conn.backup(dst_conn)
            finally:
                dst_conn.close()
        finally:
            src_conn.close()

        # Same entry point production uses: proves the copy's schema is the one
        # the kernel accepts before anyone trusts a row in it.
        broker = LedgerBroker.connect(dst_path)
        try:
            chain = broker.verify_chain()
        finally:
            broker.close()

        check_conn = _open_readonly(dst_path)
        try:
            integrity = _integrity_check(check_conn)
            dest_head = _head_sequence(check_conn)
            copy_hashes = _stored_hashes(check_conn, dest_head)
        finally:
            check_conn.close()

        if integrity != "ok":
            raise SnapshotError("integrity_check_failed", f"copy failed PRAGMA integrity_check: {integrity}")
        if not chain["ok"] or chain["first_bad_sequence"] is not None:
            raise SnapshotError(
                "chain_broken_in_copy",
                f"copy does not self-verify: {chain.get('error')} at sequence {chain.get('first_bad_sequence')}",
            )
        missing = sorted({s for s in range(1, dest_head + 1) if s not in copy_hashes})
        if missing:
            raise SnapshotError("copy_has_gaps", f"copy is missing sequences within its own head: {missing[:20]}")

        # Re-read the source *after* the copy so events appended during the copy
        # window show up as source_ahead instead of being silently swallowed.
        src_conn = _open_readonly(src_path)
        try:
            src_head = _head_sequence(src_conn)
            prefix = _stored_hashes(src_conn, dest_head)
        finally:
            src_conn.close()

        if src_head < dest_head:
            raise SnapshotError(
                "source_shorter_than_copy",
                f"source head {src_head} is behind copy head {dest_head}; the source was truncated mid-snapshot",
            )
        mismatches = sorted(s for s, digest in copy_hashes.items() if prefix.get(s) != digest)
        if mismatches:
            raise SnapshotError(
                "prefix_hash_mismatch",
                f"copy diverges from source at sequences {mismatches[:20]}",
            )

        payload = {
            "mode": "snapshot",
            "authoritative": False,
            "authority_note": _AUTHORITY_NOTE_SNAPSHOT,
            "source": str(src_path),
            "dest": str(dst_path),
            "source_head_sequence": src_head,
            "dest_head_sequence": dest_head,
            "source_ahead": src_head - dest_head,
            "compared_sequences": dest_head,
            "integrity_check": integrity,
            "chain_ok": True,
            "chain_total": int(chain["total"]),
            "first_bad_sequence": None,
            "prefix_equal": True,
            "sqlite_version": sqlite3.sqlite_version,
            "created_at_utc": _utc_now(),
        }
        sidecar = _write_provenance(dst_path, payload)
        created.append(sidecar)
        return {**payload, "provenance_file": str(sidecar)}
    except SnapshotError:
        _discard(created)
        raise
    except (LedgerError, sqlite3.Error) as exc:
        _discard(created)
        raise SnapshotError("snapshot_failed", f"snapshot aborted: {exc}") from exc


def bootstrap_store(dest: Path | str) -> dict[str, Any]:
    """Create an empty ledger at ``dest`` through the production schema path."""
    dst_path = Path(dest).expanduser().resolve()
    _require_absent_destination(dst_path)

    created: list[Path] = [dst_path]
    try:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        broker = LedgerBroker.connect(dst_path)
        try:
            chain = broker.verify_chain()
        finally:
            broker.close()

        conn = _open_readonly(dst_path)
        try:
            integrity = _integrity_check(conn)
            head = _head_sequence(conn)
            fingerprint = schema_fingerprint(conn)
        finally:
            conn.close()

        if integrity != "ok":
            raise SnapshotError("integrity_check_failed", f"bootstrapped store failed integrity_check: {integrity}")
        if not chain["ok"]:
            raise SnapshotError("chain_broken_in_bootstrap", f"empty store does not verify: {chain.get('error')}")
        if head != 0:
            raise SnapshotError("bootstrap_not_empty", f"expected an empty store, found head sequence {head}")

        payload = {
            "mode": "bootstrap",
            "authoritative": False,
            "authority_note": _AUTHORITY_NOTE_BOOTSTRAP,
            "source": None,
            "dest": str(dst_path),
            "source_head_sequence": None,
            "dest_head_sequence": 0,
            "source_ahead": 0,
            "compared_sequences": 0,
            "integrity_check": integrity,
            "chain_ok": True,
            "chain_total": 0,
            "first_bad_sequence": None,
            "prefix_equal": True,
            "schema_fingerprint": fingerprint,
            "sqlite_version": sqlite3.sqlite_version,
            "created_at_utc": _utc_now(),
        }
        sidecar = _write_provenance(dst_path, payload)
        created.append(sidecar)
        return {**payload, "provenance_file": str(sidecar)}
    except SnapshotError:
        _discard(created)
        raise
    except (LedgerError, sqlite3.Error) as exc:
        _discard(created)
        raise SnapshotError("bootstrap_failed", f"bootstrap aborted: {exc}") from exc


def _require_absent_destination(dst_path: Path) -> None:
    """Refuse to touch an existing store — no silent overwrite, no half snapshot."""
    existing = [p for p in (dst_path, provenance_path(dst_path)) if p.exists()]
    if existing:
        raise SnapshotError(
            "dest_exists",
            f"{existing[0]} already exists; choose another --dest or remove it explicitly",
        )


__all__ = ["SIDECAR_SUFFIX", "SnapshotError", "bootstrap_store", "provenance_path", "snapshot_ledger"]
