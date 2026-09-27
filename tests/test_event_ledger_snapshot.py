"""ADR-0456 B4b batch 1 — event ledger snapshot contract tests (BET-Y2Q4-T10-208).

Everything stays inside ``tmp_path``: reading or writing the host's real state
root from a test is exactly the hermetic leak this contract forbids.  The
production-ledger run (and any ``source_ahead`` it reports while live writers
append) is measured by hand and recorded in the closeout receipt, not faked here.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

OMO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(OMO_SRC) not in sys.path:
    sys.path.insert(0, str(OMO_SRC))

from omo import omo_ledger
from omo.event_ledger import LedgerBroker, schema_fingerprint
from omo.event_ledger.broker import _EVENT_LOG_COLUMNS
from omo.event_ledger.snapshot import (
    SIDECAR_SUFFIX,
    SnapshotError,
    bootstrap_store,
    provenance_path,
    snapshot_ledger,
)


def _source(tmp_path: Path, events: int = 4) -> Path:
    path = tmp_path / "source" / "event-ledger.sqlite3"
    broker = LedgerBroker.connect(path)
    try:
        for i in range(events):
            broker.append(
                event_type="Test.v1",
                producer="test",
                principal_id="p1",
                space_id="s1",
                correlation_id=f"c{i}",
                payload={"n": i},
                idempotency_key=f"ik{i}",
            )
    finally:
        broker.close()
    return path


def _smuggle_break(path: Path) -> None:
    """Append a row out of chain, the same way test_sequence_gap_detected does.

    The append-only triggers block UPDATE/DELETE, so a raw INSERT is the only way
    to reach a ledger that cannot self-verify.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    row = dict(conn.execute("SELECT * FROM event_log ORDER BY sequence DESC LIMIT 1").fetchone())
    row["sequence"] = int(row["sequence"]) + 1
    row["event_id"] = "evt_smuggled"
    row["correlation_id"] = "c9"
    row["idempotency_key"] = "ik9"
    row["event_hash"] = "0" * 64
    conn.execute(
        f"INSERT INTO event_log({', '.join(_EVENT_LOG_COLUMNS)}) VALUES ({', '.join('?' for _ in _EVENT_LOG_COLUMNS)})",
        tuple(row[column] for column in _EVENT_LOG_COLUMNS),
    )
    conn.commit()
    conn.close()


def _fingerprint(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return schema_fingerprint(conn)
    finally:
        conn.close()


def _read_receipt(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stored_hashes(path: Path, upper: int) -> dict[int, str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT sequence, event_hash FROM event_log WHERE sequence <= ? ORDER BY sequence",
            (upper,),
        ).fetchall()
        return {int(r[0]): str(r[1]) for r in rows}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Bootstrap mode
# ---------------------------------------------------------------------------


def test_bootstrap_creates_empty_self_verifying_store(tmp_path: Path) -> None:
    dest = tmp_path / "dev" / "event-ledger.sqlite3"
    receipt = bootstrap_store(dest)

    assert dest.is_file()
    assert receipt["dest_head_sequence"] == 0
    assert receipt["chain_ok"] is True
    assert receipt["integrity_check"] == "ok"

    broker = LedgerBroker.connect(dest)
    try:
        assert broker.verify_chain()["ok"] is True
        assert broker.count() == 0
    finally:
        broker.close()


def test_bootstrap_uses_the_production_schema_entry(tmp_path: Path) -> None:
    """The dev store must be indistinguishable from a freshly created prod store."""
    prod = _source(tmp_path, events=1)
    dev = tmp_path / "dev" / "event-ledger.sqlite3"
    bootstrap_store(dev)

    assert _fingerprint(dev) == _fingerprint(prod)


def test_bootstrap_provenance_declares_itself_non_authoritative(tmp_path: Path) -> None:
    dest = tmp_path / "dev" / "event-ledger.sqlite3"
    bootstrap_store(dest)
    sidecar = provenance_path(dest)

    assert sidecar == dest.with_name(f"event-ledger.sqlite3{SIDECAR_SUFFIX}")
    payload = _read_receipt(sidecar)
    assert payload["authoritative"] is False
    assert payload["mode"] == "bootstrap"
    assert "dev profile" in payload["authority_note"]


# ---------------------------------------------------------------------------
# Snapshot mode
# ---------------------------------------------------------------------------


def test_snapshot_proves_copy_is_gap_free_prefix(tmp_path: Path) -> None:
    src = _source(tmp_path, events=6)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"

    receipt = snapshot_ledger(src, dest)

    assert receipt["integrity_check"] == "ok"
    assert receipt["chain_ok"] is True
    assert receipt["prefix_equal"] is True
    assert receipt["dest_head_sequence"] == 6
    assert receipt["source_head_sequence"] == 6
    assert receipt["source_ahead"] == 0
    assert receipt["compared_sequences"] == 6
    assert receipt["first_bad_sequence"] is None
    assert receipt["authoritative"] is False
    assert _stored_hashes(src, 6) == _stored_hashes(dest, 6)


def test_snapshot_copy_verifies_its_own_chain(tmp_path: Path) -> None:
    src = _source(tmp_path, events=3)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    snapshot_ledger(src, dest)

    broker = LedgerBroker.connect(dest)
    try:
        result = broker.verify_chain()
        assert result["ok"] is True
        assert result["first_bad_sequence"] is None
        assert result["total"] == 3
    finally:
        broker.close()


def test_snapshot_provenance_names_the_authority_it_is_not(tmp_path: Path) -> None:
    src = _source(tmp_path, events=2)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    snapshot_ledger(src, dest)

    payload = _read_receipt(provenance_path(dest))
    for key in (
        "source",
        "dest",
        "source_head_sequence",
        "dest_head_sequence",
        "source_ahead",
        "integrity_check",
        "chain_ok",
        "created_at_utc",
        "sqlite_version",
    ):
        assert key in payload, key
    assert payload["authoritative"] is False
    assert Path(payload["source"]) == src
    assert "Not the authority" in payload["authority_note"]


def test_snapshot_never_writes_the_source(tmp_path: Path) -> None:
    src = _source(tmp_path, events=3)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    before = hashlib.sha256(src.read_bytes()).hexdigest()
    wal_before = _size(src.with_name(f"{src.name}-wal"))

    snapshot_ledger(src, dest)

    assert hashlib.sha256(src.read_bytes()).hexdigest() == before
    assert _size(src.with_name(f"{src.name}-wal")) == wal_before
    # A live writer must still be able to append immediately — no lock retained,
    # no checkpoint that would have truncated someone else's WAL.
    broker = LedgerBroker.connect(src)
    try:
        seq = broker.append(
            event_type="Test.v1",
            producer="test",
            principal_id="p1",
            space_id="s1",
            correlation_id="late",
            payload={},
            idempotency_key="ik-late",
        )
        assert seq == 4
        assert broker.verify_chain()["ok"] is True
    finally:
        broker.close()


def test_copy_stays_a_prefix_after_the_source_appends(tmp_path: Path) -> None:
    """The migration-boundary proof: appending later never invalidates the copy."""
    src = _source(tmp_path, events=4)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    snapshot_ledger(src, dest)

    broker = LedgerBroker.connect(src)
    try:
        broker.append(
            event_type="Test.v1",
            producer="test",
            principal_id="p1",
            space_id="s1",
            correlation_id="c-after",
            payload={},
            idempotency_key="ik-after",
        )
    finally:
        broker.close()

    assert _stored_hashes(src, 4) == _stored_hashes(dest, 4)
    conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        assert int(conn.execute("SELECT MAX(sequence) FROM event_log").fetchone()[0]) == 5
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fail-closed behaviour
# ---------------------------------------------------------------------------


def test_snapshot_refuses_existing_destination_untouched(tmp_path: Path) -> None:
    src = _source(tmp_path, events=2)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"pre-existing ledger")

    with pytest.raises(SnapshotError) as excinfo:
        snapshot_ledger(src, dest)

    assert excinfo.value.reason == "dest_exists"
    assert dest.read_bytes() == b"pre-existing ledger"


def test_snapshot_refuses_a_stale_sidecar_beside_a_free_path(tmp_path: Path) -> None:
    src = _source(tmp_path, events=1)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    provenance_path(dest).parent.mkdir(parents=True, exist_ok=True)
    provenance_path(dest).write_text("{}", encoding="utf-8")

    with pytest.raises(SnapshotError) as excinfo:
        snapshot_ledger(src, dest)

    assert excinfo.value.reason == "dest_exists"


def test_snapshot_refuses_source_that_is_not_a_ledger_and_leaves_nothing(tmp_path: Path) -> None:
    src = tmp_path / "stray.sqlite3"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE unrelated (id INTEGER)")
    conn.commit()
    conn.close()
    dest = tmp_path / "prod" / "event-ledger.sqlite3"

    with pytest.raises(SnapshotError) as excinfo:
        snapshot_ledger(src, dest)

    assert excinfo.value.reason == "source_not_a_ledger"
    assert not dest.exists()
    assert not provenance_path(dest).exists()


def test_snapshot_refuses_missing_source(tmp_path: Path) -> None:
    with pytest.raises(SnapshotError) as excinfo:
        snapshot_ledger(tmp_path / "gone.sqlite3", tmp_path / "prod" / "event-ledger.sqlite3")
    assert excinfo.value.reason == "source_missing"


def test_snapshot_refuses_to_overwrite_itself(tmp_path: Path) -> None:
    src = _source(tmp_path, events=1)
    with pytest.raises(SnapshotError) as excinfo:
        snapshot_ledger(src, src)
    assert excinfo.value.reason == "source_equals_dest"


def test_snapshot_of_a_broken_source_leaves_no_half_snapshot(tmp_path: Path) -> None:
    src = _source(tmp_path, events=2)
    _smuggle_break(src)
    dest = tmp_path / "prod" / "event-ledger.sqlite3"

    with pytest.raises(SnapshotError) as excinfo:
        snapshot_ledger(src, dest)

    assert excinfo.value.reason == "chain_broken_in_copy"
    assert not dest.exists()
    assert not provenance_path(dest).exists()


def test_bootstrap_refuses_existing_destination(tmp_path: Path) -> None:
    dest = tmp_path / "dev" / "event-ledger.sqlite3"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"keep me")

    with pytest.raises(SnapshotError) as excinfo:
        bootstrap_store(dest)

    assert excinfo.value.reason == "dest_exists"
    assert dest.read_bytes() == b"keep me"


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------


def test_cli_bootstrap_writes_receipt(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    dest = tmp_path / "dev" / "event-ledger.sqlite3"

    code = omo_ledger.main(["snapshot", "--bootstrap", "--dest", str(dest), "--json"])

    assert code == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["ok"] is True
    assert receipt["dest"] == str(dest.resolve())
    assert dest.is_file()


def test_cli_requires_exactly_one_mode(tmp_path: Path) -> None:
    dest = tmp_path / "prod" / "event-ledger.sqlite3"
    assert omo_ledger.main(["snapshot", "--dest", str(dest), "--json"]) == 1
    assert not dest.exists()

    src = _source(tmp_path, events=1)
    other = tmp_path / "prod" / "other.sqlite3"
    assert omo_ledger.main(["snapshot", "--source", str(src), "--bootstrap", "--dest", str(other), "--json"]) == 1
    assert not other.exists()


def test_cli_snapshot_does_not_create_the_default_ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The surface is built after dispatch; snapshot must not drag it in.

    Constructing ``EventLedgerSurface`` opens — and therefore creates — the
    default workspace ledger, so a path-less command that reaches it would
    silently initialize a real database the operator never asked for.
    """
    workspace = tmp_path / "ws"
    monkeypatch.setenv("WORKSPACE_ROOT", str(workspace))
    monkeypatch.delenv("OMO_EVENT_LEDGER_DB", raising=False)
    default_db = workspace / "runtime" / "omo" / "event-ledger.sqlite3"
    dest = tmp_path / "prod" / "event-ledger.sqlite3"

    assert omo_ledger.main(["snapshot", "--dest", str(dest), "--json"]) == 1
    assert not default_db.exists()

    src = _source(tmp_path, events=1)
    assert omo_ledger.main(["snapshot", "--source", str(src), "--dest", str(dest), "--json"]) == 0
    assert not default_db.exists()


@pytest.mark.parametrize(
    ("kind", "reason"),
    [("text", "source_unreadable"), ("stray-db", "source_not_a_ledger")],
)
def test_cli_snapshot_reports_stable_reason_on_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    kind: str,
    reason: str,
) -> None:
    src = tmp_path / "not-a-ledger.db"
    if kind == "text":
        src.write_text("nope", encoding="utf-8")
    else:
        conn = sqlite3.connect(str(src))
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()
    dest = tmp_path / "prod" / "event-ledger.sqlite3"

    code = omo_ledger.main(["snapshot", "--source", str(src), "--dest", str(dest)])

    assert code == 1
    receipt = json.loads(capsys.readouterr().err)
    assert receipt["ok"] is False
    assert receipt["reason"] == reason
    assert not dest.exists()


def _size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0
