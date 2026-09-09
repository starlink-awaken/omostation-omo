"""Unit tests for omo.resident.ledger_check (BET-Y1Q4-T9-01)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from omo.resident import ledger_check


def _make_ledger(path: Path) -> Path:
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    conn.close()
    return path


def test_lock_age_missing_ledger(tmp_path: Path) -> None:
    assert ledger_check.lock_age_seconds(tmp_path / "nope.sqlite3") is None


def test_lock_age_unlocked_ledger(tmp_path: Path) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")
    assert ledger_check.lock_age_seconds(ledger) is None


def test_check_lock_state_only_missing_is_typed_and_never_connects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _unexpected_connect(*_args, **_kwargs):
        raise AssertionError("missing-ledger query must not open SQLite")

    monkeypatch.setattr(ledger_check.sqlite3, "connect", _unexpected_connect)

    result = ledger_check.check_lock_state_only(tmp_path / "missing.sqlite3")

    assert result == {
        "ok": True,
        "state": "missing",
        "lock_age_seconds": None,
        "observation_mode": "read_only",
        "recovery_performed": False,
        "detail": "ledger missing",
    }


def test_check_lock_state_only_unlocked_uses_readonly_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")
    original_connect = sqlite3.connect
    calls: list[tuple[str, dict[str, object]]] = []

    def _tracking_connect(database, **kwargs):
        calls.append((str(database), dict(kwargs)))
        return original_connect(database, **kwargs)

    monkeypatch.setattr(ledger_check.sqlite3, "connect", _tracking_connect)

    result = ledger_check.check_lock_state_only(ledger)

    assert result["ok"] is True
    assert result["state"] == "unlocked"
    assert result["lock_age_seconds"] is None
    assert result["observation_mode"] == "read_only"
    assert result["recovery_performed"] is False
    assert calls
    assert all("mode=ro" in database and kwargs.get("uri") is True for database, kwargs in calls)


def test_check_lock_state_only_reports_locked_without_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")

    class _LockedConnection:
        def execute(self, _statement: str):
            return [("main", "reserved")]

        def close(self) -> None:
            return None

    monkeypatch.setattr(ledger_check.sqlite3, "connect", lambda *_args, **_kwargs: _LockedConnection())
    monkeypatch.setattr(ledger_check, "_lock_age_from_sidecars", lambda _ledger: 12.9)
    monkeypatch.setattr(
        ledger_check,
        "wal_checkpoint",
        lambda _ledger: (_ for _ in ()).throw(AssertionError("query must not checkpoint")),
    )

    result = ledger_check.check_lock_state_only(ledger)

    assert result["ok"] is False
    assert result["state"] == "locked"
    assert result["lock_age_seconds"] == 12
    assert result["observation_mode"] == "read_only"
    assert result["recovery_performed"] is False


@pytest.mark.parametrize(
    ("error", "expected_state"),
    [
        (sqlite3.OperationalError("database is locked"), "busy"),
        (sqlite3.OperationalError("disk I/O error"), "io_error"),
        (RuntimeError("unexpected probe failure"), "unknown"),
    ],
)
def test_check_lock_state_only_normalizes_open_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_state: str,
) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")

    def _raise(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(ledger_check.sqlite3, "connect", _raise)

    result = ledger_check.check_lock_state_only(ledger)

    assert result["ok"] is False
    assert result["state"] == expected_state
    assert result["observation_mode"] == "read_only"
    assert result["recovery_performed"] is False


def test_explicit_recovery_alias_preserves_existing_command() -> None:
    assert ledger_check.recover_ledger_with_wal_checkpoint is ledger_check.check_and_recover


def test_wal_checkpoint_ok(tmp_path: Path) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")
    ok, detail = ledger_check.wal_checkpoint(ledger)
    assert ok is True
    assert "ok" in detail


def test_wal_checkpoint_missing_is_ok(tmp_path: Path) -> None:
    ok, detail = ledger_check.wal_checkpoint(tmp_path / "missing.sqlite3")
    assert ok is True
    assert "missing" in detail


def test_check_and_recover_unlocked(tmp_path: Path) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")
    result = ledger_check.check_and_recover(ledger)
    assert result["ok"] is True
    assert result["lock_age_seconds"] is None
    assert result["locked"] is False


def test_maybe_kill_respects_circuit_breaker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledger_check._checkpoint_failures = 0
    result = ledger_check.maybe_kill_zombie_holders(tmp_path / "ledger.sqlite3")
    assert result["killed"] == []
    assert "budget" in result["detail"]


def test_maybe_kill_after_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = _make_ledger(tmp_path / "ledger.sqlite3")
    ledger_check._checkpoint_failures = ledger_check.MAX_CHECKPOINT_FAILURES
    monkeypatch.setattr(ledger_check, "find_sqlite_holders", lambda _p: [999001])
    monkeypatch.setattr(ledger_check, "_cpu_pct", lambda _pid, sample_seconds=2.0: 0.1)
    killed: list[int] = []

    def _fake_kill(pid: int, _sig: int) -> None:
        killed.append(pid)

    monkeypatch.setattr(ledger_check.os, "kill", _fake_kill)
    result = ledger_check.maybe_kill_zombie_holders(ledger)
    assert killed == [999001]
    assert result["killed"] == [999001]
    assert ledger_check._checkpoint_failures == 0
