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
