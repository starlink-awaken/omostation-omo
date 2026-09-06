"""Unit tests for omo.resident.status — resident runtime status snapshot.

M2.4: 验证状态快照:
- 组件字段完整 (daemon/events/sediment/alert/ledger)
- daemon 水位新鲜度 → health
- ledger 链完整性 (read-only probe, zero write locks)
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from omo.resident import status


@pytest.fixture
def _snapshot_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    delivery = tmp_path / "_delivery"
    delivery.mkdir()
    wm_dir = delivery / "resident-orchestrator" / "watermarks"
    wm_dir.mkdir(parents=True)
    # 角色水位 (daemon --once --role 每 2min 推进) — 活性判定的唯一依据
    (wm_dir / "resident-sediment.json").write_text(json.dumps({"byte_offset": 123}), encoding="utf-8")
    # 订阅层水位 (subscribe 非 cron daemon tick 证据, 更新频率低) — 必须被排除
    (wm_dir / "resident-sub.json").write_text(json.dumps({"byte_offset": 999}), encoding="utf-8")
    monkeypatch.setattr(status, "DAEMON_WATERMARKS", wm_dir)
    monkeypatch.setattr(status, "EVENTS_JSONL", tmp_path / "events.jsonl")
    monkeypatch.setattr(status, "SEDIMENT_ROOT", tmp_path / "sediment")
    monkeypatch.setattr(status, "ALERT_WATERMARK", delivery / "alert-forwarder" / "watermark.json")
    monkeypatch.setattr(status, "LEDGER", tmp_path / "ledger.sqlite3")
    return delivery


def _create_test_ledger(path: Path, rows: list[tuple[int, str, str]] | None = None) -> None:
    """Create a minimal SQLite ledger for testing (outside the status module)."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS event_log ("
        " sequence INTEGER PRIMARY KEY,"
        " event_hash TEXT NOT NULL,"
        " previous_hash TEXT NOT NULL"
        ")"
    )
    if rows:
        conn.executemany(
            "INSERT INTO event_log (sequence, event_hash, previous_hash) VALUES (?, ?, ?)",
            rows,
        )
    conn.commit()
    conn.close()


def test_snapshot_structure(_snapshot_paths: Path, tmp_path: Path) -> None:
    report = status.snapshot()
    assert set(report["components"]) == {"daemon", "events", "sediment", "alert", "ledger"}
    assert report["event_type"] == "resident.status"


def test_snapshot_daemon_fresh_is_recovered(_snapshot_paths: Path) -> None:
    report = status.snapshot()
    assert report["components"]["daemon"]["ok"] is True
    assert report["components"]["daemon"]["byte_offset"] == 123


def test_snapshot_daemon_stale_is_degraded(_snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    wm_file = _snapshot_paths / "resident-orchestrator" / "watermarks" / "resident-sediment.json"
    import os
    import time

    old = time.time() - 99999
    os.utime(wm_file, (old, old))
    report = status.snapshot()
    assert report["components"]["daemon"]["ok"] is False
    assert report["health"] == "degraded"
    assert "daemon" in report["degraded_components"]


def test_snapshot_daemon_sub_stale_role_fresh_is_recovered(
    _snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """回归: 订阅层 sub 水位陈旧但角色水位新鲜 → 不应误判 degraded."""
    sub_file = _snapshot_paths / "resident-orchestrator" / "watermarks" / "resident-sub.json"
    import os
    import time

    old = time.time() - 99999
    os.utime(sub_file, (old, old))
    report = status.snapshot()
    assert report["components"]["daemon"]["ok"] is True
    assert report["components"]["daemon"]["watermark_file"] == "resident-sediment.json"
    assert "daemon" not in report["degraded_components"]


def test_snapshot_sediment_counts(_snapshot_paths: Path, tmp_path: Path) -> None:
    (tmp_path / "sediment" / "runs").mkdir(parents=True)
    (tmp_path / "sediment" / "failures").mkdir(parents=True)
    (tmp_path / "sediment" / "runs" / "a.md").write_text("x", encoding="utf-8")
    (tmp_path / "sediment" / "failures" / "b.md").write_text("x", encoding="utf-8")
    report = status.snapshot()
    assert report["components"]["sediment"]["runs"] == 1
    assert report["components"]["sediment"]["failures"] == 1
    assert report["components"]["sediment"]["total"] == 2


def test_snapshot_no_files_not_crash(_snapshot_paths: Path) -> None:
    # 无事件/sediment/ledger 时不应崩溃；缺 ledger 冷启动非致命 (T9-01)
    report = status.snapshot()
    assert report["components"]["events"]["lines"] == 0
    assert report["components"]["ledger"]["ok"] is True
    assert report["components"]["ledger"]["lock_age_seconds"] is None
    assert report["components"]["ledger"].get("missing") is True


def test_snapshot_cold_daemon_non_fatal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cold start (no watermark) must not degrade health (BET-Y1Q4-T9-01 Spec)."""
    wm_dir = tmp_path / "watermarks"
    wm_dir.mkdir()
    monkeypatch.setattr(status, "DAEMON_WATERMARKS", wm_dir)
    monkeypatch.setattr(status, "EVENTS_JSONL", tmp_path / "events.jsonl")
    monkeypatch.setattr(status, "SEDIMENT_ROOT", tmp_path / "sediment")
    monkeypatch.setattr(status, "ALERT_WATERMARK", tmp_path / "watermark.json")
    monkeypatch.setattr(status, "LEDGER", tmp_path / "ledger.sqlite3")
    report = status.snapshot()
    assert report["components"]["daemon"]["ok"] is True
    assert report["components"]["daemon"].get("cold_start") is True
    assert report["components"]["ledger"]["ok"] is True
    assert report["health"] in {"recovered", "ok"}
    assert "daemon" not in report["degraded_components"]
    assert "ledger" not in report["degraded_components"]


# ── Zero-lock probe tests (BET-Y1Q4-T10-126) ─────────────────────────────


def test_ledger_probe_readonly_ok(_snapshot_paths: Path) -> None:
    """Read-only probe with valid chain returns ok."""
    ledger = status.LEDGER
    rows = [
        (1, "hash_a", ""),
        (2, "hash_b", "hash_a"),
        (3, "hash_c", "hash_b"),
    ]
    _create_test_ledger(ledger, rows)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "omo.resident.ledger_check.check_and_recover",
        lambda _ledger: {"ok": True, "lock_age_seconds": None, "locked": False, "detail": "unlocked"},
    )
    result = status._probe_ledger_once()
    assert result["ok"] is True
    assert result["sequence"] == 3
    assert "read-only probe" in result["detail"]
    monkeypatch.undo()


def test_ledger_probe_readonly_broken_chain(_snapshot_paths: Path) -> None:
    """Read-only probe detects broken chain."""
    ledger = status.LEDGER
    rows = [
        (1, "hash_a", ""),
        (2, "hash_b", "WRONG_PREV"),  # broken chain
    ]
    _create_test_ledger(ledger, rows)
    result = status._probe_ledger_once()
    assert result["ok"] is False
    assert "chain broken" in result["detail"]
    assert result["sequence"] == 2


def test_ledger_probe_readonly_empty_ledger(_snapshot_paths: Path) -> None:
    """Read-only probe handles empty ledger (cold start)."""
    ledger = status.LEDGER
    _create_test_ledger(ledger)
    result = status._probe_ledger_once()
    assert result["ok"] is True
    assert result["sequence"] == 0
    assert "cold start" in result["detail"]


def test_ledger_probe_readonly_no_write_lock(_snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that _probe_ledger_once never opens a write connection."""
    import unittest.mock as mock

    original_connect = sqlite3.connect
    write_attempts: list[str] = []

    def tracking_connect(*args, **kwargs):
        # Track if any non-URI or write connection is attempted
        if args and isinstance(args[0], str) and "mode=ro" not in args[0] and not kwargs.get("uri"):
            write_attempts.append(args[0])
        return original_connect(*args, **kwargs)

    ledger = status.LEDGER
    _create_test_ledger(ledger, [(1, "h1", "")])

    with mock.patch("sqlite3.connect", side_effect=tracking_connect):
        result = status._probe_ledger_once()

    assert result["ok"] is True
    assert write_attempts == [], f"Write connections attempted: {write_attempts}"


def test_ledger_snapshot_retry_on_lock(_snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ledger snapshot retries on transient lock errors in read-only probe."""
    call_count = [0]

    def flaky_probe():
        call_count[0] += 1
        if call_count[0] <= 2:
            return {"ok": False, "detail": "database is locked", "sequence": None}
        return {"ok": True, "detail": "chain ok (read-only probe)", "sequence": 42}

    status.LEDGER.touch()
    monkeypatch.setattr(status, "_probe_ledger_once", flaky_probe)
    monkeypatch.setattr(status.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        "omo.resident.ledger_check.check_and_recover",
        lambda _: {"ok": True, "lock_age_seconds": None, "locked": False, "detail": "unlocked"},
    )

    result = status._ledger_snapshot()
    assert result["ok"] is True
    assert result["sequence"] == 42
    assert call_count[0] == 3


def test_ledger_snapshot_exhausts_retry_budget(_snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ledger snapshot gives up after LEDGER_RETRY_ATTEMPTS lock errors."""
    status.LEDGER.touch()
    monkeypatch.setattr(
        status,
        "_probe_ledger_once",
        lambda: {"ok": False, "detail": "database is locked", "sequence": None},
    )
    monkeypatch.setattr(status.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        "omo.resident.ledger_check.check_and_recover",
        lambda _: {"ok": True, "lock_age_seconds": 12, "locked": True, "detail": "within budget"},
    )

    result = status._ledger_snapshot()
    assert result["ok"] is False
    assert "retry budget exhausted" in result["detail"]
    assert result["lock_age_seconds"] == 12


def test_ledger_snapshot_no_retry_on_non_lock_error(_snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-lock errors are not retried."""
    status.LEDGER.touch()
    monkeypatch.setattr(
        status,
        "_probe_ledger_once",
        lambda: {"ok": False, "detail": "read-only probe failed: disk I/O error", "sequence": None},
    )
    monkeypatch.setattr(
        "omo.resident.ledger_check.check_and_recover",
        lambda _: {"ok": True, "lock_age_seconds": None, "locked": False, "detail": "unlocked"},
    )

    result = status._ledger_snapshot()
    assert result["ok"] is False
    assert "disk I/O error" in result["detail"]


def test_concurrent_readonly_probes_no_interference(tmp_path: Path) -> None:
    """Multiple concurrent read-only probes don't interfere (zero contention)."""
    import threading

    ledger = tmp_path / "test_ledger.sqlite3"
    _create_test_ledger(ledger, [(1, "h1", ""), (2, "h2", "h1")])

    results: list[dict] = []
    errors: list[Exception] = []

    def probe():
        try:
            uri = f"file:{ledger}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=0.05)
            rows = list(conn.execute("SELECT sequence, event_hash, previous_hash FROM event_log ORDER BY sequence"))
            conn.close()
            results.append({"ok": True, "count": len(rows)})
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=probe) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == [], f"Errors during concurrent probes: {errors}"
    assert len(results) == 10
    assert all(r["ok"] and r["count"] == 2 for r in results)
