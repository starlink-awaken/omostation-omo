"""Unit tests for omo.resident.status — resident runtime status snapshot.

M2.4: 验证状态快照:
- 组件字段完整 (daemon/events/sediment/alert/ledger)
- daemon 水位新鲜度 → health
- ledger 链完整性 (read-only probe, zero write locks)
"""

from __future__ import annotations

import json
import multiprocessing
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
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
        " previous_hash TEXT"
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


def test_snapshot_daemon_fresh_is_ok(_snapshot_paths: Path) -> None:
    report = status.snapshot()
    assert report["components"]["daemon"]["ok"] is True
    assert report["components"]["daemon"]["byte_offset"] == 123
    assert report["health"] == "ok"


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
    assert report["components"]["ledger"]["observation_mode"] == "read_only"
    assert report["components"]["ledger"]["recovery_performed"] is False
    assert report["components"]["ledger"]["lock_monitor"] == {
        "locked": False,
        "checkpoint": None,
        "recovery": None,
        "detail": "ledger missing",
        "state": "missing",
        "observation_mode": "read_only",
        "recovery_performed": False,
    }
    assert report["health"] == "ok"


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
    assert report["health"] == "ok"
    assert "daemon" not in report["degraded_components"]
    assert "ledger" not in report["degraded_components"]


def test_main_returns_zero_for_ok_health(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    monkeypatch.setattr(status, "snapshot", lambda: {"health": "ok"})

    assert status.main([]) == 0
    assert json.loads(capsys.readouterr().out) == {"health": "ok"}


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
    result = status._probe_ledger_once()
    assert result["ok"] is True
    assert result["sequence"] == 3
    assert "read-only probe" in result["detail"]


def test_ledger_probe_readonly_null_genesis(_snapshot_paths: Path) -> None:
    """Production writers store genesis previous_hash as SQL NULL."""
    from omo.resident import status as st

    _create_test_ledger(
        st.LEDGER,
        rows=[
            (1, "h1", None),  # type: ignore[list-item]
            (2, "h2", "h1"),
        ],
    )
    # _create_test_ledger may reject None — open and rewrite if needed
    import sqlite3

    conn = sqlite3.connect(st.LEDGER)
    try:
        conn.execute("DELETE FROM event_log")
        conn.execute(
            "INSERT INTO event_log (sequence, event_hash, previous_hash) VALUES (?, ?, ?)",
            (1, "h1", None),
        )
        conn.execute(
            "INSERT INTO event_log (sequence, event_hash, previous_hash) VALUES (?, ?, ?)",
            (2, "h2", "h1"),
        )
        conn.commit()
    finally:
        conn.close()
    report = st.snapshot()
    assert report["components"]["ledger"]["ok"] is True
    assert report["components"]["ledger"]["recovery_performed"] is False


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


def test_ledger_snapshot_observes_lock_and_chain_once_without_recovery_or_sleep(
    _snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omo.resident import ledger_check

    status.LEDGER.touch()
    calls = {"lock": 0, "probe": 0, "sleep": 0, "recovery": 0}

    def _lock_state(_ledger: Path) -> dict:
        calls["lock"] += 1
        return {
            "ok": False,
            "state": "busy",
            "lock_age_seconds": None,
            "observation_mode": "read_only",
            "recovery_performed": False,
            "detail": "database is busy",
        }

    def _probe() -> dict:
        calls["probe"] += 1
        return {"ok": False, "detail": "database is busy", "sequence": None}

    def _unexpected_recovery(_ledger: Path) -> dict:
        calls["recovery"] += 1
        raise AssertionError("status Query must not call recovery")

    monkeypatch.setattr(ledger_check, "check_lock_state_only", _lock_state)
    monkeypatch.setattr(ledger_check, "check_and_recover", _unexpected_recovery)
    monkeypatch.setattr(ledger_check, "recover_ledger_with_wal_checkpoint", _unexpected_recovery)
    monkeypatch.setattr(
        ledger_check,
        "maybe_kill_zombie_holders",
        lambda _ledger: (_ for _ in ()).throw(AssertionError("status Query must not kill holders")),
    )
    monkeypatch.setattr(status, "_probe_ledger_once", _probe)
    monkeypatch.setattr(status.time, "sleep", lambda _delay: calls.__setitem__("sleep", calls["sleep"] + 1))

    result = status._ledger_snapshot()

    assert result["ok"] is False
    assert result["lock_monitor"]["state"] == "busy"
    assert result["lock_monitor"]["observation_mode"] == "read_only"
    assert result["lock_monitor"]["recovery_performed"] is False
    assert result["lock_monitor"]["checkpoint"] is None
    assert result["lock_monitor"]["recovery"] is None
    assert calls == {"lock": 1, "probe": 1, "sleep": 0, "recovery": 0}


@pytest.mark.parametrize(
    ("state", "detail", "lock_age", "expected_locked"),
    [
        ("locked", "ledger lock observed for 12s", 12, True),
        ("io_error", "read-only lock observation failed: disk I/O error", None, None),
        ("unknown", "read-only lock observation failed: unexpected probe failure", None, None),
    ],
)
def test_snapshot_maps_non_ok_lock_observations_without_mutation(
    _snapshot_paths: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    detail: str,
    lock_age: int | None,
    expected_locked: bool | None,
) -> None:
    from omo.resident import ledger_check

    status.LEDGER.touch()
    calls = {"lock": 0, "probe": 0}

    def _lock_state(_ledger: Path) -> dict:
        calls["lock"] += 1
        return {
            "ok": False,
            "state": state,
            "lock_age_seconds": lock_age,
            "observation_mode": "read_only",
            "recovery_performed": False,
            "detail": detail,
        }

    def _probe() -> dict:
        calls["probe"] += 1
        return {"ok": True, "detail": "chain ok (read-only probe)", "sequence": 2}

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("resident status reached a mutation-capable helper")

    monkeypatch.setattr(ledger_check, "check_lock_state_only", _lock_state)
    monkeypatch.setattr(status, "_probe_ledger_once", _probe)
    monkeypatch.setattr(ledger_check, "check_and_recover", _forbidden)
    monkeypatch.setattr(ledger_check, "recover_ledger_with_wal_checkpoint", _forbidden)
    monkeypatch.setattr(ledger_check, "wal_checkpoint", _forbidden)
    monkeypatch.setattr(ledger_check, "maybe_kill_zombie_holders", _forbidden)
    monkeypatch.setattr(ledger_check, "find_sqlite_holders", _forbidden)
    monkeypatch.setattr(ledger_check, "_cpu_pct", _forbidden)
    monkeypatch.setattr(ledger_check.os, "kill", _forbidden)

    report = status.snapshot()
    ledger = report["components"]["ledger"]

    assert report["health"] == "degraded"
    assert report["degraded_components"] == ["ledger"]
    assert ledger["ok"] is False
    assert ledger["detail"] == detail
    assert ledger["lock_age_seconds"] == lock_age
    assert ledger["observation_mode"] == "read_only"
    assert ledger["recovery_performed"] is False
    assert ledger["lock_monitor"] == {
        "locked": expected_locked,
        "checkpoint": None,
        "recovery": None,
        "detail": detail,
        "state": state,
        "observation_mode": "read_only",
        "recovery_performed": False,
    }
    assert calls == {"lock": 1, "probe": 1}


def test_ledger_snapshot_non_lock_error_is_single_observation(
    _snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omo.resident import ledger_check

    status.LEDGER.touch()
    calls = {"lock": 0, "probe": 0}

    def _lock_state(_ledger: Path) -> dict:
        calls["lock"] += 1
        return {
            "ok": True,
            "state": "unlocked",
            "lock_age_seconds": None,
            "observation_mode": "read_only",
            "recovery_performed": False,
            "detail": "ledger unlocked",
        }

    def _probe() -> dict:
        calls["probe"] += 1
        return {"ok": False, "detail": "read-only probe failed: disk I/O error", "sequence": None}

    monkeypatch.setattr(ledger_check, "check_lock_state_only", _lock_state)
    monkeypatch.setattr(status, "_probe_ledger_once", _probe)
    monkeypatch.setattr(
        ledger_check,
        "check_and_recover",
        lambda _ledger: (_ for _ in ()).throw(AssertionError("status Query must not call recovery")),
    )

    result = status._ledger_snapshot()

    assert result["ok"] is False
    assert "disk I/O error" in result["detail"]
    assert calls == {"lock": 1, "probe": 1}


def test_snapshot_query_cannot_reach_ledger_mutators(_snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omo.resident import ledger_check

    _create_test_ledger(status.LEDGER, [(1, "h1", "")])

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("resident status reached a mutation-capable helper")

    monkeypatch.setattr(ledger_check, "check_and_recover", _forbidden)
    monkeypatch.setattr(ledger_check, "recover_ledger_with_wal_checkpoint", _forbidden)
    monkeypatch.setattr(ledger_check, "wal_checkpoint", _forbidden)
    monkeypatch.setattr(ledger_check, "maybe_kill_zombie_holders", _forbidden)
    monkeypatch.setattr(ledger_check, "find_sqlite_holders", _forbidden)
    monkeypatch.setattr(ledger_check, "_cpu_pct", _forbidden)
    monkeypatch.setattr(ledger_check.os, "kill", _forbidden)

    report = status.snapshot()

    assert report["components"]["ledger"]["ok"] is True
    assert report["components"]["ledger"]["lock_monitor"]["observation_mode"] == "read_only"
    assert report["components"]["ledger"]["lock_monitor"]["recovery_performed"] is False


def test_one_hundred_concurrent_full_snapshots_are_bounded_and_side_effect_free(
    _snapshot_paths: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omo.resident import ledger_check

    _create_test_ledger(status.LEDGER, [(1, "h1", ""), (2, "h2", "h1")])
    original_connect = sqlite3.connect
    connect_calls: list[tuple[str, dict[str, object]]] = []
    connect_lock = threading.Lock()
    sleep_calls: list[float] = []

    def _tracking_connect(database, **kwargs):
        with connect_lock:
            connect_calls.append((str(database), dict(kwargs)))
        return original_connect(database, **kwargs)

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("resident status reached a mutation-capable helper")

    monkeypatch.setattr(sqlite3, "connect", _tracking_connect)
    monkeypatch.setattr(status.time, "sleep", lambda delay: sleep_calls.append(float(delay)))
    monkeypatch.setattr(ledger_check, "check_and_recover", _forbidden)
    monkeypatch.setattr(ledger_check, "recover_ledger_with_wal_checkpoint", _forbidden)
    monkeypatch.setattr(ledger_check, "wal_checkpoint", _forbidden)
    monkeypatch.setattr(ledger_check, "maybe_kill_zombie_holders", _forbidden)
    monkeypatch.setattr(ledger_check, "find_sqlite_holders", _forbidden)
    monkeypatch.setattr(ledger_check, "_cpu_pct", _forbidden)
    monkeypatch.setattr(ledger_check.os, "kill", _forbidden)

    context = multiprocessing.get_context("fork")
    result_queue = context.Queue()

    def _run_batch() -> None:
        try:
            barrier = threading.Barrier(101)

            def _full_snapshot() -> dict:
                barrier.wait(timeout=5)
                return status.snapshot()

            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=100) as pool:
                futures = [pool.submit(_full_snapshot) for _ in range(100)]
                barrier.wait(timeout=5)
                done, not_done = wait(futures, timeout=9)
            reports = [future.result() for future in done]
            result_queue.put(
                {
                    "duration": time.monotonic() - started,
                    "report_count": len(reports),
                    "not_done": len(not_done),
                    "sleep_count": len(sleep_calls),
                    "connect_count": len(connect_calls),
                    "connections_read_only": all(
                        "mode=ro" in database and kwargs.get("uri") is True for database, kwargs in connect_calls
                    ),
                    "reports_valid": all(
                        json.loads(json.dumps(report))["components"]["ledger"]["ok"] is True for report in reports
                    ),
                }
            )
        except BaseException as exc:  # noqa: BLE001 - transport worker failure to parent assertion.
            result_queue.put({"error": repr(exc)})

    process = context.Process(target=_run_batch)
    process.start()
    process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
        pytest.fail("100 full snapshots exceeded the hard 10s process budget")

    assert process.exitcode == 0
    result = result_queue.get(timeout=1)
    assert "error" not in result, result.get("error")
    assert result["not_done"] == 0
    assert result["report_count"] == 100
    assert result["duration"] < 10
    assert result["sleep_count"] == 0
    assert result["connect_count"] > 0
    assert result["connections_read_only"] is True
    assert result["reports_valid"] is True
