"""Unit tests for omo.resident.status — resident runtime status snapshot.

M2.4: 验证状态快照:
- 组件字段完整 (daemon/events/sediment/alert/ledger)
- daemon 水位新鲜度 → health
- ledger 链完整性
"""

from __future__ import annotations

import json
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
    # 无事件/sediment/ledger 时不应崩溃
    report = status.snapshot()
    assert report["components"]["events"]["lines"] == 0
    assert report["components"]["ledger"]["ok"] is False
