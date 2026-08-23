"""Unit tests for omo.resident.alert — incremental alert forwarding.

M2.1b: 验证告警转发:
- 增量读 (byte-offset 水位)
- 字段提取回退 (type/payload, 观测事件无 title/message)
- dry-run 不推进水位; 非 dry-run 推进水位
- 无 webhook 配置时安全失败
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident import alert


@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(alert, "OBS_EVENTS", tmp_path / "observability" / "events.jsonl")
    monkeypatch.setattr(alert, "WATERMARK_FILE", tmp_path / "alert-forwarder" / "watermark.json")


def _write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
        encoding="utf-8",
    )


def _critical_event(**overrides) -> dict:
    event = {
        "severity": "critical",
        "domain": "runtime",
        "type": "system.health",
        "payload": {"check": "daemon"},
        "trace_id": "t1",
    }
    event.update(overrides)
    return event


def test_event_title_fallback_type() -> None:
    e = {"severity": "critical", "type": "governance:gate_failed"}
    assert alert._event_title(e) == "governance:gate_failed"


def test_event_title_prefers_explicit_title() -> None:
    e = {"title": "real title", "type": "system.health"}
    assert alert._event_title(e) == "real title"


def test_event_body_uses_payload() -> None:
    e = {"payload": {"check": "daemon", "detail": "x"}}
    body = alert._event_body(e)
    assert "check" in body and "daemon" in body


def test_forward_dry_run_does_not_advance_watermark(tmp_path: Path) -> None:
    _write_events(tmp_path / "observability" / "events.jsonl", [_critical_event()])
    report = alert.forward(dry_run=True)
    assert report == {"events_scanned": 1, "alerted": 1, "sent": 0}
    # dry-run 不写水位
    assert not alert.WATERMARK_FILE.exists()


def test_forward_non_dry_run_advances_watermark(tmp_path: Path) -> None:
    events_file = tmp_path / "observability" / "events.jsonl"
    _write_events(events_file, [_critical_event()])
    report = alert.forward(dry_run=False)
    assert report["alerted"] == 1
    assert report["sent"] == 0  # 无 webhook 配置 → 安全失败不发送
    wm = json.loads(alert.WATERMARK_FILE.read_text(encoding="utf-8"))
    assert wm["byte_offset"] == events_file.stat().st_size


def test_forward_ignores_non_alert_severity(tmp_path: Path) -> None:
    _write_events(
        tmp_path / "observability" / "events.jsonl",
        [{"severity": "info", "type": "system.health"}],
    )
    report = alert.forward(dry_run=True)
    assert report["alerted"] == 0


def test_forward_incremental_no_new_events(tmp_path: Path) -> None:
    events_file = tmp_path / "observability" / "events.jsonl"
    _write_events(events_file, [_critical_event()])
    alert.forward(dry_run=False)
    second = alert.forward(dry_run=True)
    assert second["events_scanned"] == 0


def test_send_alert_without_connector_safe_false(tmp_path: Path) -> None:
    # alert-connectors.py 缺失 → _load_alert_connectors 返回 None → False (不崩溃)
    result = alert._send_alert(_critical_event())
    assert result is False


def test_send_alert_invalid_event_safe() -> None:
    # 无 webhook 配置路径: 不抛异常
    assert alert._send_alert({"severity": "critical"}) is False
