"""Unit tests for omo.resident.monitor — observability → alert 事件 → deliver (T10-16).

验证:
- publish_monitor 增量读 observability, severity∈{critical,degraded} → alert 事件进统一事件流
- info/recovered 事件被跳过
- dry-run 不推进水位; 非 dry-run 推进水位
- _alert_handler 调 alert-connectors deliver (复用 alert.py) + 沉淀告警记录 (幂等)
- 无 webhook 配置时安全失败 (fail-closed)
- register_with_daemon 注册契约 (safe)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident import alert, monitor


@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(monitor, "EVENTS_JSONL", tmp_path / "mesh" / "events.jsonl")
    monkeypatch.setattr(monitor, "ALERT_LEDGER", tmp_path / "state" / "resident-monitor.jsonl")
    # monitor 复用 alert.py 的水位/读取逻辑 → patch alert 模块全局
    monkeypatch.setattr(alert, "OBS_EVENTS", tmp_path / "observability" / "events.jsonl")
    monkeypatch.setattr(alert, "WATERMARK_FILE", tmp_path / "alert-forwarder" / "watermark.json")


def _write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
        encoding="utf-8",
    )


def _obs_event(**overrides) -> dict:
    event = {
        "severity": "critical",
        "domain": "runtime",
        "type": "system.health",
        "trace_id": "t1",
        "payload": {"check": "daemon"},
    }
    event.update(overrides)
    return event


def _alert_event(**overrides) -> dict:
    """monitor.publish_monitor 生成的 alert 事件形状."""
    event = {
        "event_id": "evt-alert-1",
        "event_type": monitor.ALERT_TYPE,
        "idempotency_key": "alert:t1",
        "occurred_at": "2026-08-26T00:00:01Z",
        "payload": {
            "source": "resident-monitor",
            "observability": {
                "event_id": None,
                "trace_id": "t1",
                "type": "system.health",
                "severity": "critical",
                "domain": "runtime",
                "title": "system.health",
                "body": '{"check": "daemon"}',
            },
            "ts": "2026-08-26T00:00:01Z",
        },
        "producer": "resident-monitor",
        "schema_version": "workflow-mesh/v1",
    }
    event.update(overrides)
    return event


def test_publish_monitor_routes_critical_to_events_jsonl(tmp_path: Path) -> None:
    _write_events(
        tmp_path / "observability" / "events.jsonl",
        [_obs_event(), _obs_event(severity="degraded", trace_id="t2"), _obs_event(severity="info", trace_id="t3")],
    )
    report = monitor.publish_monitor()
    assert report["published"] == 2
    assert report["events_scanned"] == 3

    lines = monitor.EVENTS_JSONL.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2  # info 被跳过
    ev1 = json.loads(lines[0])
    ev2 = json.loads(lines[1])
    assert ev1["event_type"] == monitor.ALERT_TYPE
    assert ev1["idempotency_key"] == "alert:t1"
    assert ev1["payload"]["observability"]["severity"] == "critical"
    assert ev2["payload"]["observability"]["severity"] == "degraded"


def test_publish_monitor_dry_run_no_write(tmp_path: Path) -> None:
    _write_events(tmp_path / "observability" / "events.jsonl", [_obs_event()])
    report = monitor.publish_monitor(dry_run=True)
    assert report["published"] == 0
    assert not monitor.EVENTS_JSONL.exists()
    assert not alert.WATERMARK_FILE.exists()  # dry-run 不推进水位


def test_publish_monitor_advances_watermark(tmp_path: Path) -> None:
    events_file = tmp_path / "observability" / "events.jsonl"
    _write_events(events_file, [_obs_event()])
    monitor.publish_monitor()
    wm = json.loads(alert.WATERMARK_FILE.read_text(encoding="utf-8"))
    assert wm["byte_offset"] == events_file.stat().st_size

    # 二次 publish 增量读 → 无新事件
    second = monitor.publish_monitor()
    assert second["events_scanned"] == 0
    assert second["published"] == 0


def test_publish_monitor_ignores_non_alert_severity(tmp_path: Path) -> None:
    _write_events(
        tmp_path / "observability" / "events.jsonl",
        [_obs_event(severity="info"), _obs_event(severity="recovered")],
    )
    report = monitor.publish_monitor(dry_run=True)
    assert report["published"] == 0


def test_alert_handler_calls_send_alert(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sent: list[dict] = []
    monkeypatch.setattr(alert, "_send_alert", lambda e: sent.append(e) or True)

    monitor._alert_handler(_alert_event())
    assert len(sent) == 1
    assert sent[0]["severity"] == "critical"
    assert sent[0]["trace_id"] == "t1"

    entry = json.loads(monitor.ALERT_LEDGER.read_text(encoding="utf-8").splitlines()[0])
    assert entry["delivered"] is True
    assert entry["idempotency_key"] == "alert:t1"


def test_alert_handler_idempotent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sent: list[dict] = []
    monkeypatch.setattr(alert, "_send_alert", lambda e: sent.append(e) or True)

    monitor._alert_handler(_alert_event())
    monitor._alert_handler(_alert_event())
    assert len(sent) == 1  # 同 idempotency_key 不重复外发
    lines = monitor.ALERT_LEDGER.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1


def test_alert_handler_ignores_other_types(tmp_path: Path) -> None:
    monitor._alert_handler({"event_type": "WorkflowClosed", "payload": {}})
    assert not monitor.ALERT_LEDGER.exists()


def test_alert_handler_safe_without_connector(tmp_path: Path) -> None:
    """无 webhook/connector 配置 → deliver 返回 False (fail-closed), 不崩溃."""
    monitor._alert_handler(_alert_event())
    entry = json.loads(monitor.ALERT_LEDGER.read_text(encoding="utf-8").splitlines()[0])
    assert entry["delivered"] is False


def test_register_with_daemon() -> None:
    class FakeDaemon:
        def __init__(self) -> None:
            self.registered: dict[str, tuple[object, bool]] = {}

        def register_handler(self, action, fn, *, safe=False) -> None:
            self.registered[action] = (fn, safe)

    fake = FakeDaemon()
    monitor.register_with_daemon(fake)
    fn, safe = fake.registered["alert"]
    assert safe is True
    assert fn is monitor._alert_handler
