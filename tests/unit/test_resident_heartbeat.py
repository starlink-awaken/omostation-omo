"""Unit tests for omo.resident.heartbeat — 活性心跳发布 + 台账沉淀 (T10-16).

验证:
- publish_heartbeat → 统一事件流出现 system.alive 事件 (payload 带健康快照)
- dry-run 不写事件流
- _heartbeat_handler 幂等沉淀 `.omo/state/resident-heartbeat.jsonl` 活性台账
- register_with_daemon 注册契约 (safe)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident import heartbeat

_SNAPSHOT = {
    "domain": "runtime",
    "event_type": "resident.status",
    "health": "recovered",
    "degraded_components": [],
    "components": {"daemon": {"ok": True}, "events": {"ok": True}},
    "ts": "2026-08-26T00:00:00Z",
}


@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(heartbeat, "EVENTS_JSONL", tmp_path / "mesh" / "events.jsonl")
    monkeypatch.setattr(heartbeat, "HEARTBEAT_LEDGER", tmp_path / "state" / "resident-heartbeat.jsonl")


@pytest.fixture(autouse=True)
def _fake_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(heartbeat, "_snapshot", lambda: dict(_SNAPSHOT))


def _system_alive_event(**overrides) -> dict:
    event = {
        "event_id": "evt-heartbeat-1",
        "event_type": heartbeat.SYSTEM_ALIVE_TYPE,
        "idempotency_key": f"heartbeat:{_SNAPSHOT['ts']}:{heartbeat.SYSTEM_ALIVE_TYPE}",
        "occurred_at": "2026-08-26T00:00:01Z",
        "payload": {
            "health": "recovered",
            "degraded_components": [],
            "components_summary": {"daemon": {"ok": True}},
            "source": "resident-heartbeat",
            "ts": _SNAPSHOT["ts"],
        },
        "producer": "resident-heartbeat",
        "schema_version": "workflow-mesh/v1",
    }
    event.update(overrides)
    return event


def test_publish_heartbeat_appends_system_alive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """整点窗口 (写流路径): 事件流出现 system.alive 事件 (payload 带健康快照)。"""
    monkeypatch.setattr(heartbeat, "_should_write_stream", lambda payload: True)
    report = heartbeat.publish_heartbeat()
    assert report == {"published": 1, "health": "recovered", "ts": _SNAPSHOT["ts"]}

    lines = heartbeat.EVENTS_JSONL.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event_type"] == heartbeat.SYSTEM_ALIVE_TYPE
    assert event["producer"] == "resident-heartbeat"
    assert event["payload"]["health"] == "recovered"
    assert event["payload"]["components_summary"]["daemon"]["ok"] is True


def test_publish_heartbeat_ledger_only_when_normal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """正常态非整点 (降采样路径): 不写事件流, 直接写台账 (动脉B 脑电采样)。"""
    monkeypatch.setattr(heartbeat, "_should_write_stream", lambda payload: False)
    report = heartbeat.publish_heartbeat()
    assert report["published"] == 0
    assert report["ledger_only"] is True
    assert report["health"] == "recovered"

    assert not heartbeat.EVENTS_JSONL.exists() or not heartbeat.EVENTS_JSONL.read_text(encoding="utf-8").strip()
    ledger_lines = heartbeat.HEARTBEAT_LEDGER.read_text(encoding="utf-8").splitlines()
    assert len(ledger_lines) == 1
    entry = json.loads(ledger_lines[0])
    assert entry["health"] == "recovered"


def test_publish_heartbeat_dry_run_no_write(tmp_path: Path) -> None:
    report = heartbeat.publish_heartbeat(dry_run=True)
    assert report["published"] == 0
    assert not heartbeat.EVENTS_JSONL.exists()


def test_heartbeat_handler_sediments_ledger(tmp_path: Path) -> None:
    heartbeat._heartbeat_handler(_system_alive_event())
    lines = heartbeat.HEARTBEAT_LEDGER.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["idempotency_key"].startswith("heartbeat:")
    assert entry["health"] == "recovered"
    assert entry["event_id"] == "evt-heartbeat-1"
    assert entry["degraded_components"] == []


def test_heartbeat_handler_idempotent(tmp_path: Path) -> None:
    heartbeat._heartbeat_handler(_system_alive_event())
    heartbeat._heartbeat_handler(_system_alive_event())
    lines = heartbeat.HEARTBEAT_LEDGER.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1  # 同 idempotency_key 不重复落账


def test_heartbeat_handler_ignores_other_types(tmp_path: Path) -> None:
    heartbeat._heartbeat_handler({"event_type": "WorkflowClosed", "payload": {}})
    assert not heartbeat.HEARTBEAT_LEDGER.exists()


def test_heartbeat_handler_healthy_and_degraded() -> None:
    """degraded 快照: 台账记录 health=degraded + 组件名."""
    degraded = dict(_SNAPSHOT)
    degraded["health"] = "degraded"
    degraded["degraded_components"] = ["ledger"]
    degraded["ts"] = "2026-08-26T00:01:00Z"
    event = _system_alive_event(
        idempotency_key=f"heartbeat:{degraded['ts']}:{heartbeat.SYSTEM_ALIVE_TYPE}",
        occurred_at="2026-08-26T00:01:01Z",
        payload={
            "health": "degraded",
            "degraded_components": ["ledger"],
            "components_summary": {"daemon": {"ok": True}},
            "source": "resident-heartbeat",
            "ts": degraded["ts"],
        },
    )
    heartbeat._heartbeat_handler(event)
    entry = json.loads(heartbeat.HEARTBEAT_LEDGER.read_text(encoding="utf-8").splitlines()[0])
    assert entry["health"] == "degraded"
    assert entry["degraded_components"] == ["ledger"]


def test_register_with_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDaemon:
        def __init__(self) -> None:
            self.registered: dict[str, tuple[object, bool]] = {}

        def register_handler(self, action, fn, *, safe=False) -> None:
            self.registered[action] = (fn, safe)

    fake = FakeDaemon()
    heartbeat.register_with_daemon(fake)
    fn, safe = fake.registered["heartbeat"]
    assert safe is True
    assert fn is heartbeat._heartbeat_handler


def test_publish_heartbeat_invokes_check_and_recover(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """BET-Y1Q4-T9-02: heartbeat publish path must call ledger_check.check_and_recover."""
    calls: list[Path] = []
    ledger = tmp_path / "event-ledger.sqlite3"
    ledger.touch()

    def _fake_recover(path: Path) -> dict:
        calls.append(path)
        return {"ok": True, "locked": False}

    monkeypatch.setattr("omo.resident.ledger_check.check_and_recover", _fake_recover)
    monkeypatch.setattr("omo.resident.status.LEDGER", ledger)
    monkeypatch.setattr(heartbeat, "_should_write_stream", lambda payload: False)
    heartbeat.publish_heartbeat()
    assert calls == [ledger]
