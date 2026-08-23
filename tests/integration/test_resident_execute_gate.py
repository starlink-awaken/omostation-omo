"""Integration tests for resident execution approval gate (M3.2).

验证执行闭环安全门禁:
- execute handler 非 safe: daemon 无 --yes 时 ExecutionRequested 被批准门拦截
- 有 --yes 时放行到 execution_agent handler (binding 校验 fail-closed)
- roles 分片: execute 角色只消费执行事件
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.event_ledger.broker import LedgerBroker
from omo.resident import daemon, roles


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daemon, "_ROUTES", {})
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {})
    monkeypatch.setattr(daemon, "_SAFE_HANDLERS", set())
    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", True)


@pytest.fixture
def _ledger(tmp_path: Path):
    broker = LedgerBroker.connect(tmp_path / "ledger.sqlite3")
    yield broker
    broker.close()


def _write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
        encoding="utf-8",
    )


def _exec_event(event_type: str = "ExecutionRequested") -> dict:
    return {
        "event_type": event_type,
        "workflow_run_id": "m32-approval-test",
        "event_id": f"evt_{event_type}",
        "payload": {"prompt": "测试 prompt", "timeout_seconds": 10},
    }


def _register_execute_handler(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def handler(event: dict) -> None:
        calls.append(str(event.get("event_type")))

    handler.__name__ = "spy_execution_agent"
    daemon.register_handler("execution_agent", handler, safe=False)
    return calls


def test_execution_request_blocked_without_approval(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """批准门: 无 --yes 时 ExecutionRequested 被拦截 (handler 不调用)."""
    calls = _register_execute_handler(monkeypatch)
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, [_exec_event()])
    monkeypatch.setattr(daemon, "_wm_path", lambda p: tmp_path / "watermarks" / f"{p}.json")
    daemon._load_routes(daemon._ROUTES_FILE)

    cfg = roles.ROLES["execute"]
    report = daemon.tick_once(_ledger, events_file, projector=cfg["projector"], topic_filter=set(cfg["topic_filter"]))
    assert report["processed"] == 1
    assert calls == []  # handler 被批准门拦截


def test_execution_request_passes_with_approval(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """批准门: --yes 后放行到 execution_agent handler."""
    calls = _register_execute_handler(monkeypatch)
    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", False)
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, [_exec_event()])
    monkeypatch.setattr(daemon, "_wm_path", lambda p: tmp_path / "watermarks" / f"{p}.json")
    daemon._load_routes(daemon._ROUTES_FILE)

    cfg = roles.ROLES["execute"]
    daemon.tick_once(_ledger, events_file, projector=cfg["projector"], topic_filter=set(cfg["topic_filter"]))
    assert calls == ["ExecutionRequested"]


def test_execute_role_only_consumes_execution_events(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """execute 角色只消费执行事件 (分片过滤)."""
    _register_execute_handler(monkeypatch)
    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", False)
    events_file = tmp_path / "events.jsonl"
    _write_events(
        events_file,
        [
            _exec_event("ExecutionRequested"),
            _exec_event("WorkPacketDispatched"),
            {"event_type": "WorkflowClosed", "event_id": "evt_other", "payload": {}},
        ],
    )
    monkeypatch.setattr(daemon, "_wm_path", lambda p: tmp_path / "watermarks" / f"{p}.json")
    daemon._load_routes(daemon._ROUTES_FILE)

    cfg = roles.ROLES["execute"]
    report = daemon.tick_once(_ledger, events_file, projector=cfg["projector"], topic_filter=set(cfg["topic_filter"]))
    assert report["processed"] == 2  # 只消费 2 个执行事件
