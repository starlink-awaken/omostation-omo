"""Unit tests for omo.resident.daemon — routing, conditions, byte-offset resume.

M2.1: 覆盖 daemon 核心逻辑:
- _condition_holds 受限条件求值 (fail-closed)
- _load_routes 规则加载 (fail-closed)
- _route 事件路由 → handler / placeholder / 批准门拦截
- _read_incremental byte-offset 增量读 + 截断回退
- tick_once checkpoint 推进 + topic_filter + 幂等
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.event_ledger.broker import LedgerBroker
from omo.resident import daemon


@pytest.fixture(autouse=True)
def _isolate_globals(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset daemon module globals so tests do not leak state."""
    monkeypatch.setattr(daemon, "_ROUTES", {})
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {})
    monkeypatch.setattr(daemon, "_SAFE_HANDLERS", set())
    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", True)


# ── _condition_holds ──────────────────────────────────────────────────────────


def test_condition_holds_eq_true() -> None:
    event = {"event_type": "X", "payload": {"status": "success"}}
    assert daemon._condition_holds("payload.status == 'success'", event) is True


def test_condition_holds_eq_false() -> None:
    event = {"event_type": "X", "payload": {"status": "failed"}}
    assert daemon._condition_holds("payload.status == 'success'", event) is False


def test_condition_holds_missing_payload_field_false() -> None:
    event = {"event_type": "X", "payload": {"other": 1}}
    assert daemon._condition_holds("payload.status == 'success'", event) is False


def test_condition_holds_in_membership() -> None:
    event = {"event_type": "X", "payload": {"risk": "critical"}}
    assert daemon._condition_holds("payload.risk in ('high', 'critical')", event) is True


def test_condition_holds_in_membership_not_match() -> None:
    event = {"event_type": "X", "payload": {"risk": "low"}}
    assert daemon._condition_holds("payload.risk in ('high', 'critical')", event) is False


def test_condition_holds_unsupported_expr_fail_closed() -> None:
    # 函数调用 / 变量 / 复杂表达式一律拒绝 (fail-closed)
    event = {"event_type": "X", "payload": {"status": "success"}}
    assert daemon._condition_holds("__import__('os').system('echo')", event) is False
    assert daemon._condition_holds("payload.status", event) is False
    assert daemon._condition_holds("payload.status + 'x'", event) is False
    assert daemon._condition_holds("payload.status == 'success' and True", event) is False


def test_condition_holds_invalid_syntax_fail_closed() -> None:
    assert daemon._condition_holds("payload.status ===", {"payload": {}}) is False
    assert daemon._condition_holds("", {"payload": {}}) is False


# ── _load_routes ──────────────────────────────────────────────────────────────


def test_load_routes_valid(tmp_path: Path) -> None:
    routes = tmp_path / "routes.yaml"
    routes.write_text(
        "routes:\n  - event_type: WorkflowClosed\n    action: knowledge_sediment\n    safe: true\n",
        encoding="utf-8",
    )
    result = daemon._load_routes(routes)
    assert set(result) == {"WorkflowClosed"}
    assert result["WorkflowClosed"]["action"] == "knowledge_sediment"
    assert daemon._ROUTES == result  # 全局写回


def test_load_routes_missing_file(tmp_path: Path) -> None:
    assert daemon._load_routes(tmp_path / "nope.yaml") == {}


def test_load_routes_invalid_yaml_raises(tmp_path: Path) -> None:
    routes = tmp_path / "routes.yaml"
    routes.write_text("routes: [unclosed\n  - bad", encoding="utf-8")
    with pytest.raises(Exception):
        daemon._load_routes(routes)


def test_load_routes_missing_routes_list_raises(tmp_path: Path) -> None:
    routes = tmp_path / "routes.yaml"
    routes.write_text("schema_version: resident-routes/v1\n", encoding="utf-8")
    # 缺 routes 键 → 视为空规则表 (fail-closed 为无规则), 不抛错
    assert daemon._load_routes(routes) == {}


def test_load_routes_routes_not_list_raises(tmp_path: Path) -> None:
    routes = tmp_path / "routes.yaml"
    routes.write_text("routes: not-a-list\n", encoding="utf-8")
    with pytest.raises(ValueError, match="routes list"):
        daemon._load_routes(routes)


def test_load_routes_rule_missing_event_type_raises(tmp_path: Path) -> None:
    routes = tmp_path / "routes.yaml"
    routes.write_text("routes:\n  - action: knowledge_sediment\n", encoding="utf-8")
    with pytest.raises(ValueError, match="event_type"):
        daemon._load_routes(routes)


# ── _route ────────────────────────────────────────────────────────────────────


def _recorder() -> tuple[list[dict], list[dict]]:
    """Return (calls, captured).  Calls holds event_type strings seen by handler."""
    calls: list[str] = []

    def handler(event: dict) -> None:
        calls.append(str(event.get("event_type")))

    return calls, handler


def test_route_to_safe_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, handler = _recorder()
    monkeypatch.setattr(
        daemon,
        "_ROUTES",
        {"WorkflowClosed": {"event_type": "WorkflowClosed", "action": "my_handler", "safe": True}},
    )
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"my_handler": handler})
    daemon._route({"event_type": "WorkflowClosed", "payload": {}})
    assert calls == ["WorkflowClosed"]


def test_route_condition_not_met_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, handler = _recorder()
    monkeypatch.setattr(
        daemon,
        "_ROUTES",
        {
            "WorkflowClosed": {
                "event_type": "WorkflowClosed",
                "condition": "payload.status == 'success'",
                "action": "my_handler",
                "safe": True,
            }
        },
    )
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"my_handler": handler})
    daemon._route({"event_type": "WorkflowClosed", "payload": {"status": "failed"}})
    assert calls == []


def test_route_no_rule_uses_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, handler = _recorder()
    monkeypatch.setattr(daemon, "_ROUTES", {})
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"my_handler": handler})
    daemon._route({"event_type": "UnknownType", "payload": {}})
    assert calls == []


def test_route_nonsafe_blocked_without_approval(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, handler = _recorder()
    monkeypatch.setattr(
        daemon,
        "_ROUTES",
        {"ExecutionRequested": {"event_type": "ExecutionRequested", "action": "exec", "safe": False}},
    )
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"exec": handler})
    daemon._route({"event_type": "ExecutionRequested", "payload": {}})
    assert calls == []  # 批准门拦截


def test_route_nonsafe_passes_when_approval_off(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, handler = _recorder()
    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", False)
    monkeypatch.setattr(
        daemon,
        "_ROUTES",
        {"ExecutionRequested": {"event_type": "ExecutionRequested", "action": "exec", "safe": False}},
    )
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"exec": handler})
    daemon._route({"event_type": "ExecutionRequested", "payload": {}})
    assert calls == ["ExecutionRequested"]


def test_route_handler_exception_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(event: dict) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        daemon,
        "_ROUTES",
        {"WorkflowClosed": {"event_type": "WorkflowClosed", "action": "boom", "safe": True}},
    )
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"boom": boom})
    # handler 异常不应传播 (隔离)
    daemon._route({"event_type": "WorkflowClosed", "payload": {}})


# ── _read_incremental ─────────────────────────────────────────────────────────


def _write_events(path: Path, events: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
        encoding="utf-8",
    )


def test_read_incremental_empty(tmp_path: Path) -> None:
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, [])
    events, size = daemon._read_incremental(events_file, 0)
    assert events == []
    assert size == 0


def test_read_incremental_from_offset(tmp_path: Path) -> None:
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, [{"event_type": "A"}, {"event_type": "B"}, {"event_type": "C"}])
    full_size = events_file.stat().st_size
    # offset 指向第一行结尾 → 只读 B,C
    first_line_len = len(json.dumps({"event_type": "A"}, ensure_ascii=False)) + 1
    events, size = daemon._read_incremental(events_file, first_line_len)
    assert [e["event_type"] for e in events] == ["B", "C"]
    assert size == full_size


def test_read_incremental_offset_at_end_returns_empty(tmp_path: Path) -> None:
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, [{"event_type": "A"}])
    full_size = events_file.stat().st_size
    events, size = daemon._read_incremental(events_file, full_size)
    assert events == []
    assert size == full_size


def test_read_incremental_truncate_rescans(tmp_path: Path) -> None:
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, [{"event_type": "A"}, {"event_type": "B"}])
    big_size = events_file.stat().st_size
    # 文件被截断/重建 → offset > size → 全扫
    _write_events(events_file, [{"event_type": "X"}])
    events, size = daemon._read_incremental(events_file, big_size)
    assert [e["event_type"] for e in events] == ["X"]
    assert size == events_file.stat().st_size


def test_read_incremental_missing_file(tmp_path: Path) -> None:
    events, size = daemon._read_incremental(tmp_path / "missing.jsonl", 0)
    assert events == []
    assert size == 0


def test_read_incremental_bad_lines_skipped(tmp_path: Path) -> None:
    events_file = tmp_path / "events.jsonl"
    events_file.write_text('{bad json}\n{"event_type": "OK"}\n', encoding="utf-8")
    events, _ = daemon._read_incremental(events_file, 0)
    assert [e["event_type"] for e in events] == ["OK"]


# ── tick_once ─────────────────────────────────────────────────────────────────


@pytest.fixture
def _ledger(tmp_path: Path):
    broker = LedgerBroker.connect(tmp_path / "ledger.sqlite3")
    yield broker
    broker.close()


def _tick_setup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, events: list[dict]):
    """Point watermark writes at tmp_path and wire a safe test handler."""
    wm_dir = tmp_path / "watermarks"
    monkeypatch.setattr(daemon, "_wm_path", lambda projector: wm_dir / f"{projector}.json")
    calls: list[str] = []

    def handler(event: dict) -> None:
        calls.append(str(event.get("event_type")))

    monkeypatch.setattr(
        daemon,
        "_ROUTES",
        {
            "WorkflowClosed": {"event_type": "WorkflowClosed", "action": "h", "safe": True},
            "WorkflowFailed": {"event_type": "WorkflowFailed", "action": "h", "safe": True},
        },
    )
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {"h": handler})
    events_file = tmp_path / "events.jsonl"
    _write_events(events_file, events)
    return events_file, calls, handler


def test_tick_once_processes_and_advances(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    events_file, calls, _ = _tick_setup(
        monkeypatch, tmp_path, [{"event_type": "WorkflowClosed"}, {"event_type": "WorkflowFailed"}]
    )
    (tmp_path / "watermarks").mkdir(exist_ok=True)
    report = daemon.tick_once(_ledger, events_file)
    assert report["processed"] == 2
    assert sorted(calls) == ["WorkflowClosed", "WorkflowFailed"]
    # checkpoint + 水位推进
    cp = _ledger.checkpoint_get(daemon.PROJECTOR_ID)
    assert int((cp or {}).get("last_sequence", 0)) == 2
    wm = json.loads((tmp_path / "watermarks" / f"{daemon.PROJECTOR_ID}.json").read_text())
    assert wm["byte_offset"] == events_file.stat().st_size


def test_tick_once_idempotent_second_tick(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    events_file, calls, _ = _tick_setup(monkeypatch, tmp_path, [{"event_type": "WorkflowClosed"}])
    daemon.tick_once(_ledger, events_file)
    second = daemon.tick_once(_ledger, events_file)
    assert second["processed"] == 0  # 增量读无新事件
    assert calls == ["WorkflowClosed"]  # 不重复处理


def test_tick_once_topic_filter(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    events_file, calls, _ = _tick_setup(
        monkeypatch, tmp_path, [{"event_type": "WorkflowClosed"}, {"event_type": "WorkflowFailed"}]
    )
    report = daemon.tick_once(_ledger, events_file, topic_filter={"WorkflowClosed"})
    assert report["processed"] == 1
    assert calls == ["WorkflowClosed"]
    # checkpoint 仍推进到文件末尾 (跳过的事件不重复)
    cp = _ledger.checkpoint_get(daemon.PROJECTOR_ID)
    assert int((cp or {}).get("last_sequence", 0)) == 2


def test_tick_once_independent_projectors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    events_file, calls, _ = _tick_setup(monkeypatch, tmp_path, [{"event_type": "WorkflowClosed"}])
    daemon.tick_once(_ledger, events_file, projector="proj-a")
    daemon.tick_once(_ledger, events_file, projector="proj-b")
    # 两个 projector 各自消费同一批事件 (多 agent 并行)
    assert calls == ["WorkflowClosed", "WorkflowClosed"]
    assert set(json.loads(p.read_text())["byte_offset"] for p in (tmp_path / "watermarks").glob("*.json")) == {
        events_file.stat().st_size
    }


def test_tick_once_invokes_check_and_recover(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """BET-Y1Q4-T9-02: daemon tick path must call ledger_check.check_and_recover."""
    calls: list[Path] = []

    def _fake_recover(ledger: Path) -> dict:
        calls.append(ledger)
        return {"ok": True, "locked": False}

    monkeypatch.setattr("omo.resident.ledger_check.check_and_recover", _fake_recover)
    events_file, _, _ = _tick_setup(monkeypatch, tmp_path, [])
    daemon.tick_once(_ledger, events_file)
    assert calls == [daemon.DEFAULT_LEDGER]
