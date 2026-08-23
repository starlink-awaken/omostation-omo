"""Integration tests for resident routing — rule reachability + resume.

M2.2: 走 daemon 全链路验证 (反馈 feedback_rule_reachability_verification):
- handler 注册 ≠ 规则配置; 必须验证各 event_type 实际路由到目标 handler
- 真实 resident-routes.yaml + 真实 ledger + tick_once
- 真实 sediment 端到端 (草稿落盘) + checkpoint 断点续传幂等
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.event_ledger.broker import LedgerBroker
from omo.resident import daemon, sediment


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daemon, "_ROUTES", {})
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {})
    monkeypatch.setattr(daemon, "_SAFE_HANDLERS", set())


def _write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
        encoding="utf-8",
    )


def _event(event_type: str, **overrides) -> dict:
    e = {
        "event_type": event_type,
        "workflow_run_id": "20260823T0000Z-project-code-change-int01",
        "event_id": f"evt_{event_type}",
        "trace_id": f"trace-{event_type}",
        "producer": "workflow-mesh",
        "payload": {"status": "closed"},
    }
    e.update(overrides)
    return e


def _register_spy_handlers(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    """Register spy handlers under the real action names sediment/decision/execute use."""
    calls: dict[str, list[str]] = {"knowledge_sediment": [], "decision_agent": [], "execution_agent": []}

    def make(name: str):
        # 闭包函数名须唯一, 否则 _SAFE_HANDLERS 按 __name__ 记录会串 (见 test 注释)
        def handler(event: dict) -> None:
            calls[name].append(str(event.get("event_type")))

        handler.__name__ = f"spy_{name}"  # 避免多个闭包同名导致 safe 判定串扰
        return handler

    daemon.register_handler("knowledge_sediment", make("knowledge_sediment"), safe=True)
    daemon.register_handler("decision_agent", make("decision_agent"), safe=True)
    daemon.register_handler("execution_agent", make("execution_agent"), safe=False)
    return calls


# ── 规则可达性 (M2.2 核心) ───────────────────────────────────────────────────


def test_all_route_event_types_reach_intended_handler() -> None:
    """真实 resident-routes.yaml: 每个规则 event_type → 正确 handler."""
    routes = daemon._load_routes(daemon._ROUTES_FILE)
    assert routes, "resident-routes.yaml 不应为空"

    calls = _register_spy_handlers(None)

    # sediment 目标事件 (成功类)
    for event_type in ("WorkflowClosed", "WorkflowSucceeded"):
        daemon._route(_event(event_type))
    # 当前路由表 dict 单 action: 失败类被后定义的 decision_agent 覆盖 (见下)
    for event_type in ("ExecutionRequested", "WorkPacketDispatched"):
        daemon._route(_event(event_type))

    assert sorted(calls["knowledge_sediment"]) == ["WorkflowClosed", "WorkflowSucceeded"]
    # 非 safe handler 被批准门拦截
    assert calls["execution_agent"] == []


def test_failure_events_route_to_decision_agent() -> None:
    """失败类事件 → decision_agent (路由表后者覆盖, 单 action dict 语义)."""
    daemon._load_routes(daemon._ROUTES_FILE)
    calls = _register_spy_handlers(None)

    for event_type in ("WorkflowFailed", "StepFailed", "StepTimeout"):
        daemon._route(_event(event_type))

    assert sorted(calls["decision_agent"]) == ["StepFailed", "StepTimeout", "WorkflowFailed"]
    # 同 event_type 多规则只取一条 (当前 dict 语义: 失败类不再走 sediment)
    assert calls["knowledge_sediment"] == []


def test_execution_events_blocked_until_approval(monkeypatch: pytest.MonkeyPatch) -> None:
    daemon._load_routes(daemon._ROUTES_FILE)
    calls = _register_spy_handlers(monkeypatch)

    daemon._route(_event("ExecutionRequested"))
    daemon._route(_event("WorkPacketDispatched"))
    assert calls["execution_agent"] == []  # 批准门拦截

    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", False)
    daemon._route(_event("ExecutionRequested"))
    assert calls["execution_agent"] == ["ExecutionRequested"]  # 批准后放行


def test_condition_rule_skips_nonmatching_event(monkeypatch: pytest.MonkeyPatch) -> None:
    routes = daemon._load_routes(daemon._ROUTES_FILE)
    # 为 WorkflowClosed 附加条件
    routes["WorkflowClosed"]["condition"] = "payload.status == 'closed'"
    calls = _register_spy_handlers(monkeypatch)

    daemon._route(_event("WorkflowClosed", payload={"status": "open"}))
    assert calls["knowledge_sediment"] == []

    daemon._route(_event("WorkflowClosed", payload={"status": "closed"}))
    assert calls["knowledge_sediment"] == ["WorkflowClosed"]


# ── 真实 sediment 端到端 + checkpoint 续传 ───────────────────────────────────


@pytest.fixture
def _ledger(tmp_path: Path):
    broker = LedgerBroker.connect(tmp_path / "ledger.sqlite3")
    yield broker
    broker.close()


def test_real_sediment_end_to_end_and_resume(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """真实 sediment handler: 事件 → 草稿落盘; 二次 tick 幂等; 追加事件续传."""
    monkeypatch.setattr(sediment, "SEDIMENT_ROOT", tmp_path / "sediment")
    monkeypatch.setattr(daemon, "_wm_path", lambda projector: tmp_path / "watermarks" / f"{projector}.json")
    daemon._load_routes(daemon._ROUTES_FILE)
    sediment.register_with_daemon(daemon)

    events_file = tmp_path / "events.jsonl"
    _write_events(
        events_file,
        [
            _event("WorkflowClosed", event_id="evt_c1", workflow_run_id="20260823T0000Z-run-aaa"),
            _event("WorkflowSucceeded", event_id="evt_c2", workflow_run_id="20260823T0000Z-run-bbb"),
        ],
    )

    first = daemon.tick_once(_ledger, events_file)
    assert first["processed"] == 2
    drafts = list((tmp_path / "sediment" / "runs").glob("*.md"))
    assert len(drafts) == 2  # 每个 run 一个复盘草稿

    # 幂等: 二次 tick 无新事件
    second = daemon.tick_once(_ledger, events_file)
    assert second["processed"] == 0

    # 追加事件 → 只处理新事件
    _write_events(
        events_file,
        [
            _event("WorkflowClosed", event_id="evt_c1", workflow_run_id="20260823T0000Z-run-aaa"),
            _event("WorkflowSucceeded", event_id="evt_c2", workflow_run_id="20260823T0000Z-run-bbb"),
            _event("WorkflowClosed", event_id="evt_c3", workflow_run_id="20260823T0000Z-run-ccc"),
        ],
    )
    third = daemon.tick_once(_ledger, events_file)
    assert third["processed"] == 1
    assert len(list((tmp_path / "sediment" / "runs").glob("*.md"))) == 3


def test_ledger_checkpoint_resume_across_ticks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """真实 ledger + tick_once: checkpoint 跨 tick 续传, 不重复处理历史事件."""
    monkeypatch.setattr(daemon, "_wm_path", lambda projector: tmp_path / "watermarks" / f"{projector}.json")
    daemon._load_routes(daemon._ROUTES_FILE)
    sediment.register_with_daemon(daemon)
    broker = LedgerBroker.connect(tmp_path / "ledger2.sqlite3")
    try:
        events_file = tmp_path / "events2.jsonl"
        _write_events(events_file, [_event("WorkflowClosed", event_id="evt_x")])
        daemon.tick_once(broker, events_file)
        cp = broker.checkpoint_get(daemon.PROJECTOR_ID)
        assert int((cp or {}).get("last_sequence", 0)) == 1
        # 重建 broker 模拟进程重启 → checkpoint 仍在 ledger 中
        broker.close()
        broker2 = LedgerBroker.connect(tmp_path / "ledger2.sqlite3")
        try:
            cp2 = broker2.checkpoint_get(daemon.PROJECTOR_ID)
            assert int((cp2 or {}).get("last_sequence", 0)) == 1
        finally:
            broker2.close()
    finally:
        broker.close()
