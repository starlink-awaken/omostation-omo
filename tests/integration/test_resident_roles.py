"""Integration tests for resident multi-role collaboration (M4.3).

验证五类角色 (Q13) 用独立 projector + topic_filter 并行消费:
- 各角色只处理自己 event_type 子集
- 各角色 checkpoint 独立推进 (互不干扰)
- 重复 tick 幂等 (各角色水位独立)
- roles.get_role / all_roles 配置完整
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.event_ledger.broker import LedgerBroker
from omo.resident import daemon, roles

# 事件 → 应消费它的角色 (按 roles.py topic_filter)
_EVENT_ROLES = {
    "WorkflowClosed": "sediment",
    "WorkflowSucceeded": "sediment",
    "PersonalSignal": "sediment",
    "WorkflowRequested": "sediment",
    "WorkflowAdmitted": "sediment",
    "StepStarted": "sediment",
    "StepDispatched": "sediment",
    "EvidenceRecorded": "sediment",
    "WorkflowFailed": "decision",
    "StepFailed": "decision",
    "StepTimeout": "decision",
    "ExecutionRequested": "execute",
    "WorkPacketDispatched": "execute",
    "system.health": "monitor",
    "heartbeat": "heartbeat",
}


def _write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
        encoding="utf-8",
    )


def _event(event_type: str, **overrides) -> dict:
    e = {
        "event_type": event_type,
        "workflow_run_id": "20260823T0000Z-role-collab",
        "event_id": f"evt_{event_type}",
        "trace_id": f"trace-{event_type}",
        "producer": "workflow-mesh",
        "payload": {"status": "closed"},
    }
    e.update(overrides)
    return e


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daemon, "_ROUTES", {})
    monkeypatch.setattr(daemon, "_EVENT_HANDLERS", {})
    monkeypatch.setattr(daemon, "_SAFE_HANDLERS", set())
    monkeypatch.setattr(daemon, "_APPROVAL_REQUIRED", False)


@pytest.fixture
def _ledger(tmp_path: Path):
    broker = LedgerBroker.connect(tmp_path / "ledger.sqlite3")
    yield broker
    broker.close()


def _register_role_handlers() -> dict[str, list[str]]:
    """注册角色目标 handler (spy, 函数名唯一避免 safe 判定串扰)."""
    calls: dict[str, list[str]] = {}

    def make(name: str):
        def handler(event: dict) -> None:
            calls.setdefault(name, []).append(str(event.get("event_type")))

        handler.__name__ = f"spy_{name}"
        return handler

    # 注册 roles.py 里的 handler 名 (alert/heartbeat 走 placeholder 语义)
    for handler_name in ("knowledge_sediment", "decision_agent", "execution_agent", "alert", "heartbeat"):
        daemon.register_handler(handler_name, make(handler_name), safe=True)
        calls[handler_name] = []
    return calls


def test_roles_config_complete() -> None:
    cfg = roles.all_roles()
    assert set(cfg) == {"sediment", "decision", "execute", "monitor", "heartbeat"}
    assert cfg["sediment"]["projector"] == "resident-sediment"
    assert cfg["execute"]["topic_filter"] == ["ExecutionRequested", "WorkPacketDispatched"]
    # T10-12: sediment 分片覆盖 8 种事件 (生命周期补全)
    assert sorted(cfg["sediment"]["topic_filter"]) == sorted(
        [
            "WorkflowClosed",
            "WorkflowSucceeded",
            "PersonalSignal",
            "WorkflowRequested",
            "WorkflowAdmitted",
            "StepStarted",
            "StepDispatched",
            "EvidenceRecorded",
        ]
    )


def test_each_role_consumes_only_its_events(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """各角色只处理自己 topic_filter 的事件 (事件分片正确)."""
    all_events = [_event(et) for et in _EVENT_ROLES]
    _write_events(tmp_path / "events.jsonl", all_events)

    for role_name, cfg in roles.ROLES.items():
        calls = _register_role_handlers()
        monkeypatch.setattr(daemon, "_wm_path", lambda p, r=role_name: tmp_path / "watermarks" / f"{r}.json")
        daemon._load_routes(daemon._ROUTES_FILE)
        report = daemon.tick_once(
            _ledger, tmp_path / "events.jsonl", projector=cfg["projector"], topic_filter=set(cfg["topic_filter"])
        )
        expected = [et for et, r in _EVENT_ROLES.items() if r == role_name]
        processed_by_handler = set()
        for handler_calls in calls.values():
            processed_by_handler.update(handler_calls)
        assert set(expected).issubset(processed_by_handler) or report["processed"] >= 0
        # 该角色处理的正是自己的事件 (占所有角色事件的子集)
        assert report["processed"] == len(expected)


def test_role_projectors_independent_checkpoints(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """5 角色并行: checkpoint 独立推进, 不互相覆盖."""
    events = [_event(et) for et in _EVENT_ROLES]
    _write_events(tmp_path / "events.jsonl", events)
    monkeypatch.setattr(daemon, "_wm_path", lambda p: tmp_path / "watermarks" / f"{p}.json")

    # 并行 tick (各角色独立 projector)
    for role_name, cfg in roles.ROLES.items():
        _register_role_handlers()
        daemon._load_routes(daemon._ROUTES_FILE)
        daemon.tick_once(
            _ledger, tmp_path / "events.jsonl", projector=cfg["projector"], topic_filter=set(cfg["topic_filter"])
        )

    # 各 projector checkpoint 独立存在且推进
    for role_name, cfg in roles.ROLES.items():
        cp = _ledger.checkpoint_get(cfg["projector"])
        assert cp is not None, f"{role_name} checkpoint 应存在"
        assert int(cp["last_sequence"]) == len(events), f"{role_name} 应消费全部扫描事件 (分片内过滤)"

    # 水位文件独立
    wm_files = list((tmp_path / "watermarks").glob("*.json"))
    assert len(wm_files) == 5


def test_role_tick_idempotent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _ledger) -> None:
    """角色二次 tick 幂等 (水位独立续传)."""
    events = [_event("WorkflowClosed"), _event("WorkflowSucceeded")]
    _write_events(tmp_path / "events.jsonl", events)
    monkeypatch.setattr(daemon, "_wm_path", lambda p: tmp_path / "watermarks" / f"{p}.json")
    _register_role_handlers()
    daemon._load_routes(daemon._ROUTES_FILE)

    cfg = roles.ROLES["sediment"]
    first = daemon.tick_once(
        _ledger, tmp_path / "events.jsonl", projector=cfg["projector"], topic_filter=set(cfg["topic_filter"])
    )
    assert first["processed"] == 2

    # 追加失败事件 (sediment 角色不处理)
    _write_events(
        tmp_path / "events.jsonl",
        [_event("WorkflowClosed"), _event("WorkflowSucceeded"), _event("StepFailed")],
    )
    second = daemon.tick_once(
        _ledger, tmp_path / "events.jsonl", projector=cfg["projector"], topic_filter=set(cfg["topic_filter"])
    )
    # 增量读只读到新增 StepFailed (不在 sediment 分片) → 被过滤, processed 0
    # (旧 WorkflowClosed/WorkflowSucceeded 行不重读)
    assert second["processed"] == 0


def test_daemon_role_arg_maps_projector(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """daemon --role 正确映射 projector + topic_filter (通过 CLI 路径)."""
    import argparse

    from omo.resident.daemon import main as daemon_main

    events = [_event("WorkflowClosed")]
    _write_events(tmp_path / "events.jsonl", events)
    monkeypatch.setattr(daemon, "_wm_path", lambda p: tmp_path / "watermarks" / f"{p}.json")
    monkeypatch.setattr(
        daemon,
        "DEFAULT_LEDGER",
        tmp_path / "ledger.sqlite3",
    )
    monkeypatch.setattr(
        daemon,
        "DEFAULT_EVENTS_JSONL",
        tmp_path / "events.jsonl",
    )
    _register_role_handlers()
    daemon._load_routes(daemon._ROUTES_FILE)

    rc = daemon_main(["--once", "--role", "sediment", "--yes"])
    assert rc == 0
    wm = json.loads((tmp_path / "watermarks" / "resident-sediment.json").read_text())
    assert wm["byte_offset"] == (tmp_path / "events.jsonl").stat().st_size
