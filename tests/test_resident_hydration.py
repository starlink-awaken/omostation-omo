"""Tests for hydration state machine (BET-Y1Q4-T10-132): transitions, WAL, latency, concurrency."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from omo.resident.hydration import (
    HYDRATE_BUDGET_MS,
    HydrationStateMachine,
    IllegalTransitionError,
)


@pytest.fixture()
def sm(tmp_path: Path) -> HydrationStateMachine:
    return HydrationStateMachine(db_path=tmp_path / "hydration.db", agent_id="agent-a")


def test_initial_state_dormant(sm: HydrationStateMachine):
    assert sm.state == "DORMANT"


def test_illegal_transitions_fail_closed(sm: HydrationStateMachine):
    # DORMANT -> DEHYDRATING 现为合法（初始帧落盘），改测其余非法迁移
    with pytest.raises(IllegalTransitionError):
        sm._check("ACTIVE")  # DORMANT -> ACTIVE 非法
    sm.hydrate()  # -> ACTIVE
    with pytest.raises(IllegalTransitionError):
        sm._check("HYDRATING")  # ACTIVE -> HYDRATING 非法（须先 DEHYDRATING）


def test_hydrate_dehydrate_roundtrip(sm: HydrationStateMachine):
    frame = {"context": "医共体方案评审", "step": 3, "tensor_refs": []}
    sm.dehydrate(frame)  # DORMANT 直接落盘（DORMANT 自身持久化允许）
    sm2 = HydrationStateMachine(db_path=sm.db_path, agent_id="agent-a")
    assert sm2.state == "DORMANT"
    result = sm2.hydrate()
    assert result["ok"] is True
    assert sm2.state == "ACTIVE"
    assert result["frame"]["context"] == "医共体方案评审"
    assert result["hydrate_ms"] <= HYDRATE_BUDGET_MS


def test_active_to_dehydrate_cycle(sm: HydrationStateMachine):
    sm.hydrate()
    assert sm.state == "ACTIVE"
    res = sm.dehydrate({"next": "step4"})
    assert res["ok"] is True
    assert sm.state == "DORMANT"
    # 帧已持久化
    sm2 = HydrationStateMachine(db_path=sm.db_path, agent_id="agent-a")
    assert sm2.hydrate()["frame"] == {"next": "step4"}


def test_probe_read_latency_under_15ms(sm: HydrationStateMachine):
    sm.dehydrate({"probe": True})
    worst = sm.probe_read_latency(iterations=8)
    assert worst <= 15.0, f"ro probe took {worst:.2f}ms (>15ms)"


def test_concurrent_agents_independent(tmp_path: Path):
    db = tmp_path / "shared.db"
    agents = [HydrationStateMachine(db_path=db, agent_id=f"agent-{i}") for i in range(6)]
    for i, a in enumerate(agents):
        a.dehydrate({"idx": i})
    # 并发水合
    results: dict[str, dict] = {}

    def run(a: HydrationStateMachine, idx: int):
        results[a.agent_id] = a.hydrate()

    threads = [threading.Thread(target=run, args=(a, i)) for i, a in enumerate(agents)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 6
    for i, a in enumerate(agents):
        assert results[a.agent_id]["frame"] == {"idx": i}


def test_stress_hydrate_dehydrate_cycles(tmp_path: Path):
    a = HydrationStateMachine(db_path=tmp_path / "s.db", agent_id="stress")
    t0 = time.perf_counter()
    for cycle in range(50):
        a.dehydrate({"cycle": cycle})
        a.hydrate()
    elapsed = time.perf_counter() - t0
    assert elapsed < 10, f"50 cycles took {elapsed:.1f}s"
    assert a.state == "ACTIVE"
