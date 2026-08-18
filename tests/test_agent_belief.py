"""
Unit tests for projects/omo/src/omo/omo_belief.py (BET-Y1Q1-T3-01)
"""

from pathlib import Path

import pytest

from omo.omo_belief import (
    CapabilityCalibration,
    DecisionOutcome,
    MOSBeliefManager,
    WorldSnapshot,
)


def test_belief_manager_record_and_query(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)

    b_id = mgr.record_belief(
        topic="git_isolation",
        belief_text="Agent 在共享主工作区修改代码会导致写冲突，必须走 Worktree",
        pitfall="直接在 main 运行 git commit --no-verify 覆盖改动",
        solution="使用 bash bin/gac/gac-worktree.sh claim <session> 创建隔离工作区",
        scope_path="bin/gac/*",
        source_run_id="run-test-001",
    )

    assert b_id == "belief-0001"
    assert mgr.state_file.exists()
    assert mgr.registry_file.exists()

    results = mgr.query_beliefs("git_isolation")
    assert len(results) == 1
    assert results[0]["id"] == "belief-0001"
    assert "Worktree" in results[0]["belief"]


def test_load_state_returns_all_six_tables(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    state = mgr._load_state()
    expected_keys = {
        "beliefs",
        "lessons",
        "contexts",
        "world_snapshots",
        "capability_calibrations",
        "decision_outcomes",
        "agent_skills",
        "agent_experiences",
    }
    assert set(state.keys()) == expected_keys
    for v in state.values():
        assert v == []


def test_record_world_snapshot(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    ws_id = mgr.record_world_snapshot(
        source="ci-pipeline",
        domain="governance",
        observations={"green_checks": 38, "total_checks": 39, "health": 97},
        confidence=0.95,
    )
    assert ws_id == "ws-0001"

    state = mgr._load_state()
    assert len(state["world_snapshots"]) == 1
    snap = state["world_snapshots"][0]
    assert snap["domain"] == "governance"
    assert snap["confidence"] == 0.95
    assert snap["observations"]["green_checks"] == 38

    registry = mgr._load_registry()
    assert registry.get("total_world_snapshots") == 1


def test_record_world_snapshot_with_expiry(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    ws_id = mgr.record_world_snapshot(
        source="agent-run",
        domain="deployment",
        observations={"status": "staging"},
        expires_at="2026-08-08T00:00:00Z",
    )
    assert ws_id == "ws-0001"
    state = mgr._load_state()
    assert state["world_snapshots"][0]["expires_at"] == "2026-08-08T00:00:00Z"


class TestDeltaFromPrevious:
    """BET-Y2Q1-T3-01: 世界模型增量查询 — 回答"和上次比变了什么"."""

    def test_delta_no_snapshots(self, tmp_path: Path):
        mgr = MOSBeliefManager(root=tmp_path)
        result = mgr.delta_from_previous("governance")
        assert result["has_delta"] is False
        assert result["current"] is None
        assert result["base"] is None
        assert result["changed_fields"] == []

    def test_delta_single_snapshot_no_change(self, tmp_path: Path):
        mgr = MOSBeliefManager(root=tmp_path)
        mgr.record_world_snapshot(
            source="ci", domain="governance", observations={"checks": 38}
        )
        result = mgr.delta_from_previous("governance")
        assert result["has_delta"] is False
        assert result["current"] is not None
        assert result["base"] is None  # 只有一个快照，无前驱
        assert result["delta"]["unchanged"] == {"checks": 38}

    def test_delta_detects_added_changed_removed(self, tmp_path: Path):
        mgr = MOSBeliefManager(root=tmp_path)
        mgr.record_world_snapshot(
            source="ci",
            domain="governance",
            observations={"checks": 38, "health": 97, "flaky": 3},
        )
        mgr.record_world_snapshot(
            source="ci",
            domain="governance",
            observations={"checks": 39, "health": 97, "new_metric": 1},
        )
        result = mgr.delta_from_previous("governance")
        assert result["has_delta"] is True
        d = result["delta"]
        assert d["added"] == {"new_metric": 1}
        assert d["removed"] == {"flaky": 3}
        assert d["changed"] == {"checks": {"from": 38, "to": 39}}
        assert d["unchanged"] == {"health": 97}
        assert set(result["changed_fields"]) == {"checks", "flaky", "new_metric"}

    def test_delta_filters_by_source(self, tmp_path: Path):
        mgr = MOSBeliefManager(root=tmp_path)
        mgr.record_world_snapshot(source="ci", domain="gov", observations={"a": 1})
        mgr.record_world_snapshot(source="agent", domain="gov", observations={"b": 2})
        mgr.record_world_snapshot(source="ci", domain="gov", observations={"a": 10})
        # 不按 source 过滤：取最新两条 (agent 的 b=2 和 ci 的 a=10)
        result_all = mgr.delta_from_previous("gov")
        assert result_all["current"]["observations"] == {"a": 10}
        # 按 source=ci 过滤：只比较 ci 的两条
        result_ci = mgr.delta_from_previous("gov", source="ci")
        assert result_ci["has_delta"] is True
        assert result_ci["delta"]["changed"] == {"a": {"from": 1, "to": 10}}

    def test_delta_is_domain_scoped(self, tmp_path: Path):
        mgr = MOSBeliefManager(root=tmp_path)
        mgr.record_world_snapshot(source="ci", domain="gov", observations={"a": 1})
        mgr.record_world_snapshot(source="ci", domain="deploy", observations={"b": 2})
        result = mgr.delta_from_previous("gov")
        assert result["has_delta"] is False  # gov 只有一个快照
        assert result["current"]["observations"] == {"a": 1}


def test_record_capability_calibration(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    cc_id = mgr.record_capability_calibration(
        capability_ref="ref://capability/gac-local-gate",
        success_rate=0.92,
        avg_latency_ms=450.0,
        sample_size=50,
        last_run_id="run-cal-001",
    )
    assert cc_id == "cc-0001"

    state = mgr._load_state()
    assert len(state["capability_calibrations"]) == 1
    cal = state["capability_calibrations"][0]
    assert cal["success_rate"] == 0.92
    assert cal["sample_size"] == 50
    assert cal["last_run_id"] == "run-cal-001"

    registry = mgr._load_registry()
    assert registry.get("total_capability_calibrations") == 1


def test_record_decision_outcome(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    do_id = mgr.record_decision_outcome(
        decision_type="scene_activation",
        input_summary="engineering-delivery shadow transition",
        expected_outcome="preflight pass with no blockers",
        actual_outcome="blocked by catalog capability mismatch",
        delta="internal pipeline != external catalog",
        source_run_id="run-dec-001",
    )
    assert do_id == "do-0001"

    state = mgr._load_state()
    assert len(state["decision_outcomes"]) == 1
    dec = state["decision_outcomes"][0]
    assert dec["decision_type"] == "scene_activation"
    assert dec["delta"] == "internal pipeline != external catalog"

    registry = mgr._load_registry()
    assert registry.get("total_decision_outcomes") == 1


def test_registry_tables_list_includes_all_six(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    mgr.record_belief(topic="t", belief_text="b")
    registry = mgr._load_registry()
    assert set(registry["tables"]) == {
        "agent_belief",
        "agent_lesson",
        "agent_context",
        "world_snapshot",
        "capability_calibration",
        "decision_outcome",
    }


def test_multiple_records_increment_ids(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    assert mgr.record_world_snapshot(source="a", domain="d", observations={}) == "ws-0001"
    assert mgr.record_world_snapshot(source="b", domain="d", observations={}) == "ws-0002"
    assert mgr.record_capability_calibration(capability_ref="ref://x", success_rate=0.5) == "cc-0001"
    assert (
        mgr.record_decision_outcome(
            decision_type="t",
            input_summary="i",
            expected_outcome="e",
            actual_outcome="a",
        )
        == "do-0001"
    )
    assert (
        mgr.record_decision_outcome(
            decision_type="t2",
            input_summary="i2",
            expected_outcome="e2",
            actual_outcome="a2",
        )
        == "do-0002"
    )

    state = mgr._load_state()
    assert len(state["world_snapshots"]) == 2
    assert len(state["capability_calibrations"]) == 1
    assert len(state["decision_outcomes"]) == 2


def test_audit_log_records_new_table_actions(tmp_path: Path):
    mgr = MOSBeliefManager(root=tmp_path)
    mgr.record_world_snapshot(source="s", domain="d", observations={})
    mgr.record_capability_calibration(capability_ref="ref://c", success_rate=0.8)
    mgr.record_decision_outcome(decision_type="t", input_summary="i", expected_outcome="e", actual_outcome="a")

    log_text = mgr.audit_log_file.read_text()
    assert "RECORD_WORLD_SNAPSHOT" in log_text
    assert "RECORD_CAPABILITY_CALIBRATION" in log_text
    assert "RECORD_DECISION_OUTCOME" in log_text
