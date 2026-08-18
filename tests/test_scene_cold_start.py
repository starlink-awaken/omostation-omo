"""Tests for scene cold start planner (BET-Y3H1-T3-01)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.omo_belief import MOSBeliefManager
from omo.scene_cold_start import SceneColdStartPlanner


def test_cold_start_with_rich_source(tmp_path: Path):
    """有源场景时生成复用方案."""
    mos = MOSBeliefManager(root=tmp_path)
    # 源场景: 15 条校准, 样本充足
    for _ in range(15):
        mos.record_capability_calibration(
            capability_ref="ref://scene/document-review/format-check",
            success_rate=0.85,
            sample_size=10,
        )
    planner = SceneColdStartPlanner(mos_manager=mos)
    plan = planner.plan_cold_start("new-quality-scene", operator="test")
    assert plan.source_scene_id == "document-review"
    assert plan.transferred_calibration_id is not None
    assert plan.initial_success_rate is not None
    # 打折后: 0.85 * 0.8 = 0.68, 但上限 0.6 → 0.6
    assert plan.initial_success_rate <= 0.6
    assert plan.estimated_weeks < 4.0  # 有复用应 < 4 周
    assert "document-review" in plan.provenance


def test_cold_start_no_source(tmp_path: Path):
    """无源场景时返回从零积累方案."""
    mos = MOSBeliefManager(root=tmp_path)
    planner = SceneColdStartPlanner(mos_manager=mos)
    plan = planner.plan_cold_start("brand-new-scene")
    assert plan.source_scene_id is None
    assert plan.transferred_calibration_id is None
    assert plan.estimated_weeks == 4.0
    assert "无" in plan.provenance


def test_cold_start_insufficient_samples(tmp_path: Path):
    """源场景样本不足时不可复用."""
    mos = MOSBeliefManager(root=tmp_path)
    # 仅 3 条 (低于 MIN_SOURCE_SAMPLES=10)
    for _ in range(3):
        mos.record_capability_calibration(
            capability_ref="ref://scene/x/task-a",
            success_rate=0.9,
            sample_size=2,
        )
    planner = SceneColdStartPlanner(mos_manager=mos)
    plan = planner.plan_cold_start("new-scene")
    assert plan.source_scene_id is None
    assert plan.estimated_weeks == 4.0


def test_cold_start_picks_richest_source(tmp_path: Path):
    """多个源场景时选样本最多的."""
    mos = MOSBeliefManager(root=tmp_path)
    # 场景 A: 5 条, 样本 50
    for _ in range(5):
        mos.record_capability_calibration("ref://scene/A/task", success_rate=0.7, sample_size=10)
    # 场景 B: 10 条, 样本 200 (更丰富)
    for _ in range(10):
        mos.record_capability_calibration("ref://scene/B/task", success_rate=0.8, sample_size=20)
    planner = SceneColdStartPlanner(mos_manager=mos)
    plan = planner.plan_cold_start("new-scene")
    assert plan.source_scene_id == "B"
    assert plan.source_calibration_count == 10


def test_cold_start_discount_applied(tmp_path: Path):
    """冷启动校准应用折扣."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(10):
        mos.record_capability_calibration("ref://scene/src/action", success_rate=0.9, sample_size=15)
    planner = SceneColdStartPlanner(mos_manager=mos, discount=0.8, max_initial_rate=1.0)
    plan = planner.plan_cold_start("dst")
    # 0.9 * 0.8 = 0.72 (低于上限 1.0, 不截断)
    assert plan.initial_success_rate == pytest.approx(0.72, abs=0.01)


def test_cold_start_rate_capped(tmp_path: Path):
    """冷启动校准不超过上限."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(10):
        mos.record_capability_calibration("ref://scene/src/x", success_rate=0.95, sample_size=20)
    planner = SceneColdStartPlanner(mos_manager=mos, max_initial_rate=0.5)
    plan = planner.plan_cold_start("dst")
    # 0.95 * 0.8 = 0.76, 但上限 0.5 → 0.5
    assert plan.initial_success_rate == pytest.approx(0.5)


def test_cold_start_no_mos_returns_default(tmp_path: Path):
    """无 MOS manager 时返回默认方案."""
    planner = SceneColdStartPlanner(mos_manager=None)
    plan = planner.plan_cold_start("any-scene")
    assert plan.source_scene_id is None
    assert plan.estimated_weeks == 4.0
