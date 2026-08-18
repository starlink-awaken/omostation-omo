"""Tests for projects/omo/src/omo/drift_monitor.py (BET-Y2Q3-T3-02)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.drift_monitor import DriftMonitor, DriftStatus
from omo.omo_belief import MOSBeliefManager


def test_no_calibration_not_degraded(tmp_path: Path):
    """无 calibration 数据 → 不降级 (rate=None)."""
    mos = MOSBeliefManager(root=tmp_path)
    mon = DriftMonitor(mos_manager=mos, root=tmp_path)
    status = mon.check_scene("scene-x")
    assert status.is_degraded is False
    assert status.current_rate is None


def test_high_calibration_not_degraded(tmp_path: Path):
    """calibration 高于阈值 → 不降级."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(5):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.9,
            sample_size=10,
        )
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=10, root=tmp_path)
    status = mon.check_scene("scene-x")
    assert status.is_degraded is False
    assert status.current_rate is not None and status.current_rate >= 0.6


def test_low_calibration_triggers_degradation(tmp_path: Path):
    """calibration 低于阈值 → 自动降级."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(5):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.3,
            sample_size=10,
        )
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=10, root=tmp_path)
    status = mon.check_scene("scene-x")
    assert status.is_degraded is True
    assert status.last_event == "degraded"


def test_sliding_window_respects_size(tmp_path: Path):
    """滑动窗口只计算最近 N 次."""
    mos = MOSBeliefManager(root=tmp_path)
    # 前 10 次低, 后 3 次高
    for _ in range(10):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.2,
            sample_size=5,
        )
    for _ in range(3):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.95,
            sample_size=5,
        )
    # window=5 → 只取后 5 次 (但只有 3 次高的 + 2 次低的) → avg = (3*0.95 + 2*0.2)/5 = 0.65
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=5, root=tmp_path)
    status = mon.check_scene("scene-x")
    assert status.is_degraded is False  # 0.65 >= 0.6


def test_degraded_event_persisted(tmp_path: Path):
    """降级事件持久化到 drift-events.yaml."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(3):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.2,
            sample_size=5,
        )
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=10, root=tmp_path)
    mon.check_scene("scene-x")
    assert mon.events_file.exists()
    degraded = mon.get_degraded_scenes()
    assert len(degraded) == 1
    assert degraded[0]["scene_id"] == "scene-x"
    assert degraded[0]["event_type"] == "degraded"


def test_no_duplicate_degradation(tmp_path: Path):
    """重复检查不产生重复降级事件."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(3):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.2,
            sample_size=5,
        )
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=10, root=tmp_path)
    mon.check_scene("scene-x")
    mon.check_scene("scene-x")  # 第二次不应再降级
    degraded = mon.get_degraded_scenes()
    assert len(degraded) == 1  # 仍只有 1 条


def test_restore_requires_human(tmp_path: Path):
    """降级后需人工复核方可回升."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(3):
        mos.record_capability_calibration(
            capability_ref="ref://scene/scene-x",
            success_rate=0.2,
            sample_size=5,
        )
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=10, root=tmp_path)
    mon.check_scene("scene-x")
    assert len(mon.get_degraded_scenes()) == 1

    # 人工回升
    ok = mon.restore_scene("scene-x", operator="test-operator")
    assert ok is True
    assert len(mon.get_degraded_scenes()) == 0  # 已回升


def test_restore_non_degraded_returns_false(tmp_path: Path):
    """非降级场景回升返回 False."""
    mos = MOSBeliefManager(root=tmp_path)
    mon = DriftMonitor(mos_manager=mos, root=tmp_path)
    ok = mon.restore_scene("scene-x", operator="op")
    assert ok is False


def test_check_all_scenes(tmp_path: Path):
    """批量检查多个场景."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(3):
        mos.record_capability_calibration(capability_ref="ref://scene/good", success_rate=0.9, sample_size=5)
        mos.record_capability_calibration(capability_ref="ref://scene/bad", success_rate=0.2, sample_size=5)
    mon = DriftMonitor(mos_manager=mos, threshold=0.6, window=10, root=tmp_path)
    results = mon.check_all_scenes(["good", "bad"])
    assert len(results) == 2
    by_id = {r.scene_id: r for r in results}
    assert by_id["good"].is_degraded is False
    assert by_id["bad"].is_degraded is True
