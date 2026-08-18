"""Tests for projects/omo/src/omo/mental_model.py (BET-Y2Q1-T3-03)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from omo.mental_model import MentalContext, MentalModel
from omo.omo_belief import MOSBeliefManager


class _FakeIntentSource:
    """Test double for IntentModel."""

    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def whats_most_important(self, top_n: int = 3) -> Any:
        class _Result:
            def __init__(self, items: list[Any]) -> None:
                self.items = items[:top_n]

        return _Result(self._items)


class _FakePriority:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeItem:
    def __init__(self, title: str, priority_name: str) -> None:
        self.title = title
        self.priority = _FakePriority(priority_name)


def test_mental_context_to_rationale_empty():
    ctx = MentalContext()
    assert ctx.to_rationale() == "无三模型上下文 (默认阈值决策)"


def test_mental_context_to_rationale_parts():
    ctx = MentalContext(rationale_parts=["world delta: checks", "self calibration 0.40 < 0.6"])
    r = ctx.to_rationale()
    assert "world delta" in r
    assert "self calibration" in r


def test_no_models_returns_neutral(tmp_path: Path):
    """无 mos_manager / intent_source → adjustment=0, 退化为纯阈值."""
    m = MentalModel()
    ctx = m.context_for_decision("any-scene")
    assert ctx.adjustment == 0.0
    assert not ctx.world_has_delta
    assert ctx.intent_item_count == 0


def test_world_delta_triggers_caution(tmp_path: Path):
    """world 有 delta → adjustment -= 0.10."""
    mos = MOSBeliefManager(root=tmp_path)
    mos.record_world_snapshot(source="ci", domain="governance", observations={"checks": 38})
    mos.record_world_snapshot(source="ci", domain="governance", observations={"checks": 20})

    m = MentalModel(mos_manager=mos, world_domain="governance")
    ctx = m.context_for_decision("scene-x")
    assert ctx.world_has_delta is True
    assert "checks" in ctx.world_changed_fields
    assert ctx.adjustment == pytest.approx(-0.10)


def test_low_calibration_triggers_caution(tmp_path: Path):
    """self calibration < 0.6 → adjustment -= 0.15."""
    mos = MOSBeliefManager(root=tmp_path)
    mos.record_capability_calibration(
        capability_ref="ref://scene/test-action",
        success_rate=0.4,
        sample_size=10,
    )
    m = MentalModel(mos_manager=mos, calibration_threshold=0.6)
    ctx = m.context_for_decision("scene-x", action_type="test-action")
    assert ctx.self_calibration == 0.4
    assert ctx.adjustment == pytest.approx(-0.15)


def test_combined_world_and_self(tmp_path: Path):
    """world delta + low calibration → adjustment = -0.25."""
    mos = MOSBeliefManager(root=tmp_path)
    mos.record_world_snapshot(source="ci", domain="governance", observations={"checks": 38})
    mos.record_world_snapshot(source="ci", domain="governance", observations={"checks": 20})
    mos.record_capability_calibration(
        capability_ref="ref://scene/test-action",
        success_rate=0.4,
        sample_size=10,
    )
    m = MentalModel(mos_manager=mos, world_domain="governance")
    ctx = m.context_for_decision("scene-x", action_type="test-action")
    assert ctx.adjustment == pytest.approx(-0.25)


def test_intent_model_included(tmp_path: Path):
    """intent 模型注入后应出现在 rationale 中."""
    items = [_FakeItem("Ship critical fix", "CRITICAL")]
    intent = _FakeIntentSource(items)
    m = MentalModel(intent_source=intent)
    ctx = m.context_for_decision("scene-x")
    assert ctx.intent_item_count == 1
    assert ctx.intent_top_title == "Ship critical fix"
    assert ctx.intent_top_priority == "CRITICAL"
    assert any("intent top" in p for p in ctx.rationale_parts)


def test_adjustment_clamped(tmp_path: Path):
    """adjustment 范围限制在 [-0.30, +0.10]."""
    mos = MOSBeliefManager(root=tmp_path)
    mos.record_world_snapshot(source="s", domain="d", observations={"a": 1})
    mos.record_world_snapshot(source="s", domain="d", observations={"a": 2})
    mos.record_capability_calibration(capability_ref="ref://x", success_rate=0.1, sample_size=5)
    m = MentalModel(mos_manager=mos, world_domain="d")
    ctx = m.context_for_decision("s", action_type="x")
    assert -0.30 <= ctx.adjustment <= 0.10


class TestSceneWatcherMentalIntegration:
    """SceneWatcher + MentalModel 集成: 同一 node_output 不同上下文 → 不同决策."""

    def test_same_output_different_context_different_decision(self, tmp_path: Path):
        """核心 BET-Y2Q1-T3-03 验收: 同一 confidence 在低 calibration 下被降级."""
        from omo.scenewatcher import SceneWatcher

        # 无 mental model: confidence 0.85 >= 0.8 → pass
        sw_plain = SceneWatcher(scene_id="s1", scene_path=tmp_path / "card.yaml")
        result_pass = sw_plain.evaluate_confidence({"confidence": 0.85}, node="n1")
        assert result_pass.action == "pass"

        # 有 mental model + low calibration: 0.85 - 0.15 = 0.70 < 0.8 → human_veto
        mos = MOSBeliefManager(root=tmp_path)
        mos.record_capability_calibration(
            capability_ref="ref://scene/n1",
            success_rate=0.4,
            sample_size=10,
        )
        sw_mental = SceneWatcher(
            scene_id="s1",
            scene_path=tmp_path / "card.yaml",
            mos_manager=mos,
        )
        result_veto = sw_mental.evaluate_confidence({"confidence": 0.85}, node="n1")
        assert result_veto.action == "human_veto"
        assert result_veto.confidence < 0.85
        assert "[mental]" in result_veto.reason

    def test_rationale_is_explainable(self, tmp_path: Path):
        """决策理由必须包含驱动模型标注."""
        from omo.scenewatcher import SceneWatcher

        mos = MOSBeliefManager(root=tmp_path)
        mos.record_world_snapshot(source="ci", domain="governance", observations={"checks": 38})
        mos.record_world_snapshot(source="ci", domain="governance", observations={"checks": 20})
        sw = SceneWatcher(scene_id="s1", scene_path=tmp_path / "c.yaml", mos_manager=mos)
        result = sw.evaluate_confidence({"confidence": 0.90}, node="n1")
        assert "world delta" in result.reason or "mental" in result.reason
