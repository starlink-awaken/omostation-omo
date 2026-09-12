"""test_scene_lifecycle_cruiser.py — T7-05 巡航器与金牌样例库单元测试.

覆盖: 自动晋级 (3-sample→shadow, 30+0.6→assisted/supervised)、校准不足
hold、calibration<0.5 熔断降级、routine 需人工、金牌样例去重与检索排序。
"""

from __future__ import annotations

import pytest

from omo.scene.cruiser import CruiseDecision, CruiserError, SceneLifecycleCruiser
from omo.scene.golden_samples import GoldenSampleError, GoldenSampleStore


@pytest.fixture()
def cruiser() -> SceneLifecycleCruiser:
    return SceneLifecycleCruiser()


def test_promote_draft_to_shadow(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "draft", n_samples=3, calibration=0.0)
    assert (d.action, d.lifecycle) == ("promote", "shadow")
    assert d.code == "promote/shadow-ready"


def test_hold_draft_insufficient_samples(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "draft", n_samples=2, calibration=1.0)
    assert d.action == "hold"
    assert d.code == "hold/shadow-not-ready"


def test_promote_shadow_to_assisted(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "shadow", n_samples=30, calibration=0.6)
    assert (d.action, d.lifecycle) == ("promote", "assisted")


def test_hold_assisted_low_calibration(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "shadow", n_samples=50, calibration=0.59)
    assert d.action == "hold"


def test_demote_on_calibration_drop(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "assisted", n_samples=100, calibration=0.49)
    assert (d.action, d.lifecycle) == ("demote", "shadow")
    assert d.code == "demote/calibration-drop"


def test_no_demote_below_assisted(cruiser: SceneLifecycleCruiser) -> None:
    # shadow 档无执行风险, 低校准只 hold 不降级
    d = cruiser.observe("s1", "shadow", n_samples=10, calibration=0.1)
    assert d.action == "hold"


def test_routine_gate_needs_human(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "supervised", n_samples=99, calibration=0.99)
    assert d.action == "needs_human"
    assert d.code == "needs_human/routine-gate"
    assert d.lifecycle == "supervised"  # 不自动晋级


def test_routine_top_hold(cruiser: SceneLifecycleCruiser) -> None:
    d = cruiser.observe("s1", "routine", n_samples=999, calibration=1.0)
    assert (d.action, d.code) == ("hold", "hold/at-top")


def test_invalid_lifecycle_raises(cruiser: SceneLifecycleCruiser) -> None:
    with pytest.raises(CruiserError):
        cruiser.observe("s1", "flying", n_samples=5)


def test_cruise_all_skips_invalid() -> None:
    out = SceneLifecycleCruiser().cruise_all(
        [
            {"scene_id": "s1", "lifecycle": "draft", "n_samples": 3},
            {"scene_id": "", "lifecycle": "draft", "n_samples": 3},
        ]
    )
    assert out[0].action == "promote"
    assert out[1].code.startswith("hold/invalid")


def test_golden_record_and_find() -> None:
    store = GoldenSampleStore()
    store.record("s1", {"q": "公文拟办"}, "拟办模板 v1", calibration=0.7)
    store.record("s1", {"q": "会议督办"}, "督办清单 v1", calibration=0.9)
    top = store.find_similar("s1", top_k=2)
    assert [s.calibration for s in top] == [0.9, 0.7]
    assert store.find_similar("nope") == []


def test_golden_dedup_overwrite() -> None:
    store = GoldenSampleStore()
    store.record("s1", {"q": "x"}, "v1", calibration=0.6)
    store.record("s1", {"q": "x"}, "v2", calibration=0.8)
    assert store.count("s1") == 1
    assert store.find_similar("s1")[0].result_summary == "v2"


def test_golden_bad_calibration() -> None:
    with pytest.raises(GoldenSampleError):
        GoldenSampleStore().record("s1", {"q": "x"}, "v", calibration=1.5)


def test_golden_save_load(tmp_path) -> None:
    p = tmp_path / "golden.jsonl"
    store = GoldenSampleStore()
    store.record("s1", {"q": "x"}, "v1", calibration=0.75)
    store.save(p)
    reloaded = GoldenSampleStore(p)
    assert reloaded.count("s1") == 1
    assert reloaded.find_similar("s1")[0].input_digest == store.find_similar("s1")[0].input_digest


def test_decision_is_frozen() -> None:
    d = CruiseDecision(action="hold", code="x", scene_id="s", lifecycle="draft")
    with pytest.raises(Exception):
        d.action = "promote"  # type: ignore[misc]
