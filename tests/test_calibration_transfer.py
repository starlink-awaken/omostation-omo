"""Tests for calibration transfer (BET-Y2Q3-T3-01)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.omo_belief import MOSBeliefManager


def test_transfer_calibration_basic(tmp_path: Path):
    """基础迁移: 公文场景校准 → 知识场景."""
    mos = MOSBeliefManager(root=tmp_path)
    # 写入 5 条公文场景校准
    for _ in range(5):
        mos.record_capability_calibration(
            capability_ref="ref://scene/document-review/format-check",
            success_rate=0.85,
            sample_size=10,
        )
    # 迁移到知识场景
    cc_id = mos.transfer_calibration(
        "ref://scene/document-review/format-check",
        "ref://scene/knowledge-curation/quality-check",
        operator="test",
    )
    assert cc_id is not None
    assert cc_id.startswith("cc-")
    # 验证迁移结果
    state = mos._load_state()
    transferred = [c for c in state["capability_calibrations"] if c.get("transferred_from")]
    assert len(transferred) == 1
    t = transferred[0]
    assert t["capability_ref"] == "ref://scene/knowledge-curation/quality-check"
    assert t["transferred_from"] == "ref://scene/document-review/format-check"
    assert t["success_rate"] == pytest.approx(0.85)
    assert t["sample_size"] == 50  # 5 * 10
    assert t["schema"] == "calibration-transfer/v1"


def test_transfer_insufficient_samples(tmp_path: Path):
    """样本不足时迁移返回 None."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(2):  # 仅 2 条, 低于默认 min_samples=5
        mos.record_capability_calibration(
            capability_ref="ref://scene/x",
            success_rate=0.9,
            sample_size=3,
        )
    result = mos.transfer_calibration("ref://scene/x", "ref://scene/y")
    assert result is None


def test_transfer_nonexistent_source(tmp_path: Path):
    """源校准不存在时返回 None."""
    mos = MOSBeliefManager(root=tmp_path)
    result = mos.transfer_calibration("ref://scene/nonexistent", "ref://scene/y")
    assert result is None


def test_transfer_weighted_average(tmp_path: Path):
    """迁移使用加权平均 (样本数作为权重)."""
    mos = MOSBeliefManager(root=tmp_path)
    # 不同样本数的校准
    mos.record_capability_calibration("ref://s", success_rate=0.5, sample_size=10)
    mos.record_capability_calibration("ref://s", success_rate=1.0, sample_size=30)
    mos.record_capability_calibration("ref://s", success_rate=0.8, sample_size=10)
    cc_id = mos.transfer_calibration("ref://s", "ref://t", min_samples=3)
    assert cc_id is not None
    state = mos._load_state()
    t = [c for c in state["capability_calibrations"] if c.get("transferred_from")][0]
    # 加权平均: (0.5*10 + 1.0*30 + 0.8*10) / 50 = 43/50 = 0.86
    assert t["success_rate"] == pytest.approx(0.86, abs=0.01)


def test_transfer_no_error_state_crosstalk(tmp_path: Path):
    """迁移不引入错误状态串扰 (源错误状态不影响目标)."""
    mos = MOSBeliefManager(root=tmp_path)
    # 源: 低成功率 (5 条)
    for _ in range(5):
        mos.record_capability_calibration("ref://failing-scene", success_rate=0.2, sample_size=5)
    # 目标: 已有高成功率 (1 条)
    mos.record_capability_calibration("ref://target-scene", success_rate=0.95, sample_size=20)
    # 迁移 (低 → 高)
    mos.transfer_calibration("ref://failing-scene", "ref://target-scene")
    state = mos._load_state()
    target_cals = [c for c in state["capability_calibrations"] if c["capability_ref"] == "ref://target-scene"]
    # 目标应有 2 条: 原始高 + 迁移低
    assert len(target_cals) == 2
    rates = sorted(c["success_rate"] for c in target_cals)
    assert rates[0] == pytest.approx(0.2)  # 迁移来的低
    assert rates[1] == pytest.approx(0.95)  # 原始高


def test_transfer_audit_trail(tmp_path: Path):
    """迁移操作写入审计日志."""
    mos = MOSBeliefManager(root=tmp_path)
    for _ in range(5):
        mos.record_capability_calibration("ref://a", success_rate=0.8, sample_size=10)
    mos.transfer_calibration("ref://a", "ref://b", operator="tester")
    log = mos.audit_log_file.read_text()
    assert "TRANSFER_CALIBRATION" in log
    assert "from=ref://a" in log
    assert "to=ref://b" in log
    assert "by=tester" in log
