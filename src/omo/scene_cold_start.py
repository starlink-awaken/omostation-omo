"""scene_cold_start.py — 新场景冷启动 < 2 周 (BET-Y3H1-T3-01).

证明复利存在 — 新场景能复用既有校准而非从零积累.

核心机制:
1. 模板匹配: 根据新场景的 scene_type + required_capabilities 匹配最佳源场景
2. 校准播种: 从源场景迁移初始 calibration (success_rate, sample_size)
3. 复用追溯: 记录复用来源, 确保可解释
4. 冷启动追踪: 记录从 draft → shadow 稳定所需时间

设计决策:
- 数据源: MOSBeliefManager.capability_calibration (已有)
- 匹配算法: capability_ref 子串匹配 + scene_type 加权
- 保守播种: 初始 calibration 打 0.8 折 (承认场景差异)
- 复用门槛: 源场景 sample_size >= 10 才参与复用

守 ADR-0372: 播种记录入 audit log.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# 冷启动校准折扣 (承认场景差异, 避免过度自信)
COLD_START_DISCOUNT = 0.8
# 源场景最低样本数门槛
MIN_SOURCE_SAMPLES = 10
# 冷启动初始 calibration 上限 (防止虚高)
MAX_INITIAL_RATE = 0.6


@dataclass
class ColdStartPlan:
    """新场景冷启动方案."""

    target_scene_id: str
    target_scene_type: str
    source_scene_id: str | None
    source_calibration_count: int
    initial_success_rate: float | None
    initial_sample_size: int
    transferred_calibration_id: str | None
    estimated_weeks: float  # 预估冷启动周数
    provenance: str  # 复用来源解释
    created_at: float = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_scene_id": self.target_scene_id,
            "target_scene_type": self.target_scene_type,
            "source_scene_id": self.source_scene_id,
            "source_calibration_count": self.source_calibration_count,
            "initial_success_rate": self.initial_success_rate,
            "initial_sample_size": self.initial_sample_size,
            "transferred_calibration_id": self.transferred_calibration_id,
            "estimated_weeks": self.estimated_weeks,
            "provenance": self.provenance,
            "created_at_ms": self.created_at,
        }


class SceneColdStartPlanner:
    """新场景冷启动规划器 — 匹配最佳源场景并播种校准."""

    def __init__(
        self,
        mos_manager: Any | None = None,
        *,
        discount: float = COLD_START_DISCOUNT,
        min_source_samples: int = MIN_SOURCE_SAMPLES,
        max_initial_rate: float = MAX_INITIAL_RATE,
    ) -> None:
        self.mos_manager = mos_manager
        self.discount = discount
        self.min_source_samples = min_source_samples
        self.max_initial_rate = max_initial_rate

    def plan_cold_start(
        self,
        target_scene_id: str,
        target_scene_type: str = "internal_pipeline",
        *,
        required_capabilities: list[str] | None = None,
        operator: str = "",
    ) -> ColdStartPlan:
        """为新场景生成冷启动方案."""
        if self.mos_manager is None:
            return ColdStartPlan(
                target_scene_id=target_scene_id,
                target_scene_type=target_scene_type,
                source_scene_id=None,
                source_calibration_count=0,
                initial_success_rate=None,
                initial_sample_size=0,
                transferred_calibration_id=None,
                estimated_weeks=4.0,  # 无复用时默认 4 周
                provenance="无源场景可用, 从零积累",
            )
        # 1. 查找最佳源场景 (calibration 最多 + 样本充足)
        source_scene, source_cals = self._find_best_source(required_capabilities or [])
        if source_scene is None:
            return ColdStartPlan(
                target_scene_id=target_scene_id,
                target_scene_type=target_scene_type,
                source_scene_id=None,
                source_calibration_count=0,
                initial_success_rate=None,
                initial_sample_size=0,
                transferred_calibration_id=None,
                estimated_weeks=4.0,
                provenance="无满足门槛的源场景 (样本不足)",
            )
        # 2. 迁移校准 (打折)
        total_samples = sum(c.get("sample_size", 1) for c in source_cals)
        weighted_rate = sum(
            c.get("success_rate", 0.0) * c.get("sample_size", 1) for c in source_cals
        ) / total_samples
        seeded_rate = min(weighted_rate * self.discount, self.max_initial_rate)
        # 3. 执行迁移
        transferred_id = self.mos_manager.transfer_calibration(
            source_scene,
            f"ref://scene/{target_scene_id}/initial",
            min_samples=3,  # 冷启动门槛更低
            operator=f"cold-start-{operator}",
        )
        # 4. 预估冷启动周数 (源样本越多 → 越快)
        estimated_weeks = max(0.5, 3.0 - (total_samples / 50))
        return ColdStartPlan(
            target_scene_id=target_scene_id,
            target_scene_type=target_scene_type,
            source_scene_id=source_scene,
            source_calibration_count=len(source_cals),
            initial_success_rate=round(seeded_rate, 4),
            initial_sample_size=total_samples,
            transferred_calibration_id=transferred_id,
            estimated_weeks=round(estimated_weeks, 1),
            provenance=(
                f"复用源场景 {source_scene} 的 {len(source_cals)} 条校准 "
                f"(样本 {total_samples}, 原始 rate {weighted_rate:.2f}, "
                f"打折后 {seeded_rate:.2f})"
            ),
        )

    def _find_best_source(
        self, required_capabilities: list[str]
    ) -> tuple[str | None, list[dict[str, Any]]]:
        """查找最佳源场景 (样本数最多且满足门槛)."""
        state = getattr(self.mos_manager, "_load_state", lambda: {})()
        cals = state.get("capability_calibrations", [])
        # 按 capability_ref 分组, 找样本最多的场景
        scene_samples: dict[str, list[dict[str, Any]]] = {}
        for c in cals:
            ref = c.get("capability_ref", "")
            # 提取 scene id from ref (ref://scene/<id>/...)
            # ref://scene/<id>/... → ["ref:", "", "scene", <id>, ...]
            parts = ref.split("/")
            if len(parts) >= 5 and parts[0] == "ref:" and parts[2] == "scene":
                scene_id = parts[3]
                if required_capabilities and not any(cap in ref for cap in required_capabilities):
                    continue
                scene_samples.setdefault(scene_id, []).append(c)
        # 找样本数最多且满足门槛的场景
        best_scene: str | None = None
        best_cals: list[dict[str, Any]] = []
        for scene_id, scene_cals in scene_samples.items():
            total = sum(c.get("sample_size", 1) for c in scene_cals)
            if total >= self.min_source_samples and total > sum(
                c.get("sample_size", 1) for c in best_cals
            ):
                best_scene = scene_id
                best_cals = scene_cals
        return best_scene, best_cals


__all__ = [
    "COLD_START_DISCOUNT",
    "MAX_INITIAL_RATE",
    "MIN_SOURCE_SAMPLES",
    "ColdStartPlan",
    "SceneColdStartPlanner",
]
