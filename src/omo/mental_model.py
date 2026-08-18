"""mental_model.py — 心智模型: world + self + intent 三模型融合决策上下文.

BET-Y2Q1-T3-03: Agent 据心智模型决策 (脱离纯阈值).

SceneWatcher 不再只看 node_output confidence, 而是结合:
- world model: MOSBeliefManager.delta_from_previous — "和上次比变了什么"
- self model: MOSBeliefManager capability_calibration — "我做得怎么样"
- intent model: IntentModel.whats_most_important — "现在最重要的是哪件"

设计决策: 三模型作为决策上下文注入, 使同一 node_output 在不同历史/优先级下
产生不同决策, 且决策理由可解释 (rationale 标注驱动模型).

守 ADR-0372: 决策日志入 MOS.
守 SOLID D: IntentModel 通过 Protocol 注入, 不直接依赖 agora 子模块.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class IntentSource(Protocol):
    """IntentModel 最小接口 (避免直接依赖 agora.intent)."""

    def whats_most_important(self, top_n: int = 3) -> Any: ...


@dataclass
class MentalContext:
    """单次决策的三模型上下文快照."""

    # World model
    world_domain: str = ""
    world_has_delta: bool = False
    world_changed_fields: list[str] = field(default_factory=list)
    world_current: dict[str, Any] = field(default_factory=dict)

    # Self model
    self_calibration: float | None = None
    self_sample_size: int = 0

    # Intent model
    intent_top_title: str = ""
    intent_top_priority: str = ""
    intent_item_count: int = 0

    # 综合
    adjustment: float = 0.0  # 对 confidence 的调整量 (-0.3 ~ +0.1)
    rationale_parts: list[str] = field(default_factory=list)

    def to_rationale(self) -> str:
        """生成人类可读的决策理由."""
        if not self.rationale_parts:
            return "无三模型上下文 (默认阈值决策)"
        return "; ".join(self.rationale_parts)


class MentalModel:
    """心智模型: 从 MOS + IntentModel 组装决策上下文.

    三模型对决策的影响规则:
    - world.has_delta + 关键字段变化 → 提升警惕 (adjustment -= 0.1)
    - self.calibration < 0.6 → 能力不足, 提升人工介入 (adjustment -= 0.15)
    - intent.top_priority == CRITICAL 且与当前 scene 相关 → 可能需要加速 (adjustment += 0.05)
    - 三模型均无信号 → adjustment = 0 (退化为纯阈值)
    """

    def __init__(
        self,
        mos_manager: Any | None = None,
        intent_source: IntentSource | None = None,
        *,
        world_domain: str = "governance",
        calibration_threshold: float = 0.6,
    ) -> None:
        self.mos_manager = mos_manager
        self.intent_source = intent_source
        self.world_domain = world_domain
        self.calibration_threshold = calibration_threshold

    def context_for_decision(
        self,
        scene_id: str,
        *,
        action_type: str = "",
    ) -> MentalContext:
        """为单次决策组装三模型上下文."""
        ctx = MentalContext()

        # ── World model: 世界变了什么 ──────────────────────────────
        self._apply_world_model(ctx)

        # ── Self model: 我的能力如何 ──────────────────────────────
        self._apply_self_model(ctx, action_type)

        # ── Intent model: 最重要的是什么 ──────────────────────────
        self._apply_intent_model(ctx, scene_id)

        # ── 综合 adjustment ────────────────────────────────────────
        ctx.adjustment = self._compute_adjustment(ctx)
        return ctx

    # ── 内部 ────────────────────────────────────────────────────────

    def _apply_world_model(self, ctx: MentalContext) -> None:
        if self.mos_manager is None:
            return
        try:
            delta = self.mos_manager.delta_from_previous(self.world_domain)
            ctx.world_domain = self.world_domain
            ctx.world_has_delta = delta.get("has_delta", False)
            ctx.world_changed_fields = delta.get("changed_fields", [])
            cur = delta.get("current")
            ctx.world_current = (cur.get("observations", {}) if cur else {}) or {}
            if ctx.world_has_delta:
                ctx.rationale_parts.append(
                    f"world delta: {', '.join(ctx.world_changed_fields[:3])}"
                )
        except Exception:
            logger.debug("world model read failed", exc_info=True)

    def _apply_self_model(self, ctx: MentalContext, action_type: str) -> None:
        if self.mos_manager is None:
            return
        try:
            cal = self._latest_calibration(action_type)
            if cal is not None:
                ctx.self_calibration = cal.get("success_rate")
                ctx.self_sample_size = cal.get("sample_size", 0)
                if ctx.self_calibration is not None and ctx.self_calibration < self.calibration_threshold:
                    ctx.rationale_parts.append(
                        f"self calibration {ctx.self_calibration:.2f} < {self.calibration_threshold}"
                    )
        except Exception:
            logger.debug("self model read failed", exc_info=True)

    def _apply_intent_model(self, ctx: MentalContext, scene_id: str) -> None:
        if self.intent_source is None:
            return
        try:
            result = self.intent_source.whats_most_important(top_n=3)
            items = getattr(result, "items", [])
            ctx.intent_item_count = len(items)
            if items:
                top = items[0]
                ctx.intent_top_title = getattr(top, "title", "")
                ctx.intent_top_priority = getattr(getattr(top, "priority", None), "name", "")
                ctx.rationale_parts.append(
                    f"intent top: [{ctx.intent_top_priority}] {ctx.intent_top_title}"
                )
        except Exception:
            logger.debug("intent model read failed", exc_info=True)

    def _compute_adjustment(self, ctx: MentalContext) -> float:
        """综合三模型信号 → confidence 调整量."""
        adj = 0.0
        # World: 有 delta → 更谨慎
        if ctx.world_has_delta:
            adj -= 0.10
        # Self: 校准不足 → 更谨慎
        if ctx.self_calibration is not None and ctx.self_calibration < self.calibration_threshold:
            adj -= 0.15
        # Intent: CRITICAL 且 world 也在变 → 加速关注 (轻微正向)
        if ctx.intent_top_priority == "CRITICAL" and ctx.world_has_delta:
            adj += 0.05
        return max(-0.30, min(0.10, adj))

    def _latest_calibration(self, action_type: str) -> dict[str, Any] | None:
        """读取最新 calibration (defensive)."""
        state = getattr(self.mos_manager, "_load_state", lambda: {})()
        cals = state.get("capability_calibrations", [])
        if action_type:
            filtered = [c for c in cals if action_type in c.get("capability_ref", "")]
            cals = filtered or cals
        return cals[-1] if cals else None


__all__ = ["MentalModel", "MentalContext", "IntentSource"]
