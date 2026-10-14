"""scene/cruiser.py — 场景生命周期自动巡航器 (BET-Y1Q4-T7-05).

根据样本量与校准度, 自动推进场景卡在五档生命周期
(draft → shadow → assisted → supervised → routine) 中的跃迁与降级熔断。

阈值 SSOT: ``.omo/standards/scene-card-lifecycle.yaml``
- shadow 门: n_samples ≥ 3
- assisted 门: n_samples ≥ 30 且 calibration ≥ 0.6
- 熔断: calibration < 0.5 → 提议降级并告警 (proposal only, 需人类执行)
- routine 晋级: 必须人类确认, 本模块只返回 needs_human, 永不自动晋级

设计决策:
- 纯函数式判定, 零模型调用; 输入缺失返回 hold + 机器可读 code。
- 不修改任何场景卡定义, 只输出 CruiseDecision 供调用方/人类执行。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from omo.scene.anchor import ALLOWED_LIFECYCLES

_ACTION = Literal["promote", "demote", "hold", "needs_human"]

# lifecycle 顺序 (与 scene-card-lifecycle.yaml 对齐)
_ORDER = ("draft", "shadow", "assisted", "supervised", "routine")

# 晋级门: 下一档 → (最小样本数, 最小校准度)
_PROMOTE_GATES: dict[str, tuple[int, float]] = {
    "shadow": (3, 0.0),
    "assisted": (30, 0.6),
    "supervised": (30, 0.6),
}

# 熔断阈值 (circuit breaker: 校准骤降提议回退上一级, 需人类执行)
# SSOT: .omo/standards/scene-card-lifecycle.yaml → demotion
# (calibration < 0.5 且 sample_count >= 10 → 提议降一级)。
# 本模块只输出 CruiseDecision (action="demote" 即 needs_human 语义的提议),
# 调用方必须经人类之手执行 (`omo scene demote ...`), 永不自动变更场景卡。
_DEMOTE_CALIBRATION = 0.5


class CruiserError(Exception):
    """巡航判定失败 (附机器可读 code)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class CruiseDecision:
    """一次巡航判定结果."""

    action: str
    code: str
    scene_id: str
    lifecycle: str
    detail: str = ""


class SceneLifecycleCruiser:
    """场景生命周期自动巡航器 (无状态, 线程安全)."""

    def observe(
        self,
        scene_id: str,
        lifecycle: str,
        n_samples: int = 0,
        calibration: float = 0.0,
    ) -> CruiseDecision:
        """判定给定场景的下一步 lifecycle 动作."""
        if not scene_id:
            raise CruiserError("empty-scene-id", "scene_id 不能为空")
        if lifecycle not in ALLOWED_LIFECYCLES:
            raise CruiserError("unknown-lifecycle", f"未知 lifecycle: {lifecycle}")
        if n_samples < 0:
            raise CruiserError("bad-samples", f"n_samples 非法: {n_samples}")

        idx = _ORDER.index(lifecycle)

        # 熔断优先: 已在执行档 (assisted+) 且校准骤降 → 降级
        if idx >= _ORDER.index("assisted") and calibration < _DEMOTE_CALIBRATION:
            prev = _ORDER[idx - 1]
            return CruiseDecision(
                action="demote",
                code="demote/calibration-drop",
                scene_id=scene_id,
                lifecycle=prev,
                detail=f"calibration {calibration} < {_DEMOTE_CALIBRATION}, 回退至 {prev}",
            )

        # 顶档: routine 无下一档
        if lifecycle == "routine":
            return CruiseDecision(
                action="hold",
                code="hold/at-top",
                scene_id=scene_id,
                lifecycle=lifecycle,
                detail="已处 routine 顶档",
            )

        nxt = _ORDER[idx + 1]

        # supervised → routine 必须人类把关
        if nxt == "routine":
            gate = _PROMOTE_GATES.get("supervised", (30, 0.6))
            if n_samples >= gate[0] and calibration >= gate[1]:
                return CruiseDecision(
                    action="needs_human",
                    code="needs_human/routine-gate",
                    scene_id=scene_id,
                    lifecycle=lifecycle,
                    detail="达到 routine 门但需人类确认, 不自动晋级",
                )
            return CruiseDecision(
                action="hold",
                code="hold/routine-not-ready",
                scene_id=scene_id,
                lifecycle=lifecycle,
                detail=f"routine 门未达: 需 {gate[0]} 样本 + 校准 ≥ {gate[1]}",
            )

        gate = _PROMOTE_GATES.get(nxt)
        if gate is None:
            return CruiseDecision(
                action="hold",
                code="hold/no-gate",
                scene_id=scene_id,
                lifecycle=lifecycle,
                detail=f"无 {nxt} 晋级门定义",
            )
        min_samples, min_calib = gate
        if n_samples >= min_samples and calibration >= min_calib:
            return CruiseDecision(
                action="promote",
                code=f"promote/{nxt}-ready",
                scene_id=scene_id,
                lifecycle=nxt,
                detail=f"样本 {n_samples} ≥ {min_samples}, 校准 {calibration} ≥ {min_calib}",
            )
        return CruiseDecision(
            action="hold",
            code=f"hold/{nxt}-not-ready",
            scene_id=scene_id,
            lifecycle=lifecycle,
            detail=f"{nxt} 门未达: 需 {min_samples} 样本 + 校准 ≥ {min_calib}",
        )

    def cruise_all(self, observations: list[dict[str, Any]]) -> list[CruiseDecision]:
        """批量巡航; 单条非法观测跳过并记 hold/invalid, 不阻断整体."""
        out: list[CruiseDecision] = []
        for ob in observations:
            try:
                out.append(
                    self.observe(
                        scene_id=ob.get("scene_id", ""),
                        lifecycle=ob.get("lifecycle", ""),
                        n_samples=int(ob.get("n_samples", 0)),
                        calibration=float(ob.get("calibration", 0.0)),
                    )
                )
            except CruiserError as exc:
                out.append(
                    CruiseDecision(
                        action="hold",
                        code=f"hold/invalid:{exc.code}",
                        scene_id=str(ob.get("scene_id", "")),
                        lifecycle=str(ob.get("lifecycle", "")),
                        detail=str(exc),
                    )
                )
        return out
