"""drift_monitor.py — 漂移监控与自动降级 (BET-Y2Q3-T3-02).

放权后的能力退化能被自动发现并回收权限.

核心机制:
1. calibration 滑动窗口监控 — 按 scene_id + action_type 聚合最近 N 次 calibration
2. 跌破阈值自动降级 — success_rate < threshold → 产生降级事件
3. 降级后需人工复核方可回升 — 降级状态持久化, 只接受显式 restore

设计决策:
- 数据源: MOSBeliefManager.capability_calibration 表 (已有)
- 降级状态: .omo/state/agent-beliefs/drift-events.yaml (追加式事件日志)
- 滑动窗口: 默认 window=10 次 calibration, 可配置
- 阈值: 默认 0.6 (与 scene-card-lifecycle 的 min_calibration 对齐)

守 ADR-0372: 降级事件入 MOS decision_outcome.
守 F6: 降级可逆 (人工复核后 restore).
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.omo_io import write_yaml_atomic
from omo.omo_paths import WORKSPACE_ROOT
from omo.omo_shared import load_yaml_value

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 0.6
DEFAULT_WINDOW = 10


@dataclass
class DriftEvent:
    """单次漂移/降级事件."""

    scene_id: str
    action_type: str
    event_type: str  # "degraded" | "restored"
    measured_rate: float
    threshold: float
    window_size: int
    occurred_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    reason: str = ""
    restored: bool = False
    restored_at: str | None = None


@dataclass
class DriftStatus:
    """单场景漂移状态."""

    scene_id: str
    action_type: str
    current_rate: float | None
    threshold: float
    window_size: int
    sample_count: int
    is_degraded: bool
    last_event: str | None = None  # "degraded" | "restored" | None
    degraded_at: str | None = None


class DriftMonitor:
    """漂移监控器 — 滑动窗口 calibration 监控 + 自动降级."""

    def __init__(
        self,
        mos_manager: Any | None = None,
        *,
        threshold: float = DEFAULT_THRESHOLD,
        window: int = DEFAULT_WINDOW,
        root: Path | None = None,
    ) -> None:
        self.mos_manager = mos_manager
        self.threshold = threshold
        self.window = window
        self.root = root or WORKSPACE_ROOT
        self.events_file = self.root / ".omo" / "state" / "agent-beliefs" / "drift-events.yaml"

    # ── 公共 API ────────────────────────────────────────────────────

    def check_scene(self, scene_id: str, action_type: str = "") -> DriftStatus:
        """检查单场景漂移状态, 必要时触发降级."""
        rate, samples = self._windowed_rate(scene_id, action_type)
        is_degraded = rate is not None and rate < self.threshold

        status = DriftStatus(
            scene_id=scene_id,
            action_type=action_type,
            current_rate=rate,
            threshold=self.threshold,
            window_size=self.window,
            sample_count=len(samples),
            is_degraded=is_degraded,
        )

        if is_degraded and rate is not None:
            # 检查是否已处于降级状态 (避免重复降级)
            if not self._is_already_degraded(scene_id, action_type):
                self._emit_degraded(scene_id, action_type, rate, len(samples))
                status.last_event = "degraded"
                status.degraded_at = datetime.now(UTC).isoformat()
            else:
                status.last_event = "degraded"
                status.degraded_at = self._degraded_at(scene_id, action_type)
        else:
            status.last_event = "restored" if self._is_already_degraded(scene_id, action_type) else None

        return status

    def restore_scene(self, scene_id: str, action_type: str = "", *, operator: str = "") -> bool:
        """人工复核后回升 — 仅降级状态可回升."""
        if not self._is_already_degraded(scene_id, action_type):
            return False
        event = DriftEvent(
            scene_id=scene_id,
            action_type=action_type,
            event_type="restored",
            measured_rate=self._windowed_rate(scene_id, action_type)[0] or 0.0,
            threshold=self.threshold,
            window_size=self.window,
            reason=f"human review restore by {operator}",
            restored=True,
            restored_at=datetime.now(UTC).isoformat(),
        )
        self._append_event(event)
        return True

    def check_all_scenes(self, scene_ids: list[str]) -> list[DriftStatus]:
        """批量检查多个场景."""
        return [self.check_scene(sid) for sid in scene_ids]

    def get_degraded_scenes(self) -> list[dict[str, Any]]:
        """获取当前处于降级状态的所有场景."""
        events = self._load_events()
        degraded: dict[str, dict[str, Any]] = {}
        for ev in events:
            key = f"{ev.get('scene_id')}:{ev.get('action_type')}"
            if ev.get("event_type") == "degraded":
                degraded[key] = ev
            elif ev.get("event_type") == "restored":
                degraded.pop(key, None)
        return list(degraded.values())

    # ── 内部 ────────────────────────────────────────────────────────

    def _windowed_rate(self, scene_id: str, action_type: str) -> tuple[float | None, list[dict[str, Any]]]:
        """计算滑动窗口成功率."""
        if self.mos_manager is None:
            return None, []
        try:
            state = self.mos_manager._load_state()
            cals = state.get("capability_calibrations", [])
            # 过滤: scene_id 或 action_type 匹配 capability_ref
            filtered = [
                c
                for c in cals
                if scene_id in c.get("capability_ref", "")
                or (action_type and action_type in c.get("capability_ref", ""))
            ]
            if not filtered:
                # 无过滤命中时取全部 (全局 calibration)
                filtered = cals
            # 取最近 window 次
            windowed = filtered[-self.window :] if len(filtered) > self.window else filtered
            if not windowed:
                return None, []
            avg_rate = sum(c.get("success_rate", 0.0) for c in windowed) / len(windowed)
            return round(avg_rate, 4), windowed
        except Exception:
            logger.debug("windowed_rate failed", exc_info=True)
            return None, []

    def _is_already_degraded(self, scene_id: str, action_type: str) -> bool:
        return len(self.get_degraded_scenes()) > 0 and any(
            d.get("scene_id") == scene_id and d.get("action_type") == action_type for d in self.get_degraded_scenes()
        )

    def _degraded_at(self, scene_id: str, action_type: str) -> str | None:
        for d in self.get_degraded_scenes():
            if d.get("scene_id") == scene_id and d.get("action_type") == action_type:
                return d.get("occurred_at")
        return None

    def _emit_degraded(self, scene_id: str, action_type: str, rate: float, n: int) -> None:
        event = DriftEvent(
            scene_id=scene_id,
            action_type=action_type,
            event_type="degraded",
            measured_rate=rate,
            threshold=self.threshold,
            window_size=self.window,
            reason=f"calibration {rate:.2f} < threshold {self.threshold} (window={n})",
        )
        self._append_event(event)
        logger.warning(
            "scene %s degraded: rate=%.2f < threshold=%.2f",
            scene_id,
            rate,
            self.threshold,
        )

    def _append_event(self, event: DriftEvent) -> None:
        events = self._load_events()
        events.append(asdict(event))
        self.events_file.parent.mkdir(parents=True, exist_ok=True)
        write_yaml_atomic(self.events_file, events)

    def _load_events(self) -> list[dict[str, Any]]:
        if not self.events_file.exists():
            return []
        return load_yaml_value(self.events_file) or []


__all__ = [
    "DEFAULT_THRESHOLD",
    "DEFAULT_WINDOW",
    "DriftEvent",
    "DriftMonitor",
    "DriftStatus",
]
