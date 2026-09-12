"""asd.py — ASD 五核心面板数据契约（BET-Y1Q4-T10-166）。

机器可读的 ASD 快照：Overview / Spine / Agents / Milestones / Degradation。
每面板带来源、新鲜度、降级标记。铁律：observer-blindness-never-yields-green
——观测缺口永远显示 degraded，绝不显示 PASS。

只读契约：本模块不执行检查，只定义结构 + 校验；数据由调用方（驾驶舱/
观测站）填充。未过门（R0/AS0）的 adapter 不得成为面板的数据源。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

SCHEMA = "asd-snapshot/v1"
PANELS = ("overview", "spine", "agents", "milestones", "degradation")


@dataclass
class PanelProvenance:
    source: str  # 数据源标识（如 ssot://bet-ledger）
    freshness_seconds: int | None  # None = 未知新鲜度 → 强制 degraded
    digest: str | None = None  # 内容摘要（可复算）

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "freshness_seconds": self.freshness_seconds,
            "digest": self.digest,
        }


@dataclass
class Panel:
    panel_id: str
    data: dict[str, Any]
    provenance: PanelProvenance
    gaps: list[str] = field(default_factory=list)

    @property
    def degraded(self) -> bool:
        """降级判定：有 gap 或新鲜度未知即 degraded（绝不 green-wash）。"""
        return bool(self.gaps) or self.provenance.freshness_seconds is None


def new_snapshot(generated_at: datetime | None = None) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "generated_at": (generated_at or datetime.now(UTC)).isoformat(),
        "panels": {},
        "degraded_panels": [],
    }


def attach_panel(snapshot: dict[str, Any], panel: Panel) -> dict[str, Any]:
    """挂载面板并维护降级清单。panel_id 必须是五核心之一。"""
    if panel.panel_id not in PANELS:
        raise ValueError(f"unknown ASD panel: {panel.panel_id}")
    snapshot["panels"][panel.panel_id] = {
        "data": panel.data,
        "provenance": panel.provenance.to_dict(),
        "gaps": panel.gaps,
        "degraded": panel.degraded,
    }
    if panel.degraded and panel.panel_id not in snapshot["degraded_panels"]:
        snapshot["degraded_panels"].append(panel.panel_id)
    return snapshot


def verdict(snapshot: dict[str, Any]) -> str:
    """快照判定：任一面板 degraded → PARTIAL（永远不允许汇总成 PASS）。"""
    if not snapshot["panels"]:
        return "EMPTY"
    return "PARTIAL" if snapshot["degraded_panels"] else "COMPLETE"


def validate(snapshot: dict[str, Any]) -> list[str]:
    """结构校验，返回问题列表（空 = 合法）。"""
    problems: list[str] = []
    if snapshot.get("schema") != SCHEMA:
        problems.append(f"schema mismatch: {snapshot.get('schema')}")
    panels = snapshot.get("panels", {})
    for pid in PANELS:
        if pid not in panels:
            problems.append(f"missing panel: {pid}")
        elif not isinstance(panels[pid].get("data"), dict):
            problems.append(f"panel data not object: {pid}")
    for pid, p in panels.items():
        if pid not in PANELS:
            problems.append(f"unknown panel: {pid}")
        if "provenance" not in p:
            problems.append(f"panel missing provenance: {pid}")
        # green-wash 防线：degraded 标记与 gap/新鲜度必须一致
        expect_degraded = bool(p.get("gaps")) or p.get("provenance", {}).get("freshness_seconds") is None
        if bool(p.get("degraded")) != expect_degraded:
            problems.append(f"degraded flag inconsistent: {pid}")
    return problems
