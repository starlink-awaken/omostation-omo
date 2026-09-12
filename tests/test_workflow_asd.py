"""test_workflow_asd.py — ASD 五核心面板数据契约测试（BET-Y1Q4-T10-166）。"""

from datetime import UTC, datetime

import pytest

from omo.workflow.asd import (
    Panel,
    PanelProvenance,
    SCHEMA,
    attach_panel,
    new_snapshot,
    validate,
    verdict,
)


def _panel(pid: str, fresh: int | None = 300, gaps: list[str] | None = None) -> Panel:
    return Panel(
        panel_id=pid,
        data={"sample": pid},
        provenance=PanelProvenance(source="test://unit", freshness_seconds=fresh),
        gaps=gaps or [],
    )


class TestPanelDegradation:
    def test_fresh_no_gap_is_complete(self) -> None:
        assert not _panel("overview").degraded

    def test_unknown_freshness_is_degraded(self) -> None:
        assert _panel("spine", fresh=None).degraded

    def test_gap_is_degraded(self) -> None:
        assert _panel("agents", gaps=["no_source"]).degraded


class TestAttachAndVerdict:
    def test_all_fresh_complete(self) -> None:
        snap = new_snapshot()
        for pid in ("overview", "spine", "agents", "milestones", "degradation"):
            attach_panel(snap, _panel(pid))
        assert verdict(snap) == "COMPLETE"
        assert validate(snap) == []

    def test_one_degraded_partial(self) -> None:
        snap = new_snapshot()
        for pid in ("overview", "spine", "agents", "milestones", "degradation"):
            attach_panel(snap, _panel(pid, gaps=["x"] if pid == "agents" else []))
        assert verdict(snap) == "PARTIAL"
        assert "agents" in snap["degraded_panels"]

    def test_empty_is_empty(self) -> None:
        assert verdict(new_snapshot()) == "EMPTY"

    def test_unknown_panel_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown ASD panel"):
            attach_panel(new_snapshot(), _panel("bogus"))


class TestValidate:
    def test_schema_mismatch(self) -> None:
        snap = new_snapshot()
        snap["schema"] = "wrong"
        assert any("schema mismatch" in p for p in validate(snap))

    def test_green_wash_defense(self) -> None:
        """gap 存在但 degraded=false 应被校验捕获（防线：绝不 green-wash）。"""
        snap = new_snapshot()
        snap["panels"] = {
            "overview": {"data": {}, "provenance": {"source": "t", "freshness_seconds": 300},
                         "gaps": ["x"], "degraded": False},
            "spine": {"data": {}, "provenance": {"source": "t", "freshness_seconds": 300},
                      "gaps": [], "degraded": False},
            "agents": {"data": {}, "provenance": {"source": "t", "freshness_seconds": 300},
                       "gaps": [], "degraded": False},
            "milestones": {"data": {}, "provenance": {"source": "t", "freshness_seconds": 300},
                           "gaps": [], "degraded": False},
            "degradation": {"data": {}, "provenance": {"source": "t", "freshness_seconds": 300},
                            "gaps": [], "degraded": False},
        }
        assert any("degraded flag inconsistent" in p for p in validate(snap))
