"""Tests for OMOProjectInspector."""

from pathlib import Path
from omo.omo_project_inspector import OMOProjectInspector


def test_inspect_knowledge_project():
    """Verify knowledge complex inspection succeeds and detects tests."""
    inspector = OMOProjectInspector()
    res = inspector.inspect_project("knowledge")
    assert res["ok"] is True
    assert res["layer"] == "L2"
    assert res["has_tests"] is True
    assert res["health_score"] >= 85


def test_inspect_all_projects():
    """Verify batch project inspection."""
    inspector = OMOProjectInspector()
    data = inspector.inspect_all_projects()
    assert "knowledge" in data["projects"]
    assert data["overall_avg_health"] >= 80.0
