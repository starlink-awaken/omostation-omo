"""Unit tests for omo.resident.decision — event-driven decision proposals (WP-F).

M2.1c: 覆盖决策链:
- 触发事件 (WorkflowFailed/StepFailed/StepTimeout) → 写提案 JSON (trace_id provenance)
- 非触发事件 → 返回 None, 不写提案
- proposal 带 schema/trigger_event/proposal_count
- trace_id 兜底 (event_id / 空 → "event")
"""

from __future__ import annotations

import json

import pytest

from omo.resident import decision


@pytest.fixture(autouse=True)
def _isolate_proposal_dir(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point PROPOSAL_DIR at a tmp dir and stub the best-effort scan.

    _write_proposal derives the return path via relative_to(WORKSPACE), so
    WORKSPACE must also point under tmp_path for relative_to to succeed.
    """
    monkeypatch.setattr(decision, "WORKSPACE", tmp_path)
    monkeypatch.setattr(decision, "PROPOSAL_DIR", tmp_path / ".omo" / "_knowledge" / "evolution-proposals")
    monkeypatch.setattr(decision, "_scan_proposals", lambda: [{"kind": "stub-finding"}])


def _proposal_paths() -> list:
    if not decision.PROPOSAL_DIR.exists():
        return []
    return sorted(decision.PROPOSAL_DIR.glob("decision-*.json"))


def test_decide_trigger_event_writes_proposal() -> None:
    path = decision._decide(
        {"event_type": "WorkflowFailed", "trace_id": "trace-abc", "workflow_run_id": "run-1", "event_id": "evt-1"}
    )
    assert path is not None
    paths = _proposal_paths()
    assert len(paths) == 1
    data = json.loads(paths[0].read_text(encoding="utf-8"))
    assert data["schema"] == "resident-decision/v1"
    assert data["trigger_event"]["event_type"] == "WorkflowFailed"
    assert data["trigger_event"]["trace_id"] == "trace-abc"
    assert data["trigger_event"]["workflow_run_id"] == "run-1"
    assert data["proposal_count"] == 1
    assert data["proposals"] == [{"kind": "stub-finding"}]


def test_decide_step_failed_and_step_timeout_trigger() -> None:
    for evt in ("StepFailed", "StepTimeout"):
        decision.PROPOSAL_DIR.mkdir(parents=True, exist_ok=True)
        before = len(_proposal_paths())
        path = decision._decide({"event_type": evt, "trace_id": f"trace-{evt}"})
        assert path is not None
        assert len(_proposal_paths()) == before + 1


def test_decide_non_trigger_event_returns_none() -> None:
    path = decision._decide({"event_type": "Info", "trace_id": "t1"})
    assert path is None
    assert _proposal_paths() == []


def test_decide_trace_id_falls_back_to_event_id() -> None:
    path = decision._decide({"event_type": "WorkflowFailed", "event_id": "evt-fallback"})
    assert path is not None
    assert "evt-fallback" in path


def test_decide_trace_id_empty_uses_event_token() -> None:
    path = decision._decide({"event_type": "WorkflowFailed"})
    assert path is not None
    # empty trace_id → "event" token, path relative to WORKSPACE (no leading slash)
    assert not path.startswith("/")
