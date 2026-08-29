"""Unit tests for omo.resident.decision — event-driven decision proposals (WP-F).

M2.1c: 覆盖决策链:
- 触发事件 (WorkflowFailed/StepFailed/StepTimeout) → 写提案 JSON (trace_id provenance)
- 非触发事件 → 返回 None, 不写提案
- proposal 带 schema/trigger_event/proposal_count
- trace_id 兜底 (event_id / 空 → "event")
- T10-57: 空 provenance 事件丢弃; 同 (event_type, trace_id) 每 UTC 天至多一份草稿
"""

from __future__ import annotations

import json

import pytest

from omo.resident import decision


@pytest.fixture(autouse=True)
def _isolate_proposal_dir(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point PROPOSAL_DIR *and* INBOX_DIR at tmp dirs and stub the scan.

    T10-57: INBOX_DIR must be isolated too — before this fix every pytest run
    leaked its fixture drafts into the real `.omo/_knowledge/decision-proposals`
    inbox (the md writer reads the module-level INBOX_DIR directly).
    _write_proposal derives the return path via relative_to(WORKSPACE), so
    WORKSPACE must also point under tmp_path for relative_to succeed.
    """
    monkeypatch.setattr(decision, "WORKSPACE", tmp_path)
    monkeypatch.setattr(decision, "PROPOSAL_DIR", tmp_path / ".omo" / "_knowledge" / "evolution-proposals")
    monkeypatch.setattr(decision, "INBOX_DIR", tmp_path / ".omo" / "_knowledge" / "decision-proposals")
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


def test_decide_empty_provenance_dropped() -> None:
    """T10-57: no trace_id AND no event_id → unactionable draft, not written."""
    path = decision._decide({"event_type": "WorkflowFailed"})
    assert path is None
    assert _proposal_paths() == []


def test_decide_same_trace_deduped_same_utc_day() -> None:
    """T10-57: retries of the same (event_type, trace_id) same day → one draft."""
    first = decision._decide({"event_type": "WorkflowFailed", "trace_id": "trace-dup"})
    assert first is not None
    second = decision._decide({"event_type": "WorkflowFailed", "trace_id": "trace-dup"})
    assert second is None
    assert len(_proposal_paths()) == 1


def test_decide_same_trace_different_type_not_deduped() -> None:
    first = decision._decide({"event_type": "WorkflowFailed", "trace_id": "trace-multi"})
    assert first is not None
    second = decision._decide({"event_type": "StepFailed", "trace_id": "trace-multi"})
    # not deduped; same-second writes share a filename, so the JSON content
    # (not file count) proves the second event type went through
    assert second is not None
    from pathlib import Path

    target = decision.PROPOSAL_DIR / Path(second).name
    data = json.loads(target.read_text(encoding="utf-8"))
    assert data["trigger_event"]["event_type"] == "StepFailed"
