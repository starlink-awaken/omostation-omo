"""Unit tests for omo.resident.sediment — event → knowledge draft.

M2.1b: 验证事件驱动知识沉淀:
- success 事件 → runs/ 复盘草稿
- failure 事件 → failures/ 失败模式草稿
- 无关事件 → 忽略
- _safe_slug 文件名清洗
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.resident import sediment


@pytest.fixture(autouse=True)
def _isolate_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sediment, "SEDIMENT_ROOT", tmp_path)


def _success_event(**overrides) -> dict:
    event = {
        "event_type": "WorkflowClosed",
        "workflow_run_id": "20260823T0000Z-project-code-change-abc123",
        "event_id": "evt_1234",
        "trace_id": "trace-abc",
        "producer": "workflow-mesh",
        "occurred_at": "2026-08-23T00:00:00Z",
    }
    event.update(overrides)
    return event


def test_consume_success_writes_runs_draft(tmp_path: Path) -> None:
    path = sediment.consume_event(_success_event())
    assert path is not None
    assert path.is_file()
    assert path.parent == tmp_path / "runs"
    text = path.read_text(encoding="utf-8")
    assert "运行复盘沉淀" in text
    assert "WorkflowClosed" in text
    assert "20260823T0000Z-project-code-change-abc123" in text


def test_consume_personal_signal_writes_signals_draft(tmp_path: Path) -> None:
    """个人文件信号 → 信号沉淀草稿 (signals/ 目录)."""
    event = {
        "event_type": "PersonalSignal",
        "event_id": "evt_sig1",
        "trace_id": "personal-signal:idea",
        "occurred_at": "2026-08-23T01:00:00Z",
        "producer": "personal-signals",
        "payload": {"file": "my-idea.md", "content_digest": "sha256:abc", "source": "personal-signals"},
    }
    path = sediment.consume_event(event)
    assert path is not None
    assert path.is_file()
    assert path.parent == tmp_path / "signals"
    assert path.name == "my-idea.md"
    text = path.read_text(encoding="utf-8")
    assert "个人信号沉淀" in text
    assert "my-idea.md" in text


def test_consume_inbox_signal_writes_inbox_draft(tmp_path: Path) -> None:
    """感知文件夹信号 → 感知信号沉淀草稿 (inbox/ 目录, T10-15)."""
    event = {
        "event_type": "InboxSignal",
        "event_id": "evt_inbox1",
        "trace_id": "inbox:research-2026-08-19-001",
        "occurred_at": "2026-08-25T00:00:00Z",
        "producer": "perception-inbox",
        "payload": {
            "file": "research-2026-08-19-001.md",
            "content_digest": "sha256:abc",
            "source": "perception-inbox",
        },
    }
    path = sediment.consume_event(event)
    assert path is not None
    assert path.is_file()
    assert path.parent == tmp_path / "inbox"
    assert path.name == "research-2026-08-19-001.md"
    text = path.read_text(encoding="utf-8")
    assert "感知信号沉淀" in text
    assert "research-2026-08-19-001.md" in text
    assert "perception-inbox" in text


def test_consume_failure_writes_failures_draft(tmp_path: Path) -> None:
    event = _success_event(event_type="StepFailed")
    path = sediment.consume_event(event)
    assert path is not None
    assert path.is_file()
    assert path.parent == tmp_path / "failures"
    text = path.read_text(encoding="utf-8")
    assert "失败模式沉淀" in text
    assert "StepFailed" in text
    # failure 文件名带 event_id 前缀避免同 run 多失败覆盖
    assert "evt_1234" in path.name


def test_consume_ignores_unrelated_event(tmp_path: Path) -> None:
    event = _success_event(event_type="UnknownEventType")
    assert sediment.consume_event(event) is None
    assert not (tmp_path / "runs").exists()
    assert not (tmp_path / "failures").exists()


def test_consume_succeeded_in_success_set(tmp_path: Path) -> None:
    path = sediment.consume_event(_success_event(event_type="WorkflowSucceeded"))
    assert path is not None
    assert path.parent == tmp_path / "runs"


def test_consume_admitted_is_success(tmp_path: Path) -> None:
    """WorkflowAdmitted (T10-12) 归入 SUCCESS_EVENTS → runs 草稿."""
    path = sediment.consume_event(_success_event(event_type="WorkflowAdmitted"))
    assert path is not None
    assert path.parent == tmp_path / "runs"
    assert "运行复盘沉淀" in path.read_text(encoding="utf-8")


def test_consume_lifecycle_writes_runs_draft(tmp_path: Path) -> None:
    """生命周期事件 (WorkflowRequested) → runs 草稿 (生命周期沉淀)."""
    event = _success_event(event_type="WorkflowRequested", workflow_run_id="20260825T0000Z-run-life")
    path = sediment.consume_event(event)
    assert path is not None
    assert path.is_file()
    assert path.parent == tmp_path / "runs"
    assert path.name == "20260825T0000Z-run-life.md"
    text = path.read_text(encoding="utf-8")
    assert "生命周期沉淀" in text
    assert "WorkflowRequested" in text


def test_lifecycle_run_aggregation_idempotent(tmp_path: Path) -> None:
    """同 run 多生命周期事件 → 同一草稿文件, exists 跳过不覆盖 (幂等聚合)."""
    run_id = "20260825T0000Z-run-agg"
    first = sediment.consume_event(
        _success_event(event_type="WorkflowRequested", workflow_run_id=run_id, event_id="evt_req")
    )
    assert first is not None
    first_text = first.read_text(encoding="utf-8")

    # 同 run 后续 StepStarted / StepDispatched → 同文件, 不覆盖 (仍为 WorkflowRequested 内容)
    second = sediment.consume_event(
        _success_event(event_type="StepStarted", workflow_run_id=run_id, event_id="evt_step")
    )
    third = sediment.consume_event(
        _success_event(event_type="StepDispatched", workflow_run_id=run_id, event_id="evt_disp")
    )
    assert second == first
    assert third == first
    runs = list((tmp_path / "runs").glob("*.md"))
    assert len(runs) == 1
    assert runs[0].read_text(encoding="utf-8") == first_text  # 幂等: 未覆盖
    assert "WorkflowRequested" in runs[0].read_text(encoding="utf-8")


def test_consume_evidence_writes_evidence_draft(tmp_path: Path) -> None:
    """EvidenceRecorded → evidence/ 草稿, frontmatter 带 event_id 溯源."""
    event = _success_event(
        event_type="EvidenceRecorded",
        workflow_run_id="delivery-run-pr-1893-v3",
        event_id="external-evidence:pr-1893",
    )
    path = sediment.consume_event(event)
    assert path is not None
    assert path.is_file()
    assert path.parent == tmp_path / "evidence"
    text = path.read_text(encoding="utf-8")
    assert "证据沉淀" in text
    assert "external-evidence:pr-1893" in text
    assert "delivery-run-pr-1893-v3" in text
    # 文件名含 run slug + event_id 前缀 (溯源)
    assert "delivery-run-pr-1893-v3" in path.name


def test_consume_step_timeout_is_failure(tmp_path: Path) -> None:
    path = sediment.consume_event(_success_event(event_type="StepTimeout"))
    assert path is not None
    assert path.parent == tmp_path / "failures"


def test_safe_slug_sanitizes() -> None:
    assert sediment._safe_slug("2026-08-23T00:00:00Z-run/with spaces!") == "2026-08-23T00-00-00Z-run-with-spaces"
    assert sediment._safe_slug("") == "unknown"
    assert sediment._safe_slug("a" * 200)[:80] == "a" * 80


def test_draft_contains_traceability(tmp_path: Path) -> None:
    event = _success_event()
    path = sediment.consume_event(event)
    assert path is not None
    text = path.read_text(encoding="utf-8")
    assert "trace_id: trace-abc" in text
    assert "event_id: evt_1234" in text
    assert "producer: workflow-mesh" in text
    assert "status: draft" in text
