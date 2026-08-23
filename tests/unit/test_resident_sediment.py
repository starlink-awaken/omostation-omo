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
    event = _success_event(event_type="StepStarted")
    assert sediment.consume_event(event) is None
    assert not (tmp_path / "runs").exists()
    assert not (tmp_path / "failures").exists()


def test_consume_succeeded_in_success_set(tmp_path: Path) -> None:
    path = sediment.consume_event(_success_event(event_type="WorkflowSucceeded"))
    assert path is not None
    assert path.parent == tmp_path / "runs"


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
