"""Unit tests for omo.resident.ledger_trace — 事件流 → 五问骨架确定性提取.

BET-Y1Q3-T10-17: 验证
- iter_run_sequences 按 workflow_run_id 聚合有序事件序列
- extract_deterministic_five_q 提取 objective/steps/outcome/failure/metrics
- 失败 run / 缺终态 run 的容错
- load_run_skeletons 全量索引
"""

from __future__ import annotations

import json
from pathlib import Path

from omo.resident import ledger_trace


def _write_events(path: Path, events: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events), encoding="utf-8")
    return path


def _event(
    event_type: str,
    run_id: str,
    occurred_at: str,
    payload: dict | None = None,
) -> dict:
    return {
        "event_id": f"ev-{event_type}-{run_id}-{occurred_at}",
        "event_type": event_type,
        "occurred_at": occurred_at,
        "payload": payload or {},
        "workflow_run_id": run_id,
    }


def _success_sequence() -> list[dict]:
    run_id = "20260803T060000Z-mini-abc123"
    return [
        _event(
            "WorkflowRequested",
            run_id,
            "2026-08-03T06:00:00.000000+00:00",
            {"objective": "real run test", "workflow_id": "mini"},
        ),
        _event(
            "StepStarted",
            run_id,
            "2026-08-03T06:00:05.000000+00:00",
            {"step_name": "execute", "ok": True, "status": "ok", "evidence_count": 1},
        ),
        _event(
            "WorkflowSucceeded",
            run_id,
            "2026-08-03T06:00:10.000000+00:00",
            {"ok": True, "status": "ok", "evidence_count": 1},
        ),
    ]


def _failure_sequence() -> list[dict]:
    run_id = "20260803T060000Z-bet-execution-fff111"
    return [
        _event(
            "WorkflowRequested",
            run_id,
            "2026-08-03T06:00:00.000000+00:00",
            {"objective": "run bet execution", "workflow_id": "bet-execution"},
        ),
        _event(
            "StepStarted",
            run_id,
            "2026-08-03T06:00:02.000000+00:00",
            {"step_name": "execute"},
        ),
        _event(
            "StepFailed",
            run_id,
            "2026-08-03T06:00:04.000000+00:00",
            {"step_name": "execute", "error": "workflow failed", "ok": False, "status": "failed"},
        ),
        _event(
            "WorkflowClosed",
            run_id,
            "2026-08-03T06:00:06.000000+00:00",
            {"ok": False, "status": "blocked", "evidence_count": 0},
        ),
    ]


def test_iter_run_sequences_groups_by_run_id(tmp_path: Path) -> None:
    path = _write_events(tmp_path / "events.jsonl", _success_sequence() + _failure_sequence())
    sequences = ledger_trace.iter_run_sequences(path)
    assert set(sequences) == {"20260803T060000Z-mini-abc123", "20260803T060000Z-bet-execution-fff111"}
    assert [e["event_type"] for e in sequences["20260803T060000Z-mini-abc123"]] == [
        "WorkflowRequested",
        "StepStarted",
        "WorkflowSucceeded",
    ]


def test_iter_run_sequences_skips_unrouted(tmp_path: Path) -> None:
    path = _write_events(
        tmp_path / "events.jsonl",
        [_event("WorkflowRequested", "r1", "2026-08-03T06:00:00.000000+00:00", {"objective": "a"})]
        + [{"event_type": "ExecutionRequested", "occurred_at": "2026-08-03T06:00:01.000000+00:00"}],
    )
    sequences = ledger_trace.iter_run_sequences(path)
    assert set(sequences) == {"r1"}


def test_extract_five_q_success_run(tmp_path: Path) -> None:
    path = _write_events(tmp_path / "events.jsonl", _success_sequence())
    sk = ledger_trace.extract_deterministic_five_q(
        ledger_trace.iter_run_sequences(path)["20260803T060000Z-mini-abc123"]
    )
    assert sk["run_id"] == "20260803T060000Z-mini-abc123"
    assert sk["workflow_id"] == "mini"
    assert sk["objective"] == "real run test"
    assert sk["steps"] == ["execute"]
    assert sk["outcome"] == {"ok": True, "status": "ok", "evidence_count": 1}
    assert sk["failure"] is None
    assert sk["metrics"]["event_count"] == 3
    assert sk["metrics"]["duration_s"] == 10.0


def test_extract_five_q_failure_run(tmp_path: Path) -> None:
    path = _write_events(tmp_path / "events.jsonl", _failure_sequence())
    sk = ledger_trace.extract_deterministic_five_q(
        ledger_trace.iter_run_sequences(path)["20260803T060000Z-bet-execution-fff111"]
    )
    assert sk["workflow_id"] == "bet-execution"
    assert sk["objective"] == "run bet execution"
    assert sk["steps"] == ["execute"]
    # 终态取 WorkflowClosed (status=blocked), 失败根因取 StepFailed
    assert sk["outcome"] == {"ok": False, "status": "blocked", "evidence_count": 0}
    assert sk["failure"] == {"step_name": "execute", "error": "workflow failed"}
    assert sk["metrics"]["event_count"] == 4
    assert sk["metrics"]["duration_s"] == 6.0


def test_extract_five_q_no_terminal(tmp_path: Path) -> None:
    run_id = "20260803T060000Z-mini-ghost999"
    events = [
        _event(
            "WorkflowRequested",
            run_id,
            "2026-08-03T06:00:00.000000+00:00",
            {"objective": "started but never finished", "workflow_id": "mini"},
        ),
        _event("StepStarted", run_id, "2026-08-03T06:00:01.000000+00:00", {"step_name": "execute"}),
    ]
    path = _write_events(tmp_path / "events.jsonl", events)
    sk = ledger_trace.extract_deterministic_five_q(ledger_trace.iter_run_sequences(path)[run_id])
    assert sk["objective"] == "started but never finished"
    assert sk["outcome"] is None
    assert sk["failure"] is None
    assert sk["metrics"]["event_count"] == 2


def test_load_run_skeletons_indexes_all(tmp_path: Path) -> None:
    path = _write_events(tmp_path / "events.jsonl", _success_sequence() + _failure_sequence())
    skeletons = ledger_trace.load_run_skeletons(path)
    assert set(skeletons) == {"20260803T060000Z-mini-abc123", "20260803T060000Z-bet-execution-fff111"}
    assert skeletons["20260803T060000Z-mini-abc123"]["objective"] == "real run test"
    assert skeletons["20260803T060000Z-bet-execution-fff111"]["failure"]["error"] == "workflow failed"


def test_extract_five_q_tolerates_bad_lines(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        "not-json\n" + json.dumps(_success_sequence()[0], ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    sequences = ledger_trace.iter_run_sequences(path)
    assert set(sequences) == {"20260803T060000Z-mini-abc123"}
