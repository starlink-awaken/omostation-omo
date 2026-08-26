"""Unit tests for omo.resident.promote 五问骨架填充 (BET-Y1Q3-T10-17).

验证:
- promote(fill_five_q=True) 产出 retro 含「确定性五问骨架」段 (计划/实际/结果/失败/指标)
- 语义项 (关键发现 / 交接建议) 保持空 checkbox
- promote(fill_five_q=False) 不产出骨架段, 待完善段保留原 5 checkbox
- --json 报告含 five_q_filled
- 事件流缺失时优雅降级 (不阻断 promote)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident import promote


@pytest.fixture
def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(promote, "SEDIMENT_ROOT", tmp_path / "sediment")
    monkeypatch.setattr(promote, "RETRO_ROOT", tmp_path / "retros" / "resident")
    events = tmp_path / "events.jsonl"
    monkeypatch.setattr(promote, "EVENTS_PATH", events)
    return tmp_path


def _write_draft_with_meta(root: Path, kind: str, name: str, run_id: str) -> None:
    d = root / kind
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(
        "# 运行复盘沉淀(事件驱动草稿)\n\n"
        "- event_type: WorkflowSucceeded\n"
        f"- workflow_run_id: {run_id}\n"
        f"- trace_id: {run_id}\n"
        "- event_id: ev-1\n"
        "- status: draft\n\n"
        "## 运行上下文\n\n"
        "- producer: agent-workflow\n",
        encoding="utf-8",
    )


def _write_events(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events), encoding="utf-8")


def _success_events(run_id: str) -> list[dict]:
    return [
        {
            "event_type": "WorkflowRequested",
            "occurred_at": "2026-08-03T06:00:00.000000+00:00",
            "payload": {"objective": "real run test", "workflow_id": "mini"},
            "workflow_run_id": run_id,
        },
        {
            "event_type": "StepStarted",
            "occurred_at": "2026-08-03T06:00:05.000000+00:00",
            "payload": {"step_name": "execute"},
            "workflow_run_id": run_id,
        },
        {
            "event_type": "WorkflowSucceeded",
            "occurred_at": "2026-08-03T06:00:10.000000+00:00",
            "payload": {"ok": True, "status": "ok", "evidence_count": 1},
            "workflow_run_id": run_id,
        },
    ]


def test_promote_fill_five_q_renders_skeleton(_env: Path) -> None:
    run_id = "20260803T060000Z-mini-abc123"
    _write_draft_with_meta(promote.SEDIMENT_ROOT, "runs", f"{run_id}.md", run_id)
    _write_events(promote.EVENTS_PATH, _success_events(run_id))

    report = promote.promote(dry_run=False, fill_five_q=True)
    assert report["promoted_topics"] == 1
    assert report["five_q_filled"] == 1

    retro = promote.RETRO_ROOT / "mini.md"
    text = retro.read_text(encoding="utf-8")
    assert "## 确定性五问骨架 (ledger 追溯, 自动填充)" in text
    assert "计划 (objective): real run test" in text
    assert "实际步骤: execute" in text
    assert "ok=True, status=ok, evidence_count=1" in text
    # 语义项保持空 checkbox, 确定性项不再出现在待完善段
    assert "- [ ] 关键发现" in text
    assert "- [ ] 交接建议" in text
    assert "- [ ] 计划 vs 实际" not in text
    assert "- [ ] 结果与证据" not in text


def test_promote_fill_five_q_disabled_keeps_legacy(_env: Path) -> None:
    run_id = "20260803T060000Z-mini-abc123"
    _write_draft_with_meta(promote.SEDIMENT_ROOT, "runs", f"{run_id}.md", run_id)
    _write_events(promote.EVENTS_PATH, _success_events(run_id))

    report = promote.promote(dry_run=False, fill_five_q=False)
    assert report["five_q_filled"] == 0
    text = (promote.RETRO_ROOT / "mini.md").read_text(encoding="utf-8")
    assert "## 确定性五问骨架" not in text
    assert "- [ ] 计划 vs 实际" in text
    assert "- [ ] 结果与证据" in text


def test_promote_missing_events_degrades_gracefully(_env: Path) -> None:
    run_id = "20260803T060000Z-mini-abc123"
    _write_draft_with_meta(promote.SEDIMENT_ROOT, "runs", f"{run_id}.md", run_id)
    # 不写 events.jsonl → EVENTS_PATH 不存在
    report = promote.promote(dry_run=False, fill_five_q=True)
    assert report["promoted_topics"] == 1
    assert report["five_q_filled"] == 0
    text = (promote.RETRO_ROOT / "mini.md").read_text(encoding="utf-8")
    assert "## 确定性五问骨架" not in text
    assert "- [ ] 关键发现" in text


def test_promote_cli_json_reports_five_q_filled(_env: Path, capsys: pytest.CaptureFixture) -> None:
    run_id = "20260803T060000Z-mini-abc123"
    _write_draft_with_meta(promote.SEDIMENT_ROOT, "runs", f"{run_id}.md", run_id)
    _write_events(promote.EVENTS_PATH, _success_events(run_id))

    rc = promote.main(["--dry-run", "--json", "--events-path", str(promote.EVENTS_PATH)])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["five_q_filled"] == 1


def test_promote_failure_draft_filled_by_ledger(_env: Path) -> None:
    run_id = "20260803T060000Z-bet-execution-fff111"
    _write_draft_with_meta(promote.SEDIMENT_ROOT, "failures", f"{run_id}-e5ab3f01.md", run_id)
    events = [
        {
            "event_type": "WorkflowRequested",
            "occurred_at": "2026-08-03T06:00:00.000000+00:00",
            "payload": {"objective": "run bet", "workflow_id": "bet-execution"},
            "workflow_run_id": run_id,
        },
        {
            "event_type": "StepFailed",
            "occurred_at": "2026-08-03T06:00:04.000000+00:00",
            "payload": {"step_name": "execute", "error": "workflow failed"},
            "workflow_run_id": run_id,
        },
        {
            "event_type": "WorkflowClosed",
            "occurred_at": "2026-08-03T06:00:06.000000+00:00",
            "payload": {"ok": False, "status": "blocked", "evidence_count": 0},
            "workflow_run_id": run_id,
        },
    ]
    _write_events(promote.EVENTS_PATH, events)

    report = promote.promote(dry_run=False, fill_five_q=True)
    assert report["five_q_filled"] == 1
    text = (promote.RETRO_ROOT / "bet-execution.md").read_text(encoding="utf-8")
    assert "失败根因: step=execute, error=workflow failed" in text
    assert "ok=False, status=blocked, evidence_count=0" in text
