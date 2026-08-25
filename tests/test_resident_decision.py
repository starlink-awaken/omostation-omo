#!/usr/bin/env python3

"""resident-decision 单元测试 (BET-Y1Q3-T10-13).

覆盖: 提案 JSON + md 收件箱双写 / frontmatter 溯源 / 幂等不覆盖 /
CLI 审计 list/status/show 统计 / 非触发事件忽略 / 空目录容错。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident import decision as decision_mod

TRACE_ID = "20260825T073057Z-project-code-change-77870817"
RUN_ID = "20260825T073057Z-project-code-change-77870817"
EVENT_ID = "d727581a919742c88e87d29254d31916"

SAMPLE = {
    "schema": "resident-decision/v1",
    "trigger_event": {
        "event_type": "StepFailed",
        "trace_id": TRACE_ID,
        "workflow_run_id": RUN_ID,
        "event_id": EVENT_ID,
    },
    "proposal_count": 2,
    "proposals": [
        {"action": "escalate", "level": "high", "proposal": "复现失败链路", "severity": "critical", "type": "failure"},
        {"action": "patch", "level": "medium", "proposal": "补超时重试", "severity": "warn", "type": "reliability"},
    ],
}


@pytest.fixture(autouse=True)
def _isolate_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """隔离模块级 WORKSPACE / PROPOSAL_DIR / INBOX_DIR 到临时目录."""
    monkeypatch.setattr(decision_mod, "WORKSPACE", tmp_path)
    monkeypatch.setattr(decision_mod, "PROPOSAL_DIR", tmp_path / "evolution-proposals")
    monkeypatch.setattr(decision_mod, "INBOX_DIR", tmp_path / "decision-proposals")
    yield


def _newest_proposal_json() -> Path:
    files = sorted(decision_mod.PROPOSAL_DIR.glob("decision-*.json"))
    assert files, "expected at least one proposal json"
    return files[0]


def _newest_inbox_md() -> Path:
    files = sorted(decision_mod.INBOX_DIR.glob("decision-*.md"))
    assert files, "expected at least one inbox md"
    return files[0]


def _seed_proposals() -> None:
    """写入两条不同 event_type 的提案."""
    decision_mod._write_proposal(SAMPLE, TRACE_ID)
    other = {
        **SAMPLE,
        "trigger_event": {**SAMPLE["trigger_event"], "event_type": "WorkflowFailed"},
    }
    decision_mod._write_proposal(other, "wf-fail-1")


# ── 双写: JSON + md 收件箱 ──


def test_write_proposal_dual_writes_json_and_md():
    path = decision_mod._write_proposal(SAMPLE, TRACE_ID)
    assert path is not None and path.endswith(".json")
    json_file = decision_mod.PROPOSAL_DIR / Path(path).name
    assert json_file.exists()
    md_files = list(decision_mod.INBOX_DIR.glob("decision-*.md"))
    assert len(md_files) == 1
    # 同 ts+slug 一一对应
    assert md_files[0].stem == json_file.stem


def test_inbox_frontmatter_carries_traceability():
    decision_mod._write_proposal(SAMPLE, TRACE_ID)
    md = _newest_inbox_md().read_text(encoding="utf-8")
    assert "schema: resident-decision/v1" in md
    assert "status: draft" in md
    assert "trigger_event_type: StepFailed" in md
    assert f"trace_id: {TRACE_ID}" in md
    assert f"workflow_run_id: {RUN_ID}" in md
    assert f"event_id: {EVENT_ID}" in md
    assert "proposal_count: 2" in md
    # 正文提案清单
    assert "## 触发事件" in md
    assert "## 提案内容 (2 条)" in md
    assert "[high] escalate" in md
    assert "复现失败链路" in md


def test_inbox_md_idempotent_does_not_overwrite():
    decision_mod._write_proposal_md(SAMPLE, ts="20260825-120000", slug="abc")
    p = decision_mod.INBOX_DIR / "decision-20260825-120000-abc.md"
    assert p.exists()
    p.write_text("manual-edit", encoding="utf-8")
    decision_mod._write_proposal_md(SAMPLE, ts="20260825-120000", slug="abc")
    assert p.read_text(encoding="utf-8") == "manual-edit"


def test_render_proposal_md_empty_proposals():
    md = decision_mod._render_proposal_md({**SAMPLE, "proposal_count": 0, "proposals": []})
    assert "## 提案内容 (0 条)" in md
    assert "(无提案内容 — 仅触发事件溯源)" in md


# ── CLI 审计: list / status / show ──


def test_list_proposals_newest_first_and_spread():
    _seed_proposals()
    text = decision_mod._list_proposals(limit=10)
    assert "decision proposals: 2" in text
    assert "StepFailed" in text
    assert "WorkflowFailed" in text
    assert "event_type 分布" in text


def test_status_snapshot_counts():
    _seed_proposals()
    text = decision_mod._status_text()
    assert "decision proposals: 2" in text
    assert "latest proposal:" in text
    assert "StepFailed" in text and "WorkflowFailed" in text
    assert "high" in text and "medium" in text  # level 分布
    assert "failure" in text and "reliability" in text  # type 分布


def test_show_renders_single_proposal():
    decision_mod._write_proposal(SAMPLE, TRACE_ID)
    data = decision_mod._load_proposal(_newest_proposal_json())
    assert data is not None
    md = decision_mod._render_proposal_md(data)
    assert "## 触发事件" in md
    assert "StepFailed" in md
    assert "复现失败链路" in md


def test_show_command_rejects_path_traversal_and_missing():
    assert decision_mod._show_command("../evil.json") == 2
    assert decision_mod._show_command("decision-nope.json") == 2


def test_main_status_subcommand_returns_zero(capsys):
    assert decision_mod.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "decision proposals:" in out


# ── 空目录容错 ──


def test_list_status_empty_dir_tolerant():
    text = decision_mod._list_proposals(limit=5)
    assert "decision proposals: 0" in text
    status = decision_mod._status_text()
    assert "decision proposals: 0" in status
    assert "latest proposal: n/a" in status


# ── 事件消费 ──


def test_decide_ignores_non_trigger_event(monkeypatch):
    monkeypatch.setattr(decision_mod, "_scan_proposals", lambda: [])
    assert decision_mod._decide({"event_type": "WorkflowSucceeded", "trace_id": "x"}) is None


def test_decide_writes_for_trigger_event(monkeypatch):
    monkeypatch.setattr(
        decision_mod,
        "_scan_proposals",
        lambda: [{"action": "patch", "level": "low", "proposal": "p", "severity": "info", "type": "test"}],
    )
    path = decision_mod._decide({"event_type": "StepFailed", "trace_id": TRACE_ID})
    assert path is not None
    data = json.loads((decision_mod.PROPOSAL_DIR / Path(path).name).read_text(encoding="utf-8"))
    assert data["trigger_event"]["event_type"] == "StepFailed"
    assert data["proposal_count"] == 1
    assert data["proposals"][0]["action"] == "patch"


# ── slug 归一化 ──


def test_safe_slug_normalizes_and_keeps_tail():
    assert decision_mod._safe_slug("a:b/c") == "a-b-c"
    assert decision_mod._safe_slug("") == "event"
    assert decision_mod._safe_slug("x" * 80) == "x" * 40
    # tail 保留 run 标识
    assert decision_mod._safe_slug(TRACE_ID).endswith("77870817")
