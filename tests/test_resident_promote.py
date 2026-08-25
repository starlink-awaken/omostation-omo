#!/usr/bin/env python3

"""resident-promote 单元测试 (BET-Y1Q3-T10-11).

覆盖: 主题提取 / frontmatter 解析 / 失败根因画像 / 聚合 retro 生成 /
dry-run 报告 (含全局失败画像)。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.resident import promote as promote_mod

RUN_DRAFT = """# 运行复盘沉淀(事件驱动草稿)

- event_type: WorkflowClosed
- workflow_run_id: 20260803T063243Z-governance-state-mutation-0fd89888
- trace_id: 20260803T063243Z-governance-state-mutation-0fd89888
- event_id: d727581a919742c88e87d29254d31916
- occurred_at: 2026-08-03T06:50:06.378853+00:00
- generated_at: 2026-08-23T08:50:28Z
- status: draft (事件驱动生成, 待运营 agent/人工完善为完整 retro/pattern)

## 运行上下文

- producer: agent-workflow
"""

FAIL_DRAFT = """# 失败模式沉淀(事件驱动草稿)

- event_type: StepFailed
- workflow_run_id: 20260803T063243Z-governance-state-mutation-0fd89888
- trace_id: 20260803T063243Z-governance-state-mutation-0fd89888
- event_id: e5ab3f015d24432aae6990f92e455ad3
- occurred_at: 2026-08-03T06:50:06.377506+00:00
- generated_at: 2026-08-22T22:33:15Z
- status: draft (事件驱动生成, 待运营 agent/人工完善为完整 retro/pattern)

## 失败上下文

- producer: agent-workflow
"""


def _write_draft(root: Path, kind: str, name: str, content: str) -> None:
    d = root / kind
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(content, encoding="utf-8")


@pytest.fixture
def sediment_root(tmp_path: Path) -> Path:
    """构造含 runs/failures 草稿的临时 sediment 目录."""
    root = tmp_path / "sediment"
    _write_draft(root, "runs", "20260803T063243Z-governance-state-mutation-0fd89888.md", RUN_DRAFT)
    _write_draft(root, "runs", "20260803T063617Z-mini-c9be3c75.md", RUN_DRAFT.replace(
        "0fd89888", "c9be3c75"
    ).replace("d727581a919742c88e87d29254d31916", "3f4529e75a9e494d9c2e4fd7a2e61cca"))
    _write_draft(
        root,
        "failures",
        "20260803T063243Z-governance-state-mutation-0fd89888-e5ab3f01.md",
        FAIL_DRAFT,
    )
    _write_draft(
        root,
        "failures",
        "20260803T063243Z-governance-state-mutation-0fd89888-e5ab3f02.md",
        FAIL_DRAFT.replace("e5ab3f015d24432aae6990f92e455ad3", "e5ab3f025d24432aae6990f92e455ad3"),
    )
    return root


@pytest.fixture(autouse=True)
def _isolate_roots(sediment_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """隔离模块级 SEDIMENT_ROOT / RETRO_ROOT 到临时目录."""
    monkeypatch.setattr(promote_mod, "SEDIMENT_ROOT", sediment_root)
    monkeypatch.setattr(promote_mod, "RETRO_ROOT", tmp_path / "retros" / "resident")
    yield


# ── 主题提取 ──


def test_extract_topic_strips_ts_and_run_id():
    assert promote_mod._extract_topic("20260803T063243Z-governance-state-mutation-0fd89888.md") == (
        "governance-state-mutation"
    )
    assert promote_mod._extract_topic("20260803T063617Z-mini-c9be3c75.md") == "mini"
    assert promote_mod._extract_topic("not-a-draft.md") == "unclassified"


# ── frontmatter 解析 ──


def test_parse_draft_meta_extracts_event_fields(tmp_path: Path):
    p = tmp_path / "draft.md"
    p.write_text(RUN_DRAFT, encoding="utf-8")
    meta = promote_mod._parse_draft_meta(p)
    assert meta["event_type"] == "WorkflowClosed"
    assert meta["workflow_run_id"].endswith("0fd89888")
    assert meta["trace_id"].endswith("0fd89888")
    assert len(meta["event_id"]) == 32


def test_parse_draft_meta_empty_on_missing(tmp_path: Path):
    p = tmp_path / "none.md"
    assert promote_mod._parse_draft_meta(p) == {}


# ── 失败根因画像 ──


def test_failure_breakdown_groups_by_event_type(sediment_root: Path):
    bucket = promote_mod._aggregate()["governance-state-mutation"]
    bd = promote_mod._failure_breakdown(bucket)
    assert bd["by_event_type"] == {"StepFailed": 2}
    assert bd["trace_count"] == 1
    assert len(bd["trace_ids"]) == 1


def test_failure_breakdown_empty_bucket():
    bucket = {"runs": [], "failures": [], "total": 0, "runs_meta": [], "failures_meta": []}
    bd = promote_mod._failure_breakdown(bucket)
    assert bd["by_event_type"] == {}
    assert bd["trace_count"] == 0
    assert bd["trace_ids"] == []


# ── 聚合与 retro 生成 ──


def test_aggregate_counts_runs_and_failures(sediment_root: Path):
    topics = promote_mod._aggregate()
    assert set(topics) == {"governance-state-mutation", "mini"}
    # fixture: governance 主题 = 1 run + 2 failures, mini 主题 = 1 run
    assert topics["governance-state-mutation"]["total"] == 3
    assert len(topics["governance-state-mutation"]["runs"]) == 1
    assert len(topics["governance-state-mutation"]["failures"]) == 2
    assert topics["mini"]["total"] == 1


def test_promote_writes_frontmatter_retro(sediment_root: Path, tmp_path: Path):
    report = promote_mod.promote(dry_run=False)
    retro = tmp_path / "retros" / "resident" / "governance-state-mutation.md"
    assert retro.exists()
    text = retro.read_text(encoding="utf-8")
    # frontmatter 结构化指标可检索
    assert "schema: resident-retro-candidate/v1" in text
    assert "topic: governance-state-mutation" in text
    assert "failure_rate: 0.6667" in text  # 2 failures / 3 条草稿
    assert "StepFailed: 2" in text
    assert "trace_count: 1" in text
    assert "## 失败根因画像" in text
    # 主题数 = 2 (governance-state-mutation + mini)
    assert report["promoted_topics"] == 2


def test_promote_dry_run_writes_nothing(sediment_root: Path, tmp_path: Path):
    report = promote_mod.promote(dry_run=True)
    assert report["promoted_topics"] == 0
    retro_dir = tmp_path / "retros" / "resident"
    assert not retro_dir.exists() or not list(retro_dir.iterdir())


def test_promote_report_contains_global_failure_breakdown(sediment_root: Path):
    report = promote_mod.promote(dry_run=True)
    fb = report["failure_breakdown"]
    assert fb["total_runs"] == 2
    assert fb["total_failures"] == 2
    assert fb["failure_rate"] == pytest.approx(0.5)  # 2 / (2 runs + 2 failures)
    assert fb["top_failure_event_types"] == {"StepFailed": 2}


def test_promote_limit_restricts_topics(sediment_root: Path, tmp_path: Path):
    report = promote_mod.promote(dry_run=False, limit=1)
    assert report["promoted_topics"] == 1
    retro_dir = tmp_path / "retros" / "resident"
    written = {p.stem for p in retro_dir.glob("*.md")}
    assert written == {"governance-state-mutation"}  # 草稿最多的主题优先
