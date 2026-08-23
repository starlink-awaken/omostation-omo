"""Unit tests for omo.resident.promote — sediment draft → retro promotion.

M4.1 阶段2: 验证草稿聚合提升:
- _extract_topic 从文件名提取 workflow 主题
- _aggregate 按主题聚合 runs/failures
- promote dry-run 统计; 非 dry-run 落盘 retro 文档
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.resident import promote


@pytest.fixture
def _sediment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(promote, "SEDIMENT_ROOT", tmp_path / "sediment")
    monkeypatch.setattr(promote, "RETRO_ROOT", tmp_path / "retros" / "resident")
    return promote.SEDIMENT_ROOT


def _write_draft(root: Path, kind: str, name: str) -> None:
    d = root / kind
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text("# draft\n", encoding="utf-8")


def test_extract_topic_plain() -> None:
    assert promote._extract_topic("20260803T063243Z-governance-state-mutation-0fd89888.md") == (
        "governance-state-mutation"
    )


def test_extract_topic_failure_with_event_id() -> None:
    assert promote._extract_topic("20260803T063243Z-mini-0fd89888-e5ab3f01.md") == "mini"


def test_extract_topic_unknown() -> None:
    assert promote._extract_topic("not-a-draft.md") == "unclassified"


def test_aggregate_groups_topics(_sediment: Path) -> None:
    _write_draft(_sediment, "runs", "20260803T060000Z-mini-abc123.md")
    _write_draft(_sediment, "runs", "20260803T060000Z-mini-def456.md")
    _write_draft(_sediment, "failures", "20260803T060000Z-mini-abc123-e5ab3f01.md")
    _write_draft(_sediment, "runs", "20260803T060000Z-bet-execution-fff111.md")

    topics = promote._aggregate()
    assert set(topics) == {"mini", "bet-execution"}
    assert topics["mini"]["total"] == 3
    assert len(topics["mini"]["runs"]) == 2
    assert len(topics["mini"]["failures"]) == 1


def test_promote_dry_run_no_write(_sediment: Path) -> None:
    _write_draft(_sediment, "runs", "20260803T060000Z-mini-abc123.md")
    report = promote.promote(dry_run=True)
    assert report["drafts_scanned"] == 1
    assert report["topics"] == 1
    assert report["promoted_topics"] == 0
    assert not promote.RETRO_ROOT.exists()


def test_promote_writes_retro_documents(_sediment: Path, tmp_path: Path) -> None:
    _write_draft(_sediment, "runs", "20260803T060000Z-mini-abc123.md")
    _write_draft(_sediment, "runs", "20260803T060000Z-mini-def456.md")
    report = promote.promote(dry_run=False)
    assert report["promoted_topics"] == 1
    retro = promote.RETRO_ROOT / "mini.md"
    assert retro.is_file()
    text = retro.read_text(encoding="utf-8")
    assert "mini 运行复盘聚合" in text
    assert "2 成功运行" in text
    assert "20260803T060000Z-mini-abc123.md" in text


def test_promote_limit(_sediment: Path) -> None:
    _write_draft(_sediment, "runs", "20260803T060000Z-mini-abc123.md")
    _write_draft(_sediment, "runs", "20260803T060000Z-bet-execution-fff111.md")
    report = promote.promote(dry_run=False, limit=1)
    assert report["promoted_topics"] == 1


def test_promote_empty_no_crash(_sediment: Path) -> None:
    report = promote.promote(dry_run=True)
    assert report["drafts_scanned"] == 0
    assert report["topics"] == 0
