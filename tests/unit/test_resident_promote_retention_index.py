"""Unit tests for omo.resident.promote 草稿 retention 归档 + retro 索引 (BET-Y1Q3-T10-18).

验证:
- promote(retain_days=N) 落盘时把 mtime 超保留窗口的已聚合草稿移入 gitignored 归档区
- --dry-run 报告含 archivable_count (可归档数) 且不落盘/不归档
- promote 落盘后生成 retros/resident/index.md (主题/草稿数/失败率/五问 filled)
- retain_days=0 时不归档
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from omo.resident import promote


@pytest.fixture
def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(promote, "SEDIMENT_ROOT", tmp_path / "sediment")
    monkeypatch.setattr(promote, "RETRO_ROOT", tmp_path / "retros" / "resident")
    monkeypatch.setattr(promote, "ARCHIVE_ROOT", tmp_path / "sediment-archive")
    events = tmp_path / "events.jsonl"
    monkeypatch.setattr(promote, "EVENTS_PATH", events)
    return tmp_path


def _write_draft(sediment_root: Path, kind: str, name: str, mtime_days_ago: float) -> Path:
    d = sediment_root / kind
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_text(
        "# 运行复盘沉淀(事件驱动草稿)\n\n- event_type: WorkflowSucceeded\n- workflow_run_id: run-1\n- status: draft\n",
        encoding="utf-8",
    )
    old = time.time() - mtime_days_ago * 86400
    os.utime(p, (old, old))
    return p


def _setup_drafts(root: Path, kind: str, fresh_name: str, stale_name: str) -> None:
    """root 为 tmp_path; 草稿写入 <root>/sediment/<kind>/ (与 SEDIMENT_ROOT 对齐)."""
    sediment_root = root / "sediment"
    _write_draft(sediment_root, kind, fresh_name, mtime_days_ago=1)
    _write_draft(sediment_root, kind, stale_name, mtime_days_ago=40)


def test_dry_run_reports_archivable_count_without_archiving(_env: Path) -> None:
    """dry-run 报告含 archivable_count, 但不移动草稿、不写 index."""
    _setup_drafts(_env, "runs", "20260810T000000Z-mini-fresh.md", "20260701T000000Z-mini-stale.md")
    _setup_drafts(_env, "failures", "20260810T000000Z-mini-fresh-f1.md", "20260701T000000Z-mini-stale-f1.md")

    report = promote.promote(dry_run=True, retain_days=30)

    assert report["drafts_scanned"] == 4
    assert report["archivable_count"] == 2  # 只有 40 天前的 stale 草稿超窗
    assert report["archived_count"] == 0
    assert report["index_written"] is None
    # 草稿未被移动
    assert (_env / "sediment" / "runs" / "20260701T000000Z-mini-stale.md").exists()
    assert not (_env / "sediment-archive").exists()


def test_promote_archives_stale_drafts(_env: Path) -> None:
    """落盘时把超窗草稿移入 sediment-archive/<kind>/, 新草稿保留."""
    _setup_drafts(_env, "runs", "20260810T000000Z-mini-fresh.md", "20260701T000000Z-mini-stale.md")
    _setup_drafts(_env, "failures", "20260810T000000Z-mini-fresh-f1.md", "20260701T000000Z-mini-stale-f1.md")

    report = promote.promote(dry_run=False, retain_days=30)

    assert report["archived_count"] == 2
    # stale 已移走
    assert not (_env / "sediment" / "runs" / "20260701T000000Z-mini-stale.md").exists()
    assert not (_env / "sediment" / "failures" / "20260701T000000Z-mini-stale-f1.md").exists()
    # fresh 保留
    assert (_env / "sediment" / "runs" / "20260810T000000Z-mini-fresh.md").exists()
    # 归档区按 kind 分桶
    assert (_env / "sediment-archive" / "runs" / "20260701T000000Z-mini-stale.md").exists()
    assert (_env / "sediment-archive" / "failures" / "20260701T000000Z-mini-stale-f1.md").exists()


def test_retain_days_zero_disables_archive(_env: Path) -> None:
    """retain_days=0 时不归档任何草稿."""
    _setup_drafts(_env, "runs", "20260810T000000Z-mini-fresh.md", "20260701T000000Z-mini-stale.md")

    report = promote.promote(dry_run=False, retain_days=0)

    assert report["archived_count"] == 0
    assert (_env / "sediment" / "runs" / "20260701T000000Z-mini-stale.md").exists()


def test_promote_writes_index(_env: Path) -> None:
    """落盘后生成 retros/resident/index.md, 含主题/草稿数/失败率/五问 filled 汇总."""
    _setup_drafts(_env, "runs", "20260810T000000Z-mini-fresh.md", "20260701T000000Z-mini-stale.md")
    _setup_drafts(_env, "failures", "20260810T000000Z-governance-f1.md", "20260701T000000Z-governance-f2.md")

    report = promote.promote(dry_run=False, retain_days=30)

    assert report["index_written"] is not None
    index = _env / "retros" / "resident" / "index.md"
    assert index.exists()
    content = index.read_text(encoding="utf-8")
    # 主题在表格中 (mini-* + governance-* 前缀主题)
    assert "| mini" in content
    assert "| governance" in content
    assert "草稿总数" in content
    assert "five_q_filled" in content


def test_promote_generates_five_q_skeleton_in_retro(_env: Path) -> None:
    """落盘 retro 含确定性五问骨架段 (关联到 events.jsonl 中可定位的 run)."""
    _write_draft(_env / "sediment", "runs", "20260810T000000Z-mini-run1.md", mtime_days_ago=1)
    events = _env / "events.jsonl"
    events.parent.mkdir(parents=True, exist_ok=True)
    events.write_text(
        "".join(
            json.dumps(e, ensure_ascii=False) + "\n"
            for e in [
                {
                    "event_type": "WorkflowRequested",
                    "occurred_at": "2026-08-03T06:00:00.000000+00:00",
                    "payload": {"objective": "real run test", "workflow_id": "mini"},
                    "workflow_run_id": "run-1",
                },
                {
                    "event_type": "WorkflowClosed",
                    "occurred_at": "2026-08-03T06:05:00.000000+00:00",
                    "payload": {"ok": True, "status": "succeeded", "evidence_count": 2},
                    "workflow_run_id": "run-1",
                },
            ]
        ),
        encoding="utf-8",
    )

    report = promote.promote(dry_run=False, retain_days=0)

    assert report["five_q_filled"] == 1
    retro = _env / "retros" / "resident" / "mini-run1.md"
    assert retro.exists()
    content = retro.read_text(encoding="utf-8")
    assert "确定性五问骨架" in content
    assert "计划 (objective): real run test" in content
    assert "结果与证据: ok=True" in content
