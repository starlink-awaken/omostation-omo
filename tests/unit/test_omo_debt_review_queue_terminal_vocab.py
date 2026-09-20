"""词汇契约单测 — review queue 必须与 metrics 共用终结态词表.

背景 (2026-09-20): `omo_debt_review_queue` 只过滤 `closed`, 于是 17 个用
`resolved` 关闭的债务被当作开放项 —— 实测 review-queue 的 16 个条目里
15 个是终结态假阳性。`omo_debt_metrics` 早在 2026-08-22 就以
`TERMINAL_STATES` 修过同类病 (debt_health 归零假象), review queue 未复用
该契约 → 复发。

本文件锁死两件事:
1. 单一真源 —— review queue 直接引用 metrics 的 TERMINAL_STATES, 不自建词表。
2. 终结态 (closed / resolved) 不得进入任何调度桶。
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from omo.omo_debt_metrics import TERMINAL_STATES
from omo.omo_debt_review_queue import build_review_queue

NOW = "2026-09-20T06:00:00Z"


def _item(
    id_: str,
    state: str,
    *,
    next_review_at: str | None = None,
    last_reviewed_at: str | None = None,
    severity: str = "high",
    gate_level: str = "none",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id_,
        title=f"title {id_}",
        owner="some-team",
        severity=severity,
        dimension="governance",
        subdimension="maturity",
        lifecycle_state=state,
        gate_level=gate_level,
        next_review_at=next_review_at,
        last_reviewed_at=last_reviewed_at,
        affected_roots=["bin/"],
        evidence_refs=[],
        mitigation_refs=[],
        weight=1.0,
    )


def _all_scheduled_ids(queue: dict) -> list[str]:
    return [
        entry["id"]
        for bucket in ("due_now", "upcoming", "escalation_candidates", "unscheduled")
        for entry in queue[bucket]
    ]


def test_terminal_vocab_is_the_shared_contract() -> None:
    """单源真源: 契约同时认 closed 与 resolved, 不得退化成只认 closed。"""
    assert "closed" in TERMINAL_STATES
    assert "resolved" in TERMINAL_STATES


def test_resolved_and_closed_never_scheduled(tmp_path) -> None:
    """回归: resolved 曾被当开放项漏进 unscheduled (15/16 假阳性)。"""
    items = (
        _item("R-1", "resolved"),
        _item("C-1", "closed"),
        _item("OPEN-1", "identified"),
    )
    queue = build_review_queue(items, now=NOW, repo_root=tmp_path)
    assert _all_scheduled_ids(queue) == ["OPEN-1"]


def test_terminal_item_with_review_date_still_excluded(tmp_path) -> None:
    """终结态即便带 next_review_at 也不得进 due_now/upcoming/escalation。"""
    items = (
        _item("R-2", "resolved", next_review_at="2026-09-01T00:00:00+00:00"),
        _item("C-2", "closed", next_review_at="2026-09-01T00:00:00+00:00"),
    )
    queue = build_review_queue(items, now=NOW, repo_root=tmp_path)
    assert _all_scheduled_ids(queue) == []


def test_open_item_without_next_review_lands_unscheduled(tmp_path) -> None:
    queue = build_review_queue((_item("OPEN-2", "identified"),), now=NOW, repo_root=tmp_path)
    assert [e["id"] for e in queue["unscheduled"]] == ["OPEN-2"]


def test_open_item_overdue_lands_due_now(tmp_path) -> None:
    overdue = datetime(2026, 9, 1, tzinfo=timezone.utc).isoformat()
    queue = build_review_queue(
        (_item("OPEN-3", "identified", next_review_at=overdue),), now=NOW, repo_root=tmp_path
    )
    assert [e["id"] for e in queue["due_now"]] == ["OPEN-3"]


def test_summary_counts_exclude_terminal(tmp_path) -> None:
    items = (
        _item("R-3", "resolved"),
        _item("C-3", "closed"),
        _item("OPEN-4", "identified", severity="critical"),
    )
    summary = build_review_queue(items, now=NOW, repo_root=tmp_path)["summary"]
    assert summary["unscheduled_count"] == 1
    assert summary["by_severity"] == {"critical": 1}
