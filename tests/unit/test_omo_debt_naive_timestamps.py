"""债务条目时间戳的时区契约 — naive (日期-only) 一律按 UTC 解释。

缺陷背景 (2026-11-09 实证): 条目里的 `last_reviewed_at` / `next_review_at`
绝大多数是日期-only (如 `2026-11-09`), 而文件 mtime 与 `now` 是 aware UTC。
`datetime.fromisoformat` 对前者给 naive、对后者给 aware, 直接比较/相减会抛
`TypeError: can't compare/subtract offset-naive and offset-aware datetimes`。

触发路径: 把 21 个「存在但未登记」的条目补登进 `debt.yaml::seed_items` 后,
任何一条「非终态 + refs 全部存在 + 日期-only」的条目都会让
`collect_stale_evidence_item_ids` / `build_review_queue` 崩溃, 进而**整份
dashboard 生成失败** —— 而此前这些条目因未登记而对 ledger 不可见, 于是
这个崩溃一直被掩盖。

本文件不依赖真实 .omo 状态 (用 tmp_path 自造 refs), 故不被 requires_real_omo 跳过。
"""

from __future__ import annotations

from pathlib import Path

from omo.omo_debt_metrics import compute_debt_metrics
from omo.omo_debt_registry import DebtItem
from omo.omo_debt_review_queue import build_review_queue


def _item(**overrides) -> DebtItem:
    fields = dict(
        id="NAIVE_TS",
        title="Naive timestamps",
        dimension="architecture",
        subdimension="boundaries",
        domain="workspace",
        scope="cross_project",
        severity="medium",
        weight=0.5,
        entropy_class="pointer",
        lifecycle_state="identified",
        owner="platform-governance",
        affected_roots=("projects/demo",),
        evidence_refs=("evidence.md",),
        mitigation_refs=("mitigation.md",),
        opened_at="2026-06-01",
        last_reviewed_at="2026-06-01",
        next_review_at="2026-06-20",
        gate_level="none",
        history=(),
    )
    fields.update(overrides)
    return DebtItem(**fields)


def _fixture(tmp_path: Path) -> Path:
    (tmp_path / "evidence.md").write_text("evidence\n", encoding="utf-8")
    (tmp_path / "mitigation.md").write_text("mitigation\n", encoding="utf-8")
    return tmp_path


def test_compute_debt_metrics_accepts_date_only_timestamps(tmp_path: Path) -> None:
    repo_root = _fixture(tmp_path)

    metrics = compute_debt_metrics(
        (_item(),),
        now="2026-06-10T00:00:00Z",  # aware
        repo_root=repo_root,
    )

    assert metrics.time_entropy == 0.0  # next_review 2026-06-20 尚未到期
    assert metrics.pointer_entropy == 1.0  # evidence mtime 晚于 last_reviewed


def test_build_review_queue_accepts_date_only_timestamps(tmp_path: Path) -> None:
    repo_root = _fixture(tmp_path)

    queue = build_review_queue(
        (_item(next_review_at="2026-06-15"),),
        now="2026-06-10T00:00:00Z",
        repo_root=repo_root,
    )

    assert [e["id"] for e in queue["due_now"]] == []
    assert [e["id"] for e in queue["upcoming"]] == ["NAIVE_TS"]


def test_date_only_overdue_is_reported(tmp_path: Path) -> None:
    """next_review_at 已过期时必须进 due_now, 而不是崩掉。"""
    repo_root = _fixture(tmp_path)

    queue = build_review_queue(
        (_item(next_review_at="2026-06-01"),),
        now="2026-06-10T00:00:00Z",
        repo_root=repo_root,
    )

    due = [e["id"] for e in queue["due_now"]]
    assert due == ["NAIVE_TS"]
