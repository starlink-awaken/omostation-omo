"""回归: debt registry 缺 ref 键不得崩溃.

2026-09-19 实证: `load_debt_ledger` 用 `registry["dispatch_ref"]` 直取, 当
`.omo/_truth/registry/debt.yaml` 缺少任一 `*_ref` 键时抛
`KeyError: 'dispatch_ref'` —— 真实调用路径崩溃。

背景: 那些 ref 的**目标文件**是派生产物 (dashboard/reviews/review-queue/
action-packet/owner-routing/dispatch/campaign/reporting), 由
`omo-debt refresh|dispatch|campaign|report` 按需生成, 未跑时不存在。
**键与文件是两回事** —— 键缺失不该让整条读取路径崩溃。
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from omo.omo_debt_registry import load_debt_ledger

ALL_REFS = (
    "dashboard_ref",
    "review_pack_ref",
    "review_queue_ref",
    "action_packet_ref",
    "owner_routing_ref",
    "dispatch_ref",
    "campaign_ref",
    "reporting_ref",
)

BASE = {
    "status": "active",
    "lifecycle": "ssot",
    "owner": "governance-team",
    "items_dir": ".omo/debt/items",
}


def _registry(tmp_path: Path, seed_items=None, **extra) -> Path:
    omo_dir = tmp_path / ".omo"
    (omo_dir / "_truth" / "registry").mkdir(parents=True)
    (omo_dir / "debt" / "items").mkdir(parents=True)
    doc = {**BASE, **extra}
    if seed_items is not None:
        doc["seed_items"] = seed_items
    (omo_dir / "_truth" / "registry" / "debt.yaml").write_text(
        yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8"
    )
    return omo_dir


def test_all_refs_present(tmp_path):
    omo_dir = _registry(tmp_path, **{k: f".omo/debt/{k}/current.yaml" for k in ALL_REFS})
    ledger = load_debt_ledger(omo_dir)
    assert ledger.dispatch_ref.endswith("current.yaml")


@pytest.mark.parametrize("missing", ALL_REFS)
def test_single_missing_ref_does_not_crash(tmp_path, missing):
    """核心: 缺任一个 ref 键 → 空串, 不抛 KeyError."""
    refs = {k: f".omo/debt/{k}/current.yaml" for k in ALL_REFS if k != missing}
    omo_dir = _registry(tmp_path, **refs)
    ledger = load_debt_ledger(omo_dir)  # 不得抛 KeyError
    assert getattr(ledger, missing) == ""


def test_all_refs_missing_does_not_crash(tmp_path):
    """全部 ref 缺失 (如声明被移除) → 仍可读取, 各为 ''."""
    omo_dir = _registry(tmp_path)
    ledger = load_debt_ledger(omo_dir)
    for name in ALL_REFS:
        assert getattr(ledger, name) == ""


def test_items_still_loaded_when_refs_missing(tmp_path):
    """ref 缺失不应影响 items 的加载 (items 由 seed_items 枚举)."""
    p = tmp_path / ".omo" / "debt" / "items" / "D-1.yaml"
    omo_dir = _registry(tmp_path, seed_items=[".omo/debt/items/D-1.yaml"])
    p.write_text(
        yaml.safe_dump(
            {
                "id": "D-1",
                "title": "T",
                "dimension": "architecture",
                "subdimension": "x",
                "domain": "workspace",
                "scope": "workspace",
                "severity": "medium",
                "lifecycle_state": "identified",
                "owner": "governance-team",
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    ledger = load_debt_ledger(omo_dir)
    assert len(ledger.items) >= 1
