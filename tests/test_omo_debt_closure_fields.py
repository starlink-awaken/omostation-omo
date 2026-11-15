"""闭局证据三键必须被 kernel 的 `DebtItem` 装载 —— 不能只在文件层。

契约来源：2026-11-15 实测 —— `DebtItem` 的 22 个字段里没有
`closed_at` / `close_reason` / `resolution_evidence`，而
  - 主仓 `bin/gac/check-debt-closure-discipline.py` 要求终态项**必须有** `closed_at`；
  - `omo_audit_freshness.py` 与 `omo_audit_checks.py` 要求
    `resolution_evidence`（或等价的 `closed_evidence`）>= 20 字符。

三处判据都读原始 YAML，所以门禁是绿的；但走 `load_debt_ledger` 的消费者
拿不到闭局证据 —— 实测 `load_debt_ledger` 读一份 resolved 条目时
`kernel 丢弃的键: ['close_reason', 'closed_at', 'resolution_evidence']`。
本文件把「装载且不损坏」钉住。
"""

from __future__ import annotations

from pathlib import Path

from omo.omo_debt_registry import load_debt_ledger

_BASE_ITEM = """\
---
id: {id}
title: test debt
dimension: governance
subdimension: boundary
domain: workspace
scope: cross_project
severity: medium
weight: 0.3
entropy_class: pointer
lifecycle_state: {state}
owner: platform-governance
affected_roots:
  - projects/omo
evidence_refs: []
mitigation_refs: []
opened_at: 2026-06-01T00:00:00Z
last_reviewed_at: null
next_review_at: null
gate_level: none
history: []
{extra}
"""

_REGISTRY = """\
---
dashboard_ref: .omo/debt/dashboard/current.yaml
review_pack_ref: .omo/debt/reviews/current.md
review_queue_ref: .omo/debt/review-queue/current.yaml
action_packet_ref: .omo/debt/action-packet/current.yaml
owner_routing_ref: .omo/debt/owner-routing/current.yaml
dispatch_ref: .omo/debt/dispatch/current.yaml
campaign_ref: .omo/debt/campaign/current.yaml
reporting_ref: .omo/debt/reporting/current.yaml
seed_items:
  - .omo/debt/items/DEBT-CLOSED.yaml
  - .omo/debt/items/DEBT-OPEN.yaml
  - .omo/debt/items/DEBT-LIST-EVIDENCE.yaml
  - .omo/debt/items/DEBT-LEGACY-EVIDENCE.yaml
"""


def _build(tmp_path: Path) -> Path:
    omo_dir = tmp_path / ".omo"
    (omo_dir / "_truth" / "registry").mkdir(parents=True, exist_ok=True)
    (omo_dir / "debt" / "items").mkdir(parents=True, exist_ok=True)
    (omo_dir / "_truth" / "registry" / "debt.yaml").write_text(_REGISTRY, encoding="utf-8")
    items = {
        "DEBT-CLOSED.yaml": (
            "resolved",
            "closed_at: '2026-11-15'\n"
            "close_reason: 试降 draft 被两道在案硬门拒下\n"
            "resolution_evidence: " + "x" * 30 + "\n",
        ),
        "DEBT-OPEN.yaml": ("identified", ""),
        "DEBT-LIST-EVIDENCE.yaml": (
            "resolved",
            "closed_at: '2026-11-15'\nresolution_evidence:\n  - 第一行证据\n  - 第二行证据\n",
        ),
        "DEBT-LEGACY-EVIDENCE.yaml": (
            "resolved",
            "closed_at: '2026-11-15'\n" + "closed_evidence: " + "y" * 25 + "\n",
        ),
    }
    for name, (state, extra) in items.items():
        (omo_dir / "debt" / "items" / name).write_text(
            _BASE_ITEM.format(id=name[: -len(".yaml")], state=state, extra=extra),
            encoding="utf-8",
        )
    return omo_dir


def _by_id(ledger) -> dict:
    return {item.id: item for item in ledger.items}


def test_closed_item_carries_closure_fields(tmp_path: Path) -> None:
    items = _by_id(load_debt_ledger(_build(tmp_path)))
    closed = items["DEBT-CLOSED"]
    assert closed.closed_at == "2026-11-15"
    assert closed.close_reason == "试降 draft 被两道在案硬门拒下"
    assert len(closed.resolution_evidence) == 30


def test_open_item_has_empty_closure_fields(tmp_path: Path) -> None:
    items = _by_id(load_debt_ledger(_build(tmp_path)))
    open_item = items["DEBT-OPEN"]
    assert open_item.closed_at == ""
    assert open_item.close_reason == ""
    assert open_item.resolution_evidence == ""


def test_list_form_evidence_is_joined_not_repr(tmp_path: Path) -> None:
    """裸 str() 会把列表写成 Python repr —— 那是静默损坏。"""
    items = _by_id(load_debt_ledger(_build(tmp_path)))
    evidence = items["DEBT-LIST-EVIDENCE"].resolution_evidence
    assert evidence == "第一行证据\n第二行证据"
    assert "[" not in evidence


def test_legacy_closed_evidence_is_accepted(tmp_path: Path) -> None:
    """`closed_evidence` 与 `resolution_evidence` 同权（既有等价口径，见
    tests/test_debt_evidence_field_equivalence.py）。"""
    items = _by_id(load_debt_ledger(_build(tmp_path)))
    assert len(items["DEBT-LEGACY-EVIDENCE"].resolution_evidence) == 25
