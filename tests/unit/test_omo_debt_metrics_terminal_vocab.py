"""词汇契约单测 — resolved 与 closed 同为终结态 (2026-08-22 health=0 假象修复)."""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from omo.omo_debt_metrics import TERMINAL_STATES, collect_stale_evidence_item_ids


def _item(id_, state, *, evidence=True):
    ev = Path(__file__) if evidence else None
    return SimpleNamespace(
        id=id_,
        lifecycle_state=state,
        dimension="governance",
        evidence_refs=[str(ev)] if ev else [],
        mitigation_refs=[],
        last_reviewed_at=None,
        weight=1,
        affected_roots=["bin/"],
    )


def test_terminal_states_contains_both_vocab():
    assert "closed" in TERMINAL_STATES
    assert "resolved" in TERMINAL_STATES


def test_resolved_not_counted_open():
    """回归: D-1~D-7 用 resolved 关闭后曾被误计开放 -> health 归零."""
    items = (_item("D-1", "resolved"), _item("OPEN-1", "registered"))
    open_items = [i for i in items if i.lifecycle_state not in TERMINAL_STATES]
    assert [i.id for i in open_items] == ["OPEN-1"]


def test_stale_collector_skips_resolved(tmp_path):
    resolved_no_ev = _item("D-X", "resolved", evidence=False)
    open_no_ev = _item("OPEN-2", "registered", evidence=False)
    stale = collect_stale_evidence_item_ids((resolved_no_ev, open_no_ev))
    assert "D-X" not in stale
    assert "OPEN-2" in stale
