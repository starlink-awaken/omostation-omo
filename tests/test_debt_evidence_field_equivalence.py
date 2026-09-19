"""回归: 债务证据字段等价 — closed_evidence 与 resolution_evidence 同权.

2026-09-19 实证误报: 审计的 debt integrity 只认 `resolution_evidence`, 于是把
  ATTIC_ORPHAN_GITLINK (closed) / SUBMODULE_DRIFT (resolved)
判为"无证据" —— 而两者都有 `closed_evidence` (>= 20 字符) 与 `evidence_refs`。

本仓既有工具已把两者视为等价:
  - bin/gac/fix-debt-fields.py 的占位符扫描遍历 ["closed_evidence", "resolution_evidence"]
  - bin/ssot/_shared.py::check_evidence 回退链含两者
故两处审计与之对齐, 消除误报 (实测 debt integrity 90.0 → 100.0)。
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

import omo.omo_audit  # noqa: F401  先导入顶层以建立正确依赖顺序 (避免循环导入)
from omo import omo_audit_checks, omo_audit_freshness

LONG = "A" * 40


def _item(directory: Path, name: str, **fields) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    p = directory / name
    p.write_text(yaml.safe_dump(fields, allow_unicode=True), encoding="utf-8")
    return p


@pytest.fixture
def items_dir(tmp_path, monkeypatch):
    d = tmp_path / "debt" / "items"
    d.mkdir(parents=True)
    monkeypatch.setattr(omo_audit_checks, "DEBT_ITEMS_DIR", d)
    return d


# ── debt integrity 检查 ───────────────────────────────────


def test_closed_evidence_is_accepted(items_dir):
    """核心: 只有 closed_evidence 的 closed 项不得被判无证据."""
    _item(items_dir, "a.yaml", id="A", lifecycle_state="closed", closed_evidence=LONG)
    r = omo_audit_checks.governance_check_debt_integrity()
    assert r.score == 100.0, f"closed_evidence 应被接受: {r.message}"
    assert r.severity == "ok"


def test_resolution_evidence_still_accepted(items_dir):
    _item(items_dir, "b.yaml", id="B", lifecycle_state="resolved", resolution_evidence=LONG)
    assert omo_audit_checks.governance_check_debt_integrity().score == 100.0


def test_history_note_fallback_still_works(items_dir):
    """history 末条 note 作为回退仍有效 (既有行为不得回退)."""
    _item(
        items_dir,
        "c.yaml",
        id="C",
        lifecycle_state="closed",
        history=[{"at": "2026-09-19", "action": "close", "note": LONG}],
    )
    assert omo_audit_checks.governance_check_debt_integrity().score == 100.0


def test_genuinely_evidenceless_is_still_flagged(items_dir):
    """负向: 真无证据必须仍被判出 (不能因放宽而失去检出力)."""
    _item(items_dir, "D.yaml", id="D", lifecycle_state="resolved")
    r = omo_audit_checks.governance_check_debt_integrity()
    assert r.score < 100.0
    assert "D" in str(r.details), f"应列出问题条目, 实得: {r.details}"


def test_too_short_evidence_still_flagged(items_dir):
    _item(items_dir, "e.yaml", id="E", lifecycle_state="closed", closed_evidence="short")
    assert omo_audit_checks.governance_check_debt_integrity().score < 100.0


# ── freshness 检查 ────────────────────────────────────────


def test_freshness_accepts_closed_evidence(tmp_path):
    """freshness 的 closed 项同样应接受 closed_evidence."""
    d = tmp_path / "debt" / "items"
    d.mkdir(parents=True)
    _item(d, "f.yaml", id="F", lifecycle_state="closed", closed_evidence=LONG)
    import inspect

    src = inspect.getsource(omo_audit_freshness)
    assert 'data.get("closed_evidence")' in src, "freshness 检查应同时读 closed_evidence (与 checks / _shared 对齐)"
