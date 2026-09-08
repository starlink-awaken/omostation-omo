"""Tests for SEMA crystallizer (BET-Y2Q2-T6-01): threshold, SKILL.md spec, hot reload."""

from __future__ import annotations

import json
import time
from pathlib import Path

from omo.resident.sema_crystallizer import (
    CorrectionEvent,
    CorrectionLedger,
    SemaCrystallizer,
    crystallize,
)


def _ev(key: str, i: int = 0) -> CorrectionEvent:
    return CorrectionEvent(
        key=key, rule_type="banned_phrase", pattern="为进一步推进", source="signature_diff", sample_id=f"s{i}"
    )


def test_threshold_second_occurrence_triggers():
    ledger = CorrectionLedger(threshold=2)
    assert ledger.record(_ev("k")) is False, "1st occurrence must not trigger"
    assert ledger.record(_ev("k", 1)) is True, "2nd occurrence must trigger"
    assert "k" in ledger.ready_keys()


def test_ledger_loads_rules_as_pre_aggregated_events(tmp_path: Path):
    rules = tmp_path / "rules.jsonl"
    rules.write_text(
        json.dumps({"rule_id": "HN-001", "rule_type": "banned_phrase", "pattern": "为进一步推进", "count": 4})
        + "\n"
        + json.dumps({"rule_id": "HN-002", "rule_type": "terminology_replace", "pattern": "高度重视", "count": 1})
        + "\n",
        encoding="utf-8",
    )
    c = CorrectionLedger(threshold=2)
    n = c.load_rules_as_events(rules)
    assert n == 3  # 4 capped at threshold 2 + 1
    assert c.ready_keys() == ["banned_phrase:为进一步推进"]


def test_crystallize_skill_md_schema():
    cand = crystallize(
        "banned_phrase:为进一步推进", [_ev("banned_phrase:为进一步推进"), _ev("banned_phrase:为进一步推进", 1)]
    )
    md = cand.skill_md
    assert md.startswith("---\n")
    assert "name: sema-" in md
    assert "description:" in md
    assert "## 触发条件" in md and "## 反例" in md
    assert cand.test_py.startswith('"""Auto-crystallized test')


def test_install_and_index_refresh(tmp_path: Path):
    # 造一个带 INDEX 的 skills 目录
    (tmp_path / ".agents" / "skills").mkdir(parents=True)
    (tmp_path / ".agents" / "skills" / "INDEX.md").write_text(
        "# Skills Index\n\n| 名称 | 标题 | 触发条件 | 路径 |\n|------|------|----------|------|\n",
        encoding="utf-8",
    )
    cr = SemaCrystallizer(workspace_root=tmp_path)
    cand = crystallize(
        "banned_phrase:为进一步推进", [_ev("banned_phrase:为进一步推进"), _ev("banned_phrase:为进一步推进", 1)]
    )
    d = cr.install(cand)
    assert (d / "SKILL.md").is_file() and (d / "test_skill.py").is_file()
    assert cand.name in cr.index_path().read_text(encoding="utf-8")


def test_hot_reload_under_500ms(tmp_path: Path):
    cr = SemaCrystallizer(workspace_root=tmp_path)
    cand = crystallize("k:x", [_ev("k:x"), _ev("k:x", 1)])
    cr.install(cand)
    res = cr.hot_reload()
    assert res["ok"] is True
    assert res["latency_ms"] < 500, f"hot reload took {res['latency_ms']}ms"
    assert res["skill_count"] >= 1
    manifest = json.loads((tmp_path / ".omo" / "state" / "sema-skills-manifest.json").read_text(encoding="utf-8"))
    assert "aetherforge" in manifest["active_domains"] and "agora" in manifest["active_domains"]
