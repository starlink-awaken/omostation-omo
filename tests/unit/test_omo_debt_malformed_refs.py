"""一个畸形 refs 条目不得让债务投影平面停摆 (2026-11-23 实证)。

缺陷形状: 条目 refs 里写散文注解时, 若正文含 ASCII `: ` (冒号+空格) ——
例如 `CI workflow run: / .githooks+hook-runner.sh` —— YAML 把整个**列表项**
解析成 mapping 而不是字符串。装载器 `_as_tuple` 只归一顶层 dict、不校验逐
元素, 于是 `DebtItem.mitigation_refs` (契约声明 `tuple[str, ...]`) 里混进
dict, `Path(dict)` 抛 TypeError。

实测后果 (origin/main 上仅 1 个条目中招): `com.omostation.omo-debt-refresh`
连崩 59 次 ≈29.5 h, refresh/dispatch/campaign/report 四步因 `&&` 串接全部
停摆, 四份投影冻结, 而冻结那份对外仍报 `debt_health: 100.0 /
backlog_pressure: 0`, 真实重算是 `0.0 / 7.45`。假绿比崩更贵。

同族前例: `tests/unit/test_omo_debt_naive_timestamps.py` (2026-11-09,
日期-only 时间戳让同一函数崩)。本文件走**真实入口** `refresh_outputs`,
不是从生产脚本抽函数直调 —— 否则钉不住接线。
"""

from __future__ import annotations

from pathlib import Path

import yaml

from omo.omo_debt import refresh_outputs
from omo.omo_debt_metrics import _resolve_ref_path

_NOW = "2026-11-23T00:00:00Z"

_BAD_ITEM = """\
id: "BAD-REFS"
title: "Malformed mitigation refs (prose with ASCII colon)"
dimension: "governance"
subdimension: "consistency"
domain: "infrastructure"
scope: "workspace"
severity: "medium"
weight: 0.1
entropy_class: "low"
lifecycle_state: "identified"
owner: "governance-team"
affected_roots: []
evidence_refs:
- evidence.md
mitigation_refs:
- bin/tool.py —— 层6 check_plane_coverage()：runtime GATES_LIST / CI workflow run: / .githooks+hook-runner.sh 调用行 五平面取并集
- docs/pitfalls.md ——「DEFAULT_POLICY 不是执行平面」
opened_at: "2026-11-11"
last_reviewed_at: "2026-11-11"
next_review_at: "2026-11-25"
gate_level: none
x1_policy_ref: ""
x3_tier: "Tool"
description: >-
  prose payload
decision_needed: >-
  none
history:
- at: "2026-11-11T10:13:00+00:00"
  action: identify
  note: >-
    seeded
"""

_GOOD_ITEM = """\
id: "GOOD-REFS"
title: "Well-formed refs"
dimension: "governance"
subdimension: "consistency"
domain: "infrastructure"
scope: "workspace"
severity: "medium"
weight: 0.1
entropy_class: "low"
lifecycle_state: "identified"
owner: "governance-team"
affected_roots: []
evidence_refs:
- evidence.md
mitigation_refs:
- mitigation.md
opened_at: "2026-11-11"
last_reviewed_at: "2026-11-11"
next_review_at: "2026-11-25"
gate_level: none
x1_policy_ref: ""
x3_tier: "Tool"
description: >-
  prose payload
decision_needed: >-
  none
history:
- at: "2026-11-11T10:13:00+00:00"
  action: identify
  note: >-
    seeded
"""


def _fixture(tmp_path: Path) -> Path:
    omo_dir = tmp_path / ".omo"
    (omo_dir / "_truth" / "registry").mkdir(parents=True)
    (omo_dir / "debt" / "items").mkdir(parents=True)
    (omo_dir / "_truth" / "registry" / "debt.yaml").write_text(
        "seed_items:\n- .omo/debt/items/BAD-REFS.yaml\n- .omo/debt/items/GOOD-REFS.yaml\n",
        encoding="utf-8",
    )
    (omo_dir / "debt" / "items" / "BAD-REFS.yaml").write_text(_BAD_ITEM, encoding="utf-8")
    (omo_dir / "debt" / "items" / "GOOD-REFS.yaml").write_text(_GOOD_ITEM, encoding="utf-8")
    (tmp_path / "evidence.md").write_text("evidence\n", encoding="utf-8")
    (tmp_path / "mitigation.md").write_text("mitigation\n", encoding="utf-8")
    return omo_dir


def test_resolver_returns_none_for_non_string_ref(tmp_path: Path) -> None:
    """逐元素非 str 时必须返回 None, 不得让 Path() 抛 TypeError。"""
    assert _resolve_ref_path(tmp_path, {"bin/tool.py —— 层6": " runtime"}) is None
    assert _resolve_ref_path(tmp_path, 7) is None
    assert _resolve_ref_path(tmp_path, "evidence.md") == tmp_path / "evidence.md"


def test_fixture_item_yaml_actually_produces_a_dict(tmp_path: Path) -> None:
    """钉住缺陷形状本身: 那行散文在 YAML 里确实解析成 dict 而非 str。"""
    payload = yaml.safe_load(_BAD_ITEM)
    refs = payload["mitigation_refs"]
    assert isinstance(refs, list) and len(refs) == 2
    assert isinstance(refs[0], dict), "该 fixture 必须复现 YAML 误解析, 否则用例失去意义"
    assert isinstance(yaml.safe_load(_GOOD_ITEM)["mitigation_refs"][0], str)


def test_refresh_outputs_survives_malformed_ref(tmp_path: Path) -> None:
    """真实入口: 一个畸形条目存在时, 四份投影照常产出。"""
    omo_dir = _fixture(tmp_path)

    refresh_outputs(omo_dir, _NOW)  # 修复前此处抛 TypeError

    dashboard = yaml.safe_load((omo_dir / "debt" / "dashboard" / "current.yaml").read_text(encoding="utf-8"))
    assert dashboard["generated_at"] == _NOW
    assert dashboard["debt_metrics"]["backlog_pressure"] > 0
    for name in ("review-queue", "action-packet", "owner-routing"):
        assert (omo_dir / "debt" / name / "current.yaml").exists(), f"{name} 未产出"


def test_malformed_ref_item_is_flagged_stale_not_dropped(tmp_path: Path) -> None:
    """畸形条目要被判 stale(可见), 且其后条目仍被处理(平面未中断)。"""
    omo_dir = _fixture(tmp_path)
    refresh_outputs(omo_dir, _NOW)

    queue = yaml.safe_load((omo_dir / "debt" / "review-queue" / "current.yaml").read_text(encoding="utf-8"))
    entries = {
        entry["id"]: entry
        for bucket in ("due_now", "upcoming", "escalation_candidates", "unscheduled")
        for entry in (queue.get(bucket) or [])
    }
    assert "BAD-REFS" in entries and "GOOD-REFS" in entries, list(entries)
    assert entries["BAD-REFS"]["stale_evidence"] is True
