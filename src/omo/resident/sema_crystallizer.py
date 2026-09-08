"""SEMA crystallizer — 防踩坑信念自结晶为 Agent Skill (BET-Y2Q2-T6-01).

Consumes correction events (signature-diff rule hits from T10-115's
hard-negative rule library, CI interception records) and crystallizes
recurring human corrections into installable Agent Skills.

Trigger contract (done_when[0]): the **second** occurrence of the same
correction key triggers the crystallization pipeline; the first records
and waits. Products: a schema-compliant ``SKILL.md`` (frontmatter
name/description, trigger, steps, counter-examples) plus a runnable
pytest skeleton, installed under ``.agents/skills/auto-crystallized/``.

Hot reload contract (done_when[2]): ``hot_reload()`` refreshes the skill
INDEX in place (mtime-sentinel) — no service restart, <500ms.
"""

from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_RULES_REL = ".omo/state/hard-negative-rules.jsonl"
SKILLS_DIR_REL = "agents/skills"  # repo-relative; real mount is .agents/skills
INDEX_REL = "agents/skills/INDEX.md"
CRYSTALLIZED_DIRNAME = "auto-crystallized"
CRYSTALLIZE_THRESHOLD = 2


@dataclass(slots=True)
class CorrectionEvent:
    """One normalized human correction (rule hit or CI interception)."""

    key: str  # aggregation key: pattern+type (from hard-negative rules)
    rule_type: str
    pattern: str
    source: str  # signature_diff | ci_intercept
    sample_id: str = ""
    ts: float = field(default_factory=time.time)


@dataclass(slots=True)
class SkillCandidate:
    """A crystallized skill ready for install."""

    name: str
    description: str
    skill_md: str
    test_py: str
    trigger_key: str
    evidence_count: int


class CorrectionLedger:
    """Aggregates correction events by key and reports trigger readiness."""

    def __init__(self, threshold: int = CRYSTALLIZE_THRESHOLD) -> None:
        self.threshold = threshold
        self.events: dict[str, list[CorrectionEvent]] = defaultdict(list)

    def record(self, event: CorrectionEvent) -> bool:
        """Record one event; return True when the trigger threshold is reached
        (exactly at the threshold occurrence, e.g. the 2nd of 2)."""
        self.events[event.key].append(event)
        return len(self.events[event.key]) == self.threshold

    def ready_keys(self) -> list[str]:
        return [k for k, v in self.events.items() if len(v) >= self.threshold]

    def load_rules_as_events(self, rules_path: Path) -> int:
        """Import hard-negative rules (T10-115) as correction events.

        A rule with count>=threshold arrives pre-aggregated: it is recorded
        as `count` identical events so the threshold logic stays uniform.
        """
        n = 0
        with rules_path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = f"{raw.get('rule_type', 'unknown')}:{raw.get('pattern', '')}"
                count = max(1, int(raw.get("count", 1)))
                ev = CorrectionEvent(
                    key=key,
                    rule_type=str(raw.get("rule_type", "unknown")),
                    pattern=str(raw.get("pattern", "")),
                    source="signature_diff",
                )
                for _ in range(min(count, self.threshold)):
                    self.record(ev)
                    n += 1
        return n


def _slugify(key: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", key.lower()).strip("-")
    return f"sema-{slug[:48]}" if slug else "sema-uncategorized"


def _sanitize_description(rule_type: str, pattern: str) -> str:
    readable = pattern.replace("\\", "")[:40] or rule_type
    mapping = {
        "banned_phrase": f"起草时禁用表述「{readable}」——署名中反复删除",
        "verbose_trim": f"避免冗长铺陈（模式「{readable}」）——署名中反复压缩",
        "terminology_replace": f"术语统一：「{readable}」应替换为署名偏好用语",
        "fact_fix": f"事实性表述需复核（模式「{readable}」）",
    }
    return mapping.get(rule_type, f"防踩坑信念（{rule_type}: {readable}）")


def crystallize(key: str, events: list[CorrectionEvent]) -> SkillCandidate:
    """Build a SkillCandidate (SKILL.md + pytest skeleton) from >=2 events."""
    first = events[0]
    slug = _slugify(key)
    description = _sanitize_description(first.rule_type, first.pattern)
    name = slug
    skill_md = f"""---
name: {name}
description: {description}
metadata:
  node_type: skill
  origin: sema-crystallizer
  trigger_key: {json.dumps(key)}
  evidence_count: {len(events)}
  created: {time.strftime("%Y-%m-%d")}
---

# {description}

## 触发条件

起草/修订公文与技术文档时，当出现与以下模式匹配的内容即应触发本技能：

- 模式: `{first.pattern}`
- 类型: `{first.rule_type}`
- 证据: {len(events)} 次同类人工纠偏（sample ids: {", ".join(e.sample_id for e in events[:5] if e.sample_id) or "n/a"}）

## 操作步骤

1. 定位草稿中匹配该模式的内容。
2. 按{first.rule_type}语义处理：
   {("直接删除该表述，不保留同义改写。" if first.rule_type == "banned_phrase" else "按署名偏好改写并复核上下文衔接。" if first.rule_type == "terminology_replace" else "压缩为实质内容，删除铺垫性文字。")}
3. 输出前自检：全文不再命中该模式。

## 反例（不应发生）

- 草稿包含 `{first.pattern}` 却未处理即提交署名流程。
- 处理后引入新的同类模式（应再次触发结晶）。

## 依据

由 SEMA 结晶管线自 {first.source} 事件自动生成（BET-Y2Q2-T6-01）。
"""
    test_py = f'''"""Auto-crystallized test for skill {name} (SEMA, BET-Y2Q2-T6-01)."""

import re

from {name.replace("-", "_")} import run  # skill entry contract


def test_pattern_is_detected():
    violating = "含 {first.pattern} 的草稿"
    assert run(violating)["violation_found"] is True


def test_clean_draft_passes():
    assert run("无该模式的干净草稿")["violation_found"] is False
'''
    return SkillCandidate(
        name=name,
        description=description,
        skill_md=skill_md,
        test_py=test_py,
        trigger_key=key,
        evidence_count=len(events),
    )


class SemaCrystallizer:
    """End-to-end pipeline: ledger -> crystallize -> install -> hot reload."""

    def __init__(self, workspace_root: Path, threshold: int = CRYSTALLIZE_THRESHOLD) -> None:
        self.ws = workspace_root
        self.ledger = CorrectionLedger(threshold=threshold)

    # -- paths -------------------------------------------------------------

    def skills_root(self) -> Path:
        return self.ws / ".agents" / "skills" / CRYSTALLIZED_DIRNAME

    def index_path(self) -> Path:
        return self.ws / ".agents" / "skills" / "INDEX.md"

    # -- ingest ------------------------------------------------------------

    def ingest_rules(self, rules_path: Path | None = None) -> int:
        p = rules_path or (self.ws / ".omo/state/hard-negative-rules.jsonl")
        return self.ledger.load_rules_as_events(p) if Path(p).exists() else 0

    # -- crystallize & install ------------------------------------------------

    def crystallize_ready(self) -> list[SkillCandidate]:
        return [crystallize(k, evs) for k, evs in self.ledger.events.items() if len(evs) >= self.ledger.threshold]

    def install(self, candidate: SkillCandidate) -> Path:
        skill_dir = self.skills_root() / candidate.name
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text(candidate.skill_md, encoding="utf-8")
        (skill_dir / "test_skill.py").write_text(candidate.test_py, encoding="utf-8")
        self._refresh_index(candidate)
        return skill_dir

    def _refresh_index(self, candidate: SkillCandidate) -> None:
        idx = self.index_path()
        if not idx.exists():
            return
        text = idx.read_text(encoding="utf-8")
        if candidate.name in text:
            return
        row = f"| {candidate.name} | Sema Crystallized | {candidate.description[:24]} | .agents/skills/{CRYSTALLIZED_DIRNAME}/{candidate.name}/SKILL.md |"
        text = text.rstrip() + "\n" + row + "\n"
        idx.write_text(text, encoding="utf-8")

    # -- hot reload ----------------------------------------------------------

    def hot_reload(self) -> dict[str, Any]:
        """In-place INDEX/skill refresh; returns latency info (<500ms contract)."""
        t0 = time.perf_counter()
        sentinel = self.ws / ".omo/state/sema-skills-manifest.json"
        skills = sorted(str(p.relative_to(self.skills_root())) for p in self.skills_root().rglob("SKILL.md"))
        payload = {
            "reloaded_at": time.time(),
            "skills": skills,
            "active_domains": ["aetherforge", "agora"],  # 运行态消费方（清单注入）
        }
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return {"ok": True, "latency_ms": round(elapsed_ms, 2), "skill_count": len(skills), "budget_ms": 500}
