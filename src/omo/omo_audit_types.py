#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

# T10-58: restored from pre-extraction omo_audit.py (8a816dbe^) — the
# extraction dropped this alias while keeping annotated uses of it.
Severity = Literal["ok", "warn", "fail"]


@dataclass
class CheckResult:
    """单次检查结果."""

    name: str
    category: str
    severity: Severity
    score: float  # 0-100
    message: str
    details: list[str] = field(default_factory=list)


@dataclass
class GovernanceReport:
    """巡检报告聚合."""

    date: str
    total_score: float
    grade: str
    checks: list[CheckResult]
    watchlist: list[str]
    recommendations: list[str]

    def to_markdown(self) -> str:
        """渲染为 Markdown 报告."""
        sev_emoji = {"ok": "OK", "warn": "WARN", "fail": "FAIL"}
        lines: list[str] = [
            f"# omo 治理巡检报告 — {self.date}",
            "",
            f"**总分: {self.total_score} ({self.grade})**",
            "",
            "> 巡检器只读:本报告未修改 .omo/state/、.omo/goals/、.omo/INDEX.md。",
            "> 任务: P30-W1 GOV-MERGE (omo 治理巡检, 迁移自 kairon-governance.audit)",
            "",
            "## 1. 检查结果",
            "",
            "| 检查 | 类别 | 严重度 | 分数 | 说明 |",
            "|---|---|---|---|---|",
        ]
        for c in self.checks:
            sev = sev_emoji.get(c.severity, c.severity)
            lines.append(f"| {c.name} | {c.category} | {sev} | {c.score:.0f} | {c.message} |")

        lines += ["", "## 2. 检查细节", ""]
        for c in self.checks:
            if c.details:
                lines.append(f"### {c.name}")
                lines.append("")
                for d in c.details:
                    lines.append(f"- {d}")
                lines.append("")

        if self.watchlist:
            lines += ["## 3. 新发现潜在债务(debt watchlist)", ""]
            for w in self.watchlist:
                lines.append(f"- {w}")
            lines.append("")
        else:
            lines += ["## 3. 新发现潜在债务(debt watchlist)", "", "_(无)_", ""]

        if self.recommendations:
            lines += ["## 4. 修复建议", ""]
            for r in self.recommendations:
                lines.append(f"- {r}")
            lines.append("")
        else:
            lines += ["## 4. 修复建议", "", "_(无)_", ""]

        lines += [
            "## 5. 评分方法",
            "",
            "- 总分 = 7 项检查分数的算术平均(等权)",
            "- 等级阈值: 98+=A+ | 90-97=A | 80-89=B | 70-79=C | 60-69=D | <60=F",
            "- 扣分规则:",
            "  - **lint**: 每个 ruff error 扣 5 分",
            "  - **tests**: 每个无测试的包扣 10 分",
            "  - **debt**: 每条 resolved 缺证据扣 5 分",
            "  - **knowledge**: 每条断链扣 20 分",
            "  - **tasks**: 每条不一致扣 10 分",
            "  - **agora**: 健康度 < 80% 触发 warn, < 50% 触发 fail",
            "  - (设 `OMO_AUDIT_SKIP_AGORA=1` 可跳过 agora 探活, 默认 ok=100)",
            "",
        ]
        return "\n".join(lines)


# ── YAML 工具 ──────────────────────────────────────────────


def _load_yaml_safely(path: Path) -> dict | None:
    """安全加载 YAML,失败返回 None."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        from .omo_shared import load_yaml_docs

        return load_yaml_docs(text)
    except ImportError:
        pass
    except Exception:  # defensive fallback
        return None
    return _mini_yaml_parse(text)


def _mini_yaml_parse(text: str) -> dict:
    """极简 YAML 解析器, 仅支持 'key: value' 形式的顶层字段."""
    out: dict = {}
    for line in text.splitlines():
        line = line.rstrip()
        if not line or line.startswith(("#", " ", "\t")):
            continue
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        out[key] = value
    return out
