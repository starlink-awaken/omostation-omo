#!/usr/bin/env python3
"""OMO governance audit — debt action audit trail + workspace compliance checks.

This module serves two roles:

  1. **Debt action audit trail** (X1-AUDIT-001) — `record() / query() / summary()`
     append structured records to a JSONL audit file. Linked to debt items via
     `debt_id` field.

  2. **Workspace compliance checks** (P30-W1 GOV-MERGE, migrated from
     kairon_governance.audit) — `run_governance_audit()` and helpers run 7
     checks (lint, tests, debt, ADR, tasks, agora-health, doc-lifecycle) and
     produce a Markdown + dataclass report.

The two roles share this module because both produce audit-style output
(JSONL / Markdown) and both underpin governance visibility. The debt
audit trail functions remain at the top of the file for backward
compatibility; the compliance checks follow under a clear section header.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

# 复用 omo_io.AppendOnlyLog (P49+ AppendOnlyLog 抽象: JSONL 物理读写唯一入口)
from omo.omo_io import AppendOnlyLog
from omo.omo_paths import (
    DEBT_ITEMS_DIR,
    DECISIONS_DIR,
    KAIRON_DIR,
    KAIRON_PACKAGES,
    TASKS_PLANNED_DIR,
    WORKSPACE_ROOT,
)

# 2026-08-29: governance check functions extracted to omo_audit_checks.py
from .omo_audit_types import CheckResult, GovernanceReport, _load_yaml_safely

_OMO_ROOT: Path = WORKSPACE_ROOT / ".omo"
_KAIRON_DIR: Path = KAIRON_DIR
_WORKSPACE_ROOT: Path = WORKSPACE_ROOT
ENV_SKIP_AGORA = "OMO_AUDIT_SKIP_AGORA"

from .omo_audit_checks import (
    governance_check_adr_links,
    governance_check_agora_health,
    governance_check_debt_integrity,
    governance_check_doc_lifecycle,
    governance_check_lint,
    governance_check_task_consistency,
    governance_check_test_coverage,
)

# =============================================================================
# Section 1 — Debt action audit trail (X1-AUDIT-001, pre-existing)
# =============================================================================


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_audit_file() -> Path:
    return Path.home() / "runtime" / "audit" / "governance-audit.jsonl"


# 注: per-call log 创建 (与 omo_bos_metrics 一致), 便于 monkeypatch DEFAULT_METRICS_PATH.
# AppendOnlyLog 构造轻量 (Path + Lock), per-call 创建开销可忽略.


def record(
    action: str,
    debt_id: str = "",
    actor: str = "",
    details: str = "",
    audit_file: str | Path | None = None,
) -> dict:
    """Record a governance action to the audit trail.

    Returns the record dict for reference.
    """
    entry = {
        "ts": _utc_now(),
        "action": action,
        "debt_id": debt_id,
        "actor": actor,
        "details": details,
    }
    log = AppendOnlyLog(Path(audit_file) if audit_file else _default_audit_file())
    log.append(entry, sort_keys=True)
    return entry


def record_compute_node_state(node_id: str, **fields: dict) -> dict:
    """Safe audit write-back for M1 compute node state. (R3 Adaptive Feedback)

    Loads the corresponding YAML file from projects/ecos/src/ecos/ssot/mof/m1/compute_engine/,
    updates fields (like status, last_seen), and writes back safely.
    """
    from omo.omo_io import write_yaml_atomic
    from omo.omo_shared import load_yaml

    m1_dir = Path("~/Workspace/projects/ecos/src/ecos/ssot/mof/m1/compute_engine").expanduser()
    m1_dir.mkdir(parents=True, exist_ok=True)
    target_file = None

    data: dict = {}
    for f in m1_dir.glob("*.yaml"):
        try:
            loaded = load_yaml(f)
            if loaded.get("node_id") == node_id or f.stem == node_id:
                target_file = f
                data = loaded
                break
        except Exception:  # defensive fallback
            pass

    if target_file is None:
        target_file = m1_dir / f"{node_id}.yaml"
        data = {"node_id": node_id, "schema_version": 1}

    for k, v in fields.items():
        data[k] = v

    write_yaml_atomic(target_file, data)
    return record(
        action="mesh_node_state_update",
        details=f"Updated M1 YAML for node {node_id}: {fields.get('status')}",
    )


def query(limit: int = 50, audit_file: str | Path | None = None) -> list[dict]:
    """Read the most recent audit records."""
    log = AppendOnlyLog(Path(audit_file) if audit_file else _default_audit_file())
    return log.read_all()[-limit:]


def summary(audit_file: str | Path | None = None) -> dict:
    """Return audit summary."""
    log = AppendOnlyLog(Path(audit_file) if audit_file else _default_audit_file())
    records = log.read_all()
    if not records:
        return {"total": 0, "actions": {}, "with_debt": 0}
    actions: dict[str, int] = {}
    debt_ids: set[str] = set()
    for r in records:
        a = r.get("action", "unknown")
        actions[a] = actions.get(a, 0) + 1
        if r.get("debt_id"):
            debt_ids.add(r["debt_id"])
    return {
        "total": len(records),
        "actions": actions,
        "unique_debt_ids": len(debt_ids),
        "latest": records[-1] if records else None,
    }


# =============================================================================
# Section 2 — Workspace governance compliance checks (P30-W1 GOV-MERGE)
# =============================================================================

# 模块级路径(允许测试覆盖)
_KAIRON_DIR: Path = KAIRON_DIR
_OMO_ROOT: Path = WORKSPACE_ROOT / ".omo"
_WORKSPACE_ROOT: Path = WORKSPACE_ROOT

# 环境变量开关: daemon 跑 audit 时跳过 agora 探活(避免每 tick 11 HTTP 请求)
ENV_SKIP_AGORA = "OMO_AUDIT_SKIP_AGORA"

Severity = Literal["ok", "warn", "fail"]


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


# ── 7 项检查 ─────────────────────────────────────────────


def compute_grade(score: float) -> str:
    """等权平均分 → 等级."""
    if score >= 98:
        return "A+"
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"


def build_watchlist(checks: list[CheckResult]) -> list[str]:
    """从 warn/fail 检查里提炼新债务候选(每个检查最多 5 条)."""
    watchlist: list[str] = []
    for c in checks:
        if c.severity == "ok":
            continue
        for d in c.details[:5]:
            watchlist.append(f"[{c.category}] {d}")
    return watchlist


def build_recommendations(checks: list[CheckResult]) -> list[str]:
    """基于检查类别给出修复建议."""
    recs: list[str] = []
    for c in checks:
        if c.severity == "ok":
            continue
        if c.category == "lint":
            recs.append("修复 ruff 错误, 参考 `cd projects/knowledge/kairon && uv run ruff check packages/ --fix`")
        elif c.category == "tests":
            sample = ", ".join(d.split(":")[0] for d in c.details[:3])
            recs.append(f"为 {sample} 等包至少添加 1 个 smoke test")
        elif c.category == "debt":
            recs.append("给 resolved/closed 债务补上 `resolution_evidence` 字段(>= 20 字符)")
        elif c.category == "knowledge":
            recs.append("清理 ADR INDEX.md 中的死链 / 补齐未列出的 ADR / 创建缺失的 ADR")
        elif c.category == "tasks":
            recs.append("补齐任务 YAML 中声明的 deliverables, 或将 status 回退到 in_progress")
        elif c.category == "agora":
            unhealthy = ", ".join(d for d in c.details[:5])
            recs.append(f"修复 agora 服务可达性 ({unhealthy}); 检查 service 端口与 health_endpoint 字段")
    return recs


def run_governance_audit(workspace: Path | None = None) -> GovernanceReport:
    """跑 7 项检查并聚合报告.

    workspace 参数允许测试时传入 tmp_path, 默认读 WORKSPACE_ROOT.
    """
    global _OMO_ROOT, _KAIRON_DIR, _WORKSPACE_ROOT
    original_paths: tuple[Path, Path, Path] | None = None
    if workspace is not None:
        original_paths = (_OMO_ROOT, _KAIRON_DIR, _WORKSPACE_ROOT)
        _OMO_ROOT = workspace / ".omo"
        _KAIRON_DIR = workspace / "projects" / "knowledge" / "kairon"
        _WORKSPACE_ROOT = workspace

    try:
        checks = [
            governance_check_lint(),
            governance_check_test_coverage(),
            governance_check_debt_integrity(),
            governance_check_adr_links(),
            governance_check_task_consistency(),
            governance_check_agora_health(),
            governance_check_doc_lifecycle(),
        ]
        total = sum(c.score for c in checks) / len(checks)
        return GovernanceReport(
            date=datetime.now(UTC).strftime("%Y-%m-%d"),
            total_score=round(total, 1),
            grade=compute_grade(total),
            checks=checks,
            watchlist=build_watchlist(checks),
            recommendations=build_recommendations(checks),
        )
    finally:
        if original_paths is not None:
            _OMO_ROOT, _KAIRON_DIR, _WORKSPACE_ROOT = original_paths


def render_markdown(report: GovernanceReport) -> str:
    """便捷封装: report.to_markdown()."""
    return report.to_markdown()


# =============================================================================
# Section 3 — CLI (governance subcommand)
# =============================================================================


def governance_main(argv: list[str] | None = None) -> int:
    """CLI: omo governance audit [--output PATH] [--json] [--no-history]."""
    parser = argparse.ArgumentParser(prog="omo governance audit")
    parser.add_argument("--output", "-o", default=None, help="Markdown 报告输出路径(默认 stdout)")
    parser.add_argument("--json", action="store_true", help="同时输出 JSON 数据")
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="不写入治理历史 JSONL(默认会写)",
    )
    args = parser.parse_args(argv)

    report = run_governance_audit()
    md = report.to_markdown()
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(md, encoding="utf-8")
        print(f"[AUDIT] Markdown 报告: {out_path}")
    else:
        print(md)

    if args.json:
        json_target = Path(args.output) if args.output else Path("/tmp/governance_audit.json")
        json_path = json_target.with_suffix(".json")
        json_path.write_text(
            json.dumps(asdict(report), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[AUDIT] JSON 数据: {json_path}")

    print(f"\n[AUDIT] 总分: {report.total_score} ({report.grade})")
    if report.watchlist:
        print(f"[AUDIT] 新发现潜在债务: {len(report.watchlist)} 条")

    if not args.no_history:
        try:
            from omo.omo_history import append_entry

            target = append_entry(
                {
                    "total_score": report.total_score,
                    "grade": report.grade,
                    "checks": [
                        {
                            "name": c.name,
                            "category": c.category,
                            "score": c.score,
                            "severity": c.severity,
                        }
                        for c in report.checks
                    ],
                    "watchlist_count": len(report.watchlist),
                }
            )
            print(f"[AUDIT] 治理历史已 append: {target}")
        except Exception as exc:  # defensive fallback
            print(f"[AUDIT] 治理历史写入失败(不影响主流程): {exc}", file=sys.stderr)
    return 0


def governance_history_main(argv: list[str] | None = None) -> int:
    """CLI: omo governance history [--limit N] [--trend] [--path P]."""
    parser = argparse.ArgumentParser(prog="omo governance history")
    parser.add_argument("--limit", type=int, default=30, help="显示条数")
    parser.add_argument("--trend", action="store_true", help="显示趋势图")
    parser.add_argument("--path", default=None, help="历史 JSONL 路径(默认内置)")
    args = parser.parse_args(argv)

    from omo.omo_history import read_history, render_trend_chart

    path = Path(args.path) if args.path else None
    if args.trend:
        print(render_trend_chart(path=path))
        return 0
    entries = read_history(path=path, limit=args.limit)
    if not entries:
        print("(无历史记录)")
        return 0
    for e in entries:
        score = e.get("total_score", 0.0)
        grade = e.get("grade", "?")
        watchlist = e.get("watchlist_count", 0)
        date = e.get("date", "?")
        print(f"{date}  {score:5.1f}  ({grade})  watchlist={watchlist}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """主 CLI 入口: `omo audit` 子命令路由."""
    parser = argparse.ArgumentParser(prog="omo audit", description="OMO governance audit")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("governance", help="Run 6-check workspace compliance audit")
    sub.add_parser("history", help="View governance history")
    args = parser.parse_args(argv)
    if args.cmd == "governance":
        return governance_main()
    if args.cmd == "history":
        return governance_history_main()
    parser.print_help()
    return 0


__all__ = (
    # Section 2 — governance compliance checks
    "CheckResult",
    "GovernanceReport",
    "Severity",
    "build_recommendations",
    "build_watchlist",
    "compute_grade",
    "governance_check_adr_links",
    "governance_check_agora_health",
    "governance_check_debt_integrity",
    "governance_check_lint",
    "governance_check_task_consistency",
    "governance_check_test_coverage",
    "governance_history_main",
    # Section 3 — CLI
    "governance_main",
    "main",
    "query",
    # Section 1 — debt action audit trail
    "record",
    "render_markdown",
    "run_governance_audit",
    "summary",
)


if __name__ == "__main__":
    raise SystemExit(main())
