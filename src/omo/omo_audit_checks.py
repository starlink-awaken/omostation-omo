#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from omo.omo_io import AppendOnlyLog
from omo.omo_paths import (
    DEBT_ITEMS_DIR,
    DECISIONS_DIR,
    KAIRON_DIR,
    KAIRON_PACKAGES,
    TASKS_PLANNED_DIR,
    WORKSPACE_ROOT,
)

from . import omo_audit as _audit_mod
from .omo_audit_types import CheckResult, GovernanceReport, Severity, _load_yaml_safely


def governance_check_lint() -> CheckResult:
    """跑 ruff check kairon packages/, 统计 error 数."""
    try:
        result = subprocess.run(
            ["uv", "run", "ruff", "check", "packages/", "--statistics"],
            cwd=str(_audit_mod._KAIRON_DIR),
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        output = (result.stdout or "") + (result.stderr or "")
        m = re.search(r"Found\s+(\d+)\s+errors?", output, re.IGNORECASE)
        errors = int(m.group(1)) if m else 0
        if errors == 0:
            return CheckResult(
                name="ruff lint",
                category="lint",
                severity="ok",
                score=100.0,
                message="0 errors",
            )
        sample: list[str] = []
        for line in output.splitlines():
            if re.match(r"^[^\s].+\.py:\d+:\d+:", line):
                sample.append(line.strip())
                if len(sample) >= 10:
                    break
        return CheckResult(
            name="ruff lint",
            category="lint",
            severity="warn",
            score=max(0.0, 100.0 - errors * 5),
            message=f"{errors} errors",
            details=sample,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(
            name="ruff lint",
            category="lint",
            severity="fail",
            score=0.0,
            message="ruff check timeout (180s)",
        )
    except FileNotFoundError as exc:
        return CheckResult(
            name="ruff lint",
            category="lint",
            severity="fail",
            score=0.0,
            message=f"ruff 未找到: {exc}",
        )


def governance_check_test_coverage() -> CheckResult:
    """每个非归档包至少 1 个 test_*.py."""
    packages_dir = KAIRON_PACKAGES
    if not packages_dir.exists():
        return CheckResult(
            name="test coverage",
            category="tests",
            severity="fail",
            score=0.0,
            message="packages/ 目录不存在",
        )
    missing: list[str] = []
    for pkg_dir in sorted(packages_dir.iterdir()):
        if not pkg_dir.is_dir():
            continue
        if pkg_dir.name.startswith(".") or pkg_dir.name.startswith("_"):
            continue
        tests_dir = pkg_dir / "tests"
        if not tests_dir.exists():
            missing.append(f"{pkg_dir.name}: 无 tests/ 目录")
            continue
        if not any(tests_dir.rglob("test_*.py")):
            missing.append(f"{pkg_dir.name}: tests/ 下无 test_*.py")
    if not missing:
        return CheckResult(
            name="test coverage",
            category="tests",
            severity="ok",
            score=100.0,
            message="all packages have tests",
        )
    return CheckResult(
        name="test coverage",
        category="tests",
        severity="warn",
        score=max(0.0, 100.0 - len(missing) * 10),
        message=f"{len(missing)} packages without tests",
        details=missing,
    )


def governance_check_debt_integrity() -> CheckResult:
    """检查 .omo/debt/items/ 中 lifecycle_state=resolved 的项是否有 resolution_evidence."""
    debt_items_dir = DEBT_ITEMS_DIR
    if not debt_items_dir.exists():
        return CheckResult(
            name="debt integrity",
            category="debt",
            severity="ok",
            score=100.0,
            message="no debt items dir",
        )
    suspicious: list[str] = []
    for yaml_file in sorted(debt_items_dir.glob("*.yaml")):
        data = _load_yaml_safely(yaml_file)
        if not data:
            continue
        lifecycle = str(data.get("lifecycle_state", "")).strip()
        if lifecycle not in ("resolved", "closed"):
            continue
        evidence = str(data.get("resolution_evidence", "")).strip()
        if not evidence:
            history = data.get("history")
            if isinstance(history, list) and history:
                last = history[-1]
                if isinstance(last, dict):
                    note = str(last.get("note", "")).strip()
                    if note and len(note) >= 20:
                        evidence = note
        if not evidence or len(evidence) < 20:
            suspicious.append(f"{yaml_file.stem}: lifecycle={lifecycle} 但无 resolution_evidence")
    if not suspicious:
        return CheckResult(
            name="debt integrity",
            category="debt",
            severity="ok",
            score=100.0,
            message="all resolved/closed debts have evidence",
        )
    return CheckResult(
        name="debt integrity",
        category="debt",
        severity="warn",
        score=max(0.0, 100.0 - len(suspicious) * 5),
        message=f"{len(suspicious)} resolved/closed debts lack evidence",
        details=suspicious,
    )


def governance_check_adr_links() -> CheckResult:
    """检查 ADR INDEX.md 引用的所有 ADR 都存在."""
    decisions_dir = DECISIONS_DIR
    if not decisions_dir.exists():
        return CheckResult(
            name="adr links",
            category="knowledge",
            severity="warn",
            score=50.0,
            message="decisions/ 目录不存在",
        )
    index_file = decisions_dir / "INDEX.md"
    if not index_file.exists():
        return CheckResult(
            name="adr links",
            category="knowledge",
            severity="warn",
            score=60.0,
            message="INDEX.md 不存在",
        )
    content = index_file.read_text(encoding="utf-8")
    referenced: set[str] = set()
    # Anchor on table column boundary (| or whitespace) so 4-digit date-like
    # strings inside table cells (e.g. "2026-07-24") are not extracted as ADR
    # filenames. ADR file refs always sit in their own table cell.
    for m in re.finditer(r"(?:^|[|\s])(\d{4})-([a-z0-9-]+)\.md(?=$|[|\s])", content):
        referenced.add(f"{m.group(1)}-{m.group(2)}.md")
    existing: set[str] = {p.name for p in decisions_dir.glob("[0-9][0-9][0-9][0-9]-*.md")}
    broken = sorted(referenced - existing)
    orphan = sorted(existing - referenced)
    if not broken and not orphan:
        return CheckResult(
            name="adr links",
            category="knowledge",
            severity="ok",
            score=100.0,
            message=f"all {len(referenced)} ADR links valid",
        )
    details: list[str] = []
    for b in broken:
        details.append(f"REFERENCED-BUT-MISSING: {b}")
    for o in orphan:
        details.append(f"EXISTS-BUT-UNLISTED: {o}")
    severity: Severity = "fail" if broken else "warn"
    score = max(0.0, 100.0 - len(broken) * 20 - len(orphan) * 5)
    return CheckResult(
        name="adr links",
        category="knowledge",
        severity=severity,
        score=score,
        message=f"{len(broken)} broken, {len(orphan)} unlisted ADR links",
        details=details,
    )


def governance_check_task_consistency() -> CheckResult:
    """status=completed 的任务, deliverables 列出的路径必须存在."""
    planned_dir = TASKS_PLANNED_DIR
    if not planned_dir.exists():
        return CheckResult(
            name="task consistency",
            category="tasks",
            severity="ok",
            score=100.0,
            message="no planned tasks dir",
        )
    inconsistent: list[str] = []
    checked = 0
    for yaml_file in sorted(planned_dir.glob("*.yaml")):
        data = _load_yaml_safely(yaml_file)
        if not data:
            continue
        if str(data.get("status", "")).strip() != "completed":
            continue
        checked += 1
        deliverables = data.get("deliverables", [])
        if isinstance(deliverables, str):
            continue
        if not isinstance(deliverables, list):
            continue
        for d in deliverables:
            if not isinstance(d, str):
                continue
            p = _audit_mod._WORKSPACE_ROOT / d
            if p.exists():
                continue
            # Glob 展开 (P36 W0 规则宽容: 含 * 的路径按 glob 展开, 全部命中才算 OK)
            if any(ch in d for ch in ("*", "?", "[")):
                matches = list((_audit_mod._WORKSPACE_ROOT).glob(d))
                if matches:
                    continue
                inconsistent.append(f"{yaml_file.stem} → {d} (glob 展开无匹配)")
                continue
            inconsistent.append(f"{yaml_file.stem} → {d} (status=completed 但文件不存在)")
    if checked == 0:
        return CheckResult(
            name="task consistency",
            category="tasks",
            severity="ok",
            score=100.0,
            message="no completed tasks to verify",
        )
    if not inconsistent:
        return CheckResult(
            name="task consistency",
            category="tasks",
            severity="ok",
            score=100.0,
            message=f"all {checked} completed tasks have deliverables",
        )
    return CheckResult(
        name="task consistency",
        category="tasks",
        severity="warn",
        score=max(0.0, 100.0 - len(inconsistent) * 10),
        message=f"{len(inconsistent)} missing deliverables",
        details=inconsistent,
    )


def governance_check_doc_lifecycle() -> CheckResult:
    """第 7 项: .omo/ 文档生命周期健康度 (P45 R3).

    检查项:
    - frontmatter 覆盖率 (ssot/contract/pattern) >= 80% 满分
    - 死文档占比 (contract/pattern 0 引用) < 30% 满分
    - 矛盾路径 (代码引用 .omo/_archive/) = 0 满分
    """
    from omo.omo_lint import (
        _DOC_LIFECYCLE_NEED_FRONTMATTER,
        _check_doc_referenced,
        _classify_doc,
        _parse_frontmatter,
    )

    omo = _audit_mod._WORKSPACE_ROOT / ".omo"
    if not omo.exists():
        return CheckResult(
            name="doc lifecycle",
            category="knowledge",
            severity="warn",
            score=50.0,
            message=".omo/ not found",
        )

    md_files = [f for f in omo.rglob("*.md")] + [f for f in omo.rglob("*.yaml")]
    md_files = [f for f in md_files if "_delivery" not in f.parts and "/drafts/" not in str(f)]

    need_fm_total = 0
    frontmatter_active = 0
    frontmatter_missing = 0
    dead_docs = 0
    contradictory_refs = 0

    for f in md_files:
        try:
            rel = str(f.relative_to(_audit_mod._WORKSPACE_ROOT))
        except ValueError:
            continue
        category = _classify_doc(rel)
        if category in _DOC_LIFECYCLE_NEED_FRONTMATTER:
            need_fm_total += 1
            try:
                content = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:  # defensive fallback
                continue
            fm = _parse_frontmatter(content)
            if fm and fm.get("status") in {
                "active",
                "deprecated",
                "archived",
                "experimental",
            }:
                frontmatter_active += 1
            else:
                frontmatter_missing += 1
            if category in {"contract", "pattern"}:
                has_ref, _ = _check_doc_referenced(rel, _audit_mod._WORKSPACE_ROOT)
                if not has_ref:
                    # 如果 frontmatter 标了 deprecated/archived, 不算死
                    try:
                        content = f.read_text(encoding="utf-8", errors="ignore")
                        fm = _parse_frontmatter(content)
                    except Exception:  # defensive fallback
                        fm = None
                    if fm and fm.get("status") in {"deprecated", "archived"}:
                        pass  # 已标注, OK
                    else:
                        dead_docs += 1
        # 矛盾路径: 只对 .py/.sh 真实代码引用算
        if f.suffix in {".py", ".sh"}:
            try:
                content = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:  # defensive fallback
                continue
            if ".omo/_archive/" in content or ".omo/_knowledge/management/" in content:
                contradictory_refs += 1

    # 评分
    score = 100.0
    if need_fm_total > 0:
        fm_coverage = frontmatter_active / need_fm_total * 100
        if fm_coverage < 80:
            score -= int((80 - fm_coverage) * 0.5)
    if dead_docs > 0:
        dead_ratio = dead_docs / max(need_fm_total, 1) * 100
        if dead_ratio > 30:
            score -= 20
        elif dead_ratio > 20:
            score -= 10
    if contradictory_refs > 0:
        score -= min(contradictory_refs, 30)  # 每个扣 1, 上限 30
    score = max(0.0, float(score))

    if score >= 90:
        severity = "ok"
    elif score >= 70:
        severity = "warn"
    else:
        severity = "fail"

    message = (
        f"frontmatter {frontmatter_active}/{need_fm_total} ({frontmatter_active / max(need_fm_total, 1) * 100:.0f}%), "
        f"dead docs {dead_docs}, contradictory {contradictory_refs}"
    )

    return CheckResult(
        name="doc lifecycle",
        category="knowledge",
        severity=severity,
        score=score,
        message=message[:120],
    )


def governance_check_agora_health() -> CheckResult:
    """第 6 项: agora 路由 -> 服务真实可达率(>=80% = 满分).

    单次 audit 默认会跑(HTTP 探活),
    daemon 跑时设 OMO_AUDIT_SKIP_AGORA=1 跳过.
    """
    if os.environ.get(_audit_mod.ENV_SKIP_AGORA) == "1":
        return CheckResult(
            name="agora health",
            category="agora",
            severity="ok",
            score=100.0,
            message="skipped (OMO_AUDIT_SKIP_AGORA=1)",
        )

    try:
        from omo.omo_health import (
            check_all_health,
            derive_endpoints,
            load_agora_routes,
        )
    except ImportError as exc:
        return CheckResult(
            name="agora health",
            category="agora",
            severity="fail",
            score=0.0,
            message=f"omo_health import failed: {exc}"[:120],
        )

    try:
        routes = load_agora_routes()
        endpoints = derive_endpoints(routes)
        if not endpoints:
            return CheckResult(
                name="agora health",
                category="agora",
                severity="warn",
                score=50.0,
                message="no endpoints discoverable",
            )
        import asyncio
        import threading

        coro = check_all_health(endpoints)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            res_container = []

            def _run_in_thread():
                res_container.append(asyncio.run(coro))

            t = threading.Thread(target=_run_in_thread)
            t.start()
            t.join()
            results = res_container[0]
        else:
            results = asyncio.run(coro)
        if not results:
            return CheckResult(
                name="agora health",
                category="agora",
                severity="warn",
                score=50.0,
                message="0 endpoints probed",
            )
        healthy_n = sum(1 for r in results if r.is_healthy)
        rate = healthy_n / len(results)
        score = round(rate * 100, 0)
        if score >= 80:
            severity: Severity = "ok"
        elif score >= 50:
            severity = "warn"
        else:
            severity = "fail"
        unhealthy = [r.service for r in results if not r.is_healthy][:5]
        return CheckResult(
            name="agora health",
            category="agora",
            severity=severity,
            score=score,
            message=f"{healthy_n}/{len(results)} services healthy",
            details=unhealthy,
        )
    except Exception as exc:  # defensive fallback
        return CheckResult(
            name="agora health",
            category="agora",
            severity="fail",
            score=0.0,
            message=f"probe failed: {str(exc)[:100]}",
        )
