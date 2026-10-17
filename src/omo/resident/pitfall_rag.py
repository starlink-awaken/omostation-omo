#!/usr/bin/env python3
"""omo.resident.pitfall_rag — 主权记忆逆向反哺与避坑防御引擎 (Phase 5 Sprint 1).

负责在 TaskObject 派发与执行前，逆向检索 LifeOS 权威记忆与 AGENTS.md 避坑守则，
提取防御性硬约束 (Negative Constraints) 注入执行封套；
并在代码修改后执行轻量级静态后验检查，杜绝高危模式复发。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from omo.omo_paths import code_root

logger = logging.getLogger("omo.resident.pitfall_rag")

MEMORY_DIR = Path("/Users/xiamingxing/.claude/projects/-Users-xiamingxing-Workspace/memory")
AGENTS_MD_PATH = code_root() / "AGENTS.md"

# 静态内置高频主权硬纪律 (兜底防丢)
CANONICAL_PITFALLS: list[dict[str, Any]] = [
    {
        "id": "pitfall-zsh-word-split",
        "keywords": ["zsh", "bash", "shell", "sh", "循环", "遍历", "for", "split"],
        "path_patterns": [r"\.sh$", r"bin/.*", r"dotfiles/.*"],
        "title": "Bash/Zsh 参数展开禁止字面切词",
        "rule": '【严禁】在 zsh 中使用 `VAR="a b c"; for x in $VAR`，zsh 对参数展开不做切词导致只循环 1 次；必须使用字面量列表 `for x in a b c`、数组或命令替换 `$(printf ...)`。',
        "source": "AGENTS.md § 7. Common Pitfalls",
    },
    {
        "id": "pitfall-swiftbar-domain-emoji",
        "keywords": ["swiftbar", "menubar", "status bar", "状态栏", "hub.30s.sh", "action desk", "emoji"],
        "path_patterns": [r"dotfiles/menubar/.*", r".*hub\.30s\.sh.*"],
        "title": "SwiftBar 7 域保留 Emoji 禁用于行首",
        "rule": "【严禁】在 SwiftBar 行首使用保留 emoji (🧠|🌐|🔀|🕸|⚙️|🤖|✅)，否则会被 bar-lint.sh 误判为第 8 个主权域；任务图标使用 🐝 或 ⚡️ 替代。",
        "source": "dotfiles/menubar/bar-lint.sh & PITFALL-MENUBAR-001",
    },
    {
        "id": "pitfall-adr0456-double-root",
        "keywords": ["root", "workspace", "repo_root", "state_root", "write_path", "路径", "双根", "硬编码"],
        "path_patterns": [r".*\.py$"],
        "title": "ADR-0456 双根契约严禁硬编码 Workspace",
        "rule": "【严禁】在 Python 代码中硬编码 `/Users/.../Workspace`，读面必须走 `code_root()`，写面必须走 `write_path()` 或 `state_root()`。",
        "source": "ADR-0456 / tests/unit/test_repo_root_profile.py",
    },
    {
        "id": "pitfall-memory-budget-limit",
        "keywords": ["memory", "lifeos", "topic", "memory.md", "记忆", "索引", "行数"],
        "path_patterns": [r".*memory.*", r".*\.md$"],
        "title": "LifeOS MEMORY.md 索引 200 行硬上限",
        "rule": "【严禁】为琐碎知识频繁新建 .md 文件；MEMORY.md 索引严格限制 200 行 / 40000 字符，新经验必须优先合并进已有同族主题文件。",
        "source": "AGENTS.md § 7 / MEMORY.md Contract",
    },
    {
        "id": "pitfall-worktree-isolation",
        "keywords": ["worktree", "commit", "push", "main", "主树", "隔离", "分支", "git"],
        "path_patterns": [r".*"],
        "title": "主工作区物理只读与隔离工作树强制",
        "rule": "【严禁】在主工作区 (PASW) 直接修改代码或执行 git commit/push；所有代码交付必须在 `gac-worktree.sh claim` 建立的隔离工作树中完成并通过测试。",
        "source": "GOVERNANCE.md & AGENTS.md § 0",
    },
]


def load_memory_topic_pitfalls() -> list[dict[str, Any]]:
    """轻量动态扫描 memory/ 目录中的避坑与经验专题文件 (<10ms)."""
    results: list[dict[str, Any]] = []
    if not MEMORY_DIR.is_dir():
        return results

    try:
        for f in MEMORY_DIR.glob("*.md"):
            if f.name == "MEMORY.md":
                continue
            stem = f.stem.lower()
            # 过滤含经验/故障/避坑/规约属性的文件
            if any(
                k in stem
                for k in ("pitfall", "bug", "lesson", "rootcause", "hygiene", "mismatch", "rule", "tiering", "failover")
            ):
                try:
                    text = f.read_text(encoding="utf-8")
                    title = f.stem
                    first_line = text.splitlines()[0] if text.splitlines() else f.stem
                    if first_line.startswith("#"):
                        title = first_line.lstrip("#").strip()
                    # 提取核心结论或首段
                    summary_match = re.search(r"-\s*\*\*核心结论\*\*:\s*([^\n]+)", text)
                    summary = summary_match.group(1).strip() if summary_match else ""
                    if not summary:
                        summary = text[:200].replace("\n", " ").strip()

                    keywords = re.findall(r"[a-zA-Z0-9_-]{3,}", stem)
                    results.append(
                        {
                            "id": f"mem-{f.stem}",
                            "keywords": keywords,
                            "path_patterns": [],
                            "title": title,
                            "rule": f"【实证经验】{title}: {summary[:120]}",
                            "source": f"memory/{f.name}",
                        }
                    )
                except Exception as exc:
                    logger.debug("Failed reading memory file %s: %s", f, exc)
    except Exception as exc:
        logger.warning("Error scanning memory directory: %s", exc)

    return results


def extract_relevant_pitfalls(
    objective: str,
    target_paths: list[str] | None = None,
    max_rules: int = 3,
) -> list[dict[str, Any]]:
    """根据目标意图与目标文件路径，精准匹配 Top-K 防御性硬约束 (Negative Constraints)."""
    candidates = list(CANONICAL_PITFALLS)
    candidates.extend(load_memory_topic_pitfalls())

    target_paths = target_paths or []
    obj_lower = objective.lower()
    scored_pitfalls: list[tuple[int, dict[str, Any]]] = []

    for item in candidates:
        score = 0
        # 1. 意图关键词匹配
        for kw in item.get("keywords", []):
            if kw.lower() in obj_lower:
                score += 3

        # 2. 目标路径模式匹配
        for pattern in item.get("path_patterns", []):
            for p in target_paths:
                if re.search(pattern, str(p), re.IGNORECASE):
                    score += 5

        # 3. 意图显式提到了标题名词
        if item.get("title", "").lower() in obj_lower:
            score += 4

        if score > 0:
            scored_pitfalls.append((score, item))

    # 按匹配得分降序排列
    scored_pitfalls.sort(key=lambda x: x[0], reverse=True)

    # 去重提取 Top-K
    seen_ids = set()
    selected: list[dict[str, Any]] = []
    for _, item in scored_pitfalls:
        if item["id"] not in seen_ids:
            seen_ids.add(item["id"])
            selected.append(item)
            if len(selected) >= max_rules:
                break

    # 兜底：若没有任何匹配，至少附带主工作区只读铁律
    if not selected:
        selected.append(CANONICAL_PITFALLS[4])

    return selected


def verify_code_against_pitfalls(
    diff_text: str,
    file_paths: list[str] | None = None,
) -> list[str]:
    """轻量级静态后验检查器：检查被修改的代码是否违反已知的致命踩坑模式."""
    violations: list[str] = []
    file_paths = file_paths or []

    # 1. 扫描 zsh/bash 参数切词陷阱
    # 典型违规: for x in $VAR; 或 for item in $list; (且未加引号或命令替换)
    shell_files = [p for p in file_paths if str(p).endswith(".sh")]
    if shell_files or "bin/" in diff_text or "menubar" in diff_text:
        for line in diff_text.splitlines():
            if line.startswith("+") and not line.startswith("+++"):
                added_code = line[1:].strip()
                # 检查 for ... in $VAR 且不是 $(...) 或 "${...}"
                if re.search(r"\bfor\s+\w+\s+in\s+\$[A-Za-z0-9_]+\s*(?:;|\bdo)", added_code):
                    if "$(" not in added_code and '"$' not in added_code:
                        violations.append(
                            f"【违反 zsh 参数切词纪律】发现裸变量展开循环: `{added_code}`。zsh 下参数展开不做切词只循环1次！必须使用字面量列表、命令替换或数组。"
                        )

    # 2. 扫描 ADR-0456 双根硬编码违规
    for line in diff_text.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            added_code = line[1:].strip()
            # 排除注释行与文档
            if not added_code.startswith("#") and not added_code.startswith("//"):
                if '"/Users/xiamingxing/Workspace"' in added_code or "'/Users/xiamingxing/Workspace'" in added_code:
                    violations.append(
                        f"【违反 ADR-0456 双根契约】发现硬编码工作区路径: `{added_code}`。严禁字面量路径，必须使用 `code_root()` / `write_path()` 解析。"
                    )

    # 3. 扫描 SwiftBar 7 域保留 Emoji 违规
    is_menubar_diff = (
        any("menubar" in str(p) or "hub.30s.sh" in str(p) for p in file_paths) or "hub.30s.sh" in diff_text
    )
    if is_menubar_diff:
        reserved_emojis = ("🧠", "🌐", "🔀", "🕸", "⚙️", "🤖", "✅")
        for line in diff_text.splitlines():
            if line.startswith("+") and not line.startswith("+++"):
                added_code = line[1:].strip()
                # 检查 echo "🤖 ..." 或直接以保留 emoji 开头
                for emoji in reserved_emojis:
                    if re.search(rf'echo\s+["\']?{re.escape(emoji)}\s', added_code) or added_code.startswith(
                        f"{emoji} "
                    ):
                        violations.append(
                            f"【违反 SwiftBar 域正则】新增行使用了保留 emoji `{emoji}` 作为行首: `{added_code}`。这会导致 bar-lint.sh 识别异常，请更换为 🐝 / ⚡️ 等非保留符号。"
                        )

    # 4. 扫描动态自蒸馏的确定性防御规则 (Phase 7 动态防线)
    dynamic_rules = [r for r in load_dynamic_pitfalls() if r.get("status") == "active" and r.get("pattern")]
    for rule in dynamic_rules:
        pattern = rule.get("pattern", "")
        regex_pat = rule.get("validation_regex") or re.escape(pattern)
        try:
            for line in diff_text.splitlines():
                if line.startswith("+") and not line.startswith("+++"):
                    added_code = line[1:].strip()
                    if re.search(regex_pat, added_code):
                        violations.append(
                            f"【违反动态自蒸馏防御硬约束: {rule.get('id', 'rule')}】{rule.get('title', '违规模式')}: `{added_code}`。处置对策: {rule.get('remedy', '')}"
                        )
        except Exception as exc:
            logger.debug("Regex match error for dynamic rule %s: %s", rule.get("id"), exc)

    return violations


# ── Phase 7: 动态避坑记忆自蒸馏与知识库热加载 ──

DYNAMIC_PITFALLS_DIR = code_root() / ".omo/state/memory/pitfalls"


def load_dynamic_pitfalls() -> list[dict[str, Any]]:
    """扫描并热加载所有动态自蒸馏出的规则卡片."""
    rules = []
    if not DYNAMIC_PITFALLS_DIR.exists():
        return rules
    try:
        import yaml

        for f in DYNAMIC_PITFALLS_DIR.glob("*.yaml"):
            try:
                data = yaml.safe_load(f.read_text(encoding="utf-8"))
                if isinstance(data, dict) and "id" in data:
                    data["source"] = f"dynamic/{f.name}"
                    rules.append(data)
            except Exception as exc:
                logger.debug("Failed reading dynamic pitfall file %s: %s", f, exc)
    except Exception as exc:
        logger.warning("Error scanning dynamic pitfalls directory: %s", exc)
    return rules


def get_high_priority_pitfalls() -> list[dict[str, Any]]:
    """聚合内置核心硬纪律与动态自蒸馏出的最新规则，供驾驶舱与守卫展示."""
    # 转换为统一的格式
    results = []
    # 1. 内置基础硬规则
    for item in CANONICAL_PITFALLS:
        results.append(
            {
                "id": item["id"],
                "title": item["title"],
                "domain": "governance",
                "severity": "CRITICAL" if "word-split" in item["id"] or "root" in item["id"] else "HIGH",
                "pattern": item["rule"][:100],
                "remedy": item["rule"],
                "source_file": item.get("source", "AGENTS.md"),
                "status": "active",
            }
        )

    # 2. 动态自蒸馏规则 (最新热增优先)
    for d in load_dynamic_pitfalls():
        results.insert(
            0,
            {
                "id": d.get("id"),
                "title": d.get("title", ""),
                "domain": d.get("domain", "system"),
                "severity": d.get("severity", "HIGH"),
                "pattern": d.get("pattern", ""),
                "remedy": d.get("remedy", ""),
                "source_file": d.get("source", "dynamic"),
                "status": d.get("status", "active"),
                "is_deterministic": d.get("is_deterministic", True),
            },
        )
    return results


def distill_pitfall_from_diff(
    error_log: str,
    diff_text: str = "",
    context: str = "",
) -> dict[str, Any]:
    """从报错日志与修复补丁中，提炼出标准的结构化 Anti-pattern 规则 (Phase 7).

    自动判定确定性 (is_deterministic):
    - 确定性特征 (精确报错+特定命令/语句错配): status="active", 参与静态守卫硬拦截;
    - 模糊/启发式特征: status="shadow", 仅在雷达大屏记录告警，不强行阻断。
    """
    error_lower = error_log.lower()
    diff_lower = diff_text.lower()

    # 典型模式 1: 子仓构建 runtime 错配 (如 npm vs bun)
    if "edgesout" in error_lower or ("npm install" in diff_lower and "bun.lock" in diff_lower):
        rule_id = "pitfall-distilled-npm-bun-lock-mismatch"
        title = "子模块使用 bun.lock 时严禁在主仓 CI 中使用 npm"
        domain = "ci/build"
        severity = "HIGH"
        pattern = "npm install"
        remedy = "子仓原生使用 bun 锁文件时，主仓 CI 必须对齐使用 oven-sh/setup-bun@v2 + bun install"
        regex = r"\bcd\s+projects/[a-zA-Z0-9_-]+\s*&&\s*npm\s+(install|i)"
        is_deterministic = True

    # 典型模式 2: zsh 参数裸变量展开
    elif "wordsplit" in error_lower or re.search(r"\bfor\s+\w+\s+in\s+\$[A-Za-z0-9_]+", diff_text):
        rule_id = "pitfall-distilled-zsh-wordsplit"
        title = "zsh 裸变量切词崩溃禁令"
        domain = "shell"
        severity = "CRITICAL"
        pattern = "for x in $VAR"
        remedy = "改用字面量列表或命令替换 $(printf ...) 或数组"
        regex = r"\bfor\s+\w+\s+in\s+\$[A-Za-z0-9_]+\s*(?:;|\bdo)"
        is_deterministic = True

    # 典型模式 3: 工作区双根硬编码
    elif "/users/xiamingxing/workspace" in diff_lower and "code_root" in diff_lower:
        rule_id = "pitfall-distilled-workspace-hardcoding"
        title = "严禁硬编码 Workspace 字面量路径"
        domain = "python/paths"
        severity = "HIGH"
        pattern = "/Users/xiamingxing/Workspace"
        remedy = "使用 omo.omo_paths.code_root() 或 state_root() 动态解析"
        regex = r"['\"]/Users/[a-zA-Z0-9_.-]+/Workspace"
        is_deterministic = True

    # 默认/通用提炼 (影子模式)
    else:
        # 生成基于错误前 40 字符的通用规则
        first_line = error_log.strip().splitlines()[0] if error_log.strip() else "未知系统执行异常"
        clean_name = re.sub(r"[^a-zA-Z0-9]+", "-", first_line[:30].strip()).strip("-").lower() or "generic-anomaly"
        rule_id = f"pitfall-distilled-{clean_name}"
        title = f"排障沉淀: {first_line[:50]}"
        domain = "runtime"
        severity = "MEDIUM"
        pattern = first_line[:80]
        remedy = f"关注此模式并在修改时参考 context: {context[:100]}"
        regex = ""
        is_deterministic = False

    status = "active" if is_deterministic else "shadow"

    return {
        "id": rule_id,
        "title": title,
        "domain": domain,
        "severity": severity,
        "pattern": pattern,
        "remedy": remedy,
        "validation_regex": regex,
        "is_deterministic": is_deterministic,
        "status": status,
        "evidence_context": context[:200],
    }


def save_distilled_pitfall(rule: dict[str, Any]) -> Path:
    """持久化自蒸馏规则卡片至 .omo/state/memory/pitfalls/."""
    DYNAMIC_PITFALLS_DIR.mkdir(parents=True, exist_ok=True)
    rule_id = rule.get("id", "pitfall-unnamed")
    target_path = DYNAMIC_PITFALLS_DIR / f"{rule_id}.yaml"

    import yaml

    target_path.write_text(yaml.safe_dump(rule, allow_unicode=True, sort_keys=False), encoding="utf-8")
    logger.info("Successfully persisted distilled pitfall rule: %s", target_path)
    return target_path
