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

    return violations
