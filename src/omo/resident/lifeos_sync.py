#!/usr/bin/env python3
"""omo.resident.lifeos_sync — Action Desk 签署与 LifeOS 7.40.4 权威记忆注入流 (Phase 3 Sprint 2).

负责将 SwiftBar Action Desk 或会话萃取的经验偏好卡片，
在夏明星签署后合流进 LifeOS 权威记忆库，严格守护 200 行 / 40000 字符索引预算。
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("omo.resident.lifeos_sync")

MEMORY_DIR = Path("/Users/xiamingxing/.claude/projects/-Users-xiamingxing-Workspace/memory")
INDEX_FILE = MEMORY_DIR / "MEMORY.md"
LIFEOS_CORE_DIR = Path("/Users/xiamingxing/.claude/LIFEOS/MEMORY")
CARDS_DIR = Path.home() / ".local/share/omostation/cards"

MAX_INDEX_LINES = 192  # 200 行上限扣除结构行
MAX_INDEX_CHARS = 40000


def inspect_memory_budget() -> dict[str, Any]:
    """检查 MEMORY.md 索引的行数与字符数硬限水位."""
    if not INDEX_FILE.exists():
        return {
            "ok": True,
            "exists": False,
            "line_count": 0,
            "char_count": 0,
            "lines_remaining": MAX_INDEX_LINES,
            "chars_remaining": MAX_INDEX_CHARS,
        }

    content = INDEX_FILE.read_text(encoding="utf-8")
    lines = content.splitlines()
    line_count = len(lines)
    char_count = len(content)

    return {
        "ok": line_count <= MAX_INDEX_LINES and char_count <= MAX_INDEX_CHARS,
        "exists": True,
        "line_count": line_count,
        "char_count": char_count,
        "max_lines": MAX_INDEX_LINES,
        "max_chars": MAX_INDEX_CHARS,
        "lines_remaining": max(0, MAX_INDEX_LINES - line_count),
        "chars_remaining": max(0, MAX_INDEX_CHARS - char_count),
    }


def find_related_topic_file(category: str, keywords: list[str]) -> Path | None:
    """在现有记忆文件中搜寻同族主题文件，避免为每个碎点创建新文件导致索引膨胀."""
    if not MEMORY_DIR.is_dir():
        return None

    # 1. 常见固定主题映射
    fixed_mapping = {
        "user_preference": MEMORY_DIR / "feedback-chinese-first.md",
        "swiftbar": MEMORY_DIR / "launchagents-governed-check.md",
        "model": MEMORY_DIR / "local-model-tiering-2026-09.md",
        "agent": MEMORY_DIR / "agent-secrets-hygiene.md",
        "ci": MEMORY_DIR / "ci-fix-recursive-layers.md",
    }
    for k, p in fixed_mapping.items():
        if k in category.lower() and p.exists():
            return p

    # 2. 关键词模糊匹配
    for f in MEMORY_DIR.glob("*.md"):
        if f.name == "MEMORY.md":
            continue
        stem = f.stem.lower()
        if any(kw.lower() in stem for kw in keywords if len(kw) >= 3):
            return f

    return None


def merge_belief_into_topic_file(
    topic_path: Path,
    title: str,
    summary: str,
    details: str,
    date_str: str,
) -> bool:
    """以标准追加条目的形式合流进已有主题文件."""
    try:
        content = topic_path.read_text(encoding="utf-8")
        entry_text = f"\n\n### 补充记忆: {title} ({date_str})\n- **核心结论**: {summary}\n- **实证约束**: {details}\n"
        topic_path.write_text(content + entry_text, encoding="utf-8")
        return True
    except Exception as exc:
        logger.warning("Failed to merge into %s: %s", topic_path, exc)
        return False


def create_new_topic_file(
    slug: str,
    title: str,
    summary: str,
    details: str,
    date_str: str,
) -> tuple[bool, Path]:
    """创建全新独立主题文件并合规挂载至 MEMORY.md 索引."""
    budget = inspect_memory_budget()
    if not budget["ok"] or budget["lines_remaining"] < 1:
        raise ValueError(f"MEMORY.md 超出硬限预算 (已用 {budget['line_count']}/{MAX_INDEX_LINES} 行)，阻断新建文件")

    topic_path = MEMORY_DIR / f"{slug}.md"
    body = (
        f"# {title}\n\n"
        f"- **生效日期**: {date_str}\n"
        f"- **类别**: 主权系统权威记忆\n"
        f"- **核心结论**: {summary}\n\n"
        f"## 详细约定与防御规则\n"
        f"{details}\n"
    )
    topic_path.write_text(body, encoding="utf-8")

    # 更新 MEMORY.md 索引
    index_content = INDEX_FILE.read_text(encoding="utf-8").strip()
    new_index_line = f"- [{title}]({slug}.md) — {summary}"
    INDEX_FILE.write_text(index_content + "\n" + new_index_line + "\n", encoding="utf-8")
    return True, topic_path


def sync_card_to_lifeos(card_file: Path) -> dict[str, Any]:
    """主注入逻辑: 读取待审卡片 -> 预算校验 -> 智能合流 -> 标记已批准."""
    if not card_file.exists():
        return {"ok": False, "error": f"Card file not found: {card_file}"}

    card = json.loads(card_file.read_text(encoding="utf-8"))
    title = card.get("title", "未命名偏好")
    category = card.get("category", "user_preference")
    summary = card.get("summary", "")
    details = card.get("details", "")
    date_str = card.get("created_at", datetime.now(UTC).isoformat())[:10]

    # 1. 预算检查
    budget = inspect_memory_budget()
    if not budget["ok"]:
        return {"ok": False, "error": "LifeOS MEMORY.md index budget exceeded"}

    # 2. 寻找同族文件合流
    keywords = [k for k in (category, title) if isinstance(k, str)]
    target_file = find_related_topic_file(category, keywords)

    operation = "merged_existing"
    if target_file and target_file.exists():
        merge_belief_into_topic_file(target_file, title, summary, details, date_str)
        dest_path = str(target_file)
    else:
        # 新建主题文件
        slug = f"rule-{int(datetime.now().timestamp())}"
        _, new_path = create_new_topic_file(slug, title, summary, details, date_str)
        dest_path = str(new_path)
        operation = "created_new_topic"

    # 3. 更新卡片状态为已签署
    card["status"] = "approved"
    card["synced_at"] = datetime.now(UTC).isoformat()
    card["synced_target"] = dest_path
    card["sync_operation"] = operation
    card_file.write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "operation": operation,
        "target_file": dest_path,
        "title": title,
        "budget": inspect_memory_budget(),
    }


def approve_pending_card(card_id: str) -> dict[str, Any]:
    """根据 card_id 执行一键批准与记忆闭环注入."""
    target_card = None
    for f in CARDS_DIR.glob("*.json"):
        if f.stem == card_id or f.name == f"{card_id}.json" or card_id in f.stem:
            target_card = f
            break

    if not target_card:
        return {"ok": False, "error": f"Card ID not found: {card_id}"}

    return sync_card_to_lifeos(target_card)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LifeOS 7.40.4 权威记忆注入与预算守护 CLI")
    parser.add_argument("--budget", action="store_true", help="检查 MEMORY.md 索引容量预算")
    parser.add_argument("--approve", dest="approve_id", help="一键批准并注入指定 card_id")
    parser.add_argument("--card", dest="card_path", help="指定卡片文件直接执行同步")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")

    args = parser.parse_args(argv)

    if args.budget:
        b = inspect_memory_budget()
        print(
            json.dumps(b, ensure_ascii=False, indent=2)
            if args.json
            else f"MEMORY.md 水位: {b['line_count']}/{b['max_lines']} 行, {b['char_count']}/{b['max_chars']} 字符 (余量 {b['lines_remaining']} 行)"
        )
        return 0

    if args.approve_id:
        res = approve_pending_card(args.approve_id)
        if args.json:
            print(json.dumps(res, ensure_ascii=False, indent=2))
        else:
            if res.get("ok"):
                print("\n\033[1;32m✅ 卡片已批准并成功注入 LifeOS 权威记忆\033[0m")
                print(f"📄 目标文件: {res['target_file']}")
                print(f"⚙️ 操作模式: {res['operation']}")
                b = res.get("budget", {})
                print(f"📊 索引水位: {b.get('line_count')}/{b.get('max_lines')} 行\n")
            else:
                print(f"\n\033[1;31m❌ 注入失败: {res.get('error')}\033[0m\n")
        return 0 if res.get("ok") else 1

    if args.card_path:
        res = sync_card_to_lifeos(Path(args.card_path))
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if res.get("ok") else 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
