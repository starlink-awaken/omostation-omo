#!/usr/bin/env python3
"""omo.resident.epiphany — 会话即时提炼与自进化飞轮 (Phase 4 Sprint 2).

在夏明星与常驻 Agent 交互结束或触发纠错时，
调用本地模型自动萃取用户在会话中的核心指令偏好、技术红线与架构共识，
生成待签署的候选记忆卡片推入 Action Desk，实现主权操作系统的自主进化。
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from omo.omo_paths import code_root
from omo.resident import WORKSPACE, write_path

logger = logging.getLogger("omo.resident.epiphany")

CARDS_DIR = Path.home() / ".local/share/omostation/cards"
MEMORY_CANDIDATES_DIR = WORKSPACE / ".omo/state/memory/candidates"

CORRECTION_SIGNALS = [
    "不要",
    "严禁",
    "必须",
    "改成",
    "改用",
    "优先",
    "遵循",
    "不能",
    "禁止",
    "别",
    "统一使用",
    "按规范",
    "注意",
]


def detect_correction_signals(text: str) -> bool:
    """快速启发式探测会话文本中是否含有纠错或指令偏好特征."""
    return any(sig in text for sig in CORRECTION_SIGNALS)


def distill_epiphany_from_text(
    transcript: str,
    session_id: str | None = None,
    force: bool = False,
) -> dict[str, Any] | None:
    """从会话记录中提炼出结构化经验卡片."""
    if not transcript.strip():
        return None

    if not force and not detect_correction_signals(transcript):
        return None

    # 1. 尝试调用本地重模型 (LM Studio :1234 或 AetherForge :4000)
    from omo.resident.interact import _invoke_llm_endpoint, _probe

    prompt = (
        "你是夏明星主权操作系统的认知提炼秘书。请审阅以下夏明星与系统的对话转写记录，"
        "识别夏明星表达的技术偏好、架构禁忌、规范纠错或防御规则。\n\n"
        f"【对话记录】\n{transcript[-2000:]}\n\n"
        '请严格按以下 JSON 格式输出且仅输出合法 JSON，若无任何实质偏好或纠错，输出 {"valid": false}：\n'
        "{\n"
        '  "valid": true,\n'
        '  "category": "user_preference" 或 "pitfall",\n'
        '  "title": "简明规则标题 (如: 回复优先使用简体中文)",\n'
        '  "summary": "核心一句话结论",\n'
        '  "details": "具体的防御细节、禁止事项与实证背景"\n'
        "}"
    )

    extracted_data = None
    messages = [{"role": "user", "content": prompt}]

    # 调动 MBP 128G 本地模型
    if _probe(1234):
        raw = _invoke_llm_endpoint(1234, "qwen3.6-35b-a3b-splash", messages, timeout=20, stream=False)
        if raw:
            try:
                # 提取 JSON 块
                m = re.search(r"\{.*\}", raw, re.DOTALL)
                if m:
                    extracted_data = json.loads(m.group(0))
            except Exception:
                pass

    # 兜底规则抽取 (当本地大模型未在线时)
    if not extracted_data or not extracted_data.get("valid"):
        lines = [line_str.strip() for line_str in transcript.splitlines() if line_str.strip()]
        matched_lines = [line_str for line_str in lines if any(s in line_str for s in CORRECTION_SIGNALS)][:3]
        if not matched_lines and force and lines:
            matched_lines = lines[:1]
        if matched_lines:
            extracted_data = {
                "valid": True,
                "category": "user_preference",
                "title": f"会话备忘: {matched_lines[0][:20]}",
                "summary": matched_lines[0],
                "details": "\n".join(f"- {line_str}" for line_str in matched_lines),
            }

    if not extracted_data or not extracted_data.get("valid"):
        return None

    now_ts = int(time.time())
    sess = session_id or f"sess-{now_ts}"
    card_id = f"epiphany-{sess}"

    card = {
        "id": card_id,
        "type": "memory_candidate",
        "title": extracted_data.get("title", "未命名会话偏好"),
        "category": extracted_data.get("category", "user_preference"),
        "summary": extracted_data.get("summary", ""),
        "details": extracted_data.get("details", ""),
        "status": "pending_approval",
        "created_at": datetime.now(UTC).isoformat(),
        "created_by": "session-epiphany",
        "session_id": sess,
    }

    # 智能去重：检查 Action Desk 中是否已有高度相似的待审卡片
    if CARDS_DIR.is_dir():
        for existing_file in CARDS_DIR.glob("*.json"):
            try:
                ex = json.loads(existing_file.read_text(encoding="utf-8"))
                if ex.get("status") == "pending_approval" and ex.get("type") == "memory_candidate":
                    ex_title = ex.get("title", "")
                    cur_title = card.get("title", "")
                    if (
                        ex_title
                        and cur_title
                        and (ex_title == cur_title or cur_title in ex_title or ex_title in cur_title)
                    ):
                        card["id"] = ex.get("id", card_id)
                        card_id = card["id"]
                        break
            except Exception:
                pass

    # 2. 持久化至候选目录与 Action Desk 卡片
    try:
        cand_dir = write_path(MEMORY_CANDIDATES_DIR)
        cand_dir.mkdir(parents=True, exist_ok=True)
        (cand_dir / f"{card_id}.json").write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to save candidate memory: %s", exc)

    try:
        CARDS_DIR.mkdir(parents=True, exist_ok=True)
        (CARDS_DIR / f"{card_id}.json").write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to save Action Desk card: %s", exc)

    return card


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="会话即时提炼与自进化飞轮 CLI")
    parser.add_argument("text", nargs="?", default="", help="要提炼的会话文本或纠错指令")
    parser.add_argument("--session", "-s", dest="session_id", help="会话 Session ID")
    parser.add_argument("--force", "-f", action="store_true", help="强制提炼，忽略启发式信号过滤")
    parser.add_argument("--file", dest="file_path", help="从指定文件读取会话记录")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")

    args = parser.parse_args(argv)
    text = args.text
    if args.file_path and Path(args.file_path).exists():
        text = Path(args.file_path).read_text(encoding="utf-8")

    if not text.strip():
        parser.print_help()
        return 1

    card = distill_epiphany_from_text(
        transcript=text.strip(),
        session_id=args.session_id,
        force=args.force,
    )

    if not card:
        if args.json:
            print(json.dumps({"ok": False, "reason": "未在会话中检测到高置信度纠错或技术偏好"}, ensure_ascii=False))
        else:
            print("\n\033[33mℹ️  本轮会话未检测到需要沉淀的偏好或纠错规则。\033[0m\n")
        return 0

    if args.json:
        print(json.dumps({"ok": True, "card": card}, ensure_ascii=False, indent=2))
    else:
        print("\n\033[1;32m✨ 会话即时经验提炼完成\033[0m")
        print(f"📌 卡片 ID: \033[36m{card['id']}\033[0m")
        print(f"🏷️  类别: {card['category']} · 标题: \033[1m{card['title']}\033[0m")
        print(f"🎯 核心论断: {card['summary']}")
        print("\n💡 \033[32m已推入 SwiftBar Action Desk，点击 Approve 可自动合入 LifeOS 权威记忆。\033[0m\n")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
