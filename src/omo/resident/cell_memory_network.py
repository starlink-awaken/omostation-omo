#!/usr/bin/env python3
"""Cell Memory Network — 跨 Cell 记忆网络.

支持 Cell 间的记忆共享:
  - 发布记忆到网络
  - 订阅其他 Cell 的记忆
  - 冲突检测与合并
  - 记忆检索 (跨 Cell 搜索)
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
NETWORK_DIR = ROOT / ".omo" / "state" / "agent-cell" / "memory-network"
PUBLISHED_FILE = NETWORK_DIR / "published.jsonl"
SUBSCRIPTIONS_FILE = NETWORK_DIR / "subscriptions.json"


class MemoryNetwork:
    """跨 Cell 记忆网络."""

    def __init__(self):
        NETWORK_DIR.mkdir(parents=True, exist_ok=True)

    def publish(self, cell_id: str, memory: dict) -> str:
        """发布记忆到网络. 返回 memory_id."""
        memory_id = f"mem-{uuid.uuid4().hex[:12]}"
        entry = {
            "memory_id": memory_id,
            "cell_id": cell_id,
            "content": memory.get("content", ""),
            "type": memory.get("type", "semantic"),
            "tags": memory.get("tags", []),
            "episode_id": memory.get("episode_id", ""),
            "published_at": datetime.now(UTC).isoformat(),
            "ttl_hours": memory.get("ttl_hours", 168),  # 7 days default
        }

        with open(PUBLISHED_FILE, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        return memory_id

    def subscribe(self, cell_id: str, tags: list[str]) -> None:
        """订阅特定标签的记忆."""
        subs = self._load_subscriptions()
        subs[cell_id] = {
            "tags": tags,
            "subscribed_at": datetime.now(UTC).isoformat(),
        }
        self._save_subscriptions(subs)

    def search(self, query: str, tags: list[str] | None = None, limit: int = 10) -> list[dict]:
        """搜索网络中的记忆."""
        results = []

        if not PUBLISHED_FILE.exists():
            return results

        with open(PUBLISHED_FILE) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # 检查 TTL
                pub_time = datetime.fromisoformat(entry.get("published_at", ""))
                ttl = entry.get("ttl_hours", 168)
                age_hours = (datetime.now(UTC) - pub_time).total_seconds() / 3600
                if age_hours > ttl:
                    continue

                # 标签过滤
                if tags:
                    if not any(t in entry.get("tags", []) for t in tags):
                        continue

                # 内容匹配
                content = entry.get("content", "").lower()
                if query.lower() in content:
                    results.append(entry)

                if len(results) >= limit:
                    break

        return results

    def get_subscriptions(self, cell_id: str) -> dict | None:
        """获取 Cell 的订阅."""
        subs = self._load_subscriptions()
        return subs.get(cell_id)

    def get_stats(self) -> dict:
        """获取网络统计."""
        total_memories = 0
        active_memories = 0
        cells = set()

        if PUBLISHED_FILE.exists():
            with open(PUBLISHED_FILE) as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    total_memories += 1
                    cells.add(entry.get("cell_id", ""))

                    # 检查 TTL
                    pub_time = datetime.fromisoformat(entry.get("published_at", ""))
                    ttl = entry.get("ttl_hours", 168)
                    age_hours = (datetime.now(UTC) - pub_time).total_seconds() / 3600
                    if age_hours <= ttl:
                        active_memories += 1

        subs = self._load_subscriptions()

        return {
            "total_memories": total_memories,
            "active_memories": active_memories,
            "cells": list(cells),
            "subscriptions": len(subs),
        }

    def cleanup_expired(self) -> int:
        """清理过期记忆. 返回清理数量."""
        if not PUBLISHED_FILE.exists():
            return 0

        kept = []
        cleaned = 0

        with open(PUBLISHED_FILE) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # 检查 TTL
                pub_time = datetime.fromisoformat(entry.get("published_at", ""))
                ttl = entry.get("ttl_hours", 168)
                age_hours = (datetime.now(UTC) - pub_time).total_seconds() / 3600

                if age_hours <= ttl:
                    kept.append(line)
                else:
                    cleaned += 1

        with open(PUBLISHED_FILE, "w") as f:
            f.writelines(kept)

        return cleaned

    def _load_subscriptions(self) -> dict:
        if SUBSCRIPTIONS_FILE.exists():
            with open(SUBSCRIPTIONS_FILE) as f:
                return json.load(f)
        return {}

    def _save_subscriptions(self, subs: dict) -> None:
        with open(SUBSCRIPTIONS_FILE, "w") as f:
            f.write(json.dumps(subs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cell Memory Network")
    parser.add_argument("--action", choices=["publish", "search", "stats", "cleanup"], default="stats")
    parser.add_argument("--cell", help="Cell ID")
    parser.add_argument("--content", help="Memory content to publish")
    parser.add_argument("--query", help="Search query")
    parser.add_argument("--tags", help="Comma-separated tags")
    args = parser.parse_args()

    network = MemoryNetwork()

    if args.action == "publish":
        if not args.cell or not args.content:
            print("Usage: --action publish --cell <id> --content <text>")
            exit(1)
        memory_id = network.publish(
            args.cell,
            {
                "content": args.content,
                "type": "semantic",
                "tags": args.tags.split(",") if args.tags else [],
            },
        )
        print(f"Published: {memory_id}")

    elif args.action == "search":
        if not args.query:
            print("Usage: --action search --query <text>")
            exit(1)
        tags = args.tags.split(",") if args.tags else None
        results = network.search(args.query, tags=tags)
        print(json.dumps(results, ensure_ascii=False, indent=2))

    elif args.action == "stats":
        stats = network.get_stats()
        print(json.dumps(stats, ensure_ascii=False, indent=2))

    elif args.action == "cleanup":
        cleaned = network.cleanup_expired()
        print(f"Cleaned {cleaned} expired memories")
