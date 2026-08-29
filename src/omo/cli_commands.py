#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from omo.omo_adjudication import AdjudicationStore
from omo.omo_belief import MOSBeliefManager
from omo.omo_reputation import compute_reputation


def _cmd_cache(args: list[str]) -> int:
    """状态缓存管理"""

    parser = argparse.ArgumentParser(prog="omo cache", description="状态缓存管理")
    subparsers = parser.add_subparsers(dest="cache_sub", required=True)

    subparsers.add_parser("stats", help="显示缓存统计")
    subparsers.add_parser("clear", help="清空所有缓存")
    parser_invalidate = subparsers.add_parser("invalidate", help="失效特定缓存")
    parser_invalidate.add_argument("pattern", type=str, help="缓存键匹配模式")

    parsed = parser.parse_args(args)

    from omo.state_cache import GovernanceStateCache

    omo_dir = Path.cwd() / ".omo"  # S4: bridge_utils死模块移除inline非补实现
    cache = GovernanceStateCache(omo_dir / "_cache")

    if parsed.cache_sub == "stats":
        stats = cache.get_cache_stats()
        print("📊 [State Cache] 缓存统计:")
        print(f"  总条目数: {stats['total_entries']}")
        print(f"  有效条目: {stats['valid_entries']}")
        print(f"  无效条目: {stats['invalid_entries']}")

    elif parsed.cache_sub == "clear":
        cache.invalidate_all()
        print("✅ [State Cache] 已清空所有缓存")

    elif parsed.cache_sub == "invalidate":
        cache.invalidate_on_change(parsed.pattern)  # type: ignore[attr-defined]
        print(f"✅ [State Cache] 已失效匹配 '{parsed.pattern}' 的缓存")

    return 0


def _cmd_belief(args: list[str]) -> int:
    """MOS Agent Belief 经验可观测管理"""

    parser = argparse.ArgumentParser(prog="omo belief", description="MOS Agent Belief 经验可观测性管理")
    subparsers = parser.add_subparsers(dest="sub", required=True)

    p_list = subparsers.add_parser("list", help="列出所有活跃的 Agent 信念与教训")
    p_list.add_argument("--keyword", default="", help="关键词过滤")
    p_list.add_argument("--json", action="store_true", help="JSON 输出")

    subparsers.add_parser("audit", help="查看信念审计日志")

    parsed = parser.parse_args(args)
    mgr = MOSBeliefManager()

    if parsed.sub == "list":
        beliefs = mgr.query_beliefs(parsed.keyword)
        if parsed.json:
            import json

            print(json.dumps(beliefs, ensure_ascii=False, indent=2))
        else:
            print(f"🧠 [MOS Belief Engine] 活跃信念 ({len(beliefs)} 项):")
            for b in beliefs:
                print(f"  • [{b.get('id')}] Topic: {b.get('topic')}")
                print(f"    Belief: {b.get('belief')}")
                print(f"    Run ID: {b.get('source_run_id') or 'N/A'}")
    elif parsed.sub == "audit":
        if mgr.audit_log_file.exists():
            print(mgr.audit_log_file.read_text(encoding="utf-8"))
        else:
            print("暂无审计日志")
    return 0


def _cmd_reputation(args: list[str]) -> int:
    """Agent 信誉画像 (BET-Y1Q2-T4-02)."""

    from omo.omo_belief import MOSBeliefManager

    parser = argparse.ArgumentParser(
        prog="omo reputation",
        description="Agent 信誉画像 — 从决策+裁决推导 (T4-02)",
    )
    parser.add_argument("--agent-id", default="", help="过滤特定 agent (空=全局)")
    parser.add_argument("--json", action="store_true", help="JSON 输出")

    parsed = parser.parse_args(args)
    mos = MOSBeliefManager()
    store = AdjudicationStore(mos_manager=mos)
    profile = compute_reputation(mos, store, agent_id=parsed.agent_id)

    if parsed.json:
        import json

        print(json.dumps(profile.to_dict(), ensure_ascii=False, indent=2))
    else:
        d = profile.to_dict()
        print(f"Agent 信誉画像: {d['agent_id']}")
        print(f"  决策总数: {d['total_decisions']}")
        print(f"  已裁决: {d['total_adjudicated']}")
        print(f"  accepted={d['accepted']} modified={d['modified']} rejected={d['rejected']}")
        print(f"  可靠性: {d['reliability']:.1%}")
        print(f"  准确率: {d['accuracy']:.1%}")
        print(f"  拒绝率: {d['rejection_rate']:.1%}")
        print(f"  平均置信度: {d['avg_confidence']:.3f}")
    return 0
