#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from omo.omo_adjudication import VERDICT_CONFIDENCE_DELTA, AdjudicationStore
from omo.omo_autonomy_level import AutonomyLadder
from omo.omo_belief import MOSBeliefManager


def _cmd_adjudication(args: list[str]) -> int:
    """AdjudicationRecorded 裁决管理 (BET-Y1Q1-T4-01)"""
    import argparse

    from omo.omo_adjudication import AdjudicationStore

    parser = argparse.ArgumentParser(prog="omo adjudication", description="裁决记录管理 (AdjudicationRecorded)")
    subparsers = parser.add_subparsers(dest="sub", required=True)

    p_record = subparsers.add_parser("record", help="记录一条裁决")
    p_record.add_argument("--decision-id", required=True, help="关联 decision_outcome ID")
    p_record.add_argument("--verdict", required=True, choices=["accepted", "modified", "rejected"])
    p_record.add_argument("--edit-diff", default="", help="修改 diff")
    p_record.add_argument("--time-spent", type=float, default=0.0, help="审阅耗时(秒)")
    p_record.add_argument("--adjudicator", default="", help="裁决人")
    p_record.add_argument("--notes", default="", help="备注")

    p_query = subparsers.add_parser("query", help="查询裁决")
    p_query.add_argument("--decision-id", default=None, help="按 decision_id 过滤")
    p_query.add_argument("--verdict", default=None, help="按 verdict 过滤")
    p_query.add_argument("--limit", type=int, default=50, help="返回条数上限")
    p_query.add_argument("--json", action="store_true", help="JSON 输出")

    subparsers.add_parser("stats", help="裁决统计")

    parsed = parser.parse_args(args)
    store = AdjudicationStore()

    if parsed.sub == "record":
        adj_id = store.record(
            decision_id=parsed.decision_id,
            verdict=parsed.verdict,
            edit_diff=parsed.edit_diff,
            time_spent_seconds=parsed.time_spent,
            adjudicator=parsed.adjudicator,
            notes=parsed.notes,
        )
        print(f"Recorded: {adj_id}")
    elif parsed.sub == "query":
        results = store.query(
            decision_id=parsed.decision_id,
            verdict=parsed.verdict,
            limit=parsed.limit,
        )
        if parsed.json:
            import json

            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            print(f"裁决记录 ({len(results)} 条):")
            for r in results:
                print(f"  [{r['id']}] {r['verdict']} <- {r['decision_id']} ({r.get('adjudicator', 'N/A')})")
    elif parsed.sub == "stats":
        s = store.stats()
        print(f"裁决统计: 总 {s['total']} | accepted={s['accepted']} modified={s['modified']} rejected={s['rejected']}")
    return 0


def _cmd_feedback(args: list[str]) -> int:
    """MOS 闭环: 人类裁决 → 信念修正 (BET-Y1Q2-T1-03)."""
    import argparse

    from omo.omo_adjudication import VERDICT_CONFIDENCE_DELTA, AdjudicationStore
    from omo.omo_autonomy_level import AutonomyLadder
    from omo.omo_belief import MOSBeliefManager
    from omo.omo_paths import RUNTIME_DELIVERY_DIR, RUNTIME_TRUTH_DIR

    parser = argparse.ArgumentParser(
        prog="omo feedback",
        description="MOS 闭环: 提交裁决 → 自动修正信念置信度 (T1-03)",
    )
    parser.add_argument("--decision-id", required=True, help="关联 decision_outcome ID (do-NNNN)")
    parser.add_argument(
        "--verdict",
        required=True,
        choices=["accepted", "modified", "rejected"],
        help="裁决结果",
    )
    parser.add_argument("--edit-diff", default="", help="修改 diff (modified 时建议填)")
    parser.add_argument("--time-spent", type=float, default=0.0, help="审阅耗时(秒)")
    parser.add_argument("--adjudicator", default="", help="裁决人")
    parser.add_argument("--notes", default="", help="备注")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只预览信念影响, 不写入",
    )

    parsed = parser.parse_args(args)
    mos = MOSBeliefManager(registry_file=RUNTIME_TRUTH_DIR / "registry" / "memory-os.yaml")

    if parsed.dry_run:
        outcome = mos.get_decision_outcome(parsed.decision_id)
        if outcome is None:
            print(f"decision_outcome not found: {parsed.decision_id}")
            return 1
        delta = VERDICT_CONFIDENCE_DELTA[parsed.verdict]
        belief = mos.find_belief_by_topic(outcome.get("decision_type", ""))
        print(f"decision: {outcome.get('decision_type', 'N/A')}")
        print(f"verdict: {parsed.verdict} (delta={delta:+.2f})")
        if belief:
            print(
                f"belief: {belief['id']} ({belief['topic']}) "
                f"confidence={belief.get('confidence', 1.0):.2f} "
                f"→ {max(0.0, min(1.0, belief.get('confidence', 1.0) + delta)):.2f}"
            )
        else:
            print("belief: (no matching belief found, no update)")
        return 0

    store = AdjudicationStore(
        mos_manager=mos,
        calibration_summary_path=(RUNTIME_DELIVERY_DIR / "outcomes" / "capability_calibration_summary.yaml"),
        autonomy_ladder=AutonomyLadder(registry_path=RUNTIME_TRUTH_DIR / "registry" / "autonomy-levels.yaml"),
    )
    adj_id = store.record(
        decision_id=parsed.decision_id,
        verdict=parsed.verdict,
        edit_diff=parsed.edit_diff,
        time_spent_seconds=parsed.time_spent,
        adjudicator=parsed.adjudicator,
        notes=parsed.notes,
    )

    outcome = mos.get_decision_outcome(parsed.decision_id)
    belief = mos.find_belief_by_topic(outcome.get("decision_type", "")) if outcome else None
    delta = VERDICT_CONFIDENCE_DELTA[parsed.verdict]

    print(f"Recorded: {adj_id}")
    if belief:
        state = mos._load_state()
        for b in state["beliefs"]:
            if b["id"] == belief["id"]:
                print(f"Belief updated: {b['id']} confidence={b.get('confidence', 1.0):.2f} (delta={delta:+.2f})")
                break
    else:
        print("(no matching belief — confidence unchanged)")
    return 0
