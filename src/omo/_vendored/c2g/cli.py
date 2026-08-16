import argparse
import sys
from pathlib import Path

from omo._vendored.c2g.bridge import _import_pitch, get_omo_dir
from omo._vendored.c2g.bridge_utils import get_c2g_data_dir
from omo._vendored.c2g.outcome_tracker import OutcomeTracker
from omo._vendored.c2g.pitch_analyzer import PitchIntelligenceAnalyzer
from omo._vendored.c2g.scenarios import SCENARIOS, get_scenario
from omo._vendored.c2g.strategy import strategy_audit, strategy_gc


def _slugify(text: str, max_len: int = 30) -> str:
    """文本 → 安全文件名 slug (保留中英文+数字, 其余转 -)."""
    import re

    return re.sub(r"[^a-zA-Z0-9一-龥]+", "-", text)[:max_len].strip("-")


def _pitches_dir(workspace_root: Path, adapter: str) -> Path:
    """pitches 目录 (ecos: workspace/runtime/sandbox/pitches; local: <repo_root>/.c2g_data/pitches), 自动创建."""
    d = (
        workspace_root / "runtime" / "sandbox" / "pitches"
        if adapter == "ecos"
        else get_c2g_data_dir() / "pitches"
    )
    d.mkdir(parents=True, exist_ok=True)
    return d


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="C2G (Concept-to-Goal) Engine - The Strategic Pipeline"
    )
    parser.add_argument(
        "--adapter",
        type=str,
        default="local",
        choices=["ecos", "local"],
        help="Which backend adapter to use",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. Brainstorm (V2P) - 生成结构化 Pitch (产品走查 2026-06-19 修真, 不再 Mock)
    parser_bs = subparsers.add_parser(
        "brainstorm", help="[V2P] Generate a structured Pitch from a topic"
    )
    parser_bs.add_argument("topic", type=str, help="The topic to brainstorm")
    parser_bs.add_argument(
        "--scenario",
        choices=list(SCENARIOS),
        default=None,
        help="[BET-6] 场景化 Pitch: gongwen/vault/research/family/health/finance",
    )

    # 1.5 Draft (V2P) - Interactive Pitch Wizard
    parser_draft = subparsers.add_parser(
        "draft", help="[V2P] Interactive wizard to draft a Pitch"
    )
    parser_draft.add_argument(
        "--scenario",
        choices=list(SCENARIOS),
        default=None,
        help="[BET-6] 场景化 Pitch: gongwen/vault/research/family/health/finance",
    )

    # 2. Bet (C2G) - bridging pitch to bet
    parser_bet = subparsers.add_parser(
        "bet", help="[C2G] Convert a Pitch into a tracked Bet"
    )
    parser_bet.add_argument(
        "source_file", type=str, help="Path to the Pitch markdown file"
    )

    # 3. Radar (AGC) - strategy audit
    subparsers.add_parser("radar", help="[AGC] Audit system strategy alignment (Radar)")

    # 4. GC (AGC) - entropy garbage collection
    parser_gc = subparsers.add_parser(
        "gc", help="[AGC] Garbage collect decayed Sandbox pitches"
    )
    parser_gc.add_argument(
        "--dry-run", action="store_true", help="Preview GC without moving files"
    )

    # 5. Outcome tracking (NEW) - Pitch 效果追踪
    parser_outcome = subparsers.add_parser(
        "outcome", help="[NEW] Track Pitch outcome tracking"
    )
    outcome_subparsers = parser_outcome.add_subparsers(
        dest="outcome_command", required=True
    )
    outcome_subparsers.add_parser("list", help="List all tracked Pitch outcomes")
    outcome_track = outcome_subparsers.add_parser(
        "track", help="Track a specific Pitch"
    )
    outcome_track.add_argument("pitch_id", type=str, help="Pitch ID to track")
    outcome_subparsers.add_parser("analyze", help="Analyze Pitch success factors")

    # 6. Pitch suggest (NEW) - Pitch 改进建议
    parser_suggest = subparsers.add_parser(
        "suggest", help="[NEW] Get Pitch improvement suggestions"
    )
    parser_suggest.add_argument("pitch_file", type=str, help="Path to Pitch file")

    args = parser.parse_args(argv)

    # local: 存储锚定 <repo_root>/.c2g_data (不随 cwd 漂移, 见 bridge_utils.get_c2g_data_dir)
    omo_dir = get_omo_dir(Path.cwd()) if args.adapter == "ecos" else get_c2g_data_dir()
    if args.adapter == "local":
        workspace_root = get_c2g_data_dir().parent
    else:
        workspace_root = omo_dir.parent
        if omo_dir.name == ".omo" and workspace_root.name == "omo":
            workspace_root = workspace_root.parent.parent

    if args.command == "brainstorm":
        # 修真 v1 (产品走查 2026-06-19): Mock print → 真生成 Pitch (KISS 模板, 不集成 MetaOS)
        # BET-6 (2026-06-27): --scenario 场景化 Pitch (注册表驱动, 向后兼容)
        sc = get_scenario(args.scenario)
        pitch_path = (
            _pitches_dir(workspace_root, args.adapter)
            / f"Idea-{_slugify(args.topic)}.md"
        )
        if pitch_path.exists():
            print(f"ℹ️  Pitch 已存在: {pitch_path} (用 draft 补细节, 或直接 bet)")
            return 0
        if sc:
            sections = "".join(f"## {t}\n{p}\n\n" for t, p in sc.sections)
            nogos = "".join(f"- {n}\n" for n in sc.nogos)
            content = (
                f"# {args.topic}\n\n"
                f"> **Scenario**: {sc.key} ({sc.label})\n"
                f"> **Upstream**: (待填 — {sc.upstream_hint})\n"
                f"> **Appetite:** (待填 — e.g. 2小时 / 1天 / 1周)\n\n"
                f"## 背景与上下文\n(brainstorm 自动生成, 补充: 为什么想做? 解决什么痛点?)\n\n"
                f"{sections}"
                f"## NoGos (YAGNI)\n{nogos}"
            )
        else:
            content = (
                f"# {args.topic}\n\n"
                f"> **Upstream**: (待填 — bet 前必须声明北极星, 否则 CR-STRATEGY-01 孤儿拦截)\n"
                f"> **Appetite:** (待填 — e.g. 2小时 / 1天 / 1周)\n\n"
                f"## 背景与上下文\n(brainstorm 自动生成, 补充: 为什么想做? 解决什么痛点?)\n\n"
                f"## 目标\n- \n\n## NoGos (YAGNI)\n- \n"
            )
        pitch_path.write_text(content, encoding="utf-8")
        suffix = f" [场景: {sc.label}]" if sc else ""
        print(f"🧠 [V2P] brainstorm 真生成 Pitch{suffix}: {pitch_path}")
        print(
            f"➡️ 下一步: 编辑补 Upstream/Appetite → `c2g --adapter ecos bet {pitch_path.resolve()}`"
        )

    elif args.command == "draft":
        sc = get_scenario(args.scenario)
        scenario_tag = f" [场景: {sc.label}]" if sc else ""
        print(f"\n🧠 [C2G 战略向导{scenario_tag}] 让我们把模糊的点子变成具体的行动：")
        try:
            idea = input("? 一句话描述您的点子 (Core Idea): ").strip()
            upstream_hint = (
                f" (Upstream, {sc.upstream_hint})"
                if sc
                else " (Upstream, e.g. 提升工程质量)"
            )
            upstream = input(
                f"? 这个点子的北极星/上游愿景是什么{upstream_hint}: "
            ).strip()
            appetite = input(
                "? 您的胃口/预算是多少 (Appetite, e.g. 2小时 / 1周): "
            ).strip()
            context = input("? 补充一些背景信息 (可选): ").strip()
        except KeyboardInterrupt:
            print("\n❌ 已取消。")
            return 1

        if not idea:
            print("❌ 点子不能为空，已取消。")
            return 1

        pitch_path = (
            _pitches_dir(workspace_root, args.adapter) / f"Idea-{_slugify(idea)}.md"
        )
        if sc:
            sections = "".join(f"## {t}\n{p}\n\n" for t, p in sc.sections)
            scenario_line = f"> **Scenario**: {sc.key} ({sc.label})\n"
        else:
            sections = ""
            scenario_line = ""
        content = (
            f"# {idea}\n\n"
            f"{scenario_line}"
            f"> **Upstream**: {upstream or 'Unknown'}\n> **Appetite:** {appetite or 'Unknown'}\n\n"
            f"## 背景与上下文\n{context}\n\n"
            f"{sections}"
        )
        pitch_path.write_text(content, encoding="utf-8")

        print(f"\n✅ 成功！Pitch{scenario_tag} 已生成于 {pitch_path}")
        print(
            f"➡️ 下一步：您可以执行 `workspace compass bet {pitch_path}` 进行下注转换。"
        )

    elif args.command == "bet":
        source = Path(args.source_file)
        if not source.exists():
            print(f"❌ Error: Pitch file {source} not found.")
            return 1
        print("🌉 [C2G] 触发桥接，验证 M2 Schema 与 L0 约束...")
        _import_pitch(source, omo_dir, args.adapter)

    elif args.command == "radar":
        strategy_audit(omo_dir, args.adapter)

    elif args.command == "gc":
        strategy_gc(workspace_root, args.adapter)

    elif args.command == "outcome":
        # 新功能: Outcome 追踪
        tracker = OutcomeTracker(get_c2g_data_dir())
        if args.outcome_command == "list":
            print("📊 [Outcome] Pitch 效果排行榜:")
            leaderboard = tracker.get_leaderboard()
            if not leaderboard:
                print("  (暂无数据)")
            else:
                for i, item in enumerate(leaderboard, 1):
                    score = item.get("success_score", 0)
                    path = item.get("pitch_path", "")
                    print(f"  {i}. {path} - 成功率: {score:.0%}")
        elif args.outcome_command == "track":
            lifecycle = tracker.track_pitch_lifecycle(args.pitch_id)
            if lifecycle:
                print("📊 [Outcome] Pitch 完整生命周期:")
                print(f"  Pitch ID: {lifecycle.pitch_id}")
                print(f"  路径: {lifecycle.pitch_path}")
                print(f"  创建于: {lifecycle.created_at}")
                print(f"  生成任务: {len(lifecycle.generated_tasks)}")
                print(f"  完成任务: {len(lifecycle.completed_tasks)}")
                print(f"  失败任务: {len(lifecycle.failed_tasks)}")
                print(f"  成功率: {lifecycle.success_score:.0%}")
            else:
                print(f"❌ 未找到 Pitch ID: {args.pitch_id}")
        elif args.outcome_command == "analyze":
            factors = tracker.analyze_pitch_success_factors()
            print("📈 [Outcome] Pitch 成功关键因素分析:")
            if factors.high_success_patterns:
                print("  高成功率模式:")
                for p in factors.high_success_patterns:
                    print(f"    ✓ {p}")
            if factors.recommended_appetite:
                print("  推荐时间预算:")
                for k, v in factors.recommended_appetite.items():
                    print(f"    {k}: {v}")

    elif args.command == "suggest":
        # 新功能: Pitch 改进建议
        pitch_path = Path(args.pitch_file)
        if not pitch_path.exists():
            print(f"❌ Error: Pitch file {pitch_path} not found.")
            return 1
        content = pitch_path.read_text(encoding="utf-8")

        tracker = OutcomeTracker(get_c2g_data_dir())
        analyzer = PitchIntelligenceAnalyzer(tracker)

        # 预测成功率
        prob = analyzer.predict_pitch_success_probability(content)
        print(f"📈 [Pitch Intelligence] 预测成功率: {prob:.0%}")

        # 生成改进建议
        suggestions = analyzer.suggest_pitch_improvements(content)
        print("💡 [Pitch Intelligence] 改进建议:")
        for s in suggestions:
            status = s.get("status", "")
            mark = "✓" if status == "ok" else "•"
            print(f"  {mark} [{s.get('category')}] {s.get('message')}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
