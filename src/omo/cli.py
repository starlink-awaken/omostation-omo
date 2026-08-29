#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import warnings


def main(argv: list[str] | None = None) -> int:
    warnings.warn(
        "omo CLI 为内部程序接口。人类用户请使用 cockpit。",
        DeprecationWarning,
        stacklevel=2,
    )
    args = list(argv if argv is not None else sys.argv[1:])
    if args and args[0] == "blueprint":
        from omo.blueprint_control import main as blueprint_main

        return blueprint_main(args[1:])
    if args and args[0] == "resident":
        from omo.resident.cli import main as resident_main

        return resident_main(args[1:])
    # P48-W2: serve 子命令 (stdin/stdout JSON-RPC, 供 agora subprocess spawn)
    if args and args[0] == "serve":
        from omo.omo_sync_serve import serve as omo_serve

        return omo_serve()
    if args and args[0] in {"capability", "registry", "scenario", "pkg"}:
        from omo.omo_capability import main as capability_main

        return capability_main(args)
    if args and args[0] == "baseline":
        from omo.omo_baseline_write import main as baseline_main

        return baseline_main(args[1:])
    if args and args[0] == "metacognition":
        from omo.omo_metacognition import main as metacognition_main

        return metacognition_main(args[1:])
    if args and args[0] == "phase14":
        from omo.omo_phase14 import main as phase14_main

        return phase14_main(args[1:])
    if args and args[0] == "phase15":
        from omo.omo_phase15 import main as phase15_main

        return phase15_main(args[1:])
    if args and args[0] == "phase16":
        from omo.omo_phase16 import main as phase16_main

        return phase16_main(args[1:])

    if args and args[0] == "ledger":
        from omo.omo_ledger import main as ledger_main

        return ledger_main(args[1:])
    if args and args[0] == "cell":
        from omo.resident.cell_cli import main as cell_main

        return cell_main(args[1:])

    if args and args[0] == "bridge":
        print("⚠️ DEPRECATED: 'omo bridge' 已迁移，建议改用 'workspace compass bet'。")
        from omo.omo_bridge import main as bridge_main

        return bridge_main(args[1:])
    if args and args[0] == "cards":
        from omo.omo_cards import main as cards_main

        return cards_main(args[1:])
    if args and args[0] == "gc":
        from omo.omo_gc import main as gc_main

        return gc_main(args[1:])

    if args and args[0] == "goal":
        from omo.omo_goal import main as goal_main

        return goal_main(args[1:])
    if args and args[0] == "knowledge":
        from omo.omo_knowledge import main as knowledge_main

        return knowledge_main(args[1:])
    if args and args[0] == "delivery":
        from omo.omo_delivery import main as delivery_main

        return delivery_main(args[1:])
    if args and args[0] == "standard":
        from omo.omo_standard import main as standard_main

        return standard_main(args[1:])
    if args and args[0] == "state":
        from omo.omo_state import main as state_main

        return state_main(args[1:])
    if args and args[0] == "debt":
        from omo.omo_debt_cli import main as debt_main

        return debt_main(args[1:])
    if args and args[0] == "i0":
        from omo.omo_i0 import main as i0_main

        return i0_main(args[1:])

    if args and args[0] == "belief":
        return _cmd_belief(args[1:])
    if args and args[0] == "adjudication":
        return _cmd_adjudication(args[1:])
    if args and args[0] == "feedback":
        return _cmd_feedback(args[1:])
    if args and args[0] == "reputation":
        return _cmd_reputation(args[1:])
    if args and args[0] == "compass":
        from omo.omo_compass import main as compass_main

        return compass_main(args[1:])
    if args and args[0] == "observability":
        from omo.omo_observability import main as obs_main

        return obs_main(args[1:])
    if args and args[0] in ("log", "metric"):
        from omo.omo_observability import main as obs_main

        return obs_main(args)

    if args and args[0] == "event":
        from omo.omo_event import main as event_main

        return event_main(args[1:])

    if args and args[0] == "alert":
        from omo.omo_alert import main as alert_main

        return alert_main(args[1:])

    if args and args[0] == "dashboard":
        from omo.omo_dashboard import main as dash_main

        return dash_main(args[1:])

    if args and args[0] == "task":
        from omo.omo_task import main as task_main

        rc = task_main(args[1:])
        # ISC-10: task 状态变更后刷新 debt dashboard (治本: 看板不再依赖手动 `omo debt refresh`).
        # 根因: refresh_outputs 原本只在 `omo debt refresh` 触发, task create/close/promote/archive
        # 改状态不刷看板 → debt-dashboard generated_at 停更. 本 post-hook 让状态变更自动触发刷新.
        if rc == 0:
            _refresh_dashboard_safely("task")
        return rc

    if args and args[0] == "evidence":
        from omo.omo_evidence import main as ev_main

        return ev_main(args[1:])

    if args and args[0] == "cost":
        from omo.omo_cost import main as cost_main

        return cost_main(args[1:])

    if args and args[0] == "governance":
        from omo.omo_audit import governance_history_main, governance_main
        from omo.omo_governance import main as governance_ops_main

        sub = args[1] if len(args) > 1 else "audit"
        # 修 P36 bug: 之前 None 触发 governance_main 用 sys.argv[1:] 重解析, 导致
        # "omo governance audit" 无 --output 时报 "unrecognized arguments: governance audit"
        rest = args[2:] if len(args) > 2 else []
        if sub == "history":
            return governance_history_main(rest)
        if sub in {
            "propose",
            "approve",
            "apply",
            "list",
            "surfaces",
            "ingress-goal",
            "ingress-task",
            "ingress-debt",
        }:
            return governance_ops_main(args[1:])
        if sub in ("audit", "--help", "-h", None):
            return governance_main(rest)
        # unknown sub: treat as audit args
        return governance_main(args[1:])

    if args and args[0] == "daemon":
        from omo.omo_daemon import main as daemon_main

        return daemon_main(args[1:])

    if args and args[0] == "sse-daemon":
        from omo.omo_sse_daemon import main as sse_daemon_main

        return sse_daemon_main()  # type: ignore[return-value]

    if args and args[0] == "bos":
        # BOS (Banyan Object Service) URI 注册/查询 — P33-W1 战役 2 起步
        from omo.omo_bos import main as bos_main

        return bos_main(args[1:])

    if args and args[0] == "health":
        from omo.omo_health import main as health_main

        return health_main(args[1:])

    if args and args[0] == "readiness":
        from omo.omo_readiness import main as readiness_main

        return readiness_main(args[1:])

    if args and args[0] == "external-resources":
        from omo.omo_external_resources import main as external_resources_main

        return external_resources_main(args[1:])

    if args and args[0] in ("x-axis", "xaxis"):
        from omo.omo_xplane import main as xplane_main

        return xplane_main(args[1:])

    if args and args[0] == "project":
        import argparse

        p_parser = argparse.ArgumentParser(prog="omo project", description="17 项目全景 4D 体检与诊断")
        p_sub = p_parser.add_subparsers(dest="subcmd")
        p_inspect = p_sub.add_parser("inspect", help="体检指定项目")
        p_inspect.add_argument("project_name", nargs="?", default="", help="项目名称")
        p_inspect.add_argument("--json", action="store_true", help="JSON 输出")

        p_list = p_sub.add_parser("list", help="列出所有注册项目")
        p_list.add_argument("--json", action="store_true", help="JSON 输出")

        p_args = p_parser.parse_args(args[1:])
        from omo.omo_project_inspector import (
            OMOProjectInspector,
            format_project_inspection,
        )

        inspector = OMOProjectInspector()

        if p_args.subcmd == "inspect":
            if not p_args.project_name:
                data = inspector.inspect_all_projects()
                if p_args.json:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(f"═══ 17 项目全景体检概览 (平均健康度: {data['overall_avg_health']}/100) ═══")
                    for proj_k, proj_v in data["projects"].items():
                        print(
                            f"  • [{proj_v.get('layer', 'N/A')}] {proj_k:<18} 健康度: {proj_v.get('health_score', 0):>3}/100 | {proj_v.get('scale', {}).get('files', 0):>3} 文件 | {proj_v.get('scale', {}).get('loc', 0):>6} LOC"
                        )
                return 0
            else:
                data = inspector.inspect_project(p_args.project_name)
                if p_args.json:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(format_project_inspection(data))
                return 0 if data.get("ok") else 1
        elif p_args.subcmd == "list":
            projs = inspector.get_registered_projects()
            if p_args.json:
                print(json.dumps(projs, indent=2))
            else:
                print("📋 注册项目列表:", ", ".join(projs))
            return 0
        else:
            p_parser.print_help()
            return 0

    if args and args[0] in ("panorama", "full-spectrum"):
        import argparse

        pan_parser = argparse.ArgumentParser(prog="omo panorama", description="7 维全景终极可观测仪表盘")
        pan_parser.add_argument("--json", action="store_true", help="JSON 输出")
        pan_args = pan_parser.parse_args(args[1:])
        from omo.omo_panorama import OMOPanoramaEngine, format_panorama_report

        engine = OMOPanoramaEngine()
        data = engine.get_full_panorama()
        if pan_args.json:
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print(format_panorama_report(data))
        return 0

    if args and args[0] == "inspect":
        import argparse

        parser = argparse.ArgumentParser(prog="omo inspect", description="统一检查入口")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parsed = parser.parse_args(args[1:])
        from omo.omo_inspect import cmd_inspect

        return cmd_inspect(json_output=parsed.json)

    if args and args[0] == "healing":
        return _cmd_healing(args[1:])

    if args and args[0] == "predict":
        from .cli_predict import _cmd_predict

        return _cmd_predict(args[1:])

    if args and args[0] == "cache":
        return _cmd_cache(args[1:])

    if args and args[0] == "logs":
        # Round 10 P0: 统一管理 .omo/_knowledge/*.jsonl (list/inspect/tail/audit)
        from omo.omo_logs import main as logs_main

        return logs_main(args[1:])

    if args and args[0] == "lint":
        # Round 15 P0 (P1-2 from pattern §11.6): 静态校验 7 consumer 写时走 Pydantic schema
        from omo.omo_lint import main as lint_main

        return lint_main(args[1:])

    if args and args[0] == "acl":
        # Scheme C 5c L2 (ADR-0189): path ACL plan/apply (opt-in OMO_OS_ACL=1)
        from omo.omo_acl import main as acl_main

        return acl_main(args[1:])

    if args and args[0] == "lint-metrics":
        # Round 42 P0: omo lint schemas + §17 metrics (单命令跑两者, CI 友好)
        from omo.omo_lint import cmd_lint_schemas

        return cmd_lint_schemas(metrics=True)

    if args and args[0] == "trail":
        # Round 12 P0: omo_trail 第 7 consumer CLI (record/show)
        # Round 19 P0: 加 seed 子命令, 让 trail 业务真落地
        from omo.omo_trail import main as trail_main

        return trail_main(args[1:])

    if args and args[0] == "audit-rollout":
        # Round 27 P0: 跨仓 baseline 聚合 (§12.5.1 步骤 1)
        from omo.omo_audit_rollout import main as rollout_main

        return rollout_main(args[1:])

    # OPC-P3 D1 wiring: omo_worker module 暴露 worker/task 全套子命令
    # (validate / promote-apply / promote-eval / promote-readiness / ...),
    # 原 cli.py 没有 dispatch 此入口
    if args and args[0] in {"worker", "wt"}:
        from omo.omo_worker import main as worker_main

        # The facade parser still owns the worker/task namespace. Preserve the
        # public `omo worker <command>` shape while routing through that parser.
        if len(args) > 1 and args[1] == "task":
            return worker_main(["task", *args[2:]])
        return worker_main(["worker", *args[1:]])

    if args and args[0] == "workspace":
        # ISC-46: workspace status 作为 worktree dirty 计数唯一 SSOT (治本 E3)
        from omo.omo_workspace import main as workspace_main

        return workspace_main(args[1:])

    if args and args[0] == "strategy":
        print("⚠️ DEPRECATED: 'omo strategy' 已迁移，建议改用 'workspace compass radar' 或 'workspace compass gc'。")
        from omo.omo_strategy import main as strategy_main

        return strategy_main(args[1:])

    if args and args[0] == "manage":
        from omo.omo_manage import main as manage_main

        return manage_main(args[1:])

    if args and args[0] == "validate":
        from omo.omo_validate import main as validate_main

        return validate_main(args[1:])

    if args and args[0] == "audit":
        return _cmd_audit(args[1:])

    if args and args[0] == "doctor":
        import argparse

        parser = argparse.ArgumentParser(prog="omo doctor", description="统一健康检查入口")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parsed = parser.parse_args(args[1:])
        from omo.omo_doctor import cmd_doctor

        return cmd_doctor(json_output=parsed.json)

    if args and args[0] == "inspect":
        import argparse

        parser = argparse.ArgumentParser(prog="omo inspect", description="统一检查入口")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parsed = parser.parse_args(args[1:])
        from omo.omo_inspect import cmd_inspect

        return cmd_inspect(json_output=parsed.json)

    if args and args[0] == "docs":
        import argparse

        parser = argparse.ArgumentParser(prog="omo docs", description="CLI 文档自动生成")
        parser.add_argument("--output", "-o", type=str, help="输出文件路径")
        parsed = parser.parse_args(args[1:])
        from omo.omo_docs import cmd_docs

        return cmd_docs(output=parsed.output)

    if args and args[0] == "report":
        import argparse

        parser = argparse.ArgumentParser(prog="omo report", description="综合报告生成")
        parser.add_argument("--output", "-o", type=str, help="输出文件路径")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parsed = parser.parse_args(args[1:])
        from omo.omo_report import cmd_report

        return cmd_report(output=parsed.output, json_output=parsed.json)

    if args and args[0] == "watch":
        import argparse

        parser = argparse.ArgumentParser(prog="omo watch", description="实时监控模式")
        parser.add_argument("--interval", "-i", type=int, default=60, help="检查间隔 (秒)")
        parser.add_argument("--count", "-n", type=int, default=None, help="最大检查次数")
        parsed = parser.parse_args(args[1:])
        from omo.omo_watch import cmd_watch

        return cmd_watch(interval=parsed.interval, max_iterations=parsed.count)

    # 兜底:有参但无匹配子命令 → 报错退出;无参 → 静默退出 0(保持原行为)
    if args:
        print(f"Unknown subcommand: {args[0]}", file=sys.stderr)
        return 1
    return 0


def _refresh_dashboard_safely(trigger: str = "") -> None:
    """ISC-10: 状态变更命令后安全刷新 debt dashboard.

    refresh_outputs 失败不阻塞主命令 (dashboard 是衍生视图, best-effort 容错).
    由 cli 分发层 post-hook 调用 (task / healing 等状态变更入口).
    """
    try:
        import os
        from datetime import UTC, datetime
        from pathlib import Path

        from omo.omo_debt import refresh_outputs

        ws = Path(os.environ.get("WORKSPACE_ROOT", str(Path.home() / "Workspace")))
        omo_dir = ws / ".omo"
        if not omo_dir.is_dir():
            return
        now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        refresh_outputs(omo_dir, now)
    except Exception as e:
        print(f"⚠️  [dashboard refresh skipped via {trigger}]: {e}", file=sys.stderr)

# 2026-08-29: subcommand functions extracted to focused modules
from .cli_audit import _cmd_audit
from .cli_healing import _cmd_healing
from .cli_adjudication_feedback import _cmd_adjudication, _cmd_feedback
from .cli_commands import _cmd_belief, _cmd_cache, _cmd_reputation
from .cli_predict import _cmd_predict


if __name__ == "__main__":
    raise SystemExit(main())
