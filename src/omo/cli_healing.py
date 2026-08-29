#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from omo.omo_self_healing import SelfHealingEngine


def _cmd_healing(args: list[str]) -> int:
    """omo healing <subcommand> — 自愈引擎管理 CLI。

    Subcommands:
        status      — 显示引擎当前状态
        fix-run <n> — 手动执行修复脚本
        fix-list    — 列出所有可用修复脚本
        rules       — 列出所有规则
        config      — 导出当前规则到 YAML
        history     — 显示触发和修复历史
    """
    if not args:
        print("Usage: omo healing <status|fix-run|fix-list|rules|config|history>")
        return 1

    sub = args[0]

    if sub == "status":
        import json

        from omo.omo_self_healing import get_healing_engine

        engine = get_healing_engine()
        status = engine.get_status()
        print(json.dumps(status, indent=2, default=str, ensure_ascii=False))

    elif sub == "fix-run":
        if len(args) < 2:
            print("Usage: omo healing fix-run <name>")
            return 1
        from omo.omo_self_healing import run_fix

        result = run_fix(args[1])
        status_icon = "✅" if result["success"] else "❌"
        print(f"{status_icon} {result['fix_name']}: {result['output']}")
        return 0 if result["success"] else 1

    elif sub == "fix-list":
        from omo.omo_self_healing import list_fixes

        for fix in list_fixes():
            print(f"  - {fix}")

    elif sub == "rules":
        from omo.omo_self_healing import get_healing_engine

        engine = get_healing_engine()
        for r in engine._rules:
            fixes = f" fixes={r.fix_names}" if r.fix_names else ""
            print(f"  {r.name}: threshold={r.threshold} {r.severity}{fixes}")

    elif sub == "config":
        from omo.omo_self_healing import get_healing_engine, save_rules

        engine = get_healing_engine()
        save_rules(engine._rules)
        print("Rules saved to .omo/self_healing_rules.yaml")

    elif sub == "history":
        import json

        from omo.omo_self_healing import get_history

        data = get_history()
        print(json.dumps(data, indent=2, default=str, ensure_ascii=False))

    else:
        print(f"Unknown subcommand: {sub}")
        return 1

    return 0
