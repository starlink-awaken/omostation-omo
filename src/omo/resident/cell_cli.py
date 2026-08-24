#!/usr/bin/env python3
"""Agent Cell CLI — 动态 Agent Cell 命令行入口.

用法:
    omo cell plan "意图描述"           # 生成执行计划
    omo cell execute '<plan_json>'     # 执行计划
    omo cell verify '<result_json>'    # 验证结果
    omo cell govern <action>           # 风险评估
    omo cell pdp <action>              # 策略决策
    omo cell pep <action>              # 策略执行
    omo cell memory process '<episode>' # 记忆处理
    omo cell memory consolidate        # 记忆整合
    omo cell replay '<episode>'        # 回放
    omo cell shadow "意图"             # 影子运行
    omo cell eval [N]                  # 评估性能
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


def _run(script: str, args: list[str]) -> int:
    """运行脚本."""
    import subprocess

    script_path = ROOT / script
    if not script_path.exists():
        print(f"Error: Script not found: {script}", file=sys.stderr)
        return 1
    result = subprocess.run(["python3", str(script_path)] + args, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
    return result.returncode


def cmd_plan(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell plan <intent>", file=sys.stderr)
        return 1
    return _run("projects/omo/src/omo/resident/planner.py", ["--intent", args[0], "--json"])


def cmd_execute(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell execute <plan_json>", file=sys.stderr)
        return 1
    return _run("projects/omo/src/omo/resident/executor.py", ["--plan", args[0], "--json"])


def cmd_verify(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell verify <result_json>", file=sys.stderr)
        return 1
    return _run("projects/omo/src/omo/resident/verifier.py", ["--result", args[0], "--json"])


def cmd_govern(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell govern <action> [target]", file=sys.stderr)
        return 1
    req = {"action": args[0], "target": args[1] if len(args) > 1 else ""}
    return _run("projects/omo/src/omo/resident/governor.py", ["--assess", json.dumps(req), "--json"])


def cmd_pdp(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell pdp <action> [target]", file=sys.stderr)
        return 1
    req = {"action": args[0], "target": args[1] if len(args) > 1 else ""}
    return _run("projects/omo/src/omo/resident/pdp_pep.py", ["--check", json.dumps(req), "--json"])


def cmd_pep(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell pep <action> [target]", file=sys.stderr)
        return 1
    req = {"action": args[0], "target": args[1] if len(args) > 1 else ""}
    return _run("projects/omo/src/omo/resident/pdp_pep.py", ["--enforce", json.dumps(req), "--json"])


def cmd_memory(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell memory <process|consolidate> [episode_json]", file=sys.stderr)
        return 1
    subcmd = args[0]
    if subcmd == "process":
        if len(args) < 2:
            print("Usage: omo cell memory process <episode_json>", file=sys.stderr)
            return 1
        return _run("projects/omo/src/omo/resident/memory_pipeline.py", ["--process", args[1], "--json"])
    elif subcmd == "consolidate":
        return _run("projects/omo/src/omo/resident/memory_pipeline.py", ["--consolidate", "--json"])
    else:
        print(f"Unknown memory subcommand: {subcmd}", file=sys.stderr)
        return 1


def cmd_replay(args: list[str]) -> int:
    if not args:
        print("Usage: omo cell replay <shadow|eval> [json]", file=sys.stderr)
        return 1
    subcmd = args[0]
    if subcmd == "shadow":
        if len(args) < 2:
            print("Usage: omo cell replay shadow <intent>", file=sys.stderr)
            return 1
        return _run("projects/omo/src/omo/resident/replay.py", ["--shadow", "--intent", args[1], "--json"])
    elif subcmd == "eval":
        n = int(args[1]) if len(args) > 1 else 5
        return _run("projects/omo/src/omo/resident/replay.py", ["--eval", "--episodes", str(n), "--json"])
    elif subcmd == "run":
        if len(args) < 2:
            print("Usage: omo cell replay run <episode_json>", file=sys.stderr)
            return 1
        return _run("projects/omo/src/omo/resident/replay.py", ["--replay", args[1], "--json"])
    else:
        print(f"Unknown replay subcommand: {subcmd}", file=sys.stderr)
        return 1


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 0

    cmd = argv[0]
    handlers = {
        "plan": cmd_plan,
        "execute": cmd_execute,
        "verify": cmd_verify,
        "govern": cmd_govern,
        "pdp": cmd_pdp,
        "pep": cmd_pep,
        "memory": cmd_memory,
        "replay": cmd_replay,
    }

    handler = handlers.get(cmd)
    if handler is None:
        print(f"Unknown cell command: {cmd}", file=sys.stderr)
        print(f"Available: {', '.join(handlers.keys())}", file=sys.stderr)
        return 1

    return handler(argv[1:])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
