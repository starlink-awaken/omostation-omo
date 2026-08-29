#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from omo.omo_audit import governance_main
from omo.omo_audit import query as audit_query


def _cmd_audit(args: list[str]) -> int:
    """omo audit <subcommand> — X 审计工具集.

    Subcommands:
        cards      — CARDS X3 value metrics (SQLite 聚合)
        vault      — Vault X1 audit (Markdown content hash + author tracking)
        freshness  — X2 freshness audit (3 条 P43 巡检规则)
    """
    if not args or args[0] in ("-h", "--help"):
        print("omo audit — X 审计工具集\n")
        print("Usage: omo audit <subcommand> [options]\n")
        print("Subcommands:")
        print("  cards      CARDS X3 value metrics (SQLite 聚合)")
        print("  vault      Vault X1 audit (Markdown content hash + author tracking)")
        print("  freshness  X2 freshness audit (3 条 P43 巡检规则)")
        print("\nUse 'omo audit <subcommand> --help' for subcommand help.")
        return 0

    sub = args[0]
    rest = args[1:]

    if sub == "cards":
        import argparse

        parser = argparse.ArgumentParser(
            prog="omo audit cards",
            description="CARDS X3 value metrics — 从 SQLite 聚合 card 指标",
        )
        parser.add_argument("--db", type=str, help="显式指定 db 路径")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parser.add_argument("--output", type=str, help="写入文件 (相对于 workspace)")
        parsed = parser.parse_args(rest)
        from omo.omo_audit_cards import cmd_cards

        return cmd_cards(db_path=parsed.db, json_output=parsed.json, output=parsed.output)

    if sub == "vault":
        import argparse

        parser = argparse.ArgumentParser(
            prog="omo audit vault",
            description="Vault X1 audit — Markdown content hash + author tracking",
        )
        parser.add_argument("--days", type=int, default=90, help="staleness 阈值 (天)")
        parser.add_argument("--root", type=str, help="扫描根目录 (默认 workspace root)")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parser.add_argument("--output", type=str, help="写入文件 (相对于 workspace)")
        parsed = parser.parse_args(rest)
        from omo.omo_audit_vault import cmd_vault

        return cmd_vault(
            days=parsed.days,
            root=parsed.root,
            json_output=parsed.json,
            output=parsed.output,
        )

    if sub == "freshness":
        import argparse

        parser = argparse.ArgumentParser(
            prog="omo audit freshness",
            description="X2 freshness audit — 执行 3 条 P43 巡检规则",
        )
        parser.add_argument("--dry-run", action="store_true", help="仅输出，不写审计日志")
        parser.add_argument("--only", type=str, help="仅运行指定规则")
        parser.add_argument("--json", action="store_true", help="JSON 输出")
        parsed = parser.parse_args(rest)
        from omo.omo_audit_freshness import cmd_freshness

        return cmd_freshness(dry_run=parsed.dry_run, only=parsed.only, json_output=parsed.json)

    print(f"Unknown audit subcommand: {sub}")
    return 1
