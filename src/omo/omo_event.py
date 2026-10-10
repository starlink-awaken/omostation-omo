#!/usr/bin/env python3
"""OMO event CLI — inspect and subscribe to Agora event bus events.

Round 5 (P3): 新增 ``emit`` 子命令, 用户主动写结构化事件到 .jsonl.
这是 AppendOnlyLog 模式的样板 (第 5 个 consumer): 用户面向的'写事件'接口.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

from omo.omo_io import AppendOnlyLog
from omo.omo_paths import state_root


def default_event_log_path() -> Path:
    """`emit` 的落点 —— 追加写运行态，跟随 profile 的 state 根 (BET-Y2Q4-T10-239)。

    原先是模块级常量，在 import 时读 `WORKSPACE_ROOT` 并以仓根的 host 字面量兜底；
    那条缝从未被声明过，所以恒等于兜底值，且在干净检出上指向不存在的路径。
    改成调用时刻解析后，未声明 profile 时取值与历史逐字节相同。
    """
    return state_root() / ".omo" / "_knowledge" / "omo-events.jsonl"


def _agora_url(path: str) -> str:
    port = os.environ.get("AGORA_MCP_PORT", "7430")
    return f"http://localhost:{port}{path}"


def cmd_event_list(limit: int) -> int:
    """List recent events from Agora EventBus."""
    try:
        resp = urlopen(Request(f"{_agora_url('/api/events')}?limit={limit}"), timeout=3)
        events = json.loads(resp.read())
        if isinstance(events, list):
            print(f"{'TIMESTAMP':30s} {'TYPE':20s} {'DETAILS'}")
            print("-" * 80)
            for e in events[:limit]:
                ts = e.get("timestamp", e.get("ts", "?"))[:26]
                et = e.get("type", e.get("event", "?"))[:18]
                detail = json.dumps(e.get("data", e.get("payload", "")))[:40]
                print(f"{ts:30s} {et:20s} {detail}")
            print(f"\nTotal: {len(events)} events")
        else:
            print(f"EventBus 返回了: {events}")
    except Exception as ex:  # defensive fallback
        print(f"⚠️  EventBus 不可用 ({ex})")
    return 0


def cmd_event_emit(
    event_type: str,
    source: str,
    payload: str,
    log_path: Path,
) -> int:
    """Append-only 写 1 条事件到 .jsonl (AppendOnlyLog 样板).

    用法:
        omo event emit --type my_event --source my_script --payload '{"k":"v"}'
    """
    from omo.omo_audit import _utc_now

    record = {
        "ts": _utc_now(),
        "kind": event_type,
        "source": source,
        "payload": payload,
    }
    log = AppendOnlyLog(log_path)
    log.append(record, sort_keys=True)
    print(f"✅ event emitted: kind={event_type} source={source}")
    print(f"   log: {log_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="omo event",
        description="OMO EventBus inspector + 事件 emit 样板 (Round 5 P3)",
    )
    sub = parser.add_subparsers(dest="command")

    el = sub.add_parser("list", help="List recent events from Agora EventBus")
    el.add_argument("--limit", "-n", type=int, default=20)

    # Round 5 (P3): emit 子命令 — AppendOnlyLog 写汇样板
    em = sub.add_parser("emit", help="Append-only 写 1 条结构化事件到 .jsonl")
    em.add_argument(
        "--type",
        required=True,
        help="事件类型 (e.g. my_event, my_workflow_done)",
    )
    em.add_argument(
        "--source",
        default="cli",
        help="事件来源标识 (e.g. my_script, omo_daemon)",
    )
    em.add_argument(
        "--payload",
        default="{}",
        help="事件 payload (JSON 字符串, 默认空 dict)",
    )
    em.add_argument(
        "--log",
        type=Path,
        default=default_event_log_path(),
        help=f"落点 .jsonl (默认: {default_event_log_path()})",
    )

    args = parser.parse_args(argv)
    if args.command == "list":
        return cmd_event_list(args.limit)
    if args.command == "emit":
        return cmd_event_emit(args.type, args.source, args.payload, args.log)
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
