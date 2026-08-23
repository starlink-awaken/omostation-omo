#!/usr/bin/env python3

"""resident-roles — 五类常驻 agent 角色协作配置 (M4.3).

Q13: 常驻 5 类 agent (心脏心跳/眼睛监控/大脑决策/记忆沉淀/手执行), 每类角色
使用独立 projector (checkpoint) + topic_filter (事件分片) 并行消费事件流,
互不干扰地推进各自水位。

角色 → event_type 子集 → 目标 handler:
- 记忆 sediment: 成功/关闭事件 → knowledge_sediment
- 大脑 decision: 失败事件 → decision_agent
- 手   execute: 执行请求事件 → execution_agent
- 眼睛 monitor: 可观测/告警事件 → alert
- 心脏 heartbeat: 心跳事件 → heartbeat (placeholder 预留)
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

# role → {projector, topic_filter, handler, desc}
ROLES: dict[str, dict[str, Any]] = {
    "sediment": {
        "projector": "resident-sediment",
        "topic_filter": ["WorkflowClosed", "WorkflowSucceeded"],
        "handler": "knowledge_sediment",
        "desc": "记忆沉淀 — 成功运行 → 知识草稿",
    },
    "decision": {
        "projector": "resident-decision",
        "topic_filter": ["WorkflowFailed", "StepFailed", "StepTimeout"],
        "handler": "decision_agent",
        "desc": "大脑决策 — 失败事件 → 决策提案",
    },
    "execute": {
        "projector": "resident-execute",
        "topic_filter": ["ExecutionRequested", "WorkPacketDispatched"],
        "handler": "execution_agent",
        "desc": "手执行 — 执行请求 → pi-worker (非 safe, 需批准门)",
    },
    "monitor": {
        "projector": "resident-monitor",
        "topic_filter": ["system.health", "governance:gate_failed", "alert"],
        "handler": "alert",
        "desc": "眼睛监控 — 可观测/告警事件 → 告警通道",
    },
    "heartbeat": {
        "projector": "resident-heartbeat",
        "topic_filter": ["heartbeat", "system.alive"],
        "handler": "heartbeat",
        "desc": "心脏心跳 — 存活/心跳事件 (预留)",
    },
}


def get_role(name: str) -> dict[str, Any] | None:
    """按角色名返回配置, 未知角色返回 None."""
    return ROLES.get(name)


def all_roles() -> dict[str, Any]:
    """返回全部角色配置 (不含 desc)."""
    return {name: {k: v for k, v in cfg.items() if k != "desc"} for name, cfg in ROLES.items()}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    args = parser.parse_args(argv)
    if args.json:
        print(json.dumps(all_roles(), ensure_ascii=False, indent=2))
        return 0
    print(f"常驻角色: {len(ROLES)} 类 (各自独立 projector + topic_filter 并行消费)")
    for name, cfg in ROLES.items():
        print(f"  - {name}: projector={cfg['projector']} events={','.join(cfg['topic_filter'])}")
        print(f"      {cfg['desc']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
