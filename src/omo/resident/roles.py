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
        "topic_filter": [
            "WorkflowClosed",
            "WorkflowSucceeded",
            "PersonalSignal",
            "InboxSignal",
            "WorkflowRequested",
            "WorkflowAdmitted",
            "StepStarted",
            "StepDispatched",
            "EvidenceRecorded",
        ],
        "handler": "knowledge_sediment",
        "desc": "记忆沉淀 — 运行生命周期/成功/个人信号/感知信号/证据 → 知识草稿",
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


# 4 大认知领域定义 (4-Domain Grid)
DOMAINS: dict[str, dict[str, Any]] = {
    "gov_ssot": {
        "name": "治理与契约域",
        "projects": ["omo", "ecos", "protocols", "bin/gac"],
        "ssot_paths": [".omo/_truth/", "projects/ecos/src/ecos/ssot/", "protocols/"],
        "desc": "负责 SSOT 零漂移、GaC 规则有效性、Task/Debt 闭环与架构元模型对齐",
    },
    "knowledge_mos": {
        "name": "知识与记忆域",
        "projects": ["knowledge", "knowledge/gbrain", "knowledge/kairon"],
        "ssot_paths": [".omo/_knowledge/", "kos/"],
        "desc": "负责记忆一致性、检索准确率、沉淀草稿提炼、向量与图谱索引健康度",
    },
    "compute_fabric": {
        "name": "算力与织网域",
        "projects": ["agora", "runtime", "omlxc", "aetherforge"],
        "ssot_paths": ["projects/agora/etc/bos-services.yaml"],
        "desc": "负责 223+ BOS URI 连通性、本地显存与温度预算、0ms TTFT KV 快照",
    },
    "ingress_lifeos": {
        "name": "人机与价值域",
        "projects": ["cockpit", "spaces", "family-hub"],
        "ssot_paths": ["docs/scene-cards/", ".omo/goals/", "spaces/"],
        "desc": "负责 Decision-Inbox 响应 SLA、North Star 时间账本真实性、场景激活",
    },
}

# 4 角高阶守护角色 (B.D.S.K. 4-Corner Cabinet)
SWARM_ROLES: dict[str, dict[str, Any]] = {
    "builder": {
        "name": "Builder 工匠",
        "action_mode": "write",
        "desc": "领域工匠：专职代码与配置实现，强制绑定 Workflow Run-ID 与 Claim 写面",
    },
    "devil": {
        "name": "Devil 挑战者",
        "action_mode": "attack",
        "desc": "红队挑战：定期注入 Chaos 变异，测试规则与监控活性，抓捕假干活与假心跳",
    },
    "sage": {
        "name": "Sage 架构法官",
        "action_mode": "audit",
        "desc": "架构法官：审查 SSOT、单向依赖与 MOF 契约，产出结构化决策提案推入 Inbox",
    },
    "keeper": {
        "name": "Keeper 守门人",
        "action_mode": "prune",
        "desc": "减法守门：核算膨胀指数，执行日落退役与减法配额 (Subtraction Quota)",
    },
}


def get_role(name: str) -> dict[str, Any] | None:
    """按角色名返回配置, 未知角色返回 None."""
    return ROLES.get(name)


def all_roles() -> dict[str, Any]:
    """返回全部角色配置 (不含 desc)."""
    return {name: {k: v for k, v in cfg.items() if k != "desc"} for name, cfg in ROLES.items()}


def all_domains() -> dict[str, Any]:
    """返回全部认知领域配置."""
    return DOMAINS


def all_swarm_roles() -> dict[str, Any]:
    """返回全部 4 角守护角色配置."""
    return SWARM_ROLES


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    parser.add_argument("--swarm", action="store_true", help="展示 4 域 × 4 角蜂群矩阵")
    args = parser.parse_args(argv)

    if args.swarm:
        data = {"domains": DOMAINS, "swarm_roles": SWARM_ROLES}
        if args.json:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print("=== 自治蜂群 4 大认知领域与 4 角守护角色矩阵 ===")
            print("\n【4 大认知领域】")
            for d_id, d in DOMAINS.items():
                print(f"  • [{d_id}] {d['name']} -> 项目: {', '.join(d['projects'])}")
                print(f"      职责: {d['desc']}")
            print("\n【4 角守护角色 (B.D.S.K.)】")
            for r_id, r in SWARM_ROLES.items():
                print(f"  • @{r_id.capitalize()} ({r['name']}) [模式: {r['action_mode']}]")
                print(f"      职责: {r['desc']}")
        return 0

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
