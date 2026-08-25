#!/usr/bin/env python3
"""PDP/PEP — 策略决策点 (Policy Decision Point) + 策略执行点 (Policy Enforcement Point).

AGE-v2 Cell 治理核心:
  - PDP: 评估动作请求的风险等级 + 策略合规性 → 决策 (auto_execute / human_approve / reject)
  - PEP: 执行决策 → 允许/阻断动作 + 审计日志

与 governor.py 的关系:
  - governor.py 做单动作风险分级 (R0-R3)
  - pdp_pep.py 做上下文感知的策略评估 (考虑调用链、时间、频率等)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident.governor import (
    DECISION_APPROVE,
    DECISION_AUTO,
    DECISION_REJECT,
    RISK_R0,
    RISK_R1,
    RISK_R2,
    RISK_R3,
    Governor,
)

ROOT = Path(__file__).resolve().parents[3]

# 策略集: 定义不同场景下的额外约束
POLICY_SETS = {
    "default": {
        "description": "默认策略集",
        "max_r0_per_minute": 60,
        "max_r1_per_minute": 30,
        "max_r2_per_hour": 10,
        "max_r3_per_day": 5,
        "require_audit_for_r1": True,
        "block_r3_without_sync": True,
    },
    "cartridge": {
        "description": "域 Cartridge 执行策略",
        "max_r0_per_minute": 120,
        "max_r1_per_minute": 60,
        "max_r2_per_hour": 20,
        "max_r3_per_day": 10,
        "require_audit_for_r1": True,
        "block_r3_without_sync": True,
    },
    "batch": {
        "description": "批量操作策略",
        "max_r0_per_minute": 30,
        "max_r1_per_minute": 15,
        "max_r2_per_hour": 5,
        "max_r3_per_day": 2,
        "require_audit_for_r1": True,
        "block_r3_without_sync": True,
    },
}


class PDP:
    """策略决策点 — 评估动作请求 + 上下文 → 决策."""

    def __init__(self, policy_set: str = "default"):
        self.policy = POLICY_SETS.get(policy_set, POLICY_SETS["default"])
        self.governor = Governor()
        self.decision_log: list[dict] = []

    def evaluate(self, action_request: dict, context: dict | None = None) -> dict:
        """评估动作请求，返回决策."""
        context = context or {}
        action_id = action_request.get("action_id", f"act-{uuid.uuid4().hex[:12]}")

        # 1. 基础风险分级 (复用 governor)
        risk_level = self.governor.assess_risk(action_request)
        base_decision = self.governor.decide(risk_level, action_request)

        # 2. 上下文感知的策略评估
        constraints: list[str] = []

        # 频率限制检查
        recent_count = context.get("recent_count", 0)
        freq_limit = self._get_freq_limit(risk_level)
        if recent_count >= freq_limit:
            constraints.append(f"频率超限: {recent_count}/{freq_limit} per window")
            if risk_level == RISK_R0:
                base_decision["decision"] = DECISION_APPROVE
                base_decision["audit_required"] = True
            elif risk_level in (RISK_R1, RISK_R2):
                base_decision["decision"] = DECISION_APPROVE
                base_decision["escalation_required"] = True

        # R3 同步确认
        if risk_level == RISK_R3 and self.policy.get("block_r3_without_sync"):
            if not context.get("sync_confirmed", False):
                base_decision["decision"] = DECISION_REJECT
                constraints.append("R3 需同步确认 (block_r3_without_sync)")

        # 3. 构建最终决策
        decision = {
            "schema": "pdp-decision/v1",
            "action_id": action_id,
            "risk_level": risk_level,
            "decision": base_decision["decision"],
            "reason": base_decision.get("reason", ""),
            "policy_set": self.policy.get("description", "unknown"),
            "constraints": constraints,
            "evidence_ref": f"evidence://pdp/{action_id}",
            "timestamp": datetime.now(UTC).isoformat(),
        }

        if base_decision.get("audit_required"):
            decision["audit_required"] = True
        if base_decision.get("escalation_required"):
            decision["escalation_required"] = True
            decision["escalation_ref"] = f"escalation://{action_id}"

        self.decision_log.append(decision)
        return decision

    def _get_freq_limit(self, risk_level: str) -> int:
        """获取风险等级的频率限制."""
        limits = {
            RISK_R0: self.policy.get("max_r0_per_minute", 60),
            RISK_R1: self.policy.get("max_r1_per_minute", 30),
            RISK_R2: self.policy.get("max_r2_per_hour", 10),
            RISK_R3: self.policy.get("max_r3_per_day", 5),
        }
        return limits.get(risk_level, 10)

    def get_stats(self) -> dict:
        """获取 PDP 统计."""
        total = len(self.decision_log)
        auto = sum(1 for d in self.decision_log if d["decision"] == DECISION_AUTO)
        approve = sum(1 for d in self.decision_log if d["decision"] == DECISION_APPROVE)
        reject = sum(1 for d in self.decision_log if d["decision"] == DECISION_REJECT)
        return {
            "total_decisions": total,
            "auto_execute": auto,
            "human_approve": approve,
            "reject": reject,
            "policy_set": self.policy.get("description", "unknown"),
        }


class PEP:
    """策略执行点 — 执行 PDP 决策 + 审计."""

    def __init__(self, pdp: PDP | None = None):
        self.pdp = pdp or PDP()
        self.enforcement_log: list[dict] = []

    def enforce(self, action_request: dict, context: dict | None = None) -> dict:
        """执行决策 → 允许/阻断 + 审计."""
        decision = self.pdp.evaluate(action_request, context)

        result = {
            "schema": "pep-result/v1",
            "action_id": decision["action_id"],
            "action": action_request.get("action", ""),
            "target": action_request.get("target", ""),
            "allowed": True,
            "decision": decision["decision"],
            "risk_level": decision["risk_level"],
            "audit_ref": decision["evidence_ref"],
            "timestamp": datetime.now(UTC).isoformat(),
        }

        if decision["decision"] == DECISION_REJECT:
            result["allowed"] = False
            result["blocked_reason"] = "; ".join(decision.get("constraints", [])) or "策略拒绝"
        elif decision["decision"] == DECISION_APPROVE:
            result["requires_human"] = True
            if decision.get("escalation_ref"):
                result["escalation_ref"] = decision["escalation_ref"]

        self.enforcement_log.append(result)
        return result

    def check_allowed(self, action_request: dict, context: dict | None = None) -> bool:
        """快速检查是否允许（不记录审计）."""
        decision = self.pdp.evaluate(action_request, context)
        return decision["decision"] != DECISION_REJECT

    def get_stats(self) -> dict:
        """获取 PEP 统计."""
        total = len(self.enforcement_log)
        allowed = sum(1 for r in self.enforcement_log if r["allowed"])
        blocked = total - allowed
        return {
            "total_enforcements": total,
            "allowed": allowed,
            "blocked": blocked,
            "block_rate": round(blocked / total, 3) if total > 0 else 0.0,
        }


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="PDP/PEP Policy Engine")
    parser.add_argument("--action", help="Action to evaluate")
    parser.add_argument("--target", default="", help="Action target")
    parser.add_argument("--policy", default="default", choices=list(POLICY_SETS.keys()))
    parser.add_argument("--enforce", action="store_true", help="Run enforcement (PEP mode)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    pep = PEP(PDP(policy_set=args.policy))

    if args.action:
        req = {"action": args.action, "target": args.target}
        if args.enforce:
            result = pep.enforce(req)
        else:
            result = pep.pdp.evaluate(req)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        demo_actions = [
            {"action": "read_file", "target": "README.md"},
            {"action": "scan", "target": "docs/"},
            {"action": "commit_code", "target": "main"},
            {"action": "deploy_production", "target": "prod"},
        ]
        print("=== PDP Demo ===")
        for req in demo_actions:
            decision = pep.pdp.evaluate(req)
            print(f"  {req['action']:<20} → {decision['risk_level']} | {decision['decision']}")

        print("\n=== PEP Demo ===")
        for req in demo_actions:
            result = pep.enforce(req)
            status = "✓" if result["allowed"] else "✗"
            print(f"  {status} {req['action']:<20} | allowed={result['allowed']}")
