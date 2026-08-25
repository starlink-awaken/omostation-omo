#!/usr/bin/env python3
"""Cell Cartridge — AGE-v2 Cell 与域 Cartridge 治理桥接.

将 cartridge 策略执行接入 Cell PDP/PEP 治理引擎:
  - cartridge 策略规则 → Cell PDP 评估
  - cartridge 执行 → Cell PEP 强制执行
  - 审计日志 → Cell handoff_log
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident.governor import RISK_R0, RISK_R1, RISK_R2, RISK_R3
from omo.resident.pdp_pep import PDP, PEP

ROOT = Path(__file__).resolve().parents[3]

# Cartridge 策略严重度 → 风险等级映射
SEVERITY_TO_RISK = {
    "CRITICAL": RISK_R3,
    "HIGH": RISK_R2,
    "MEDIUM": RISK_R1,
    "LOW": RISK_R0,
    "INFO": RISK_R0,
}


class CartridgeGovernance:
    """Cartridge 策略的 Cell 治理."""

    def __init__(self, policy_set: str = "cartridge"):
        self.pdp = PDP(policy_set=policy_set)
        self.pep = PEP(self.pdp)
        self.audit_log: list[dict] = []

    def evaluate_cartridge_action(self, cartridge_id: str, action: dict, policies: list[dict]) -> dict:
        """评估 cartridge 动作是否符合策略.

        Args:
            cartridge_id: cartridge 标识
            action: {"action": str, "target": str, "args": dict}
            policies: [{"id": str, "severity": str, "constraint": str}]

        Returns:
            治理决策结果
        """
        # 1. 基础风险分级
        max_risk = RISK_R0
        triggered_policies = []

        for policy in policies:
            severity = policy.get("severity", "MEDIUM")
            risk = SEVERITY_TO_RISK.get(severity, RISK_R1)

            # 检查策略约束 (简化版 - 实际应解析 constraint 表达式)
            if self._check_constraint(action, policy):
                if risk > max_risk:
                    max_risk = risk
                triggered_policies.append(policy["id"])

        # 2. PDP 评估
        action_request = {
            "action": action.get("action", ""),
            "target": action.get("target", ""),
            "action_id": f"cart-{cartridge_id}-{uuid.uuid4().hex[:8]}",
        }

        decision = self.pdp.evaluate(
            action_request,
            context={
                "cartridge_id": cartridge_id,
                "triggered_policies": triggered_policies,
            },
        )

        # 3. 如果有触发的策略，升级风险
        if triggered_policies:
            decision["risk_level"] = max_risk
            decision["triggered_policies"] = triggered_policies
            if max_risk == RISK_R3:
                decision["decision"] = "reject"
            elif max_risk == RISK_R2:
                decision["decision"] = "human_approve"

        # 4. 审计
        self.audit_log.append(
            {
                "cartridge_id": cartridge_id,
                "action": action,
                "decision": decision,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )

        return decision

    def enforce_cartridge_execution(self, cartridge_id: str, intent: str, policies: list[dict]) -> dict:
        """强制执行 cartridge 执行."""
        action = {
            "action": "cartridge_execute",
            "target": cartridge_id,
            "args": {"intent": intent},
        }
        return self.pep.enforce(
            action,
            context={
                "cartridge_id": cartridge_id,
                "policies": [p["id"] for p in policies],
            },
        )

    def _check_constraint(self, action: dict, policy: dict) -> bool:
        """检查动作是否触发策略约束 (简化版).

        实际应解析 constraint 表达式 (如 CEL/Rego).
        这里使用关键词匹配作为演示.
        """
        constraint = policy.get("constraint", "")
        action_str = json.dumps(action)

        # 简单关键词检查
        if "public-cloud" in constraint and "public-cloud" in action_str:
            return True
        if "budget" in constraint and "budget" in action_str:
            return True
        if "mlps_grade" in constraint and "system_level" in action_str:
            return True

        return False

    def get_audit_summary(self) -> dict:
        """获取审计摘要."""
        total = len(self.audit_log)
        blocked = sum(1 for a in self.audit_log if a["decision"].get("decision") == "reject")
        approved = sum(1 for a in self.audit_log if a["decision"].get("decision") == "auto_execute")
        escalated = total - blocked - approved

        return {
            "total_evaluations": total,
            "blocked": blocked,
            "auto_approved": approved,
            "escalated": escalated,
            "block_rate": round(100 * blocked / total, 1) if total > 0 else 0,
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cell Cartridge Governance")
    parser.add_argument("--demo", action="store_true", help="Run demo")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    gov = CartridgeGovernance()

    if args.demo:
        # 演示: 评估一个 cartridge 动作
        demo_policies = [
            {
                "id": "RULE-WEIJIAN-DATA-01",
                "severity": "CRITICAL",
                "constraint": "not (contains(action.uri, 'public-cloud') and not action.args.is_sanitized)",
            },
            {
                "id": "RULE-WEIJIAN-FINANCE-02",
                "severity": "HIGH",
                "constraint": "action.args.budget_cny <= 500000 or action.args.has_expert_review == true",
            },
        ]

        demo_actions = [
            {"action": "deploy", "target": "public-cloud", "args": {"is_sanitized": False}},
            {"action": "deploy", "target": "private-cloud", "args": {"is_sanitized": True}},
            {"action": "budget", "target": "project-x", "args": {"budget_cny": 600000, "has_expert_review": False}},
        ]

        print("=== Cartridge Governance Demo ===")
        for action in demo_actions:
            result = gov.evaluate_cartridge_action("cartridge-weijian-v1", action, demo_policies)
            status = (
                "✓" if result["decision"] == "auto_execute" else "⚠" if result["decision"] == "human_approve" else "✗"
            )
            print(f"  {status} {action['action']} → {action['target']}: {result['decision']} ({result['risk_level']})")

        if args.json:
            print("\n" + json.dumps(gov.get_audit_summary(), ensure_ascii=False, indent=2))
    else:
        print("Use --demo to run a demo")
