#!/usr/bin/env python3
"""Governor — Agent Cell 治理器. 风险分级与审批决策."""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]

RISK_R0, RISK_R1, RISK_R2, RISK_R3 = "R0", "R1", "R2", "R3"
DECISION_AUTO, DECISION_APPROVE, DECISION_REJECT = "auto_execute", "human_approve", "reject"

RISK_RULES = {
    RISK_R0: {
        "description": "只读、无副作用",
        "actions": [
            "read_file",
            "list_files",
            "search",
            "query_status",
            "get_info",
            "scan",
            "check",
            "validate",
            "lint",
            "format",
        ],
        "decision": DECISION_AUTO,
    },
    RISK_R1: {
        "description": "低风险、可逆",
        "actions": ["format_code", "generate_doc", "create_draft", "run_tests", "backup", "snapshot", "log"],
        "decision": DECISION_AUTO,
        "needs_audit": True,
    },
    RISK_R2: {
        "description": "中等风险、需审批",
        "actions": ["commit_code", "modify_config", "create_pr", "deploy_staging"],
        "decision": DECISION_APPROVE,
    },
    RISK_R3: {
        "description": "高风险、需同步确认",
        "actions": ["deploy_production", "delete_data", "modify_permissions", "push_main"],
        "decision": DECISION_APPROVE,
        "requires_sync": True,
    },
}

AUTO_ACTIONS = set(a for r in RISK_RULES.values() for a in r["actions"])


class Governor:
    def __init__(self):
        self.decision_log = []
        self.audit_queue = []

    def assess_risk(self, action_request: dict) -> str:
        action = action_request.get("action", "")
        target = str(action_request.get("target", "")).lower()
        if action in RISK_RULES[RISK_R0]["actions"]:
            return RISK_R0
        if action in RISK_RULES[RISK_R1]["actions"]:
            return RISK_R1
        if action in RISK_RULES[RISK_R2]["actions"]:
            return RISK_R2
        if action in RISK_RULES[RISK_R3]["actions"] or "production" in target or "main" in target:
            return RISK_R3
        return RISK_R2

    def decide(self, risk_level: str, action_request: dict | None = None) -> dict:
        rule = RISK_RULES.get(risk_level, RISK_RULES[RISK_R2])
        decision = {
            "schema": "governor-decision/v1",
            "action_id": (action_request or {}).get("action_id", "unknown"),
            "risk_level": risk_level,
            "decision": rule["decision"],
            "reason": rule["description"],
            "timestamp": datetime.now(UTC).isoformat(),
        }
        if risk_level == RISK_R1:
            decision["audit_required"] = True
            self.audit_queue.append(decision)
        if rule["decision"] == DECISION_APPROVE:
            decision["escalation_ref"] = f"escalation://{decision['action_id']}"
        self.decision_log.append(decision)
        return decision

    def assess_and_decide(self, action_request: dict) -> dict:
        return self.decide(self.assess_risk(action_request), action_request)

    def get_stats(self) -> dict:
        return {
            "total_decisions": len(self.decision_log),
            "auto_execute": sum(1 for d in self.decision_log if d["decision"] == DECISION_AUTO),
            "human_approve": sum(1 for d in self.decision_log if d["decision"] == DECISION_APPROVE),
            "pending_audit": len(self.audit_queue),
        }


def _is_auto_executable(event: dict) -> bool:
    payload = event.get("payload", {})
    if not isinstance(payload, dict):
        return False
    action = payload.get("action", "")
    instruction = payload.get("instruction", "")
    event_type = event.get("event_type", "")
    if action in AUTO_ACTIONS or instruction in AUTO_ACTIONS:
        return True
    return event_type in {"WorkflowClosed", "WorkflowSucceeded", "PersonalSignal", "heartbeat", "system.alive"}


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--assess")
    parser.add_argument("--decide", action="store_true")
    parser.add_argument("--risk", choices=["R0", "R1", "R2", "R3"])
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    g = Governor()
    if args.assess:
        req = json.loads(args.assess)
        print(json.dumps({"risk_level": g.assess_risk(req), "action": req.get("action", "?")}, ensure_ascii=False))
    elif args.decide:
        req = json.loads(args.action) if args.action else {}
        print(json.dumps(g.decide(args.risk or g.assess_risk(req), req), ensure_ascii=False, indent=2))
    elif args.status:
        print(json.dumps(g.get_stats(), ensure_ascii=False, indent=2))
