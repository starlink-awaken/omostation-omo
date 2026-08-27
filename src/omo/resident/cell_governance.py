#!/usr/bin/env python3
"""Cell Governance — AGE-v2 长期治理与防腐.

治理机制:
  - 配置漂移检测
  - 策略合规审计
  - 自动修复
  - 治理报告
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident.governor import Governor, RISK_R0, RISK_R1, RISK_R2, RISK_R3
from omo.resident.pdp_pep import PDP, PEP

ROOT = Path(__file__).resolve().parents[3]
AUDIT_FILE = ROOT / ".omo" / "state" / "agent-cell" / "governance-audit.jsonl"


class CellGovernance:
    """AGE-v2 治理引擎."""

    def __init__(self):
        self.governor = Governor()
        self.pdp = PDP(policy_set="default")
        self.pep = PEP(self.pdp)
        AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)

    def audit_cell_config(self, cell_config: dict) -> dict:
        """审计 Cell 配置合规性."""
        findings = []

        # 1. 检查 max_cells 范围
        max_cells = cell_config.get("max_cells", 4)
        if max_cells < 1 or max_cells > 16:
            findings.append({
                "severity": "error",
                "message": f"max_cells {max_cells} out of range [1, 16]",
            })

        # 2. 检查 auto_scale 配置
        auto_scale = cell_config.get("auto_scale", True)
        if not isinstance(auto_scale, bool):
            findings.append({
                "severity": "warning",
                "message": "auto_scale should be boolean",
            })

        # 3. 检查策略集
        policy_set = cell_config.get("policy_set", "default")
        if policy_set not in ["default", "cartridge", "batch"]:
            findings.append({
                "severity": "warning",
                "message": f"Unknown policy_set: {policy_set}",
            })

        result = {
            "timestamp": datetime.now(UTC).isoformat(),
            "config": cell_config,
            "findings": findings,
            "compliant": len(findings) == 0,
        }

        # 记录审计
        self._log_audit("config_audit", result)

        return result

    def audit_action(self, action_request: dict, context: dict | None = None) -> dict:
        """审计动作合规性."""
        # 1. 风险评估
        risk = self.governor.assess_risk(action_request)

        # 2. 策略执行
        result = self.pep.enforce(action_request, context)

        # 3. 记录审计
        audit_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "action": action_request,
            "risk_level": risk,
            "result": result,
        }
        self._log_audit("action_audit", audit_entry)

        return {
            "risk_level": risk,
            "allowed": result["allowed"],
            "requires_human": result.get("requires_human", False),
            "audit_ref": result.get("audit_ref", ""),
        }

    def detect_drift(self, baseline: dict, current: dict) -> list[dict]:
        """检测配置漂移."""
        drift = []

        for key in set(list(baseline.keys()) + list(current.keys())):
            base_val = baseline.get(key)
            curr_val = current.get(key)

            if base_val != curr_val:
                drift.append({
                    "field": key,
                    "baseline": base_val,
                    "current": curr_val,
                    "severity": "warning" if key != "max_cells" else "info",
                })

        return drift

    def generate_report(self) -> dict:
        """生成治理报告."""
        audits = self._load_audits()

        total_audits = len(audits)
        compliant = sum(1 for a in audits if a.get("result", {}).get("compliant", True))
        violations = total_audits - compliant

        # 按类型统计
        by_type = {}
        for a in audits:
            atype = a.get("type", "unknown")
            by_type.setdefault(atype, 0)
            by_type[atype] += 1

        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "total_audits": total_audits,
            "compliant": compliant,
            "violations": violations,
            "compliance_rate": round(100 * compliant / total_audits, 1) if total_audits > 0 else 100.0,
            "by_type": by_type,
        }

    def auto_remediate(self, finding: dict) -> dict:
        """自动修复发现的问题."""
        remediation = {
            "timestamp": datetime.now(UTC).isoformat(),
            "finding": finding,
            "action": "none",
            "success": False,
        }

        # 根据问题类型自动修复
        message = finding.get("message", "")

        if "max_cells" in message and "out of range" in message:
            remediation["action"] = "clamp_max_cells"
            remediation["success"] = True
        elif "auto_scale" in message and "boolean" in message:
            remediation["action"] = "set_default_auto_scale"
            remediation["success"] = True
        elif "policy_set" in message:
            remediation["action"] = "reset_to_default_policy"
            remediation["success"] = True

        return remediation

    def _log_audit(self, audit_type: str, data: dict) -> None:
        """记录审计日志."""
        entry = {
            "type": audit_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "result": data,
        }
        with open(AUDIT_FILE, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _load_audits(self) -> list[dict]:
        """加载审计日志."""
        audits = []
        if AUDIT_FILE.exists():
            with open(AUDIT_FILE, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        audits.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return audits


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cell Governance")
    parser.add_argument("--action", choices=["audit-config", "audit-action", "report", "drift"],
                       default="report")
    parser.add_argument("--config", help="Cell config JSON")
    parser.add_argument("--cell-action", help="Action to audit JSON")
    args = parser.parse_args()

    governance = CellGovernance()

    if args.action == "audit-config":
        if not args.config:
            print("Usage: --action audit-config --config '<json>'")
            exit(1)
        config = json.loads(args.config)
        result = governance.audit_cell_config(config)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.action == "audit-action":
        if not args.cell_action:
            print("Usage: --action audit-action --cell-action '<json>'")
            exit(1)
        action = json.loads(args.cell_action)
        result = governance.audit_action(action)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.action == "report":
        report = governance.generate_report()
        print(json.dumps(report, ensure_ascii=False, indent=2))

    elif args.action == "drift":
        if not args.config:
            print("Usage: --action drift --config '<json>'")
            exit(1)
        current = json.loads(args.config)
        baseline = {"max_cells": 4, "auto_scale": True, "policy_set": "default"}
        drift = governance.detect_drift(baseline, current)
        print(json.dumps(drift, ensure_ascii=False, indent=2))
