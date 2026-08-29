#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from omo.predictive_governance import PredictiveGovernanceEngine


def _cmd_predict(args: list[str]) -> int:
    """预测性治理 - 事前预警"""

    parser = argparse.ArgumentParser(prog="omo predict", description="预测性治理 - 事前预警")
    subparsers = parser.add_subparsers(dest="predict_sub", required=True)

    parser_risks = subparsers.add_parser("risks", help="预测未来治理风险")
    parser_risks.add_argument("--days", type=int, default=7, help="预测未来天数 (默认: 7)")
    parser_risks.add_argument("--json", action="store_true", help="JSON 输出")

    parser_debt = subparsers.add_parser("debt", help="预测债务恶化风险")
    parser_debt.add_argument("--days", type=int, default=30, help="预测未来天数 (默认: 30)")
    parser_debt.add_argument("--json", action="store_true", help="JSON 输出")

    subparsers.add_parser("actions", help="推荐预防性治理动作")

    subparsers.add_parser("alerts", help="生成早期预警")

    parsed = parser.parse_args(args)

    omo_dir = Path.cwd() / ".omo"  # S4: bridge_utils死模块移除inline非补实现
    engine = PredictiveGovernanceEngine(omo_dir)

    if parsed.predict_sub == "risks":
        forecast = engine.forecast_governance_risks(parsed.days)
        if parsed.json:
            import json

            data = {
                "time_horizon_days": forecast.time_horizon_days,
                "overall_risk_level": forecast.overall_risk_level,
                "high_risks_count": len(forecast.high_risks),
                "medium_risks_count": len(forecast.medium_risks),
                "low_risks_count": len(forecast.low_risks),
                "key_trends": forecast.key_trends,
            }
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print(f"📊 [Predictive Governance] 风险预测 (未来 {parsed.days} 天):")
            print(f"  整体风险级别: {forecast.overall_risk_level.upper()}")
            print(f"  高风险: {len(forecast.high_risks)} 项")
            print(f"  中风险: {len(forecast.medium_risks)} 项")
            print(f"  低风险: {len(forecast.low_risks)} 项")
            if forecast.key_trends:
                print("  关键趋势:")
                for trend in forecast.key_trends:
                    print(f"    • {trend}")

    elif parsed.predict_sub == "debt":
        risks = engine.predict_debt_deterioration(parsed.days)
        if parsed.json:
            import json

            data = [
                {
                    "debt_id": r.debt_id,
                    "risk_score": r.risk_score,
                    "predicted_deterioration_days": r.predicted_deterioration_days,
                    "recommended_action": r.recommended_action,
                    "contributing_factors": r.contributing_factors,
                }
                for r in risks
            ]
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print(f"📊 [Predictive Governance] 债务恶化预测 (未来 {parsed.days} 天):")
            if not risks:
                print("  ✓ 未检测到高风险债务")
            else:
                for risk in risks:
                    icon = "🔴" if risk.risk_score > 0.8 else "🟡"
                    print(
                        f"  {icon} {risk.debt_id}: 风险分数 {risk.risk_score:.0%}, "
                        f"预计恶化: {risk.predicted_deterioration_days} 天, "
                        f"建议: {risk.recommended_action}"
                    )

    elif parsed.predict_sub == "actions":
        actions = engine.recommend_proactive_actions()
        print("💡 [Predictive Governance] 推荐预防性治理动作:")
        if not actions:
            print("  (无推荐动作)")
        else:
            for action in actions:
                print(f"  优先级 {action.priority}: {action.action}")
                print(f"    理由: {action.rationale}")
                print(f"    工作量: {action.effort_estimate}, 影响: {action.estimated_impact}")

    elif parsed.predict_sub == "alerts":
        alerts = engine.generate_early_warning_alerts()
        print("⚠️ [Predictive Governance] 早期预警:")
        if not alerts:
            print("  (无预警)")
        else:
            for alert in alerts:
                icon = "🔴" if alert.get("severity") == "critical" else "🟡"
                print(f"  {icon} {alert.get('message')}")

    return 0
