#!/usr/bin/env python3
"""Planner — Agent Cell 规划者. 意图解析 → 任务分解 → 执行计划."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


class Planner:
    def __init__(self):
        self.plan_counter = 0

    def parse_intent(self, intent: dict | str) -> dict:
        if isinstance(intent, str):
            return {
                "raw_text": intent,
                "type": "text",
                "keywords": [
                    v for v in ["分析", "修复", "生成", "创建", "检查", "运行", "部署", "搜索", "查询"] if v in intent
                ],
            }
        return intent

    def decompose_tasks(self, intent: dict) -> list[dict]:
        tasks = []
        raw = intent.get("raw_text", "")
        if "分析" in raw or "analysis" in raw.lower():
            tasks = [
                {"id": "t1", "action": "scan", "target": "directory"},
                {"id": "t2", "action": "read_file", "target": "*.md"},
                {"id": "t3", "action": "generate_doc", "target": "report"},
            ]
        elif "修复" in raw or "fix" in raw.lower():
            tasks = [
                {"id": "t1", "action": "query_status", "target": "ci"},
                {"id": "t2", "action": "read_file", "target": "logs"},
                {"id": "t3", "action": "format_code", "target": "source"},
                {"id": "t4", "action": "run_tests", "target": "test_suite"},
            ]
        elif "生成" in raw or "generate" in raw.lower():
            tasks = [
                {"id": "t1", "action": "search", "target": "templates"},
                {"id": "t2", "action": "create_draft", "target": "output"},
                {"id": "t3", "action": "generate_doc", "target": "final"},
            ]
        else:
            tasks = [
                {"id": "t1", "action": "search", "target": "query"},
                {"id": "t2", "action": "read_file", "target": "results"},
                {"id": "t3", "action": "generate_doc", "target": "summary"},
            ]
        return tasks

    def create_plan(self, intent: dict | str, context: dict | None = None) -> dict:
        self.plan_counter += 1
        parsed = self.parse_intent(intent)
        tasks = self.decompose_tasks(parsed)
        actions = {t.get("action", "") for t in tasks}
        risk = (
            "R3"
            if actions & {"deploy_production", "delete_data", "modify_permissions", "push_main"}
            else "R2"
            if actions & {"commit_code", "modify_config", "create_pr"}
            else "R0"
        )
        return {
            "schema": "execution-plan/v1",
            "plan_id": f"plan-{uuid.uuid4().hex[:12]}",
            "intent": parsed,
            "tasks": tasks,
            "estimated_steps": len(tasks),
            "risk_assessment": risk,
            "created_at": datetime.now(UTC).isoformat(),
        }


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--intent")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    p = Planner()
    plan = p.create_plan(args.intent or "")
    if args.json:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
    else:
        print(f"Plan {plan['plan_id']}: {plan['estimated_steps']} steps, risk={plan['risk_assessment']}")
        for t in plan["tasks"]:
            print(f"  {t['id']}: {t['action']} → {t['target']}")
