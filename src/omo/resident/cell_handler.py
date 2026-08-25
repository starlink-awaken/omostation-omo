#!/usr/bin/env python3
"""Cell Handler — Resident 事件 → AGE-v2 Cell 路由.

将 resident daemon 的事件路由到 Cell Pool:
  - WorkflowClosed → Cell sediment (记忆沉淀)
  - ExecutionRequested → Cell 执行
  - WorkflowFailed → Cell 故障分析
  - StepTimeout → Cell 超时处理
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident.cell_pool import CellPool
from omo.resident.cell_state import CellStateManager

ROOT = Path(__file__).resolve().parents[3]


class CellHandler:
    """Resident 事件 → Cell 处理器."""

    def __init__(self, pool: CellPool | None = None):
        self.pool = pool or CellPool(max_cells=4, enable_persistence=True)
        self.state_manager = CellStateManager()

    def on_workflow_closed(self, event: dict) -> dict:
        """WorkflowClosed 事件 → Cell 记忆沉淀."""
        workflow_id = event.get("workflow_id", "")
        result = event.get("result", {})

        # 创建 Episode 处理工作流输出
        episode_id = f"sediment-{workflow_id}"
        dispatch = self.pool.dispatch_episode(episode_id, {
            "raw_text": f"沉淀工作流 {workflow_id} 的输出",
            "source": "resident",
            "event_type": "WorkflowClosed",
        })

        # 执行记忆沉淀
        cell = self.pool.get_cell(dispatch["cell_id"])
        if cell:
            cell.handoff("planner", "executor", {"plan": {"tasks": [
                {"action": "generate_doc", "target": f"sediment-{workflow_id}"}
            ]}})

        self.pool.complete_episode(episode_id, "accept")

        return {
            "handler": "cell_handler",
            "event": "WorkflowClosed",
            "episode_id": episode_id,
            "cell_id": dispatch["cell_id"],
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def on_execution_requested(self, event: dict) -> dict:
        """ExecutionRequested 事件 → Cell 执行."""
        action = event.get("action", {})
        action_id = event.get("action_id", "")

        # 仅处理 R0/R1 (R2/R3 需人工审批)
        risk_level = event.get("risk_level", "R2")
        if risk_level in ("R2", "R3"):
            return {
                "handler": "cell_handler",
                "event": "ExecutionRequested",
                "status": "deferred",
                "reason": f"Risk level {risk_level} requires human approval",
                "action_id": action_id,
            }

        episode_id = f"exec-{action_id}"
        dispatch = self.pool.dispatch_episode(episode_id, {
            "raw_text": action.get("instruction", ""),
            "source": "resident",
            "event_type": "ExecutionRequested",
        })

        self.pool.complete_episode(episode_id, "accept")

        return {
            "handler": "cell_handler",
            "event": "ExecutionRequested",
            "episode_id": episode_id,
            "cell_id": dispatch["cell_id"],
            "status": "dispatched",
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def on_workflow_failed(self, event: dict) -> dict:
        """WorkflowFailed 事件 → Cell 故障分析."""
        workflow_id = event.get("workflow_id", "")
        error = event.get("error", "")

        episode_id = f"failure-{workflow_id}"
        dispatch = self.pool.dispatch_episode(episode_id, {
            "raw_text": f"分析工作流 {workflow_id} 失败原因: {error}",
            "source": "resident",
            "event_type": "WorkflowFailed",
        })

        self.pool.complete_episode(episode_id, "accept")

        return {
            "handler": "cell_handler",
            "event": "WorkflowFailed",
            "episode_id": episode_id,
            "cell_id": dispatch["cell_id"],
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def on_step_timeout(self, event: dict) -> dict:
        """StepTimeout 事件 → Cell 超时处理."""
        step_id = event.get("step_id", "")
        timeout_seconds = event.get("timeout_seconds", 0)

        return {
            "handler": "cell_handler",
            "event": "StepTimeout",
            "step_id": step_id,
            "timeout_seconds": timeout_seconds,
            "status": "logged",
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def handle_event(self, event_type: str, event: dict) -> dict:
        """通用事件路由."""
        handlers = {
            "WorkflowClosed": self.on_workflow_closed,
            "ExecutionRequested": self.on_execution_requested,
            "WorkflowFailed": self.on_workflow_failed,
            "StepTimeout": self.on_step_timeout,
        }
        handler = handlers.get(event_type)
        if handler:
            return handler(event)
        return {
            "handler": "cell_handler",
            "event": event_type,
            "status": "unhandled",
            "timestamp": datetime.now(UTC).isoformat(),
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cell Event Handler")
    parser.add_argument("--event", help="Event type")
    parser.add_argument("--payload", default="{}", help="Event payload JSON")
    args = parser.parse_args()

    handler = CellHandler()
    if args.event:
        payload = json.loads(args.payload)
        result = handler.handle_event(args.event, payload)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # 演示模式
        demo_events = [
            ("WorkflowClosed", {"workflow_id": "wf-001", "result": {"ok": True}}),
            ("ExecutionRequested", {"action_id": "act-001", "action": {"instruction": "扫描文件"}, "risk_level": "R0"}),
            ("WorkflowFailed", {"workflow_id": "wf-002", "error": "timeout"}),
        ]
        for event_type, payload in demo_events:
            result = handler.handle_event(event_type, payload)
            print(f"{event_type}: {result.get('status', result.get('episode_id', 'N/A'))}")
