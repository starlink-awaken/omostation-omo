#!/usr/bin/env python3
"""Cell Coordinator — Agent Cell 协调器."""

from __future__ import annotations
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]

CELL_IDLE = "idle"
CELL_PLANNING = "planning"
CELL_EXECUTING = "executing"
CELL_VERIFYING = "verifying"
CELL_COMPLETED = "completed"
CELL_FAILED = "failed"

ROLE_PLANNER = "planner"
ROLE_EXECUTOR = "executor"
ROLE_VERIFIER = "verifier"


class CellCoordinator:
    def __init__(self, cell_id: str | None = None):
        self.cell_id = cell_id or f"cell-{uuid.uuid4().hex[:12]}"
        self.state = CELL_IDLE
        self.current_role = None
        self.episode_id = None
        self.context = {}
        self.handoff_log = []

    def start_episode(self, episode_id: str, intent: dict) -> dict:
        self.episode_id = episode_id
        self.context = {"intent": intent, "plan": None, "result": None, "verdict": None}
        self.state = CELL_PLANNING
        self.current_role = ROLE_PLANNER
        return {"cell_id": self.cell_id, "episode_id": episode_id, "state": self.state, "current_role": self.current_role, "next_action": "plan"}

    def handoff(self, from_role: str, to_role: str, artifacts: dict) -> dict:
        handoff_record = {
            "schema": "handoff/v1",
            "from_role": from_role,
            "to_role": to_role,
            "context": self.context.copy(),
            "artifacts": artifacts,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "evidence_ref": f"evidence://cell/{self.cell_id}/handoff/{len(self.handoff_log)}",
        }
        self.handoff_log.append(handoff_record)
        role_state_map = {ROLE_PLANNER: CELL_PLANNING, ROLE_EXECUTOR: CELL_EXECUTING, ROLE_VERIFIER: CELL_VERIFYING}
        self.state = role_state_map.get(to_role, CELL_IDLE)
        self.current_role = to_role
        if to_role == ROLE_EXECUTOR:
            self.context["plan"] = artifacts.get("plan")
        elif to_role == ROLE_VERIFIER:
            self.context["result"] = artifacts.get("result")
        return handoff_record

    def complete(self, verdict: str) -> dict:
        self.state = CELL_COMPLETED
        self.context["verdict"] = verdict
        self.current_role = None
        return {"cell_id": self.cell_id, "episode_id": self.episode_id, "state": self.state, "verdict": verdict, "handoff_count": len(self.handoff_log)}

    def fail(self, error: str) -> dict:
        self.state = CELL_FAILED
        self.error = error
        self.current_role = None
        return {"cell_id": self.cell_id, "episode_id": self.episode_id, "state": self.state, "error": error}

    def get_status(self) -> dict:
        return {"cell_id": self.cell_id, "episode_id": self.episode_id, "state": self.state, "current_role": self.current_role, "handoff_count": len(self.handoff_log)}


if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser()
    parser.add_argument("--episode")
    parser.add_argument("--action", choices=["plan", "execute", "verify"])
    parser.add_argument("--intent")
    parser.add_argument("--verdict")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    c = CellCoordinator()
    if args.action == "plan" and args.episode:
        r = c.start_episode(args.episode, json.loads(args.intent) if args.intent else {})
        print(json.dumps(r, ensure_ascii=False))
    elif args.action == "execute":
        r = c.handoff("planner", "executor", json.loads(args.artifacts) if args.artifacts else {})
        print(json.dumps(r, ensure_ascii=False))
    elif args.verdict:
        r = c.complete(args.verdict)
        print(json.dumps(r, ensure_ascii=False))
    else:
        print(json.dumps(c.get_status(), ensure_ascii=False))
