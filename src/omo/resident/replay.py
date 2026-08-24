#!/usr/bin/env python3
"""Replay Framework — 回放/影子/Eval 框架."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
REPLAY_DIR = ROOT / ".omo/state/agent-cell-replay"
EPISODE_LOG = REPLAY_DIR / "episodes.jsonl"
EVAL_LOG = REPLAY_DIR / "eval_results.jsonl"


class ReplayFramework:
    def __init__(self):
        REPLAY_DIR.mkdir(parents=True, exist_ok=True)

    def replay_episode(self, episode: dict) -> dict:
        from omo.resident.cell import CellCoordinator

        c = CellCoordinator()
        r = c.start_episode(f"replay-{episode.get('episode_id', '?')}", episode.get("intent", {}))
        result = {
            "schema": "replay-result/v1",
            "replay_id": f"replay-{uuid.uuid4().hex[:12]}",
            "original_episode_id": episode.get("episode_id", "?"),
            "cell_id": r["cell_id"],
            "state": r["state"],
            "replayed_at": datetime.now(UTC).isoformat(),
        }
        with open(EPISODE_LOG, "a") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")
        return result

    def shadow_run(self, intent: dict) -> dict:
        from omo.resident.cell import CellCoordinator
        from omo.resident.executor import Executor
        from omo.resident.planner import Planner
        from omo.resident.verifier import Verifier

        c = CellCoordinator()
        c.start_episode(f"shadow-{uuid.uuid4().hex[:12]}", intent)
        plan = Planner().create_plan(intent)
        exec_result = Executor(backend="local").execute_plan(plan)
        verdict = Verifier().verify(exec_result, intent)
        c.complete(verdict.get("verdict", "reject"))
        result = {
            "schema": "shadow-result/v1",
            "shadow_id": f"shadow-{uuid.uuid4().hex[:12]}",
            "intent": intent,
            "verdict": verdict,
            "side_effects": False,
            "executed_at": datetime.now(UTC).isoformat(),
        }
        with open(EPISODE_LOG, "a") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")
        return result

    def eval_cell(self, episodes: list[dict]) -> dict:
        results = [self.replay_episode(ep) for ep in episodes]
        completed = sum(1 for r in results if r.get("state") == "completed")
        result = {
            "schema": "eval-result/v1",
            "eval_id": f"eval-{uuid.uuid4().hex[:12]}",
            "total_episodes": len(results),
            "completed": completed,
            "success_rate": round(completed / len(results), 2) if results else 0,
            "evaluated_at": datetime.now(UTC).isoformat(),
        }
        with open(EVAL_LOG, "a") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")
        return result

    def get_stats(self) -> dict:
        return {
            "total_replays": sum(1 for _ in open(EPISODE_LOG)) if EPISODE_LOG.exists() else 0,
            "total_evals": sum(1 for _ in open(EVAL_LOG)) if EVAL_LOG.exists() else 0,
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--replay")
    parser.add_argument("--shadow", action="store_true")
    parser.add_argument("--intent")
    parser.add_argument("--eval", action="store_true")
    parser.add_argument("--episodes", type=int, default=5)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    f = ReplayFramework()
    if args.replay:
        print(json.dumps(f.replay_episode(json.loads(args.replay)), ensure_ascii=False, indent=2))
    elif args.shadow:
        print(json.dumps(f.shadow_run(json.loads(args.intent) if args.intent else {}), ensure_ascii=False, indent=2))
    elif args.eval:
        test_eps = [{"episode_id": f"eval-{i}", "intent": {"goal": f"test-{i}"}} for i in range(args.episodes)]
        print(json.dumps(f.eval_cell(test_eps), ensure_ascii=False, indent=2))
    elif args.status:
        print(json.dumps(f.get_stats(), ensure_ascii=False, indent=2))
