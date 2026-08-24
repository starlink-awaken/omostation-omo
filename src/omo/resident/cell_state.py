#!/usr/bin/env python3
"""Cell State Persistence — Agent Cell 状态持久化. 保存/恢复 Cell 状态，支持重启后恢复."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
STATE_DIR = ROOT / ".omo/state/agent-cell"
STATE_FILE = STATE_DIR / "cell_states.json"


class CellStateManager:
    """管理 Cell 状态的保存与恢复."""

    def __init__(self):
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    def save_state(self, cell_state: dict) -> str:
        """保存 Cell 状态到磁盘. 返回 state_id."""
        state_id = cell_state.get("cell_id") or f"cell-{uuid.uuid4().hex[:12]}"
        cell_state["state_id"] = state_id
        cell_state["saved_at"] = datetime.now(UTC).isoformat()

        states = self._load_all_states()
        states[state_id] = cell_state
        self._save_all_states(states)
        return state_id

    def load_state(self, state_id: str) -> dict | None:
        """从磁盘恢复 Cell 状态."""
        states = self._load_all_states()
        return states.get(state_id)

    def load_latest(self, episode_id: str | None = None) -> dict | None:
        """加载最新的 Cell 状态，可选按 episode_id 过滤."""
        states = self._load_all_states()
        if not states:
            return None
        if episode_id:
            matching = [s for s in states.values() if s.get("episode_id") == episode_id]
            if matching:
                return max(matching, key=lambda s: s.get("saved_at", ""))
        return max(states.values(), key=lambda s: s.get("saved_at", ""))

    def list_states(self) -> list[dict]:
        """列出所有保存的状态摘要."""
        states = self._load_all_states()
        return [
            {
                "state_id": s.get("state_id", ""),
                "cell_id": s.get("cell_id", ""),
                "episode_id": s.get("episode_id"),
                "state": s.get("state", "unknown"),
                "saved_at": s.get("saved_at", ""),
            }
            for s in states.values()
        ]

    def delete_state(self, state_id: str) -> bool:
        """删除指定状态."""
        states = self._load_all_states()
        if state_id in states:
            del states[state_id]
            self._save_all_states(states)
            return True
        return False

    def cleanup_stale(self, max_age_hours: int = 24) -> int:
        """清理超过 max_age_hours 的旧状态. 返回清理数量."""
        states = self._load_all_states()
        now = datetime.now(UTC)
        to_remove = []
        for sid, s in states.items():
            saved = s.get("saved_at", "")
            if saved:
                try:
                    saved_dt = datetime.fromisoformat(saved)
                    age = (now - saved_dt).total_seconds() / 3600
                    if age > max_age_hours:
                        to_remove.append(sid)
                except (ValueError, TypeError):
                    pass
        for sid in to_remove:
            del states[sid]
        if to_remove:
            self._save_all_states(states)
        return len(to_remove)

    def _load_all_states(self) -> dict[str, dict]:
        if not STATE_FILE.exists():
            return {}
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _save_all_states(self, states: dict[str, dict]) -> None:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, ensure_ascii=False, indent=2)


def snapshot_cell(cell: Any) -> dict:
    """从 CellCoordinator 实例创建状态快照."""
    return {
        "cell_id": cell.cell_id,
        "state": cell.state,
        "current_role": cell.current_role,
        "episode_id": cell.episode_id,
        "context": cell.context,
        "handoff_log": cell.handoff_log,
    }


def restore_cell(cell: Any, state: dict) -> None:
    """从状态快照恢复 CellCoordinator 实例."""
    cell.cell_id = state.get("cell_id", cell.cell_id)
    cell.state = state.get("state", "idle")
    cell.current_role = state.get("current_role")
    cell.episode_id = state.get("episode_id")
    cell.context = state.get("context", {})
    cell.handoff_log = state.get("handoff_log", [])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cell State Manager")
    parser.add_argument("--action", choices=["save", "load", "list", "cleanup"], default="list")
    parser.add_argument("--state-id")
    parser.add_argument("--file")
    args = parser.parse_args()

    manager = CellStateManager()

    if args.action == "list":
        states = manager.list_states()
        print(json.dumps(states, ensure_ascii=False, indent=2))

    elif args.action == "save" and args.file:
        with open(args.file, encoding="utf-8") as f:
            state = json.load(f)
        sid = manager.save_state(state)
        print(f"Saved state: {sid}")

    elif args.action == "load" and args.state_id:
        state = manager.load_state(args.state_id)
        print(json.dumps(state or {}, ensure_ascii=False, indent=2))

    elif args.action == "cleanup":
        count = manager.cleanup_stale()
        print(f"Cleaned up {count} stale states")
