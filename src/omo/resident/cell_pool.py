#!/usr/bin/env python3
"""Cell Pool — 多 Cell 调度池. 智能 Episode 分配 + 负载均衡 + 故障转移."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident.cell import (
    CELL_COMPLETED,
    CELL_EXECUTING,
    CELL_FAILED,
    CELL_IDLE,
    CELL_PLANNING,
    CELL_VERIFYING,
    CellCoordinator,
)
from omo.resident.cell_state import CellStateManager, restore_cell, snapshot_cell

ROOT = Path(__file__).resolve().parents[3]


class CellPool:
    """多 Cell 调度池. 管理多个 Cell 实例，智能分配 Episode + 自动扩缩容."""

    def __init__(self, max_cells: int = 4, enable_persistence: bool = True,
                 min_cells: int = 1, auto_scale: bool = True):
        self.max_cells = max_cells
        self.min_cells = min_cells
        self.auto_scale_enabled = auto_scale
        self.enable_persistence = enable_persistence
        self.cells: dict[str, CellCoordinator] = {}
        self.cell_states: dict[str, str] = {}  # cell_id -> state mapping
        self.episode_assignments: dict[str, str] = {}  # episode_id -> cell_id
        self.dispatch_log: list[dict] = []
        self.state_manager = CellStateManager() if enable_persistence else None
        # 自动扩缩容阈值
        self.scale_up_threshold = 0.8  # 80% 利用率时扩容
        self.scale_down_threshold = 0.3  # 30% 利用率时缩容
        self.scale_cooldown = 300  # 5分钟冷却期
        self.last_scale_time = 0

    def create_cell(self, cell_id: str | None = None) -> CellCoordinator:
        """创建新的 Cell 实例."""
        if len(self.cells) >= self.max_cells:
            raise RuntimeError(f"Cell pool full: {self.max_cells} cells max")
        cell = CellCoordinator(cell_id=cell_id)
        self.cells[cell.cell_id] = cell
        return cell

    def get_or_create_cell(self, episode_id: str) -> CellCoordinator:
        """获取或创建适合处理该 Episode 的 Cell."""
        # 如果已有分配，直接返回
        if episode_id in self.episode_assignments:
            cell_id = self.episode_assignments[episode_id]
            if cell_id in self.cells:
                return self.cells[cell_id]

        # 寻找空闲 Cell
        idle_cell = self._find_idle_cell()
        if idle_cell:
            self.episode_assignments[episode_id] = idle_cell.cell_id
            return idle_cell

        # 池已满，选择负载最低的 Cell
        if len(self.cells) >= self.max_cells:
            cell = self._select_least_loaded()
            self.episode_assignments[episode_id] = cell.cell_id
            return cell

        # 创建新 Cell
        cell = self.create_cell()
        self.episode_assignments[episode_id] = cell.cell_id
        return cell

    def dispatch_episode(self, episode_id: str, intent: dict) -> dict:
        """智能分配 Episode 到合适的 Cell."""
        # 先检查是否有已分配的 Cell
        if episode_id in self.episode_assignments:
            cell_id = self.episode_assignments[episode_id]
            if cell_id in self.cells:
                cell = self.cells[cell_id]
                result = cell.start_episode(episode_id, intent)
                return {**result, "pool_size": len(self.cells), "strategy": "reuse_assigned"}

        # 寻找空闲 Cell
        idle_cell = self._find_idle_cell()
        if idle_cell:
            self.episode_assignments[episode_id] = idle_cell.cell_id
            result = idle_cell.start_episode(episode_id, intent)
            return {**result, "pool_size": len(self.cells), "strategy": "reuse_idle"}

        # 池已满，选择负载最低的 Cell
        if len(self.cells) >= self.max_cells:
            cell = self._select_least_loaded()
            self.episode_assignments[episode_id] = cell.cell_id
            result = cell.start_episode(episode_id, intent)
            return {**result, "pool_size": len(self.cells), "strategy": "least_loaded"}

        # 创建新 Cell
        cell = self.create_cell()
        self.episode_assignments[episode_id] = cell.cell_id
        result = cell.start_episode(episode_id, intent)

        dispatch_record = {
            "schema": "dispatch/v1",
            "episode_id": episode_id,
            "cell_id": cell.cell_id,
            "pool_size": len(self.cells),
            "timestamp": datetime.now(UTC).isoformat(),
            "strategy": "new_cell",
        }
        self.dispatch_log.append(dispatch_record)

        if self.enable_persistence and self.state_manager:
            self._persist_cell_state(cell)

        return {**result, "pool_size": len(self.cells), "strategy": "new_cell"}

    def complete_episode(self, episode_id: str, verdict: str) -> dict:
        """完成 Episode 并释放 Cell."""
        cell_id = self.episode_assignments.get(episode_id)
        if not cell_id or cell_id not in self.cells:
            return {"ok": False, "error": f"Episode {episode_id} not found in pool"}

        cell = self.cells[cell_id]
        result = cell.complete(verdict)

        # 清理分配记录
        del self.episode_assignments[episode_id]

        # Cell 完成后回到 idle 状态，可被复用
        cell.state = CELL_IDLE
        cell.current_role = None
        cell.episode_id = None

        # 如果空闲 Cell 过多，回收多余的
        self._maybe_recycle_cell(cell_id)

        if self.enable_persistence and self.state_manager:
            self._persist_cell_state(cell)

        return result

    def recover_cell(self, state_id: str) -> CellCoordinator | None:
        """从持久化状态恢复 Cell."""
        if not self.state_manager:
            return None
        state = self.state_manager.load_state(state_id)
        if not state:
            return None
        cell = CellCoordinator()
        restore_cell(cell, state)
        self.cells[cell.cell_id] = cell
        if cell.episode_id:
            self.episode_assignments[cell.episode_id] = cell.cell_id
        return cell

    def get_pool_status(self) -> dict:
        """获取池状态概览."""
        state_counts: dict[str, int] = {}
        for cell in self.cells.values():
            state_counts[cell.state] = state_counts.get(cell.state, 0) + 1
        return {
            "total_cells": len(self.cells),
            "max_cells": self.max_cells,
            "min_cells": self.min_cells,
            "active_episodes": len(self.episode_assignments),
            "utilization": round(len(self.episode_assignments) / max(len(self.cells), 1), 2),
            "auto_scale": self.auto_scale_enabled,
            "state_distribution": state_counts,
            "cells": [
                {
                    "cell_id": c.cell_id,
                    "state": c.state,
                    "episode_id": c.episode_id,
                    "handoff_count": len(c.handoff_log),
                }
                for c in self.cells.values()
            ],
        }

    def scale_up(self, increment: int = 1) -> bool:
        """扩容: 增加 max_cells."""
        new_max = self.max_cells + increment
        if new_max > 16:  # 硬上限
            return False
        self.max_cells = new_max
        self.last_scale_time = datetime.now(UTC).timestamp()
        return True

    def scale_down(self, decrement: int = 1) -> bool:
        """缩容: 减少 max_cells."""
        new_max = self.max_cells - decrement
        if new_max < self.min_cells:
            return False
        self.max_cells = new_max
        self.last_scale_time = datetime.now(UTC).timestamp()
        return True

    def auto_scale(self) -> dict:
        """基于负载自动扩缩容."""
        if not self.auto_scale_enabled:
            return {"action": "disabled"}

        now = datetime.now(UTC).timestamp()
        if now - self.last_scale_time < self.scale_cooldown:
            return {"action": "cooldown"}

        utilization = len(self.episode_assignments) / max(len(self.cells), 1)

        if utilization >= self.scale_up_threshold:
            if self.scale_up():
                return {"action": "scale_up", "max_cells": self.max_cells}
        elif utilization <= self.scale_down_threshold:
            if self.scale_down():
                return {"action": "scale_down", "max_cells": self.max_cells}

        return {"action": "stable", "utilization": round(utilization, 2)}

    def get_metrics(self) -> dict:
        """获取详细指标 (用于监控)."""
        now = datetime.now(UTC)
        return {
            "timestamp": now.isoformat(),
            "pool": {
                "total_cells": len(self.cells),
                "max_cells": self.max_cells,
                "min_cells": self.min_cells,
                "active_episodes": len(self.episode_assignments),
                "utilization": round(len(self.episode_assignments) / max(len(self.cells), 1), 2),
            },
            "dispatch": {
                "total_dispatches": len(self.dispatch_log),
                "recent_1h": sum(
                    1 for d in self.dispatch_log
                    if (now - datetime.fromisoformat(d.get("timestamp", now.isoformat()))).total_seconds() < 3600
                ),
            },
            "auto_scale": {
                "enabled": self.auto_scale_enabled,
                "last_scale": self.last_scale_time,
            },
        }

    def get_cell(self, cell_id: str) -> CellCoordinator | None:
        """获取指定 Cell."""
        return self.cells.get(cell_id)

    def remove_cell(self, cell_id: str) -> bool:
        """移除 Cell."""
        if cell_id in self.cells:
            # 清理相关分配
            episodes_to_remove = [ep for ep, cid in self.episode_assignments.items() if cid == cell_id]
            for ep in episodes_to_remove:
                del self.episode_assignments[ep]
            del self.cells[cell_id]
            return True
        return False

    def _find_idle_cell(self) -> CellCoordinator | None:
        """寻找空闲 Cell."""
        for cell in self.cells.values():
            if cell.state == CELL_IDLE:
                return cell
        return None

    def _select_least_loaded(self) -> CellCoordinator:
        """选择负载最低的 Cell（handoff 最少）."""
        return min(self.cells.values(), key=lambda c: len(c.handoff_log))

    def _determine_strategy(self, cell: CellCoordinator) -> str:
        """确定调度策略."""
        if cell.state == CELL_IDLE:
            return "reuse_idle"
        if len(self.cells) >= self.max_cells:
            return "least_loaded"
        return "new_cell"

    def _maybe_recycle_cell(self, cell_id: str) -> None:
        """回收空闲 Cell（保留至少 1 个）."""
        idle_count = sum(1 for c in self.cells.values() if c.state == CELL_IDLE)
        if idle_count > 1 and cell_id in self.cells:
            del self.cells[cell_id]

    def _persist_cell_state(self, cell: CellCoordinator) -> None:
        """持久化 Cell 状态."""
        if self.state_manager:
            snapshot = snapshot_cell(cell)
            self.state_manager.save_state(snapshot)


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Cell Pool Manager")
    parser.add_argument("--action", choices=["dispatch", "status", "complete", "recover"], default="status")
    parser.add_argument("--episode")
    parser.add_argument("--intent")
    parser.add_argument("--verdict")
    parser.add_argument("--state-id")
    parser.add_argument("--max-cells", type=int, default=4)
    args = parser.parse_args()

    pool = CellPool(max_cells=args.max_cells)

    if args.action == "status":
        print(json.dumps(pool.get_pool_status(), ensure_ascii=False, indent=2))

    elif args.action == "dispatch" and args.episode:
        intent = json.loads(args.intent) if args.intent else {}
        result = pool.dispatch_episode(args.episode, intent)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.action == "complete" and args.episode:
        result = pool.complete_episode(args.episode, args.verdict or "accept")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.action == "recover" and args.state_id:
        cell = pool.recover_cell(args.state_id)
        if cell:
            print(json.dumps({"ok": True, "cell_id": cell.cell_id, "state": cell.state}, ensure_ascii=False))
        else:
            print(json.dumps({"ok": False, "error": "State not found"}, ensure_ascii=False))
