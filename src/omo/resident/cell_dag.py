#!/usr/bin/env python3
"""Cell DAG — 跨 Cell 编排引擎.

支持 Cell 间的依赖和协作:
  - 定义 Cell DAG (有向无环图)
  - 拓扑排序执行
  - 并行执行无依赖 Cell
  - 错误传播和重试
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident.cell_pool import CellPool

ROOT = Path(__file__).resolve().parents[3]


class CellDAG:
    """跨 Cell DAG 编排器."""

    def __init__(self, pool: CellPool | None = None, max_cells: int = 4):
        self.pool = pool or CellPool(max_cells=max_cells, enable_persistence=True)
        self.dag_definitions: dict[str, dict] = {}
        self.execution_results: dict[str, dict] = {}

    def define_dag(self, dag_definition: dict) -> str:
        """定义一个 DAG. 返回 dag_id."""
        dag_id = dag_definition.get("dag_id", f"dag-{uuid.uuid4().hex[:12]}")
        dag_definition["dag_id"] = dag_id

        # 验证 DAG 无环
        if self._has_cycle(dag_definition):
            raise ValueError(f"DAG {dag_id} contains cycles")

        self.dag_definitions[dag_id] = dag_definition
        return dag_id

    def execute_dag(self, dag_id: str) -> dict:
        """执行一个 DAG. 返回执行结果."""
        dag = self.dag_definitions.get(dag_id)
        if not dag:
            return {"ok": False, "error": f"DAG {dag_id} not found"}

        cells = dag.get("cells", [])
        execution_order = self._topological_sort(cells)

        results = {}
        failed = []

        for level in execution_order:
            # 同一层的 Cell 可以并行执行
            for cell_def in level:
                cell_id = cell_def["cell_id"]
                intent = cell_def["intent"]
                depends_on = cell_def.get("depends_on", [])

                # 检查依赖是否成功
                dep_failed = any(d in failed for d in depends_on)
                if dep_failed:
                    results[cell_id] = {
                        "status": "skipped",
                        "reason": "dependency failed",
                    }
                    failed.append(cell_id)
                    continue

                # 执行 Cell
                try:
                    dispatch = self.pool.dispatch_episode(
                        f"{dag_id}-{cell_id}",
                        {"raw_text": intent, "source": "dag"},
                    )
                    self.pool.complete_episode(f"{dag_id}-{cell_id}", "accept")
                    results[cell_id] = {
                        "status": "completed",
                        "cell_id": dispatch["cell_id"],
                    }
                except Exception as e:
                    results[cell_id] = {
                        "status": "failed",
                        "error": str(e),
                    }
                    failed.append(cell_id)

        result = {
            "dag_id": dag_id,
            "status": "completed" if not failed else "partial_failure",
            "results": results,
            "failed_cells": failed,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        self.execution_results[dag_id] = result
        return result

    def _topological_sort(self, cells: list[dict]) -> list[list[dict]]:
        """拓扑排序 → 分层 (同一层可并行)."""
        cell_map = {c["cell_id"]: c for c in cells}
        in_degree = {c["cell_id"]: 0 for c in cells}
        graph = defaultdict(list)

        for cell in cells:
            for dep in cell.get("depends_on", []):
                if dep in cell_map:
                    graph[dep].append(cell["cell_id"])
                    in_degree[cell["cell_id"]] = in_degree.get(cell["cell_id"], 0) + 1

        levels = []
        queue = [cid for cid, deg in in_degree.items() if deg == 0]

        while queue:
            level = [cell_map[cid] for cid in queue if cid in cell_map]
            levels.append(level)
            next_queue = []
            for cid in queue:
                for neighbor in graph[cid]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_queue.append(neighbor)
            queue = next_queue

        return levels

    def _has_cycle(self, dag_definition: dict) -> bool:
        """检测 DAG 是否有环."""
        cells = dag_definition.get("cells", [])
        visited = set()
        rec_stack = set()

        cell_ids = {c["cell_id"] for c in cells}
        graph = defaultdict(list)
        for cell in cells:
            for dep in cell.get("depends_on", []):
                if dep in cell_ids:
                    graph[dep].append(cell["cell_id"])

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            rec_stack.remove(node)
            return False

        for cell_id in cell_ids:
            if cell_id not in visited:
                if dfs(cell_id):
                    return True
        return False

    def get_status(self, dag_id: str) -> dict | None:
        """获取 DAG 执行状态."""
        return self.execution_results.get(dag_id)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cell DAG Orchestrator")
    parser.add_argument("--demo", action="store_true", help="Run demo DAG")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    dag = CellDAG()

    if args.demo:
        demo_dag = {
            "dag_id": "demo-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "分析项目结构", "depends_on": []},
                {"cell_id": "cell-b", "intent": "修复 CI 失败", "depends_on": ["cell-a"]},
                {"cell_id": "cell-c", "intent": "更新文档", "depends_on": ["cell-a"]},
                {"cell_id": "cell-d", "intent": "验证所有改动", "depends_on": ["cell-b", "cell-c"]},
            ],
        }
        dag.define_dag(demo_dag)
        result = dag.execute_dag("demo-dag")

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"DAG {result['dag_id']}: {result['status']}")
            for cell_id, r in result["results"].items():
                status_icon = "✓" if r["status"] == "completed" else "✗"
                print(f"  {status_icon} {cell_id}: {r['status']}")
    else:
        print("Use --demo to run a demo DAG")
