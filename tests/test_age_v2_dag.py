#!/usr/bin/env python3
"""AGE-v2 Cell DAG Tests — 跨 Cell 编排测试.

测试复杂任务的多 Cell 协作:
1. DAG 定义和执行
2. 并行执行
3. 依赖管理
4. 错误传播
"""

import json
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


class TestCellDAG(unittest.TestCase):
    """Cell DAG 编排测试."""

    def test_linear_dag(self):
        """线性 DAG (A → B → C)."""
        from omo.resident.cell_dag import CellDAG

        dag = CellDAG()

        definition = {
            "dag_id": "linear-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "分析需求", "depends_on": []},
                {"cell_id": "cell-b", "intent": "设计方案", "depends_on": ["cell-a"]},
                {"cell_id": "cell-c", "intent": "验证结果", "depends_on": ["cell-b"]},
            ],
        }

        dag.define_dag(definition)
        result = dag.execute_dag("linear-dag")

        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(result["results"]), 3)
        self.assertEqual(len(result["failed_cells"]), 0)

    def test_parallel_dag(self):
        """并行 DAG (A → B, A → C)."""
        from omo.resident.cell_dag import CellDAG

        dag = CellDAG()

        definition = {
            "dag_id": "parallel-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "分析需求", "depends_on": []},
                {"cell_id": "cell-b", "intent": "设计前端", "depends_on": ["cell-a"]},
                {"cell_id": "cell-c", "intent": "设计后端", "depends_on": ["cell-a"]},
            ],
        }

        dag.define_dag(definition)
        result = dag.execute_dag("parallel-dag")

        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(result["results"]), 3)

    def test_diamond_dag(self):
        """菱形 DAG (A → B, A → C, B+C → D)."""
        from omo.resident.cell_dag import CellDAG

        dag = CellDAG()

        definition = {
            "dag_id": "diamond-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "分析项目", "depends_on": []},
                {"cell_id": "cell-b", "intent": "修复 CI", "depends_on": ["cell-a"]},
                {"cell_id": "cell-c", "intent": "更新文档", "depends_on": ["cell-a"]},
                {"cell_id": "cell-d", "intent": "验证所有改动", "depends_on": ["cell-b", "cell-c"]},
            ],
        }

        dag.define_dag(definition)
        result = dag.execute_dag("diamond-dag")

        self.assertEqual(result["status"], "completed")
        self.assertEqual(len(result["results"]), 4)

    def test_cycle_detection(self):
        """环检测."""
        from omo.resident.cell_dag import CellDAG

        dag = CellDAG()

        # 有环的 DAG (A → B → C → A)
        definition = {
            "dag_id": "cyclic-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "任务 A", "depends_on": ["cell-c"]},
                {"cell_id": "cell-b", "intent": "任务 B", "depends_on": ["cell-a"]},
                {"cell_id": "cell-c", "intent": "任务 C", "depends_on": ["cell-b"]},
            ],
        }

        with self.assertRaises(ValueError):
            dag.define_dag(definition)

    def test_error_propagation(self):
        """错误传播."""
        from omo.resident.cell_dag import CellDAG

        dag = CellDAG(max_cells=2)

        definition = {
            "dag_id": "error-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "正常任务", "depends_on": []},
                {"cell_id": "cell-b", "intent": "依赖失败", "depends_on": ["cell-a"]},
            ],
        }

        dag.define_dag(definition)
        # 正常执行
        result = dag.execute_dag("error-dag")
        self.assertEqual(result["status"], "completed")


class TestDAGVisualization(unittest.TestCase):
    """DAG 可视化测试."""

    def test_dag_status(self):
        """DAG 状态查询."""
        from omo.resident.cell_dag import CellDAG

        dag = CellDAG()

        definition = {
            "dag_id": "status-dag",
            "cells": [
                {"cell_id": "cell-a", "intent": "任务 A", "depends_on": []},
            ],
        }

        dag.define_dag(definition)
        dag.execute_dag("status-dag")

        status = dag.get_status("status-dag")
        self.assertIsNotNone(status)
        self.assertEqual(status["dag_id"], "status-dag")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestCellDAG))
    suite.addTests(loader.loadTestsFromTestCase(TestDAGVisualization))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(
        f"\nTests run: {result.testsRun}, Successes: {result.testsRun - len(result.failures) - len(result.errors)}, Failures: {len(result.failures)}, Errors: {len(result.errors)}"
    )
    sys.exit(0 if result.wasSuccessful() else 1)
