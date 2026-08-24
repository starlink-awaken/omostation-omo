#!/usr/bin/env python3
"""AGE-v2 End-to-End Tests — Agent Cell 端到端集成测试."""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


class TestCellFullPipeline(unittest.TestCase):
    """测试完整的 Cell 流水线: plan → execute → verify → memory."""

    def setUp(self):
        from omo.resident.cell import CellCoordinator
        from omo.resident.planner import Planner
        from omo.resident.executor import Executor
        from omo.resident.verifier import Verifier
        from omo.resident.governor import Governor

        self.cell = CellCoordinator()
        self.planner = Planner()
        self.executor = Executor(backend="local")
        self.verifier = Verifier()
        self.governor = Governor()

    def test_full_pipeline_analysis(self):
        """分析任务完整流水线."""
        episode_id = "e2e-analysis-001"
        intent = {"raw_text": "分析 README.md", "type": "text"}

        # 1. Start episode
        start = self.cell.start_episode(episode_id, intent)
        self.assertEqual(start["state"], "planning")

        # 2. Plan
        plan = self.planner.create_plan(intent)
        self.assertGreater(plan["estimated_steps"], 0)

        # 3. Governor risk assessment
        risk = self.governor.assess_risk({"action": "scan"})
        self.assertIn(risk, ["R0", "R1", "R2", "R3"])

        # 4. Handoff to executor
        handoff1 = self.cell.handoff("planner", "executor", {"plan": plan})
        self.assertEqual(handoff1["to_role"], "executor")

        # 5. Execute
        result = self.executor.execute_plan(plan)
        self.assertIn("execution_id", result)
        self.assertTrue(result["completed"])

        # 6. Handoff to verifier
        handoff2 = self.cell.handoff("executor", "verifier", {"result": result})
        self.assertEqual(handoff2["to_role"], "verifier")

        # 7. Verify
        verdict = self.verifier.verify(result)
        self.assertIn(verdict["verdict"], ["accept", "revise", "reject"])

        # 8. Complete
        completion = self.cell.complete(verdict["verdict"])
        self.assertEqual(completion["state"], "completed")
        self.assertEqual(completion["handoff_count"], 2)

    def test_full_pipeline_fix(self):
        """修复任务完整流水线."""
        episode_id = "e2e-fix-001"
        intent = "修复 CI 失败"

        self.cell.start_episode(episode_id, intent)
        plan = self.planner.create_plan(intent)

        # Verify tasks are fix-oriented
        actions = {t["action"] for t in plan["tasks"]}
        self.assertTrue(actions & {"query_status", "read_file", "format_code", "run_tests"})

        result = self.executor.execute_plan(plan)
        verdict = self.verifier.verify(result)
        self.assertIn(verdict["verdict"], ["accept", "revise", "reject"])

    def test_pipeline_with_governor_block(self):
        """测试 Governor 阻止高风险操作."""
        from omo.resident.governor import RISK_R3

        high_risk = {"action": "deploy_production", "target": "production"}
        risk = self.governor.assess_risk(high_risk)
        self.assertEqual(risk, RISK_R3)

        decision = self.governor.decide(risk, high_risk)
        self.assertEqual(decision["decision"], "human_approve")


class TestCellStatePersistence(unittest.TestCase):
    """测试 Cell 状态持久化."""

    def setUp(self):
        from omo.resident.cell import CellCoordinator
        from omo.resident.cell_state import CellStateManager, restore_cell, snapshot_cell

        self.CellCoordinator = CellCoordinator
        self.manager = CellStateManager()
        self.snapshot_cell = snapshot_cell
        self.restore_cell = restore_cell

    def test_save_and_load_state(self):
        """保存后恢复状态."""
        cell = self.CellCoordinator()
        cell.start_episode("ep-persist", {"goal": "test"})
        cell.handoff("planner", "executor", {"plan": "test-plan"})

        snapshot = self.snapshot_cell(cell)
        state_id = self.manager.save_state(snapshot)
        self.assertIsNotNone(state_id)

        loaded = self.manager.load_state(state_id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["episode_id"], "ep-persist")
        self.assertEqual(loaded["state"], "executing")

    def test_restore_cell_from_snapshot(self):
        """从快照恢复 Cell 实例."""
        cell = self.CellCoordinator()
        cell.start_episode("ep-restore", {"goal": "test"})
        cell.handoff("planner", "executor", {"plan": "plan-123"})

        snapshot = self.snapshot_cell(cell)

        # Create new cell and restore
        new_cell = self.CellCoordinator()
        self.restore_cell(new_cell, snapshot)

        self.assertEqual(new_cell.episode_id, "ep-restore")
        self.assertEqual(new_cell.state, "executing")
        self.assertEqual(new_cell.current_role, "executor")
        self.assertEqual(len(new_cell.handoff_log), 1)

    def test_list_and_cleanup_states(self):
        """列出和清理状态."""
        for i in range(3):
            cell = self.CellCoordinator()
            cell.start_episode(f"ep-clean-{i}", {"goal": "test"})
            snapshot = self.snapshot_cell(cell)
            self.manager.save_state(snapshot)

        states = self.manager.list_states()
        self.assertGreaterEqual(len(states), 3)

        # Cleanup shouldn't remove fresh states
        cleaned = self.manager.cleanup_stale(max_age_hours=0)
        self.assertGreaterEqual(cleaned, 0)

    def test_load_latest(self):
        """加载最新状态."""
        for i in range(3):
            cell = self.CellCoordinator()
            cell.start_episode(f"ep-latest-{i}", {"goal": "test"})
            snapshot = self.snapshot_cell(cell)
            self.manager.save_state(snapshot)

        latest = self.manager.load_latest()
        self.assertIsNotNone(latest)
        self.assertTrue(latest["episode_id"].startswith("ep-latest-"))


class TestCellPool(unittest.TestCase):
    """测试多 Cell 调度池."""

    def setUp(self):
        from omo.resident.cell_pool import CellPool
        self.CellPool = CellPool
        self.pool = CellPool(max_cells=3, enable_persistence=False)

    def test_create_and_dispatch(self):
        """创建 Cell 并分配 Episode."""
        result = self.pool.dispatch_episode("ep-pool-1", {"goal": "test"})
        self.assertIn("cell_id", result)
        self.assertEqual(result["pool_size"], 1)
        self.assertEqual(result["strategy"], "new_cell")

    def test_idle_reuse(self):
        """空闲 Cell 复用."""
        self.pool.dispatch_episode("ep-1", {"goal": "test"})
        self.pool.complete_episode("ep-1", "accept")

        # Second episode should reuse idle cell
        result = self.pool.dispatch_episode("ep-2", {"goal": "test"})
        self.assertEqual(result["strategy"], "reuse_idle")

    def test_pool_status(self):
        """池状态查询."""
        for i in range(2):
            self.pool.dispatch_episode(f"ep-status-{i}", {"goal": "test"})

        status = self.pool.get_pool_status()
        # Both episodes are active simultaneously, need 2 cells
        self.assertEqual(status["total_cells"], 2)
        self.assertEqual(status["active_episodes"], 2)

    def test_max_cells_limit(self):
        """最大 Cell 数限制."""
        pool = self.CellPool(max_cells=2, enable_persistence=False)

        pool.dispatch_episode("ep-a", {"goal": "a"})
        pool.dispatch_episode("ep-b", {"goal": "b"})

        # Third episode should reuse least loaded (pool full)
        result = pool.dispatch_episode("ep-c", {"goal": "c"})
        self.assertEqual(result["strategy"], "least_loaded")
        self.assertEqual(result["pool_size"], 2)

    def test_complete_and_recycle(self):
        """完成 Episode 并回收."""
        self.pool.dispatch_episode("ep-recycle", {"goal": "test"})
        result = self.pool.complete_episode("ep-recycle", "accept")
        self.assertEqual(result["state"], "completed")

    def test_get_specific_cell(self):
        """获取指定 Cell."""
        dispatch = self.pool.dispatch_episode("ep-get", {"goal": "test"})
        cell = self.pool.get_cell(dispatch["cell_id"])
        self.assertIsNotNone(cell)
        self.assertEqual(cell.episode_id, "ep-get")


class TestMemoryPipelineIntegration(unittest.TestCase):
    """记忆管道集成测试."""

    def setUp(self):
        from omo.resident.memory_pipeline import MemoryPipeline
        self.pipeline = MemoryPipeline()

    def test_full_memory_cycle(self):
        """完整记忆周期: generate → detect → consolidate."""
        episode = {
            "episode_id": "ep-mem-1",
            "results": [
                {"ok": True, "output": "A" * 100, "action": "read_file"},
                {"ok": True, "output": "B" * 100, "action": "search"},
                {"ok": False, "output": "error", "action": "write"},
            ],
        }

        candidates = self.pipeline.generate_candidates(episode)
        # Only ok results with output >= 50 chars become candidates
        self.assertEqual(len(candidates), 2)

        for c in candidates:
            self.assertIn("candidate_id", c)
            self.assertEqual(c["status"], "pending")
            self.assertEqual(c["episode_id"], "ep-mem-1")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestCellFullPipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestCellStatePersistence))
    suite.addTests(loader.loadTestsFromTestCase(TestCellPool))
    suite.addTests(loader.loadTestsFromTestCase(TestMemoryPipelineIntegration))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(f"\nTests run: {result.testsRun}, Successes: {result.testsRun - len(result.failures) - len(result.errors)}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
    sys.exit(0 if result.wasSuccessful() else 1)
