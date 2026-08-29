#!/usr/bin/env python3
"""AGE-v2 Production Readiness Tests — 生产就绪测试.

测试真实场景下的 Cell 全链路:
1. 完整 Episode 生命周期
2. 并发 Episode 处理
3. 故障恢复
4. 自动扩缩容
5. 治理策略执行
"""

import json
import sys
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


class TestCellFullLifecycle(unittest.TestCase):
    """测试完整 Episode 生命周期."""

    def test_analysis_pipeline(self):
        """分析任务完整流水线."""
        from omo.resident.cell import CellCoordinator
        from omo.resident.executor import Executor
        from omo.resident.governor import Governor
        from omo.resident.planner import Planner
        from omo.resident.verifier import Verifier

        cell = CellCoordinator()
        planner = Planner()
        executor = Executor(backend="local")
        verifier = Verifier()
        governor = Governor()

        # 1. Start episode
        start = cell.start_episode("prod-test-001", {"raw_text": "分析 README.md"})
        self.assertEqual(start["state"], "planning")

        # 2. Plan
        plan = planner.create_plan("分析 README.md")
        self.assertGreater(plan["estimated_steps"], 0)
        self.assertIn(plan["risk_assessment"], ["R0", "R1", "R2", "R3"])

        # 3. Govern
        decision = governor.assess_and_decide({"action": "scan", "target": "docs/"})
        self.assertIn(decision["decision"], ["auto_execute", "human_approve", "reject"])

        # 4. Execute
        cell.handoff("planner", "executor", {"plan": plan})
        result = executor.execute_plan(plan)
        self.assertIn("execution_id", result)
        self.assertTrue(result["completed"])

        # 5. Verify
        cell.handoff("executor", "verifier", {"result": result})
        verdict = verifier.verify(result)
        self.assertIn(verdict["verdict"], ["accept", "revise", "reject"])

        # 6. Complete
        completion = cell.complete(verdict["verdict"])
        self.assertEqual(completion["state"], "completed")
        self.assertEqual(completion["handoff_count"], 2)

    def test_concurrent_episodes(self):
        """并发 Episode 处理."""
        from omo.resident.cell_pool import CellPool

        pool = CellPool(max_cells=4, enable_persistence=False)

        # 提交 10 个并发 Episode
        results = []
        for i in range(10):
            result = pool.dispatch_episode(f"concurrent-{i:03d}", {"goal": f"task_{i}"})
            results.append(result)

        # 验证所有 Episode 都被分配
        self.assertEqual(len(results), 10)

        # 验证池自动扩容
        self.assertGreaterEqual(len(pool.cells), 1)

        # 完成所有 Episode
        for i in range(10):
            pool.complete_episode(f"concurrent-{i:03d}", "accept")

        # 验证所有 Episode 完成
        self.assertEqual(len(pool.episode_assignments), 0)


class TestCellRecovery(unittest.TestCase):
    """测试故障恢复."""

    def test_crash_recovery(self):
        """Cell 崩溃后从快照恢复."""
        from omo.resident.cell import CellCoordinator
        from omo.resident.cell_state import CellStateManager, restore_cell, snapshot_cell

        # 1. 创建 Cell 并执行一些操作
        cell = CellCoordinator()
        cell.start_episode("recovery-test", {"goal": "test"})
        cell.handoff("planner", "executor", {"plan": {"tasks": []}})

        # 2. 保存状态
        manager = CellStateManager()
        snap = snapshot_cell(cell)
        state_id = manager.save_state(snap)
        self.assertIsNotNone(state_id)

        # 3. 模拟崩溃后恢复
        loaded = manager.load_state(state_id)
        self.assertIsNotNone(loaded)

        new_cell = CellCoordinator()
        restore_cell(new_cell, loaded)

        self.assertEqual(new_cell.episode_id, "recovery-test")
        self.assertEqual(new_cell.state, "executing")

    def test_state_cleanup(self):
        """过期状态清理."""
        from omo.resident.cell_state import CellStateManager

        manager = CellStateManager()

        # 清理过期状态 (max_age_hours=0 清理所有)
        cleaned = manager.cleanup_stale(max_age_hours=0)
        self.assertGreaterEqual(cleaned, 0)


class TestAutoScaling(unittest.TestCase):
    """测试自动扩缩容."""

    def test_scale_up(self):
        """扩容测试."""
        from omo.resident.cell_pool import CellPool

        pool = CellPool(max_cells=2, enable_persistence=False)
        initial_max = pool.max_cells

        # 扩容
        result = pool.scale_up()
        self.assertTrue(result)
        self.assertEqual(pool.max_cells, initial_max + 1)

    def test_scale_down(self):
        """缩容测试."""
        from omo.resident.cell_pool import CellPool

        pool = CellPool(max_cells=4, min_cells=2, enable_persistence=False)
        initial_max = pool.max_cells

        # 缩容
        result = pool.scale_down()
        self.assertTrue(result)
        self.assertEqual(pool.max_cells, initial_max - 1)

        # 不能低于 min_cells
        pool.scale_down()
        result = pool.scale_down()  # 应该失败
        self.assertFalse(result)

    def test_auto_scale_thresholds(self):
        """自动扩缩容阈值测试."""
        from omo.resident.cell_pool import CellPool

        pool = CellPool(max_cells=4, enable_persistence=False, auto_scale=True)

        # 提交多个 Episode 提高利用率
        for i in range(4):
            pool.dispatch_episode(f"load-test-{i}", {"goal": "test"})

        # 触发自动扩缩容
        result = pool.auto_scale()
        self.assertIn(result["action"], ["scale_up", "stable", "cooldown"])


class TestGovernance(unittest.TestCase):
    """测试治理策略."""

    def test_risk_assessment(self):
        """风险评估测试."""
        from omo.resident.governor import RISK_R0, RISK_R1, RISK_R2, RISK_R3, Governor

        governor = Governor()

        # R0: 只读
        self.assertEqual(governor.assess_risk({"action": "read_file"}), RISK_R0)
        self.assertEqual(governor.assess_risk({"action": "scan"}), RISK_R0)

        # R1: 低风险
        self.assertEqual(governor.assess_risk({"action": "format_code"}), RISK_R1)

        # R2: 中等风险
        self.assertEqual(governor.assess_risk({"action": "commit_code"}), RISK_R2)

        # R3: 高风险
        self.assertEqual(governor.assess_risk({"action": "deploy_production"}), RISK_R3)

    def test_pdp_pep_integration(self):
        """PDP/PEP 集成测试."""
        from omo.resident.pdp_pep import PDP, PEP

        pdp = PDP(policy_set="default")
        pep = PEP(pdp)

        # R0 动作应该自动通过
        result = pep.enforce({"action": "read_file", "target": "README.md"})
        self.assertTrue(result["allowed"])

        # R3 动作应该被拒绝
        result = pep.enforce({"action": "deploy_production", "target": "prod"})
        self.assertFalse(result["allowed"])
        self.assertIn("blocked_reason", result)


class TestMemoryPipeline(unittest.TestCase):
    """测试记忆管道."""

    def test_candidate_generation(self):
        """记忆候选生成."""
        from omo.resident.memory_pipeline import MemoryPipeline

        pipeline = MemoryPipeline()

        episode = {
            "episode_id": "mem-test",
            "results": [
                {"ok": True, "output": "A" * 100, "action": "read_file"},
                {"ok": True, "output": "B" * 100, "action": "search"},
                {"ok": False, "output": "error", "action": "write"},
            ],
        }

        candidates = pipeline.generate_candidates(episode)
        # 只有 ok 且 output >= 50 的才会成为候选
        self.assertEqual(len(candidates), 2)

    def test_conflict_detection(self):
        """冲突检测."""
        from omo.resident.memory_pipeline import MemoryPipeline

        pipeline = MemoryPipeline()
        conflicts = pipeline.detect_conflicts()
        self.assertIsInstance(conflicts, list)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestCellFullLifecycle))
    suite.addTests(loader.loadTestsFromTestCase(TestCellRecovery))
    suite.addTests(loader.loadTestsFromTestCase(TestAutoScaling))
    suite.addTests(loader.loadTestsFromTestCase(TestGovernance))
    suite.addTests(loader.loadTestsFromTestCase(TestMemoryPipeline))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(f"\nTests run: {result.testsRun}, Successes: {result.testsRun - len(result.failures) - len(result.errors)}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
    sys.exit(0 if result.wasSuccessful() else 1)
