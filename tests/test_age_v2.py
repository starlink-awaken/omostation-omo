#!/usr/bin/env python3
"""AGE-v2 Unit Tests — Agent Cell 单元测试."""

import json
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


class TestCellCoordinator(unittest.TestCase):
    def setUp(self):
        from omo.resident.cell import CellCoordinator

        self.coordinator = CellCoordinator()

    def test_start_episode(self):
        result = self.coordinator.start_episode("test-001", {"goal": "test"})
        self.assertEqual(result["state"], "planning")
        self.assertEqual(result["current_role"], "planner")

    def test_handoff(self):
        self.coordinator.start_episode("test-001", {"goal": "test"})
        handoff = self.coordinator.handoff("planner", "executor", {"plan": "test"})
        self.assertEqual(handoff["from_role"], "planner")
        self.assertEqual(self.coordinator.state, "executing")

    def test_complete(self):
        self.coordinator.start_episode("test-001", {"goal": "test"})
        result = self.coordinator.complete("accept")
        self.assertEqual(result["state"], "completed")

    def test_fail(self):
        self.coordinator.start_episode("test-001", {"goal": "test"})
        result = self.coordinator.fail("error")
        self.assertEqual(result["state"], "failed")


class TestGovernor(unittest.TestCase):
    def setUp(self):
        from omo.resident.governor import Governor

        self.governor = Governor()

    def test_assess_risk_r0(self):
        self.assertEqual(self.governor.assess_risk({"action": "read_file"}), "R0")

    def test_assess_risk_r3(self):
        self.assertEqual(self.governor.assess_risk({"action": "deploy_production"}), "R3")

    def test_decide_r0_auto(self):
        decision = self.governor.decide("R0", {"action_id": "t1"})
        self.assertEqual(decision["decision"], "auto_execute")

    def test_decide_r3_approve(self):
        decision = self.governor.decide("R3", {"action_id": "t2"})
        self.assertEqual(decision["decision"], "human_approve")


class TestPlanner(unittest.TestCase):
    def setUp(self):
        from omo.resident.planner import Planner

        self.planner = Planner()

    def test_create_plan(self):
        plan = self.planner.create_plan("分析 docs/")
        self.assertGreater(plan["estimated_steps"], 0)


class TestExecutor(unittest.TestCase):
    def setUp(self):
        from omo.resident.executor import Executor

        self.executor = Executor(backend="local")

    def test_execute_scan(self):
        result = self.executor.execute_task({"action": "scan", "target": "docs/"})
        self.assertTrue(result.get("ok"))

    def test_execute_read_file(self):
        result = self.executor.execute_task({"action": "read_file", "target": "README.md"})
        self.assertTrue(result.get("ok"))


class TestVerifier(unittest.TestCase):
    def setUp(self):
        from omo.resident.verifier import Verifier

        self.verifier = Verifier()

    def test_verify_success(self):
        exec_result = {"execution_id": "e1", "results": [{"ok": True, "output": "valid output content here"}]}
        verdict = self.verifier.verify(exec_result)
        self.assertIn(verdict["verdict"], ["accept", "revise", "reject"])

    def test_quick_check(self):
        result = self.verifier.quick_check("valid output")
        self.assertTrue(result["ok"])


class TestMemoryPipeline(unittest.TestCase):
    def setUp(self):
        from omo.resident.memory_pipeline import MemoryPipeline

        self.pipeline = MemoryPipeline()

    def test_generate_candidates(self):
        episode = {"episode_id": "ep1", "results": [{"ok": True, "output": "x" * 100, "action": "read"}]}
        candidates = self.pipeline.generate_candidates(episode)
        self.assertGreater(len(candidates), 0)


class TestReplayFramework(unittest.TestCase):
    def setUp(self):
        from omo.resident.replay import ReplayFramework

        self.framework = ReplayFramework()

    def test_replay_episode(self):
        episode = {"episode_id": "ep1", "intent": {}, "tasks": []}
        result = self.framework.replay_episode(episode)
        self.assertIn("replay_id", result)

    def test_shadow_run(self):
        result = self.framework.shadow_run({"goal": "test"})
        self.assertFalse(result["side_effects"])


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestCellCoordinator))
    suite.addTests(loader.loadTestsFromTestCase(TestGovernor))
    suite.addTests(loader.loadTestsFromTestCase(TestPlanner))
    suite.addTests(loader.loadTestsFromTestCase(TestExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestVerifier))
    suite.addTests(loader.loadTestsFromTestCase(TestMemoryPipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestReplayFramework))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(
        f"\nTests run: {result.testsRun}, Successes: {result.testsRun - len(result.failures) - len(result.errors)}, Failures: {len(result.failures)}, Errors: {len(result.errors)}"
    )
    sys.exit(0 if result.wasSuccessful() else 1)
