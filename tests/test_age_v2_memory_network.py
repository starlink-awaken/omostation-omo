#!/usr/bin/env python3
"""AGE-v2 Memory Network Tests — 跨 Cell 记忆网络测试."""

import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


class TestMemoryNetwork(unittest.TestCase):
    """记忆网络测试."""

    def test_publish_memory(self):
        """发布记忆."""
        from omo.resident.cell_memory_network import MemoryNetwork

        network = MemoryNetwork()
        memory_id = network.publish("cell-001", {
            "content": "Architecture decision: use microservices",
            "type": "semantic",
            "tags": ["architecture", "decision"],
        })

        self.assertIsNotNone(memory_id)
        self.assertTrue(memory_id.startswith("mem-"))

    def test_search_memory(self):
        """搜索记忆."""
        from omo.resident.cell_memory_network import MemoryNetwork

        network = MemoryNetwork()

        # 发布一些记忆
        network.publish("cell-001", {
            "content": "Use Python for backend",
            "tags": ["tech-stack"],
        })
        network.publish("cell-002", {
            "content": "Use React for frontend",
            "tags": ["tech-stack"],
        })

        # 搜索
        results = network.search("Python")
        self.assertGreater(len(results), 0)

    def test_subscribe(self):
        """订阅记忆."""
        from omo.resident.cell_memory_network import MemoryNetwork

        network = MemoryNetwork()
        network.subscribe("cell-001", ["architecture", "decision"])

        subs = network.get_subscriptions("cell-001")
        self.assertIsNotNone(subs)
        self.assertIn("architecture", subs["tags"])

    def test_search_by_tags(self):
        """按标签搜索."""
        from omo.resident.cell_memory_network import MemoryNetwork

        network = MemoryNetwork()

        network.publish("cell-001", {
            "content": "Database optimization completed",
            "tags": ["database", "optimization"],
        })

        results = network.search("", tags=["database"])
        self.assertGreater(len(results), 0)

    def test_cleanup_expired(self):
        """清理过期记忆."""
        from omo.resident.cell_memory_network import MemoryNetwork

        network = MemoryNetwork()
        cleaned = network.cleanup_expired()
        self.assertGreaterEqual(cleaned, 0)

    def test_network_stats(self):
        """网络统计."""
        from omo.resident.cell_memory_network import MemoryNetwork

        network = MemoryNetwork()
        stats = network.get_stats()

        self.assertIn("total_memories", stats)
        self.assertIn("active_memories", stats)
        self.assertIn("cells", stats)
        self.assertIn("subscriptions", stats)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestMemoryNetwork))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(f"\nTests run: {result.testsRun}, Successes: {result.testsRun - len(result.failures) - len(result.errors)}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
    sys.exit(0 if result.wasSuccessful() else 1)
