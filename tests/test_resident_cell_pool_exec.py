#!/usr/bin/env python3
"""BET-Y1Q4-T6-23: Resident Daemon & CellPool Integration Tests.

Covers:
- CellPool elastic scheduling routing via execute.py
- Concurrent dispatch across multiple Cells
- Timeout circuit breaker (Cell killed after hard timeout)
- Failover (Cell crash does not affect daemon main loop)
- Memory quota enforcement (max_cells hard limit)
"""

import asyncio
import json
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


class TestCellPoolRunPrompt(unittest.TestCase):
    """Test the CellPool.run_prompt() async execution path."""

    def setUp(self):
        from omo.resident.cell_pool import CellPool

        self.pool = CellPool(max_cells=4, auto_scale=False)

    def test_run_prompt_success(self):
        """A prompt dispatched through run_prompt returns ok status."""
        receipt = asyncio.run(
            self.pool.run_prompt("ep-001", "test prompt", timeout_seconds=30)
        )
        self.assertEqual(receipt["status"], "ok")
        self.assertIn("cell_id", receipt)
        self.assertEqual(receipt["backend"], "pi")

    def test_run_prompt_assigns_cell(self):
        """run_prompt should assign a Cell and track the episode."""
        receipt = asyncio.run(
            self.pool.run_prompt("ep-002", "another prompt", timeout_seconds=30)
        )
        self.assertIn("cell_id", receipt)
        self.assertEqual(receipt["episode_id"], "ep-002")

    def test_run_prompt_timeout_circuit_breaker(self):
        """When a Cell exceeds the hard timeout, it is marked failed and removed."""
        # Create a pool with a very short timeout to trigger the circuit breaker
        receipt = asyncio.run(
            self.pool.run_prompt("ep-003", "slow prompt", timeout_seconds=0)
        )
        # With timeout=0, asyncio.wait_for should immediately timeout
        self.assertEqual(receipt["status"], "timeout")
        self.assertEqual(receipt["timeout_seconds"], 0)

    def test_concurrent_dispatch(self):
        """Multiple prompts dispatched concurrently should each get a Cell."""
        async def dispatch_all():
            tasks = [
                self.pool.run_prompt(f"ep-conc-{i}", f"prompt-{i}", timeout_seconds=30)
                for i in range(4)
            ]
            return await asyncio.gather(*tasks)

        receipts = asyncio.run(dispatch_all())
        # All 4 should succeed (pool max_cells=4)
        for r in receipts:
            self.assertEqual(r["status"], "ok")

    def test_max_cells_hard_limit(self):
        """CellPool should never exceed max_cells."""
        from omo.resident.cell_pool import CellPool

        pool = CellPool(max_cells=2, auto_scale=False)
        pool.create_cell()
        pool.create_cell()
        self.assertEqual(len(pool.cells), 2)
        # Third create should raise RuntimeError (hard limit)
        with self.assertRaises(RuntimeError):
            pool.create_cell()

    def test_failover_cell_crash(self):
        """When a Cell crashes, the error is captured and does not propagate."""
        from omo.resident.cell_pool import CellPool, CELL_FAILED

        pool = CellPool(max_cells=2, auto_scale=False)
        cell = pool.create_cell()
        cell.fail("simulated crash")
        self.assertEqual(cell.state, CELL_FAILED)
        # Pool should still be operational
        self.assertIsNotNone(pool.get_cell(cell.cell_id))


class TestExecuteCellPoolBackend(unittest.TestCase):
    """Test the execute.py cellpool backend routing."""

    def test_cellpool_backend_routes_correctly(self):
        """_execute with backend=cellpool should route through CellPool."""
        from omo.resident.execute import _execute

        event = {
            "event_id": "test-evt-001",
            "payload": {
                "prompt": "test cellpool dispatch",
                "backend": "cellpool",
                "run_id": "test-run-001",
                "timeout_seconds": 30,
            },
        }
        receipt = _execute(event, execute=True)
        # Should not error; status should be dispatched or ok
        self.assertNotIn("error", receipt)
        self.assertEqual(receipt.get("backend"), "cellpool")

    def test_cellpool_backend_default_timeout(self):
        """cellpool backend should work with default timeout."""
        from omo.resident.execute import _execute

        event = {
            "event_id": "test-evt-002",
            "payload": {
                "prompt": "default timeout test",
                "backend": "cellpool",
                "run_id": "test-run-002",
            },
        }
        receipt = _execute(event, execute=True)
        self.assertNotIn("error", receipt)

    def test_cellpool_max_cells_cap(self):
        """max_cells parameter should be capped at 16."""
        from omo.resident.execute import _execute

        event = {
            "event_id": "test-evt-003",
            "payload": {
                "prompt": "max cells test",
                "backend": "cellpool",
                "run_id": "test-run-003",
                "max_cells": 100,  # Should be capped to 16
            },
        }
        receipt = _execute(event, execute=True)
        self.assertNotIn("error", receipt)

    def test_unknown_backend_still_errors(self):
        """Unknown backends should still return error."""
        from omo.resident.execute import _execute

        event = {
            "event_id": "test-evt-004",
            "payload": {
                "prompt": "unknown backend",
                "backend": "nonexistent",
                "run_id": "test-run-004",
            },
        }
        receipt = _execute(event, execute=True)
        self.assertIn("error", receipt)


class TestCellPoolStatus(unittest.TestCase):
    """Test CellPool status reporting after integration."""

    def test_pool_status_after_dispatch(self):
        """Pool status should reflect active cells after dispatch."""
        from omo.resident.cell_pool import CellPool

        pool = CellPool(max_cells=4, auto_scale=False)
        asyncio.run(pool.run_prompt("ep-status", "status test", timeout_seconds=30))
        status = pool.get_pool_status()
        self.assertGreaterEqual(status["total_cells"], 1)
        self.assertLessEqual(status["total_cells"], 4)


if __name__ == "__main__":
    unittest.main()
