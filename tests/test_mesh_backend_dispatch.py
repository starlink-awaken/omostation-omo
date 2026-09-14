#!/usr/bin/env python3
"""Tests for B-slot backend dispatch through the Mesh (SFOP rule 3).

Verifies:
- dispatch_backend is byte-for-byte equivalent to calling fn(args)
- a BackendDispatched event is recorded in the Mesh store
- Mesh write failure never breaks the backend (graceful degradation)
- cli.py cell branch routes through Mesh dispatch_backend
"""

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from omo.workflow_mesh import WorkflowMeshStore, dispatch_backend


def _fake_backend(argv):
    print("OUT", *argv)
    print("ERR", file=sys.stderr)
    return 42


class TestDispatchBackendByteForByte(unittest.TestCase):
    """dispatch_backend must be byte-for-byte equivalent to fn(args)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = WorkflowMeshStore(Path(self.tmp) / ".omo")

    def test_return_code_unchanged(self):
        rc = dispatch_backend(self.store, "cell", _fake_backend, ["plan", "x"])
        self.assertEqual(rc, 42)

    def test_stdout_unchanged(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            dispatch_backend(self.store, "cell", _fake_backend, ["plan", "x"])
        self.assertEqual(buf.getvalue(), "OUT plan x\n")

    def test_stderr_unchanged(self):
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            dispatch_backend(self.store, "cell", _fake_backend, ["plan", "x"])
        self.assertEqual(buf.getvalue(), "ERR\n")

    def test_empty_args(self):
        def echo(argv):
            return len(argv)

        rc = dispatch_backend(self.store, "cell", echo, [])
        self.assertEqual(rc, 0)


class TestDispatchBackendMeshEvent(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = WorkflowMeshStore(Path(self.tmp) / ".omo")

    def test_event_written(self):
        dispatch_backend(self.store, "cell", _fake_backend, ["plan", "x"])
        events = self.store.events()
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["event_type"], "BackendDispatched")
        self.assertEqual(ev["payload"]["backend"], "cell")
        self.assertEqual(ev["payload"]["args"], ["plan", "x"])
        self.assertEqual(ev["producer"], "omo.cell")

    def test_each_dispatch_is_unique(self):
        dispatch_backend(self.store, "cell", _fake_backend, ["a"])
        dispatch_backend(self.store, "cell", _fake_backend, ["b"])
        events = self.store.events()
        self.assertEqual(len(events), 2)
        self.assertNotEqual(events[0]["event_id"], events[1]["event_id"])


class TestDispatchBackendGracefulDegradation(unittest.TestCase):
    """Mesh write failure must never break the backend."""

    def test_backend_survives_store_failure(self):
        class BadStore:
            def append(self, event):
                raise RuntimeError("disk full")

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = dispatch_backend(BadStore(), "cell", _fake_backend, ["plan"])
        self.assertEqual(rc, 42)
        self.assertEqual(buf.getvalue(), "OUT plan\n")


class TestCliCellRoutesThroughMesh(unittest.TestCase):
    """cli.py cell branch must route through Mesh dispatch_backend."""

    def test_cell_branch_uses_dispatch_backend(self):
        cli_src = (REPO / "src/omo/cli.py").read_text()
        self.assertIn('args[0] == "cell"', cli_src)
        self.assertIn("cell_cli", cli_src)
        # 核心断言：cell 分支必须经 Mesh 分发，不得直连 cell_main
        self.assertIn("dispatch_backend", cli_src)


if __name__ == "__main__":
    unittest.main()
