#!/usr/bin/env python3
"""Tests for B-slot backend dispatch through the Mesh (SFOP rule 3).

Verifies:
- a successful append still calls fn(args) and returns its status
- a BackendDispatched event is recorded in the Mesh store
- an append failure is a visible non-zero result and does not call fn
- cli.py cell refuses a workspace with no .omo and does not enter the backend
"""

import contextlib
import io
import os
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


class TestDispatchBackendWriteFailure(unittest.TestCase):
    """An event-write failure is visible and does not enter the backend."""

    def test_append_failure_is_visible_and_skips_backend(self):
        class BadStore:
            def append(self, event):
                raise RuntimeError("disk full")

        entered = []

        def spy(argv):
            entered.append(list(argv))
            print("OUT", *argv)
            return 42

        out = io.StringIO()
        err = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = dispatch_backend(BadStore(), "cell", spy, ["plan"])
        self.assertEqual(rc, 1)
        self.assertEqual(entered, [])
        self.assertEqual(out.getvalue(), "")
        self.assertIn("Mesh dispatch event write failed", err.getvalue())
        self.assertIn("disk full", err.getvalue())


class TestCliCellRoutesThroughMesh(unittest.TestCase):
    """cli.py cell branch must fail closed before the backend."""

    def test_cell_branch_uses_dispatch_backend(self):
        cli_src = (REPO / "src/omo/cli.py").read_text()
        self.assertIn('args[0] == "cell"', cli_src)
        self.assertIn("cell_cli", cli_src)
        self.assertIn("dispatch_backend", cli_src)
        self.assertIn("if omo_dir is None", cli_src)
        self.assertNotIn("return cell_main(", cli_src)

    def test_missing_omo_does_not_enter_backend(self):
        import omo.cli as cli
        import omo.resident.cell_cli as cell_cli

        entered = []

        def spy(argv):
            entered.append(list(argv))
            return 0

        previous = os.environ.get("WORKSPACE_ROOT")
        original = cell_cli.main
        cell_cli.main = spy
        try:
            with tempfile.TemporaryDirectory() as tmp:
                os.environ["WORKSPACE_ROOT"] = tmp
                for _ in range(2):
                    err = io.StringIO()
                    with contextlib.redirect_stderr(err):
                        rc = cli.main(["cell", "plan"])
                    self.assertEqual(rc, 1)
                    self.assertIn("no .omo directory", err.getvalue())
        finally:
            cell_cli.main = original
            if previous is None:
                os.environ.pop("WORKSPACE_ROOT", None)
            else:
                os.environ["WORKSPACE_ROOT"] = previous
        self.assertEqual(entered, [])


if __name__ == "__main__":
    unittest.main()
