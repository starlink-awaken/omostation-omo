"""BET-Y1Q3-T4-05 (WP2) — resident Executor honesty tests.

Covers spec §4/§6:
- fixed-success actions without admitted context return not_executed (RED→GREEN)
- read-only actions keep working
- receipt-backed effectful execution via run_sandbox_tool (with admitted context)
- replay of the same idempotency identity reuses the receipt (no second effect)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from omo.resident.executor import Executor  # noqa: E402


def _make_store(tmp_path: Path):
    """建一个带 admitted context 的 WorkflowMeshStore (复用 sandbox 测试的装配)。"""
    from omo.workflow_mesh import WorkflowMeshStore

    store = WorkflowMeshStore(tmp_path)
    run = store.start_workflow(workflow_id="wf-wp2", blueprint_id="bp-wp2")
    return store, run


class TestHonestExecutor:
    def test_effectful_without_context_rejected_not_executed(self, tmp_path: Path) -> None:
        """spec 验收 1 (RED 语义): generate_doc 无 admitted context 必须 not_executed。"""
        ex = Executor(omo_dir=tmp_path)
        result = ex.execute_task({"action": "generate_doc", "target": "report.md"})
        assert result["ok"] is False
        assert result["effect"] == "not_executed"
        assert "admitted workflow context required" in result["error"]

    @pytest.mark.parametrize(
        "action",
        ["generate_doc", "create_draft", "format_code", "run_tests", "backup", "snapshot"],
    )
    def test_all_fixed_success_actions_now_reject_without_context(self, tmp_path: Path, action: str) -> None:
        ex = Executor(omo_dir=tmp_path)
        result = ex.execute_task({"action": action, "target": "x"})
        assert result["effect"] == "not_executed", action

    def test_read_only_actions_still_work(self, tmp_path: Path) -> None:
        ex = Executor(omo_dir=tmp_path)
        result = ex.execute_task({"action": "query_status", "target": ""})
        assert result["ok"] is True
        assert "effect" not in result  # 只读不受影响

    def test_execute_plan_completed_only_from_real_results(self, tmp_path: Path) -> None:
        """completed 只由真实结果推导——effectful 无 context 时 completed=False。"""
        ex = Executor(omo_dir=tmp_path)
        plan = {
            "plan_id": "p1",
            "tasks": [
                {"action": "query_status", "target": ""},
                {"action": "generate_doc", "target": "x.md"},
            ],
        }
        result = ex.execute_plan(plan)
        assert result["completed"] is False  # 假成功不再聚合为 completed

    def test_effectful_with_admitted_context_is_receipt_backed(self, tmp_path: Path) -> None:
        """spec 验收 3: 真 mesh admission → receipt-backed 执行 → 幂等重放。

        装配复用 tests/test_sandbox_tool_runner.py 的 _context helper (同一条
        WorkflowRequested → Admitted → Dispatched 事件链)。
        """
        from tests.test_sandbox_tool_runner import _context

        context = _context(tmp_path, "run-wp2")
        context["now"] = "2026-08-03T00:00:10Z"  # grant 有效期内 (与装配时间一致)
        ex = Executor(omo_dir=tmp_path)
        r1 = ex.execute_task({"action": "generate_doc", "target": "doc.md", "admitted_context": context})
        assert r1.get("effect") == "executed", r1
        assert r1["ok"] is True
        assert r1.get("receipt_digest")
        # 重放: 同 identity → 复用 receipt (spec 验收 4)
        r2 = ex.execute_task({"action": "generate_doc", "target": "doc.md", "admitted_context": context})
        assert r2.get("receipt_digest") == r1.get("receipt_digest")
