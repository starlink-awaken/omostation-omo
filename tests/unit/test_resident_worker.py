"""test_resident_worker — 验证 Agora A2A 任务真实执行驱动器与自愈引擎."""

import json
import time
from pathlib import Path
from unittest.mock import patch

from omo.resident.worker import (
    execute_agent_task,
    gc_orphan_worktrees,
    load_task_envelope,
)


def test_load_task_envelope(tmp_path):
    """验证读取任务封套."""
    task_file = tmp_path / "TSK-001.json"
    task_data = {
        "task_id": "TSK-001",
        "target_agent": "codex",
        "objective": "修复单测",
        "status": "pending_isolation",
    }
    task_file.write_text(json.dumps(task_data), encoding="utf-8")

    with patch("omo.resident.worker.TASKS_DIR", tmp_path):
        loaded = load_task_envelope("TSK-001")
        assert loaded is not None
        assert loaded["task_id"] == "TSK-001"
        assert loaded["target_agent"] == "codex"


def test_execute_agent_task_missing(tmp_path):
    """验证不存在任务时的失败处理."""
    with patch("omo.resident.worker.TASKS_DIR", tmp_path):
        res = execute_agent_task("TSK-NON-EXISTENT")
        assert res["ok"] is False
        assert "not found" in res["error"]


def test_execute_agent_task_dry_run(tmp_path):
    """验证演练模式下生成证据、更新状态并推入 Action Desk 卡片."""
    task_file = tmp_path / "tasks" / "TSK-999.json"
    task_file.parent.mkdir(parents=True, exist_ok=True)
    task_data = {
        "task_id": "TSK-999",
        "session_id": "session-999",
        "target_agent": "claude-code",
        "objective": "重构状态解析器",
        "status": "worktree_ready",
        "execution_context": {"worktree_path": str(tmp_path / "fake-wt")},
    }
    task_file.write_text(json.dumps(task_data), encoding="utf-8")

    cards_dir = tmp_path / "cards"

    with patch("omo.resident.worker.TASKS_DIR", tmp_path / "tasks"), patch("omo.resident.worker.CARDS_DIR", cards_dir):
        res = execute_agent_task("TSK-999", run_tests=True, dry_run=True)
        assert res["ok"] is True
        assert res["status"] == "ready_for_review"
        assert res["target_agent"] == "claude-code"
        assert "evidence" in res

        # 检查 TaskObject 文件是否已更新状态
        updated = json.loads(task_file.read_text(encoding="utf-8"))
        assert updated["status"] == "ready_for_review"
        assert "diff_summary" in updated["evidence"]

        # 检查 Action Desk 卡片是否生成
        card_file = cards_dir / "task-dispatch-session-999.json"
        assert card_file.exists()
        card_data = json.loads(card_file.read_text(encoding="utf-8"))
        assert card_data["type"] == "agora_task"
        assert card_data["patch_ready"] is True


def test_gc_orphan_worktrees(tmp_path):
    """验证超过保护期的孤儿隔离工作树被安全回收 (Q6 裁定)."""
    parent = tmp_path / "parent_ws"
    parent.mkdir(parents=True, exist_ok=True)

    # 模拟一个 28 小时前的孤儿工作树
    old_wt = parent / "Workspace-isolation-old-task"
    old_wt.mkdir(parents=True, exist_ok=True)
    old_time = time.time() - (28 * 3600)
    import os

    os.utime(str(old_wt), (old_time, old_time))

    # 模拟一个 2 小时前的活跃工作树
    recent_wt = parent / "Workspace-isolation-recent-task"
    recent_wt.mkdir(parents=True, exist_ok=True)

    with patch("omo.resident.worker.WORKSPACE", parent / "Workspace"):
        cleaned = gc_orphan_worktrees(max_age_hours=24, dry_run=False)
        assert len(cleaned) == 1
        assert "old-task" in cleaned[0]
        assert not old_wt.exists()
        assert recent_wt.exists()
