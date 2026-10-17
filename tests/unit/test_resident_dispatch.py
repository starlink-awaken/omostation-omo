"""test_resident_dispatch — 验证 Agora 蜂群任务派发与 Worktree 隔离编排器."""

import json
from pathlib import Path
from unittest.mock import patch

from omo.resident.dispatch import (
    create_task_envelope,
    dispatch_task,
    get_available_agents,
    match_agent_for_objective,
    prepare_isolated_worktree,
)


def test_get_available_agents():
    """验证从能力注册表或兜底清单获取可用 Agent."""
    agents = get_available_agents()
    assert isinstance(agents, list)
    agent_ids = {a["id"] for a in agents if "id" in a}
    assert "claude-code" in agent_ids or "codex" in agent_ids


def test_match_agent_for_objective():
    """验证基于目标意图自动路由至最佳 Agent."""
    assert match_agent_for_objective("重构核心架构并更新 MCP") == "claude-code"
    assert match_agent_for_objective("修复单测与 bug 实现") == "codex"
    assert match_agent_for_objective("文档格式化与 typo 清理") == "crush"
    assert match_agent_for_objective("普通日常代码需求") == "codex"


def test_create_task_envelope():
    """验证标准 TaskObject v1.0 任务封套生成."""
    envelope = create_task_envelope(
        objective="实现双机电源感知",
        target_agent="codex",
        risk_tier="R2",
        session_id="patrol-power-fix",
    )
    assert envelope["$schema"] == "https://ecos.internal/schemas/task-object-v1.json"
    assert envelope["task_id"].startswith("TSK-")
    assert envelope["target_agent"] == "codex"
    assert envelope["risk_tier"] == "R2"
    assert envelope["session_id"] == "patrol-power-fix"
    assert envelope["constraints"]["main_workspace_read_only"] is True
    assert envelope["execution_context"]["branch"] == "work/patrol-power-fix"
    assert "pitfall_rules" in envelope["constraints"]
    assert len(envelope["constraints"]["pitfall_rules"]) >= 1
    assert "system_guidance" in envelope
    assert "【执行守则与防御约束】" in envelope["system_guidance"]


def test_prepare_isolated_worktree_dry_run():
    """验证 dry-run 下物理隔离工作区规划."""
    ok, msg, path = prepare_isolated_worktree("session-123", dry_run=True)
    assert ok is True
    assert "[dry-run]" in msg
    assert path is not None
    assert "session-123" in path


def test_dispatch_task_dry_run(tmp_path):
    """验证全链路任务派发与 Action Desk 卡片注册."""
    with (
        patch("omo.resident.dispatch.TASKS_DIR", tmp_path / "tasks"),
        patch("omo.resident.dispatch.CARDS_DIR", tmp_path / "cards"),
    ):
        res = dispatch_task(
            objective="测试全自动派发",
            target_agent="claude-code",
            risk_tier="R1",
            auto_worktree=True,
            dry_run=True,
        )
        assert res["ok"] is True
        assert res["target_agent"] == "claude-code"
        assert res["card_registered"] is True
        assert res["worktree"]["ok"] is True

        # 检查持久化产物
        task_files = list((tmp_path / "tasks").glob("*.json"))
        assert len(task_files) == 1
        saved_task = json.loads(task_files[0].read_text(encoding="utf-8"))
        assert saved_task["target_agent"] == "claude-code"

        card_files = list((tmp_path / "cards").glob("*.json"))
        assert len(card_files) == 1
        saved_card = json.loads(card_files[0].read_text(encoding="utf-8"))
        assert saved_card["type"] == "agora_task"
        assert saved_card["status"] == "pending_approval"
