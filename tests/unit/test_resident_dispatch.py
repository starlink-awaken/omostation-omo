"""test_resident_dispatch — 验证 Agora 蜂群任务派发与 Worktree 隔离编排器."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from omo.resident.dispatch import (
    _find_recent_dispatch,
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


def _tasks(tmp_path) -> list[dict]:
    return [json.loads(f.read_text(encoding="utf-8")) for f in sorted((tmp_path / "tasks").glob("*.json"))]


def test_dispatch_task_dedupes_same_objective(tmp_path):
    """验证同一 objective 在窗口内只留一档一卡, 第二次派发折叠到既有 task_id."""
    with (
        patch("omo.resident.dispatch.TASKS_DIR", tmp_path / "tasks"),
        patch("omo.resident.dispatch.CARDS_DIR", tmp_path / "cards"),
    ):
        first = dispatch_task(
            objective="自愈修复: dashboard_projection",
            target_agent="claude-code",
            dry_run=True,
        )
        second = dispatch_task(objective="自愈修复:  dashboard_projection", dry_run=True)

        assert "deduplicated" not in first
        assert second["deduplicated"] is True
        assert second["task_id"] == first["task_id"]
        assert second["card_registered"] is False
        assert len(_tasks(tmp_path)) == 1
        assert len(list((tmp_path / "cards").glob("*.json"))) == 1


def test_dispatch_task_dedupe_preserves_consumer_keys(tmp_path):
    """验证折叠返回值仍提供 interact.py 直接下标读取的四个键, 且不虚报已建工作树."""
    obj = "自治自愈 [HEAL_TEMP_RESIDUE]: 清理或归档根目录临时缓存与临时产物"
    with (
        patch("omo.resident.dispatch.TASKS_DIR", tmp_path / "tasks"),
        patch("omo.resident.dispatch.CARDS_DIR", tmp_path / "cards"),
    ):
        dispatch_task(objective=obj, target_agent="codex", dry_run=True)
        res = dispatch_task(objective=obj, dry_run=True)

        assert res["task_id"]
        assert res["session_id"]
        assert res["target_agent"] == "codex"
        assert "重复派发已折叠" in res["worktree"]["message"]
        # dry-run 的 worktree_path 只是估计值, 不在盘上 ⇒ 折叠分支不得报 ok=True
        assert res["worktree"]["ok"] is False


def test_find_recent_dispatch_window_and_corruption(tmp_path):
    """验证判据面四个边界: 窗口内命中、窗口外放行、不同 objective 不误吞、损坏档案不参与判定."""
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    fresh = {
        "task_id": "TSK-FRESH",
        "objective": "自愈修复: projects/omo/tests",
        "created_at": datetime.now(UTC).isoformat(),
    }
    stale = {
        "task_id": "TSK-STALE",
        "objective": "自治自愈 [HEAL_BRANCH_CLAIMS_ARCHIVE]",
        "created_at": (datetime.now(UTC) - timedelta(days=4)).isoformat(),
    }
    (tasks / "TSK-FRESH.json").write_text(json.dumps(fresh, ensure_ascii=False), encoding="utf-8")
    (tasks / "TSK-STALE.json").write_text(json.dumps(stale, ensure_ascii=False), encoding="utf-8")
    (tasks / "broken.json").write_text("{not json", encoding="utf-8")

    assert _find_recent_dispatch("自愈修复:  projects/omo/tests", 259200, tasks)["task_id"] == "TSK-FRESH"
    assert _find_recent_dispatch(stale["objective"], 259200, tasks) is None
    assert _find_recent_dispatch("别的条件", 259200, tasks) is None
    assert _find_recent_dispatch(fresh["objective"], 0, tasks) is None


def test_dispatch_task_dedupe_allows_reissue_after_window(tmp_path):
    """验证 TTL 过期后同 objective 可再建档 —— 窗口是新鲜度判据, 不是永久抑制."""
    obj = "自治自愈 [HEAL_STRAY_TEST_SCRIPTS]: 将根目录游离测试脚本安全归档"
    with (
        patch("omo.resident.dispatch.TASKS_DIR", tmp_path / "tasks"),
        patch("omo.resident.dispatch.CARDS_DIR", tmp_path / "cards"),
    ):
        dispatch_task(objective=obj, dry_run=True)
        for f in (tmp_path / "tasks").glob("*.json"):
            data = json.loads(f.read_text(encoding="utf-8"))
            data["created_at"] = (datetime.now(UTC) - timedelta(days=4)).isoformat()
            f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        again = dispatch_task(objective=obj, dry_run=True)
        assert "deduplicated" not in again
        assert len(_tasks(tmp_path)) == 2
