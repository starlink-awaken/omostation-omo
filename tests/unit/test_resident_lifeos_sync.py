"""test_resident_lifeos_sync — 验证 Action Desk 签署与 LifeOS 7.40.4 权威记忆注入流."""

import json
from pathlib import Path
from unittest.mock import patch

from omo.resident.lifeos_sync import (
    approve_pending_card,
    create_new_topic_file,
    find_related_topic_file,
    inspect_memory_budget,
    merge_belief_into_topic_file,
    sync_card_to_lifeos,
)


def test_inspect_memory_budget_normal(tmp_path):
    """验证正常水位下的预算检测."""
    index_file = tmp_path / "MEMORY.md"
    index_file.write_text("- [规则1](rule1.md) — 规则描述\n- [规则2](rule2.md) — 规则描述\n", encoding="utf-8")
    with patch("omo.resident.lifeos_sync.INDEX_FILE", index_file):
        budget = inspect_memory_budget()
        assert budget["ok"] is True
        assert budget["exists"] is True
        assert budget["line_count"] == 2
        assert budget["lines_remaining"] == 190


def test_inspect_memory_budget_overflow(tmp_path):
    """验证超出 192 行预算时的阻断检测."""
    index_file = tmp_path / "MEMORY.md"
    lines = [f"- [规则{i}](rule{i}.md) — 描述" for i in range(200)]
    index_file.write_text("\n".join(lines), encoding="utf-8")
    with patch("omo.resident.lifeos_sync.INDEX_FILE", index_file):
        budget = inspect_memory_budget()
        assert budget["ok"] is False
        assert budget["line_count"] == 200
        assert budget["lines_remaining"] == 0


def test_find_related_topic_file(tmp_path):
    """验证同族主题文件模糊查找."""
    pref_file = tmp_path / "feedback-chinese-first.md"
    pref_file.write_text("# 中文回复偏好\n", encoding="utf-8")
    with patch("omo.resident.lifeos_sync.MEMORY_DIR", tmp_path):
        target = find_related_topic_file("user_preference", ["回复", "中文"])
        assert target is not None
        assert target.name == "feedback-chinese-first.md"


def test_merge_belief_into_topic_file(tmp_path):
    """验证以追加形式合流进已有主题文件."""
    topic_file = tmp_path / "existing-topic.md"
    topic_file.write_text("# 既有主题\n\n初始内容\n", encoding="utf-8")
    ok = merge_belief_into_topic_file(
        topic_path=topic_file,
        title="新增代码规范",
        summary="严禁在主仓执行硬重置",
        details="遵循 PASW 隔离原则",
        date_str="2026-10-16",
    )
    assert ok is True
    content = topic_file.read_text(encoding="utf-8")
    assert "### 补充记忆: 新增代码规范" in content
    assert "严禁在主仓执行硬重置" in content


def test_create_new_topic_file(tmp_path):
    """验证新主题文件创建及索引挂载."""
    index_file = tmp_path / "MEMORY.md"
    index_file.write_text("# 记忆索引\n\n", encoding="utf-8")
    with (
        patch("omo.resident.lifeos_sync.MEMORY_DIR", tmp_path),
        patch("omo.resident.lifeos_sync.INDEX_FILE", index_file),
    ):
        ok, path = create_new_topic_file(
            slug="rule-new-test",
            title="全新测试规则",
            summary="测试概要",
            details="测试细节",
            date_str="2026-10-16",
        )
        assert ok is True
        assert path.exists()
        assert "# 全新测试规则" in path.read_text(encoding="utf-8")
        assert "- [全新测试规则](rule-new-test.md) — 测试概要" in index_file.read_text(encoding="utf-8")


def test_sync_card_to_lifeos(tmp_path):
    """验证完整的卡片批准合流生命周期."""
    index_file = tmp_path / "MEMORY.md"
    index_file.write_text("# 索引\n", encoding="utf-8")

    card_file = tmp_path / "card-123.json"
    card_data = {
        "id": "card-123",
        "title": "优先中文回复",
        "category": "user_preference",
        "summary": "保持中文交互",
        "details": "代码与标识符保持英文",
        "status": "pending_approval",
        "created_at": "2026-10-16T12:00:00Z",
    }
    card_file.write_text(json.dumps(card_data), encoding="utf-8")

    with (
        patch("omo.resident.lifeos_sync.MEMORY_DIR", tmp_path),
        patch("omo.resident.lifeos_sync.INDEX_FILE", index_file),
    ):
        res = sync_card_to_lifeos(card_file)
        assert res["ok"] is True
        assert res["title"] == "优先中文回复"

        # 检查卡片状态是否变为 approved
        updated_card = json.loads(card_file.read_text(encoding="utf-8"))
        assert updated_card["status"] == "approved"
        assert "synced_at" in updated_card
