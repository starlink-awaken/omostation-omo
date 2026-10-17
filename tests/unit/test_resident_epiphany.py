"""test_resident_epiphany — 验证会话即时提炼与自进化飞轮."""

import json
from pathlib import Path
from unittest.mock import patch

from omo.resident.epiphany import (
    detect_correction_signals,
    distill_epiphany_from_text,
)


def test_detect_correction_signals():
    """验证纠错与偏好关键词探测."""
    assert detect_correction_signals("回复优先使用简体中文") is True
    assert detect_correction_signals("严禁在主仓执行硬重置") is True
    assert detect_correction_signals("必须遵循 bar-lint.sh 门禁") is True
    assert detect_correction_signals("今天天气不错，你好啊") is False


def test_distill_epiphany_ignore_noise():
    """验证闲聊且无纠错时安全跳过，不制造垃圾卡片."""
    card = distill_epiphany_from_text("好的，收到，马上开始处理！", force=False)
    assert card is None


def test_distill_epiphany_rule_extraction(tmp_path):
    """验证在模拟环境下规则提取生成候选卡片并推入 Action Desk."""
    transcript = "User: 以后所有的修改必须遵循隔离 worktree 规范，严禁直接在主仓做修改。\nAgent: 明白，收到。\n"

    with (
        patch("omo.resident.epiphany.MEMORY_CANDIDATES_DIR", tmp_path / "cand"),
        patch("omo.resident.epiphany.CARDS_DIR", tmp_path / "cards"),
        patch("omo.resident.interact._probe", return_value=False),
    ):  # 模拟大模型不在线，走规则兜底
        card = distill_epiphany_from_text(transcript, session_id="test-sess-01", force=False)
        assert card is not None
        assert card["type"] == "memory_candidate"
        assert card["category"] == "user_preference"
        assert "隔离" in card["summary"] or "必须" in card["summary"]

        # 检查卡片文件落地
        card_file = tmp_path / "cards" / "epiphany-test-sess-01.json"
        assert card_file.exists()
        saved = json.loads(card_file.read_text(encoding="utf-8"))
        assert saved["session_id"] == "test-sess-01"


def test_distill_epiphany_force(tmp_path):
    """验证 force 参数强制提取."""
    with (
        patch("omo.resident.epiphany.MEMORY_CANDIDATES_DIR", tmp_path / "cand"),
        patch("omo.resident.epiphany.CARDS_DIR", tmp_path / "cards"),
        patch("omo.resident.interact._probe", return_value=False),
    ):
        card = distill_epiphany_from_text("这是一条普通陈述", session_id="force-01", force=True)
        assert card is not None
        assert card["session_id"] == "force-01"


def test_distill_epiphany_deduplication(tmp_path):
    """验证同质卡片复用与去重."""
    cards_dir = tmp_path / "cards"
    cards_dir.mkdir(parents=True, exist_ok=True)
    existing_card = {
        "id": "epiphany-existing-01",
        "type": "memory_candidate",
        "title": "会话备忘: 严禁在主仓硬重置",
        "summary": "严禁在主仓硬重置",
        "status": "pending_approval",
    }
    (cards_dir / "epiphany-existing-01.json").write_text(json.dumps(existing_card), encoding="utf-8")

    with (
        patch("omo.resident.epiphany.MEMORY_CANDIDATES_DIR", tmp_path / "cand"),
        patch("omo.resident.epiphany.CARDS_DIR", cards_dir),
        patch("omo.resident.interact._probe", return_value=False),
    ):
        card = distill_epiphany_from_text("严禁在主仓硬重置", session_id="sess-dup", force=True)
        assert card is not None
        assert card["id"] == "epiphany-existing-01"  # 成功去重复用现有卡片 ID
