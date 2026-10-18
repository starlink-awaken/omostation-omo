#!/usr/bin/env python3
"""tests.unit.test_resident_chat — 常驻 Agent 对话引擎与记忆召回单元测试 (Phase 10)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from omo.resident.chat_engine import (
    ChatMessage,
    ChatSession,
    ResidentChatEngine,
    get_chat_engine,
)
from omo.resident.inference_gateway import InferenceResult


@pytest.fixture
def temp_workspace(tmp_path: Path) -> Path:
    daemon_dir = tmp_path / ".omo/state/daemon"
    daemon_dir.mkdir(parents=True, exist_ok=True)
    mem_dir = tmp_path / ".omo/state/memory"
    mem_dir.mkdir(parents=True, exist_ok=True)

    # 模拟自结晶记忆文件
    crystal_file = mem_dir / "distilled_crystallization.json"
    crystal_file.write_text(
        json.dumps(
            {
                "preferences": [
                    {
                        "id": "pref-lang-zh",
                        "domain": "language",
                        "directive": "回复优先使用简体中文",
                        "confidence": 1.0,
                        "source": "user_global_rule",
                    },
                    {
                        "id": "pref-git-isolation",
                        "domain": "git",
                        "directive": "主工作区只读，交付在隔离 worktree 完成",
                        "confidence": 1.0,
                        "source": "governance_policy",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return tmp_path


def test_chat_message_and_session_serialization():
    msg = ChatMessage(
        role="user",
        content="hello resident",
        metadata={"tier": "tier2_dialog", "node": "local-mac"},
    )
    d = msg.to_dict()
    assert d["role"] == "user"
    assert d["content"] == "hello resident"
    assert d["metadata"]["tier"] == "tier2_dialog"

    recovered = ChatMessage.from_dict(d)
    assert recovered.role == "user"
    assert recovered.content == "hello resident"
    assert recovered.metadata["node"] == "local-mac"

    sess = ChatSession(
        session_id="test-sess",
        created_at="2026-10-19T00:00:00Z",
        updated_at="2026-10-19T00:00:00Z",
        messages=[msg],
    )
    sess_dict = sess.to_dict()
    assert sess_dict["session_id"] == "test-sess"
    assert len(sess_dict["messages"]) == 1

    recovered_sess = ChatSession.from_dict(sess_dict)
    assert recovered_sess.session_id == "test-sess"
    assert len(recovered_sess.messages) == 1
    assert recovered_sess.messages[0].content == "hello resident"


def test_recall_memories(temp_workspace: Path):
    engine = ResidentChatEngine(workspace_root=temp_workspace)
    prefs, pitfalls = engine.recall_memories("请帮我在 git 分支上做修改")

    # 应召回 git 隔离偏好或高置信度全局偏好
    assert len(prefs) >= 1
    assert any("git" in p["domain"] or "language" in p["domain"] for p in prefs)

    # 应召回主工作区只读或相关的避坑硬约束
    assert len(pitfalls) >= 1
    assert any("worktree" in p["id"] or "git" in p["rule"].lower() for p in pitfalls)


def test_chat_session_lifecycle(temp_workspace: Path):
    mock_gateway = MagicMock()
    mock_gateway.chat_completion.return_value = InferenceResult(
        content="收到指令，已进入常驻协同模式。",
        target_node="local-mac",
        target_host="127.0.0.1",
        target_port=1234,
        tier="tier2_dialog",
        fallback_used=False,
        latency_ms=12.5,
        model_used="qwen2.5:32b",
    )

    engine = ResidentChatEngine(workspace_root=temp_workspace, gateway=mock_gateway)

    # 1. 第一轮对话
    msg1 = engine.chat("你好常驻智能体", session_id="sess-001")
    assert msg1.role == "assistant"
    assert "收到指令" in msg1.content
    assert msg1.metadata["target_node"] == "local-mac"
    assert msg1.metadata["tier"] == "tier2_dialog"
    assert len(msg1.metadata["recalled_memories"]) >= 1

    # 检查持久化文件
    session_file = temp_workspace / ".omo/state/daemon/chat_session.json"
    assert session_file.is_file()
    saved = json.loads(session_file.read_text(encoding="utf-8"))
    assert "sess-001" in saved
    assert len(saved["sess-001"]["messages"]) == 2  # 1 user + 1 assistant

    # 2. 第二轮对话 (上下文保留)
    msg2 = engine.chat("请汇报一下当前系统健康度", session_id="sess-001")
    assert msg2.role == "assistant"
    hist = engine.get_history(session_id="sess-001")
    assert len(hist) == 4  # 2 user + 2 assistant

    # 3. 清空历史
    engine.clear_history(session_id="sess-001")
    hist_after = engine.get_history(session_id="sess-001")
    assert len(hist_after) == 0


def test_react_probe_execution_and_guard(temp_workspace: Path):
    mock_gateway = MagicMock()
    # 第一次输出 Action: probe_system_health()，第二次输出最终结论
    mock_gateway.chat_completion.side_effect = [
        InferenceResult(
            content='需要先检查系统状态。\nAction: probe_system_health()\n',
            target_node="local-mac",
            target_host="127.0.0.1",
            target_port=1234,
            tier="tier2_dialog",
            fallback_used=False,
            latency_ms=15.0,
            model_used="qwen2.5:32b",
        ),
        InferenceResult(
            content='当前守护进程运行正常，健康度 100%。',
            target_node="local-mac",
            target_host="127.0.0.1",
            target_port=1234,
            tier="tier2_dialog",
            fallback_used=False,
            latency_ms=10.0,
            model_used="qwen2.5:32b",
        ),
    ]

    with patch("omo.resident.interact.execute_react_tool") as mock_tool:
        mock_tool.return_value = '{"health": "ok", "daemon": {"ok": true}}'
        engine = ResidentChatEngine(workspace_root=temp_workspace, gateway=mock_gateway)
        msg = engine.chat("检查一下系统健康状态", session_id="test-react")

        assert "运行正常" in msg.content
        assert "probe_system_health" in msg.metadata["react_tools_used"]
        mock_tool.assert_called_once_with("probe_system_health", "")


def test_interact_ask_compatibility(temp_workspace: Path):
    from omo.resident.interact import ask, cmd_ask

    with patch("omo.resident.chat_engine.get_chat_engine") as mock_get_engine:
        mock_engine = MagicMock()
        mock_engine.chat.return_value = ChatMessage(
            role="assistant",
            content="自动化兼容测试通过",
            metadata={"target_node": "local-mac", "latency_ms": 5.0},
        )
        mock_get_engine.return_value = mock_engine

        res = ask("测试提问")
        assert res == "自动化兼容测试通过"
        mock_engine.chat.assert_called_once()
