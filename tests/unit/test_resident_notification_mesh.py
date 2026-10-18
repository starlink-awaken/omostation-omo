#!/usr/bin/env python3
"""projects/omo/tests/unit/test_resident_notification_mesh.py — 单元测试 Notification 与 动态节点网格."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from omo.resident.notification import (
    AlertLevel,
    NotificationEvent,
    NotificationHub,
    NotificationThrottler,
    sanitize_text,
)
from omo.resident.swarm_mesh import SwarmComputeMesh, SwarmNodeSpec


def test_sanitize_sensitive_tokens():
    raw = (
        "Error pushing to git with token ghp_1234567890abcdef1234567890abcdef "
        "and OpenAI key sk-1234567890abcdef1234567890 and feishu "
        "https://open.feishu.cn/open-apis/bot/v2/hook/abcdef-1234-5678"
    )
    clean = sanitize_text(raw)
    assert "ghp_" not in clean
    assert "sk-" not in clean
    assert "abcdef-1234-5678" not in clean
    assert "***REDACTED***" in clean


def test_notification_throttling():
    throttler = NotificationThrottler(window_seconds=10.0)
    ev1 = NotificationEvent(
        id="test-1",
        level=AlertLevel.P0_CRITICAL,
        title="Cluster Offline",
        body="All GPU nodes unreachable",
    )
    should1, count1 = throttler.should_send(ev1)
    assert should1 is True
    assert count1 == 0

    # 立即再次发送相同事件
    should2, count2 = throttler.should_send(ev1)
    assert should2 is False
    assert count2 == 1

    # 发送不同事件
    ev2 = NotificationEvent(
        id="test-2",
        level=AlertLevel.P1_WARNING,
        title="New Pitfall Rule",
        body="Worktree guard rule candidate",
    )
    should3, count3 = throttler.should_send(ev2)
    assert should3 is True
    assert count3 == 0


def test_notification_hub_dispatch_and_history():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        hub = NotificationHub(workspace_root=ws)

        with patch.object(hub, "send_macos_notification", return_value=True) as mock_macos:
            ev = NotificationEvent(
                id="test-notify-1",
                level=AlertLevel.P1_WARNING,
                title="Disk space warning",
                body="Disk usage > 85%",
            )
            result = hub.notify(ev, force_macos=True)
            assert result["dispatched"] is True
            assert result["channels"]["macos"] is True
            mock_macos.assert_called_once()

            # 验证审计日志
            history = hub.get_recent_history()
            assert len(history) == 1
            assert history[0]["id"] == "test-notify-1"
            assert history[0]["level"] == "P1_WARNING"


def test_swarm_mesh_dynamic_registration_and_persistence():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        dyn_file = ws / "dynamic_nodes.json"

        mesh = SwarmComputeMesh(dynamic_nodes_file=dyn_file)
        # 初始默认 3 个基础节点
        initial_count = len(mesh._nodes)

        # 注册一台新机器 (如将来的 Mac Studio M4 Max)
        new_node = SwarmNodeSpec(
            id="mac-studio-m4",
            name="Mac Studio M4 Max",
            role="accelerator",
            host="100.64.110.150",
            inference_port=1234,
            os="darwin",
            arch="arm64",
            memory_gb=192.0,
            vram_gb=160.0,
            supported_tiers=["tier3_deep_reasoning"],
            priority=95,
            description="Future Mac Studio high-memory cluster node",
        )
        mesh.register_dynamic_node(new_node)
        assert len(mesh._nodes) == initial_count + 1
        assert dyn_file.is_file()

        # 检查持久化内容
        content = json.loads(dyn_file.read_text(encoding="utf-8"))
        assert len(content) == 1
        assert content[0]["id"] == "mac-studio-m4"
        assert content[0]["vram_gb"] == 160.0

        # 新建一个实例重新加载
        mesh2 = SwarmComputeMesh(dynamic_nodes_file=dyn_file)
        assert len(mesh2._nodes) == initial_count + 1
        loaded = next(n for n in mesh2._nodes if n.id == "mac-studio-m4")
        assert loaded.source == "dynamic"
        assert loaded.vram_gb == 160.0


def test_swarm_mesh_unregister_dynamic_node():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        dyn_file = ws / "dynamic_nodes.json"

        mesh = SwarmComputeMesh(dynamic_nodes_file=dyn_file)
        node = SwarmNodeSpec(
            id="temp-worker-1",
            name="Temporary Worker",
            role="worker",
            host="10.0.0.99",
            inference_port=11434,
        )
        mesh.register_dynamic_node(node)
        assert any(n.id == "temp-worker-1" for n in mesh._nodes)

        # 成功移除动态节点
        ok = mesh.unregister_dynamic_node("temp-worker-1")
        assert ok is True
        assert not any(n.id == "temp-worker-1" for n in mesh._nodes)

        # SSOT 基础节点不可动态移除
        ok_ssot = mesh.unregister_dynamic_node("local-mac")
        assert ok_ssot is False
