"""test_resident_interact_extended — 验证常驻 Agent 扩展 ReAct 探针与动态算力分派."""

import json
from unittest.mock import patch

from omo.resident.interact import (
    execute_react_tool,
    resolve_compute_tier,
)


def test_probe_hardware_compute():
    """验证双机硬件算力拓扑探针."""
    raw = execute_react_tool("probe_hardware_compute", "")
    data = json.loads(raw)
    assert "mbp_flagship" in data
    assert "macmini_sentinel" in data
    assert data["mbp_flagship"]["unified_memory"] == "128.0 GB"
    assert data["mbp_flagship"]["gpu_vram_limit"] == "112.0 GB"
    assert data["macmini_sentinel"]["unified_memory"] == "24.0 GB"


def test_probe_agora_mesh():
    """验证 Agora 蜂群与注册 Agent 探针."""
    raw = execute_react_tool("probe_agora_mesh", "")
    data = json.loads(raw)
    assert "agora_mesh_agents" in data
    agents = [a["id"] for a in data["agora_mesh_agents"]]
    assert "claude-code" in agents
    assert "codex" in agents


def test_probe_bos_capabilities():
    """验证 BOS 服务发现与能力契约探针."""
    raw = execute_react_tool("probe_bos_capabilities", "")
    data = json.loads(raw)
    assert data["bos_uri_count"] >= 5
    assert "bos://compute/aetherforge/infer" in data["core_services"]
    assert "bos://governance/gac/audit" in data["core_services"]


def test_probe_cockpit_commands():
    """验证 Cockpit CLI 命令矩阵探针."""
    raw = execute_react_tool("probe_cockpit_commands", "")
    data = json.loads(raw)
    assert "cockpit_commands" in data
    cmds = data["cockpit_commands"]
    assert "resident" in cmds
    assert "health" in cmds
    assert "gate" in cmds


def test_resolve_compute_tier_mbp_heavy():
    """验证 MBP 128G 本地重算力分派."""
    with (
        patch("omo.resident.interact._probe") as mock_probe,
        patch("omo.resident.interact._probe_lm_studio_active_model", return_value="swift-1.5-qwen3.8-27b-splash"),
    ):
        # 模拟 1234 端口在线
        mock_probe.side_effect = lambda port, *args: port == 1234
        label, model, port = resolve_compute_tier("请审查当前系统的复杂架构", requested_tier="deep")
        assert "MBP 重算力" in label
        assert port == 1234
        assert "qwen" in model.lower()


def test_resolve_compute_tier_cloud_pro():
    """验证长文本或明确指定时升级云端旗舰."""
    with patch("omo.resident.interact._probe") as mock_probe:
        mock_probe.side_effect = lambda port, *args: port == 3425
        label, model, port = resolve_compute_tier("这是一段需要提炼和深度复盘的内容" * 50)
        assert "云端旗舰" in label
        assert port == 3425
        assert "glm-5.3" in model


def test_triage_component_failure_r0():
    """验证针对状态/租约类异常自动判定为 R0 运维级自愈."""
    from omo.resident.interact import triage_component_failure

    res = triage_component_failure(
        component="dashboard_projection",
        status="stale",
        error_log="projection_stale: lease expired at 2026-10-17T08:00:00Z",
        auto_dispatch=True,
        dry_run=True,
    )
    assert res["ok"] is True
    assert res["risk_tier"] == "R0"
    assert "租约" in res["suggested_action"] or "刷新" in res["suggested_action"]
    assert res["dispatched"] is True
    assert res["task_id"] is not None


def test_triage_component_failure_r1():
    """验证针对代码/单测类异常自动判定为 R1 隔离工程自愈."""
    from omo.resident.interact import triage_component_failure

    res = triage_component_failure(
        component="projects/omo/tests",
        status="failed",
        error_log="AssertionError: expected 5 got 4 in test_runner.py",
        auto_dispatch=True,
        dry_run=True,
    )
    assert res["ok"] is True
    assert res["risk_tier"] == "R1"
    assert "Worker" in res["suggested_action"]
    assert res["dispatched"] is True


def test_guarded_web_server_allowlist():
    """resident web 控制面只放行 本机 / 家域 LAN / tailnet; 跨域一律拒。

    背景: 原实现 bind 0.0.0.0 且无来源校验 ⇒ 单位实验域 (192.168.77.0/24)
    的 bot 可无鉴权直读 /api/status (2026-10-28 实测返回 200)。
    """
    from omo.resident.interact import _GuardedHTTPServer

    assert _GuardedHTTPServer.client_allowed("127.0.0.1")
    assert _GuardedHTTPServer.client_allowed("::1")
    assert _GuardedHTTPServer.client_allowed("192.168.17.5")      # 家域 LAN
    assert _GuardedHTTPServer.client_allowed("100.64.110.118")    # tailnet
    assert not _GuardedHTTPServer.client_allowed("192.168.77.2")  # 单位实验域
    assert not _GuardedHTTPServer.client_allowed("203.0.113.9")


def test_guarded_web_server_rejects_invalid_cidr_loudly(monkeypatch):
    """非法 CIDR 必须响亮抛错 —— 不能被静默吞掉 (2026-10-28 首版把 /16 写进来,
    ip_network 抛 ValueError 后被 except 吞掉, 整条家域 LAN 被静默拒绝)。"""
    import pytest

    from omo.resident.interact import _GuardedHTTPServer

    monkeypatch.setattr(_GuardedHTTPServer, "ALLOWED_CIDRS", ("192.168.17.0/16",))
    with pytest.raises(ValueError):
        _GuardedHTTPServer.client_allowed("192.168.17.5")
