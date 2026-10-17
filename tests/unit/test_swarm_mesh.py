"""test_swarm_mesh — 验证可扩展异构算力网络与自适应切流网关 (Phase 7)."""

import json
from unittest.mock import patch

from omo.resident.swarm_mesh import (
    SwarmComputeMesh,
    SwarmNodeSpec,
    get_swarm_mesh,
)


def test_swarm_mesh_default_nodes():
    """验证默认集群包含物理已知节点与兼容性."""
    mesh = SwarmComputeMesh()
    data = mesh.probe_mesh()
    assert data["ok"] is True
    assert data["total_nodes"] >= 3
    assert "nodes" in data
    node_ids = [n["id"] for n in data["nodes"]]
    assert "local-mac" in node_ids
    assert "macmini" in node_ids
    assert "y7000p" in node_ids

    # 兼容历史双机断言
    assert "mbp_flagship" in data
    assert "macmini_sentinel" in data
    assert data["mbp_flagship"]["unified_memory"] == "128.0 GB"
    assert data["macmini_sentinel"]["unified_memory"] == "24.0 GB"


def test_swarm_mesh_dynamic_registration_future_machine():
    """验证未来任意新节点即插即用动态注册."""
    mesh = SwarmComputeMesh()
    initial_count = len(mesh._nodes)

    future_node = SwarmNodeSpec(
        id="cloud-gpu-h100",
        name="Cloud H100 Cluster",
        role="accelerator",
        host="100.64.120.99",
        inference_port=8000,
        os="linux",
        arch="x86_64",
        memory_gb=256.0,
        vram_gb=80.0,
        supported_tiers=["tier3_deep_reasoning", "tier2_batch_embed"],
        priority=95,
        description="未来动态挂载的高性能 H100 云节点",
    )
    mesh.register_dynamic_node(future_node)
    assert len(mesh._nodes) == initial_count + 1

    data = mesh.probe_mesh()
    node_ids = [n["id"] for n in data["nodes"]]
    assert "cloud-gpu-h100" in node_ids


def test_swarm_mesh_tier_routing_and_fallback():
    """验证任务阶梯自适应路由与离线降级机制."""
    mesh = SwarmComputeMesh()

    # 模拟所有远端离线，仅本地在线
    with patch("omo.resident.swarm_mesh._probe_socket") as mock_probe:
        # 仅 local-mac (127.0.0.1) 在线
        mock_probe.side_effect = lambda host, port, *args: host == "127.0.0.1"

        # 调度轻量巡检任务 (Tier 1 优先 macmini) -> 因 macmini 离线自动降级回 local
        route = mesh.route_task("tier1_inspection")
        assert route["routed"] is True
        assert route["fallback"] is True
        assert route["target_node"] == "local-mac"

    # 模拟 macmini 在线
    with patch("omo.resident.swarm_mesh._probe_socket") as mock_probe:
        mock_probe.side_effect = lambda host, port, *args: host == "100.64.110.118"

        route = mesh.route_task("tier1_inspection")
        assert route["routed"] is True
        assert route["fallback"] is False
        assert route["target_node"] == "macmini"
