#!/usr/bin/env python3
"""omo.resident.swarm_mesh — 可扩展异构多机算力网络与自适应切流网关 (Phase 7).

支持物理多机（MBP 旗舰、MacMini 哨兵、Nvidia PC、未来新接入机器）的动态注册、
低开销端口心跳感知、任务阶梯路由 (Tier-based Routing) 与故障优雅降级。
"""

from __future__ import annotations

import json
import logging
import socket
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from omo.omo_paths import code_root

logger = logging.getLogger("omo.resident.swarm_mesh")

NODES_REGISTRY_PATH = code_root() / ".omo/_truth/registry/compute/nodes.yaml"
MACMINI_TAILSCALE_IP = "100.64.110.118"


DYNAMIC_NODES_PATH = code_root() / ".omo/state/compute/dynamic_nodes.json"


@dataclass
class SwarmNodeSpec:
    id: str
    name: str
    role: str  # flagship | sentinel | accelerator | worker | cloud
    host: str
    inference_port: int
    health_path: str = ""
    os: str = "darwin"
    arch: str = "arm64"
    memory_gb: float = 16.0
    vram_gb: float = 0.0
    supported_tiers: list[str] = field(default_factory=lambda: ["tier1_inspection"])
    priority: int = 50
    description: str = ""
    source: str = "ssot"  # ssot | dynamic


# 默认基础已知节点画像 (与 nodes.yaml 和物理硬件对齐，支持运行时无缝扩展)
DEFAULT_NODE_SPECS: list[SwarmNodeSpec] = [
    SwarmNodeSpec(
        id="local-mac",
        name="MBP Flagship (主旗舰机)",
        role="flagship",
        host="127.0.0.1",
        inference_port=1234,
        health_path="/v1/models",
        os="darwin",
        arch="arm64",
        memory_gb=128.0,
        vram_gb=112.0,
        supported_tiers=["tier3_deep_reasoning", "tier2_code_gen", "tier1_inspection"],
        priority=100,
        description="Apple M3 Max 128G 统一内存本地主控",
        source="ssot",
    ),
    SwarmNodeSpec(
        id="macmini",
        name="MacMini Sentinel (常驻哨兵)",
        role="sentinel",
        host=MACMINI_TAILSCALE_IP,
        inference_port=11434,
        health_path="/api/tags",
        os="darwin",
        arch="arm64",
        memory_gb=24.0,
        vram_gb=20.0,
        supported_tiers=["tier1_inspection", "tier1_rag_filter"],
        priority=80,
        description="局域网/Tailnet 常驻哨兵，跑 9B 巡检模型",
        source="ssot",
    ),
    SwarmNodeSpec(
        id="y7000p",
        name="Y7000P PC (Nvidia 高性能工作站)",
        role="accelerator",
        host="100.64.110.120",
        inference_port=11434,
        health_path="/api/tags",
        os="linux",
        arch="x86_64",
        memory_gb=32.0,
        vram_gb=8.0,
        supported_tiers=["tier2_batch_embed", "tier2_code_gen"],
        priority=70,
        description="Nvidia 独立显卡 PC，适合重度批处理推断与蒸馏",
        source="ssot",
    ),
]


def _probe_socket(host: str, port: int, timeout: float = 0.5) -> bool:
    """轻量 socket 探活."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (TimeoutError, ConnectionRefusedError, OSError):
        return False


def _fetch_models(host: str, port: int, path: str = "/v1/models", timeout: float = 0.8) -> list[str]:
    """尝试读取节点当前活跃的模型列表."""
    url = f"http://{host}:{port}{path}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "omo-swarm-prober"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                body = json.loads(resp.read().decode("utf-8"))
                if "data" in body and isinstance(body["data"], list):
                    return [m.get("id", "") for m in body["data"] if m.get("id")]
                if "models" in body and isinstance(body["models"], list):
                    return [m.get("name", "") for m in body["models"] if m.get("name")]
    except Exception:
        pass
    return []


class SwarmComputeMesh:
    """多机异构算力织网管理器 (支持动态新机器接入与持久化)."""

    def __init__(self, registry_file: Path | None = None, dynamic_nodes_file: Path | None = None) -> None:
        from omo.resident import write_path

        self.registry_file = registry_file or NODES_REGISTRY_PATH
        self.dynamic_file = dynamic_nodes_file or write_path(DYNAMIC_NODES_PATH)
        self._nodes: list[SwarmNodeSpec] = list(DEFAULT_NODE_SPECS)
        self._load_from_registry()
        self._load_dynamic_nodes()

    def _load_from_registry(self) -> None:
        """从 SSOT nodes.yaml 动态加载外部与已知机器."""
        if not self.registry_file.exists():
            return
        try:
            import yaml

            docs = list(yaml.safe_load_all(self.registry_file.read_text(encoding="utf-8")))
            # 找到包含 nodes 键的主体文档
            data = next((d for d in docs if isinstance(d, dict) and "nodes" in d), None)
            if not data:
                return
            reg_nodes = data.get("nodes", [])
            for r in reg_nodes:
                nid = r.get("id")
                existing = next((n for n in self._nodes if n.id == nid), None)
                if not existing and nid and nid != "cloud":
                    new_node = SwarmNodeSpec(
                        id=nid,
                        name=r.get("name", f"Node: {nid}"),
                        role=r.get("role", "worker"),
                        host=r.get("host", r.get("tailnet_ip", "127.0.0.1")),
                        inference_port=r.get("port", 11434),
                        health_path=r.get("health_path", "/api/tags"),
                        os=r.get("os", "linux"),
                        arch=r.get("architecture", "x86_64"),
                        memory_gb=float(r.get("memory_gb", 16.0)),
                        vram_gb=float(r.get("vram_gb", 0.0)),
                        supported_tiers=r.get("supported_tiers", ["tier1_inspection"]),
                        priority=int(r.get("priority", 50)),
                        description=r.get("description", "从 nodes.yaml 注册的算力节点"),
                        source="ssot",
                    )
                    self._nodes.append(new_node)
        except Exception as exc:
            logger.warning("Failed to parse nodes registry %s: %s", self.registry_file, exc)

    def _load_dynamic_nodes(self) -> None:
        """从 dynamic_nodes.json 加载未来新接入机器."""
        if not self.dynamic_file.is_file():
            return
        try:
            data = json.loads(self.dynamic_file.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for item in data:
                    node = SwarmNodeSpec(
                        id=item["id"],
                        name=item.get("name", item["id"]),
                        role=item.get("role", "worker"),
                        host=item["host"],
                        inference_port=int(item.get("port", 11434)),
                        health_path=item.get("health_path", "/api/tags"),
                        os=item.get("os", "linux"),
                        arch=item.get("arch", "x86_64"),
                        memory_gb=float(item.get("memory_gb", 16.0)),
                        vram_gb=float(item.get("vram_gb", 0.0)),
                        supported_tiers=item.get("supported_tiers", ["tier1_inspection"]),
                        priority=int(item.get("priority", 50)),
                        description=item.get("description", "动态注册的外部新机器"),
                        source="dynamic",
                    )
                    # 避免重复
                    self._nodes = [n for n in self._nodes if n.id != node.id]
                    self._nodes.append(node)
        except Exception as exc:
            logger.warning("Failed to load dynamic nodes: %s", exc)

    def save_dynamic_nodes(self) -> None:
        """持久化所有动态注册的新节点."""
        try:
            self.dynamic_file.parent.mkdir(parents=True, exist_ok=True)
            dynamic_list = [
                {
                    "id": n.id,
                    "name": n.name,
                    "role": n.role,
                    "host": n.host,
                    "port": n.inference_port,
                    "health_path": n.health_path,
                    "os": n.os,
                    "arch": n.arch,
                    "memory_gb": n.memory_gb,
                    "vram_gb": n.vram_gb,
                    "supported_tiers": n.supported_tiers,
                    "priority": n.priority,
                    "description": n.description,
                }
                for n in self._nodes
                if n.source == "dynamic"
            ]
            self.dynamic_file.write_text(json.dumps(dynamic_list, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to save dynamic nodes: %s", exc)

    def register_dynamic_node(self, node: SwarmNodeSpec, persist: bool = True) -> SwarmNodeSpec:
        """支持运行时热插入新节点并持久化."""
        node.source = "dynamic"
        self._nodes = [n for n in self._nodes if n.id != node.id]
        self._nodes.append(node)
        if persist:
            self.save_dynamic_nodes()
        return node

    def unregister_dynamic_node(self, node_id: str) -> bool:
        """下线或移除动态注册的新节点 (SSOT 节点不可动态移除)."""
        target = next((n for n in self._nodes if n.id == node_id), None)
        if not target or target.source != "dynamic":
            return False
        self._nodes = [n for n in self._nodes if n.id != node_id]
        self.save_dynamic_nodes()
        return True

    def probe_mesh(self) -> dict[str, Any]:
        """全量探测所有节点，返回标准集群拓扑（含向后兼容字段）."""
        node_results = []
        online_count = 0
        total_vram = 0.0

        for node in self._nodes:
            is_port_open = _probe_socket(node.host, node.inference_port)
            models = _fetch_models(node.host, node.inference_port, node.health_path) if is_port_open else []

            if is_port_open:
                status = "online"
                online_count += 1
                total_vram += node.vram_gb
            elif node.role == "flagship":
                status = "degraded"
            else:
                status = "standby"

            node_dict = {
                "id": node.id,
                "name": node.name,
                "role": node.role,
                "host": node.host,
                "port": node.inference_port,
                "os": node.os,
                "arch": node.arch,
                "memory_gb": f"{node.memory_gb:.1f} GB",
                "vram_gb": f"{node.vram_gb:.1f} GB" if node.vram_gb > 0 else "N/A",
                "status": status,
                "active_models": models[:5],
                "supported_tiers": node.supported_tiers,
                "priority": node.priority,
                "description": node.description,
                "source": node.source,
            }
            node_results.append(node_dict)

        mbp_spec = next((n for n in node_results if n["id"] in ("local-mac", "mbp")), None)
        mini_spec = next((n for n in node_results if n["id"] in ("macmini", "macmini-sentinel")), None)

        mbp_legacy = {
            "unified_memory": "128.0 GB",
            "gpu_vram_limit": "112.0 GB",
            "status": mbp_spec["status"] if mbp_spec else "online",
            "inference_port": 1234,
            "active_heavy_models": mbp_spec.get("active_models", []) if mbp_spec else [],
            "aetherforge_port_4000": _probe_socket("127.0.0.1", 4000),
        }
        mini_legacy = {
            "unified_memory": "24.0 GB",
            "status": mini_spec["status"] if mini_spec else "standby",
            "tailnet_ip": MACMINI_TAILSCALE_IP,
            "inference_port": 11434,
            "agent_daemon_online": mini_spec["status"] == "online" if mini_spec else False,
        }

        return {
            "ok": True,
            "total_nodes": len(node_results),
            "online_nodes": online_count,
            "cluster_vram_gb": f"{total_vram:.1f} GB",
            "nodes": node_results,
            "router_policy": "Tier-based Adaptive Offloading (Local Heavy -> Sentinel Light -> Cloud Fallback)",
            "mbp_flagship": mbp_legacy,
            "macmini_sentinel": mini_legacy,
        }

    def route_task(self, tier: str = "tier1_inspection") -> dict[str, Any]:
        """自适应任务分派路由 (Offloading Router).

        策略:
        - 若为轻量任务 (tier1)，优先卸载给非旗舰边缘节点 (sentinel / worker)；
        - 若目标边缘节点全离线，自动 fallback 回本地旗舰主控。
        """
        candidates = [n for n in self._nodes if tier in n.supported_tiers]
        if tier.startswith("tier1"):
            # 轻量任务: 边缘哨兵优先，本地旗舰排后
            candidates.sort(key=lambda x: (x.role == "flagship", -x.priority))
        else:
            # 重度任务: 旗舰/高算力优先
            candidates.sort(key=lambda x: -x.priority)

        # 尝试选择首个在线的候选节点
        for c in candidates:
            if _probe_socket(c.host, c.inference_port):
                is_fallback = c.role == "flagship" and tier.startswith("tier1")
                return {
                    "routed": True,
                    "target_node": c.id,
                    "target_host": c.host,
                    "target_port": c.inference_port,
                    "tier": tier,
                    "fallback": is_fallback,
                }

        # 全部不可达降级回 local-mac
        local = next((n for n in self._nodes if n.id == "local-mac"), self._nodes[0])
        return {
            "routed": True,
            "target_node": local.id,
            "target_host": local.host,
            "target_port": local.inference_port,
            "tier": tier,
            "fallback": True,
            "reason": "All eligible swarm nodes offline; emergency fallback to local host",
        }


_MESH_INSTANCE: SwarmComputeMesh | None = None


def get_swarm_mesh() -> SwarmComputeMesh:
    """全局单例获取 SwarmComputeMesh."""
    global _MESH_INSTANCE
    if _MESH_INSTANCE is None:
        _MESH_INSTANCE = SwarmComputeMesh()
    return _MESH_INSTANCE
