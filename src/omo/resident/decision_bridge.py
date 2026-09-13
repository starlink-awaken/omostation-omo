"""decision_bridge.py — 因果决策图与 PROV-O 审计导出 (BET-Y1Q4-T5-05).

文件契约: .omo/state/decision-graph/graph.jsonl
  - 每行一个节点 (DecisionNode) 或一条边 (CausalEdge)
  - Cockport handler 与本模块以文件契约解耦, 不互相 import
  - 追加式写入, 无覆盖/删除

设计原则:
  - 确定性算法, 零模型调用
  - 文件契约解耦 (omo ↔ cockpit 不 import)
  - PROV-O 审计导出符合 W3C 标准
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from omo.resident import WORKSPACE

# ── 常量 ──────────────────────────────────────────────

GRAPH_DIR = WORKSPACE / ".omo" / "state" / "decision-graph"
GRAPH_FILE = GRAPH_DIR / "graph.jsonl"

NodeKind = Literal["patrol", "healing", "decision"]
RelationType = Literal["CAUSED", "INFLUENCED"]

# W3C PROV-O 命名空间
_PROV_NS = "http://www.w3.org/ns/prov#"
_DCT_NS = "http://purl.org/dc/terms/"
_XSD_NS = "http://www.w3.org/2001/XMLSchema#"

_KIND_TO_PROV: dict[str, str] = {
    "patrol": f"{_PROV_NS}Activity",
    "healing": f"{_PROV_NS}Activity",
    "decision": f"{_PROV_NS}Entity",
}


# ── 数据模型 ──────────────────────────────────────────


class DecisionNode:
    """因果决策图中的节点.

    Attributes:
        node_id: 唯一标识符 (uuid4 hex[:12])
        kind: 节点类型 — patrol (巡检) | healing (自愈) | decision (决策)
        actor: 执行者标识 (agent name)
        action: 动作描述
        ts: ISO-8601 UTC 时间戳
        context: 关联上下文 (trace_id / event_id 等)
    """

    __slots__ = ("node_id", "kind", "actor", "action", "ts", "context")

    def __init__(
        self,
        node_id: str | None = None,
        kind: NodeKind = "decision",
        actor: str = "resident",
        action: str = "",
        ts: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.node_id = node_id or uuid.uuid4().hex[:12]
        self.kind = kind
        self.actor = actor
        self.action = action
        self.ts = ts or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.context = context or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "_type": "node",
            "node_id": self.node_id,
            "kind": self.kind,
            "actor": self.actor,
            "action": self.action,
            "ts": self.ts,
            "context": self.context,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DecisionNode:
        return cls(
            node_id=d.get("node_id"),
            kind=d.get("kind", "decision"),
            actor=d.get("actor", "resident"),
            action=d.get("action", ""),
            ts=d.get("ts"),
            context=d.get("context", {}),
        )

    def __repr__(self) -> str:
        return f"DecisionNode({self.node_id}, kind={self.kind}, actor={self.actor})"


class CausalEdge:
    """因果边 — 节点间因果关系.

    Attributes:
        src: 源节点 ID
        dst: 目标节点 ID
        relation: CAUSED (强因果) | INFLUENCED (弱影响)
    """

    __slots__ = ("src", "dst", "relation")

    def __init__(self, src: str, dst: str, relation: RelationType = "CAUSED") -> None:
        self.src = src
        self.dst = dst
        self.relation = relation

    def to_dict(self) -> dict[str, str]:
        return {"_type": "edge", "src": self.src, "dst": self.dst, "relation": self.relation}

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> CausalEdge:
        return cls(
            src=d["src"],
            dst=d["dst"],
            relation=d.get("relation", "CAUSED"),
        )

    def __repr__(self) -> str:
        return f"CausalEdge({self.src} -{self.relation}-> {self.dst})"


# ── 图存储引擎 ────────────────────────────────────────


class DecisionGraph:
    """因果决策图 — append-only 存储 + 查询.

    持久化: .omo/state/decision-graph/graph.jsonl
    文件契约: Cockport handler 直接读取此文件, 不 import 本模块。
    """

    def __init__(self, graph_file: Path | None = None) -> None:
        self._path = Path(graph_file) if graph_file else GRAPH_FILE
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._nodes: dict[str, DecisionNode] = {}
        self._edges: list[CausalEdge] = []
        self._adjacency: dict[str, list[CausalEdge]] = {}  # src → [edges]
        self._reverse: dict[str, list[CausalEdge]] = {}    # dst → [edges]
        self._load()

    # ── 持久化 ──────────────────────────────────────

    def _load(self) -> None:
        """从 graph.jsonl 加载所有节点与边."""
        if not self._path.exists():
            return
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if d.get("_type") == "node":
                    node = DecisionNode.from_dict(d)
                    self._nodes[node.node_id] = node
                elif d.get("_type") == "edge":
                    edge = CausalEdge.from_dict(d)
                    self._edges.append(edge)
                    self._adjacency.setdefault(edge.src, []).append(edge)
                    self._reverse.setdefault(edge.dst, []).append(edge)

    def _append(self, record: dict[str, Any]) -> None:
        """追加写入一条记录到 graph.jsonl."""
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    # ── 写入操作 ────────────────────────────────────

    def record_patrol(
        self,
        actor: str,
        action: str,
        context: dict[str, Any] | None = None,
        node_id: str | None = None,
    ) -> DecisionNode:
        """记录一次巡检动作, 生成图原生节点.

        Args:
            actor: 执行巡检的 agent 名称
            action: 巡检动作描述
            context: 关联上下文 (trace_id, scan_id 等)
            node_id: 可选的显式 ID (用于外部引用)

        Returns:
            创建的 DecisionNode
        """
        node = DecisionNode(
            node_id=node_id,
            kind="patrol",
            actor=actor,
            action=action,
            context=context,
        )
        self._nodes[node.node_id] = node
        self._append(node.to_dict())
        return node

    def record_healing(
        self,
        actor: str,
        action: str,
        triggered_by: str,
        context: dict[str, Any] | None = None,
        node_id: str | None = None,
    ) -> DecisionNode:
        """记录一次自愈动作, 生成节点与因果连线.

        自愈节点 CAUSED 自触发它的巡检节点。
        若 context 中指定 affected_entities, 则对每个实体
        创建 INFLUENCED 边。

        Args:
            actor: 执行自愈的 agent 名称
            action: 自愈动作描述
            triggered_by: 触发此自愈的巡检节点 ID
            context: 关联上下文 (含可选 affected_entities)
            node_id: 可选的显式 ID

        Returns:
            创建的 DecisionNode
        """
        node = DecisionNode(
            node_id=node_id,
            kind="healing",
            actor=actor,
            action=action,
            context=context,
        )
        self._nodes[node.node_id] = node
        self._append(node.to_dict())

        # 巡检 CAUSED 自愈
        edge = CausalEdge(src=triggered_by, dst=node.node_id, relation="CAUSED")
        self._add_edge(edge)

        # 自愈 INFLUENCED 受影响实体 (每个实体一个虚拟节点)
        affected = (context or {}).get("affected_entities", [])
        for entity_ref in affected:
            entity_node = DecisionNode(
                node_id=f"entity-{uuid.uuid5(uuid.NAMESPACE_URL, entity_ref).hex[:10]}",
                kind="decision",
                actor="entity",
                action=f"affected: {entity_ref}",
                context={"entity_ref": entity_ref},
            )
            if entity_node.node_id not in self._nodes:
                self._nodes[entity_node.node_id] = entity_node
                self._append(entity_node.to_dict())
            infl_edge = CausalEdge(
                src=node.node_id,
                dst=entity_node.node_id,
                relation="INFLUENCED",
            )
            self._add_edge(infl_edge)

        return node

    def record_decision(
        self,
        actor: str,
        action: str,
        triggered_by: str | list[str] | None = None,
        context: dict[str, Any] | None = None,
        node_id: str | None = None,
    ) -> DecisionNode:
        """记录一个决策节点, 可选关联触发者.

        Args:
            actor: 决策者
            action: 决策描述
            triggered_by: 触发此决策的节点 ID (单个或列表)
            context: 关联上下文
            node_id: 可选的显式 ID

        Returns:
            创建的 DecisionNode
        """
        node = DecisionNode(
            node_id=node_id,
            kind="decision",
            actor=actor,
            action=action,
            context=context,
        )
        self._nodes[node.node_id] = node
        self._append(node.to_dict())

        if triggered_by:
            triggers = triggered_by if isinstance(triggered_by, list) else [triggered_by]
            for trig in triggers:
                edge = CausalEdge(src=trig, dst=node.node_id, relation="CAUSED")
                self._add_edge(edge)

        return node

    def _add_edge(self, edge: CausalEdge) -> None:
        """内部: 添加边到内存索引并持久化."""
        self._edges.append(edge)
        self._adjacency.setdefault(edge.src, []).append(edge)
        self._reverse.setdefault(edge.dst, []).append(edge)
        self._append(edge.to_dict())

    # ── 查询操作 ────────────────────────────────────

    def ancestors(self, node_id: str) -> list[dict[str, Any]]:
        """返回指定节点的完整因果祖先链 (BFS 反向遍历).

        从目标节点出发, 沿边的反向 (dst→src) 遍历,
        返回按深度排序的祖先节点列表。

        Args:
            node_id: 目标节点 ID

        Returns:
            List of dicts with keys: node_id, kind, actor, action,
            relation (from ancestor to descendant), depth
        """
        if node_id not in self._nodes:
            return []

        result: list[dict[str, Any]] = []
        visited: set[str] = set()
        # BFS: [(current_id, depth)]
        queue: list[tuple[str, int]] = [(node_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)
            # 不添加 depth==0 的起始节点到结果, 但处理其边

            # 查找指向 current 的边 (反向)
            for edge in self._reverse.get(current_id, []):
                src_id = edge.src
                if src_id in visited:
                    continue
                src_node = self._nodes.get(src_id)
                if src_node is None:
                    continue
                result.append(
                    {
                        "node_id": src_id,
                        "kind": src_node.kind,
                        "actor": src_node.actor,
                        "action": src_node.action,
                        "relation": edge.relation,
                        "depth": depth + 1,
                    }
                )
                queue.append((src_id, depth + 1))

        return result

    def downstream(self, node_id: str) -> list[dict[str, Any]]:
        """返回指定节点的下游影响面拓扑 (BFS 正向遍历, 分层).

        Args:
            node_id: 源节点 ID

        Returns:
            List of dicts with keys: node_id, kind, actor, action,
            relation, depth
        """
        if node_id not in self._nodes:
            return []

        result: list[dict[str, Any]] = []
        visited: set[str] = set()
        queue: list[tuple[str, int]] = [(node_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)
            # 不添加 depth==0 的起始节点到结果, 但处理其边

            for edge in self._adjacency.get(current_id, []):
                dst_id = edge.dst
                if dst_id in visited:
                    continue
                dst_node = self._nodes.get(dst_id)
                if dst_node is None:
                    continue
                result.append(
                    {
                        "node_id": dst_id,
                        "kind": dst_node.kind,
                        "actor": dst_node.actor,
                        "action": dst_node.action,
                        "relation": edge.relation,
                        "depth": depth + 1,
                    }
                )
                queue.append((dst_id, depth + 1))

        return result

    def get_node(self, node_id: str) -> DecisionNode | None:
        """获取指定节点, 不存在返回 None."""
        return self._nodes.get(node_id)

    def summary(self) -> dict[str, Any]:
        """返回图的统计摘要.

        Returns:
            Dict with node/edge counts, kind breakdown, relation breakdown.
        """
        kind_counts: dict[str, int] = {}
        for n in self._nodes.values():
            kind_counts[n.kind] = kind_counts.get(n.kind, 0) + 1

        relation_counts: dict[str, int] = {}
        for e in self._edges:
            relation_counts[e.relation] = relation_counts.get(e.relation, 0) + 1

        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "by_kind": kind_counts,
            "by_relation": relation_counts,
        }

    # ── PROV-O 导出 ─────────────────────────────────

    def export_provo(self, path: str | Path, fmt: Literal["turtle", "jsonld"] = "turtle") -> None:
        """导出图数据为 W3C PROV-O 标准格式.

        Args:
            path: 输出文件路径
            fmt: "turtle" 或 "jsonld"

        Raises:
            ValueError: 如果 fmt 不是 turtle/jsonld
        """
        path = Path(path)
        if fmt not in ("turtle", "jsonld"):
            raise ValueError(f"Unsupported format: {fmt!r}. Use 'turtle' or 'jsonld'.")

        if fmt == "turtle":
            self._export_provo_turtle(path)
        else:
            self._export_provo_jsonld(path)

    def _export_provo_turtle(self, path: Path) -> None:
        """导出为 PROV-O Turtle 格式."""
        lines: list[str] = [
            "# W3C PROV-O Audit Export",
            f"# Generated: {datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}",
            f"# Nodes: {len(self._nodes)}, Edges: {len(self._edges)}",
            "",
            "@prefix prov: <http://www.w3.org/ns/prov#> .",
            "@prefix dct: <http://purl.org/dc/terms/> .",
            "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
            "@prefix omo: <urn:omo:decision:> .",
            "",
        ]

        # 声明每个节点
        for node in self._nodes.values():
            uri = f"omo:{node.node_id}"
            prov_type = _KIND_TO_PROV.get(node.kind, f"{_PROV_NS}Entity")
            lines.append(f"<{uri}> a <{prov_type}> ;")
            lines.append(f"    dct:created \"{node.ts}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;")
            lines.append(f"    omo:actor \"{node.actor}\" ;")
            lines.append(f"    omo:action \"{node.action}\" .")
            lines.append("")

        # 声明每条边的关系
        for edge in self._edges:
            src_uri = f"omo:{edge.src}"
            dst_uri = f"omo:{edge.dst}"
            if edge.relation == "CAUSED":
                lines.append(f"<{dst_uri}> prov:wasGeneratedBy <{src_uri}> .")
            else:
                lines.append(f"<{dst_uri}> prov:wasInfluencedBy <{src_uri}> .")

        lines.append("")
        path.write_text("\n".join(lines), encoding="utf-8")

    def _export_provo_jsonld(self, path: Path) -> None:
        """导出为 PROV-O JSON-LD 格式."""
        nodes_out: list[dict[str, Any]] = []
        for node in self._nodes.values():
            prov_type = _KIND_TO_PROV.get(node.kind, f"{_PROV_NS}Entity")
            nodes_out.append(
                {
                    "@id": f"omo:{node.node_id}",
                    "@type": prov_type,
                    "http://purl.org/dc/terms/created": {
                        "@value": node.ts,
                        "@type": f"{_XSD_NS}dateTime",
                    },
                    "urn:omo:decision:actor": node.actor,
                    "urn:omo:decision:action": node.action,
                }
            )

        edges_out: list[dict[str, Any]] = []
        for edge in self._edges:
            prov_pred = (
                f"{_PROV_NS}wasGeneratedBy"
                if edge.relation == "CAUSED"
                else f"{_PROV_NS}wasInfluencedBy"
            )
            edges_out.append(
                {
                    "@id": f"omo:{edge.dst}",
                    prov_pred: {"@id": f"omo:{edge.src}"},
                }
            )

        doc = {
            "@context": {
                "prov": _PROV_NS,
                "dct": _DCT_NS,
                "omo": "urn:omo:decision:",
            },
            "@graph": nodes_out + edges_out,
        }
        path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")


# ── 便捷函数 ──────────────────────────────────────────


def load_graph(graph_file: Path | None = None) -> DecisionGraph:
    """加载因果决策图 (便捷工厂函数)."""
    return DecisionGraph(graph_file=graph_file)
