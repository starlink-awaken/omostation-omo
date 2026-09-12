"""
decision_bridge.py — Resident 因果决策图 (T5-05)

将 Resident Daemon 的巡检、自愈、决策动作记录为因果图节点与边，
支持祖先链 / 下游拓扑查询与 W3C PROV-O 审计导出。
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Literal

Relation = Literal["CAUSED", "INFLUENCED"]
Kind = Literal["patrol", "healing", "decision"]


class DecisionGraphError(Exception):
    """因果决策图层错误。"""


@dataclass
class DecisionNode:
    """单个决策 / 巡检 / 自愈动作的图原生记录。"""

    node_id: str
    kind: Kind
    actor: str
    action: str
    ts: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CausalEdge:
    """因果边: src → dst。"""

    src: str
    dst: str
    relation: Relation

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DecisionGraph:
    """追加式因果决策图存储。

    持久化为 `.omo/state/decision-graph/graph.jsonl`，每行一条 JSON
    （节点或边）。查询在内存中通过 BFS 计算。
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.graph_path = self.root / "graph.jsonl"
        self._nodes: dict[str, DecisionNode] = {}
        self._edges: list[CausalEdge] = []
        self._out: dict[str, list[str]] = {}
        self._in: dict[str, list[str]] = {}
        self._load()

    # ---- persistence ---------------------------------------------------

    def _load(self) -> None:
        self._nodes.clear()
        self._edges.clear()
        self._out.clear()
        self._in.clear()
        if not self.graph_path.exists():
            return
        with self.graph_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if "relation" in obj:
                    edge = CausalEdge(**obj)
                    self._edges.append(edge)
                    self._out.setdefault(edge.src, []).append(edge.dst)
                    self._in.setdefault(edge.dst, []).append(edge.src)
                else:
                    node = DecisionNode(**obj)
                    self._nodes[node.node_id] = node

    def _append(self, obj: dict[str, Any]) -> None:
        self.graph_path.parent.mkdir(parents=True, exist_ok=True)
        with self.graph_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # ---- mutations -----------------------------------------------------

    def record_patrol(
        self,
        actor: str,
        action: str,
        context: dict[str, Any] | None = None,
    ) -> DecisionNode:
        """记录巡检动作；返回创建的节点。"""
        node = DecisionNode(
            node_id=f"patrol-{uuid.uuid4().hex[:12]}",
            kind="patrol",
            actor=actor,
            action=action,
            ts=datetime.now(timezone.utc).isoformat(),
            context=context or {},
        )
        self._nodes[node.node_id] = node
        self._append(node.to_dict())
        return node

    def record_healing(
        self,
        actor: str,
        action: str,
        caused_by: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> DecisionNode:
        """记录自愈动作；如果给出 caused_by 则建立因果边。"""
        node = DecisionNode(
            node_id=f"healing-{uuid.uuid4().hex[:12]}",
            kind="healing",
            actor=actor,
            action=action,
            ts=datetime.now(timezone.utc).isoformat(),
            context=context or {},
        )
        self._nodes[node.node_id] = node
        self._append(node.to_dict())
        if caused_by and caused_by in self._nodes:
            self._link(caused_by, node.node_id, "CAUSED")
        return node

    def record_decision(
        self,
        actor: str,
        action: str,
        influenced: list[str] | None = None,
        context: dict[str, Any] | None = None,
    ) -> DecisionNode:
        """记录决策动作；可选标记受影响的下游实体。"""
        node = DecisionNode(
            node_id=f"decision-{uuid.uuid4().hex[:12]}",
            kind="decision",
            actor=actor,
            action=action,
            ts=datetime.now(timezone.utc).isoformat(),
            context=context or {},
        )
        self._nodes[node.node_id] = node
        self._append(node.to_dict())
        for target in influenced or []:
            self._link(node.node_id, target, "INFLUENCED")
        return node

    def _link(self, src: str, dst: str, relation: Relation) -> None:
        edge = CausalEdge(src=src, dst=dst, relation=relation)
        self._edges.append(edge)
        self._out.setdefault(src, []).append(dst)
        self._in.setdefault(dst, []).append(src)
        self._append(edge.to_dict())

    # ---- queries -------------------------------------------------------

    def ancestors(self, node_id: str) -> list[dict[str, Any]]:
        """返回完整因果祖先链（BFS 向上，按层级）。"""
        if node_id not in self._nodes:
            raise DecisionGraphError(f"node not found: {node_id}")
        visited: list[dict[str, Any]] = []
        frontier = [node_id]
        seen: set[str] = set()
        while frontier:
            next_frontier: list[str] = []
            for nid in frontier:
                for pred in self._in.get(nid, []):
                    if pred in seen:
                        continue
                    seen.add(pred)
                    node = self._nodes[pred]
                    visited.append(node.to_dict())
                    next_frontier.append(pred)
            frontier = next_frontier
        return visited

    def downstream(self, node_id: str) -> list[dict[str, Any]]:
        """返回下游影响面拓扑（BFS 向下）。"""
        if node_id not in self._nodes:
            raise DecisionGraphError(f"node not found: {node_id}")
        visited: list[dict[str, Any]] = []
        frontier = [node_id]
        seen: set[str] = set()
        while frontier:
            next_frontier: list[str] = []
            for nid in frontier:
                for succ in self._out.get(nid, []):
                    if succ in seen:
                        continue
                    seen.add(succ)
                    node = self._nodes[succ]
                    visited.append(node.to_dict())
                    next_frontier.append(succ)
            frontier = next_frontier
        return visited

    def summary(self) -> dict[str, Any]:
        """节点 / 边统计。"""
        return {
            "nodes": len(self._nodes),
            "edges": len(self._edges),
            "by_kind": {
                k: sum(1 for n in self._nodes.values() if n.kind == k)
                for k in ("patrol", "healing", "decision")
            },
        }

    def get_node(self, node_id: str) -> DecisionNode:
        if node_id not in self._nodes:
            raise DecisionGraphError(f"node not found: {node_id}")
        return self._nodes[node_id]

    # ---- PROV-O export -------------------------------------------------

    def export_provo(self, path: str | Path, fmt: Literal["turtle", "jsonld"] = "turtle") -> Path:
        """导出 W3C PROV-O 审计文件。"""
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "turtle":
            out.write_text(self._render_turtle(), encoding="utf-8")
        elif fmt == "jsonld":
            out.write_text(self._render_jsonld(), encoding="utf-8")
        else:
            raise DecisionGraphError(f"unknown fmt: {fmt}")
        return out

    def _render_turtle(self) -> str:
        lines = [
            "@prefix prov: <http://www.w3.org/ns/prov#> .",
            "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            "@prefix : <http://omostation.local/decision-graph#> .",
            "",
        ]
        for node in self._nodes.values():
            uri = f":{node.node_id}"
            lines.append(f"{uri} a prov:Activity ;")
            if node.kind == "decision":
                lines.append(f'    prov:wasAssociatedWith :actor_{node.actor} ;')
            else:
                lines.append(f'    prov:wasAssociatedWith :actor_{node.actor} ;')
            lines.append(f'    rdfs:label "{node.action}" ;')
            lines.append(f'    prov:atTime "{node.ts}"^^xsd:dateTime .')
            lines.append("")
        for edge in self._edges:
            if edge.relation == "CAUSED":
                lines.append(f":{edge.dst} prov:wasGeneratedBy :{edge.src} .")
            else:
                lines.append(f":{edge.dst} prov:wasInfluencedBy :{edge.src} .")
        return "\n".join(lines) + "\n"

    def _render_jsonld(self) -> str:
        doc = {
            "@context": {
                "prov": "http://www.w3.org/ns/prov#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@graph": [],
        }
        for node in self._nodes.values():
            entry = {
                "@id": f"urn:decision:{node.node_id}",
                "@type": "prov:Activity",
                "prov:wasAssociatedWith": {"@id": f"urn:actor:{node.actor}"},
                "prov:atTime": {"@value": node.ts, "@type": "xsd:dateTime"},
            }
            doc["@graph"].append(entry)
        for edge in self._edges:
            if edge.relation == "CAUSED":
                doc["@graph"].append({
                    "@id": f"urn:decision:{edge.dst}",
                    "prov:wasGeneratedBy": {"@id": f"urn:decision:{edge.src}"},
                })
            else:
                doc["@graph"].append({
                    "@id": f"urn:decision:{edge.dst}",
                    "prov:wasInfluencedBy": {"@id": f"urn:decision:{edge.src}"},
                })
        return json.dumps(doc, indent=2, ensure_ascii=False)
