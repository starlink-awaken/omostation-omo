"""
test_resident_causal_decision.py — T5-05 单测

覆盖: 巡检/自愈建图与连线、祖先链与下游拓扑查询、仲裁仿真评测、
低置信 HITL 升级、PROV-O 双格式导出。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omo.resident.arbitration import PrecedentArbiter, Precedent
from omo.resident.decision_bridge import DecisionGraph, DecisionGraphError


@pytest.fixture
def tmp_graph(tmp_path: Path) -> DecisionGraph:
    return DecisionGraph(tmp_path / "decision-graph")


@pytest.fixture
def tmp_arbiter(tmp_path: Path) -> PrecedentArbiter:
    return PrecedentArbiter(tmp_path / "precedents.jsonl")


class TestDecisionGraph:
    def test_record_patrol(self, tmp_graph: DecisionGraph) -> None:
        node = tmp_graph.record_patrol("resident", "check_disk", {"path": "/"})
        assert node.kind == "patrol"
        assert node.actor == "resident"
        assert node.node_id.startswith("patrol-")

    def test_record_healing_with_cause(self, tmp_graph: DecisionGraph) -> None:
        patrol = tmp_graph.record_patrol("resident", "detect_high_cpu")
        healing = tmp_graph.record_healing("resident", "restart_service", caused_by=patrol.node_id)
        assert healing.kind == "healing"
        # 因果边应存在
        ancestors = tmp_graph.ancestors(healing.node_id)
        assert len(ancestors) == 1
        assert ancestors[0]["node_id"] == patrol.node_id

    def test_ancestors_chain(self, tmp_graph: DecisionGraph) -> None:
        a = tmp_graph.record_patrol("r", "a")
        b = tmp_graph.record_healing("r", "b", caused_by=a.node_id)
        c = tmp_graph.record_healing("r", "c", caused_by=b.node_id)
        ancestors = tmp_graph.ancestors(c.node_id)
        ids = {n["node_id"] for n in ancestors}
        assert a.node_id in ids
        assert b.node_id in ids

    def test_downstream(self, tmp_graph: DecisionGraph) -> None:
        root = tmp_graph.record_patrol("r", "root")
        d1 = tmp_graph.record_decision("r", "d1", influenced=[])
        # 手动建立 influence
        tmp_graph._link(root.node_id, d1.node_id, "INFLUENCED")
        downstream = tmp_graph.downstream(root.node_id)
        assert any(n["node_id"] == d1.node_id for n in downstream)

    def test_summary(self, tmp_graph: DecisionGraph) -> None:
        tmp_graph.record_patrol("r", "p1")
        tmp_graph.record_healing("r", "h1")
        s = tmp_graph.summary()
        assert s["nodes"] == 2
        assert s["by_kind"]["patrol"] == 1
        assert s["by_kind"]["healing"] == 1

    def test_node_not_found(self, tmp_graph: DecisionGraph) -> None:
        with pytest.raises(DecisionGraphError):
            tmp_graph.ancestors("nonexistent")

    def test_persistence(self, tmp_path: Path) -> None:
        g1 = DecisionGraph(tmp_path / "dg")
        g1.record_patrol("r", "persist_test")
        # 重新加载
        g2 = DecisionGraph(tmp_path / "dg")
        s = g2.summary()
        assert s["nodes"] == 1


class TestProvoExport:
    def test_export_turtle(self, tmp_graph: DecisionGraph, tmp_path: Path) -> None:
        patrol = tmp_graph.record_patrol("r", "patrol_action")
        healing = tmp_graph.record_healing("r", "heal_action", caused_by=patrol.node_id)
        out = tmp_path / "prov.ttl"
        tmp_graph.export_provo(out, fmt="turtle")
        content = out.read_text()
        assert "@prefix prov:" in content
        assert "prov:wasGeneratedBy" in content

    def test_export_jsonld(self, tmp_graph: DecisionGraph, tmp_path: Path) -> None:
        tmp_graph.record_patrol("r", "patrol_action")
        out = tmp_path / "prov.jsonld"
        tmp_graph.export_provo(out, fmt="jsonld")
        doc = json.loads(out.read_text())
        assert "@graph" in doc
        assert len(doc["@graph"]) > 0


class TestArbitration:
    def test_simulation_100_hit(self, tmp_arbiter: PrecedentArbiter) -> None:
        """仿真评测：构造已知正确解的冲突场景，仲裁命中 100%。"""
        # 注册先例
        tmp_arbiter.register("cpu high restart service", "restart", 0.95, "success")
        tmp_arbiter.register("cpu high restart service", "restart", 0.90, "success")
        tmp_arbiter.register("cpu high scale up", "scale", 0.60, "fail")

        result = tmp_arbiter.arbitrate("cpu high restart service")
        assert result.resolution == "restart"
        assert result.confidence >= 0.85
        assert not result.escalated

    def test_low_confidence_escalates(self, tmp_arbiter: PrecedentArbiter) -> None:
        """低置信度冲突必须升级 HITL。"""
        tmp_arbiter.register("disk full unknown", "cleanup", 0.50, "partial")
        result = tmp_arbiter.arbitrate("disk full unknown")
        assert result.escalated
        assert result.confidence < 0.85

    def test_empty_precedents_escalates(self, tmp_arbiter: PrecedentArbiter) -> None:
        """无先例可检索时升级 HITL。"""
        result = tmp_arbiter.arbitrate("completely novel conflict")
        assert result.escalated
        assert result.confidence == 0.0

    def test_escalate_hitl_payload(self, tmp_arbiter: PrecedentArbiter) -> None:
        result = tmp_arbiter.arbitrate("novel")
        payload = tmp_arbiter.escalate_hitl("novel", result)
        assert payload["type"] == "escalation"
        assert payload["reason"] == "low_confidence_arbitration"

    def test_register_and_load(self, tmp_path: Path) -> None:
        store = tmp_path / "prec.jsonl"
        a = PrecedentArbiter(store)
        a.register("sig", "res", 0.9, "ok")
        # 重新加载
        b = PrecedentArbiter(store)
        result = b.arbitrate("sig")
        assert result.resolution == "res"
