#!/usr/bin/env python3

"""因果决策图与先例仲裁引擎 单元测试 (BET-Y1Q4-T5-05).

覆盖:
  decision_bridge: DecisionGraph CRUD / BFS 祖先链 / 下游影响面 /
    持久化 reload / PROV-O Turtle + JSON-LD 导出 / load_graph 工厂
  arbitration: PrecedentArbiter 精确匹配 / 无先例升级 / 低置信度断路器 /
    Jaccard 模糊匹配 / HITL 升级记录 / 模拟场景 / 持久化
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from omo.resident.arbitration import (
    CONFIDENCE_THRESHOLD,
    SIMILARITY_THRESHOLD,
    ArbitrationResult,
    Conflict,
    Precedent,
    PrecedentArbiter,
    SimulationScenario,
    create_simulation_scenarios,
    load_arbiter,
    run_simulation,
)
from omo.resident.decision_bridge import (
    CausalEdge,
    DecisionGraph,
    DecisionNode,
    load_graph,
)


# ═══════════════════════════════════════════════════════════════════════
# decision_bridge 测试
# ═══════════════════════════════════════════════════════════════════════


class TestDecisionNode:
    """DecisionNode 数据模型测试."""

    def test_defaults(self):
        node = DecisionNode(kind="patrol", actor="a", action="b")
        assert node.kind == "patrol"
        assert node.actor == "a"
        assert node.action == "b"
        assert node.context == {}
        assert node.node_id  # auto-generated
        assert node.ts  # auto-timestamp

    def test_to_dict_from_dict_roundtrip(self):
        node = DecisionNode(
            node_id="test-1",
            kind="decision",
            actor="governor",
            action="approve",
            ts="2026-09-12T12:00:00Z",
            context={"key": "value"},
        )
        d = node.to_dict()
        assert d["_type"] == "node"
        assert d["node_id"] == "test-1"
        assert d["kind"] == "decision"
        node2 = DecisionNode.from_dict(d)
        assert node2.node_id == node.node_id
        assert node2.kind == node.kind
        assert node2.context == node.context

    def test_repr(self):
        node = DecisionNode(kind="patrol", actor="a", action="b", node_id="x")
        r = repr(node)
        assert "patrol" in r
        assert "a" in r


class TestCausalEdge:
    """CausalEdge 数据模型测试."""

    def test_basic(self):
        edge = CausalEdge(src="a", dst="b", relation="CAUSED")
        assert edge.src == "a"
        assert edge.dst == "b"
        assert edge.relation == "CAUSED"

    def test_to_dict_from_dict(self):
        edge = CausalEdge(src="s", dst="d", relation="INFLUENCED")
        d = edge.to_dict()
        assert d["_type"] == "edge"
        assert d["src"] == "s"
        edge2 = CausalEdge.from_dict(d)
        assert edge2.dst == "d"
        assert edge2.relation == "INFLUENCED"


class TestDecisionGraph:
    """DecisionGraph 核心功能测试."""

    @pytest.fixture
    def graph(self, tmp_path):
        """返回带有测试数据的 DecisionGraph."""
        gf = tmp_path / "graph.jsonl"
        g = DecisionGraph(graph_file=gf)
        return g

    @pytest.fixture
    def chain(self, graph):
        """patrol → healing → decision 因果链."""
        n1 = graph.record_patrol(actor="patrol-agent", action="health-check")
        n2 = graph.record_healing(
            actor="heal-agent",
            action="restart",
            triggered_by=n1.node_id,
            context={"affected_entities": ["api-svc", "db-cluster"]},
        )
        n3 = graph.record_decision(
            actor="governor", action="approve-deploy", triggered_by=n2.node_id
        )
        return n1, n2, n3, graph

    def test_record_patrol(self, graph):
        node = graph.record_patrol(actor="a", action="b", context={"k": "v"})
        assert node.kind == "patrol"
        assert node.actor == "a"
        assert node.context == {"k": "v"}
        assert graph.get_node(node.node_id) is not None

    def test_record_healing_with_affected(self, graph):
        n1 = graph.record_patrol(actor="p", action="scan")
        n2 = graph.record_healing(
            actor="h", action="fix", triggered_by=n1.node_id,
            context={"affected_entities": ["svc-a", "svc-b"]},
        )
        s = graph.summary()
        assert s["total_nodes"] == 4  # patrol + healing + 2 entity nodes
        assert s["total_edges"] == 3  # 1 CAUSED + 2 INFLUENCED

    def test_record_decision_multi_trigger(self, graph):
        n1 = graph.record_patrol(actor="a", action="x")
        n2 = graph.record_patrol(actor="b", action="y")
        n3 = graph.record_decision(
            actor="g", action="merge", triggered_by=[n1.node_id, n2.node_id]
        )
        s = graph.summary()
        assert s["total_edges"] == 2
        assert s["by_relation"]["CAUSED"] == 2

    def test_ancestors_simple(self, chain):
        _, _, n3, graph = chain
        anc = graph.ancestors(n3.node_id)
        assert len(anc) == 2
        kinds = {a["kind"] for a in anc}
        assert kinds == {"healing", "patrol"}

    def test_ancestors_depth(self, chain):
        _, _, n3, graph = chain
        anc = graph.ancestors(n3.node_id)
        depths = [a["depth"] for a in anc]
        assert depths == [1, 2]  # healing(1), patrol(2)

    def test_ancestors_unknown_node(self, graph):
        assert graph.ancestors("nonexistent") == []

    def test_downstream(self, chain):
        n1, _, _, graph = chain
        down = graph.downstream(n1.node_id)
        assert len(down) == 4  # healing + decision + 2 entities
        relations = [d["relation"] for d in down]
        assert "CAUSED" in relations
        assert "INFLUENCED" in relations

    def test_downstream_unknown_node(self, graph):
        assert graph.downstream("nonexistent") == []

    def test_summary_empty(self, graph):
        s = graph.summary()
        assert s["total_nodes"] == 0
        assert s["total_edges"] == 0
        assert s["by_kind"] == {}
        assert s["by_relation"] == {}

    def test_summary_populated(self, chain):
        _, _, _, graph = chain
        s = graph.summary()
        assert s["total_nodes"] == 5
        assert s["total_edges"] == 4
        assert s["by_kind"]["patrol"] == 1
        assert s["by_kind"]["healing"] == 1
        assert s["by_kind"]["decision"] == 3
        assert s["by_relation"]["CAUSED"] == 2
        assert s["by_relation"]["INFLUENCED"] == 2

    def test_get_node_exists(self, chain):
        _, _, n3, graph = chain
        assert graph.get_node(n3.node_id) is not None
        assert graph.get_node(n3.node_id).action == "approve-deploy"

    def test_get_node_missing(self, graph):
        assert graph.get_node("missing") is None

    def test_persistence_reload(self, tmp_path):
        gf = tmp_path / "graph.jsonl"
        g1 = DecisionGraph(graph_file=gf)
        n1 = g1.record_patrol(actor="a", action="b")
        n2 = g1.record_decision(actor="c", action="d", triggered_by=n1.node_id)

        # Reload from same file
        g2 = DecisionGraph(graph_file=gf)
        assert g2.summary()["total_nodes"] == 2
        assert g2.summary()["total_edges"] == 1
        assert g2.get_node(n2.node_id).actor == "c"

    def test_load_graph_helper(self, tmp_path):
        gf = tmp_path / "graph.jsonl"
        g1 = DecisionGraph(graph_file=gf)
        g1.record_patrol(actor="a", action="b")
        g2 = load_graph(gf)
        assert g2.summary()["total_nodes"] == 1


class TestProvoExport:
    """PROV-O 导出格式测试."""

    @pytest.fixture
    def graph(self, tmp_path):
        gf = tmp_path / "graph.jsonl"
        g = DecisionGraph(graph_file=gf)
        n1 = g.record_patrol(actor="p", action="scan")
        n2 = g.record_healing(
            actor="h", action="fix", triggered_by=n1.node_id,
            context={"affected_entities": ["svc"]},
        )
        n3 = g.record_decision(actor="g", action="approve", triggered_by=n2.node_id)
        return g

    def test_turtle_format(self, graph, tmp_path):
        out = tmp_path / "graph.ttl"
        graph.export_provo(out, fmt="turtle")
        content = out.read_text()

        assert "@prefix prov:" in content
        assert "@prefix dct:" in content
        assert "@prefix xsd:" in content
        assert "@prefix omo:" in content
        assert "prov:wasGeneratedBy" in content
        assert "prov:wasInfluencedBy" in content
        assert content.count("wasGeneratedBy") == 2
        assert content.count("wasInfluencedBy") == 1
        assert "a <http://www.w3.org/ns/prov#Activity>" in content
        assert "a <http://www.w3.org/ns/prov#Entity>" in content

    def test_jsonld_format(self, graph, tmp_path):
        out = tmp_path / "graph.jsonld"
        graph.export_provo(out, fmt="jsonld")
        doc = json.loads(out.read_text())

        assert "@context" in doc
        assert "@graph" in doc
        assert doc["@context"]["prov"] == "http://www.w3.org/ns/prov#"
        graph_items = doc["@graph"]
        # Check node entries have @id and @type
        node_items = [g for g in graph_items if "@type" in g]
        assert len(node_items) == 4  # 3 main + 1 entity
        # Check edge entries have PROV predicates
        edge_items = [g for g in graph_items if "@id" in g and "@type" not in g]
        assert len(edge_items) == 3  # 2 CAUSED + 1 INFLUENCED
        # Check PROV namespace
        prov_ns = "http://www.w3.org/ns/prov#"
        preds = [p for g in edge_items for p in g if p.startswith(prov_ns)]
        assert any("wasGeneratedBy" in p for p in preds)
        assert any("wasInfluencedBy" in p for p in preds)

    def test_invalid_format_raises(self, graph, tmp_path):
        out = tmp_path / "bad.out"
        with pytest.raises(ValueError, match="Unsupported format"):
            graph.export_provo(out, fmt="rdfa")

    def test_turtle_node_declaration(self, graph, tmp_path):
        out = tmp_path / "graph.ttl"
        graph.export_provo(out, fmt="turtle")
        content = out.read_text()
        # Each node should have omo:actor and omo:action
        assert "omo:actor" in content
        assert "omo:action" in content
        assert "dct:created" in content


# ═══════════════════════════════════════════════════════════════════════
# arbitration 测试
# ═══════════════════════════════════════════════════════════════════════


class TestPrecedent:
    """Precedent 数据模型测试."""

    def test_defaults(self):
        p = Precedent(signature="s", resolution="r")
        assert p.confidence == 0.9
        assert p.outcome == ""
        assert p.precedent_id
        assert p.created_at == ""  # defaults to empty == ""  # defaults to empty

    def test_to_dict_from_dict(self):
        p = Precedent(signature="cfg", resolution="merge", confidence=0.7, outcome="ok")
        d = p.to_dict()
        assert d["signature"] == "cfg"
        p2 = Precedent.from_dict(d)
        assert p2.confidence == 0.7
        assert p2.outcome == "ok"


class TestConflict:
    """Conflict 数据模型测试."""

    def test_basic(self):
        c = Conflict(signature="s", agents=["a", "b"], positions={"a": "x", "b": "y"})
        assert c.signature == "s"
        assert len(c.agents) == 2
        assert c.conflict_id


class TestArbitrationResult:
    """ArbitrationResult 数据模型测试."""

    def test_success(self):
        r = ArbitrationResult(resolution="r", confidence=0.95, basis=["p1"])
        assert r.escalated is False
        assert r.escalation_reason == ""

    def test_escalated(self):
        r = ArbitrationResult(
            resolution="", confidence=0.3, escalated=True, escalation_reason="low_conf"
        )
        assert r.escalated is True


class TestPrecedentArbiter:
    """PrecedentArbiter 核心仲裁逻辑测试."""

    @pytest.fixture
    def arbiter(self, tmp_path):
        pf = tmp_path / "precedents.jsonl"
        return PrecedentArbiter(precedents_file=pf)

    def test_exact_match(self, arbiter):
        arbiter.add_precedent(signature="cfg", resolution="merge", confidence=0.92)
        arbiter.add_precedent(signature="cfg", resolution="merge", confidence=0.88)
        result = arbiter.arbitrate(Conflict(signature="cfg", agents=["a"], positions={"a": "x"}))
        assert result.resolution == "merge"
        assert result.confidence >= CONFIDENCE_THRESHOLD
        assert not result.escalated

    def test_no_precedent_escalates(self, arbiter):
        result = arbiter.arbitrate(Conflict(signature="unknown", agents=["a"], positions={"a": "x"}))
        assert result.escalated is True
        assert result.confidence == 0.0

    def test_low_confidence_circuit_breaker(self, arbiter):
        arbiter.add_precedent(signature="risky", resolution="a", confidence=0.30)
        arbiter.add_precedent(signature="risky", resolution="b", confidence=0.25)
        result = arbiter.arbitrate(Conflict(signature="risky", agents=["a"], positions={"a": "x"}))
        assert result.escalated is True

    def test_fuzzy_jaccard_match(self, arbiter):
        arbiter.add_precedent(signature="deploy-order-critical-path", resolution="dep-first", confidence=0.90)
        # 相似签名 (Jaccard > SIMILARITY_THRESHOLD)
        result = arbiter.arbitrate(Conflict(signature="deploy-order-critical", agents=["a"], positions={"a": "x"}))
        assert result.resolution == "dep-first"
        assert not result.escalated

    def test_dissimilar_no_match(self, arbiter):
        arbiter.add_precedent(signature="config-conflict", resolution="merge", confidence=0.90)
        result = arbiter.arbitrate(Conflict(signature="permission-issue", agents=["a"], positions={"a": "x"}))
        assert result.escalated is True

    def test_escalate_hitl(self, arbiter):
        arbiter.add_precedent(signature="risky", resolution="a", confidence=0.30)
        arbiter.add_precedent(signature="risky", resolution="b", confidence=0.25)
        conf = Conflict(signature="risky", agents=["a"], positions={"a": "x"})
        result = arbiter.arbitrate(conf)
        esc = arbiter.escalate_hitl(conf, result)
        assert esc["status"] == "pending_human_review"
        assert esc["escalation_reason"].startswith("confidence_below_threshold")
        assert esc["conflict_signature"] == "risky"

    def test_summary(self, arbiter):
        arbiter.add_precedent(signature="s1", resolution="r1", confidence=0.9)
        arbiter.add_precedent(signature="s2", resolution="r2", confidence=0.8)
        arbiter.arbitrate(Conflict(signature="s1", agents=["a"], positions={"a": "x"}))
        arbiter.arbitrate(Conflict(signature="unknown", agents=["a"], positions={"a": "x"}))
        sm = arbiter.summary()
        assert sm["total_precedents"] == 2
        assert "threshold" in sm
        assert "similarity_threshold" in sm
        assert "by_resolution" in sm

    def test_add_precedent_persistence(self, tmp_path):
        pf = tmp_path / "precedents.jsonl"
        a1 = PrecedentArbiter(precedents_file=pf)
        a1.add_precedent(signature="s", resolution="r", confidence=0.9)
        a2 = PrecedentArbiter(precedents_file=pf)
        assert a2.summary()["total_precedents"] == 1

    def test_constants(self):
        assert CONFIDENCE_THRESHOLD == 0.85
        assert SIMILARITY_THRESHOLD == 0.6


class TestSimulation:
    """模拟场景测试."""

    def test_create_scenarios(self):
        scenarios = create_simulation_scenarios()
        assert len(scenarios) == 5
        names = [s.name for s in scenarios]
        assert "config_value_conflict" in names
        assert "deploy_order_conflict" in names

    def test_run_simulation(self, tmp_path):
        pf = tmp_path / "precedents.jsonl"
        arbiter = PrecedentArbiter(precedents_file=pf)
        result = run_simulation(arbiter)
        assert result["total"] == 5
        assert 0.0 <= result["hit_rate"] <= 1.0
        assert "passed" in result
        assert "failed" in result
        assert "details" in result

    def test_load_arbiter(self, tmp_path):
        pf = tmp_path / "precedents.jsonl"
        a1 = PrecedentArbiter(precedents_file=pf)
        a1.add_precedent(signature="s", resolution="r")
        a2 = load_arbiter(pf)
        assert a2.summary()["total_precedents"] == 1


class TestSimulationScenario:
    """SimulationScenario 数据模型测试."""

    def test_basic(self):
        scenario = SimulationScenario(
            name="test",
            conflict=Conflict(signature="s", agents=["a"], positions={"a": "x"}),
            expected_resolution="r",
        )
        assert scenario.name == "test"
        assert scenario.conflict.signature == "s"
