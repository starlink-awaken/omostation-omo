"""Unit tests for Architecture Causal Blackboard (projects/omo/src/omo/blackboard)."""

import tempfile
from pathlib import Path

from omo.blackboard.client import BlackboardClient


def test_blackboard_lifecycle():
    with tempfile.NamedTemporaryFile(suffix=".sqlite3") as tmp:
        client = BlackboardClient(tmp.name)

        # 1. Upsert Nodes
        client.upsert_node(
            node_id="proj:omo",
            domain="gov_ssot",
            node_type="project",
            path_or_uri="projects/omo",
            metadata={"version": "1.0.0"},
        )
        client.upsert_node(
            node_id="rule:X1-C01",
            domain="gov_ssot",
            node_type="rule",
            path_or_uri="projects/ecos/src/ecos/ssot/registry/L0-constraints.yaml",
        )

        node = client.get_node("proj:omo")
        assert node is not None
        assert node["domain"] == "gov_ssot"
        assert node["metadata"]["version"] == "1.0.0"

        # 2. Add Causal Edges
        client.add_edge(
            source_id="proj:omo",
            target_id="rule:X1-C01",
            relation_type="monitored_by",
        )
        deps = client.get_dependencies("proj:omo")
        assert len(deps) == 1
        assert deps[0]["target_id"] == "rule:X1-C01"

        dependents = client.get_dependents("rule:X1-C01")
        assert len(dependents) == 1
        assert dependents[0]["source_id"] == "proj:omo"

        # 3. Record Physical Measurement Facts
        fact_id = client.record_fact(
            node_id="rule:X1-C01",
            actor_id="devil:gov",
            fact_type="chaos_probe",
            exit_code=0,
            proof_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            execution_ms=45,
            verdict="pass",
            details={"probe": "dead_ref_check"},
        )
        assert fact_id > 0

        latest = client.get_latest_facts("rule:X1-C01")
        assert len(latest) == 1
        assert latest[0]["verdict"] == "pass"

        # 4. Leases & Heartbeats
        claimed = client.claim_lease(
            domain="gov_ssot",
            role="devil",
            agent_id="agent-devil-01",
            ttl_seconds=60,
        )
        assert claimed is True

        # Duplicate claim by another agent should fail
        claimed_other = client.claim_lease(
            domain="gov_ssot",
            role="devil",
            agent_id="agent-devil-02",
            ttl_seconds=60,
        )
        assert claimed_other is False

        leases = client.list_active_leases()
        assert len(leases) == 1
        assert leases[0]["agent_id"] == "agent-devil-01"

        # 5. Corroded Nodes Check
        client.record_fact(
            node_id="rule:X1-C01",
            actor_id="devil:gov",
            fact_type="chaos_probe",
            exit_code=1,
            proof_hash="hash-error",
            execution_ms=12,
            verdict="corroded",
        )
        corroded = client.get_corroded_nodes()
        assert len(corroded) == 1
        assert corroded[0]["node_id"] == "rule:X1-C01"
        assert corroded[0]["verdict"] == "corroded"

        # 6. Summary metrics
        summary = client.get_summary()
        assert summary["total_nodes"] == 2
        assert summary["total_edges"] == 1
        assert summary["total_facts"] == 2
        assert summary["corroded_nodes"] == 1
        assert summary["active_leases"] == 1

        client.close()
