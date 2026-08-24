#!/usr/bin/env python3
"""Architecture Causal Blackboard (架构因果黑板) Python Client & CLI."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path("runtime/omo/architecture_graph.sqlite3")
SCHEMA_FILE = Path(__file__).parent / "schema.sql"


class BlackboardClient:
    """SQLite-backed Architecture Causal Blackboard Client."""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        if self.db_path != Path(":memory:"):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
        )
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        """Initialize SQLite database with WAL mode and schema."""
        if str(self.db_path) != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")
        self.conn.executescript(schema_sql)
        self.conn.commit()

    def close(self) -> None:
        """Close SQLite connection."""
        self.conn.close()

    def __enter__(self) -> BlackboardClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ---------------- Nodes ----------------
    def upsert_node(
        self,
        node_id: str,
        domain: str,
        node_type: str,
        path_or_uri: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Insert or update an architecture node."""
        meta_json = json.dumps(metadata or {}, ensure_ascii=False)
        sql = """
        INSERT INTO graph_nodes (id, domain, node_type, path_or_uri, metadata, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            domain=excluded.domain,
            node_type=excluded.node_type,
            path_or_uri=excluded.path_or_uri,
            metadata=excluded.metadata,
            updated_at=CURRENT_TIMESTAMP;
        """
        self.conn.execute(sql, (node_id, domain, node_type, path_or_uri, meta_json))
        self.conn.commit()

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Retrieve node by ID."""
        cur = self.conn.execute("SELECT * FROM graph_nodes WHERE id = ?", (node_id,))
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        d["metadata"] = json.loads(d["metadata"] or "{}")
        return d

    def list_nodes(self, domain: str | None = None, node_type: str | None = None) -> list[dict[str, Any]]:
        """List nodes filtered by domain and/or node_type."""
        conditions: list[str] = []
        params: list[Any] = []
        if domain:
            conditions.append("domain = ?")
            params.append(domain)
        if node_type:
            conditions.append("node_type = ?")
            params.append(node_type)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM graph_nodes {where} ORDER BY domain, id"
        cur = self.conn.execute(sql, params)
        results = []
        for r in cur.fetchall():
            d = dict(r)
            d["metadata"] = json.loads(d["metadata"] or "{}")
            results.append(d)
        return results

    # ---------------- Edges ----------------
    def add_edge(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Add a directed causal/dependency edge."""
        meta_json = json.dumps(metadata or {}, ensure_ascii=False)
        sql = """
        INSERT INTO graph_edges (source_id, target_id, relation_type, metadata)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(source_id, target_id, relation_type) DO UPDATE SET
            metadata=excluded.metadata;
        """
        self.conn.execute(sql, (source_id, target_id, relation_type, meta_json))
        self.conn.commit()

    def get_dependencies(self, node_id: str, relation_type: str | None = None) -> list[dict[str, Any]]:
        """Get outgoing edges (dependencies)."""
        sql = "SELECT * FROM graph_edges WHERE source_id = ?"
        params: list[Any] = [node_id]
        if relation_type:
            sql += " AND relation_type = ?"
            params.append(relation_type)
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def get_dependents(self, node_id: str, relation_type: str | None = None) -> list[dict[str, Any]]:
        """Get incoming edges (nodes that depend on this node)."""
        sql = "SELECT * FROM graph_edges WHERE target_id = ?"
        params: list[Any] = [node_id]
        if relation_type:
            sql += " AND relation_type = ?"
            params.append(relation_type)
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    # ---------------- Measurement Facts ----------------
    def record_fact(
        self,
        node_id: str,
        actor_id: str,
        fact_type: str,
        exit_code: int,
        proof_hash: str,
        execution_ms: int,
        verdict: str,
        details: dict[str, Any] | None = None,
    ) -> int:
        """Record a physical measurement fact."""
        details_json = json.dumps(details or {}, ensure_ascii=False)
        sql = """
        INSERT INTO measurement_facts (
            node_id, actor_id, fact_type, exit_code, proof_hash, execution_ms, verdict, details
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur = self.conn.execute(
            sql,
            (node_id, actor_id, fact_type, exit_code, proof_hash, execution_ms, verdict, details_json),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_latest_facts(self, node_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Get latest measurement facts."""
        sql = "SELECT * FROM measurement_facts"
        params: list[Any] = []
        if node_id:
            sql += " WHERE node_id = ?"
            params.append(node_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        cur = self.conn.execute(sql, params)
        results = []
        for r in cur.fetchall():
            d = dict(r)
            d["details"] = json.loads(d["details"] or "{}")
            results.append(d)
        return results

    def get_corroded_nodes(self) -> list[dict[str, Any]]:
        """Get nodes with recent 'corroded' or 'fail' verdicts."""
        sql = """
        SELECT f.*, n.domain, n.node_type, n.path_or_uri
        FROM measurement_facts f
        JOIN graph_nodes n ON f.node_id = n.id
        WHERE f.verdict IN ('fail', 'corroded')
        AND f.id IN (
            SELECT MAX(id) FROM measurement_facts GROUP BY node_id
        )
        ORDER BY f.id DESC
        """
        cur = self.conn.execute(sql)
        results = []
        for r in cur.fetchall():
            d = dict(r)
            d["details"] = json.loads(d["details"] or "{}")
            results.append(d)
        return results

    # ---------------- Domain Leases ----------------
    def claim_lease(
        self,
        domain: str,
        role: str,
        agent_id: str,
        ttl_seconds: int = 300,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Claim a domain custodian lease (returns True on success, False if already held)."""
        now = datetime.datetime.now(datetime.UTC)
        expires = now + datetime.timedelta(seconds=ttl_seconds)
        meta_json = json.dumps(metadata or {}, ensure_ascii=False)

        # Check existing lease
        cur = self.conn.execute(
            "SELECT agent_id, lease_expires_at FROM domain_leases WHERE domain = ? AND role = ?",
            (domain, role),
        )
        row = cur.fetchone()
        if row:
            exp_str = row["lease_expires_at"]
            if isinstance(exp_str, str):
                exp_dt = datetime.datetime.fromisoformat(exp_str)
            else:
                exp_dt = exp_str
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=datetime.UTC)

            if exp_dt > now and row["agent_id"] != agent_id:
                # Valid lease held by another agent
                return False

        sql = """
        INSERT INTO domain_leases (domain, role, agent_id, lease_expires_at, heartbeat_at, metadata)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(domain, role) DO UPDATE SET
            agent_id=excluded.agent_id,
            lease_expires_at=excluded.lease_expires_at,
            heartbeat_at=CURRENT_TIMESTAMP,
            metadata=excluded.metadata;
        """
        self.conn.execute(sql, (domain, role, agent_id, expires.isoformat(), meta_json))
        self.conn.commit()
        return True

    def list_active_leases(self) -> list[dict[str, Any]]:
        """List currently active domain leases."""
        now = datetime.datetime.now(datetime.UTC).isoformat()
        cur = self.conn.execute("SELECT * FROM domain_leases WHERE lease_expires_at > ?", (now,))
        results = []
        for r in cur.fetchall():
            d = dict(r)
            d["metadata"] = json.loads(d["metadata"] or "{}")
            results.append(d)
        return results

    # ---------------- Summary & Statistics ----------------
    def get_summary(self) -> dict[str, Any]:
        """Get summary metrics of the architecture blackboard."""
        total_nodes = self.conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
        total_edges = self.conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
        total_facts = self.conn.execute("SELECT COUNT(*) FROM measurement_facts").fetchone()[0]
        corroded_count = len(self.get_corroded_nodes())
        active_leases = len(self.list_active_leases())

        cur = self.conn.execute("SELECT domain, COUNT(*) FROM graph_nodes GROUP BY domain")
        domain_counts = {row[0]: row[1] for row in cur.fetchall()}

        return {
            "total_nodes": total_nodes,
            "total_edges": total_edges,
            "total_facts": total_facts,
            "corroded_nodes": corroded_count,
            "active_leases": active_leases,
            "domain_node_counts": domain_counts,
        }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Architecture Causal Blackboard CLI")
    parser.add_argument("--db", type=str, default=None, help="Database path")
    subparsers = parser.add_subparsers(dest="command")

    # summary
    subparsers.add_parser("summary", help="Show blackboard summary")

    # list-nodes
    p_nodes = subparsers.add_parser("list-nodes", help="List nodes")
    p_nodes.add_argument("--domain", type=str, default=None)
    p_nodes.add_argument("--type", type=str, default=None)

    # corroded
    subparsers.add_parser("corroded", help="List corroded or failing nodes")

    # leases
    subparsers.add_parser("leases", help="List active domain custodian leases")

    # json flag
    parser.add_argument("--json", action="store_true", help="Output JSON format")

    args = parser.parse_args(argv)
    client = BlackboardClient(args.db)

    if args.command == "summary" or not args.command:
        s = client.get_summary()
        if args.json:
            print(json.dumps(s, ensure_ascii=False, indent=2))
        else:
            print("=== 架构因果黑板 (Architecture Blackboard) 概览 ===")
            print(f"  • 总节点数: {s['total_nodes']} | 总边数: {s['total_edges']} | 总事实数: {s['total_facts']}")
            print(f"  • 腐蚀/异常节点: {s['corroded_nodes']} | 活跃租约: {s['active_leases']}")
            print("  • 领域节点分布:")
            for d, cnt in s["domain_node_counts"].items():
                print(f"    - {d}: {cnt}")
        return 0

    if args.command == "list-nodes":
        nodes = client.list_nodes(domain=args.domain, node_type=args.type)
        if args.json:
            print(json.dumps(nodes, ensure_ascii=False, indent=2))
        else:
            print(f"节点清单 (共 {len(nodes)} 个):")
            for n in nodes:
                print(f"  [{n['domain']}] {n['node_type']} :: {n['id']} -> {n['path_or_uri'] or '-'}")
        return 0

    if args.command == "corroded":
        corroded = client.get_corroded_nodes()
        if args.json:
            print(json.dumps(corroded, ensure_ascii=False, indent=2))
        else:
            if not corroded:
                print("✅ 无腐蚀/异常节点 (All green)")
            else:
                print(f"⚠️ 发现 {len(corroded)} 个腐蚀/异常节点:")
                for c in corroded:
                    print(f"  - [{c['domain']}] {c['node_id']} (actor: {c['actor_id']}, verdict: {c['verdict']})")
        return 0

    if args.command == "leases":
        leases = client.list_active_leases()
        if args.json:
            print(json.dumps(leases, ensure_ascii=False, indent=2))
        else:
            print(f"活跃领域守卫租约 (共 {len(leases)} 个):")
            for lease in leases:
                print(
                    f"  [{lease['domain']}] {lease['role']} -> {lease['agent_id']} (expires: {lease['lease_expires_at']})"
                )
        return 0

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
