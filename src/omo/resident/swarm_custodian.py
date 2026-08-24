#!/usr/bin/env python3
"""Swarm Custodian (蜂群领域守卫与自治同步器) — OMO Resident Component."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from omo.blackboard.client import BlackboardClient
from omo.resident.roles import DOMAINS, SWARM_ROLES


def find_workspace_root() -> Path:
    """Find the workspace root directory containing .gitmodules or docs/project-registry.yaml."""
    p = Path(os.environ.get("WORKSPACE_ROOT", Path.cwd())).resolve()
    for parent in [p] + list(p.parents):
        if (parent / ".gitmodules").exists() or (parent / "docs/project-registry.yaml").exists():
            return parent
    return p


class SwarmCustodian:
    """4-Domain Swarm Custodian orchestrator."""

    def __init__(self, db_path: str | Path | None = None):
        self.workspace_root = find_workspace_root()
        if db_path is None:
            db_path = self.workspace_root / "runtime" / "omo" / "architecture_graph.sqlite3"
        self.bb = BlackboardClient(db_path)

    def bootstrap_blackboard(self) -> dict[str, int]:
        """Bootstrap the blackboard with core project nodes and SSOT contracts."""
        node_count = 0
        edge_count = 0

        for domain_id, domain_info in DOMAINS.items():
            # Domain Node
            d_node_id = f"domain:{domain_id}"
            self.bb.upsert_node(
                node_id=d_node_id,
                domain=domain_id,
                node_type="domain",
                metadata={"name": domain_info["name"], "desc": domain_info["desc"]},
            )
            node_count += 1

            # Project Nodes
            for proj in domain_info["projects"]:
                p_node_id = f"proj:{proj}"
                self.bb.upsert_node(
                    node_id=p_node_id,
                    domain=domain_id,
                    node_type="project",
                    path_or_uri=f"projects/{proj}"
                    if not proj.startswith("bin/")
                    and not proj.startswith("protocols")
                    and not proj.startswith("spaces")
                    else proj,
                )
                self.bb.add_edge(d_node_id, p_node_id, "owns")
                node_count += 1
                edge_count += 1

            # SSOT Paths
            for sp in domain_info["ssot_paths"]:
                s_node_id = f"ssot:{sp.strip('/')}"
                self.bb.upsert_node(
                    node_id=s_node_id,
                    domain=domain_id,
                    node_type="ssot_file",
                    path_or_uri=sp,
                )
                self.bb.add_edge(d_node_id, s_node_id, "governs")
                node_count += 1
                edge_count += 1

        return {"nodes_bootstrapped": node_count, "edges_bootstrapped": edge_count}

    def inspect_domain(self, domain_id: str, actor_id: str = "sage:custodian") -> dict[str, Any]:
        """Run health inspection for a specific domain and record physical measurement facts."""
        if domain_id not in DOMAINS:
            raise ValueError(f"Unknown domain: {domain_id}")

        domain_info = DOMAINS[domain_id]
        results: list[dict[str, Any]] = []

        start_t = time.perf_counter()
        # Verify physical existence of projects and SSOT files
        for proj in domain_info["projects"]:
            p_node_id = f"proj:{proj}"
            path_str = (
                f"projects/{proj}"
                if not proj.startswith("bin/") and not proj.startswith("protocols") and not proj.startswith("spaces")
                else proj
            )
            p_path = (self.workspace_root / path_str).resolve()

            exists = p_path.exists()
            exit_code = 0 if exists else 1
            verdict = "pass" if exists else "fail"
            proof_str = f"{proj}:{exists}:{time.time()}"
            proof_hash = hashlib.sha256(proof_str.encode("utf-8")).hexdigest()
            exec_ms = int((time.perf_counter() - start_t) * 1000)

            fact_id = self.bb.record_fact(
                node_id=p_node_id,
                actor_id=actor_id,
                fact_type="health_check",
                exit_code=exit_code,
                proof_hash=proof_hash,
                execution_ms=max(1, exec_ms),
                verdict=verdict,
                details={"path": str(p_path), "exists": exists},
            )
            results.append(
                {
                    "node_id": p_node_id,
                    "fact_id": fact_id,
                    "verdict": verdict,
                    "exit_code": exit_code,
                }
            )

        return {
            "domain": domain_id,
            "inspected_nodes": len(results),
            "results": results,
            "all_passed": all(r["verdict"] == "pass" for r in results),
        }

    def run_all(self, actor_id: str = "sage:custodian") -> dict[str, Any]:
        """Run inspection for all 4 domains."""
        self.bootstrap_blackboard()
        summary = {}
        for d in DOMAINS:
            summary[d] = self.inspect_domain(d, actor_id=actor_id)
        return {
            "status": "ok",
            "domains": summary,
            "blackboard_summary": self.bb.get_summary(),
        }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", action="store_true", help="初始化黑板节点与边")
    parser.add_argument(
        "--inspect", type=str, choices=list(DOMAINS.keys()) + ["all"], default="all", help="巡检指定领域或全域"
    )
    parser.add_argument("--actor", type=str, default="sage:custodian", help="Actor 标识")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    args = parser.parse_args(argv)

    custodian = SwarmCustodian()

    if args.bootstrap:
        res = custodian.bootstrap_blackboard()
        if args.json:
            print(json.dumps(res, ensure_ascii=False, indent=2))
        else:
            print(f"✅ 黑板自举完成: 节点 {res['nodes_bootstrapped']} 个, 边 {res['edges_bootstrapped']} 条")
        return 0

    if args.inspect == "all":
        res = custodian.run_all(actor_id=args.actor)
    else:
        custodian.bootstrap_blackboard()
        res = custodian.inspect_domain(args.inspect, actor_id=args.actor)

    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        print("=== 蜂群领域守卫 (Swarm Custodian) 巡检报告 ===")
        if "domains" in res:
            for d_id, r in res["domains"].items():
                icon = "✅" if r["all_passed"] else "❌"
                print(
                    f"  {icon} [{d_id}] 巡检 {r['inspected_nodes']} 节点 -> 状态: {'PASS' if r['all_passed'] else 'FAIL'}"
                )
            print("\n黑板统计:")
            s = res["blackboard_summary"]
            print(f"  总节点: {s['total_nodes']} | 总边: {s['total_edges']} | 总事实: {s['total_facts']}")
        else:
            icon = "✅" if res["all_passed"] else "❌"
            print(
                f"  {icon} [{res['domain']}] 巡检 {res['inspected_nodes']} 节点 -> 状态: {'PASS' if res['all_passed'] else 'FAIL'}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
