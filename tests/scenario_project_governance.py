#!/usr/bin/env python3
"""
真实复杂场景: 项目治理全链路

场景描述:
主人收到一个项目治理任务:
1. 分析当前项目健康状态
2. 修复 CI 中的 ruff format 失败
3. 更新过期文档 (staleness)
4. 审计技术债务
5. 生成治理报告

这个场景涉及:
- 多个 Cell 协作 (DAG 编排)
- 治理决策 (R0-R3 风险分级)
- 记忆共享 (跨 Cell)
- 自动扩缩容
- 监控可观测
"""

import json
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


def print_header(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def print_step(step: str, detail: str = ""):
    prefix = "  ▸"
    if detail:
        print(f"{prefix} {step}: {detail}")
    else:
        print(f"{prefix} {step}")


def run_cell_command(cell_pool, episode_id: str, intent: str, strategy: str = "dispatch") -> dict:
    """运行单个 Cell Episode."""
    if strategy == "dispatch":
        result = cell_pool.dispatch_episode(episode_id, {"raw_text": intent})
        cell_pool.complete_episode(episode_id, "accept")
        return result
    return {}


def main():
    from omo.resident.cell_pool import CellPool
    from omo.resident.cell_dag import CellDAG
    from omo.resident.governor import Governor, RISK_R0, RISK_R1, RISK_R2, RISK_R3
    from omo.resident.pdp_pep import PDP, PEP
    from omo.resident.memory_pipeline import MemoryPipeline
    from omo.resident.cell_memory_network import MemoryNetwork
    from omo.resident.cell_governance import CellGovernance

    print_header("场景: 项目治理全链路")

    # 初始化组件
    pool = CellPool(max_cells=4, enable_persistence=True)
    dag = CellDAG(pool=pool)
    governor = Governor()
    pdp = PDP(policy_set="cartridge")
    pep = PEP(pdp)
    memory = MemoryPipeline()
    network = MemoryNetwork()
    governance = CellGovernance()

    # ─────────────────────────────────────────────────────────────
    # Phase 1: 项目健康分析
    # ─────────────────────────────────────────────────────────────
    print_header("Phase 1: 项目健康分析")

    # 1.1 创建分析计划
    print_step("创建分析计划", "扫描项目结构 + 检查 CI 状态")
    plan_result = run_cell_command(pool, "ep-analyze", "分析项目健康状态: 扫描目录结构, 检查 CI 状态")
    print_step("Cell 分配", f"cell_id={plan_result.get('cell_id', 'N/A')[:20]}... strategy={plan_result.get('strategy', 'N/A')}")

    # 1.2 治理评估 - 分析动作是 R0 (只读)
    risk = governor.assess_risk({"action": "scan", "target": "docs/"})
    print_step("治理评估", f"action=scan → risk={risk}")
    assert risk == RISK_R0, f"Expected R0, got {risk}"

    # 1.3 发布记忆到网络
    network.publish("cell-analyze", {
        "content": "项目健康分析完成: 发现 3 个过期文档, 1 个 CI 失败",
        "type": "semantic",
        "tags": ["health", "analysis"],
    })
    print_step("记忆发布", "已发布到跨 Cell 记忆网络")

    # ─────────────────────────────────────────────────────────────
    # Phase 2: CI 修复 (DAG 编排)
    # ─────────────────────────────────────────────────────────────
    print_header("Phase 2: CI 修复 (DAG 编排)")

    # 2.1 定义 DAG
    ci_dag = {
        "dag_id": "ci-fix-dag",
        "cells": [
            {"cell_id": "diagnose", "intent": "诊断 CI 失败原因", "depends_on": []},
            {"cell_id": "fix-format", "intent": "修复 ruff format 失败", "depends_on": ["diagnose"]},
            {"cell_id": "verify-fix", "intent": "验证 CI 修复结果", "depends_on": ["fix-format"]},
        ],
    }

    print_step("定义 DAG", f"dag_id={ci_dag['dag_id']}, cells={len(ci_dag['cells'])}")
    for cell in ci_dag["cells"]:
        print_step(f"  Cell: {cell['cell_id']}", f"depends_on={cell['depends_on']}")

    # 2.2 执行 DAG
    dag.define_dag(ci_dag)
    dag_result = dag.execute_dag("ci-fix-dag")
    print_step("DAG 执行结果", f"status={dag_result['status']}")
    for cell_id, r in dag_result["results"].items():
        status_icon = "✓" if r["status"] == "completed" else "✗"
        print_step(f"  {status_icon} {cell_id}", f"{r['status']}")

    # 2.3 治理评估 - 格式化代码是 R1 (低风险)
    risk = governor.assess_risk({"action": "format_code", "target": "src/"})
    print_step("治理评估", f"action=format_code → risk={risk}")
    assert risk == RISK_R1, f"Expected R1, got {risk}"

    # 2.4 PDP/PEP 策略执行
    decision = pdp.evaluate({"action": "format_code", "target": "src/"})
    print_step("PDP 评估", f"decision={decision['decision']}")

    pep_result = pep.enforce({"action": "format_code", "target": "src/"})
    print_step("PEP 执行", f"allowed={pep_result['allowed']}, requires_human={pep_result.get('requires_human', False)}")

    # ─────────────────────────────────────────────────────────────
    # Phase 3: 文档更新 (记忆利用)
    # ─────────────────────────────────────────────────────────────
    print_header("Phase 3: 文档更新 (记忆利用)")

    # 3.1 搜索记忆网络
    results = network.search("过期文档", tags=["health"])
    print_step("记忆搜索", f"找到 {len(results)} 条相关记忆")

    # 3.2 生成记忆候选
    episode_data = {
        "episode_id": "ep-doc-update",
        "intent": "更新过期文档",
        "results": [
            {"ok": True, "output": "A" * 100, "action": "read_file"},
            {"ok": True, "output": "B" * 100, "action": "search"},
        ],
    }
    candidates = memory.generate_candidates(episode_data)
    print_step("记忆候选生成", f"生成 {len(candidates)} 个候选")

    # 3.3 治理评估 - 文档更新是 R1 (低风险)
    risk = governor.assess_risk({"action": "generate_doc", "target": "report"})
    print_step("治理评估", f"action=generate_doc → risk={risk}")
    assert risk == RISK_R1, f"Expected R1, got {risk}"

    # ─────────────────────────────────────────────────────────────
    # Phase 4: 技术债务审计
    # ─────────────────────────────────────────────────────────────
    print_header("Phase 4: 技术债务审计")

    # 4.1 配置审计
    audit_result = governance.audit_cell_config({
        "max_cells": 4,
        "auto_scale": True,
        "policy_set": "cartridge",
    })
    print_step("配置审计", f"compliant={audit_result['compliant']}, findings={len(audit_result['findings'])}")

    # 4.2 动作审计
    audit = governance.audit_action({"action": "commit_code", "target": "main"})
    print_step("动作审计", f"risk={audit['risk_level']}, allowed={audit['allowed']}")

    # 4.3 漂移检测
    drift = governance.detect_drift(
        baseline={"max_cells": 4, "auto_scale": True},
        current={"max_cells": 8, "auto_scale": False}
    )
    print_step("漂移检测", f"发现 {len(drift)} 处漂移")
    for d in drift:
        print_step(f"  ⚠ {d['field']}", f"baseline={d['baseline']} → current={d['current']}")

    # ─────────────────────────────────────────────────────────────
    # Phase 5: 自动扩缩容
    # ─────────────────────────────────────────────────────────────
    print_header("Phase 5: 自动扩缩容")

    # 5.1 模拟高负载
    print_step("模拟高负载", "提交 4 个并发 Episode")
    for i in range(4):
        pool.dispatch_episode(f"load-{i}", {"goal": f"并发任务 {i}"})

    # 5.2 触发自动扩容
    scale_result = pool.auto_scale()
    print_step("自动扩缩容", f"action={scale_result['action']}, max_cells={pool.max_cells}")

    # 5.3 完成所有 Episode
    for i in range(4):
        pool.complete_episode(f"load-{i}", "accept")

    # 5.4 触发自动缩容
    scale_result = pool.auto_scale()
    print_step("自动缩容", f"action={scale_result['action']}")

    # ─────────────────────────────────────────────────────────────
    # Phase 6: 监控与报告
    # ─────────────────────────────────────────────────────────────
    print_header("Phase 6: 监控与报告")

    # 6.1 Pool 状态
    status = pool.get_pool_status()
    print_step("Pool 状态", f"cells={status['total_cells']}/{status['max_cells']}, utilization={status['utilization']}")

    # 6.2 详细指标
    metrics = pool.get_metrics()
    print_step("详细指标", f"total_dispatches={metrics['dispatch']['total_dispatches']}")

    # 6.3 治理报告
    report = governance.generate_report()
    print_step("治理报告", f"compliance_rate={report.get('compliance_rate', 'N/A')}%")

    # 6.4 记忆网络统计
    net_stats = network.get_stats()
    print_step("记忆网络", f"total_memories={net_stats['total_memories']}, cells={len(net_stats['cells'])}")

    # 6.5 清理过期记忆
    cleaned = network.cleanup_expired()
    print_step("记忆清理", f"cleaned={cleaned} expired memories")

    # ─────────────────────────────────────────────────────────────
    # 总结
    # ─────────────────────────────────────────────────────────────
    print_header("场景执行总结")

    summary = {
        "phases_completed": 6,
        "cells_used": len(pool.cells),
        "episodes_executed": len(pool.dispatch_log),
        "memory_candidates": len(candidates),
        "governance_decisions": 5,
        "auto_scaling_events": 2,
    }

    for k, v in summary.items():
        print_step(k.replace("_", " ").title(), str(v))

    print(f"\n{'='*60}")
    print("  场景执行完成 ✓")
    print(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
