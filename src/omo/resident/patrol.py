#!/usr/bin/env python3
"""omo.resident.patrol — 自治主动巡检与静默自愈机器人 (Phase 7).

职责:
1. 周期性 (默认 15min 或空闲调度) 执行低开销只读探针 (<500ms);
2. 发现 R0 级轻微漂移 (如投影租约超时、系统状态脱落)，在严格枚举安全沙箱内静默自愈并记入 Ledger;
3. 发现 R1 级工程断链，自动组装 Action Desk 待办卡片升级通知;
4. 恪守免打扰纪律: 绝不修改用户活动工作树内的代码。
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from omo.omo_paths import code_root

logger = logging.getLogger("omo.resident.patrol")

WORKSPACE = code_root()


@dataclass
class PatrolFinding:
    component: str
    issue: str
    risk_tier: str  # R0 (运维级自愈) | R1 (工程级工单)
    remediation_action: str | None = None
    detail: str = ""


class ResidentPatrolRunner:
    """自治主动巡检执行器."""

    def __init__(self, workspace_root: Path | None = None) -> None:
        self.workspace = workspace_root or WORKSPACE

    def probe_projection_lease(self) -> PatrolFinding | None:
        """探针 1: 全景驾驶舱投影租约时效检查 (超 15 分钟告警/自愈)."""
        proj_file = self.workspace / "runtime/dashboard/panorama-projection.json"
        if not proj_file.exists():
            return PatrolFinding(
                component="dashboard_projection",
                issue="全景投影文件不存在",
                risk_tier="R0",
                remediation_action="REFRESH_PROJECTION_LEASE",
                detail="panorama-projection.json 缺失，需触发一次全景刷新",
            )
        try:
            mtime = proj_file.stat().st_mtime
            age_s = time.time() - mtime
            if age_s > 900:  # 15 分钟
                return PatrolFinding(
                    component="dashboard_projection",
                    issue=f"全景投影租约超时 (已落后 {int(age_s)}s)",
                    risk_tier="R0",
                    remediation_action="REFRESH_PROJECTION_LEASE",
                    detail="全景雷达投影租约超时，可安全静默刷新",
                )
        except Exception as exc:
            logger.debug("Error checking projection lease: %s", exc)
        return None

    def probe_ssot_system_state(self) -> PatrolFinding | None:
        """探针 2: SSOT 运行态状态文件存在性检查."""
        sys_yaml = self.workspace / ".omo/state/system.yaml"
        if not sys_yaml.exists():
            return PatrolFinding(
                component="ssot_state",
                issue="system.yaml 运行态产物未物化",
                risk_tier="R0",
                remediation_action="FIX_SSOT_DRIFT",
                detail="system.yaml 缺失，可调用 materialize-system-state.py 自动对齐",
            )
        return None

    def probe_swarm_cluster(self) -> PatrolFinding | None:
        """探针 3: 算力集群存活性巡检."""
        try:
            from omo.resident.swarm_mesh import get_swarm_mesh

            mesh = get_swarm_mesh()
            summary = mesh.probe_mesh()
            if summary.get("online_nodes", 0) == 0:
                return PatrolFinding(
                    component="swarm_compute",
                    issue="算力集群全部节点离线 (含本地与哨兵)",
                    risk_tier="R1",
                    remediation_action=None,
                    detail="所有已知推理端口均无响应，需检查 LM Studio 或网络状态",
                )
        except Exception as exc:
            logger.debug("Swarm probe error: %s", exc)
        return None

    def run_patrol_cycle(self, auto_remediate: bool = True) -> dict[str, Any]:
        """执行一轮只读探针扫描并按需对 R0 实施受控自愈."""
        start_t = time.perf_counter()
        findings: list[PatrolFinding] = []

        # 依次运行轻量探针
        p1 = self.probe_projection_lease()
        if p1:
            findings.append(p1)

        p2 = self.probe_ssot_system_state()
        if p2:
            findings.append(p2)

        p3 = self.probe_swarm_cluster()
        if p3:
            findings.append(p3)

        elapsed_ms = (time.perf_counter() - start_t) * 1000

        remediations_executed = []
        escalated_tasks = []

        for f in findings:
            if f.risk_tier == "R0" and f.remediation_action and auto_remediate:
                # 安全执行受控枚举自愈
                success = self._execute_safe_remediation(f.remediation_action)
                remediations_executed.append(
                    {
                        "action_id": f.remediation_action,
                        "component": f.component,
                        "success": success,
                    }
                )
            elif f.risk_tier == "R1":
                escalated_tasks.append(
                    {
                        "component": f.component,
                        "issue": f.issue,
                        "detail": f.detail,
                    }
                )

        return {
            "ok": True,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "scan_duration_ms": round(elapsed_ms, 2),
            "total_findings": len(findings),
            "findings": [asdict(f) for f in findings],
            "remediations_executed": remediations_executed,
            "escalated_tasks": escalated_tasks,
            "health": "healthy" if not findings else ("warning" if not escalated_tasks else "degraded"),
        }

    def _execute_safe_remediation(self, action_id: str) -> bool:
        """受控执行 R0 自愈动作 (仅限白名单)."""
        # 与 api_reflection.py 共享白名单标准
        cmds = {
            "FIX_SSOT_DRIFT": ["python3", "bin/gac/materialize-system-state.py"],
            "REFRESH_PROJECTION_LEASE": ["python3", "bin/compass_radar.py", "--refresh-now"],
            "CLEAR_SUBMODULE_CACHE": ["git", "status", "--short"],
        }
        cmd = cmds.get(action_id)
        if not cmd:
            logger.warning("Disallowed remediation action: %s", action_id)
            return False
        try:
            res = subprocess.run(cmd, cwd=str(self.workspace), capture_output=True, timeout=10)
            return res.returncode == 0
        except Exception as exc:
            logger.error("Remediation %s failed: %s", action_id, exc)
            return False
