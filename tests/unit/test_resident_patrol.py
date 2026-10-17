"""test_resident_patrol — 验证自治主动巡检与静默自愈机器人 (Phase 7)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from omo.resident.patrol import PatrolFinding, ResidentPatrolRunner


def test_patrol_runner_healthy_cycle():
    """验证健康基线下巡检快速通过且耗时可控."""
    runner = ResidentPatrolRunner()
    report = runner.run_patrol_cycle(auto_remediate=False)
    assert report["ok"] is True
    # 耗时控制在低开销范围内 (容忍 CI 虚拟化负载抖动)
    assert report["scan_duration_ms"] < 3000
    assert report["health"] in ("healthy", "warning", "degraded")


def test_patrol_runner_r0_detection_and_remediation(tmp_path):
    """验证 R0 级轻微漂移 (如 system.yaml 缺失) 自动触发静默自愈."""
    fake_workspace = tmp_path / "mock_ws"
    fake_workspace.mkdir()
    state_dir = fake_workspace / ".omo/state"
    state_dir.mkdir(parents=True)
    # 模拟投影文件存在且新鲜
    proj_dir = fake_workspace / "runtime/dashboard"
    proj_dir.mkdir(parents=True)
    (proj_dir / "panorama-projection.json").write_text("{}", encoding="utf-8")
    # system.yaml 故意缺失

    runner = ResidentPatrolRunner(workspace_root=fake_workspace)
    finding = runner.probe_ssot_system_state()
    assert finding is not None
    assert finding.risk_tier == "R0"
    assert finding.remediation_action == "FIX_SSOT_DRIFT"

    with patch.object(runner, "_execute_safe_remediation", return_value=True) as mock_exec:
        report = runner.run_patrol_cycle(auto_remediate=True)
        assert len(report["remediations_executed"]) > 0
        action_ids = [r["action_id"] for r in report["remediations_executed"]]
        assert "FIX_SSOT_DRIFT" in action_ids
        mock_exec.assert_called_with("FIX_SSOT_DRIFT")


def test_patrol_runner_r1_escalation():
    """验证 R1 级断链 (如全部算力节点离线) 自动升级待办而不盲目自愈."""
    runner = ResidentPatrolRunner()
    with patch("omo.resident.swarm_mesh.get_swarm_mesh") as mock_get_mesh:
        mock_mesh = MagicMock()
        mock_mesh.probe_mesh.return_value = {"online_nodes": 0}
        mock_get_mesh.return_value = mock_mesh

        finding = runner.probe_swarm_cluster()
        assert finding is not None
        assert finding.risk_tier == "R1"
        assert finding.remediation_action is None

        report = runner.run_patrol_cycle(auto_remediate=True)
        assert len(report["escalated_tasks"]) > 0
        assert report["health"] == "degraded"
