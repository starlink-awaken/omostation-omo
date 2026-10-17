"""test_resident_night_patrol — 验证 MBP 128G 电源感知夜间深度巡检引擎."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from omo.resident.night_patrol import (
    check_architecture_drift,
    check_power_and_idle,
    collect_recent_commits,
    draft_decision_proposal,
    run_night_patrol,
)


def test_check_power_and_idle_force():
    """验证 --force 参数能够忽略电源与时间限制."""
    ok, msg = check_power_and_idle(force=True)
    assert ok is True
    assert "强制" in msg


def test_check_power_and_idle_battery():
    """验证电池供电状态下自动跳过巡检."""
    with patch("omo.resident.night_patrol.subprocess.run") as mock_run:
        mock_run.return_value.stdout = (
            "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=123) 85%; discharging;"
        )
        ok, msg = check_power_and_idle(force=False)
        assert ok is False
        assert "电池供电" in msg


def test_check_power_and_idle_time_window():
    """验证白天工作时间自动拦截."""
    with (
        patch("omo.resident.night_patrol.subprocess.run") as mock_run,
        patch("omo.resident.night_patrol.datetime") as mock_dt,
    ):
        mock_run.return_value.stdout = "Now drawing from 'AC Power'\n"
        mock_dt.now.return_value.hour = 14  # 下午 2 点
        ok, msg = check_power_and_idle(force=False)
        assert ok is False
        assert "夜间闲置窗口" in msg


def test_collect_recent_commits():
    """验证 Git 提交记录收集与结构化解析."""
    with patch("omo.resident.night_patrol.subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "a1b2c3d|xiamingxing|feat(resident): add night patrol|2026-10-16\n"
            "e4f5g6h|xiamingxing|fix(lifeos): guard memory budget|2026-10-16\n"
        )
        commits = collect_recent_commits(hours=24)
        assert len(commits) == 2
        assert commits[0]["hash"] == "a1b2c3d"
        assert commits[0]["subject"] == "feat(resident): add night patrol"


def test_check_architecture_drift(tmp_path):
    """验证架构漂移检查."""
    with patch("omo.resident.night_patrol.WORKSPACE", tmp_path):
        # 模拟产生散落测试脚本
        (tmp_path / "test_scratch.py").write_text("# temp\n")
        report = check_architecture_drift()
        assert report["drift_detected"] is True
        assert any("散落测试脚本" in it for it in report["items"])


def test_draft_decision_proposal(tmp_path):
    """验证决策提案自动起草."""
    with patch("omo.resident.night_patrol.PROPOSALS_DIR", tmp_path):
        proposal_path = draft_decision_proposal(
            title="解决测试碎片散落问题",
            rationale="- 根目录下发现孤立脚本",
            action="归档至 projects/omo/tests/",
        )
        assert proposal_path is not None
        assert proposal_path.exists()
        body = proposal_path.read_text(encoding="utf-8")
        assert "架构演进提案: 解决测试碎片散落问题" in body
        assert "MBP 128G 夜间巡检 Taskforce" in body


def test_run_night_patrol_skip_on_battery():
    """验证巡检在电池状态下优雅跳过."""
    with patch("omo.resident.night_patrol.check_power_and_idle") as mock_check:
        mock_check.return_value = (False, "电池续航")
        res = run_night_patrol(force=False)
        assert res["ok"] is False
        assert res["skipped"] is True
        assert "电池续航" in res["reason"]


def test_run_night_patrol_success(tmp_path):
    """验证巡检在 mock 环境下的完整闭环执行."""
    with (
        patch("omo.resident.night_patrol.check_power_and_idle") as mock_check,
        patch("omo.resident.night_patrol.collect_recent_commits") as mock_commits,
        patch("omo.resident.night_patrol.check_architecture_drift") as mock_drift,
        patch("omo.resident.night_patrol.REPORTS_DIR", tmp_path / "reports"),
        patch("omo.resident.night_patrol.CARDS_DIR", tmp_path / "cards"),
    ):
        mock_check.return_value = (True, "AC 电源常驻")
        mock_commits.return_value = [{"hash": "123", "author": "me", "subject": "test", "date": "today"}]
        mock_drift.return_value = {"drift_detected": False, "items": []}

        res = run_night_patrol(force=True, deep=False)
        assert res["ok"] is True
        assert res["commits_reviewed"] == 1
        assert res["drift_audit"]["drift_detected"] is False

        # 检查最新报告文件已写入
        report_file = tmp_path / "reports" / "night-patrol-latest.json"
        assert report_file.exists()
        saved = json.loads(report_file.read_text(encoding="utf-8"))
        assert saved["commits_reviewed"] == 1
