#!/usr/bin/env python3
"""tests/unit/test_resident_service_runner.py — 单元测试常驻服务调度器与 LaunchAgent 控制 (Phase 9)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from omo.resident.service_ctl import generate_plist_xml, get_service_status
from omo.resident.service_runner import ResidentServiceRunner, ServiceState


def test_service_runner_state_persistence():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        runner = ResidentServiceRunner(workspace_root=ws)
        assert runner.state.status == "running"

        # 触发巡检模拟
        with patch.object(runner, "run_patrol_job", return_value={"ok": True}):
            res = runner.run_single_job("patrol")
            assert res["ok"] is True

        # 触发简报模拟
        with patch.object(runner, "run_briefing_job", return_value={"ok": True}):
            res = runner.run_single_job("briefing")
            assert res["ok"] is True

        # 重建实例检查持久化恢复
        runner.save_state()
        runner2 = ResidentServiceRunner(workspace_root=ws)
        assert runner2.state.status == "running"


def test_generate_plist_xml():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        xml = generate_plist_xml(ws)
        assert "com.omostation.resident-agent" in xml
        assert "<key>KeepAlive</key>" in xml
        assert "<true/>" in xml
        assert str(ws) in xml
        assert "resident-daemon.log" in xml


def test_get_service_status():
    with tempfile.TemporaryDirectory() as tmp_dir:
        ws = Path(tmp_dir)
        status = get_service_status(ws)
        assert "installed" in status
        assert "label" in status
        assert status["label"] == "com.omostation.resident-agent"
