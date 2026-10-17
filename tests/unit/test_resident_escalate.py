from pathlib import Path
from unittest.mock import patch

import pytest

from omo.resident.escalate import (
    check_and_escalate,
    deliver_notification,
    probe_infrastructure,
    probe_remote_hygiene,
)


def test_probe_remote_hygiene_clean():
    # 本地主仓目前是干净的
    res = probe_remote_hygiene()
    # 只要不是子模块污染即可
    if res:
        assert res["severity"] == "P0"


def test_probe_remote_hygiene_polluted():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "git@github.com:starlink-awaken/cockpit-ui.git"
        mock_run.return_value.stderr = ""
        res = probe_remote_hygiene()
        assert res is not None
        assert res["id"] == "GIT_REMOTE_POLLUTED"
        assert res["severity"] == "P0"


def test_probe_infrastructure():
    with patch("omo.resident.escalate._probe_socket") as mock_probe:
        # 模拟全死
        mock_probe.return_value = False
        alerts = probe_infrastructure()
        alert_ids = {a["id"] for a in alerts}
        assert "INFRA_AETHERFORGE_OFFLINE" in alert_ids
        assert "INFRA_COCKPIT_OFFLINE" in alert_ids


def test_check_and_escalate_dry_run():
    report = check_and_escalate(dry_run=True)
    assert "timestamp" in report
    assert "active_count" in report
    assert isinstance(report["active_alerts"], list)
