"""test_workflow_role_admission.py — Role 准入门卫测试（BET-Y1Q4-T10-165）。"""

import tempfile
from pathlib import Path

import pytest
import yaml

from omo.workflow.role_admission import (
    VALID_STATES,
    AdmissionError,
    RoleAdmission,
    can_act,
    check_transition,
    load_registry,
    render_gate_report,
)


@pytest.fixture
def reg_file(tmp_path: Path) -> Path:
    data = {
        "roles": [
            {"role_id": "role:planner", "state": "admitted", "adapter": "direct-local", "evidence_ref": "sha256:abc"},
            {"role_id": "role:orca-agent", "state": "r0_canary", "adapter": "orca", "evidence_ref": ""},
            {"role_id": "role:multica-agent", "state": "observer", "adapter": "multica", "evidence_ref": ""},
        ]
    }
    p = tmp_path / "role-admission.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


class TestLoad:
    def test_load(self, reg_file: Path) -> None:
        reg = load_registry(reg_file)
        assert len(reg) == 3
        assert reg["role:planner"].state == "admitted"

    def test_missing_file(self, tmp_path: Path) -> None:
        assert load_registry(tmp_path / "nope.yaml") == {}

    def test_illegal_state(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.safe_dump({"roles": [{"role_id": "x", "state": "galaxy"}]}), encoding="utf-8")
        with pytest.raises(AdmissionError):
            load_registry(p)


class TestCanAct:
    def test_admitted_can_all(self, reg_file: Path) -> None:
        reg = load_registry(reg_file)
        assert can_act(reg, "role:planner", "write")
        assert can_act(reg, "role:planner", "autonomous")
        assert can_act(reg, "role:planner", "scale")

    def test_r0_cannot_write(self, reg_file: Path) -> None:
        reg = load_registry(reg_file)
        assert not can_act(reg, "role:orca-agent", "write")
        assert not can_act(reg, "role:orca-agent", "scale")

    def test_unknown_role_fail_closed(self, reg_file: Path) -> None:
        reg = load_registry(reg_file)
        assert not can_act(reg, "role:ghost", "write")

    def test_unknown_action_denied(self, reg_file: Path) -> None:
        reg = load_registry(reg_file)
        assert not can_act(reg, "role:planner", "teleport")


class TestTransition:
    def test_forward(self) -> None:
        check_transition("observer", "r0_canary")
        check_transition("r0_canary", "as0")
        check_transition("as0", "admitted")

    def test_no_skip(self) -> None:
        with pytest.raises(AdmissionError):
            check_transition("observer", "admitted")

    def test_rollback_allowed(self) -> None:
        check_transition("as0", "observer")


class TestReport:
    def test_blocked_reason_shown(self, reg_file: Path) -> None:
        reg = load_registry(reg_file)
        rep = render_gate_report(reg)
        orca = next(r for r in rep["roles"] if r["role_id"] == "role:orca-agent")
        assert orca["can_write"] is False
        assert orca["blocked_reason"] is not None
        assert "未过门" in orca["blocked_reason"]
