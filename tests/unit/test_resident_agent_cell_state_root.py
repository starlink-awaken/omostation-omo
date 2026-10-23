"""Agent Cell state writes and Panorama's collector must share one canonical root."""

from __future__ import annotations

from pathlib import Path

import pytest

from omo.omo_paths import STATE_ROOT_ENV, WORKSPACE_ROOT
from omo.resident.cell_governance import AUDIT_FILE_RELPATH
from omo.resident.cell_memory_network import NETWORK_DIR_RELPATH
from omo.resident.cell_state import STATE_DIR_RELPATH


def test_agent_cell_state_roots_are_undeclared_workspace_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(STATE_ROOT_ENV, raising=False)

    expected = WORKSPACE_ROOT / ".omo" / "state" / "agent-cell"

    assert STATE_DIR_RELPATH == expected.relative_to(WORKSPACE_ROOT)
    assert NETWORK_DIR_RELPATH == (expected / "memory-network").relative_to(WORKSPACE_ROOT)
    assert AUDIT_FILE_RELPATH == (expected / "governance-audit.jsonl").relative_to(WORKSPACE_ROOT)


def test_agent_cell_state_roots_follow_declared_state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    declared = tmp_path / "state-root"
    monkeypatch.setenv(STATE_ROOT_ENV, str(declared))

    from omo.resident.cell_governance import CellGovernance
    from omo.resident.cell_memory_network import MemoryNetwork
    from omo.resident.cell_state import CellStateManager

    assert CellStateManager().state_dir() == declared / ".omo" / "state" / "agent-cell"
    assert MemoryNetwork().network_dir() == declared / ".omo" / "state" / "agent-cell" / "memory-network"
    assert CellGovernance().audit_file() == declared / ".omo" / "state" / "agent-cell" / "governance-audit.jsonl"
