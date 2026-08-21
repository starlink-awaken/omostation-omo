"""Workflow lifecycle effects must stay inside the registry-owned workspace."""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

import omo.workflow.core as core_mod
import omo.workflow.diagnostics as diagnostics_mod
import omo.workflow.lifecycle as lifecycle_mod
from omo.omo_belief import MOSBeliefManager


def _registry(workspace: Path) -> dict:
    return {
        "runner": {
            "workspace_root": str(workspace),
            "run_state_dir": "runs",
            "lock_state_dir": "locks",
            "ledger_path": "events.jsonl",
        }
    }


def test_registry_workspace_root_anchors_relative_runtime_paths(tmp_path: Path) -> None:
    registry = _registry(tmp_path)

    assert core_mod.registry_workspace_root(registry) == tmp_path.resolve()
    assert core_mod.run_state_dir(registry) == tmp_path / "runs"
    assert core_mod.lock_state_dir(registry) == tmp_path / "locks"
    assert core_mod.ledger_path(registry) == tmp_path / "events.jsonl"


def test_closeout_side_effects_persist_only_under_registry_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace = tmp_path / "isolated-workspace"
    (workspace / "projects" / "omo").mkdir(parents=True)
    registry = _registry(workspace)
    mos = MOSBeliefManager(root=workspace)
    mos.record_belief(topic="workflow:mini", belief_text="seed")

    subprocess_calls: list[tuple[tuple, dict]] = []

    def fake_run(*args, **kwargs):
        subprocess_calls.append((args, kwargs))
        return subprocess.CompletedProcess(args=args[0], returncode=0)

    monkeypatch.setattr(lifecycle_mod.subprocess, "run", fake_run)

    lifecycle_mod._run_closeout_side_effects(
        registry,
        {"workflow_id": "mini", "objective": "isolated closeout", "path": "runs/run-1.yaml"},
        "run-1",
    )

    state = yaml.safe_load((workspace / ".omo/state/agent-beliefs/index.yaml").read_text(encoding="utf-8"))
    assert len(state["beliefs"]) == 2
    assert (workspace / ".agents/skills/workflow:mini/SKILL.md").is_file()
    assert subprocess_calls
    assert all(call[1].get("cwd") in {workspace, workspace / "projects/omo"} for call in subprocess_calls)


def test_close_run_routes_mesh_event_to_registry_workspace(tmp_path: Path, monkeypatch) -> None:
    registry = _registry(tmp_path)
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    (run_dir / "run-1.yaml").write_text(
        yaml.safe_dump({"run_id": "run-1", "workflow_id": "mini", "status": "active"}),
        encoding="utf-8",
    )
    captured: list[Path] = []

    def capture_event(*_args, workspace=None, **_kwargs):
        captured.append(Path(workspace))
        return True

    monkeypatch.setattr(lifecycle_mod, "emit_workflow_mesh_event", capture_event)

    lifecycle_mod.close_run(registry, "run-1", "blocked", ["test"], release=False)

    assert captured == [tmp_path.resolve()]


def test_successful_closeout_emits_one_rich_mesh_event_in_registry_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry = _registry(tmp_path)
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    (run_dir / "run-1.yaml").write_text(
        yaml.safe_dump(
            {
                "run_id": "run-1",
                "workflow_id": "mini",
                "objective": "isolated closeout",
                "status": "active",
                "locks": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(lifecycle_mod, "heartbeat_run", lambda *_args, **_kwargs: {"count": 0})
    monkeypatch.setattr(
        diagnostics_mod,
        "build_verify_report",
        lambda *_args, **_kwargs: {"ok": True, "check_count": 1},
    )
    monkeypatch.setattr(
        diagnostics_mod,
        "build_observe_report",
        lambda *_args, **_kwargs: {"ok": True, "decision": "continue"},
    )
    side_effect_roots: list[Path] = []
    monkeypatch.setattr(
        lifecycle_mod,
        "_run_closeout_side_effects",
        lambda reg, *_args: side_effect_roots.append(core_mod.registry_workspace_root(reg)),
    )
    emitted: list[tuple[str, Path, dict]] = []

    def capture_event(event_type, _run_id, payload, workspace=None, **_kwargs):
        emitted.append((event_type, Path(workspace), payload))
        return True

    monkeypatch.setattr(lifecycle_mod, "emit_workflow_mesh_event", capture_event)

    report = lifecycle_mod.closeout_run(
        registry,
        "run-1",
        "ok",
        ["test"],
        ["README.md"],
        False,
        False,
        False,
        False,
    )

    assert report["ok"] is True
    assert side_effect_roots == [tmp_path.resolve()]
    assert len(emitted) == 1
    assert emitted[0][0] == "AgentWorkflowClosed"
    assert emitted[0][1] == tmp_path.resolve()
    assert emitted[0][2]["verify_ok"] is True
    assert emitted[0][2]["observe_decision"] == "continue"
