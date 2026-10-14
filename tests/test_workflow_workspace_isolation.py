"""Workflow lifecycle effects must stay inside the registry-owned workspace."""

from __future__ import annotations

import json
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


def test_production_layout_in_linked_worktree_bridges_to_canonical(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """BET-Y2Q4-T10-208: production runner layout inside a linked worktree of
    the canonical checkout writes through a symlink bridge, while the dict
    path stays worktree-relative (#4435: survives `worktree remove --force`)."""
    canonical = tmp_path / "canonical"
    (canonical / "docs").mkdir(parents=True)
    (canonical / "docs" / "project-registry.yaml").write_text("schema: project-registry/v1\n", encoding="utf-8")
    (canonical / ".git" / "worktrees" / "wt").mkdir(parents=True)
    workspace = tmp_path / "worktree"
    workspace.mkdir()
    (workspace / ".git").write_text(f"gitdir: {canonical / '.git' / 'worktrees' / 'wt'}\n", encoding="utf-8")
    monkeypatch.setenv("OMOSTATION_ROOT", str(canonical))
    monkeypatch.delenv("OMOSTATION_STATE_ROOT", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "empty-home"))
    registry = {"runner": {"workspace_root": str(workspace)}}

    run_dir = core_mod.run_state_dir(registry)
    lock_dir = core_mod.lock_state_dir(registry)
    ledger = core_mod.ledger_path(registry)

    bridge = workspace / ".omo" / "_delivery" / "agent-workflows"
    assert bridge.is_symlink()
    assert run_dir == workspace.resolve() / ".omo/_delivery/agent-workflows/runs"
    assert lock_dir == workspace.resolve() / ".omo/_delivery/agent-workflows/locks"
    assert ledger == workspace.resolve() / ".omo/_delivery/agent-workflows/events.jsonl"
    canonical_delivery = canonical / ".omo" / "_delivery" / "agent-workflows"
    assert run_dir.resolve() == (canonical_delivery / "runs").resolve()
    assert ledger.resolve() == (canonical_delivery / "events.jsonl").resolve()


def test_observe_matches_absolute_lock_ref_inside_registry_root(tmp_path: Path, monkeypatch) -> None:
    runtime_workspace = tmp_path / "runtime-workspace"
    registry = _registry(runtime_workspace)
    run_id = "run-absolute-lock"
    lock_path = runtime_workspace / "locks" / "path_foo.py.lock.yaml"
    run_path = runtime_workspace / "runs" / f"{run_id}.yaml"
    lock_path.parent.mkdir(parents=True)
    run_path.parent.mkdir(parents=True)
    lock_path.write_text(yaml.safe_dump({"run_id": run_id, "scope": "path:foo.py"}), encoding="utf-8")
    run_path.write_text(
        yaml.safe_dump(
            {
                "run_id": run_id,
                "workflow_id": "mini",
                "status": "active",
                "locks": [str(lock_path.resolve())],
            }
        ),
        encoding="utf-8",
    )
    (runtime_workspace / "events.jsonl").write_text(
        json.dumps({"event": "agent_workflow_start", "run_id": run_id}) + "\n",
        encoding="utf-8",
    )
    # Simulate the source checkout differing from the registry-owned runtime root.
    monkeypatch.setattr(core_mod, "WORKSPACE", runtime_workspace)

    report = diagnostics_mod.build_observe_report(registry, run_id)

    assert report["decision"] == "continue"
    assert not any(item["kind"] == "active_run_missing_locks" for item in report["findings"])


def test_observe_halts_absolute_lock_ref_outside_registry_root(tmp_path: Path, monkeypatch) -> None:
    runtime_workspace = tmp_path / "runtime-workspace"
    registry = _registry(runtime_workspace)
    run_id = "run-external-lock"
    run_path = runtime_workspace / "runs" / f"{run_id}.yaml"
    run_path.parent.mkdir(parents=True)
    external_ref = (tmp_path / "external-locks" / "path_foo.py.lock.yaml").resolve()
    run_path.write_text(
        yaml.safe_dump(
            {
                "run_id": run_id,
                "workflow_id": "mini",
                "status": "active",
                "locks": [str(external_ref)],
            }
        ),
        encoding="utf-8",
    )
    (runtime_workspace / "events.jsonl").write_text(
        json.dumps({"event": "agent_workflow_start", "run_id": run_id}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(core_mod, "WORKSPACE", runtime_workspace)

    report = diagnostics_mod.build_observe_report(registry, run_id)

    assert report["decision"] == "halt"
    assert any(
        item["kind"] == "lock_path_outside_registry_root" and item["severity"] == "halt" for item in report["findings"]
    )


def test_observe_halts_active_run_with_non_list_locks_payload(tmp_path: Path, monkeypatch) -> None:
    runtime_workspace = tmp_path / "runtime-workspace"
    registry = _registry(runtime_workspace)
    run_id = "run-invalid-lock-payload"
    run_path = runtime_workspace / "runs" / f"{run_id}.yaml"
    run_path.parent.mkdir(parents=True)
    run_path.write_text(
        yaml.safe_dump(
            {
                "run_id": run_id,
                "workflow_id": "mini",
                "status": "active",
                "locks": {"path": "locks/path_foo.py.lock.yaml"},
            }
        ),
        encoding="utf-8",
    )
    (runtime_workspace / "events.jsonl").write_text(
        json.dumps({"event": "agent_workflow_start", "run_id": run_id}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(core_mod, "WORKSPACE", runtime_workspace)

    report = diagnostics_mod.build_observe_report(registry, run_id)

    assert report["decision"] == "halt"
    assert any(
        item["kind"] == "invalid_lock_payload" and item["severity"] == "halt" and item["run_id"] == run_id
        for item in report["findings"]
    )


def test_observe_halts_active_run_with_invalid_lock_entries(tmp_path: Path, monkeypatch) -> None:
    runtime_workspace = tmp_path / "runtime-workspace"
    registry = _registry(runtime_workspace)
    run_id = "run-invalid-lock-entries"
    run_path = runtime_workspace / "runs" / f"{run_id}.yaml"
    run_path.parent.mkdir(parents=True)
    run_path.write_text(
        yaml.safe_dump(
            {
                "run_id": run_id,
                "workflow_id": "mini",
                "status": "active",
                "locks": [None, "", "   ", 42],
            }
        ),
        encoding="utf-8",
    )
    (runtime_workspace / "events.jsonl").write_text(
        json.dumps({"event": "agent_workflow_start", "run_id": run_id}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(core_mod, "WORKSPACE", runtime_workspace)

    report = diagnostics_mod.build_observe_report(registry, run_id)

    invalid_findings = [item for item in report["findings"] if item["kind"] == "invalid_lock_ref"]
    assert report["decision"] == "halt"
    assert [item["index"] for item in invalid_findings] == [0, 1, 2, 3]
    assert all(item["severity"] == "halt" and item["run_id"] == run_id for item in invalid_findings)


def test_heartbeat_resolves_legacy_relative_lock_from_registry_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime_workspace = tmp_path / "runtime-workspace"
    source_workspace = tmp_path / "source-workspace"
    registry = _registry(runtime_workspace)
    lock_path = runtime_workspace / "locks/path_foo.py.lock.yaml"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(
        yaml.safe_dump(
            {
                "run_id": "run-relative-lock",
                "actor": "agent-a",
                "scope": "path:foo.py",
                "created_at": "2026-08-21T00:00:00Z",
                "last_heartbeat": "2026-08-21T00:00:00Z",
                "expires_at": "2026-08-22T00:00:00Z",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    run_path = runtime_workspace / "runs/run-relative-lock.yaml"
    run_path.parent.mkdir(parents=True)
    run_path.write_text(
        yaml.safe_dump(
            {
                "run_id": "run-relative-lock",
                "workflow_id": "mini",
                "status": "active",
                "locks": ["locks/path_foo.py.lock.yaml"],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(lifecycle_mod, "WORKSPACE", source_workspace)

    receipt = lifecycle_mod.heartbeat_run(registry, "run-relative-lock")

    assert receipt["renewed"] == ["locks/path_foo.py.lock.yaml"]
    refreshed = yaml.safe_load(lock_path.read_text(encoding="utf-8"))
    assert refreshed["last_heartbeat"] == receipt["heartbeat_at"]


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
