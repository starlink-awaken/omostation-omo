from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest
import yaml

from omo.omo_worker_core import (
    _build_launch_argv,
    _default_enabled_worker_id,
    _require_admitted_worker,
    _require_worker_policy,
)
from omo.omo_worker_dispatch import dispatch_task


def _task_fixture(root: Path, *, worker: dict) -> Path:
    active_dir = root / ".omo" / "tasks" / "active"
    registry_dir = root / ".omo" / "_truth" / "registry"
    active_dir.mkdir(parents=True)
    registry_dir.mkdir(parents=True)
    (registry_dir / "workers.yaml").write_text(yaml.safe_dump({"workers": [worker]}, sort_keys=False), encoding="utf-8")
    task_path = active_dir / "TASK-ADMISSION-GATE.yaml"
    task = {
        "id": "TASK-ADMISSION-GATE",
        "title": "Admission gate fixture",
        "status": "pending",
        "assigned_to": None,
        "dispatch_id": None,
        "run_ref": None,
        "approval_ref": None,
        "review_ref": None,
        "knowledge_refs": [],
        "handoff_refs": [],
        "risk_level": "L1",
        "allowed_operation_level": "L1",
        "human_approval_required": False,
        "source_docs": ["docs/source.md"],
        "entry_gate": [],
        "evidence_required": ["worker review"],
        "deliverables": ["docs/result.md"],
        "test_plan": ["pytest"],
    }
    if worker.get("require_explicit_capabilities") is True:
        task["required_capabilities"] = ["reasoning"]
    task_path.write_text(
        yaml.safe_dump(task, sort_keys=False),
        encoding="utf-8",
    )
    return task_path


def _worker(
    *,
    enabled: bool = True,
    admission_state: str = "admitted",
    transports: dict | None = None,
) -> dict:
    return {
        "id": "pi",
        "enabled": enabled,
        "admission_state": admission_state,
        "transports": transports
        if transports is not None
        else {
            "cli_prompt": {"command": "pi --prompt {prompt}"},
            "acp_stdio": {"command": "pi --acp --acp-transport stdio"},
        },
    }


def _admitted_pi_worker() -> dict:
    return {
        "id": "pi",
        "enabled": True,
        "admission_state": "admitted",
        "provider_ref": "pi",
        "role": "worker",
        "class": "external_agent_cli",
        "transports": {
            "cli_prompt": {
                "command": (
                    '/usr/bin/python3 "{workspace_root}/bin/gac/pi-worker-adapter.py" '
                    "run --execute "
                    '--timeout-seconds 120 --prompt "{prompt}"'
                )
            },
            "acp_stdio": {
                "command": (
                    '/usr/bin/python3 "{workspace_root}/bin/gac/pi-worker-adapter.py" run --acp --acp-transport stdio'
                )
            },
        },
        "capabilities": ["reasoning", "verification"],
        "require_explicit_capabilities": True,
        "allowed_operation_level": "L0",
        "forbidden_domains": ["apple", "wechat", "smb", "family", "media"],
        "write_scope": {"mode": "none"},
        "lease_policy": {
            "heartbeat_interval_seconds": 300,
            "warning_after_seconds": 900,
            "lease_expired_after_seconds": 1200,
            "reclaim_after_seconds": 1800,
        },
    }


def _file_snapshot(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (root / ".omo").rglob("*")
        if path.is_file()
    }


def test_dispatch_rejects_declared_worker_before_any_runtime_write(
    tmp_path: Path,
) -> None:
    task_path = _task_fixture(tmp_path, worker=_worker(enabled=False, admission_state="declared"))
    before = _file_snapshot(tmp_path)
    runs_dir = tmp_path / ".omo" / "workers" / "runs"
    mesh_log = tmp_path / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"

    with pytest.raises(
        ValueError,
        match=r"worker admission denied: worker_id=pi reason=disabled",
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=["docs/"],
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert yaml.safe_load(task_path.read_text(encoding="utf-8"))["status"] == "pending"
    assert not runs_dir.exists()
    assert not mesh_log.exists()


@pytest.mark.parametrize(
    ("worker_id", "registry", "reason"),
    [
        ("missing", {"workers": []}, "not_registered"),
        (
            "pi",
            {"workers": [_worker(enabled=False, admission_state="admitted")]},
            "disabled",
        ),
        (
            "pi",
            {"workers": [_worker(enabled=True, admission_state="declared")]},
            "not_admitted",
        ),
        (
            "pi",
            {"workers": [_worker(enabled=True, admission_state="admitted", transports={})]},
            "transport_missing",
        ),
    ],
)
def test_worker_admission_reasons_are_stable(worker_id: str, registry: dict, reason: str) -> None:
    with pytest.raises(
        ValueError,
        match=rf"worker admission denied: worker_id={worker_id} reason={reason}",
    ):
        _require_admitted_worker(registry, worker_id, "cli_prompt")


def test_worker_admission_returns_admitted_worker() -> None:
    worker = _worker()
    assert _require_admitted_worker({"workers": [worker]}, "pi", "cli_prompt") == worker


def test_default_worker_skips_declared_enabled_worker() -> None:
    declared = _worker(enabled=True, admission_state="declared")
    admitted = _worker(enabled=True, admission_state="admitted")
    admitted["id"] = "admitted-pi"
    assert _default_enabled_worker_id({"workers": [declared, admitted]}) == "admitted-pi"


def test_default_worker_requires_an_admitted_worker() -> None:
    with pytest.raises(ValueError, match="no admitted worker is registered"):
        _default_enabled_worker_id({"workers": [_worker(enabled=True, admission_state="declared")]})


def test_admitted_pi_worker_uses_one_shell_free_omo_transport(tmp_path: Path) -> None:
    pi = _admitted_pi_worker()

    assert pi["enabled"] is True
    assert pi["admission_state"] == "admitted"
    assert pi["provider_ref"] == "pi"
    assert pi["role"] == "worker"
    assert pi["class"] == "external_agent_cli"
    assert pi["capabilities"] == ["reasoning", "verification"]
    assert pi["require_explicit_capabilities"] is True
    assert pi["allowed_operation_level"] == "L0"
    assert pi["write_scope"] == {"mode": "none"}
    assert pi["transports"] == {
        "cli_prompt": {
            "command": (
                '/usr/bin/python3 "{workspace_root}/bin/gac/pi-worker-adapter.py" '
                "run --execute "
                '--timeout-seconds 120 --prompt "{prompt}"'
            )
        },
        "acp_stdio": {
            "command": (
                '/usr/bin/python3 "{workspace_root}/bin/gac/pi-worker-adapter.py" run --acp --acp-transport stdio'
            )
        },
    }
    assert "receipt" not in pi["transports"]["cli_prompt"]["command"]

    prompt = "quoted prompt; $(must remain one argv)"
    workspace_root = tmp_path / "omo workspace"
    workspace_root.mkdir()
    argv = _build_launch_argv(
        {"workers": [pi]},
        "pi",
        "cli_prompt",
        prompt,
        workspace_root=workspace_root,
    )

    assert argv == [
        "/usr/bin/python3",
        str(workspace_root / "bin/gac/pi-worker-adapter.py"),
        "run",
        "--execute",
        "--timeout-seconds",
        "120",
        "--prompt",
        prompt,
    ]
    assert argv.count(prompt) == 1
    assert "-c" not in argv
    assert not any(fragment in argument for argument in argv for fragment in ("&&", "||", "|"))


@pytest.mark.parametrize("worker_id", ["pi", "omp"])
def test_bound_worker_command_expands_delivery_identity_as_exact_argv_tokens(tmp_path: Path, worker_id: str) -> None:
    command = (
        f'/usr/bin/{worker_id} "{{prompt}}" '
        '--run-id "{run_id}" '
        '--packet-id "{packet_id}" '
        '--packet-hash "{packet_hash}" '
        '--instruction-binding-json "{instruction_binding_json}"'
    )
    worker = _worker()
    worker["id"] = worker_id
    worker["transports"] = {"cli_prompt": {"command": command}}
    instruction_binding = {
        "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
        "instruction_version": "blueprint-agent-instruction-pack/v1",
        "content_digest": "sha256:" + "b" * 64,
        "instruction_profile": "executor",
    }

    argv = _build_launch_argv(
        {"workers": [worker]},
        worker_id,
        "cli_prompt",
        "prompt with spaces",
        workspace_root=tmp_path,
        run_id="run-001",
        packet_id="WP-BP-0123456789abcdef",
        packet_hash="sha256:" + "a" * 64,
        instruction_binding=instruction_binding,
    )

    assert argv == [
        f"/usr/bin/{worker_id}",
        "prompt with spaces",
        "--run-id",
        "run-001",
        "--packet-id",
        "WP-BP-0123456789abcdef",
        "--packet-hash",
        "sha256:" + "a" * 64,
        "--instruction-binding-json",
        json.dumps(instruction_binding, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    ]


@pytest.mark.parametrize(
    ("task_level", "allowed_paths", "task_capabilities", "packet", "reason"),
    [
        ("L1", [], [], None, "operation_level_exceeded"),
        ("L0", ["docs/"], [], None, "write_scope_denied"),
        ("L0", [], ["code_change"], None, "capability_mismatch"),
        (
            "L0",
            [],
            [],
            {"admission": {"capabilities": ["runtime"]}},
            "capability_mismatch",
        ),
    ],
)
def test_pi_policy_rejection_is_side_effect_free(
    tmp_path: Path,
    task_level: str,
    allowed_paths: list[str],
    task_capabilities: list[str],
    packet: dict | None,
    reason: str,
) -> None:
    pi = _admitted_pi_worker()
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = task_level
    task["allowed_operation_level"] = task_level
    if task_capabilities:
        task["required_capabilities"] = task_capabilities
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(
        ValueError,
        match=rf"worker policy denied: worker_id=pi reason={reason}",
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=allowed_paths,
            workflow_packet=packet,
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()
    assert not (tmp_path / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl").exists()


def test_task_risk_level_cannot_be_downgraded_by_allowed_operation_level(
    tmp_path: Path,
) -> None:
    pi = _admitted_pi_worker()
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L3"
    task["allowed_operation_level"] = "L0"
    task["human_approval_required"] = True
    task["approval_ref"] = "APPROVAL-TEST"
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(
        ValueError,
        match=(
            r"worker policy denied: worker_id=pi reason=operation_level_exceeded "
            r"requested=L3 allowed=L0"
        ),
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()
    assert not (tmp_path / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl").exists()


@pytest.mark.parametrize(
    ("location", "field", "value"),
    [
        ("task", "required_capabilities", []),
        ("task", "capabilities", "reasoning"),
        ("packet", "required_capabilities", [""]),
        ("packet", "capabilities", ["reasoning", ""]),
        ("admission", "capabilities", [1]),
    ],
)
def test_invalid_capability_requirements_are_side_effect_free(
    tmp_path: Path, location: str, field: str, value: object
) -> None:
    pi = _admitted_pi_worker()
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    packet: dict | None = None
    if location == "task":
        task[field] = value
    elif location == "packet":
        packet = {field: value}
    else:
        packet = {"admission": {field: value}}
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(
        ValueError,
        match=r"worker policy denied: worker_id=pi reason=invalid_capability_requirements",
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_explicit_capability_policy_rejects_missing_requirements_without_writes(
    tmp_path: Path,
) -> None:
    pi = _admitted_pi_worker()
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    task.pop("required_capabilities")
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(
        ValueError,
        match=r"worker policy denied: worker_id=pi reason=capability_requirements_missing",
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_policy_helper_preserves_legacy_workers_without_new_policy_fields() -> None:
    worker = _worker()
    task = {"allowed_operation_level": "L1"}

    assert (
        _require_worker_policy(
            {"default_allowed_operation_level": "L1"},
            worker,
            task,
            allowed_write_paths=["docs/"],
        )
        == worker
    )


def test_legacy_worker_must_declare_nonempty_required_capabilities() -> None:
    worker = _worker()

    with pytest.raises(
        ValueError,
        match=r"worker policy denied: worker_id=pi reason=capability_mismatch",
    ):
        _require_worker_policy(
            {"default_allowed_operation_level": "L1"},
            worker,
            {
                "allowed_operation_level": "L1",
                "required_capabilities": ["runtime"],
            },
            allowed_write_paths=[],
        )


def test_admitted_pi_worker_without_packet_is_observer_only(
    tmp_path: Path,
) -> None:
    pi = _admitted_pi_worker()
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(ValueError, match="observer-only"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_invalid_command_template_is_rejected_before_run_artifacts(
    tmp_path: Path,
) -> None:
    pi = _admitted_pi_worker()
    pi["transports"]["acp_stdio"]["command"] = 'pi "{unknown_placeholder}"'
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(ValueError, match="invalid worker command template"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            launch=False,
            now="2026-08-13T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_unbound_launch_is_rejected_before_provider_call(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pi = _admitted_pi_worker()
    task_path = _task_fixture(tmp_path, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.run", lambda *_args, **_kwargs: pytest.fail("no launch"))

    with pytest.raises(
        ValueError,
        match="observer-only",
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            launch=True,
            now="2026-08-13T01:02:03+00:00",
        )

    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_interactive_supervisor_worker_rejects_legacy_direct_launch_without_writes(
    tmp_path: Path,
) -> None:
    worker = _admitted_pi_worker()
    worker["id"] = "codex"
    worker["supervision"] = {"controller_direct_start_required": True}
    task_path = _task_fixture(tmp_path, worker=worker)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = _file_snapshot(tmp_path)

    with pytest.raises(
        ValueError,
        match="controller direct start is required for worker_id=codex",
    ):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="codex",
            allowed_write_paths=[],
            launch=True,
            now="2026-08-14T01:02:03+00:00",
        )

    assert _file_snapshot(tmp_path) == before
