from __future__ import annotations

import hashlib
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

import omo.worker_lifecycle as worker_lifecycle_mod
import omo.workflow_dispatch as workflow_dispatch_mod
from omo.omo_worker_core import (
    _build_launch_argv,
    _default_enabled_worker_id,
    _require_admitted_worker,
    _require_worker_policy,
)
from omo.omo_worker_dispatch import dispatch_task
from omo.worker_lifecycle import (
    WorkerLifecycleError,
    acknowledge_worker,
    new_worker_ack_origin_proof,
    record_step_dispatch,
)
from omo.workflow_mesh import WorkflowMeshEventError, WorkflowMeshStore, new_workflow_event


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


INSTRUCTION_BINDING = {
    "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
    "instruction_version": "blueprint-agent-instruction-pack/v1",
    "content_digest": "sha256:" + "c" * 64,
    "instruction_profile": "executor",
}


def _seed_exact_workflow_packet(
    root: Path,
    *,
    persisted_dispatch_id: str = "preflight:run-production-exact:dispatch",
    ttl_seconds: int = 900,
) -> tuple[dict, dict]:
    run_id = "run-production-exact"
    requirements = [{"capability_id": "skill:git-discipline", "operation": "load", "effect": "read_only"}]
    requirements_digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(requirements, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    exact_identity = {
        "bet_id": "BET-BOUND",
        "workflow_id": "test-workflow",
        "correlation_id": run_id,
        "workflow_run_id": run_id,
        "packet_id": "WP-BET-PRODUCTION",
        "packet_hash": "sha256:" + "a" * 64,
        "assignment_id": "preflight:run-production-exact:assignment",
        "dispatch_id": persisted_dispatch_id,
        "actor_id": "actor:production-test",
        "delivery_attempt_id": "attempt:production-test",
        "capability_requirements": requirements,
        "capability_requirements_digest": requirements_digest,
    }
    policy = {
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": exact_identity["bet_id"],
        "workflow_id": "test-workflow",
        "workflow_run_id": run_id,
        "packet_id": exact_identity["packet_id"],
        "packet_hash": exact_identity["packet_hash"],
        "capability_requirements": requirements,
        "capability_requirements_digest": requirements_digest,
        "actor_id": exact_identity["actor_id"],
        "delivery_attempt_id": exact_identity["delivery_attempt_id"],
        "source_receipt_digests": ["sha256:" + "3" * 64],
        "requested_budget": 0.0,
    }
    issued_at = datetime.now(UTC).replace(microsecond=0)
    grant = {
        "admission_id": "admit-production-exact",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "agent-workflow",
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": exact_identity["bet_id"],
        "workflow_id": exact_identity["workflow_id"],
        "step_run_ids": [f"{run_id}:execute"],
        "capabilities": ["reasoning"],
        "policy_digest": hashlib.sha256(
            json.dumps(policy, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "issued_at": issued_at.isoformat(),
        "expires_at": (issued_at + timedelta(seconds=ttl_seconds)).isoformat(),
        "request_identity": exact_identity,
    }
    grant["proof"] = hashlib.sha256(
        json.dumps(grant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    store = WorkflowMeshStore(root / ".omo")
    store.append(
        new_workflow_event(
            "WorkflowRequested",
            run_id,
            payload={
                "exact_request_discriminator": "agent-workflow-exact/v1",
                "bet_id": exact_identity["bet_id"],
                "workflow_id": "test-workflow",
                "request_identity": exact_identity,
            },
        )
    )
    store.append(
        new_workflow_event(
            "WorkflowAdmitted",
            run_id,
            payload={
                "admission": grant,
                "policy": policy,
                "exact_request_discriminator": "agent-workflow-exact/v1",
                "bet_id": exact_identity["bet_id"],
                "workflow_id": exact_identity["workflow_id"],
                "policy_digest": grant["policy_digest"],
                "proof": grant["proof"],
                "request_identity": exact_identity,
            },
        )
    )
    packet = {
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "admission": grant,
        "request_identity": {
            "bet_id": "BET-BOUND",
            "workflow_id": "test-workflow",
            "packet_id": exact_identity["packet_id"],
            "packet_hash": exact_identity["packet_hash"],
            "dispatch_id": "caller-forged-dispatch",
            "instruction_binding": INSTRUCTION_BINDING,
        },
    }
    return packet, exact_identity


def _exact_worker_fixture(root: Path) -> tuple[Path, dict]:
    pi = _admitted_pi_worker()
    pi["transports"]["acp_stdio"]["worker_ack_protocol"] = "omo-worker-origin-ack/v1"
    task_path = _task_fixture(root, worker=pi)
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L0"
    task["allowed_operation_level"] = "L0"
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    return task_path, pi


def _healthy_reasoning() -> dict:
    return {
        "status": "healthy",
        "capabilities": {"reasoning": {"available": True}},
        "observed_at": "2026-08-30T00:00:00+00:00",
        "source": "test",
    }


def test_canonical_exact_dispatch_caller_mints_private_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, exact_identity = _seed_exact_workflow_packet(tmp_path)
    monkeypatch.setattr(workflow_dispatch_mod, "admit_workflow", lambda *_args, **_kwargs: packet)

    result = workflow_dispatch_mod.dispatch_admitted_workflow(
        tmp_path,
        task_id="TASK-ADMISSION-GATE",
        worker_id="pi",
        allowed_write_paths=[],
        backend="runtime",
        required_capabilities=["reasoning"],
        capability_health=_healthy_reasoning(),
        launch=False,
        workflow_run_id=packet["workflow_run_id"],
        request_identity=packet["request_identity"],
    )

    persisted_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace") for path in (tmp_path / ".omo").rglob("*") if path.is_file()
    )
    snapshot = WorkflowMeshStore(tmp_path / ".omo").snapshot(packet["workflow_run_id"])
    assert result["worker_dispatch"]["dispatch_id"] == exact_identity["dispatch_id"]
    assert snapshot["state"] == "dispatched"
    assert snapshot["worker"]["ack_origin_commitment"].startswith("sha256:")
    assert "worker_ack_origin_proof" not in persisted_text


def test_canonical_exact_dispatch_rejects_public_proof_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    packet["worker_ack_origin_proof"] = new_worker_ack_origin_proof()
    monkeypatch.setattr(workflow_dispatch_mod, "admit_workflow", lambda *_args, **_kwargs: packet)
    before = _file_snapshot(tmp_path)

    with pytest.raises(ValueError, match="private proof is forbidden in public dispatch data"):
        workflow_dispatch_mod.dispatch_admitted_workflow(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            backend="runtime",
            required_capabilities=["reasoning"],
            capability_health=_healthy_reasoning(),
            launch=False,
            workflow_run_id=packet["workflow_run_id"],
            request_identity=packet["request_identity"],
        )

    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_exact_production_dispatch_uses_persisted_request_dispatch_id(tmp_path: Path) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, exact_identity = _seed_exact_workflow_packet(tmp_path)

    dispatched = dispatch_task(
        tmp_path,
        task_id="TASK-ADMISSION-GATE",
        worker_id="pi",
        allowed_write_paths=[],
        workflow_packet=packet,
        worker_ack_origin_proof=new_worker_ack_origin_proof(),
        launch=False,
        now="2026-08-30T01:02:03+00:00",
    )

    snapshot = WorkflowMeshStore(tmp_path / ".omo").snapshot(packet["workflow_run_id"])
    assert dispatched["dispatch_id"] == exact_identity["dispatch_id"]
    assert snapshot["worker"]["dispatch_id"] == exact_identity["dispatch_id"]


def test_exact_production_success_records_authenticated_completion_and_redacts_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    chronology: list[str] = []

    class CompletedWorker:
        pid = 42401

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del stdout, stderr, text
            assert start_new_session is True
            chronology.append("spawn")
            self.args = argv
            self.cwd = Path(cwd)
            self.env = env
            self.returncode = None

        def communicate(self, timeout=None):
            del timeout
            chronology.append("communicate")
            before_ack = WorkflowMeshStore(tmp_path / ".omo").events()
            assert before_ack[-1]["event_type"] == "StepStarted"
            assert self.env["OMO_WORKER_ACK_ORIGIN_PROOF"] == origin_proof
            context = json.loads(self.env["OMO_WORKER_ACK_CONTEXT_JSON"])
            omo_dir = context.pop("omo_dir")
            acknowledge_worker(
                self.cwd / omo_dir,
                **context,
                ack_decision="proceed",
                origin_proof=self.env["OMO_WORKER_ACK_ORIGIN_PROOF"],
            )
            self.returncode = 0
            return f"completed {origin_proof}", ""

        def kill(self):  # pragma: no cover - successful process is never killed.
            raise AssertionError("successful worker must not be killed")

    class _NoopOpener:
        def open(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", CompletedWorker)
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("exact synchronous production must use Popen"),
    )
    monkeypatch.setattr("urllib.request.build_opener", lambda *_args, **_kwargs: _NoopOpener())

    dispatched = dispatch_task(
        tmp_path,
        task_id="TASK-ADMISSION-GATE",
        worker_id="pi",
        allowed_write_paths=[],
        workflow_packet=packet,
        worker_ack_origin_proof=origin_proof,
        launch=True,
        now="2026-08-30T01:02:03+00:00",
    )

    store = WorkflowMeshStore(tmp_path / ".omo")
    snapshot = store.snapshot(packet["workflow_run_id"])
    log_path = tmp_path / ".omo" / "workers" / "runs" / f"{dispatched['dispatch_id']}-stdout.log"
    assert snapshot["state"] == "succeeded"
    assert chronology == ["spawn", "communicate"]
    assert snapshot["worker_completion_receipt"]["dispatch_id"] == exact_identity["dispatch_id"]
    assert origin_proof not in json.dumps(store.events())
    assert origin_proof not in log_path.read_text(encoding="utf-8")
    assert "[REDACTED]" in log_path.read_text(encoding="utf-8")


def test_exact_production_caps_wait_and_ack_lease_to_admission_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path, ttl_seconds=30)
    origin_proof = new_worker_ack_origin_proof()
    observed: dict[str, float | int] = {}

    class AdmissionBoundedWorker:
        pid = 42431

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del stdout, stderr, text
            assert start_new_session is True
            self.args = argv
            self.cwd = Path(cwd)
            self.env = env
            self.returncode = None

        def communicate(self, timeout=None):
            assert timeout is not None
            observed["wait_timeout"] = timeout
            context = json.loads(self.env["OMO_WORKER_ACK_CONTEXT_JSON"])
            observed["lease_seconds"] = context["lease_seconds"]
            assert 0 < timeout <= 30
            assert 0 < context["lease_seconds"] <= 30
            omo_dir = context.pop("omo_dir")
            acknowledge_worker(
                self.cwd / omo_dir,
                **context,
                ack_decision="proceed",
                origin_proof=self.env["OMO_WORKER_ACK_ORIGIN_PROOF"],
            )
            self.returncode = 0
            return "bounded-success", ""

    class _NoopOpener:
        def open(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", AdmissionBoundedWorker)
    monkeypatch.setattr("urllib.request.build_opener", lambda *_args, **_kwargs: _NoopOpener())

    dispatch_task(
        tmp_path,
        task_id="TASK-ADMISSION-GATE",
        worker_id="pi",
        allowed_write_paths=[],
        workflow_packet=packet,
        worker_ack_origin_proof=origin_proof,
        launch=True,
        now="2026-08-30T01:02:03+00:00",
    )

    snapshot = WorkflowMeshStore(tmp_path / ".omo").snapshot(packet["workflow_run_id"])
    lease_expires_at = datetime.fromisoformat(snapshot["worker"]["lease_expires_at"].replace("Z", "+00:00"))
    admission_expires_at = datetime.fromisoformat(packet["admission"]["expires_at"])
    assert observed["wait_timeout"] <= 30
    assert observed["lease_seconds"] <= 30
    assert lease_expires_at <= admission_expires_at


@pytest.mark.parametrize(
    ("phase", "interrupt_type"),
    [
        ("step_started", KeyboardInterrupt),
        ("step_started", SystemExit),
        ("communicate", KeyboardInterrupt),
        ("communicate", SystemExit),
    ],
)
def test_exact_post_spawn_base_exception_reaps_group_and_reraises_original(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    interrupt_type: type[BaseException],
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    interruption = interrupt_type(f"stable-{phase}-interruption")
    spawned_pid = 42430
    lifecycle = {"group_alive": True, "signals": [], "communicate_calls": 0}

    class InterruptedWorker:
        pid = spawned_pid

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del cwd, stdout, stderr, text, env
            assert start_new_session is True
            self.args = argv
            self.returncode = None

        def communicate(self, timeout=None):
            lifecycle["communicate_calls"] += 1
            if phase == "communicate" and lifecycle["communicate_calls"] == 1:
                raise interruption
            self.returncode = -15
            return "interrupted", ""

    original_append = WorkflowMeshStore.append

    def interrupt_step_started(store, event):
        if phase == "step_started" and event["event_type"] == "StepStarted":
            raise interruption
        return original_append(store, event)

    def fake_killpg(process_group_id, sig):
        assert process_group_id == spawned_pid
        lifecycle["signals"].append(sig)
        if sig == 0:
            if lifecycle["group_alive"]:
                return None
            raise ProcessLookupError("interrupted group absent")
        if sig in {signal.SIGTERM, signal.SIGKILL}:
            lifecycle["group_alive"] = False

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", InterruptedWorker)
    monkeypatch.setattr(WorkflowMeshStore, "append", interrupt_step_started)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.getpgid", lambda pid: pid)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.killpg", fake_killpg)

    with pytest.raises(interrupt_type, match=f"stable-{phase}-interruption") as raised:
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    assert raised.value is interruption
    assert lifecycle["group_alive"] is False
    assert signal.SIGTERM in lifecycle["signals"]
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
    assert origin_proof not in json.dumps(events)
    if phase == "communicate":
        assert any(event["event_type"] == "StepFailed" for event in events)


@pytest.mark.parametrize(
    ("phase", "interrupt_type"),
    [
        ("log_write", KeyboardInterrupt),
        ("ack_snapshot", SystemExit),
        ("group_inspection", KeyboardInterrupt),
        ("completion", SystemExit),
    ],
)
def test_exact_outer_post_spawn_boundary_cleans_every_late_interruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    interrupt_type: type[BaseException],
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    interruption = interrupt_type(f"stable-late-{phase}-interruption")
    spawned_pid = 42432
    lifecycle = {
        "group_alive": True,
        "signals": [],
        "communicate_calls": 0,
        "main_communicate_done": False,
        "group_interrupt_raised": False,
    }

    class LateInterruptedWorker:
        pid = spawned_pid

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del stdout, stderr, text
            assert start_new_session is True
            self.args = argv
            self.cwd = Path(cwd)
            self.env = env
            self.returncode = None

        def communicate(self, timeout=None):
            lifecycle["communicate_calls"] += 1
            if not lifecycle["main_communicate_done"]:
                context = json.loads(self.env["OMO_WORKER_ACK_CONTEXT_JSON"])
                omo_dir = context.pop("omo_dir")
                acknowledge_worker(
                    self.cwd / omo_dir,
                    **context,
                    ack_decision="proceed",
                    origin_proof=self.env["OMO_WORKER_ACK_ORIGIN_PROOF"],
                )
                lifecycle["main_communicate_done"] = True
                self.returncode = 0
            return "late-interruption", ""

    original_write_text = __import__("omo.omo_worker_dispatch", fromlist=["write_text_atomic"]).write_text_atomic

    def interrupt_log_write(path, content):
        if phase == "log_write" and str(path).endswith("-stdout.log"):
            raise interruption
        return original_write_text(path, content)

    original_worker_snapshot = WorkflowMeshStore.worker_snapshot

    def interrupt_ack_snapshot(store, workflow_run_id):
        if phase == "ack_snapshot":
            raise interruption
        return original_worker_snapshot(store, workflow_run_id)

    def fake_killpg(process_group_id, sig):
        assert process_group_id == spawned_pid
        lifecycle["signals"].append(sig)
        if sig == 0:
            if phase == "group_inspection" and not lifecycle["group_interrupt_raised"]:
                lifecycle["group_interrupt_raised"] = True
                raise interruption
            if phase == "completion" and lifecycle["main_communicate_done"]:
                lifecycle["group_alive"] = False
            if lifecycle["group_alive"]:
                return None
            raise ProcessLookupError("late-interruption group absent")
        if not lifecycle["group_alive"]:
            raise ProcessLookupError("late-interruption group absent")
        if sig in {signal.SIGTERM, signal.SIGKILL}:
            lifecycle["group_alive"] = False

    original_completion = worker_lifecycle_mod.record_worker_completion

    def interrupt_completion(*args, **kwargs):
        if phase == "completion":
            raise interruption
        return original_completion(*args, **kwargs)

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", LateInterruptedWorker)
    monkeypatch.setattr("omo.omo_worker_dispatch.write_text_atomic", interrupt_log_write)
    monkeypatch.setattr(WorkflowMeshStore, "worker_snapshot", interrupt_ack_snapshot)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.getpgid", lambda pid: pid)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.killpg", fake_killpg)
    monkeypatch.setattr(worker_lifecycle_mod, "record_worker_completion", interrupt_completion)

    with pytest.raises(interrupt_type, match=f"stable-late-{phase}-interruption") as raised:
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    assert raised.value is interruption
    assert lifecycle["group_alive"] is False
    assert signal.SIGTERM in lifecycle["signals"]
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
    assert origin_proof not in json.dumps(events)


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_exact_group_derivation_interruption_uses_provisional_pid_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupt_type: type[BaseException],
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    interruption = interrupt_type("stable-group-derivation-interruption")
    spawned_pid = 42433
    lifecycle = {"group_alive": True, "signals": [], "communicate_calls": 0}

    class DerivationInterruptedWorker:
        pid = spawned_pid

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del cwd, stdout, stderr, text, env
            assert start_new_session is True
            self.args = argv
            self.returncode = None

        def communicate(self, timeout=None):
            lifecycle["communicate_calls"] += 1
            self.returncode = -15
            return "derivation-interrupted", ""

    def interrupt_getpgid(_pid):
        raise interruption

    def fake_killpg(process_group_id, sig):
        assert process_group_id == spawned_pid
        lifecycle["signals"].append(sig)
        if sig == 0:
            if lifecycle["group_alive"]:
                return None
            raise ProcessLookupError("derivation group absent")
        if not lifecycle["group_alive"]:
            raise ProcessLookupError("derivation group absent")
        if sig in {signal.SIGTERM, signal.SIGKILL}:
            lifecycle["group_alive"] = False

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", DerivationInterruptedWorker)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.getpgid", interrupt_getpgid)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.killpg", fake_killpg)

    with pytest.raises(interrupt_type, match="stable-group-derivation-interruption") as raised:
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    assert raised.value is interruption
    assert lifecycle["group_alive"] is False
    assert signal.SIGTERM in lifecycle["signals"]
    assert lifecycle["communicate_calls"] >= 1
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert not any(event["event_type"] == "StepStarted" for event in events)
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
    assert origin_proof not in json.dumps(events)


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_exact_interruption_immediately_after_popen_return_reaps_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupt_type: type[BaseException],
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    interruption = interrupt_type("stable-after-popen-return-interruption")
    spawned_pid = 42434
    lifecycle = {"pid_reads": 0, "group_alive": True, "signals": [], "communicate_calls": 0}

    class ImmediatelyInterruptedWorker:
        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del cwd, stdout, stderr, text, env
            assert start_new_session is True
            self.args = argv
            self.returncode = None

        @property
        def pid(self):
            lifecycle["pid_reads"] += 1
            if lifecycle["pid_reads"] == 1:
                raise interruption
            return spawned_pid

        def communicate(self, timeout=None):
            lifecycle["communicate_calls"] += 1
            self.returncode = -15
            return "popen-interrupted", ""

    def fake_killpg(process_group_id, sig):
        assert process_group_id == spawned_pid
        lifecycle["signals"].append(sig)
        if sig == 0:
            if lifecycle["group_alive"]:
                return None
            raise ProcessLookupError("post-popen group absent")
        if not lifecycle["group_alive"]:
            raise ProcessLookupError("post-popen group absent")
        if sig in {signal.SIGTERM, signal.SIGKILL}:
            lifecycle["group_alive"] = False

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", ImmediatelyInterruptedWorker)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.killpg", fake_killpg)

    with pytest.raises(interrupt_type, match="stable-after-popen-return-interruption") as raised:
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    assert raised.value is interruption
    assert lifecycle["pid_reads"] >= 2
    assert lifecycle["group_alive"] is False
    assert signal.SIGTERM in lifecycle["signals"]
    assert lifecycle["communicate_calls"] >= 1
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert not any(event["event_type"] == "StepStarted" for event in events)
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
    assert origin_proof not in json.dumps(events)


def test_exact_zero_return_parent_with_live_descendant_fails_before_completion(
    tmp_path: Path,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    pid_path = tmp_path / "zero-return-pids.txt"
    script_path = tmp_path / "ack-and-leave-descendant.py"
    script_path.write_text(
        "\n".join(
            [
                "import json",
                "import os",
                "from pathlib import Path",
                "import subprocess",
                "import sys",
                "from omo.worker_lifecycle import acknowledge_worker",
                "child_code = 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'",
                "child = subprocess.Popen(",
                "    [sys.executable, '-c', child_code],",
                "    stdin=subprocess.DEVNULL,",
                "    stdout=subprocess.DEVNULL,",
                "    stderr=subprocess.DEVNULL,",
                ")",
                "Path(sys.argv[1]).write_text(f'{os.getpid()} {child.pid}', encoding='utf-8')",
                "context = json.loads(os.environ['OMO_WORKER_ACK_CONTEXT_JSON'])",
                "omo_dir = context.pop('omo_dir')",
                "acknowledge_worker(",
                "    Path.cwd() / omo_dir,",
                "    **context,",
                "    ack_decision='proceed',",
                "    origin_proof=os.environ['OMO_WORKER_ACK_ORIGIN_PROOF'],",
                ")",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    registry_path = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["workers"][0]["transports"]["acp_stdio"]["command"] = " ".join(
        [shlex.quote(sys.executable), shlex.quote(str(script_path)), shlex.quote(str(pid_path))]
    )
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    spawned_pids: list[int] = []

    def pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    try:
        with pytest.raises(RuntimeError, match="process group remained live after successful return"):
            dispatch_task(
                tmp_path,
                task_id="TASK-ADMISSION-GATE",
                worker_id="pi",
                allowed_write_paths=[],
                workflow_packet=packet,
                worker_ack_origin_proof=origin_proof,
                launch=True,
                now="2026-08-30T01:02:03+00:00",
            )
        deadline = time.monotonic() + 5
        while not pid_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pid_path.exists()
        spawned_pids.extend(int(item) for item in pid_path.read_text(encoding="utf-8").split())
        deadline = time.monotonic() + 5
        while any(pid_alive(pid) for pid in spawned_pids) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert len(spawned_pids) == 2
        assert not any(pid_alive(pid) for pid in spawned_pids)
        events = WorkflowMeshStore(tmp_path / ".omo").events()
        persisted_text = "\n".join(
            path.read_text(encoding="utf-8", errors="replace")
            for path in (tmp_path / ".omo").rglob("*")
            if path.is_file()
        )
        assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
        assert origin_proof not in persisted_text
    finally:
        if pid_path.exists() and not spawned_pids:
            spawned_pids.extend(int(item) for item in pid_path.read_text(encoding="utf-8").split())
        if spawned_pids:
            try:
                os.killpg(spawned_pids[0], signal.SIGKILL)
            except ProcessLookupError:
                pass
        for pid in spawned_pids:
            if pid_alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_exact_production_missing_origin_proof_rejects_before_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    before_events = WorkflowMeshStore(tmp_path / ".omo").events()
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.subprocess.Popen",
        lambda *_args, **_kwargs: pytest.fail("missing proof must reject before subprocess"),
    )

    with pytest.raises(ValueError, match="origin proof is unavailable"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    assert WorkflowMeshStore(tmp_path / ".omo").events() == before_events
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


@pytest.mark.parametrize(
    "public_shape",
    ["top_level_key", "request_key", "capability_value", "substring_value"],
)
def test_exact_production_rejects_public_private_proof_data_before_effects(
    tmp_path: Path,
    public_shape: str,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    private_proof = new_worker_ack_origin_proof()
    if public_shape == "top_level_key":
        packet["worker_ack_origin_proof"] = private_proof
    elif public_shape == "request_key":
        packet["request_identity"]["ack origin proof"] = new_worker_ack_origin_proof()
    elif public_shape == "capability_value":
        packet["request_identity"]["capabilities"] = ["origin_proof"]
    else:
        packet["request_identity"]["note"] = f"prefix-{private_proof}-suffix"
    before = _file_snapshot(tmp_path)

    with pytest.raises(ValueError, match="private proof is forbidden in public dispatch data"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=private_proof,
            launch=False,
            now="2026-08-30T01:02:03+00:00",
        )

    persisted_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace") for path in (tmp_path / ".omo").rglob("*") if path.is_file()
    )
    assert _file_snapshot(tmp_path) == before
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()
    assert private_proof not in persisted_text


def test_exact_production_allows_public_ack_origin_digest(tmp_path: Path) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, exact_identity = _seed_exact_workflow_packet(tmp_path)
    packet["request_identity"]["ack_origin_proof_digest"] = "sha256:" + "d" * 64

    dispatched = dispatch_task(
        tmp_path,
        task_id="TASK-ADMISSION-GATE",
        worker_id="pi",
        allowed_write_paths=[],
        workflow_packet=packet,
        worker_ack_origin_proof=new_worker_ack_origin_proof(),
        launch=False,
        now="2026-08-30T01:02:03+00:00",
    )

    assert dispatched["dispatch_id"] == exact_identity["dispatch_id"]


@pytest.mark.parametrize(("returncode", "durable_ack"), [(1, True), (0, False)])
def test_exact_production_failure_or_missing_ack_never_completes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    durable_ack: bool,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()

    class WorkerResult:
        pid = 42402

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del stdout, stderr, text
            assert start_new_session is True
            self.args = argv
            self.cwd = Path(cwd)
            self.env = env
            self.returncode = None
            self.communications = 0

        def communicate(self, timeout=None):
            del timeout
            self.communications += 1
            if self.communications > 1:
                return "worker-result", ""
            assert WorkflowMeshStore(tmp_path / ".omo").events()[-1]["event_type"] == "StepStarted"
            if durable_ack:
                context = json.loads(self.env["OMO_WORKER_ACK_CONTEXT_JSON"])
                omo_dir = context.pop("omo_dir")
                acknowledge_worker(
                    self.cwd / omo_dir,
                    **context,
                    ack_decision="proceed",
                    origin_proof=self.env["OMO_WORKER_ACK_ORIGIN_PROOF"],
                )
            self.returncode = returncode
            return "worker-result", ""

        def terminate(self):
            return None

        def kill(self):  # pragma: no cover - graceful cleanup succeeds.
            raise AssertionError("non-timeout worker should terminate gracefully")

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", WorkerResult)
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("exact synchronous production must use Popen"),
    )

    with pytest.raises(RuntimeError):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    if returncode != 0:
        assert any(event["event_type"] == "StepFailed" for event in events)
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)


def test_exact_production_spawn_failure_has_no_step_started(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.subprocess.Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("spawn failed")),
    )
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("exact synchronous production must use Popen"),
    )

    with pytest.raises(OSError, match="spawn failed"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=new_worker_ack_origin_proof(),
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert not any(event["event_type"] == "StepStarted" for event in events)
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)


def test_exact_production_timeout_records_honest_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)

    class TimedOutWorker:
        pid = 42403

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del cwd, stdout, stderr, text, env
            assert start_new_session is True
            self.args = argv
            self.returncode = None
            self.communications = 0
            self.killed = False

        def communicate(self, timeout=None):
            self.communications += 1
            if self.communications == 1:
                raise subprocess.TimeoutExpired(self.args, timeout)
            self.returncode = -9
            return "partial", "timeout"

        def terminate(self):
            return None

        def kill(self):
            self.killed = True

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", TimedOutWorker)
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("exact synchronous production must use Popen"),
    )

    with pytest.raises(RuntimeError, match="timed out"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=new_worker_ack_origin_proof(),
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert any(event["event_type"] == "StepStarted" for event in events)
    assert any(event["event_type"] == "StepFailed" for event in events)
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)


@pytest.mark.parametrize("cleanup_failure", ["terminate_error", "terminate_timeout"])
def test_exact_production_step_started_append_failure_reaps_spawned_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cleanup_failure: str,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    spawned_pid = 42410
    lifecycle = {
        "terminated": False,
        "killed": False,
        "drained": False,
        "communicate_calls": 0,
        "signals": [],
    }
    group = {"alive": True}

    class SpawnedWorker:
        pid = spawned_pid

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del cwd, stdout, stderr, text
            assert start_new_session is True
            self.args = argv
            self.env = env
            self.returncode = None

        def terminate(self):
            lifecycle["terminated"] = True
            if cleanup_failure == "terminate_error":
                raise OSError("graceful terminate failed")

        def communicate(self, timeout=None):
            lifecycle["communicate_calls"] += 1
            if cleanup_failure == "terminate_timeout" and lifecycle["communicate_calls"] == 1:
                raise subprocess.TimeoutExpired(self.args, timeout)
            lifecycle["drained"] = True
            self.returncode = -9
            return "partial", "terminated"

        def kill(self):
            lifecycle["killed"] = True

    def fake_killpg(process_group_id, sig):
        assert process_group_id == spawned_pid
        lifecycle["signals"].append(sig)
        if sig == 0:
            if group["alive"]:
                return None
            raise ProcessLookupError("expected process group is absent")
        if sig == signal.SIGTERM and cleanup_failure == "terminate_error":
            raise OSError("graceful group terminate failed")
        if sig == signal.SIGKILL:
            group["alive"] = False

    original_append = WorkflowMeshStore.append

    def reject_step_started(store, event):
        if event["event_type"] == "StepStarted":
            raise WorkflowMeshEventError("stable-step-start-failure")
        return original_append(store, event)

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", SpawnedWorker)
    monkeypatch.setattr(WorkflowMeshStore, "append", reject_step_started)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.getpgid", lambda pid: pid)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.killpg", fake_killpg)

    with pytest.raises(WorkflowMeshEventError, match="stable-step-start-failure"):
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    persisted_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace") for path in (tmp_path / ".omo").rglob("*") if path.is_file()
    )
    assert lifecycle == {
        "terminated": False,
        "killed": False,
        "drained": True,
        "communicate_calls": 1 if cleanup_failure == "terminate_error" else 2,
        "signals": [signal.SIGTERM, 0, signal.SIGKILL, 0, 0],
    }
    assert not any(event["event_type"] == "StepStarted" for event in events)
    assert not any(event["event_type"] == "StepFailed" for event in events)
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
    assert origin_proof not in json.dumps(events)
    assert origin_proof not in persisted_text


@pytest.mark.parametrize("group_failure", ["persistent_live", "signal_failure"])
def test_exact_production_cleanup_fails_closed_when_validated_group_survives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    group_failure: str,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    spawned_pid = 42420
    lifecycle = {
        "terminated": False,
        "killed": False,
        "drained": 0,
        "signals": [],
        "communicate_timeouts": [],
    }

    class PersistentGroupWorker:
        pid = spawned_pid

        def __init__(self, argv, *, cwd, stdout, stderr, text, env, start_new_session):
            del argv, cwd, stdout, stderr, text, env
            assert start_new_session is True
            self.returncode = None

        def terminate(self):
            lifecycle["terminated"] = True

        def kill(self):
            lifecycle["killed"] = True

        def communicate(self, timeout=None):
            lifecycle["communicate_timeouts"].append(timeout)
            lifecycle["drained"] += 1
            self.returncode = -9
            return "partial", "cleanup"

    original_append = WorkflowMeshStore.append

    def reject_step_started(store, event):
        if event["event_type"] == "StepStarted":
            raise WorkflowMeshEventError("stable-persistent-group-step-start-failure")
        return original_append(store, event)

    def fake_killpg(process_group_id, sig):
        assert process_group_id == spawned_pid
        lifecycle["signals"].append(sig)
        if sig == 0:
            return None
        if group_failure == "signal_failure":
            raise PermissionError("group signal denied")
        return None

    clock = {"value": 0.0}

    def fast_monotonic():
        clock["value"] += 0.5
        return clock["value"]

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.Popen", PersistentGroupWorker)
    monkeypatch.setattr(WorkflowMeshStore, "append", reject_step_started)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.getpgid", lambda pid: pid)
    monkeypatch.setattr("omo.omo_worker_dispatch.os.killpg", fake_killpg)
    monkeypatch.setattr("omo.omo_worker_dispatch.time.monotonic", fast_monotonic)

    with pytest.raises(RuntimeError, match="exact worker process-group cleanup failed") as raised:
        dispatch_task(
            tmp_path,
            task_id="TASK-ADMISSION-GATE",
            worker_id="pi",
            allowed_write_paths=[],
            workflow_packet=packet,
            worker_ack_origin_proof=origin_proof,
            launch=True,
            now="2026-08-30T01:02:03+00:00",
        )

    assert isinstance(raised.value.__cause__, WorkflowMeshEventError)
    assert str(raised.value.__cause__) == "stable-persistent-group-step-start-failure"
    assert signal.SIGTERM in lifecycle["signals"]
    assert signal.SIGKILL in lifecycle["signals"]
    assert 0 in lifecycle["signals"]
    assert lifecycle["terminated"] is False
    assert lifecycle["killed"] is False
    assert all(timeout is not None and 0 <= timeout <= 5 for timeout in lifecycle["communicate_timeouts"])
    if group_failure == "persistent_live":
        assert lifecycle["drained"] >= 1
        assert lifecycle["communicate_timeouts"]
    else:
        assert lifecycle["drained"] == 0
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    persisted_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace") for path in (tmp_path / ".omo").rglob("*") if path.is_file()
    )
    assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
    assert origin_proof not in json.dumps(events)
    assert origin_proof not in persisted_text


def test_exact_production_step_started_failure_reaps_real_descendant_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    pid_path = tmp_path / "spawned-pids.txt"
    ready_path = tmp_path / "descendant-ready.txt"
    script_path = tmp_path / "spawn-descendant.py"
    script_path.write_text(
        "\n".join(
            [
                "import os",
                "from pathlib import Path",
                "import subprocess",
                "import sys",
                "import time",
                "child_code = (",
                '    "import signal, sys, time; from pathlib import Path; "',
                '    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "',
                "    \"Path(sys.argv[1]).write_text('ready', encoding='utf-8'); time.sleep(60)\"",
                ")",
                "child = subprocess.Popen(",
                "    [sys.executable, '-c', child_code, sys.argv[2]],",
                "    stdin=subprocess.DEVNULL,",
                "    stdout=subprocess.DEVNULL,",
                "    stderr=subprocess.DEVNULL,",
                ")",
                "while not Path(sys.argv[2]).exists(): time.sleep(0.01)",
                "Path(sys.argv[1]).write_text(f'{os.getpid()} {child.pid}', encoding='utf-8')",
                "time.sleep(60)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    registry_path = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["workers"][0]["transports"]["acp_stdio"]["command"] = " ".join(
        [
            shlex.quote(sys.executable),
            shlex.quote(str(script_path)),
            shlex.quote(str(pid_path)),
            shlex.quote(str(ready_path)),
        ]
    )
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    spawned_pids: list[int] = []
    original_append = WorkflowMeshStore.append

    def reject_after_descendant_exists(store, event):
        if event["event_type"] != "StepStarted":
            return original_append(store, event)
        deadline = time.monotonic() + 5
        while not pid_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pid_path.exists(), "spawned parent never published descendant PID"
        spawned_pids.extend(int(item) for item in pid_path.read_text(encoding="utf-8").split())
        raise WorkflowMeshEventError("stable-real-step-start-failure")

    def pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    monkeypatch.setattr(WorkflowMeshStore, "append", reject_after_descendant_exists)
    try:
        with pytest.raises(WorkflowMeshEventError, match="stable-real-step-start-failure"):
            dispatch_task(
                tmp_path,
                task_id="TASK-ADMISSION-GATE",
                worker_id="pi",
                allowed_write_paths=[],
                workflow_packet=packet,
                worker_ack_origin_proof=origin_proof,
                launch=True,
                now="2026-08-30T01:02:03+00:00",
            )
        deadline = time.monotonic() + 5
        while any(pid_alive(pid) for pid in spawned_pids) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert len(spawned_pids) == 2
        assert not any(pid_alive(pid) for pid in spawned_pids)
        events = WorkflowMeshStore(tmp_path / ".omo").events()
        assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
        assert origin_proof not in json.dumps(events)
    finally:
        for pid in spawned_pids:
            if pid_alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_exact_production_parent_exit_getpgid_race_reaps_expected_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, _pi = _exact_worker_fixture(tmp_path)
    packet, _exact_identity = _seed_exact_workflow_packet(tmp_path)
    origin_proof = new_worker_ack_origin_proof()
    pid_path = tmp_path / "race-pids.txt"
    ready_path = tmp_path / "race-descendant-ready.txt"
    script_path = tmp_path / "spawn-race-descendant.py"
    script_path.write_text(
        "\n".join(
            [
                "import os",
                "from pathlib import Path",
                "import subprocess",
                "import sys",
                "import time",
                "child_code = (",
                '    "import signal, sys, time; from pathlib import Path; "',
                '    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "',
                "    \"Path(sys.argv[1]).write_text('ready', encoding='utf-8'); time.sleep(60)\"",
                ")",
                "child = subprocess.Popen(",
                "    [sys.executable, '-c', child_code, sys.argv[2]],",
                "    stdin=subprocess.DEVNULL,",
                "    stdout=subprocess.DEVNULL,",
                "    stderr=subprocess.DEVNULL,",
                ")",
                "while not Path(sys.argv[2]).exists(): time.sleep(0.01)",
                "Path(sys.argv[1]).write_text(f'{os.getpid()} {child.pid}', encoding='utf-8')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    registry_path = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["workers"][0]["transports"]["acp_stdio"]["command"] = " ".join(
        [
            shlex.quote(sys.executable),
            shlex.quote(str(script_path)),
            shlex.quote(str(pid_path)),
            shlex.quote(str(ready_path)),
        ]
    )
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    spawned_pids: list[int] = []
    original_append = WorkflowMeshStore.append

    def reject_after_parent_exits(store, event):
        if event["event_type"] != "StepStarted":
            return original_append(store, event)
        deadline = time.monotonic() + 5
        while not pid_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pid_path.exists(), "race parent never published descendant PID"
        spawned_pids.extend(int(item) for item in pid_path.read_text(encoding="utf-8").split())
        raise WorkflowMeshEventError("stable-getpgid-race-step-start-failure")

    def pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    monkeypatch.setattr(WorkflowMeshStore, "append", reject_after_parent_exits)
    monkeypatch.setattr(
        "omo.omo_worker_dispatch.os.getpgid",
        lambda _pid: (_ for _ in ()).throw(ProcessLookupError("parent exited before getpgid")),
    )
    try:
        with pytest.raises(WorkflowMeshEventError, match="stable-getpgid-race-step-start-failure"):
            dispatch_task(
                tmp_path,
                task_id="TASK-ADMISSION-GATE",
                worker_id="pi",
                allowed_write_paths=[],
                workflow_packet=packet,
                worker_ack_origin_proof=origin_proof,
                launch=True,
                now="2026-08-30T01:02:03+00:00",
            )
        deadline = time.monotonic() + 5
        while any(pid_alive(pid) for pid in spawned_pids) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert len(spawned_pids) == 2
        assert not any(pid_alive(pid) for pid in spawned_pids)
        events = WorkflowMeshStore(tmp_path / ".omo").events()
        assert not any(event["event_type"] == "WorkflowSucceeded" for event in events)
        assert origin_proof not in json.dumps(events)
    finally:
        for pid in spawned_pids:
            if pid_alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_step_dispatch_rejects_forged_admission_without_writing(tmp_path: Path) -> None:
    store = WorkflowMeshStore(tmp_path / ".omo")
    del store
    before = _file_snapshot(tmp_path)
    with pytest.raises(WorkerLifecycleError, match="admission binding mismatch"):
        record_step_dispatch(
            tmp_path,
            workflow_run_id="run-forged",
            trace_id="trace-forged",
            dispatch_id="dispatch-forged",
            worker_id="worker-1",
            step_run_id="step-1",
            admission_id="admission-forged",
            policy_digest="sha256:" + "a" * 64,
            packet_id="WP-FORGED",
            packet_hash="sha256:" + "b" * 64,
            instruction_binding=INSTRUCTION_BINDING,
        )
    assert _file_snapshot(tmp_path) == before
