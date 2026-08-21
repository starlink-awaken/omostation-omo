from __future__ import annotations

# ruff: noqa: I001

from datetime import UTC, datetime
from pathlib import Path
import json
import shlex
import subprocess

import pytest
import yaml

from omo.workflow_dispatch import (
    WorkflowDispatchError,
    admit_requested_workflow,
    admit_workflow,
    consume_pending_workflow_requests,
    dispatch_admitted_workflow,
)
from omo.workflow_mesh import WorkflowMeshStore, new_workflow_event


def _task(tmp_path: Path, *, approval_ref: str | None = None) -> None:
    task_dir = tmp_path / ".omo" / "tasks" / "active"
    task_dir.mkdir(parents=True)
    registry_dir = tmp_path / ".omo" / "_truth" / "registry"
    registry_dir.mkdir(parents=True)
    (registry_dir / "workers.yaml").write_text(
        yaml.safe_dump(
            {
                "workers": [
                    {
                        "id": "worker-a",
                        "enabled": True,
                        "admission_state": "admitted",
                        "transports": {
                            "cli_prompt": {
                                "command": "worker-a",
                                "ack_command": "python -m omo.cli worker mesh-ack",
                            }
                        },
                        "capabilities": ["workflow.execute", "runtime"],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    task = {
        "id": "TASK-MESH-1",
        "title": "Mesh dispatch",
        "status": "pending",
        "assigned_to": None,
        "dispatch_id": None,
        "run_ref": None,
        "approval_ref": approval_ref,
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
    (task_dir / "TASK-MESH-1.yaml").write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")


def _health() -> dict:
    return {
        "status": "healthy",
        "source": "agora",
        "observed_at": datetime.now(UTC).isoformat(),
        "capabilities": {
            "workflow.execute": {"available": True, "health": "green"},
            "runtime": {"available": True, "health": "green"},
        },
    }


def _request_identity() -> dict:
    return {
        "bet_id": "BET-1",
        "packet_id": "WP-BP-0123456789abcdef",
        "packet_hash": "sha256:" + "a" * 64,
        "task_ref": ".omo/tasks/active/TASK-MESH-1.yaml",
        "instruction_binding": {
            "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
            "instruction_version": "blueprint-agent-instruction-pack/v1",
            "content_digest": "sha256:" + "b" * 64,
            "instruction_profile": "executor",
        },
    }


def test_admit_workflow_records_request_and_grant(tmp_path: Path) -> None:
    _task(tmp_path)
    packet = admit_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        backend="runtime",
        required_capabilities=["workflow.execute", "runtime"],
        capability_health=_health(),
        workflow_run_id="run-mesh-1",
        now="2026-08-01T10:00:00+00:00",
    )

    grant = packet["admission"]
    assert grant["workflow_run_id"] == "run-mesh-1"
    assert grant["proof"]
    snapshot = WorkflowMeshStore(tmp_path / ".omo").snapshot("run-mesh-1")
    assert snapshot["state"] == "admitted"
    assert snapshot["admission"]["admission_id"] == grant["admission_id"]


def test_admit_workflow_merges_validated_blueprint_identity_into_request(
    tmp_path: Path,
) -> None:
    _task(tmp_path)
    identity = _request_identity()

    admit_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        backend="runtime",
        required_capabilities=["runtime"],
        capability_health=_health(),
        workflow_run_id="run-identity",
        request_identity=identity,
    )

    requested = WorkflowMeshStore(tmp_path / ".omo").events()[0]
    assert requested["event_type"] == "WorkflowRequested"
    assert {key: requested["payload"][key] for key in identity} == identity


def test_admit_workflow_rejects_invalid_blueprint_identity_before_mesh_write(
    tmp_path: Path,
) -> None:
    _task(tmp_path)
    with pytest.raises(WorkflowDispatchError, match="request identity"):
        admit_workflow(
            tmp_path,
            task_id="TASK-MESH-1",
            backend="runtime",
            required_capabilities=["runtime"],
            capability_health=_health(),
            request_identity={
                "bet_id": "BET-1",
                "packet_id": "WP-1",
                "packet_hash": "not-a-hash",
                "task_ref": ".omo/tasks/active/TASK-MESH-1.yaml",
                "instruction_binding": {
                    "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
                    "instruction_version": "blueprint-agent-instruction-pack/v1",
                    "content_digest": "sha256:" + "b" * 64,
                    "instruction_profile": "executor",
                },
            },
        )
    assert WorkflowMeshStore(tmp_path / ".omo").events() == []


def test_admit_requested_workflow_uses_explicit_now_for_approval_expiry(
    tmp_path: Path,
) -> None:
    approval_ref = ".omo/workers/runs/request-approval.yaml"
    _task(tmp_path, approval_ref=approval_ref)
    task_path = tmp_path / ".omo" / "tasks" / "active" / "TASK-MESH-1.yaml"
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["human_approval_required"] = True
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    approval_path = tmp_path / approval_ref
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_text(
        yaml.safe_dump(
            {
                "approval_id": "approval-request-1",
                "task_id": "TASK-MESH-1",
                "approval_status": "granted",
                "approval_scope": "workflow.execute",
                "expires_at": "2099-01-01T00:00:00+00:00",
                "refs": {
                    "task_ref": ".omo/tasks/active/TASK-MESH-1.yaml",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    store = WorkflowMeshStore(tmp_path / ".omo")
    store.append(
        new_workflow_event(
            "WorkflowRequested",
            "run-expired-request",
            producer="test",
            idempotency_key="run-expired-request:requested",
            payload={
                "task_id": "TASK-MESH-1",
                "task_ref": ".omo/tasks/active/TASK-MESH-1.yaml",
            },
        )
    )

    with pytest.raises(WorkflowDispatchError, match="expired"):
        admit_requested_workflow(
            tmp_path,
            workflow_run_id="run-expired-request",
            backend="runtime",
            required_capabilities=["runtime"],
            capability_health=_health(),
            now="2100-01-01T00:00:00+00:00",
        )

    assert [event["event_type"] for event in store.events()] == ["WorkflowRequested"]


def test_admit_workflow_accepts_same_task_promotion_approval_ref(
    tmp_path: Path,
) -> None:
    approval_ref = ".omo/workers/runs/promotion-approval.yaml"
    _task(tmp_path, approval_ref=approval_ref)
    task_path = tmp_path / ".omo" / "tasks" / "active" / "TASK-MESH-1.yaml"
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["human_approval_required"] = True
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    approval_path = tmp_path / approval_ref
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_text(
        yaml.safe_dump(
            {
                "approval_id": "promotion-approval-1",
                "task_id": "TASK-MESH-1",
                "approval_status": "granted",
                "approval_scope": "task.promote_apply",
                "refs": {"task_ref": ".omo/tasks/planned/TASK-MESH-1.yaml"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    admitted = admit_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        backend="runtime",
        required_capabilities=["runtime"],
        capability_health=_health(),
    )

    assert admitted["approval"]["status"] == "granted"
    assert admitted["approval"]["scope"] == "task.promote_apply"


def test_admit_workflow_fails_closed_for_unhealthy_capability(tmp_path: Path) -> None:
    _task(tmp_path)
    health = _health()
    health["capabilities"]["runtime"]["available"] = False
    with pytest.raises(WorkflowDispatchError, match="unavailable"):
        admit_workflow(
            tmp_path,
            task_id="TASK-MESH-1",
            backend="runtime",
            required_capabilities=["runtime"],
            capability_health=health,
        )


def test_dispatch_bridge_records_step_dispatch_worker_context(tmp_path: Path) -> None:
    _task(tmp_path)
    packet = dispatch_admitted_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        worker_id="worker-a",
        allowed_write_paths=["docs/"],
        backend="runtime",
        required_capabilities=["workflow.execute", "runtime"],
        capability_health=_health(),
        workflow_run_id="run-dispatch-bridge",
        now="2026-08-01T10:00:00+00:00",
        request_identity=_request_identity(),
    )

    snapshot = WorkflowMeshStore(tmp_path / ".omo").snapshot("run-dispatch-bridge")
    assert snapshot["state"] == "dispatched"
    assert snapshot["worker"]["dispatch_id"] == packet["worker_dispatch"]["dispatch_id"]
    assert snapshot["worker"]["worker_id"] == "worker-a"


def test_missing_ack_command_is_zero_side_effect_and_retryable(tmp_path: Path) -> None:
    _task(tmp_path)
    registry_path = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    ack_command = registry["workers"][0]["transports"]["cli_prompt"].pop("ack_command")
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    before = {
        str(path.relative_to(tmp_path)): path.read_bytes() for path in (tmp_path / ".omo").rglob("*") if path.is_file()
    }
    options = {
        "task_id": "TASK-MESH-1",
        "worker_id": "worker-a",
        "allowed_write_paths": ["docs/"],
        "backend": "runtime",
        "required_capabilities": ["workflow.execute", "runtime"],
        "capability_health": _health(),
        "workflow_run_id": "run-ack-command-retry",
        "now": "2026-08-02T10:00:00+00:00",
        "request_identity": _request_identity(),
    }

    with pytest.raises(ValueError, match="ack_command_missing"):
        dispatch_admitted_workflow(tmp_path, **options)

    after = {
        str(path.relative_to(tmp_path)): path.read_bytes() for path in (tmp_path / ".omo").rglob("*") if path.is_file()
    }
    assert after == before

    registry["workers"][0]["transports"]["cli_prompt"]["ack_command"] = ack_command
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    result = dispatch_admitted_workflow(tmp_path, **options)

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert result["workflow_run_id"] == "run-ack-command-retry"
    assert [event["event_type"] for event in events] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
        "WorkerAcknowledged",
    ]


def test_bound_dispatch_persists_and_launches_exact_delivery_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _task(tmp_path)
    registry_path = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["workers"][0]["transports"]["cli_prompt"]["command"] = (
        'worker-a "{prompt}" --run-id "{run_id}" --packet-id "{packet_id}" '
        '--packet-hash "{packet_hash}" --instruction-binding-json "{instruction_binding_json}"'
    )
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    launched: list[list[str]] = []
    ack_secrets: list[str] = []
    real_run = subprocess.run

    def run(argv, **kwargs):
        if "mesh-ack" in argv:
            ack_secrets.append(kwargs["env"]["OMO_WORKER_ACK_ORIGIN_PROOF"])
            return real_run(argv, **kwargs)
        launched.append(argv)
        return type("Result", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    monkeypatch.setattr("omo.omo_worker_dispatch.subprocess.run", run)
    result = dispatch_admitted_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        worker_id="worker-a",
        allowed_write_paths=["docs/"],
        backend="runtime",
        required_capabilities=["workflow.execute", "runtime"],
        capability_health=_health(),
        workflow_run_id="run-bound-launch",
        launch=True,
        request_identity=_request_identity(),
    )

    dispatch = yaml.safe_load((tmp_path / result["worker_dispatch"]["dispatch_path"]).read_text(encoding="utf-8"))
    persisted = shlex.split(dispatch["execution"]["launch_command"])
    assert len(launched) == 1
    assert len(ack_secrets) == 1
    assert launched[0][0] == persisted[0]
    assert launched[0][2:] == persisted[2:]
    assert launched[0][1].startswith("# Worker Prompt Contract")
    assert persisted[persisted.index("--run-id") + 1] == "run-bound-launch"
    assert persisted[persisted.index("--packet-id") + 1] == _request_identity()["packet_id"]
    assert (
        json.loads(persisted[persisted.index("--instruction-binding-json") + 1])
        == _request_identity()["instruction_binding"]
    )
    assert ack_secrets[0] not in json.dumps(result)
    assert all(
        ack_secrets[0] not in path.read_text(encoding="utf-8")
        for path in (tmp_path / ".omo").rglob("*")
        if path.is_file()
    )


def test_admit_workflow_requires_granted_approval(tmp_path: Path) -> None:
    approval_ref = ".omo/workers/runs/approval.yaml"
    _task(tmp_path, approval_ref=approval_ref)
    task_path = tmp_path / ".omo" / "tasks" / "active" / "TASK-MESH-1.yaml"
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["risk_level"] = "L2"
    task["allowed_operation_level"] = "L2"
    task["human_approval_required"] = True
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    with pytest.raises(WorkflowDispatchError, match="approval"):
        admit_workflow(
            tmp_path,
            task_id="TASK-MESH-1",
            backend="runtime",
            required_capabilities=["runtime"],
            capability_health=_health(),
        )


def test_admit_workflow_rejects_budget_overrun(tmp_path: Path) -> None:
    _task(tmp_path)
    with pytest.raises(WorkflowDispatchError, match="budget"):
        admit_workflow(
            tmp_path,
            task_id="TASK-MESH-1",
            backend="runtime",
            required_capabilities=["runtime"],
            capability_health=_health(),
            requested_budget=2,
            remaining_budget=1,
        )


def test_legacy_dispatch_without_packet_is_observer_only(tmp_path: Path) -> None:
    _task(tmp_path)
    from omo.omo_worker_dispatch import dispatch_task
    from omo.workflow_mesh import WorkflowMeshStore

    with pytest.raises(ValueError, match="observer-only"):
        dispatch_task(
            tmp_path,
            task_id="TASK-MESH-1",
            worker_id="worker-a",
            allowed_write_paths=["docs/"],
            launch=False,
            transport="cli_prompt",
            now="2026-08-02T10:00:00+00:00",
        )

    store = WorkflowMeshStore(tmp_path / ".omo")
    assert store.events() == []
    assert not (tmp_path / ".omo" / "workers" / "runs").exists()


def test_dispatch_with_packet_emits_step_dispatched(tmp_path: Path) -> None:
    """Phase 2: dispatch_task with workflow_packet should emit StepDispatched only."""
    _task(tmp_path)
    from omo.omo_worker_dispatch import dispatch_task
    from omo.workflow_mesh import WorkflowMeshStore

    packet = admit_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        backend="runtime",
        required_capabilities=["workflow.execute", "runtime"],
        capability_health=_health(),
        workflow_run_id="run-packet-test",
        now="2026-08-02T10:00:00+00:00",
        request_identity=_request_identity(),
    )

    dispatch_task(
        tmp_path,
        task_id="TASK-MESH-1",
        worker_id="worker-a",
        allowed_write_paths=["docs/"],
        launch=False,
        transport="cli_prompt",
        workflow_packet=packet,
        now="2026-08-02T10:00:00+00:00",
    )

    store = WorkflowMeshStore(tmp_path / ".omo")
    events = store.events()
    step_dispatched = [e for e in events if e["event_type"] == "StepDispatched"]

    assert len(step_dispatched) == 1, "Should emit exactly one StepDispatched"
    assert step_dispatched[0]["workflow_run_id"] == "run-packet-test"
    assert step_dispatched[0]["payload"]["worker_id"] == "worker-a"

    snapshot = store.snapshot("run-packet-test")
    assert snapshot["state"] == "dispatched"


def test_dispatch_admitted_workflow_no_double_step_dispatched(tmp_path: Path) -> None:
    """Phase 2: dispatch_admitted_workflow should not double-emit StepDispatched."""
    _task(tmp_path)
    from omo.workflow_mesh import WorkflowMeshStore

    dispatch_admitted_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        worker_id="worker-a",
        allowed_write_paths=["docs/"],
        backend="runtime",
        required_capabilities=["workflow.execute", "runtime"],
        capability_health=_health(),
        workflow_run_id="run-no-double",
        now="2026-08-02T10:00:00+00:00",
        request_identity=_request_identity(),
    )

    store = WorkflowMeshStore(tmp_path / ".omo")
    events = store.events()
    step_dispatched = [e for e in events if e["event_type"] == "StepDispatched"]

    assert len(step_dispatched) == 1, "Should not double-emit StepDispatched"


def test_consume_pending_workflow_requests_iris_fast_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """P0 完整第三块: consume 闭环 - planned run → admit → iris 快速路径."""
    import omo.workflow_dispatch as wd
    from omo.workflow_mesh import WorkflowMeshStore, new_workflow_event

    _task(tmp_path)  # active task TASK-MESH-1

    # 手动发 WorkflowRequested event (planned state, 声明 iris capability)
    store = WorkflowMeshStore(tmp_path / ".omo")
    run_id = "run-consume-iris"
    store.append(
        new_workflow_event(
            "WorkflowRequested",
            run_id,
            trace_id=run_id,
            producer="test",
            idempotency_key=f"{run_id}:requested",
            payload={
                "task_id": "TASK-MESH-1",
                "task_ref": ".omo/tasks/active/TASK-MESH-1.yaml",
                "required_capabilities": ["iris:apple_mail"],
            },
        )
    )

    # mock iris 快速路径 (避免 subprocess)
    dispatched: list[dict] = []

    def fake_iris_dispatch(root, packet, iris_caps, omo_dir=".omo"):
        dispatched.append({"run_id": packet["workflow_run_id"], "caps": list(iris_caps)})
        return {**packet, "iris_dispatch": [], "dispatch_state": "dispatched"}

    monkeypatch.setattr(wd, "_dispatch_iris_via_executor", fake_iris_dispatch)

    health = {
        "status": "healthy",
        "source": "iris-entry-points",
        "observed_at": datetime.now(UTC).isoformat(),
        "capabilities": {"iris:apple_mail": {"available": True, "health": "green"}},
    }

    result = consume_pending_workflow_requests(tmp_path, capability_health=health, omo_dir=".omo")

    assert result["total_planned"] == 1
    assert len(result["consumed"]) == 1
    assert result["consumed"][0]["workflow_run_id"] == run_id
    assert result["consumed"][0]["iris"] is True
    assert len(dispatched) == 1
    assert dispatched[0]["caps"] == ["iris:apple_mail"]
    assert result["failed"] == []

    # verify mesh 状态机推进 (admitted 之后)
    snap = store.snapshot(run_id)
    assert snap["state"] in {"admitted", "dispatched", "running"}


def test_consume_pending_workflow_requests_skips_non_planned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """consume 跳过 non-planned run (不重复消费 admitted/succeeded)."""
    import omo.workflow_dispatch as wd

    _task(tmp_path)

    # 先 admit 一个 run (state → admitted, 非 planned)
    admit_workflow(
        tmp_path,
        task_id="TASK-MESH-1",
        backend="runtime",
        required_capabilities=["workflow.execute", "runtime"],
        capability_health=_health(),
        workflow_run_id="run-already-admitted",
        now="2026-08-02T10:00:00+00:00",
    )

    dispatched: list[int] = []
    monkeypatch.setattr(
        wd,
        "_dispatch_iris_via_executor",
        lambda *a, **k: dispatched.append(1) or {},
    )

    result = consume_pending_workflow_requests(tmp_path, capability_health=_health(), omo_dir=".omo")

    # 没 planned run → 0 consumed, 0 skipped, 0 failed
    assert result["total_planned"] == 0
    assert result["consumed"] == []
    assert result["skipped"] == []
    assert result["failed"] == []
    assert dispatched == []
