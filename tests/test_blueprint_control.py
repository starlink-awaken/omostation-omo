from __future__ import annotations

import hashlib
import json
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from ecos.ssot.tools.work_packet_compiler import canonicalize, compute_packet_hash

from omo.blueprint_control import BlueprintControlError, BlueprintControlService
from omo.cli import main as cli_main
from omo.orchestration_contract import OrchestrationContractCoordinator
from omo.workflow_dispatch import WorkflowDispatchError
from omo.workflow_mesh import WorkflowMeshStore

BET_ID = "BET-Y1Q2-T1-18"
TASK_ID = "TASK-BLUEPRINT-1"
SPEC_PATH = "docs/specs/blueprint.md"
SPEC_REF = f"repo://{SPEC_PATH}"
SPEC_VERSION = "1.0.0"


def _workspace(tmp_path: Path) -> tuple[dict, dict]:
    spec = tmp_path / SPEC_PATH
    spec.parent.mkdir(parents=True)
    spec.write_text("# Accepted blueprint\n", encoding="utf-8")
    digest = "sha256:" + hashlib.sha256(spec.read_bytes()).hexdigest()

    bet = {
        "id": BET_ID,
        "track": "T5-ORCH",
        "window": "Y1Q2",
        "title": "Supervised blueprint control",
        "status": "candidate",
        "goal": "Ship one supervised production slice",
        "why_now": "The contract is accepted and bounded",
        "accepted_specifications": [
            {
                "spec_ref": SPEC_REF,
                "spec_version": SPEC_VERSION,
                "content_digest": digest,
            }
        ],
        "done_when": ["candidate is independently verified"],
        "verify": [{"cmd": "pytest -q", "expect": "exit 0"}],
        "non_goals": ["no automatic merge"],
        "circuit_breaker": "stop on scope or approval drift",
    }
    ledger = tmp_path / "docs" / "plans" / "3y-bet-ledger.yaml"
    ledger.parent.mkdir(parents=True)
    ledger.write_text(
        yaml.safe_dump({"bets": [bet]}, sort_keys=False), encoding="utf-8"
    )

    approval_ref = ".omo/workers/runs/approval.yaml"
    task = {
        "id": TASK_ID,
        "title": "Implement the supervised slice",
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
        "human_approval_required": True,
        "source_docs": [SPEC_PATH],
        "read_surfaces": ["src/", SPEC_PATH],
        "write_surfaces": ["src/omo/blueprint_control.py"],
        "required_capabilities": ["workflow.execute", "python"],
        "entry_gate": ["controller approval"],
        "acceptance_criteria": ["focused test command exits zero"],
        "evidence_required": ["focused tests", "diff check"],
        "deliverables": ["src/omo/blueprint_control.py"],
        "test_plan": ["/usr/bin/true"],
        "depends_on": [],
    }
    task_path = tmp_path / ".omo" / "tasks" / "active" / f"{TASK_ID}.yaml"
    task_path.parent.mkdir(parents=True)
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    return bet, task


def _dispatch_authority(
    tmp_path: Path,
    *,
    approval_task_id: str = TASK_ID,
    expires_at: str = "2026-08-16T00:00:00+00:00",
    worker_capabilities: list[str] | None = None,
) -> None:
    registry = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry.parent.mkdir(parents=True)
    registry.write_text(
        yaml.safe_dump(
            {
                "workers": [
                    {
                        "id": "worker-a",
                        "enabled": True,
                        "admission_state": "admitted",
                        "allowed_operation_level": "L1",
                        "write_scope": {"mode": "bounded"},
                        "transports": {"cli_prompt": {"command": "worker-a"}},
                        "capabilities": worker_capabilities
                        or ["workflow.execute", "python"],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    approval = tmp_path / ".omo" / "workers" / "runs" / "approval.yaml"
    approval.parent.mkdir(parents=True, exist_ok=True)
    approval.write_text(
        yaml.safe_dump(
            {
                "approval_id": "approval-blueprint-1",
                "task_id": approval_task_id,
                "approval_status": "granted",
                "approval_scope": "workflow.execute",
                "approved_at": "2026-08-14T09:00:00+00:00",
                "expires_at": expires_at,
                "refs": {
                    "task_ref": f".omo/tasks/active/{TASK_ID}.yaml",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _health() -> dict:
    return {
        "status": "healthy",
        "source": "test",
        "observed_at": "2026-08-14T09:30:00+00:00",
        "capabilities": {
            "workflow.execute": {"available": True},
            "python": {"available": True},
        },
    }


def _compile(tmp_path: Path):  # noqa: ANN202
    return BlueprintControlService(tmp_path).compile_packet(
        bet_id=BET_ID,
        task_id=TASK_ID,
        spec_ref=SPEC_REF,
        spec_version=SPEC_VERSION,
        expires_at="2026-08-15T00:00:00+00:00",
    )


def _git(
    tmp_path: Path, *args: str, input_bytes: bytes | None = None
) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=tmp_path,
        input=input_bytes,
        capture_output=True,
        check=True,
    )


def _commit_baseline(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.name", "Blueprint Test")
    _git(tmp_path, "config", "user.email", "blueprint@example.invalid")
    (tmp_path / ".gitignore").write_text(".omo/\n", encoding="utf-8")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-qm", "baseline")


def _adapter_receipt(
    tmp_path: Path,
    *,
    readiness: str = "model_output_observed",
    paths: list[str] | None = None,
) -> dict:
    targets = paths or ["src/omo/blueprint_control.py"]
    _git(tmp_path, "add", "-N", "--", *targets)
    patch = _git(tmp_path, "diff", "--binary", "HEAD", "--", *targets).stdout
    changed = (
        _git(tmp_path, "diff", "--name-only", "HEAD", "--", *targets)
        .stdout.decode()
        .splitlines()
    )
    _git(tmp_path, "reset", "-q", "--", *targets)
    receipt = {
        "baseline_digest": "sha256:" + "1" * 64,
        "changed_paths": changed,
        "exit_code": 0,
        "output_sha256": hashlib.sha256(b"final model output").hexdigest(),
        "patch_digest": "sha256:" + hashlib.sha256(patch).hexdigest(),
        "post_digest": "sha256:" + "2" * 64,
        "readiness": readiness,
        "schema": "codex-worker-receipt/v1",
        "status": "succeeded",
        "supervision": {
            "controller_approval": "granted",
            "provider_review": "completed_without_observed_escalation",
        },
        "worker": "codex",
    }
    canonical = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    receipt["receipt_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
    return receipt


def _dispatched_repo(tmp_path: Path, *, now: str = "2026-08-14T10:00:00+00:00"):  # noqa: ANN202
    _workspace(tmp_path)
    _dispatch_authority(tmp_path)
    _commit_baseline(tmp_path)
    service = BlueprintControlService(tmp_path)
    compiled = _compile(tmp_path)
    dispatched = service.dispatch_packet(
        compiled,
        worker_id="worker-a",
        capability_health=_health(),
        now=now,
    )
    return service, compiled, dispatched


def _supervisor_start_receipt(
    tmp_path: Path,
    compiled,
    dispatched,  # noqa: ANN001
):  # noqa: ANN202
    prompt_ref = str(dispatched["prompt_path"])
    prompt_digest = (
        "sha256:" + hashlib.sha256((tmp_path / prompt_ref).read_bytes()).hexdigest()
    )
    return {
        "schema": "orca-codex-supervisor/v1",
        "ok": True,
        "state": "awaiting_human_action",
        "binding": {
            "workflow_run_id": dispatched["workflow_run_id"],
            "omo_task_id": TASK_ID,
            "packet_id": compiled.packet["packet_id"],
            "packet_hash": compiled.packet_hash,
            "omo_dispatch_id": dispatched["dispatch_id"],
            "prompt_ref": prompt_ref,
            "prompt_digest": prompt_digest,
        },
        "orca": {
            "run_id": "orca-run-001",
            "task_id": "orca-task-001",
            "dispatch_id": "orca-dispatch-001",
            "terminal_handle": "terminal-001",
        },
        "human_action_required": True,
        "input_accepted": "unproven",
        "model_completion": "unproven",
    }


def _supervisor_collect_receipt(
    tmp_path: Path,
    compiled,
    dispatched,  # noqa: ANN001
):  # noqa: ANN202
    receipt = _supervisor_start_receipt(tmp_path, compiled, dispatched)
    return {
        **receipt,
        "state": "settled",
        "model_completion": "observed",
        "transcript_digest": "sha256:" + "7" * 64,
    }


def test_compile_is_deterministic_and_contains_governed_contract(
    tmp_path: Path,
) -> None:
    bet, task = _workspace(tmp_path)

    first = _compile(tmp_path)
    second = _compile(tmp_path)

    assert first == second
    assert first.packet["packet_id"].startswith("WP-BP-")
    assert first.packet_hash.startswith("sha256:")
    assert first.packet["status"] == "candidate"
    assert first.packet["authority"]["human_gate"] is True
    assert first.packet["scope"]["read_surfaces"] == task["read_surfaces"]
    assert first.packet["scope"]["write_surfaces"] == task["write_surfaces"]
    assert (
        first.packet["assignment"]["required_capabilities"]
        == task["required_capabilities"]
    )
    assert first.packet["scope"]["non_goals"] == bet["non_goals"]
    assert (
        first.packet["acceptance"]["evidence_requirements"] == task["evidence_required"]
    )
    assert first.packet["acceptance"]["done_when"] == [
        {
            "id": "AC1",
            "assertion": task["acceptance_criteria"][0],
            "evidence_type": "command_receipt",
        }
    ]
    assert first.packet["acceptance"]["verify_commands"] == ["/usr/bin/true"]


def test_compile_requires_exact_accepted_spec_digest(tmp_path: Path) -> None:
    _workspace(tmp_path)
    (tmp_path / SPEC_PATH).write_text("# drifted\n", encoding="utf-8")

    with pytest.raises(BlueprintControlError, match="digest"):
        _compile(tmp_path)


def test_compile_rejects_missing_accepted_binding(tmp_path: Path) -> None:
    _workspace(tmp_path)
    ledger_path = tmp_path / "docs" / "plans" / "3y-bet-ledger.yaml"
    ledger = yaml.safe_load(ledger_path.read_text(encoding="utf-8"))
    ledger["bets"][0]["accepted_specifications"] = []
    ledger_path.write_text(yaml.safe_dump(ledger, sort_keys=False), encoding="utf-8")

    with pytest.raises(BlueprintControlError, match="accepted specification"):
        _compile(tmp_path)


def test_compile_ignores_fake_noncanonical_ledger(tmp_path: Path) -> None:
    bet, _ = _workspace(tmp_path)
    canonical = tmp_path / "docs" / "plans" / "3y-bet-ledger.yaml"
    canonical.unlink()
    fake = tmp_path / ".omo" / "fake-ledger.yaml"
    fake.parent.mkdir(parents=True, exist_ok=True)
    fake.write_text(yaml.safe_dump({"bets": [bet]}, sort_keys=False), encoding="utf-8")

    with pytest.raises(BlueprintControlError, match="canonical BET ledger"):
        _compile(tmp_path)


def test_compile_requires_task_human_gate(tmp_path: Path) -> None:
    _workspace(tmp_path)
    task_path = tmp_path / ".omo" / "tasks" / "active" / f"{TASK_ID}.yaml"
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["human_approval_required"] = False
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")

    with pytest.raises(BlueprintControlError, match="human gate"):
        _compile(tmp_path)


def test_compile_rejects_unsafe_output_path_without_mesh_writes(tmp_path: Path) -> None:
    _workspace(tmp_path)
    task_path = tmp_path / ".omo" / "tasks" / "active" / f"{TASK_ID}.yaml"
    task = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    task["write_surfaces"] = ["../outside.md"]
    task_path.write_text(yaml.safe_dump(task, sort_keys=False), encoding="utf-8")
    before = deepcopy(WorkflowMeshStore(tmp_path / ".omo").events())

    with pytest.raises(BlueprintControlError, match="unsafe"):
        _compile(tmp_path)

    assert WorkflowMeshStore(tmp_path / ".omo").events() == before == []


def test_dispatch_records_exact_mesh_order_and_transport_only_state(
    tmp_path: Path,
) -> None:
    _workspace(tmp_path)
    _dispatch_authority(tmp_path)
    service = BlueprintControlService(tmp_path)

    result = service.dispatch_packet(
        _compile(tmp_path),
        worker_id="worker-a",
        capability_health=_health(),
        now="2026-08-14T10:00:00+00:00",
    )

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert [event["event_type"] for event in events] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
    ]
    identity = events[0]["payload"]
    assert identity["bet_id"] == BET_ID
    assert identity["packet_id"] == result["packet_id"]
    assert identity["packet_hash"] == result["packet_hash"]
    assert result["state"] == "transport_accepted"
    assert "ready" not in result["state"]
    assert "succeeded" not in result["state"]

    dispatch = yaml.safe_load(
        (tmp_path / result["dispatch_path"]).read_text(encoding="utf-8")
    )
    assert dispatch["blueprint"] == {
        "packet_id": result["packet_id"],
        "packet_hash": result["packet_hash"],
        "bet_id": BET_ID,
    }
    assert dispatch["control_state"] == {
        "controller_approval": "granted",
        "transport": "accepted",
        "readiness": "unproven",
        "provider_review": "unknown",
    }


@pytest.mark.parametrize(
    ("approval_mode", "message"),
    [
        ("missing", "missing"),
        ("expired", "expired"),
        ("mismatched", "mismatch"),
    ],
)
def test_dispatch_fails_closed_for_invalid_approval(
    tmp_path: Path, approval_mode: str, message: str
) -> None:
    _workspace(tmp_path)
    if approval_mode == "expired":
        _dispatch_authority(tmp_path, expires_at="2026-08-14T09:59:59+00:00")
    elif approval_mode == "mismatched":
        _dispatch_authority(tmp_path, approval_task_id="TASK-OTHER")
    else:
        _dispatch_authority(tmp_path)
        (tmp_path / ".omo" / "workers" / "runs" / "approval.yaml").unlink()

    with pytest.raises(WorkflowDispatchError, match=message):
        BlueprintControlService(tmp_path).dispatch_packet(
            _compile(tmp_path),
            worker_id="worker-a",
            capability_health=_health(),
            now="2026-08-14T10:00:00+00:00",
        )

    assert WorkflowMeshStore(tmp_path / ".omo").events() == []


def test_dispatch_rejects_worker_capability_mismatch(tmp_path: Path) -> None:
    _workspace(tmp_path)
    _dispatch_authority(tmp_path, worker_capabilities=["workflow.execute"])

    with pytest.raises(ValueError, match="capability_mismatch"):
        BlueprintControlService(tmp_path).dispatch_packet(
            _compile(tmp_path),
            worker_id="worker-a",
            capability_health=_health(),
            now="2026-08-14T10:00:00+00:00",
        )


def test_mesh_append_failure_propagates_without_transport_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _workspace(tmp_path)
    _dispatch_authority(tmp_path)
    original_append = WorkflowMeshStore.append
    calls = 0

    def fail_step_append(self, event):  # noqa: ANN001, ANN202
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("mesh unavailable")
        return original_append(self, event)

    monkeypatch.setattr(WorkflowMeshStore, "append", fail_step_append)
    service = BlueprintControlService(tmp_path)
    compiled = _compile(tmp_path)
    task_path = tmp_path / ".omo" / "tasks" / "active" / f"{TASK_ID}.yaml"
    task_before = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    run_dir = tmp_path / ".omo" / "workers" / "runs"
    artifacts_before = {path.name for path in run_dir.iterdir()}

    with pytest.raises(RuntimeError, match="mesh unavailable"):
        service.dispatch_packet(
            compiled,
            worker_id="worker-a",
            capability_health=_health(),
            now="2026-08-14T10:00:00+00:00",
        )

    assert yaml.safe_load(task_path.read_text(encoding="utf-8")) == task_before
    assert {path.name for path in run_dir.iterdir()} == artifacts_before
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert [event["event_type"] for event in events] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
    ]


def test_observe_does_not_promote_exit_zero_to_readiness(tmp_path: Path) -> None:
    _workspace(tmp_path)
    _dispatch_authority(tmp_path)
    service = BlueprintControlService(tmp_path)
    result = service.dispatch_packet(
        _compile(tmp_path),
        worker_id="worker-a",
        capability_health=_health(),
        now="2026-08-14T10:00:00+00:00",
    )
    dispatch_path = tmp_path / result["dispatch_path"]
    receipt_path = dispatch_path.with_name(
        dispatch_path.name.removesuffix("-dispatch.yaml") + "-receipt.yaml"
    )
    receipt_path.write_text(
        yaml.safe_dump({"returncode": 0, "transport": "accepted"}),
        encoding="utf-8",
    )

    observation = service.observe_dispatch(result)

    assert observation["state"] == "transport_accepted"
    assert observation["control_state"]["readiness"] == "unproven"
    assert observation["receipt_observed"] is True


def test_supervised_start_freezes_baseline_then_pauses_for_human(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    target = tmp_path / "src" / "omo" / "blueprint_control.py"
    calls: list[dict] = []

    def supervisor(**kwargs):  # noqa: ANN003, ANN202
        calls.append(kwargs)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        return _supervisor_start_receipt(tmp_path, compiled, dispatched)

    started = service.start_supervised_execution(
        compiled,
        dispatched,
        supervisor=supervisor,
        timeout_seconds=5,
    )

    assert started["state"] == "awaiting_human_action"
    assert started["human_action_required"] is True
    assert started["input_accepted"] == "unproven"
    assert started["model_completion"] == "unproven"
    assert started["spec_binding"] == compiled.packet["spec_binding"]
    assert started["prompt_ref"] == dispatched["prompt_path"]
    assert started["prompt_digest"] == started["prompt_binding"]["prompt_digest"]
    assert calls[0]["action"] == "start"
    assert calls[0]["packet_id"] == compiled.packet["packet_id"]
    assert calls[0]["packet_hash"] == compiled.packet_hash
    assert calls[0]["workspace_root"] == str(tmp_path.resolve())
    assert "argv" not in calls[0]
    assert "launch_command" not in calls[0]
    assert target.is_file()
    assert not _git(
        tmp_path,
        "ls-tree",
        started["baseline_tree"],
        "--",
        "src/omo/blueprint_control.py",
    ).stdout
    execution_path = tmp_path / started["execution_projection"]
    assert execution_path.is_file()
    persisted = json.loads(execution_path.read_text(encoding="utf-8"))
    assert persisted["projection_digest"].startswith("sha256:")
    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert event_types[-1:] == ["StepStarted"]
    assert "WorkflowSucceeded" not in event_types
    assert "EvidenceRecorded" not in event_types


def test_supervised_start_marks_projection_failed_when_mesh_start_is_not_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def fail_append(_self, _event):  # noqa: ANN001, ANN202
        raise RuntimeError("mesh write unavailable")

    monkeypatch.setattr(WorkflowMeshStore, "append", fail_append)
    with pytest.raises(RuntimeError, match="mesh write unavailable"):
        service.start_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: _supervisor_start_receipt(
                tmp_path, compiled, dispatched
            ),
        )
    execution_path = service._execution_projection_path(dispatched)  # noqa: SLF001
    persisted = json.loads(execution_path.read_text(encoding="utf-8"))
    assert persisted["state"] == "control_projection_failed"
    assert persisted["candidate_collected"] is False
    assert persisted["orca"]["terminal_handle"] == "terminal-001"
    with pytest.raises(BlueprintControlError, match="control projection failed"):
        service.start_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must not start another worker"),
        )


def test_supervised_start_crash_leaves_stable_startup_unknown_and_never_relaunches(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    seen: list[dict] = []

    def crash_after_external_start(**kwargs):  # noqa: ANN003, ANN202
        seen.append(kwargs)
        projection_path = service._execution_projection_path(dispatched)  # noqa: SLF001
        persisted = json.loads(projection_path.read_text(encoding="utf-8"))
        assert persisted["state"] == "starting"
        assert persisted["baseline_tree"]
        assert persisted["binding"]["packet_hash"] == compiled.packet_hash
        raise RuntimeError("controller crashed after Orca side effect")

    with pytest.raises(RuntimeError, match="controller crashed"):
        service.start_supervised_execution(
            compiled,
            dispatched,
            supervisor=crash_after_external_start,
        )

    execution_path = service._execution_projection_path(dispatched)  # noqa: SLF001
    persisted = json.loads(execution_path.read_text(encoding="utf-8"))
    assert persisted["state"] == "startup_outcome_unknown"
    assert persisted["idempotency_key"] == seen[0]["idempotency_key"]
    with pytest.raises(BlueprintControlError, match="startup outcome is unknown"):
        service.start_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must not launch twice"),
        )


def test_supervised_start_invalid_receipt_preserves_safe_recovery_facts(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    with pytest.raises(BlueprintControlError, match="start receipt is invalid"):
        service.start_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: {
                "schema": "orca-codex-supervisor/v1",
                "ok": False,
                "stage": "worker_start",
                "reason": "orca_worker_not_started",
                "residual_resources": [
                    "orca:run:run-001",
                    "orca:task:task-001",
                    "not-allowed",
                ],
            },
        )

    execution_path = service._execution_projection_path(dispatched)  # noqa: SLF001
    persisted = json.loads(execution_path.read_text(encoding="utf-8"))
    assert persisted["state"] == "startup_outcome_unknown"
    failure = persisted["supervisor_failure"]
    assert failure["stage"] == "worker_start"
    assert failure["reason"] == "orca_worker_not_started"
    assert failure["residual_resources"] == []
    assert failure["receipt_digest"].startswith("sha256:")
    with pytest.raises(BlueprintControlError, match="startup outcome is unknown"):
        service.start_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must not launch twice"),
        )


def test_supervised_collect_keeps_active_worker_paused_without_candidate(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    service.start_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: _supervisor_start_receipt(
            tmp_path, compiled, dispatched
        ),
    )

    active = service.collect_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: {
            **_supervisor_start_receipt(tmp_path, compiled, dispatched),
            "ok": False,
            "stage": "worker_show",
            "reason": "worker_not_settled",
            "residual_resources": ["terminal-001"],
        },
    )

    assert active["state"] == "awaiting_human_action"
    assert active["candidate_collected"] is False
    assert active["binding"]["packet_hash"] == compiled.packet_hash
    assert active["orca"]["terminal_handle"] == "terminal-001"
    projection = service._candidate_projection_path(dispatched)  # noqa: SLF001
    assert not projection.exists()
    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "WorkflowSucceeded" not in event_types
    assert "EvidenceRecorded" not in event_types
    assert "StepFailed" not in event_types


def test_supervised_collect_settled_worker_builds_independent_candidate(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    target = tmp_path / "src" / "omo" / "blueprint_control.py"

    def start_supervisor(**_kwargs):  # noqa: ANN003, ANN202
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 2\n", encoding="utf-8")
        return _supervisor_start_receipt(tmp_path, compiled, dispatched)

    service.start_supervised_execution(
        compiled, dispatched, supervisor=start_supervisor
    )
    measured: list[list[str]] = []

    def measurer(*, argv, **_kwargs):  # noqa: ANN001, ANN202
        measured.append(argv)
        return {"returncode": 0, "stdout": b"directly measured"}

    collected = service.collect_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: _supervisor_collect_receipt(
            tmp_path, compiled, dispatched
        ),
        acceptance_runner=measurer,
    )

    assert collected["state"] == "candidate_collected"
    assert collected["manifest"]["changed_paths"] == ["src/omo/blueprint_control.py"]
    assert collected["transport_receipt"]["output_digest"] == "7" * 64
    assert collected["transport_receipt"]["orca_dispatch_id"] == "orca-dispatch-001"
    assert collected["acceptance_measurements"][0]["source"] == (
        "deterministic-command"
    )
    assert measured == [["/usr/bin/true"]]
    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "WorkflowSucceeded" in event_types
    assert "EvidenceRecorded" in event_types
    replay = service.collect_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: pytest.fail("valid replay must not recollect"),
    )
    assert replay == collected


def test_supervised_collect_evidence_failure_transitions_to_auditable_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    target = tmp_path / "src" / "omo" / "blueprint_control.py"

    def start_supervisor(**_kwargs):  # noqa: ANN003, ANN202
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 3\n", encoding="utf-8")
        return _supervisor_start_receipt(tmp_path, compiled, dispatched)

    service.start_supervised_execution(
        compiled, dispatched, supervisor=start_supervisor
    )

    def fail_evidence(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise RuntimeError("evidence store unavailable")

    monkeypatch.setattr(
        OrchestrationContractCoordinator, "record_candidate", fail_evidence
    )
    with pytest.raises(RuntimeError, match="evidence store unavailable"):
        service.collect_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: _supervisor_collect_receipt(
                tmp_path, compiled, dispatched
            ),
        )

    snapshot = WorkflowMeshStore(tmp_path / ".omo").snapshot(
        dispatched["workflow_run_id"]
    )
    assert snapshot["state"] == "failed"
    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert event_types[-3:] == [
        "WorkflowSucceeded",
        "CompensationStarted",
        "StepFailed",
    ]
    failed_path = service._candidate_projection_path(dispatched)  # noqa: SLF001
    failed = json.loads(failed_path.read_text(encoding="utf-8"))
    assert failed["state"] == "candidate_collection_failed"
    with pytest.raises(BlueprintControlError, match="candidate collection failed"):
        service.collect_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must not recollect"),
        )


@pytest.mark.parametrize(
    "mutation",
    ["baseline_tree", "baseline_digest", "write_surfaces", "worker_id"],
)
def test_supervised_collect_rejects_execution_projection_not_backed_by_external_facts(
    tmp_path: Path, mutation: str
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    started = service.start_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: _supervisor_start_receipt(
            tmp_path, compiled, dispatched
        ),
    )
    execution_path = tmp_path / started["execution_projection"]
    projection = json.loads(execution_path.read_text(encoding="utf-8"))
    if mutation == "baseline_tree":
        projection[mutation] = "0" * 40
    elif mutation == "baseline_digest":
        projection[mutation] = "sha256:" + "0" * 64
    elif mutation == "write_surfaces":
        projection[mutation] = ["src/other.py"]
    else:
        projection[mutation] = "other-worker"
    projection["projection_digest"] = BlueprintControlService._projection_digest(  # noqa: SLF001
        projection
    )
    execution_path.write_text(json.dumps(projection), encoding="utf-8")

    with pytest.raises(BlueprintControlError, match="execution external fact mismatch"):
        service.collect_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must fail before Orca collect"),
        )


def test_supervised_candidate_replay_rejects_forged_projection_and_cross_dispatch(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    target = tmp_path / "src" / "omo" / "blueprint_control.py"

    def start_supervisor(**_kwargs):  # noqa: ANN003, ANN202
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 4\n", encoding="utf-8")
        return _supervisor_start_receipt(tmp_path, compiled, dispatched)

    service.start_supervised_execution(
        compiled, dispatched, supervisor=start_supervisor
    )
    collected = service.collect_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: _supervisor_collect_receipt(
            tmp_path, compiled, dispatched
        ),
    )
    candidate_path = service._candidate_projection_path(dispatched)  # noqa: SLF001
    forged = json.loads(candidate_path.read_text(encoding="utf-8"))
    forged["transport_receipt"]["dispatch_id"] = "cross-run-dispatch"
    receipt = forged["transport_receipt"]
    receipt["receipt_digest"] = compute_packet_hash(
        canonicalize(
            {key: value for key, value in receipt.items() if key != "receipt_digest"}
        )
    )
    forged["projection_digest"] = BlueprintControlService._projection_digest(  # noqa: SLF001
        forged
    )
    candidate_path.write_text(json.dumps(forged), encoding="utf-8")

    with pytest.raises(BlueprintControlError, match="candidate projection invalid"):
        service.collect_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must not recollect"),
        )

    candidate_path.write_text(
        json.dumps({**collected, "projection_digest": "sha256:" + "0" * 64}),
        encoding="utf-8",
    )
    with pytest.raises(BlueprintControlError, match="candidate projection invalid"):
        service.collect_supervised_execution(
            compiled,
            dispatched,
            supervisor=lambda **_kwargs: pytest.fail("must not recollect"),
        )


def test_supervised_collect_rejects_rehashed_binding_tamper_before_orca_call(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    started = service.start_supervised_execution(
        compiled,
        dispatched,
        supervisor=lambda **_kwargs: _supervisor_start_receipt(
            tmp_path, compiled, dispatched
        ),
    )
    execution_path = tmp_path / started["execution_projection"]
    projection = json.loads(execution_path.read_text(encoding="utf-8"))
    projection["binding"]["packet_hash"] = "sha256:" + "0" * 64
    projected = {
        key: value for key, value in projection.items() if key != "projection_digest"
    }
    canonical = json.dumps(projected, sort_keys=True, separators=(",", ":"))
    projection["projection_digest"] = (
        "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
    )
    execution_path.write_text(json.dumps(projection), encoding="utf-8")
    called = False

    def supervisor(**_kwargs):  # noqa: ANN003, ANN202
        nonlocal called
        called = True
        return _supervisor_collect_receipt(tmp_path, compiled, dispatched)

    with pytest.raises(BlueprintControlError, match="execution binding mismatch"):
        service.collect_supervised_execution(
            compiled,
            dispatched,
            supervisor=supervisor,
        )

    assert called is False
    assert not service._candidate_projection_path(dispatched).exists()  # noqa: SLF001
    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "WorkflowSucceeded" not in event_types
    assert "EvidenceRecorded" not in event_types


def test_execute_collect_and_independent_verify_use_real_git_delta(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    measured_commands: list[list[str]] = []

    def measurer(*, argv, **_kwargs):  # noqa: ANN001, ANN202
        measured_commands.append(argv)
        return {"returncode": 0, "stdout": b"measured"}

    collected = service.execute_and_collect(
        compiled,
        dispatched,
        runner=runner,
        acceptance_runner=measurer,
        timeout_seconds=5,
    )
    verified = service.verify_candidate(
        compiled,
        dispatched,
        collected,
        verifier=lambda **_kwargs: {"returncode": 0, "stdout": b"green"},
        timeout_seconds=5,
    )

    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert [event["event_type"] for event in events] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
        "StepStarted",
        "WorkflowSucceeded",
        "EvidenceRecorded",
        "WorkflowVerified",
    ]
    assert collected["manifest"]["changed_paths"] == ["src/omo/blueprint_control.py"]
    assert collected["manifest"]["claims"] == [
        {
            "acceptance_id": "AC1",
            "assertion": "focused test command exits zero",
            "evidence_refs": [collected["acceptance_measurements"][0]["evidence_ref"]],
        }
    ]
    assert collected["acceptance_measurements"][0]["command"] == ["/usr/bin/true"]
    assert measured_commands == [["/usr/bin/true"]]
    assert collected["patch_ref"].startswith("git-object://")
    assert verified["state"] == "independently_verified"


def test_collect_refuses_to_invent_acceptance_claims_without_direct_measurement(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    with pytest.raises(BlueprintControlError, match="acceptance measurement failed"):
        service.execute_and_collect(
            compiled,
            dispatched,
            runner=runner,
            acceptance_runner=lambda **_kwargs: {
                "returncode": 1,
                "stdout": b"not measured",
            },
        )

    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "WorkflowSucceeded" not in event_types
    assert "EvidenceRecorded" not in event_types


def test_baseline_is_frozen_before_runner_can_modify_workspace(tmp_path: Path) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("PRE_CALLBACK = True\n", encoding="utf-8")
        on_process_started()
        target.write_text("FINAL = True\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    collected = service.execute_and_collect(compiled, dispatched, runner=runner)
    patch = _git(
        tmp_path,
        "cat-file",
        "blob",
        collected["patch_ref"].removeprefix("git-object://"),
    ).stdout

    assert b"new file mode" in patch
    assert b"FINAL = True" in patch


def test_collect_replay_returns_persisted_candidate_without_rerunning(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    calls = 0

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        nonlocal calls
        calls += 1
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    first = service.execute_and_collect(compiled, dispatched, runner=runner)
    replay = service.execute_and_collect(compiled, dispatched, runner=runner)

    assert replay == first
    assert calls == 1
    assert [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ].count("EvidenceRecorded") == 1


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("digest", "receipt digest"),
        ("readiness", "model output"),
        ("path", "changed paths"),
    ],
)
def test_collect_rejects_untrusted_adapter_receipt_without_evidence(
    tmp_path: Path, mutation: str, message: str
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt = _adapter_receipt(workspace_root)
        if mutation == "digest":
            receipt["receipt_sha256"] = "0" * 64
        elif mutation == "readiness":
            receipt["readiness"] = "transport_accepted"
            canonical = json.dumps(
                {k: v for k, v in receipt.items() if k != "receipt_sha256"},
                sort_keys=True,
                separators=(",", ":"),
            )
            receipt["receipt_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
        elif mutation == "path":
            receipt["changed_paths"] = []
            canonical = json.dumps(
                {k: v for k, v in receipt.items() if k != "receipt_sha256"},
                sort_keys=True,
                separators=(",", ":"),
            )
            receipt["receipt_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        return {"returncode": 0}

    with pytest.raises(BlueprintControlError, match=message):
        service.execute_and_collect(compiled, dispatched, runner=runner)

    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "EvidenceRecorded" not in event_types
    assert "WorkflowVerified" not in event_types


def test_runner_prelaunch_rejection_has_no_step_started_or_candidate(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(**_kwargs):  # noqa: ANN003, ANN202
        raise BlueprintControlError("runner prelaunch rejection")

    with pytest.raises(BlueprintControlError, match="prelaunch rejection"):
        service.execute_and_collect(compiled, dispatched, runner=runner)

    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "StepStarted" not in event_types
    assert "StepFailed" not in event_types
    assert "WorkflowSucceeded" not in event_types
    assert "EvidenceRecorded" not in event_types


def test_started_provider_human_review_fails_step_without_candidate(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt = _adapter_receipt(workspace_root)
        receipt["supervision"]["provider_review"] = "human_required"
        canonical = json.dumps(
            {key: value for key, value in receipt.items() if key != "receipt_sha256"},
            sort_keys=True,
            separators=(",", ":"),
        )
        receipt["receipt_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        return {"returncode": 0}

    with pytest.raises(BlueprintControlError, match="human approval"):
        service.execute_and_collect(compiled, dispatched, runner=runner)

    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert event_types[-2:] == ["StepStarted", "StepFailed"]
    assert "WorkflowSucceeded" not in event_types
    assert "EvidenceRecorded" not in event_types


def test_transport_ack_without_receipt_fails_started_step_without_evidence(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        return {"returncode": 0, "transport": "accepted"}

    with pytest.raises(BlueprintControlError, match="receipt is missing"):
        service.execute_and_collect(compiled, dispatched, runner=runner)

    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert event_types[-2:] == ["StepStarted", "StepFailed"]
    assert "EvidenceRecorded" not in event_types


def test_out_of_scope_git_delta_is_measured_and_rejected(tmp_path: Path) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        allowed = workspace_root / "src" / "omo" / "blueprint_control.py"
        allowed.parent.mkdir(parents=True, exist_ok=True)
        allowed.write_text("VALUE = 1\n", encoding="utf-8")
        outside = workspace_root / "outside.txt"
        outside.write_text("escaped\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(
                _adapter_receipt(
                    workspace_root,
                    paths=["src/omo/blueprint_control.py", "outside.txt"],
                )
            ),
            encoding="utf-8",
        )
        return {"returncode": 0}

    with pytest.raises(BlueprintControlError, match="out-of-scope"):
        service.execute_and_collect(compiled, dispatched, runner=runner)

    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert event_types[-1] == "StepFailed"
    assert "EvidenceRecorded" not in event_types


def test_failed_verifier_compensates_and_restores_exact_baseline(
    tmp_path: Path,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    baseline = _git(tmp_path, "status", "--porcelain=v1", "-z").stdout

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    collected = service.execute_and_collect(compiled, dispatched, runner=runner)
    result = service.verify_candidate(
        compiled,
        dispatched,
        collected,
        verifier=lambda **_kwargs: {"returncode": 1, "stdout": b"red"},
    )

    assert result["state"] == "closed"
    assert result["baseline_digest"] == collected["baseline_digest"]
    assert _git(tmp_path, "status", "--porcelain=v1", "-z").stdout == baseline
    event_types = [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]
    assert "WorkflowVerified" not in event_types
    assert event_types[-4:] == [
        "CompensationStarted",
        "WorkflowRecovered",
        "WorkflowCancelled",
        "WorkflowClosed",
    ]


def test_tampered_patch_leaves_rejected_run_unclosed(tmp_path: Path) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    collected = service.execute_and_collect(compiled, dispatched, runner=runner)
    collected["patch_digest"] = "sha256:" + "0" * 64
    result = service.rollback_candidate(dispatched, collected)

    assert result["state"] == "rollback_unconfirmed"
    assert (
        WorkflowMeshStore(tmp_path / ".omo").snapshot(dispatched["workflow_run_id"])[
            "state"
        ]
        == "compensating"
    )


def test_candidate_from_other_run_cannot_compensate_or_close_current_run(
    tmp_path: Path,
) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    service_a, compiled_a, dispatched_a = _dispatched_repo(root_a)
    service_b, compiled_b, dispatched_b = _dispatched_repo(
        root_b, now="2026-08-14T10:01:00+00:00"
    )

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"VALUE = {workspace_root.name!r}\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    candidate_a = service_a.execute_and_collect(compiled_a, dispatched_a, runner=runner)
    candidate_b = service_b.execute_and_collect(compiled_b, dispatched_b, runner=runner)
    blob_b = _git(
        root_b,
        "cat-file",
        "blob",
        candidate_b["patch_ref"].removeprefix("git-object://"),
    ).stdout
    imported_oid = (
        _git(root_a, "hash-object", "-w", "--stdin", input_bytes=blob_b)
        .stdout.decode()
        .strip()
    )
    candidate_b["patch_ref"] = f"git-object://{imported_oid}"
    candidate_b["manifest"]["artifact_refs"] = [f"git-object://{imported_oid}"]
    before = WorkflowMeshStore(root_a / ".omo").events()

    result = service_a.rollback_candidate(dispatched_a, candidate_b)

    assert result == {
        "state": "rollback_unconfirmed",
        "reason": "candidate_binding_mismatch",
    }
    after = WorkflowMeshStore(root_a / ".omo").events()
    assert after == before
    assert "CompensationStarted" not in [event["event_type"] for event in after]
    assert "WorkflowClosed" not in [event["event_type"] for event in after]
    assert (
        WorkflowMeshStore(root_a / ".omo").snapshot(dispatched_a["workflow_run_id"])[
            "state"
        ]
        == "succeeded"
    )
    assert (
        candidate_a["transport_receipt"]["dispatch_id"]
        != candidate_b["transport_receipt"]["dispatch_id"]
    )


def _write_cli_packet(tmp_path: Path, compiled) -> str:  # noqa: ANN001
    packet_ref = ".omo/workers/runs/blueprint-packet.json"
    path = tmp_path / packet_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"packet": compiled.packet, "packet_hash": compiled.packet_hash},
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return packet_ref


def test_default_supervisor_forwards_deterministic_start_idempotency_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "bin" / "gac" / "orca-codex-supervisor.py"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    observed: list[list[str]] = []

    def run(command, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        observed.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"ok": False, "reason": "fixture"}),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)

    BlueprintControlService(tmp_path)._default_supervisor(
        action="start",
        timeout_seconds=12,
        workflow_run_id="wf-001",
        omo_task_id=TASK_ID,
        packet_id="packet-001",
        packet_hash="sha256:" + "a" * 64,
        omo_dispatch_id="dispatch-001",
        prompt_ref="prompts/task.md",
        prompt_digest="sha256:" + "b" * 64,
        idempotency_key="orca-codex-start:stable",
    )

    assert observed[0][-4:] == [
        "--idempotency-key",
        "orca-codex-start:stable",
        "--timeout-ms",
        "12000",
    ]


def test_cli_compile_emits_json_and_persists_only_explicit_packet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _workspace(tmp_path)

    result = cli_main(
        [
            "blueprint",
            "compile",
            "--root",
            str(tmp_path),
            "--bet-id",
            BET_ID,
            "--task-id",
            TASK_ID,
            "--spec-ref",
            SPEC_REF,
            "--spec-version",
            SPEC_VERSION,
            "--expires-at",
            "2026-08-15T00:00:00+00:00",
            "--packet-file",
            ".omo/workers/runs/blueprint-packet.json",
        ]
    )

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "compiled"
    assert output["packet_id"].startswith("WP-BP-")
    assert (tmp_path / output["packet_file"]).is_file()


def test_cli_dispatch_missing_approval_is_json_error_without_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _workspace(tmp_path)
    _dispatch_authority(tmp_path)
    (tmp_path / ".omo" / "workers" / "runs" / "approval.yaml").unlink()
    (tmp_path / ".omo" / "workers" / "runs" / "health.json").write_text(
        json.dumps(_health()), encoding="utf-8"
    )
    packet_ref = _write_cli_packet(tmp_path, _compile(tmp_path))

    result = cli_main(
        [
            "blueprint",
            "dispatch",
            "--root",
            str(tmp_path),
            "--packet-file",
            packet_ref,
            "--worker-id",
            "worker-a",
            "--capability-health-file",
            ".omo/workers/runs/health.json",
        ]
    )

    captured = capsys.readouterr()
    output = json.loads(captured.out)
    assert result != 0
    assert output["ok"] is False
    assert output["error"] == "controller_approval_required"
    assert "Traceback" not in captured.err


def test_cli_rejects_absolute_artifact_reference_as_json_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _workspace(tmp_path)

    result = cli_main(
        [
            "blueprint",
            "compile",
            "--root",
            str(tmp_path),
            "--bet-id",
            BET_ID,
            "--task-id",
            TASK_ID,
            "--spec-ref",
            SPEC_REF,
            "--spec-version",
            SPEC_VERSION,
            "--expires-at",
            "2026-08-15T00:00:00+00:00",
            "--packet-file",
            str(tmp_path / "outside.json"),
        ]
    )

    assert result != 0
    assert json.loads(capsys.readouterr().out) == {
        "error": "blueprint_command_failed",
        "ok": False,
    }
    assert not (tmp_path / "outside.json").exists()


def test_cli_observe_and_execute_input_ack_never_claim_model_success(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _workspace(tmp_path)
    _dispatch_authority(tmp_path)
    runner = tmp_path / "input-only-runner"
    runner.write_text(
        '#!/bin/sh\nprintf \'{"transport": "accepted"}\' > "$2"\n',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    registry_path = tmp_path / ".omo" / "_truth" / "registry" / "workers.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["workers"][0]["transports"]["cli_prompt"]["command"] = str(runner)
    registry_path.write_text(
        yaml.safe_dump(registry, sort_keys=False), encoding="utf-8"
    )
    _commit_baseline(tmp_path)
    compiled = _compile(tmp_path)
    packet_ref = _write_cli_packet(tmp_path, compiled)
    dispatched = BlueprintControlService(tmp_path).dispatch_packet(
        compiled,
        worker_id="worker-a",
        capability_health=_health(),
        now="2026-08-14T10:00:00+00:00",
    )

    observed = cli_main(
        [
            "blueprint",
            "observe",
            "--root",
            str(tmp_path),
            "--dispatch-file",
            str(dispatched["dispatch_path"]),
        ]
    )
    observed_output = json.loads(capsys.readouterr().out)
    assert observed == 0
    assert observed_output["state"] == "transport_accepted"

    def supervisor(self, *, action, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        if action == "start":
            return _supervisor_start_receipt(tmp_path, compiled, dispatched)
        return {
            **_supervisor_start_receipt(tmp_path, compiled, dispatched),
            "ok": False,
            "stage": "worker_show",
            "reason": "worker_not_settled",
            "residual_resources": ["terminal-001"],
        }

    monkeypatch.setattr(BlueprintControlService, "_default_supervisor", supervisor)

    executed = cli_main(
        [
            "blueprint",
            "execute",
            "--root",
            str(tmp_path),
            "--packet-file",
            packet_ref,
            "--dispatch-file",
            str(dispatched["dispatch_path"]),
            "--candidate-file",
            ".omo/workers/runs/blueprint-candidate.json",
            "--approval-ref",
            ".omo/workers/runs/approval.yaml",
            "--supervised",
            "--timeout-seconds",
            "5",
        ]
    )
    executed_output = json.loads(capsys.readouterr().out)
    assert executed == 0
    assert executed_output["ok"] is True
    assert executed_output["state"] == "awaiting_human_action"
    assert executed_output["human_action_required"] is True
    assert not (tmp_path / ".omo/workers/runs/blueprint-candidate.json").exists()
    assert "EvidenceRecorded" not in [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]

    collected = cli_main(
        [
            "blueprint",
            "collect",
            "--root",
            str(tmp_path),
            "--packet-file",
            packet_ref,
            "--dispatch-file",
            str(dispatched["dispatch_path"]),
            "--candidate-file",
            ".omo/workers/runs/blueprint-candidate.json",
            "--timeout-seconds",
            "5",
        ]
    )
    collected_output = json.loads(capsys.readouterr().out)
    assert collected != 0
    assert collected_output["ok"] is False
    assert collected_output["state"] == "awaiting_human_action"
    assert collected_output["candidate_collected"] is False
    assert not (tmp_path / ".omo/workers/runs/blueprint-candidate.json").exists()


def test_cli_verifier_reject_and_rollback_mismatch_never_report_success(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, compiled, dispatched = _dispatched_repo(tmp_path)
    packet_ref = _write_cli_packet(tmp_path, compiled)

    def runner(*, workspace_root, receipt_path, on_process_started, **_kwargs):  # noqa: ANN001, ANN202
        on_process_started()
        target = workspace_root / "src" / "omo" / "blueprint_control.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        receipt_path.write_text(
            json.dumps(_adapter_receipt(workspace_root)), encoding="utf-8"
        )
        return {"returncode": 0}

    projection = service.execute_and_collect(compiled, dispatched, runner=runner)
    dispatch_ref = str(dispatched["dispatch_path"])
    candidate_ref = ".omo/workers/runs/blueprint-candidate.json"
    (tmp_path / candidate_ref).write_text(json.dumps(projection), encoding="utf-8")

    with monkeypatch.context() as reject_context:
        reject_context.setattr(
            BlueprintControlService,
            "_default_verifier",
            staticmethod(
                lambda **_kwargs: {"returncode": 1, "stdout": b"explicit reject"}
            ),
        )
        rejected = cli_main(
            [
                "blueprint",
                "verify",
                "--root",
                str(tmp_path),
                "--packet-file",
                packet_ref,
                "--dispatch-file",
                dispatch_ref,
                "--candidate-file",
                candidate_ref,
                "--timeout-seconds",
                "5",
            ]
        )
    rejected_output = json.loads(capsys.readouterr().out)
    assert rejected != 0
    assert rejected_output["ok"] is False
    assert rejected_output["error"] == "verification_rejected"
    assert "WorkflowVerified" not in [
        event["event_type"] for event in WorkflowMeshStore(tmp_path / ".omo").events()
    ]

    # A fresh candidate with a tampered rollback digest must remain a non-success.
    mismatch_root = tmp_path / "mismatch"
    mismatch_root.mkdir()
    service, compiled, dispatched = _dispatched_repo(mismatch_root)
    mismatch_projection = service.execute_and_collect(
        compiled, dispatched, runner=runner
    )
    mismatch_candidate_ref = ".omo/workers/runs/blueprint-candidate.json"
    projection_path = mismatch_root / mismatch_candidate_ref
    projection_path.write_text(json.dumps(mismatch_projection), encoding="utf-8")
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    projection["patch_digest"] = "sha256:" + "0" * 64
    projection_path.write_text(json.dumps(projection), encoding="utf-8")

    mismatch = cli_main(
        [
            "blueprint",
            "rollback",
            "--root",
            str(mismatch_root),
            "--dispatch-file",
            str(dispatched["dispatch_path"]),
            "--candidate-file",
            mismatch_candidate_ref,
        ]
    )
    mismatch_output = json.loads(capsys.readouterr().out)
    assert mismatch != 0
    assert mismatch_output["ok"] is False
    assert mismatch_output["state"] == "rollback_unconfirmed"
