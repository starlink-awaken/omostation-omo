from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from omo.blueprint_control import BlueprintControlError, BlueprintControlService
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
    ledger.write_text(yaml.safe_dump({"bets": [bet]}, sort_keys=False), encoding="utf-8")

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
        "evidence_required": ["focused tests", "diff check"],
        "deliverables": ["src/omo/blueprint_control.py"],
        "test_plan": ["pytest -q"],
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


def test_compile_is_deterministic_and_contains_governed_contract(tmp_path: Path) -> None:
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
    assert first.packet["assignment"]["required_capabilities"] == task["required_capabilities"]
    assert first.packet["scope"]["non_goals"] == bet["non_goals"]
    assert first.packet["acceptance"]["evidence_requirements"] == task["evidence_required"]
    assert first.packet["acceptance"]["done_when"] == bet["done_when"]
    assert first.packet["acceptance"]["verify_commands"] == ["pytest -q"]


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


def test_dispatch_records_exact_mesh_order_and_transport_only_state(tmp_path: Path) -> None:
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
