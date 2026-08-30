import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import omo.worker_lifecycle as worker_lifecycle_mod
import omo.workflow.core as workflow_core_mod
import omo.workflow.lifecycle as workflow_lifecycle_mod
import omo.workflow_dispatch as workflow_dispatch_mod
from omo.workflow.lifecycle import start_run
from omo.workflow_mesh import (
    WorkflowMeshEventError,
    WorkflowMeshStore,
    new_workflow_event,
)


def _grant(run_id: str, step_run_ids: list[str]) -> dict:
    grant = {
        "admission_id": f"adm-{run_id}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "test",
        "step_run_ids": step_run_ids,
        "capabilities": ["execute"],
        "policy_digest": "policy-test",
        "issued_at": datetime.now(UTC).isoformat(),
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
    }
    grant["proof"] = hashlib.sha256(
        json.dumps(grant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return grant


def _admit(run_id: str, step_run_ids: list[str]) -> dict:
    grant = _grant(run_id, step_run_ids)
    return new_workflow_event("WorkflowAdmitted", run_id, payload={"admission": grant, **grant})


def _agent_workflow_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    for relative in ("runs", "locks", "ledger", ".omo"):
        (tmp_path / relative).mkdir()
    monkeypatch.setattr(workflow_core_mod, "WORKSPACE", tmp_path)
    monkeypatch.setattr(workflow_lifecycle_mod, "WORKSPACE", tmp_path)
    return {
        "runner": {
            "run_state_dir": "runs",
            "lock_state_dir": "locks",
            "ledger_path": "ledger/events.jsonl",
        },
        "workflows": [
            {
                "id": "test-workflow",
                "title": "Test",
                "purpose": "test",
                "agents": {"test-agent": {"actor": "tester"}},
                "allowed_lanes": [],
                "lock_scopes": [],
                "phases": {},
            }
        ],
        "agent_profiles": {
            "test-agent": {
                "id": "test-agent",
                "actor": "tester",
                "allowed_workflows": ["*"],
            }
        },
    }


def _agent_workflow_context() -> dict[str, str]:
    return {
        "actor": "tester",
        "profile": "test-agent",
        "project": "",
        "format": "openspec",
        "source_file": "",
        "run_id": "",
    }


def _prepared_agent_workflow_identity() -> dict[str, Any]:
    spec_binding = {
        "spec_ref": "repo://docs/spec.md",
        "spec_version": "1.0.0",
        "content_digest": "sha256:" + "1" * 64,
        "decision_ref": "decision://accepted/BET-BOUND",
    }
    requirements = [
        {"capability_id": "skill:git-discipline", "operation": "load", "effect": "read_only"},
        {"capability_id": "workflow:bet-execution", "operation": "load", "effect": "read_only"},
    ]
    packet = {
        "packet_id": "WP-BET-BOUND",
        "schema_version": "work-packet/v2",
        "bet_id": "BET-BOUND",
        "spec_binding": spec_binding,
        "capability_requirements": requirements,
    }
    canonical = json.dumps(requirements, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "spec_binding": spec_binding,
        "work_packet": packet,
        "work_packet_hash": "sha256:" + "2" * 64,
        "capability_requirements_digest": "sha256:" + hashlib.sha256(canonical.encode()).hexdigest(),
    }


def _agent_workflow_preflight(run_id: str, identity: dict[str, Any]) -> dict[str, Any]:
    return {
        "requirements_digest": identity["capability_requirements_digest"],
        "binding": {
            "correlation_id": run_id,
            "workflow_run_id": run_id,
            "packet_id": identity["work_packet"]["packet_id"],
            "packet_hash": identity["work_packet_hash"],
            "assignment_id": f"preflight:{run_id}:assignment",
            "dispatch_id": f"preflight:{run_id}:dispatch",
            "actor_id": "actor:test",
            "delivery_attempt_id": "attempt:test",
        },
        "receipts": [
            {
                "capability_id": requirement["capability_id"],
                "source_digest": "sha256:" + str(index + 3) * 64,
                "receipt_digest": "sha256:" + str(index + 5) * 64,
            }
            for index, requirement in enumerate(identity["work_packet"]["capability_requirements"])
        ],
        "invoked": False,
        "value_indicator_policy": False,
    }


def _start_agent_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    registry = _agent_workflow_registry(tmp_path, monkeypatch)
    identity = _prepared_agent_workflow_identity()
    monkeypatch.setattr(workflow_lifecycle_mod, "_prepare_bet_execution", lambda _bet_id: deepcopy(identity))
    record = start_run(
        registry,
        registry["workflows"][0],
        _agent_workflow_context(),
        "exact pre-spawn admission",
        False,
        False,
        bet_id="BET-BOUND",
        start_preflight=lambda run_id, prepared: _agent_workflow_preflight(run_id, prepared),
    )
    return record, identity, registry


def _stub_successful_closeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from omo.workflow import diagnostics as diagnostics_mod

    monkeypatch.setattr(workflow_lifecycle_mod, "heartbeat_run", lambda *_args, **_kwargs: {"count": 0})
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
    monkeypatch.setattr(workflow_lifecycle_mod, "_run_closeout_side_effects", lambda *_args, **_kwargs: None)


def _dispatch_and_start_exact(
    tmp_path: Path,
    record: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    run_id = record["run_id"]
    store = WorkflowMeshStore(tmp_path / ".omo")
    grant = store.snapshot(run_id)["admission"]
    step_run_id = grant["step_run_ids"][0]
    worker_id = "agent-workflow"
    dispatch_id = grant["request_identity"]["dispatch_id"]
    instruction_binding = {
        "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
        "instruction_version": "blueprint-agent-instruction-pack/v1",
        "content_digest": "sha256:" + "b" * 64,
        "instruction_profile": "executor",
    }
    origin_proof = worker_lifecycle_mod.new_worker_ack_origin_proof()
    worker_lifecycle_mod.record_step_dispatch(
        tmp_path / ".omo",
        workflow_run_id=run_id,
        trace_id=run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=grant["admission_id"],
        policy_digest=grant["policy_digest"],
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=instruction_binding,
        ack_origin_proof=origin_proof,
    )
    worker_lifecycle_mod.acknowledge_worker(
        tmp_path / ".omo",
        workflow_run_id=run_id,
        trace_id=run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
        step_run_id=step_run_id,
        admission_id=grant["admission_id"],
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=instruction_binding,
        ack_decision="proceed",
        origin_proof=origin_proof,
    )
    store.append(
        new_workflow_event(
            "StepStarted",
            run_id,
            payload={
                "step_run_id": step_run_id,
                "step_name": "execute",
                "admission_id": grant["admission_id"],
            },
        )
    )
    return grant, {
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "step_run_id": step_run_id,
        "admission_id": grant["admission_id"],
        "origin_proof": origin_proof,
    }


def _interrupt_exact_start_after_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, Any], dict[str, Any]]:
    registry = _agent_workflow_registry(tmp_path, monkeypatch)
    identity = _prepared_agent_workflow_identity()
    monkeypatch.setattr(workflow_lifecycle_mod, "_prepare_bet_execution", lambda _bet_id: deepcopy(identity))

    def interrupt_after_request(*_args, **_kwargs):
        raise KeyboardInterrupt("simulated interruption after WorkflowRequested")

    monkeypatch.setattr(workflow_lifecycle_mod, "admit_agent_workflow_start", interrupt_after_request)
    with pytest.raises(KeyboardInterrupt, match="simulated interruption"):
        start_run(
            registry,
            registry["workflows"][0],
            _agent_workflow_context(),
            "interrupted exact start",
            False,
            False,
            bet_id="BET-BOUND",
            start_preflight=lambda run_id, prepared: _agent_workflow_preflight(run_id, prepared),
        )
    run_path = next((tmp_path / "runs").glob("*.yaml"))
    _path, record = workflow_lifecycle_mod.read_run(registry, run_path.stem)
    assert record["status"] == "active"
    assert WorkflowMeshStore(tmp_path / ".omo").snapshot(record["run_id"])["state"] == "planned"
    return record, registry


def test_workflow_mesh_store_projects_lifecycle_and_is_idempotent(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    requested = new_workflow_event(
        "WorkflowRequested",
        "run-1",
        producer="ecos",
        payload={"workflow": "mesh-test", "task_id": "task-1"},
    )
    started = new_workflow_event(
        "StepStarted",
        "run-1",
        producer="runtime",
        payload={"step_run_id": "step-1", "admission_id": "adm-run-1"},
    )
    succeeded = new_workflow_event("WorkflowSucceeded", "run-1", producer="runtime", payload={"step_count": 1})

    store.append(requested)
    store.append(_admit("run-1", ["step-1"]))
    store.append(
        new_workflow_event(
            "StepDispatched",
            "run-1",
            producer="runtime",
            payload={"step_run_id": "step-1", "admission_id": "adm-run-1"},
        )
    )
    store.append(started)
    store.append(succeeded)
    assert store.append(succeeded) == succeeded
    assert store.snapshot("run-1")["state"] == "succeeded"
    assert store.snapshot("run-1")["event_count"] == 5
    assert store.snapshot("run-1")["metadata"]["workflow"] == "mesh-test"


def test_scene_binding_is_projected_and_immutable(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    run_id = "run-scene-binding"
    binding = {
        "scene_id": "official-document-review",
        "journey_id": "draft-to-approval",
        "outcome_metric": "review_cycle_time",
    }
    store.append(new_workflow_event("WorkflowRequested", run_id, scene_binding=binding))

    assert store.snapshot(run_id)["scene_binding"] == binding

    changed_binding = {**binding, "outcome_metric": "unapproved-change"}
    with pytest.raises(WorkflowMeshEventError, match="cannot change"):
        store.append(new_workflow_event("WorkflowFailed", run_id, scene_binding=changed_binding))


def test_scene_binding_requires_all_business_identifiers(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    with pytest.raises(WorkflowMeshEventError, match="missing fields"):
        store.append(
            new_workflow_event(
                "WorkflowRequested",
                "run-incomplete-scene",
                scene_binding={"scene_id": "official-document-review"},
            )
        )


def test_successful_run_can_be_verified_merged_and_closed(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    grant = _grant("run-lifecycle", ["step-1"])
    for event_type in (
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
        "StepStarted",
        "WorkflowSucceeded",
        "EvidenceRecorded",
        "WorkflowVerified",
        "PRMerged",
        "WorkflowClosed",
    ):
        payload = (
            {"evidence_id": "evidence-1", "kind": "test", "uri": "memory://evidence-1"}
            if event_type == "EvidenceRecorded"
            else (
                {"admission": grant, **grant}
                if event_type == "WorkflowAdmitted"
                else (
                    {
                        "step_run_id": "step-1",
                        "step_name": "compile",
                        "admission_id": grant["admission_id"],
                    }
                    if event_type in {"StepDispatched", "StepStarted"}
                    else {}
                )
            )
        )
        store.append(new_workflow_event(event_type, "run-lifecycle", payload=payload))

    snapshot = store.snapshot("run-lifecycle")
    assert snapshot["state"] == "closed"
    assert snapshot["last_event_type"] == "WorkflowClosed"
    assert snapshot["event_count"] == 9


def test_succeeded_candidate_can_enter_compensation_and_close_cancelled(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    run_id = "run-succeeded-compensation"
    step_run_id = f"{run_id}:step-1"
    grant = _grant(run_id, [step_run_id])
    store.append(new_workflow_event("WorkflowRequested", run_id))
    store.append(_admit(run_id, [step_run_id]))
    step_context = {
        "step_run_id": step_run_id,
        "admission_id": grant["admission_id"],
    }
    store.append(new_workflow_event("StepDispatched", run_id, payload=step_context))
    store.append(new_workflow_event("StepStarted", run_id, payload=step_context))
    store.append(new_workflow_event("WorkflowSucceeded", run_id))
    store.append(new_workflow_event("CompensationStarted", run_id, payload=step_context))
    store.append(new_workflow_event("WorkflowRecovered", run_id))
    store.append(new_workflow_event("WorkflowCancelled", run_id))
    store.append(new_workflow_event("WorkflowClosed", run_id))

    assert store.snapshot(run_id)["state"] == "closed"


def test_step_run_checkpoint_and_evidence_are_queryable(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    events = [
        ("WorkflowRequested", {}),
        ("WorkflowAdmitted", {}),
        ("StepDispatched", {"step_run_id": "step-1", "step_name": "compile"}),
        (
            "StepStarted",
            {"step_run_id": "step-1", "step_name": "compile", "attempt": 2},
        ),
        (
            "CheckpointSaved",
            {
                "step_run_id": "step-1",
                "step_name": "compile",
                "checkpoint_id": "cp-1",
                "next_turn": 3,
                "attempt": 2,
            },
        ),
        ("WorkflowSucceeded", {}),
        ("EvidenceRecorded", {"evidence_id": "ev-1", "sha256": "abc"}),
    ]
    grant = _grant("run-query", ["step-1"])
    for event_type, payload in events:
        if event_type == "WorkflowAdmitted":
            payload = {"admission": grant, **grant}
        elif payload.get("step_run_id"):
            payload["admission_id"] = grant["admission_id"]
        store.append(new_workflow_event(event_type, "run-query", payload=payload))

    assert (
        store.step_snapshot("run-query", "step-1")["checkpoint"]["checkpoint_id"]  # type: ignore[reportOptionalSubscript]
        == "cp-1"
    )  # type: ignore[reportOptionalSubscript]
    assert store.evidence_snapshot("run-query", "ev-1")["sha256"] == "abc"  # type: ignore[reportOptionalSubscript]


def test_verified_requires_evidence(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", "run-no-evidence"))
    grant = _grant("run-no-evidence", ["step-1"])
    store.append(_admit("run-no-evidence", ["step-1"]))
    store.append(
        new_workflow_event(
            "StepDispatched",
            "run-no-evidence",
            payload={"step_run_id": "step-1", "admission_id": grant["admission_id"]},
        )
    )
    store.append(
        new_workflow_event(
            "StepStarted",
            "run-no-evidence",
            payload={"step_run_id": "step-1", "admission_id": grant["admission_id"]},
        )
    )
    store.append(new_workflow_event("WorkflowSucceeded", "run-no-evidence"))
    with pytest.raises(WorkflowMeshEventError, match="EvidenceRecorded"):
        store.append(new_workflow_event("WorkflowVerified", "run-no-evidence"))


def test_failed_backend_can_recover_with_explicit_event(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", "run-recovery"))
    store.append(_admit("run-recovery", ["step-1"]))
    store.append(new_workflow_event("BackendUnavailable", "run-recovery"))
    store.append(new_workflow_event("WorkflowRecovered", "run-recovery"))
    store.append(new_workflow_event("WorkflowSucceeded", "run-recovery"))

    assert store.snapshot("run-recovery")["state"] == "succeeded"


def test_append_order_wins_over_late_timestamp(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    requested = new_workflow_event("WorkflowRequested", "run-order")
    grant = _grant("run-order", ["step-1"])
    admitted = _admit("run-order", ["step-1"])
    failed = new_workflow_event("WorkflowFailed", "run-order")
    late_step = new_workflow_event(
        "StepStarted",
        "run-order",
        payload={"step_run_id": "step-1", "admission_id": grant["admission_id"]},
    )
    late_step["occurred_at"] = "1970-01-01T00:00:00+00:00"
    store.append(requested)
    store.append(admitted)
    store.append(failed)
    with pytest.raises(WorkflowMeshEventError, match="terminal"):
        store.append(late_step)


def test_terminal_run_rejects_later_event(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", "run-2"))
    store.append(new_workflow_event("WorkflowFailed", "run-2"))
    with pytest.raises(WorkflowMeshEventError, match="terminal"):
        store.append(new_workflow_event("StepStarted", "run-2"))


def test_unadmitted_step_is_rejected(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", "run-unadmitted"))
    with pytest.raises(WorkflowMeshEventError, match=r"invalid transition planned -> dispatched"):
        store.append(
            new_workflow_event(
                "StepDispatched",
                "run-unadmitted",
                payload={"step_run_id": "run-unadmitted:step-1"},
            )
        )


def test_retry_and_compensation_events_preserve_step_truth(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    run_id = "run-compensation"
    step_id = f"{run_id}:step-1"
    grant = _grant(run_id, [step_id])
    store.append(new_workflow_event("WorkflowRequested", run_id))
    store.append(_admit(run_id, [step_id]))
    store.append(
        new_workflow_event(
            "StepDispatched",
            run_id,
            payload={"step_run_id": step_id, "admission_id": grant["admission_id"]},
        )
    )
    store.append(
        new_workflow_event(
            "StepStarted",
            run_id,
            payload={"step_run_id": step_id, "admission_id": grant["admission_id"]},
        )
    )
    store.append(
        new_workflow_event(
            "StepRetryScheduled",
            run_id,
            payload={"step_run_id": step_id, "admission_id": grant["admission_id"]},
        )
    )
    store.append(
        new_workflow_event(
            "CompensationStarted",
            run_id,
            payload={"step_run_id": step_id, "admission_id": grant["admission_id"]},
        )
    )
    store.append(
        new_workflow_event(
            "StepFailed",
            run_id,
            payload={"step_run_id": step_id, "admission_id": grant["admission_id"]},
        )
    )
    store.append(new_workflow_event("WorkflowFailed", run_id))
    snapshot = store.snapshot(run_id)
    assert snapshot["state"] == "failed"
    assert snapshot["step_runs"][step_id]["last_event_type"] == "StepFailed"


def test_unknown_event_is_rejected(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    with pytest.raises(WorkflowMeshEventError, match="Unknown"):
        store.append(new_workflow_event("NotARealEvent", "run-3"))


def test_idempotency_key_is_authoritative_across_event_ids(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    first = new_workflow_event(
        "WorkflowRequested",
        "run-idempotent",
        idempotency_key="run-idempotent:requested",
    )
    duplicate = new_workflow_event(
        "WorkflowRequested",
        "run-idempotent",
        idempotency_key="run-idempotent:requested",
    )
    store.append(first)
    with pytest.raises(WorkflowMeshEventError, match="Conflicting duplicate"):
        store.append(duplicate)


def test_agent_workflow_mesh_bridge_start_event(tmp_path, monkeypatch):
    """Phase 1b: Agent Workflow start emits Mesh event."""

    from omo.workflow.mesh_agent_events import emit_workflow_mesh_event

    # Simulate workspace with minimal OMO structure
    omo_dir = tmp_path / ".omo"
    omo_dir.mkdir()

    # Test direct emission
    result = emit_workflow_mesh_event(
        "AgentWorkflowStarted",
        "test-run-123",
        {"workflow_id": "test-wf", "actor": "test-user"},
        workspace=tmp_path,
    )
    assert result is True, "Should succeed in emitting event"

    # Verify event was stored
    event_file = omo_dir / "_knowledge" / "workflow-mesh" / "events.jsonl"
    assert event_file.exists()
    content = event_file.read_text()
    assert "AgentWorkflowStarted" in content
    assert "test-run-123" in content


def test_agent_workflow_mesh_bridge_with_scene_binding(tmp_path, monkeypatch):
    """Phase 4: Agent Workflow start emits Mesh event with scene_binding."""
    from omo.workflow.mesh_agent_events import emit_workflow_mesh_event
    from omo.workflow_mesh import WorkflowMeshStore

    omo_dir = tmp_path / ".omo"
    omo_dir.mkdir()

    result = emit_workflow_mesh_event(
        "AgentWorkflowStarted",
        "test-run-scene-001",
        {"workflow_id": "test-wf", "actor": "test-user"},
        workspace=tmp_path,
        scene_binding={
            "scene_id": "scene-test",
            "journey_id": "journey-test",
            "outcome_metric": "metric-test",
        },
    )
    assert result is True

    store = WorkflowMeshStore(omo_dir)
    events = store.events()
    assert len(events) >= 1
    assert events[-1]["payload"]["scene_binding"]["scene_id"] == "scene-test"
    assert events[-1]["payload"]["scene_binding"]["journey_id"] == "journey-test"
    assert events[-1]["payload"]["scene_binding"]["outcome_metric"] == "metric-test"


def test_agent_workflow_start_persists_exact_admission_before_return(tmp_path, monkeypatch):
    from omo.workflow.mesh_agent_events import emit_workflow_mesh_event

    record, _identity, _registry = _start_agent_workflow(tmp_path, monkeypatch)
    omo_dir = tmp_path / ".omo"
    store = WorkflowMeshStore(omo_dir)
    snapshot = store.snapshot(record["run_id"])

    assert snapshot["state"] == "admitted"
    grant = snapshot["admission"]
    identity = grant["request_identity"]
    requirements = record["work_packet"]["capability_requirements"]
    assert "capability_requirements" not in record
    assert identity["capability_requirements"] == requirements
    assert identity["capability_requirements_digest"] == record["capability_requirements_digest"]
    assert identity["packet_id"] == record["work_packet"]["packet_id"]
    assert identity["packet_hash"] == record["work_packet_hash"]
    assert identity["workflow_run_id"] == record["run_id"]
    assert identity["actor_id"] == record["capability_preflight"]["binding"]["actor_id"]
    assert identity["delivery_attempt_id"] == record["capability_preflight"]["binding"]["delivery_attempt_id"]
    assert grant["proof"]

    events = store.events()
    assert [event["event_type"] for event in events] == ["WorkflowRequested", "WorkflowAdmitted"]
    assert not any(event["event_type"] == "StepDispatched" for event in events)

    emit_workflow_mesh_event(
        "AgentWorkflowClosed",
        record["run_id"],
        {"status": "succeeded", "ok": True, "capabilities": ["forged-closeout"]},
        workspace=tmp_path,
    )
    admissions = [event for event in store.events() if event["event_type"] == "WorkflowAdmitted"]
    assert len(admissions) == 1
    persisted = admissions[0]["payload"]["admission"]
    assert persisted["admission_id"] == grant["admission_id"]
    assert persisted["proof"] == grant["proof"]


def test_exact_agent_workflow_persisted_start_dispatch_close(tmp_path, monkeypatch):
    record, _identity, registry = _start_agent_workflow(tmp_path, monkeypatch)
    grant, completion_context = _dispatch_and_start_exact(tmp_path, record)
    store = WorkflowMeshStore(tmp_path / ".omo")
    worker_lifecycle_mod.record_worker_completion(
        tmp_path / ".omo",
        **completion_context,
        result_digest="sha256:" + "c" * 64,
    )
    _stub_successful_closeout(monkeypatch)

    report = workflow_lifecycle_mod.closeout_run(
        registry,
        record["run_id"],
        "ok",
        ["exact lifecycle"],
        [],
        False,
        False,
        False,
        False,
    )

    events = store.events()
    assert report["ok"] is True
    assert report["run"]["status"] == "ok"
    assert [event["event_type"] for event in events] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
        "WorkerAcknowledged",
        "StepStarted",
        "WorkflowSucceeded",
        "WorkflowClosed",
    ]
    assert store.snapshot(record["run_id"])["state"] == "closed"
    admissions = [event for event in events if event["event_type"] == "WorkflowAdmitted"]
    assert len(admissions) == 1
    assert admissions[0]["payload"]["admission"]["admission_id"] == grant["admission_id"]
    assert admissions[0]["payload"]["admission"]["proof"] == grant["proof"]


def test_exact_agent_workflow_rejects_forged_generic_success(tmp_path, monkeypatch):
    record, _identity, _registry = _start_agent_workflow(tmp_path, monkeypatch)
    _grant, _completion_context = _dispatch_and_start_exact(tmp_path, record)
    store = WorkflowMeshStore(tmp_path / ".omo")

    with pytest.raises(WorkflowMeshEventError, match="authenticated worker completion append"):
        store.append(
            new_workflow_event(
                "WorkflowSucceeded",
                record["run_id"],
                payload={"completed_step_run_id": f"{record['run_id']}:execute"},
            )
        )


def test_exact_agent_workflow_rejects_raw_deterministic_completion_receipt(tmp_path, monkeypatch):
    record, _identity, _registry = _start_agent_workflow(tmp_path, monkeypatch)
    grant, completion_context = _dispatch_and_start_exact(tmp_path, record)
    store = WorkflowMeshStore(tmp_path / ".omo")
    worker = store.snapshot(record["run_id"])["worker"]
    receipt = {
        "status": "succeeded",
        "workflow_run_id": record["run_id"],
        "admission_id": grant["admission_id"],
        "step_run_id": completion_context["step_run_id"],
        "dispatch_id": completion_context["dispatch_id"],
        "worker_id": completion_context["worker_id"],
        "ack_origin_proof_digest": worker["ack_origin_proof_digest"],
        "completion_origin_commitment": "sha256:" + "e" * 64,
        "result_digest": "sha256:" + "d" * 64,
    }
    receipt["receipt_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    forged = new_workflow_event(
        "WorkflowSucceeded",
        record["run_id"],
        producer="worker",
        payload={"worker_completion_receipt": receipt},
    )

    with pytest.raises(WorkflowMeshEventError, match="authenticated worker completion append"):
        store.append(forged)
    assert store.snapshot(record["run_id"])["state"] == "running"


@pytest.mark.parametrize("entrypoint", ["direct", "closeout"])
@pytest.mark.parametrize("status", ["blocked", "failed"])
def test_interrupted_exact_request_unsuccessful_close_is_honest_terminal(
    tmp_path,
    monkeypatch,
    entrypoint,
    status,
):
    record, registry = _interrupt_exact_start_after_request(tmp_path, monkeypatch)
    if entrypoint == "closeout":
        _stub_successful_closeout(monkeypatch)
        result = workflow_lifecycle_mod.closeout_run(
            registry,
            record["run_id"],
            status,
            ["interrupted request close"],
            [],
            False,
            False,
            False,
            False,
        )["run"]
    else:
        result = workflow_lifecycle_mod.close_run(
            registry,
            record["run_id"],
            status,
            ["interrupted request close"],
            True,
        )

    store = WorkflowMeshStore(tmp_path / ".omo")
    assert result["status"] == status
    assert store.snapshot(record["run_id"])["state"] == "closed"
    assert [event["event_type"] for event in store.events()] == [
        "WorkflowRequested",
        "WorkflowCancelled",
        "WorkflowClosed",
    ]


@pytest.mark.parametrize("entrypoint", ["direct", "closeout"])
def test_interrupted_exact_request_ok_close_rejects_stably(tmp_path, monkeypatch, entrypoint):
    record, registry = _interrupt_exact_start_after_request(tmp_path, monkeypatch)
    if entrypoint == "closeout":
        _stub_successful_closeout(monkeypatch)
        close = lambda: workflow_lifecycle_mod.closeout_run(
            registry,
            record["run_id"],
            "ok",
            ["must have admission and completion"],
            [],
            False,
            False,
            False,
            False,
        )
    else:
        close = lambda: workflow_lifecycle_mod.close_run(
            registry,
            record["run_id"],
            "ok",
            ["must have admission and completion"],
            True,
        )

    with pytest.raises(Exception, match="EXACT_WORKFLOW_ADMISSION_NOT_PERSISTED"):
        close()

    _path, persisted = workflow_lifecycle_mod.read_run(registry, record["run_id"])
    store = WorkflowMeshStore(tmp_path / ".omo")
    assert persisted["status"] == "active"
    assert store.snapshot(record["run_id"])["state"] == "planned"
    assert [event["event_type"] for event in store.events()] == ["WorkflowRequested"]


def test_exact_agent_workflow_admitted_only_ok_closeout_rejects_without_synthetic_events(tmp_path, monkeypatch):
    from omo.workflow import mesh_agent_events as mesh_agent_events_mod

    record, _identity, registry = _start_agent_workflow(tmp_path, monkeypatch)
    store = WorkflowMeshStore(tmp_path / ".omo")
    _stub_successful_closeout(monkeypatch)

    def reject_legacy_chain(*_args, **_kwargs):
        raise AssertionError("exact closeout must not synthesize another admission")

    monkeypatch.setattr(mesh_agent_events_mod, "_emit_admission_chain", reject_legacy_chain)

    with pytest.raises(Exception, match="EXACT_WORKFLOW_EXECUTION_NOT_PERSISTED"):
        workflow_lifecycle_mod.closeout_run(
            registry,
            record["run_id"],
            "ok",
            ["exact lifecycle"],
            [],
            False,
            False,
            False,
            False,
        )

    _path, run = workflow_lifecycle_mod.read_run(registry, record["run_id"])
    assert run["status"] == "active"
    assert store.snapshot(record["run_id"])["state"] == "admitted"
    assert [event["event_type"] for event in store.events()] == ["WorkflowRequested", "WorkflowAdmitted"]


@pytest.mark.parametrize("status", ["blocked", "failed"])
def test_exact_agent_workflow_direct_unsuccessful_close_is_honest_terminal(tmp_path, monkeypatch, status):
    record, _identity, registry = _start_agent_workflow(tmp_path, monkeypatch)
    store = WorkflowMeshStore(tmp_path / ".omo")
    grant = store.snapshot(record["run_id"])["admission"]

    closed = workflow_lifecycle_mod.close_run(
        registry,
        record["run_id"],
        status,
        ["honest unsuccessful close"],
        True,
    )

    events = store.events()
    assert closed["status"] == status
    assert [event["event_type"] for event in events] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "WorkflowCancelled",
        "WorkflowClosed",
    ]
    assert store.snapshot(record["run_id"])["state"] == "closed"
    assert not any(event["event_type"] in {"StepDispatched", "StepStarted"} for event in events)
    assert store.snapshot(record["run_id"])["admission"]["admission_id"] == grant["admission_id"]


def test_exact_agent_workflow_direct_ok_close_rejects_admitted_only_run(tmp_path, monkeypatch):
    record, _identity, registry = _start_agent_workflow(tmp_path, monkeypatch)
    store = WorkflowMeshStore(tmp_path / ".omo")

    with pytest.raises(Exception, match="EXACT_WORKFLOW_EXECUTION_NOT_PERSISTED"):
        workflow_lifecycle_mod.close_run(
            registry,
            record["run_id"],
            "ok",
            ["must not invent execution"],
            True,
        )

    _path, run = workflow_lifecycle_mod.read_run(registry, record["run_id"])
    assert run["status"] == "active"
    assert store.snapshot(record["run_id"])["state"] == "admitted"
    assert [event["event_type"] for event in store.events()] == ["WorkflowRequested", "WorkflowAdmitted"]


def test_exact_agent_workflow_unsuccessful_closed_run_cannot_be_reclosed_ok(tmp_path, monkeypatch):
    record, _identity, registry = _start_agent_workflow(tmp_path, monkeypatch)
    workflow_lifecycle_mod.close_run(
        registry,
        record["run_id"],
        "blocked",
        ["honest blocked close"],
        True,
    )

    with pytest.raises(Exception, match="EXACT_WORKFLOW_COMPLETION_RECEIPT_NOT_PERSISTED"):
        workflow_lifecycle_mod.close_run(
            registry,
            record["run_id"],
            "ok",
            ["must not promote blocked close"],
            True,
        )

    _path, persisted = workflow_lifecycle_mod.read_run(registry, record["run_id"])
    assert persisted["status"] == "blocked"
    assert WorkflowMeshStore(tmp_path / ".omo").snapshot(record["run_id"])["state"] == "closed"


def test_legacy_agent_workflow_real_start_close_remains_supported(tmp_path, monkeypatch):
    registry = _agent_workflow_registry(tmp_path, monkeypatch)
    record = start_run(
        registry,
        registry["workflows"][0],
        _agent_workflow_context(),
        "legacy lifecycle",
        False,
        False,
    )

    closed = workflow_lifecycle_mod.close_run(
        registry,
        record["run_id"],
        "ok",
        ["legacy close"],
        True,
    )

    store = WorkflowMeshStore(tmp_path / ".omo")
    assert closed["status"] == "ok"
    assert store.snapshot(record["run_id"])["state"] == "closed"
    assert [event["event_type"] for event in store.events()] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
        "StepStarted",
        "WorkflowSucceeded",
        "WorkflowClosed",
    ]


def test_exact_request_cannot_bypass_strict_admission_by_omitting_backend(tmp_path, monkeypatch):
    record, _identity, _registry = _start_agent_workflow(tmp_path, monkeypatch)
    persisted = WorkflowMeshStore(tmp_path / ".omo").events()
    requested = deepcopy(persisted[0])
    admitted = deepcopy(persisted[1])
    grant = admitted["payload"]["admission"]
    grant.pop("backend")
    grant["request_identity"]["actor_id"] = "actor:forged"
    grant["proof"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in grant.items() if key != "proof"},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

    forged_store = WorkflowMeshStore(tmp_path / "forged-exact")
    forged_store.append(requested)
    with pytest.raises(WorkflowMeshEventError, match="Agent Workflow admission"):
        forged_store.append(admitted)


def test_exact_agent_workflow_start_failure_closes_run_and_releases_locks(tmp_path, monkeypatch):
    registry = _agent_workflow_registry(tmp_path, monkeypatch)
    registry["workflows"][0]["lock_scopes"] = ["path:owned.py"]
    identity = _prepared_agent_workflow_identity()
    monkeypatch.setattr(workflow_lifecycle_mod, "_prepare_bet_execution", lambda _bet_id: deepcopy(identity))

    def reject_admission(*_args, **_kwargs):
        raise RuntimeError("forged exact admission")

    monkeypatch.setattr(workflow_lifecycle_mod, "admit_agent_workflow_start", reject_admission)

    with pytest.raises(Exception, match="WORKFLOW_MESH_ADMISSION_FAILED"):
        start_run(
            registry,
            registry["workflows"][0],
            _agent_workflow_context(),
            "reject exact admission",
            False,
            False,
            bet_id="BET-BOUND",
            start_preflight=lambda run_id, prepared: _agent_workflow_preflight(run_id, prepared),
        )

    run_paths = list((tmp_path / "runs").glob("*.yaml"))
    assert len(run_paths) == 1
    _run_path, run = workflow_lifecycle_mod.read_run(registry, run_paths[0].stem)
    assert run["status"] == "failed"
    assert run["released_locks"]
    assert any("WORKFLOW_MESH_ADMISSION_FAILED" in item for item in run["evidence"])
    assert list((tmp_path / "locks").glob("*.yaml")) == []
    store = WorkflowMeshStore(tmp_path / ".omo")
    assert store.snapshot(run["run_id"])["state"] == "closed"
    assert not any(event["event_type"] == "StepDispatched" for event in store.events())


def _mutate_agent_workflow_admission(record: dict[str, Any], mutation: str) -> None:
    requirements = record["work_packet"]["capability_requirements"]
    preflight = record["capability_preflight"]
    binding = preflight["binding"]
    if mutation == "missing_requirements":
        record["work_packet"].pop("capability_requirements")
    elif mutation == "reordered_requirements":
        record["work_packet"]["capability_requirements"] = list(reversed(requirements))
    elif mutation == "requirements_digest":
        record["capability_requirements_digest"] = "sha256:" + "9" * 64
    elif mutation == "packet_id":
        record["work_packet"]["packet_id"] = "WP-FORGED"
    elif mutation == "packet_hash":
        record["work_packet_hash"] = "sha256:" + "8" * 64
    elif mutation == "workflow_run_id":
        binding["workflow_run_id"] = "cross-run"
    elif mutation == "actor_id":
        binding["actor_id"] = "actor:forged"
    elif mutation == "delivery_attempt_id":
        binding["delivery_attempt_id"] = "attempt:forged"
    elif mutation == "receipt_order":
        preflight["receipts"] = list(reversed(preflight["receipts"]))
    elif mutation == "value_indicator_policy":
        preflight["value_indicator_policy"] = True
    else:  # pragma: no cover - the parameter list is the authority.
        raise AssertionError(mutation)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_requirements",
        "reordered_requirements",
        "requirements_digest",
        "packet_id",
        "packet_hash",
        "workflow_run_id",
        "actor_id",
        "delivery_attempt_id",
        "receipt_order",
        "value_indicator_policy",
    ],
)
def test_agent_workflow_start_rejects_forged_exact_identity_before_effects(
    tmp_path,
    monkeypatch,
    mutation,
):
    registry = _agent_workflow_registry(tmp_path, monkeypatch)
    prepared = _prepared_agent_workflow_identity()
    monkeypatch.setattr(workflow_lifecycle_mod, "_prepare_bet_execution", lambda _bet_id: deepcopy(prepared))

    counters = {"dispatch": 0, "subprocess": 0, "load": 0}

    def unexpected_dispatch(*_args, **_kwargs):
        counters["dispatch"] += 1

    def unexpected_subprocess(*_args, **_kwargs):
        counters["subprocess"] += 1

    def unexpected_load(*_args, **_kwargs):
        counters["load"] += 1

    monkeypatch.setattr(worker_lifecycle_mod, "record_step_dispatch", unexpected_dispatch)
    monkeypatch.setattr(workflow_lifecycle_mod.subprocess, "run", unexpected_subprocess)
    monkeypatch.setattr(workflow_dispatch_mod, "load_yaml", unexpected_load)

    real_admit = getattr(workflow_dispatch_mod, "admit_agent_workflow_start", None)

    def forged_admit(root, *, record, omo_dir=".omo"):
        _mutate_agent_workflow_admission(record, mutation)
        if real_admit is not None:
            return real_admit(root, record=record, omo_dir=omo_dir)
        return None

    monkeypatch.setattr(
        workflow_lifecycle_mod,
        "admit_agent_workflow_start",
        forged_admit,
        raising=False,
    )

    with pytest.raises(Exception):
        start_run(
            registry,
            registry["workflows"][0],
            _agent_workflow_context(),
            "reject forged admission",
            False,
            False,
            bet_id="BET-BOUND",
            start_preflight=lambda run_id, identity: _agent_workflow_preflight(run_id, identity),
        )

    assert counters == {"dispatch": 0, "subprocess": 0, "load": 0}
    events = WorkflowMeshStore(tmp_path / ".omo").events()
    assert not any(event["event_type"] == "StepDispatched" for event in events)
