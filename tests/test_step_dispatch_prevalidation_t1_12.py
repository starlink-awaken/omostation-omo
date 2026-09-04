"""Regression test for BET-Y1Q3-T1-12 WP-P1: OMO StepDispatched pre-validation.

Proves the workflow_mesh rejects StepDispatched when the persisted
admission has been tampered with between WorkflowAdmitted and StepDispatched.

Reference: docs/superpowers/specs/2026-08-24-exact-capability-binding-design.md
"BET-Y1Q3-T1-12 done_when: OMO 在 StepDispatched 前回验 persisted admitted state"

The implementation re-validates:
- admission_id (workflow_mesh.py line ~673)
- policy_digest (~line ~688)
- capability_requirements_digest (~line ~686)
- exact_request_identity fields (~line ~683-690)
- admission.proof (computed hash over admission body — catches tamper)

These checks already exist; this test pins the behavior so future regressions
are caught at CI time rather than at production canary.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from omo.workflow_mesh import (
    WorkflowMeshEventError,
    WorkflowMeshStore,
    new_workflow_event,
    worker_ack_origin_digest,
)


def _build_exact_dispatch_event(
    run_id: str,
    grant: dict,
    *,
    origin_proof: str,
) -> dict:
    """Construct a well-formed StepDispatched event with valid origin proof.

    Mirrors tests/test_worker_lifecycle_mesh.py::_raw_exact_dispatch_event
    """
    from tests.test_worker_lifecycle_mesh import (
        _binding,
        new_worker_ack_origin_proof,
    )

    identity = grant["request_identity"]
    payload = {
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": identity["bet_id"],
        "workflow_id": identity["workflow_id"],
        "dispatch_id": identity["dispatch_id"],
        "worker_id": "worker-t1-12-test",
        "step_run_id": grant["step_run_ids"][0],
        "step_name": "execute",
        "admission_id": grant["admission_id"],
        "policy_digest": grant["policy_digest"],
        "packet_id": identity["packet_id"],
        "packet_hash": identity["packet_hash"],
        "instruction_binding": _binding()["instruction_binding"],
        "ack_origin_nonce": f"nonce-t1-12-{run_id}",
        "capability_requirements_digest": identity["capability_requirements_digest"],
    }
    payload["ack_origin_commitment"] = worker_ack_origin_digest(
        origin_proof,
        {**payload, "workflow_run_id": run_id},
    )
    return new_workflow_event(
        "StepDispatched",
        run_id,
        trace_id=run_id,
        producer="omo.t1_12.wp_p1_test",
        idempotency_key=f"{run_id}:step-dispatched:t1-12:{identity['dispatch_id']}",
        payload=payload,
    )


def _bootstrap_exact_admitted_run(tmp_path, monkeypatch):
    """Helper: run a workflow up to admitted state via the exact binding API."""
    from tests.test_worker_lifecycle_mesh import (
        _admit_exact,
        _exact_grant,
        new_worker_ack_origin_proof,
    )

    run_id = f"run-t1-12-wp-p1-{tmp_path.name}"
    grant, policy = _exact_grant(run_id, f"{run_id}:execute")
    store = WorkflowMeshStore(tmp_path / ".omo")
    _admit_exact(store, run_id, grant, policy)
    snapshot = store.snapshot(run_id)
    assert snapshot["state"] == "admitted", f"expected admitted, got {snapshot['state']}"
    return store, run_id, grant, new_worker_ack_origin_proof()


def _rewrite_persisted_log(events, jsonl_path: Path) -> None:
    """Atomically rewrite the workflow-mesh.jsonl file with the (possibly tampered) events."""
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with open(jsonl_path, "w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev, ensure_ascii=False) + "\n")


def test_step_dispatched_rejects_tampered_admission_id(tmp_path, monkeypatch) -> None:
    """If persisted admission_id is mutated, StepDispatched must fail-closed."""
    store, run_id, grant, origin_proof = _bootstrap_exact_admitted_run(tmp_path, monkeypatch)

    events = store.events()
    admitted_event = next(e for e in events if e["event_type"] == "WorkflowAdmitted")
    original_admission_id = admitted_event["payload"]["admission"]["admission_id"]
    admitted_event["payload"]["admission"]["admission_id"] = original_admission_id + "-forged"

    jsonl_path = tmp_path / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
    _rewrite_persisted_log(events, jsonl_path)

    store2 = WorkflowMeshStore(tmp_path / ".omo")
    sd_event = _build_exact_dispatch_event(run_id, grant, origin_proof=origin_proof)
    with pytest.raises(WorkflowMeshEventError) as exc:
        store2.append_exact_step_dispatch(sd_event, origin_proof=origin_proof)
    msg = str(exc.value).lower()
    assert any(s in msg for s in ("admission", "policy", "proof", "mismatch")), (
        f"expected admission mismatch error, got: {exc.value}"
    )


def test_step_dispatched_rejects_expired_admission(tmp_path, monkeypatch) -> None:
    """If persisted admission's expires_at is in the past, StepDispatched must fail-closed."""
    store, run_id, grant, origin_proof = _bootstrap_exact_admitted_run(tmp_path, monkeypatch)

    events = store.events()
    admitted_event = next(e for e in events if e["event_type"] == "WorkflowAdmitted")
    admitted_event["payload"]["admission"]["expires_at"] = "2026-01-01T00:00:00+00:00"

    jsonl_path = tmp_path / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
    _rewrite_persisted_log(events, jsonl_path)

    store2 = WorkflowMeshStore(tmp_path / ".omo")
    sd_event = _build_exact_dispatch_event(run_id, grant, origin_proof=origin_proof)
    with pytest.raises(WorkflowMeshEventError) as exc:
        store2.append_exact_step_dispatch(sd_event, origin_proof=origin_proof)
    msg = str(exc.value).lower()
    assert any(s in msg for s in ("expired", "admission", "valid")), (
        f"expected expired admission error, got: {exc.value}"
    )


def test_step_dispatched_accepts_clean_persisted_admission(tmp_path, monkeypatch) -> None:
    """Sanity: legitimate StepDispatched on clean admission is accepted.

    Positive control ensures the negative tests above don't pass for the wrong
    reason (e.g. unrelated bug that breaks all StepDispatched).
    """
    store, run_id, grant, origin_proof = _bootstrap_exact_admitted_run(tmp_path, monkeypatch)
    sd_event = _build_exact_dispatch_event(run_id, grant, origin_proof=origin_proof)
    # Should NOT raise
    store.append_exact_step_dispatch(sd_event, origin_proof=origin_proof)
    events = store.events()
    sd = [e for e in events if e["event_type"] == "StepDispatched"]
    assert len(sd) == 1, f"expected 1 StepDispatched event, got {len(sd)}"
