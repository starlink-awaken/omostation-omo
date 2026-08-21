from __future__ import annotations

# ruff: noqa: I001

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from omo.worker_lifecycle import (
    WorkerLifecycleError,
    acknowledge_worker,
    new_worker_ack_origin_proof,
    expire_worker_lease,
    reclaim_worker,
    record_step_dispatch,
    renew_worker_lease,
    scan_worker_leases,
)
from omo.workflow_mesh import WorkflowMeshEventError, WorkflowMeshStore, new_workflow_event
from omo.cli import main as cli_main

_ORIGIN_PROOFS: dict[str, str] = {}


def _grant(run_id: str, step_run_id: str) -> dict:
    grant = {
        "admission_id": f"adm-{run_id}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "test",
        "step_run_ids": [step_run_id],
        "capabilities": ["execute"],
        "policy_digest": "policy-test",
        "issued_at": datetime.now(UTC).isoformat(),
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
    }
    grant["proof"] = hashlib.sha256(
        json.dumps(grant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return grant


def _admit(store: WorkflowMeshStore, run_id: str, grant: dict) -> None:
    store.append(new_workflow_event("WorkflowRequested", run_id))
    store.append(new_workflow_event("WorkflowAdmitted", run_id, payload={"admission": grant, **grant}))


def _context(tmp_path, run_id: str = "run-worker") -> dict[str, str]:
    step_run_id = f"{run_id}:execute"
    grant = _grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    _admit(store, run_id, grant)
    origin_proof = new_worker_ack_origin_proof()
    _ORIGIN_PROOFS[run_id] = origin_proof
    record_step_dispatch(
        tmp_path,
        workflow_run_id=run_id,
        trace_id=run_id,
        dispatch_id="dispatch-1",
        worker_id="worker-a",
        step_run_id=step_run_id,
        admission_id=grant["admission_id"],
        packet_id="WP-BP-0123456789abcdef",
        packet_hash="sha256:" + "a" * 64,
        instruction_binding={
            "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
            "instruction_version": "blueprint-agent-instruction-pack/v1",
            "content_digest": "sha256:" + "b" * 64,
            "instruction_profile": "executor",
        },
        ack_origin_proof=origin_proof,
    )
    return {
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "dispatch_id": "dispatch-1",
        "worker_id": "worker-a",
        "step_run_id": step_run_id,
        "admission_id": grant["admission_id"],
    }


def _binding() -> dict:
    return {
        "packet_id": "WP-BP-0123456789abcdef",
        "packet_hash": "sha256:" + "a" * 64,
        "instruction_binding": {
            "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
            "instruction_version": "blueprint-agent-instruction-pack/v1",
            "content_digest": "sha256:" + "b" * 64,
            "instruction_profile": "executor",
        },
        "ack_decision": "proceed",
    }


def _origin_proof(context: dict[str, str]) -> str:
    return _ORIGIN_PROOFS[context["workflow_run_id"]]


def test_worker_lifecycle_consumes_origin_proof_once(tmp_path):
    context = _context(tmp_path)
    ack = acknowledge_worker(
        tmp_path,
        **context,
        **_binding(),
        origin_proof=_origin_proof(context),
        lease_seconds=60,
        now="2026-08-02T00:00:00Z",
    )
    with pytest.raises(WorkerLifecycleError, match="already consumed"):
        acknowledge_worker(
            tmp_path,
            **context,
            **_binding(),
            origin_proof=_origin_proof(context),
            lease_seconds=60,
            now="2026-08-02T00:00:00Z",
        )

    renewed = renew_worker_lease(
        tmp_path,
        **context,
        lease_seconds=60,
        now="2026-08-02T00:00:30Z",
        heartbeat_id="hb-1",
    )
    snapshot = WorkflowMeshStore(tmp_path).snapshot(context["workflow_run_id"])
    assert renewed["event_type"] == "WorkerLeaseRenewed"
    assert snapshot["state"] == "running"
    assert snapshot["worker"]["state"] == "active"
    assert snapshot["worker"]["heartbeat_id"] == "hb-1"
    assert snapshot["worker"]["ack_decision"] == "proceed"
    assert snapshot["worker"]["packet_hash"] == _binding()["packet_hash"]
    assert snapshot["worker"]["instruction_binding"] == _binding()["instruction_binding"]
    assert snapshot["worker"]["ack_origin_proof_consumed"] is True
    assert _origin_proof(context) not in json.dumps(WorkflowMeshStore(tmp_path).events())
    assert len(snapshot["worker_events"]) == 2


def test_worker_lease_expires_only_after_deadline_and_can_be_reclaimed(tmp_path):
    context = _context(tmp_path, "run-expiry")
    acknowledge_worker(
        tmp_path,
        **context,
        **_binding(),
        origin_proof=_origin_proof(context),
        lease_seconds=60,
        now="2026-08-02T00:00:00Z",
    )
    with pytest.raises(WorkerLifecycleError, match="not expired"):
        expire_worker_lease(tmp_path, **context, now="2026-08-02T00:00:30Z")

    expired = expire_worker_lease(
        tmp_path,
        **context,
        now="2026-08-02T00:01:00Z",
        reason="worker_lost",
    )
    assert expired["event_type"] == "WorkerLeaseExpired"
    assert (
        expire_worker_lease(
            tmp_path,
            **context,
            now="2026-08-02T00:02:00Z",
            reason="worker_lost",
        )
        == expired
    )

    reclaimed = reclaim_worker(
        tmp_path,
        **context,
        successor_worker_id="worker-b",
        successor_dispatch_id="dispatch-2",
        now="2026-08-02T00:01:05Z",
    )
    snapshot = WorkflowMeshStore(tmp_path).snapshot(context["workflow_run_id"])
    assert reclaimed["event_type"] == "WorkerReclaimed"
    assert snapshot["state"] == "running"
    assert snapshot["worker"]["state"] == "reclaimed"
    assert snapshot["worker"]["successor_worker_id"] == "worker-b"


def test_worker_heartbeat_requires_ack_and_owner_context(tmp_path):
    context = _context(tmp_path, "run-invalid")
    with pytest.raises(WorkerLifecycleError, match="ACK"):
        renew_worker_lease(tmp_path, **context)  # type: ignore[reportArgumentType]

    acknowledge_worker(
        tmp_path,
        **context,
        **_binding(),
        origin_proof=_origin_proof(context),
        lease_seconds=60,
        now="2026-08-02T00:00:00Z",
    )
    with pytest.raises(WorkerLifecycleError, match="owner"):
        renew_worker_lease(
            tmp_path,
            **{**context, "worker_id": "worker-other"},  # type: ignore[reportArgumentType]
            heartbeat_id="hb-other",
            now="2026-08-02T00:00:30Z",
        )


def test_worker_reclaim_requires_expiry(tmp_path):
    context = _context(tmp_path, "run-reclaim-before-expiry")
    acknowledge_worker(
        tmp_path,
        **context,
        **_binding(),
        origin_proof=_origin_proof(context),
        lease_seconds=60,
        now="2026-08-02T00:00:00Z",
    )
    with pytest.raises(WorkerLifecycleError, match="expired"):
        reclaim_worker(
            tmp_path,
            **context,
            successor_worker_id="worker-b",
            successor_dispatch_id="dispatch-2",
            now="2026-08-02T00:01:00Z",
        )


def test_mesh_watchdog_dry_run_is_read_only_and_apply_expires_once(tmp_path):
    context = _context(tmp_path, "run-watchdog")
    acknowledge_worker(
        tmp_path,
        **context,
        **_binding(),
        origin_proof=_origin_proof(context),
        lease_seconds=60,
        now="2026-08-02T00:00:00Z",
    )

    dry_run = scan_worker_leases(tmp_path, now="2026-08-02T00:01:00Z", apply=False)

    assert dry_run["schema"] == "workflow-mesh-watchdog/v1"
    assert dry_run["mode"] == "dry_run"
    assert dry_run["due_count"] == 1
    assert dry_run["expired_count"] == 0
    assert len(WorkflowMeshStore(tmp_path).events()) == 4

    applied = scan_worker_leases(
        tmp_path,
        now="2026-08-02T00:01:00Z",
        apply=True,
        reason="watchdog_timeout",
    )
    assert applied["mode"] == "apply"
    assert applied["expired_count"] == 1
    snapshot = WorkflowMeshStore(tmp_path).snapshot(context["workflow_run_id"])
    assert snapshot["worker"]["state"] == "lease_expired"
    assert snapshot["worker"]["reason"] == "watchdog_timeout"

    repeated = scan_worker_leases(tmp_path, now="2026-08-02T00:02:00Z", apply=True)
    assert repeated["expired_count"] == 0
    assert repeated["due_count"] == 0
    assert not any(event["event_type"] == "WorkerReclaimed" for event in WorkflowMeshStore(tmp_path).events())


def test_mesh_watchdog_does_not_expire_a_live_lease(tmp_path):
    context = _context(tmp_path, "run-live")
    acknowledge_worker(
        tmp_path,
        **context,
        **_binding(),
        origin_proof=_origin_proof(context),
        lease_seconds=60,
        now="2026-08-02T00:00:00Z",
    )

    result = scan_worker_leases(tmp_path, now="2026-08-02T00:00:59Z", apply=True)

    assert result["due_count"] == 0
    assert result["expired_count"] == 0
    assert WorkflowMeshStore(tmp_path).snapshot(context["workflow_run_id"])["worker"]["state"] == "acknowledged"


def test_mesh_watchdog_cli_uses_public_worker_command(tmp_path, capsys):
    assert cli_main(["worker", "mesh-watchdog", "--json", "--omo-dir", str(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "workflow-mesh-watchdog/v1"
    assert payload["mode"] == "dry_run"


def test_mesh_ack_cli_records_worker_originated_proceed_binding(tmp_path, capsys, monkeypatch):
    context = _context(tmp_path, "run-cli-ack")
    binding = _binding()

    monkeypatch.setenv("OMO_WORKER_ACK_ORIGIN_PROOF", _origin_proof(context))
    assert (
        cli_main(
            [
                "worker",
                "mesh-ack",
                context["workflow_run_id"],
                "--trace-id",
                context["trace_id"],
                "--dispatch-id",
                context["dispatch_id"],
                "--worker",
                context["worker_id"],
                "--step-run-id",
                context["step_run_id"],
                "--admission-id",
                context["admission_id"],
                "--packet-id",
                binding["packet_id"],
                "--packet-hash",
                binding["packet_hash"],
                "--instruction-binding-json",
                json.dumps(binding["instruction_binding"]),
                "--ack-decision",
                "proceed",
                "--omo-dir",
                str(tmp_path),
            ]
        )
        == 0
    )

    assert "event_type=WorkerAcknowledged" in capsys.readouterr().out
    ack = WorkflowMeshStore(tmp_path).snapshot(context["workflow_run_id"])["worker"]
    assert ack["ack_decision"] == "proceed"
    assert ack["instruction_binding"] == binding["instruction_binding"]


def test_public_binding_cannot_forge_worker_ack(tmp_path):
    context = _context(tmp_path, "run-forged-ack")
    events_before = WorkflowMeshStore(tmp_path).events()

    with pytest.raises(WorkerLifecycleError, match="origin proof"):
        acknowledge_worker(
            tmp_path,
            **context,
            **_binding(),
            origin_proof="controller-knows-only-public-fields",
        )

    assert WorkflowMeshStore(tmp_path).events() == events_before


def test_bound_stop_and_raw_mesh_append_cannot_bypass_origin_proof(tmp_path):
    context = _context(tmp_path, "run-bound-stop-proof")
    binding = _binding()
    events_before = WorkflowMeshStore(tmp_path).events()

    with pytest.raises(WorkerLifecycleError, match="origin proof"):
        acknowledge_worker(
            tmp_path,
            **context,
            **{**binding, "ack_decision": "stop"},
        )

    forged = new_workflow_event(
        "WorkerAcknowledged",
        context["workflow_run_id"],
        producer="worker",
        idempotency_key=f"{context['workflow_run_id']}:worker-ack:{context['dispatch_id']}",
        payload={
            **context,
            **binding,
            "acknowledged_at": "2026-08-02T00:00:00Z",
            "lease_expires_at": "2026-08-02T00:01:00Z",
            "ack_origin_proof_digest": WorkflowMeshStore(tmp_path).worker_snapshot(context["workflow_run_id"])[
                "ack_origin_commitment"
            ],
        },
    )
    with pytest.raises(WorkflowMeshEventError, match="authenticated worker append"):
        WorkflowMeshStore(tmp_path).append(forged)

    assert WorkflowMeshStore(tmp_path).events() == events_before
