from __future__ import annotations

# ruff: noqa: I001

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

import omo.worker_lifecycle as worker_lifecycle_mod
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
    grant, _policy = _exact_grant(run_id, step_run_id)
    grant["backend"] = "test"
    grant["policy_digest"] = "policy-test"
    grant["proof"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in grant.items() if key != "proof"},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return grant


def _admit(store: WorkflowMeshStore, run_id: str, grant: dict) -> None:
    store.append(new_workflow_event("WorkflowRequested", run_id))
    store.append(new_workflow_event("WorkflowAdmitted", run_id, payload={"admission": grant, **grant}))


def _exact_grant(run_id: str, step_run_id: str) -> tuple[dict, dict]:
    requirements = [
        {"capability_id": "skill:git-discipline", "operation": "load", "effect": "read_only"},
        {"capability_id": "workflow:bet-execution", "operation": "load", "effect": "read_only"},
    ]
    requirements_digest = "sha256:" + hashlib.sha256(
        json.dumps(requirements, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    request_identity = {
        "bet_id": "BET-BOUND",
        "workflow_id": "test-workflow",
        "correlation_id": run_id,
        "workflow_run_id": run_id,
        "packet_id": "WP-BP-0123456789abcdef",
        "packet_hash": "sha256:" + "a" * 64,
        "assignment_id": f"preflight:{run_id}:assignment",
        "dispatch_id": f"preflight:{run_id}:dispatch",
        "actor_id": "actor:test",
        "delivery_attempt_id": "attempt:test",
        "capability_requirements": requirements,
        "capability_requirements_digest": requirements_digest,
    }
    policy = {
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": request_identity["bet_id"],
        "workflow_id": "test-workflow",
        "workflow_run_id": run_id,
        "packet_id": request_identity["packet_id"],
        "packet_hash": request_identity["packet_hash"],
        "capability_requirements": requirements,
        "capability_requirements_digest": requirements_digest,
        "actor_id": request_identity["actor_id"],
        "delivery_attempt_id": request_identity["delivery_attempt_id"],
        "source_receipt_digests": ["sha256:" + "3" * 64, "sha256:" + "4" * 64],
        "requested_budget": 0.0,
    }
    grant = {
        "admission_id": f"adm-{run_id}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "agent-workflow",
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": request_identity["bet_id"],
        "workflow_id": request_identity["workflow_id"],
        "step_run_ids": [step_run_id],
        "capabilities": [requirement["capability_id"] for requirement in requirements],
        "policy_digest": hashlib.sha256(
            json.dumps(policy, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "request_identity": request_identity,
        "issued_at": datetime.now(UTC).isoformat(),
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
    }
    grant["proof"] = hashlib.sha256(
        json.dumps(grant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return grant, policy


def _admit_exact(store: WorkflowMeshStore, run_id: str, grant: dict, policy: dict) -> None:
    store.append(
        new_workflow_event(
            "WorkflowRequested",
            run_id,
            payload={
                "exact_request_discriminator": "agent-workflow-exact/v1",
                "bet_id": grant["request_identity"]["bet_id"],
                "workflow_id": "test-workflow",
                "request_identity": grant["request_identity"],
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
                "bet_id": grant["request_identity"]["bet_id"],
                "workflow_id": grant["request_identity"]["workflow_id"],
                "policy_digest": grant["policy_digest"],
                "proof": grant["proof"],
                "request_identity": grant["request_identity"],
            },
        )
    )


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
        policy_digest="policy-test",
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


def _reproof(grant: dict) -> None:
    grant["proof"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in grant.items() if key != "proof"},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def test_exact_step_dispatch_rejects_expired_admission_without_event(tmp_path):
    run_id = "run-expired-exact-dispatch"
    step_run_id = f"{run_id}:execute"
    grant, policy = _exact_grant(run_id, step_run_id)
    grant["issued_at"] = "2026-08-30T00:00:00+00:00"
    grant["expires_at"] = "2026-08-30T00:15:00+00:00"
    _reproof(grant)
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    before = list(store.events())

    with pytest.raises(WorkerLifecycleError, match="admission.*expired"):
        record_step_dispatch(
            tmp_path,
            workflow_run_id=run_id,
            trace_id=run_id,
            dispatch_id=grant["request_identity"]["dispatch_id"],
            worker_id="worker-a",
            step_run_id=step_run_id,
            admission_id=grant["admission_id"],
            policy_digest=grant["policy_digest"],
            packet_id=grant["request_identity"]["packet_id"],
            packet_hash=grant["request_identity"]["packet_hash"],
            instruction_binding=_binding()["instruction_binding"],
            ack_origin_proof=new_worker_ack_origin_proof(),
        )

    assert store.events() == before


def test_exact_worker_completion_rejects_expired_admission_without_success(tmp_path, monkeypatch):
    run_id = "run-expired-exact-completion"
    step_run_id = f"{run_id}:execute"
    issued_at = datetime.now(UTC).replace(microsecond=0)
    expires_at = issued_at + timedelta(hours=1)
    grant, policy = _exact_grant(run_id, step_run_id)
    grant["issued_at"] = issued_at.isoformat()
    grant["expires_at"] = expires_at.isoformat()
    _reproof(grant)
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    origin_proof = new_worker_ack_origin_proof()
    binding = _binding()
    context = {
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "dispatch_id": grant["request_identity"]["dispatch_id"],
        "worker_id": "worker-a",
        "step_run_id": step_run_id,
        "admission_id": grant["admission_id"],
    }
    record_step_dispatch(
        tmp_path,
        **context,
        policy_digest=grant["policy_digest"],
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
        ack_origin_proof=origin_proof,
    )
    acknowledge_worker(
        tmp_path,
        **context,
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
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
    monkeypatch.setattr(worker_lifecycle_mod, "_utc", lambda _value=None: expires_at + timedelta(seconds=1))

    with pytest.raises(WorkerLifecycleError, match="admission.*expired"):
        worker_lifecycle_mod.record_worker_completion(
            tmp_path,
            **context,
            origin_proof=origin_proof,
            result_digest="sha256:" + "c" * 64,
        )

    assert not any(event["event_type"] == "WorkflowSucceeded" for event in store.events())


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
            origin_proof=new_worker_ack_origin_proof(),
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


def _forge_exact_admission(admission: dict, mutation: str) -> None:
    identity = admission["request_identity"]
    if mutation == "admission_id":
        admission["admission_id"] = "adm-forged"
    elif mutation == "policy_digest":
        admission["policy_digest"] = "f" * 64
    elif mutation == "proof":
        admission["proof"] = "forged-proof"
        return
    elif mutation == "packet_id":
        identity["packet_id"] = "WP-FORGED"
    elif mutation == "packet_hash":
        identity["packet_hash"] = "sha256:" + "f" * 64
    elif mutation == "workflow_run_id":
        identity["workflow_run_id"] = "cross-run"
    elif mutation == "actor_id":
        identity["actor_id"] = ""
    elif mutation == "delivery_attempt_id":
        identity["delivery_attempt_id"] = ""
    elif mutation == "dispatch_id":
        identity["dispatch_id"] = "dispatch-forged"
    elif mutation == "ordered_requirements":
        identity["capability_requirements"] = list(reversed(identity["capability_requirements"]))
    elif mutation == "requirements_digest":
        identity["capability_requirements_digest"] = "sha256:" + "e" * 64
    elif mutation == "admitted_step_id":
        admission["step_run_ids"] = ["run-other:execute"]
    else:  # pragma: no cover - the parameter list is the authority.
        raise AssertionError(mutation)
    admission["proof"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in admission.items() if key != "proof"},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


@pytest.mark.parametrize(
    "mutation",
    [
        "admission_id",
        "policy_digest",
        "proof",
        "packet_id",
        "packet_hash",
        "workflow_run_id",
        "actor_id",
        "delivery_attempt_id",
        "dispatch_id",
        "ordered_requirements",
        "requirements_digest",
        "admitted_step_id",
    ],
)
def test_step_dispatch_rejects_forged_exact_admission_identity(tmp_path, monkeypatch, mutation):
    run_id = "run-exact-dispatch"
    step_run_id = f"{run_id}:execute"
    grant, policy = _exact_grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    forged_snapshot = store.snapshot(run_id)
    _forge_exact_admission(forged_snapshot["admission"], mutation)

    class ForgedSnapshotStore:
        def snapshot(self, workflow_run_id):
            assert workflow_run_id == run_id
            return forged_snapshot

        def events(self):
            return store.events()

        def append(self, event):
            return store.append(event)

    monkeypatch.setattr(worker_lifecycle_mod, "_store", lambda _omo_dir: ForgedSnapshotStore())

    with pytest.raises(WorkerLifecycleError, match="admission binding mismatch"):
        record_step_dispatch(
            tmp_path,
            workflow_run_id=run_id,
            trace_id=run_id,
            dispatch_id=grant["request_identity"]["dispatch_id"],
            worker_id="worker-exact",
            step_run_id=step_run_id,
            admission_id=grant["admission_id"],
            policy_digest=grant["policy_digest"],
            packet_id=grant["request_identity"]["packet_id"],
            packet_hash=grant["request_identity"]["packet_hash"],
        )
    assert WorkflowMeshStore(tmp_path).snapshot(run_id)["state"] == "admitted"
    assert not any(event["event_type"] == "StepDispatched" for event in store.events())


def test_step_dispatch_rejects_forged_caller_dispatch_id(tmp_path):
    run_id = "run-exact-caller-dispatch"
    step_run_id = f"{run_id}:execute"
    grant, policy = _exact_grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)

    with pytest.raises(WorkerLifecycleError, match="admission binding mismatch"):
        record_step_dispatch(
            tmp_path,
            workflow_run_id=run_id,
            trace_id=run_id,
            dispatch_id="caller-forged-dispatch",
            worker_id="worker-exact",
            step_run_id=step_run_id,
            admission_id=grant["admission_id"],
            policy_digest=grant["policy_digest"],
            packet_id=grant["request_identity"]["packet_id"],
            packet_hash=grant["request_identity"]["packet_hash"],
        )

    assert store.snapshot(run_id)["state"] == "admitted"
    assert not any(event["event_type"] == "StepDispatched" for event in store.events())
