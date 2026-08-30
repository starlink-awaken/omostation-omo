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
from omo.workflow_mesh import (
    WorkflowMeshEventError,
    WorkflowMeshStore,
    new_workflow_event,
    worker_ack_origin_digest,
)
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


@pytest.mark.parametrize("field", ["assignment_id", "dispatch_id"])
def test_step_dispatch_rejects_empty_persisted_exact_ids_without_event(tmp_path, monkeypatch, field):
    run_id = f"run-empty-exact-{field}"
    step_run_id = f"{run_id}:execute"
    grant, policy = _exact_grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    forged_snapshot = store.snapshot(run_id)
    forged_snapshot["admission"]["request_identity"][field] = ""
    forged_snapshot["exact_request_identity"][field] = ""
    _reproof(forged_snapshot["admission"])

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
            dispatch_id="" if field == "dispatch_id" else grant["request_identity"]["dispatch_id"],
            worker_id="worker-exact",
            step_run_id=step_run_id,
            admission_id=grant["admission_id"],
            policy_digest=grant["policy_digest"],
            packet_id=grant["request_identity"]["packet_id"],
            packet_hash=grant["request_identity"]["packet_hash"],
        )

    assert store.snapshot(run_id)["state"] == "admitted"
    assert not any(event["event_type"] == "StepDispatched" for event in store.events())


@pytest.mark.parametrize(
    "mutation",
    [
        "dispatch_id",
        "packet_id",
        "packet_hash",
        "policy_digest",
        "admission_id",
        "step_run_id",
        "requirements_digest",
    ],
)
def test_raw_exact_step_dispatched_append_revalidates_persisted_binding(tmp_path, mutation):
    run_id = f"run-raw-exact-dispatch-{mutation}"
    step_run_id = f"{run_id}:execute"
    grant, policy = _exact_grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    identity = grant["request_identity"]
    payload = {
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": identity["bet_id"],
        "workflow_id": identity["workflow_id"],
        "dispatch_id": identity["dispatch_id"],
        "worker_id": "worker-exact",
        "step_run_id": step_run_id,
        "step_name": "execute",
        "admission_id": grant["admission_id"],
        "policy_digest": grant["policy_digest"],
        "packet_id": identity["packet_id"],
        "packet_hash": identity["packet_hash"],
        "instruction_binding": None,
        "ack_origin_nonce": None,
        "capability_requirements_digest": identity["capability_requirements_digest"],
    }
    field = "capability_requirements_digest" if mutation == "requirements_digest" else mutation
    payload[field] = "forged"
    before = list(store.events())

    with pytest.raises(WorkflowMeshEventError):
        store.append(
            new_workflow_event(
                "StepDispatched",
                run_id,
                payload=payload,
            )
        )

    assert store.events() == before
    assert store.snapshot(run_id)["state"] == "admitted"


def _raw_exact_dispatch_event(run_id: str, grant: dict, *, origin_proof: str):
    identity = grant["request_identity"]
    payload = {
        "exact_request_discriminator": "agent-workflow-exact/v1",
        "bet_id": identity["bet_id"],
        "workflow_id": identity["workflow_id"],
        "dispatch_id": identity["dispatch_id"],
        "worker_id": "worker-chosen-by-raw-caller",
        "step_run_id": grant["step_run_ids"][0],
        "step_name": "execute",
        "admission_id": grant["admission_id"],
        "policy_digest": grant["policy_digest"],
        "packet_id": identity["packet_id"],
        "packet_hash": identity["packet_hash"],
        "instruction_binding": _binding()["instruction_binding"],
        "ack_origin_nonce": "raw-caller-chosen-nonce",
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
        producer="omo.worker_lifecycle",
        idempotency_key=f"{run_id}:step-dispatched:{identity['dispatch_id']}",
        payload=payload,
    )


def test_generic_append_rejects_well_formed_exact_step_dispatched(tmp_path):
    run_id = "run-generic-exact-dispatch-denied"
    grant, policy = _exact_grant(run_id, f"{run_id}:execute")
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    event = _raw_exact_dispatch_event(run_id, grant, origin_proof=new_worker_ack_origin_proof())
    before = list(store.events())

    with pytest.raises(WorkflowMeshEventError, match="authenticated exact dispatch"):
        store.append(event)

    assert store.events() == before


def test_authenticated_exact_dispatch_append_requires_private_proof(tmp_path):
    run_id = "run-authenticated-exact-dispatch"
    grant, policy = _exact_grant(run_id, f"{run_id}:execute")
    store = WorkflowMeshStore(tmp_path)
    _admit_exact(store, run_id, grant, policy)
    origin_proof = new_worker_ack_origin_proof()
    event = _raw_exact_dispatch_event(run_id, grant, origin_proof=origin_proof)
    assert hasattr(store, "append_exact_step_dispatch"), "exact dispatch append method is unavailable"

    stored = store.append_exact_step_dispatch(event, origin_proof=origin_proof)
    assert stored["event_type"] == "StepDispatched"

    other_run = "run-authenticated-exact-dispatch-wrong-proof"
    other_grant, other_policy = _exact_grant(other_run, f"{other_run}:execute")
    other_store = WorkflowMeshStore(tmp_path / "wrong-proof")
    _admit_exact(other_store, other_run, other_grant, other_policy)
    other_event = _raw_exact_dispatch_event(other_run, other_grant, origin_proof=origin_proof)
    with pytest.raises(WorkflowMeshEventError, match="dispatch origin proof"):
        other_store.append_exact_step_dispatch(other_event, origin_proof=new_worker_ack_origin_proof())


def test_exact_worker_ack_lease_is_capped_by_admission_expiry(tmp_path):
    run_id = "run-exact-admission-bounded-lease"
    step_run_id = f"{run_id}:execute"
    issued_at = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=10)
    expires_at = issued_at + timedelta(seconds=900)
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
    acknowledged = acknowledge_worker(
        tmp_path,
        **context,
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
        ack_decision="proceed",
        origin_proof=origin_proof,
        lease_seconds=1200,
        now=(issued_at + timedelta(seconds=10)).isoformat(),
    )

    lease_expires_at = datetime.fromisoformat(
        acknowledged["payload"]["lease_expires_at"].replace("Z", "+00:00")
    )
    assert lease_expires_at <= expires_at


def _live_exact_worker_for_ttl_boundary(tmp_path, run_id: str, *, ttl_seconds: int = 600):
    step_run_id = f"{run_id}:execute"
    issued_at = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=10)
    expires_at = issued_at + timedelta(seconds=ttl_seconds)
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
    return store, grant, binding, context, origin_proof, issued_at, expires_at


@pytest.mark.parametrize("append_kind", ["ack", "completion"])
def test_store_authenticated_append_rejects_event_outside_exact_admission_window(tmp_path, append_kind):
    run_id = f"run-store-ttl-{append_kind}"
    store, grant, binding, context, origin_proof, issued_at, expires_at = _live_exact_worker_for_ttl_boundary(
        tmp_path,
        run_id,
    )
    if append_kind == "ack":
        worker = store.snapshot(run_id)["worker"]
        ack_context = {**worker, "workflow_run_id": run_id}
        commitment = worker_ack_origin_digest(origin_proof, ack_context)
        event = new_workflow_event(
            "WorkerAcknowledged",
            run_id,
            trace_id=run_id,
            producer="worker",
            idempotency_key=f"{run_id}:worker-ack:{context['dispatch_id']}",
            payload={
                **context,
                "acknowledged_at": issued_at.isoformat(),
                "lease_expires_at": (issued_at + timedelta(seconds=60)).isoformat(),
                "packet_id": grant["request_identity"]["packet_id"],
                "packet_hash": grant["request_identity"]["packet_hash"],
                "instruction_binding": binding["instruction_binding"],
                "ack_decision": "proceed",
                "ack_origin_proof_digest": commitment,
            },
        )
        event["occurred_at"] = (expires_at + timedelta(seconds=1)).isoformat()
        before = list(store.events())
        with pytest.raises(WorkflowMeshEventError, match="admission.*expired"):
            store.append_worker_ack(event, origin_proof=origin_proof)
    else:
        acknowledge_worker(
            tmp_path,
            **context,
            packet_id=grant["request_identity"]["packet_id"],
            packet_hash=grant["request_identity"]["packet_hash"],
            instruction_binding=binding["instruction_binding"],
            ack_decision="proceed",
            origin_proof=origin_proof,
            lease_seconds=60,
        )
        store.append(
            new_workflow_event(
                "StepStarted",
                run_id,
                payload={
                    "step_run_id": context["step_run_id"],
                    "step_name": "execute",
                    "admission_id": context["admission_id"],
                },
            )
        )
        worker = store.snapshot(run_id)["worker"]
        result_digest = "sha256:" + "c" * 64
        completion_context = {**worker, "workflow_run_id": run_id, "result_digest": result_digest}
        receipt = {
            "status": "succeeded",
            "exact_request_discriminator": "agent-workflow-exact/v1",
            "bet_id": grant["request_identity"]["bet_id"],
            "workflow_id": grant["request_identity"]["workflow_id"],
            "workflow_run_id": run_id,
            "admission_id": context["admission_id"],
            "step_run_id": context["step_run_id"],
            "dispatch_id": context["dispatch_id"],
            "worker_id": context["worker_id"],
            "ack_origin_proof_digest": worker["ack_origin_proof_digest"],
            "completion_origin_commitment": worker_ack_origin_digest(origin_proof, completion_context),
            "result_digest": result_digest,
        }
        receipt["receipt_digest"] = "sha256:" + hashlib.sha256(
            json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        event = new_workflow_event(
            "WorkflowSucceeded",
            run_id,
            trace_id=run_id,
            producer="worker",
            idempotency_key=f"{run_id}:worker-completed:{context['dispatch_id']}",
            payload={"worker_completion_receipt": receipt},
        )
        event["occurred_at"] = (expires_at + timedelta(seconds=1)).isoformat()
        before = list(store.events())
        with pytest.raises(WorkflowMeshEventError, match="admission.*expired"):
            store.append_worker_completion(event, origin_proof=origin_proof)

    assert store.events() == before


def test_exact_renewed_lease_is_capped_to_admission_and_rejects_future_now(tmp_path):
    run_id = "run-exact-renewal-admission-boundary"
    store, grant, binding, context, origin_proof, _issued_at, expires_at = _live_exact_worker_for_ttl_boundary(
        tmp_path,
        run_id,
        ttl_seconds=300,
    )
    acknowledge_worker(
        tmp_path,
        **context,
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
        ack_decision="proceed",
        origin_proof=origin_proof,
        lease_seconds=60,
    )
    renewed = renew_worker_lease(tmp_path, **context, lease_seconds=1200)
    renewed_expiry = datetime.fromisoformat(renewed["payload"]["lease_expires_at"].replace("Z", "+00:00"))
    assert renewed_expiry <= expires_at
    before = list(store.events())

    with pytest.raises(WorkerLifecycleError, match="admission.*expired"):
        renew_worker_lease(
            tmp_path,
            **context,
            lease_seconds=60,
            now=(expires_at + timedelta(seconds=1)).isoformat(),
            heartbeat_id="forged-future-now",
        )

    assert store.events() == before


def _raw_exact_renewal_event(run_id: str, context: dict, *, heartbeat_at: datetime, lease_expires_at: datetime):
    return new_workflow_event(
        "WorkerLeaseRenewed",
        run_id,
        trace_id=run_id,
        producer="worker",
        idempotency_key=f"{run_id}:worker-heartbeat:{context['dispatch_id']}:raw-renewal",
        payload={
            "dispatch_id": context["dispatch_id"],
            "worker_id": context["worker_id"],
            "step_run_id": context["step_run_id"],
            "admission_id": context["admission_id"],
            "heartbeat_id": "raw-renewal",
            "heartbeat_at": heartbeat_at.isoformat(),
            "lease_expires_at": lease_expires_at.isoformat(),
        },
    )


def test_generic_append_rejects_exact_worker_lease_renewal(tmp_path):
    run_id = "run-generic-exact-renewal-denied"
    store, grant, binding, context, origin_proof, _issued_at, expires_at = _live_exact_worker_for_ttl_boundary(
        tmp_path,
        run_id,
    )
    acknowledge_worker(
        tmp_path,
        **context,
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
        ack_decision="proceed",
        origin_proof=origin_proof,
        lease_seconds=60,
    )
    now = datetime.now(UTC)
    event = _raw_exact_renewal_event(
        run_id,
        context,
        heartbeat_at=now,
        lease_expires_at=min(now + timedelta(seconds=60), expires_at),
    )
    before = list(store.events())

    with pytest.raises(WorkflowMeshEventError, match="authenticated exact lease renewal"):
        store.append(event)

    assert store.events() == before


def test_locked_exact_renewal_append_caps_and_rejects_expiry_race(tmp_path):
    run_id = "run-locked-exact-renewal"
    store, grant, binding, context, origin_proof, _issued_at, expires_at = _live_exact_worker_for_ttl_boundary(
        tmp_path,
        run_id,
        ttl_seconds=300,
    )
    acknowledge_worker(
        tmp_path,
        **context,
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
        ack_decision="proceed",
        origin_proof=origin_proof,
        lease_seconds=60,
    )
    assert hasattr(store, "append_exact_worker_lease"), "exact renewal append method is unavailable"
    now = datetime.now(UTC)
    event = _raw_exact_renewal_event(
        run_id,
        context,
        heartbeat_at=now,
        lease_expires_at=expires_at + timedelta(seconds=900),
    )
    stored = store.append_exact_worker_lease(event)
    stored_expiry = datetime.fromisoformat(stored["payload"]["lease_expires_at"].replace("Z", "+00:00"))
    assert stored_expiry <= expires_at

    expired_event = _raw_exact_renewal_event(
        run_id,
        context,
        heartbeat_at=expires_at + timedelta(seconds=1),
        lease_expires_at=expires_at + timedelta(seconds=60),
    )
    expired_event["idempotency_key"] += ":expired"
    before = list(store.events())
    with pytest.raises(WorkflowMeshEventError, match="admission.*expired"):
        store.append_exact_worker_lease(expired_event)
    assert store.events() == before


def test_exact_renewal_same_heartbeat_is_semantically_idempotent_and_conflicts_fail(tmp_path):
    run_id = "run-exact-renewal-idempotency"
    store, grant, binding, context, origin_proof, _issued_at, _expires_at = _live_exact_worker_for_ttl_boundary(
        tmp_path,
        run_id,
    )
    acknowledge_worker(
        tmp_path,
        **context,
        packet_id=grant["request_identity"]["packet_id"],
        packet_hash=grant["request_identity"]["packet_hash"],
        instruction_binding=binding["instruction_binding"],
        ack_decision="proceed",
        origin_proof=origin_proof,
        lease_seconds=60,
    )
    heartbeat_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    first = renew_worker_lease(
        tmp_path,
        **context,
        lease_seconds=60,
        now=heartbeat_at,
        heartbeat_id="same-heartbeat",
    )
    before_repeat = list(store.events())
    repeated = renew_worker_lease(
        tmp_path,
        **context,
        lease_seconds=60,
        now=heartbeat_at,
        heartbeat_id="same-heartbeat",
    )
    assert repeated == first
    assert store.events() == before_repeat

    with pytest.raises(WorkerLifecycleError, match="conflicting exact worker lease renewal"):
        renew_worker_lease(
            tmp_path,
            **context,
            lease_seconds=30,
            now=heartbeat_at,
            heartbeat_id="same-heartbeat",
        )
    assert store.events() == before_repeat
