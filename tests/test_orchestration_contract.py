"""Regression tests for the orchestrator-neutral delivery contract MVP."""

from __future__ import annotations

import hashlib
import json

import pytest
from ecos.ssot.tools.work_packet_compiler import (
    build_command_check,
    build_verification_receipt,
    canonicalize,
    compute_packet_hash,
)

from omo.orchestration_contract import (
    KandevFixtureAdapter,
    OrchestrationContractCoordinator,
    OrchestrationContractError,
    validate_capability_requirements,
)
from omo.workflow_mesh import WorkflowMeshStore, new_workflow_event

NOW = "2026-08-13T06:00:00Z"
CAPABILITY_REQUIREMENTS = [
    {"capability_id": "skill:git-discipline", "operation": "load", "effect": "read_only"},
    {"capability_id": "workflow:bet-execution", "operation": "load", "effect": "read_only"},
]


@pytest.mark.parametrize(
    "requirements",
    [
        [CAPABILITY_REQUIREMENTS[0], CAPABILITY_REQUIREMENTS[0]],
        [{"capability_id": "skill:*", "operation": "load", "effect": "read_only"}],
        [{"capability_id": "skill:git-discipline", "operation": "invoke", "effect": "effectful"}],
        [{"capability_id": "workflow:bet-execution", "operation": "execute", "effect": "read_only"}],
        [{"capability_id": "workflow:bet-execution", "operation": "load", "effect": "write"}],
        [{"capability_id": "workflow:bet-execution", "operation": "load"}],
        [{**CAPABILITY_REQUIREMENTS[0], "adapter": "caller-supplied"}],
    ],
)
def test_v2_packet_rejects_invalid_capability_requirements(requirements) -> None:
    with pytest.raises(OrchestrationContractError, match="capability_requirements_invalid"):
        validate_capability_requirements(requirements)


def test_capability_requirements_are_canonical_and_optional() -> None:
    assert validate_capability_requirements(None) == []
    assert validate_capability_requirements(CAPABILITY_REQUIREMENTS) == CAPABILITY_REQUIREMENTS


def _packet() -> dict[str, object]:
    return {
        "packet_id": "WP-ORCH-001",
        "schema_version": "work-packet/v1",
        "blueprint_ref": "blueprint://orchestration-contract/001",
        "wave": "Y1Q2",
        "bet_id": "BET-Y1Q2-T1-14",
        "strategic_outcome": "orchestrator-neutral evidence chain",
        "objective": "connect a candidate manifest to mesh evidence",
        "why_now": "multiple external orchestrators need one delivery contract",
        "status": "active",
        "authority": {"strategist": "omo", "human_gate": False, "risk_level": "R1"},
        "scope": {
            "read_surfaces": ["projects/omo/src/omo/"],
            "write_surfaces": ["projects/omo/src/omo/", "README.md"],
            "non_goals": ["live Kandev", "scheduler"],
        },
        "dependencies": {
            "required_packets": [],
            "required_services": [],
            "required_decisions": [],
        },
        "acceptance": {
            "done_when": [
                {
                    "id": "AC1",
                    "assertion": "receipt is independently verifiable",
                    "evidence_type": "test_result",
                }
            ],
            "verify_commands": [["pytest", "-q"]],
        },
        "budgets": {
            "appetite_hours": 1.0,
            "max_elapsed_hours": 2.0,
            "max_changed_files": 2,
            "max_new_files": 2,
            "max_new_top_level_components": 1,
        },
        "rollback": {"strategy": "revert", "data_migration": False},
        "circuit_breaker": {"when": ["scope expansion"], "action": "interrupt"},
        "assignment": {
            "executor_class": "E1",
            "verifier_class": "V1",
            "same_model_verification_allowed": True,
            "expires_at": "2026-08-14T00:00:00+08:00",
        },
    }


def _v2_packet(
    workspace_root,
    *,
    spec_ref: str = "repo://specs/orchestration-contract.md",
    decision_ref: str = "decision://accepted/BET-Y1Q2-T1-14",
    content: bytes = b"# Accepted orchestration contract\n",
    bet_status: str = "done",
) -> dict[str, object]:
    spec_path = workspace_root / "specs" / "orchestration-contract.md"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_bytes(content)
    instruction_path = workspace_root / "docs" / "operations" / "blueprint-agent-instruction-pack-v1.md"
    instruction_path.parent.mkdir(parents=True, exist_ok=True)
    instruction_content = b"# Blueprint Agent Instruction Pack v1\n"
    instruction_path.write_bytes(instruction_content)
    ledger_path = workspace_root / "docs" / "plans" / "3y-bet-ledger.yaml"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    ledger_path.write_text(
        "---\nmeta: {}\n---\nbets:\n"
        "- id: BET-Y1Q2-T1-14\n"
        f"  status: {bet_status}\n"
        "  accepted_specifications:\n"
        f"  - spec_ref: {spec_ref}\n"
        "    spec_version: 1.0.0\n"
        f"    content_digest: {digest}\n",
        encoding="utf-8",
    )
    base = _packet()
    base["scope"] = {
        **base["scope"],
        "read_surfaces": [
            *base["scope"]["read_surfaces"],
            "specs/orchestration-contract.md",
        ],
    }
    return {
        **base,
        "schema_version": "work-packet/v2",
        "spec_binding": {
            "spec_ref": spec_ref,
            "spec_version": "1.0.0",
            "content_digest": digest,
            "decision_ref": decision_ref,
        },
        "instruction_binding": {
            "instruction_ref": "repo://docs/operations/blueprint-agent-instruction-pack-v1.md",
            "instruction_version": "blueprint-agent-instruction-pack/v1",
            "content_digest": "sha256:" + hashlib.sha256(instruction_content).hexdigest(),
            "instruction_profile": "executor",
        },
    }


def _hash(packet: dict[str, object]) -> str:
    return compute_packet_hash(canonicalize(packet))


def _manifest(
    packet: dict[str, object],
    *,
    changed_paths: list[str] | None = None,
    packet_hash: str | None = None,
) -> dict[str, object]:
    return {
        "packet_id": packet["packet_id"],
        "packet_hash": packet_hash or _hash(packet),
        "assignment_id": "ASG-ORCH-001",
        "agent_id": "kandev-fixture-agent",
        "status": "candidate",
        "changed_paths": changed_paths or ["projects/omo/src/omo/orchestration_contract.py"],
        "claims": [
            {
                "acceptance_id": "AC1",
                "assertion": packet["acceptance"]["done_when"][0]["assertion"],
                "evidence_refs": ["git-object://" + "a" * 40],
            }
        ],
        "checks": [build_command_check(["pytest", "-q"], 0, "fixture green")],
        "recommended_next": "verify",
        "surface_delta": {"files": 1, "loc": 20},
        "artifact_refs": ["git-object://" + "a" * 40],
    }


def _manifest_digest(manifest: dict[str, object]) -> str:
    return compute_packet_hash(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _fixture(
    packet: dict[str, object],
    *,
    workflow_run_id: str = "run-orch",
    assignment_id: str = "ASG-ORCH-001",
    step_run_id: str = "run-orch:step-1",
    state: str = "succeeded",
    output_digest: str | None = None,
) -> dict[str, object]:
    return {
        "external_task_id": "kandev-task-001",
        "workflow_run_id": workflow_run_id,
        "bet_id": packet["bet_id"],
        "packet_id": packet["packet_id"],
        "packet_hash": _hash(packet),
        "assignment_id": assignment_id,
        "step_run_id": step_run_id,
        "adapter_metadata": {"source": "offline-fixture"},
        "state": state,
        "observed_at": NOW,
        "provenance_ref": "fixture://kandev/task-001",
        "output_digest": output_digest or hashlib.sha256(b"fixture output").hexdigest(),
    }


def _grant(run_id: str, step_run_id: str) -> dict[str, object]:
    grant: dict[str, object] = {
        "admission_id": f"adm-{run_id}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "orchestration-contract-test",
        "step_run_ids": [step_run_id],
        "capabilities": ["execute"],
        "policy_digest": "orchestration-contract/v1",
        "issued_at": NOW,
        "expires_at": "2026-08-13T07:00:00Z",
    }
    canonical = json.dumps(grant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    grant["proof"] = hashlib.sha256(canonical).hexdigest()
    return grant


def _seed_succeeded_run(
    tmp_path,
    run_id: str = "run-orch",
    *,
    dispatch_id: str | None = None,
    worker_id: str | None = None,
) -> str:
    step_run_id = f"{run_id}:step-1"
    grant = _grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", run_id, payload={"bet_id": "BET-Y1Q2-T1-14"}))
    store.append(new_workflow_event("WorkflowAdmitted", run_id, payload={"admission": grant, **grant}))
    context = {"step_run_id": step_run_id, "admission_id": grant["admission_id"]}
    if dispatch_id is not None:
        context["dispatch_id"] = dispatch_id
    if worker_id is not None:
        context["worker_id"] = worker_id
    store.append(new_workflow_event("StepDispatched", run_id, payload=context))
    store.append(new_workflow_event("StepStarted", run_id, payload=context))
    store.append(new_workflow_event("WorkflowSucceeded", run_id))
    return step_run_id


def _transport_receipt(
    packet: dict[str, object],
    manifest: dict[str, object],
    *,
    workflow_run_id: str = "run-orch",
    step_run_id: str = "run-orch:step-1",
    dispatch_id: str = "dispatch-orch-001",
    worker_id: str = "codex-supervised",
) -> dict[str, object]:
    receipt: dict[str, object] = {
        "receipt_id": "receipt-orch-001",
        "workflow_run_id": workflow_run_id,
        "step_run_id": step_run_id,
        "bet_id": packet["bet_id"],
        "packet_id": packet["packet_id"],
        "packet_hash": _hash(packet),
        "assignment_id": manifest["assignment_id"],
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
        "output_digest": hashlib.sha256(b"supervised output").hexdigest(),
        "changed_paths": manifest["changed_paths"],
        "observed_at": NOW,
        "provenance_ref": "receipt://codex/dispatch-orch-001",
    }
    receipt["receipt_digest"] = compute_packet_hash(canonicalize(receipt))
    return receipt


def _receipt(
    packet: dict[str, object],
    *,
    verdict: str = "accept",
    candidate_packet_hash: str | None = None,
    measured_packet_hash: str | None = None,
):
    packet_hash = _hash(packet)
    return build_verification_receipt(
        packet=packet,
        candidate_packet_hash=candidate_packet_hash or packet_hash,
        measured_packet_hash=measured_packet_hash or packet_hash,
        executor_model_family="fixture-executor",
        verifier_model_family="fixture-verifier",
        verdict=verdict,
        checks=[build_command_check(["pytest", "-q"], 0, "verified")],
    )


def test_fixture_metadata_is_not_part_of_packet_hash_and_live_transport_is_disabled():
    packet = _packet()
    adapter = KandevFixtureAdapter(_fixture(packet))

    assert _hash(packet) == _hash({**packet, "adapter_metadata": {"ui_status": "polling"}})
    assert adapter.collect("kandev-task-001")["external_task_id"] == "kandev-task-001"
    with pytest.raises(OrchestrationContractError, match="not_enabled"):
        adapter.dispatch(packet)


def test_candidate_evidence_then_acceptance_forms_one_identity_chain(tmp_path):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)

    evidence = coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )
    verified = coordinator.accept_verification(
        workflow_run_id="run-orch",
        packet=packet,
        manifest=_manifest(packet),
        verification_receipt=_receipt(packet),
    )

    events = WorkflowMeshStore(tmp_path).events()
    assert evidence["event_type"] == "EvidenceRecorded"
    assert verified["event_type"] == "WorkflowVerified"
    assert evidence["payload"]["decision_factors"]["artifact_refs_digest"] == compute_packet_hash(
        json.dumps(["git-object://" + "a" * 40], ensure_ascii=False, separators=(",", ":"))
    )
    assert [event["event_type"] for event in events][-2:] == [
        "EvidenceRecorded",
        "WorkflowVerified",
    ]
    assert verified["payload"] | {"receipt_hash": None, "manifest_digest": None} == {
        "assignment_id": "ASG-ORCH-001",
        "manifest_digest": None,
        "packet_hash": _hash(packet),
        "packet_id": "WP-ORCH-001",
        "bet_id": packet["bet_id"],
        "step_run_id": step_run_id,
        "receipt_hash": None,
        "source_receipt_hash": _receipt(packet).receipt_hash,
    }
    assert verified["payload"]["receipt_hash"] == compute_packet_hash(
        f"run-orch\nASG-ORCH-001\n{packet['bet_id']}\n{step_run_id}\n"
        f"{verified['payload']['manifest_digest']}\n{_receipt(packet).receipt_hash}"
    )
    assert WorkflowMeshStore(tmp_path).snapshot("run-orch")["state"] == "verified"


def test_v2_spec_binding_is_revalidated_before_candidate_and_acceptance(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    omo_dir = workspace_root / ".omo"
    step_run_id = _seed_succeeded_run(omo_dir)
    packet = _v2_packet(workspace_root)
    monkeypatch.setattr("omo.orchestration_contract.WORKSPACE_ROOT", workspace_root)
    coordinator = OrchestrationContractCoordinator(omo_dir)

    evidence = coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )
    verified = coordinator.accept_verification(
        workflow_run_id="run-orch",
        packet=packet,
        manifest=_manifest(packet),
        verification_receipt=_receipt(packet),
    )

    assert evidence["event_type"] == "EvidenceRecorded"
    assert verified["event_type"] == "WorkflowVerified"


def test_candidate_bet_with_exact_accepted_spec_is_executable(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    omo_dir = workspace_root / ".omo"
    step_run_id = _seed_succeeded_run(omo_dir)
    packet = _v2_packet(workspace_root, bet_status="candidate")
    monkeypatch.setattr("omo.orchestration_contract.WORKSPACE_ROOT", workspace_root)

    evidence = OrchestrationContractCoordinator(omo_dir).record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )

    assert evidence["event_type"] == "EvidenceRecorded"


def test_candidate_bet_without_exact_binding_is_rejected_without_events(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    omo_dir = workspace_root / ".omo"
    step_run_id = _seed_succeeded_run(omo_dir)
    packet = _v2_packet(workspace_root, bet_status="candidate")
    ledger_path = workspace_root / "docs" / "plans" / "3y-bet-ledger.yaml"
    ledger_path.write_text(
        ledger_path.read_text(encoding="utf-8").replace(packet["spec_binding"]["content_digest"], "sha256:" + "f" * 64),
        encoding="utf-8",
    )
    monkeypatch.setattr("omo.orchestration_contract.WORKSPACE_ROOT", workspace_root)

    with pytest.raises(OrchestrationContractError, match="spec_binding_invalid"):
        OrchestrationContractCoordinator(omo_dir).record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=_manifest(packet),
            fixture=_fixture(packet),
        )

    assert "EvidenceRecorded" not in [event["event_type"] for event in WorkflowMeshStore(omo_dir).events()]


def test_generic_candidate_receipt_binds_dispatch_and_manifest(tmp_path):
    dispatch_id = "dispatch-orch-001"
    worker_id = "codex-supervised"
    step_run_id = _seed_succeeded_run(tmp_path, dispatch_id=dispatch_id, worker_id=worker_id)
    packet = _packet()
    manifest = _manifest(packet)
    receipt = _transport_receipt(
        packet,
        manifest,
        step_run_id=step_run_id,
        dispatch_id=dispatch_id,
        worker_id=worker_id,
    )

    evidence = OrchestrationContractCoordinator(tmp_path).record_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=manifest,
        transport_receipt=receipt,
    )

    decision_factors = evidence["payload"]["decision_factors"]
    assert {
        key: decision_factors[key]
        for key in (
            "packet_id",
            "packet_hash",
            "assignment_id",
            "bet_id",
            "dispatch_id",
            "worker_id",
        )
    } == {
        "packet_id": packet["packet_id"],
        "packet_hash": _hash(packet),
        "assignment_id": manifest["assignment_id"],
        "bet_id": packet["bet_id"],
        "dispatch_id": dispatch_id,
        "worker_id": worker_id,
    }
    assert decision_factors["receipt_digest"] == receipt["receipt_digest"]

    mismatched_receipt = {
        **receipt,
        "receipt_id": "receipt-orch-other",
        "dispatch_id": "dispatch-other",
    }
    mismatched_receipt["receipt_digest"] = compute_packet_hash(
        canonicalize({key: value for key, value in mismatched_receipt.items() if key != "receipt_digest"})
    )
    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        OrchestrationContractCoordinator(tmp_path).record_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=manifest,
            transport_receipt=mismatched_receipt,
        )


def test_generic_candidate_without_mesh_dispatch_rejects_forged_fixture_worker(
    tmp_path,
):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    manifest = _manifest(packet)
    receipt = _transport_receipt(
        packet,
        manifest,
        step_run_id=step_run_id,
        dispatch_id="kandev:kandev-task-001",
        worker_id="kandev-fixture-agent",
    )

    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        OrchestrationContractCoordinator(tmp_path).record_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=manifest,
            transport_receipt=receipt,
        )

    assert "EvidenceRecorded" not in [event["event_type"] for event in WorkflowMeshStore(tmp_path).events()]


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("missing_binding", "spec_binding_invalid"),
        ("missing_file", "spec_ref_invalid"),
        ("absolute", "spec_ref_invalid"),
        ("traversal", "spec_ref_invalid"),
        ("digest_mismatch", "spec_digest_mismatch"),
        ("decision_unconfirmed", "spec_binding_invalid"),
        ("decision_unknown", "spec_binding_invalid"),
        ("decision_binding_mismatch", "spec_binding_invalid"),
        ("workspace_unavailable", "spec_binding_invalid"),
        ("outside_read_scope", "spec_ref_invalid"),
    ],
)
def test_v2_invalid_spec_binding_fails_closed_before_evidence(tmp_path, monkeypatch, case, reason):
    workspace_root = tmp_path / "workspace"
    omo_dir = workspace_root / ".omo"
    step_run_id = _seed_succeeded_run(omo_dir)
    packet = _v2_packet(workspace_root)
    monkeypatch.setattr("omo.orchestration_contract.WORKSPACE_ROOT", workspace_root)
    manifest_packet = packet
    if case == "missing_binding":
        packet.pop("spec_binding")
        manifest_packet = _packet()
    elif case == "missing_file":
        packet["spec_binding"] = {
            **packet["spec_binding"],
            "spec_ref": "repo://specs/missing.md",
        }
    elif case == "absolute":
        packet["spec_binding"] = {
            **packet["spec_binding"],
            "spec_ref": "repo:///tmp/spec.md",
        }
    elif case == "traversal":
        packet["spec_binding"] = {
            **packet["spec_binding"],
            "spec_ref": "repo://../outside.md",
        }
    elif case == "digest_mismatch":
        packet["spec_binding"] = {
            **packet["spec_binding"],
            "content_digest": "sha256:" + "0" * 64,
        }
    elif case == "decision_unconfirmed":
        packet["spec_binding"] = {
            **packet["spec_binding"],
            "decision_ref": "decision://proposed/BET-Y1Q2-T1-14",
        }
    elif case == "decision_unknown":
        packet["spec_binding"] = {
            **packet["spec_binding"],
            "decision_ref": "decision://accepted/BET-UNKNOWN",
        }
    elif case == "decision_binding_mismatch":
        ledger_path = workspace_root / "docs" / "plans" / "3y-bet-ledger.yaml"
        ledger_path.write_text(
            ledger_path.read_text().replace(packet["spec_binding"]["content_digest"], "sha256:" + "f" * 64),
            encoding="utf-8",
        )
    elif case == "outside_read_scope":
        packet["scope"] = {
            **packet["scope"],
            "read_surfaces": ["projects/omo/src/omo/"],
        }
    else:
        monkeypatch.setattr("omo.orchestration_contract.WORKSPACE_ROOT", None)

    with pytest.raises(OrchestrationContractError, match=reason):
        OrchestrationContractCoordinator(omo_dir).record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=_manifest(manifest_packet),
            fixture=_fixture(manifest_packet),
        )

    event_types = [event["event_type"] for event in WorkflowMeshStore(omo_dir).events()]
    assert "EvidenceRecorded" not in event_types
    assert "WorkflowVerified" not in event_types


def test_coordinator_rejects_caller_supplied_workspace_authority(tmp_path):
    with pytest.raises(TypeError, match="workspace_root"):
        OrchestrationContractCoordinator(  # type: ignore[call-arg]
            tmp_path / ".omo", workspace_root=tmp_path
        )


def test_v2_spec_digest_drift_after_collection_blocks_verification(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    omo_dir = workspace_root / ".omo"
    step_run_id = _seed_succeeded_run(omo_dir)
    packet = _v2_packet(workspace_root)
    monkeypatch.setattr("omo.orchestration_contract.WORKSPACE_ROOT", workspace_root)
    coordinator = OrchestrationContractCoordinator(omo_dir)
    coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )
    (workspace_root / "specs" / "orchestration-contract.md").write_text("# Mutated after collection\n")

    with pytest.raises(OrchestrationContractError, match="spec_digest_mismatch"):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=_receipt(packet),
        )

    event_types = [event["event_type"] for event in WorkflowMeshStore(omo_dir).events()]
    assert event_types.count("EvidenceRecorded") == 1
    assert "WorkflowVerified" not in event_types


@pytest.mark.parametrize(
    ("manifest", "reason"),
    [
        (
            lambda packet: _manifest(packet, packet_hash="sha256:" + "0" * 64),
            "packet_hash_mismatch",
        ),
        (
            lambda packet: _manifest(packet, changed_paths=["../../outside.py"]),
            "manifest_scope_violation",
        ),
    ],
)
def test_bad_candidate_never_records_evidence_or_verified(tmp_path, manifest, reason):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)

    with pytest.raises(OrchestrationContractError, match=reason):
        coordinator.record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=manifest(packet),
            fixture=_fixture(packet),
        )

    assert [event["event_type"] for event in WorkflowMeshStore(tmp_path).events()] == [
        "WorkflowRequested",
        "WorkflowAdmitted",
        "StepDispatched",
        "StepStarted",
        "WorkflowSucceeded",
    ]


def test_transport_failure_never_records_succeeded_evidence(tmp_path):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()

    with pytest.raises(OrchestrationContractError, match="transport_failed"):
        OrchestrationContractCoordinator(tmp_path).record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=_manifest(packet),
            fixture=_fixture(packet, state="failed"),
        )

    assert all(event["event_type"] != "EvidenceRecorded" for event in WorkflowMeshStore(tmp_path).events())


@pytest.mark.parametrize(
    ("verdict", "reason"),
    [("revise", "verification_revise"), ("reject", "verification_rejected")],
)
def test_non_accepting_verdict_never_appends_verified(tmp_path, verdict, reason):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)
    coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )

    with pytest.raises(OrchestrationContractError, match=reason):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=_receipt(packet, verdict=verdict),
        )

    assert WorkflowMeshStore(tmp_path).snapshot("run-orch")["state"] == "succeeded"


def test_same_receipt_replay_is_idempotent_but_conflicting_fixture_fails_closed(
    tmp_path,
):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)
    kwargs = {
        "workflow_run_id": "run-orch",
        "step_run_id": step_run_id,
        "packet": packet,
        "manifest": _manifest(packet),
    }

    first = coordinator.record_kandev_candidate(**kwargs, fixture=_fixture(packet))
    assert coordinator.record_kandev_candidate(**kwargs, fixture=_fixture(packet)) == first
    with pytest.raises(OrchestrationContractError, match="manifest_conflict"):
        coordinator.record_kandev_candidate(
            **kwargs,
            fixture=_fixture(packet, output_digest=hashlib.sha256(b"changed").hexdigest()),
        )

    receipt = _receipt(packet)
    verified = coordinator.accept_verification(
        workflow_run_id="run-orch",
        packet=packet,
        manifest=_manifest(packet),
        verification_receipt=receipt,
    )
    assert (
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=receipt,
        )
        == verified
    )
    assert [event["event_type"] for event in WorkflowMeshStore(tmp_path).events()].count("WorkflowVerified") == 1


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("workflow_run_id", "run-other"),
        ("packet_id", "WP-OTHER"),
        ("packet_hash", "sha256:" + "e" * 64),
        ("assignment_id", "ASG-OTHER"),
    ],
)
def test_fixture_identity_must_match_current_workflow_packet_and_assignment(tmp_path, field, wrong_value):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    bad_fixture = _fixture(packet)
    bad_fixture[field] = wrong_value

    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        OrchestrationContractCoordinator(tmp_path).record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=_manifest(packet),
            fixture=bad_fixture,
        )


def test_claim_or_check_change_for_same_external_task_is_a_manifest_conflict(tmp_path):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)
    fixture = _fixture(packet)
    coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=fixture,
    )
    changed = _manifest(packet)
    changed["checks"][0]["stdout_hash"] = "sha256:" + "b" * 64

    with pytest.raises(OrchestrationContractError, match="manifest_conflict"):
        coordinator.record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=changed,
            fixture=fixture,
        )


@pytest.mark.parametrize("mutation", ["duplicate_id", "wrong_assertion", "unbound_ref"])
def test_candidate_claims_must_be_directly_bound_to_packet_and_durable_artifacts(tmp_path, mutation):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    manifest = _manifest(packet)
    if mutation == "duplicate_id":
        manifest["claims"].append(dict(manifest["claims"][0]))
    elif mutation == "wrong_assertion":
        manifest["claims"][0]["assertion"] = "executor invented this assertion"
    else:
        manifest["claims"][0]["evidence_refs"] = ["git-object://" + "b" * 40]

    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        OrchestrationContractCoordinator(tmp_path).record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=manifest,
            fixture=_fixture(packet),
        )


def test_accept_requires_evidence_binds_candidate_hash_and_allows_independent_measurement(
    tmp_path,
):
    _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)

    with pytest.raises(OrchestrationContractError, match="evidence_missing"):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=_receipt(packet),
        )

    step_run_id = "run-orch:step-1"
    coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )
    with pytest.raises(OrchestrationContractError, match="packet_hash_mismatch"):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=_receipt(packet, candidate_packet_hash="sha256:" + "e" * 64),
        )
    accepted = coordinator.accept_verification(
        workflow_run_id="run-orch",
        packet=packet,
        manifest=_manifest(packet),
        verification_receipt=_receipt(packet, measured_packet_hash="sha256:" + "f" * 64),
    )
    assert accepted["event_type"] == "WorkflowVerified"


def test_candidate_must_cover_every_acceptance_id_and_verification_checks_must_pass(
    tmp_path,
):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    packet["acceptance"] = {
        **packet["acceptance"],
        "done_when": [
            *packet["acceptance"]["done_when"],
            {
                "id": "AC2",
                "assertion": "second measurement",
                "evidence_type": "test_result",
            },
        ],
    }
    coordinator = OrchestrationContractCoordinator(tmp_path)
    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        coordinator.record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=_manifest(packet),
            fixture=_fixture(packet),
        )

    baseline = _packet()
    coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=baseline,
        manifest=_manifest(baseline),
        fixture=_fixture(baseline),
    )
    receipt = _receipt(baseline)
    receipt.checks[0].returncode = 1
    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=baseline,
            manifest=_manifest(baseline),
            verification_receipt=receipt,
        )


@pytest.mark.parametrize("case", ["wrong_bet", "unknown_step", "not_succeeded"])
def test_candidate_must_bind_to_a_succeeded_mesh_run_bet_and_admitted_step(tmp_path, case):
    packet = _packet()
    run_id = "run-orch"
    step_run_id = _seed_succeeded_run(tmp_path, run_id)
    if case == "wrong_bet":
        packet["bet_id"] = "BET-OTHER"
    elif case == "unknown_step":
        step_run_id = "run-orch:step-unknown"
    else:
        run_id = "run-incomplete"
        step_run_id = "run-incomplete:step-1"
        WorkflowMeshStore(tmp_path).append(
            new_workflow_event("WorkflowRequested", run_id, payload={"bet_id": packet["bet_id"]})
        )

    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        OrchestrationContractCoordinator(tmp_path).record_kandev_candidate(
            workflow_run_id=run_id,
            step_run_id=step_run_id,
            packet=packet,
            manifest=_manifest(packet),
            fixture=_fixture(packet, workflow_run_id=run_id),
        )


@pytest.mark.parametrize(
    "case",
    [
        "failed_check",
        "empty_evidence",
        "budget_files",
        "changed_path_count",
        "count_mismatch",
        "missing_artifact",
        "bad_artifact",
        "duplicate_path",
        "prefix_escape",
    ],
)
def test_candidate_manifest_requires_passing_checks_claim_evidence_and_budget(tmp_path, case):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    manifest = _manifest(packet)
    if case == "failed_check":
        manifest["checks"] = [build_command_check(["pytest", "-q"], 1, "fixture red")]
    elif case == "empty_evidence":
        manifest["claims"] = [
            {
                "acceptance_id": "AC1",
                "assertion": "candidate ready",
                "evidence_refs": [],
            }
        ]
    elif case == "budget_files":
        manifest["surface_delta"] = {"files": 3, "loc": 20}
    elif case == "count_mismatch":
        manifest["surface_delta"] = {"files": 2, "loc": 20}
    elif case == "missing_artifact":
        manifest["artifact_refs"] = []
    elif case == "bad_artifact":
        manifest["artifact_refs"] = ["file:///tmp/not-durable"]
    elif case == "duplicate_path":
        packet["scope"] = {**packet["scope"], "write_surfaces": ["README.md"]}
        packet["budgets"] = {**packet["budgets"], "max_changed_files": 2}
        manifest = _manifest(packet, changed_paths=["README.md", "README.md"])
        manifest["surface_delta"] = {"files": 1, "loc": 20}
    elif case == "prefix_escape":
        manifest = _manifest(packet, changed_paths=["projects/omo/src/omox/evil.py"])
    else:
        packet["scope"] = {
            **packet["scope"],
            "write_surfaces": ["README.md", "NOTICE.md", "LICENSE.md"],
        }
        packet["budgets"] = {**packet["budgets"], "max_changed_files": 2}
        manifest = _manifest(packet, changed_paths=["README.md", "NOTICE.md", "LICENSE.md"])

    expected_reason = "manifest_scope_violation" if case == "prefix_escape" else "verification_unprovable"
    with pytest.raises(OrchestrationContractError, match=expected_reason):
        OrchestrationContractCoordinator(tmp_path).record_kandev_candidate(
            workflow_run_id="run-orch",
            step_run_id=step_run_id,
            packet=packet,
            manifest=manifest,
            fixture=_fixture(packet),
        )


def test_mutated_verification_receipt_with_stale_hash_is_unprovable(tmp_path):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)
    coordinator.record_kandev_candidate(
        workflow_run_id="run-orch",
        step_run_id=step_run_id,
        packet=packet,
        manifest=_manifest(packet),
        fixture=_fixture(packet),
    )

    revised = _receipt(packet, verdict="revise")
    revised.verdict = "accept"
    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=revised,
        )

    failed_check = build_verification_receipt(
        packet=packet,
        candidate_packet_hash=_hash(packet),
        measured_packet_hash="sha256:" + "f" * 64,
        executor_model_family="fixture-executor",
        verifier_model_family="fixture-verifier",
        verdict="accept",
        checks=[build_command_check(["pytest", "-q"], 1, "failed verification")],
    )
    stale_hash = failed_check.receipt_hash
    failed_check.checks[0].returncode = 0
    assert failed_check.receipt_hash == stale_hash
    with pytest.raises(OrchestrationContractError, match="verification_unprovable"):
        coordinator.accept_verification(
            workflow_run_id="run-orch",
            packet=packet,
            manifest=_manifest(packet),
            verification_receipt=failed_check,
        )


def test_verified_run_replays_identical_evidence_but_rejects_changed_candidate(
    tmp_path,
):
    step_run_id = _seed_succeeded_run(tmp_path)
    packet = _packet()
    coordinator = OrchestrationContractCoordinator(tmp_path)
    kwargs = {
        "workflow_run_id": "run-orch",
        "step_run_id": step_run_id,
        "packet": packet,
        "manifest": _manifest(packet),
    }
    first = coordinator.record_kandev_candidate(**kwargs, fixture=_fixture(packet))
    coordinator.accept_verification(
        workflow_run_id="run-orch",
        packet=packet,
        manifest=_manifest(packet),
        verification_receipt=_receipt(packet),
    )

    assert coordinator.record_kandev_candidate(**kwargs, fixture=_fixture(packet)) == first
    with pytest.raises(OrchestrationContractError, match="manifest_conflict"):
        coordinator.record_kandev_candidate(
            **kwargs,
            fixture=_fixture(
                packet,
                output_digest=hashlib.sha256(b"changed after verify").hexdigest(),
            ),
        )


def test_source_receipt_cannot_cross_run_or_assignment_binding(tmp_path):
    packet = _packet()
    first_step = _seed_succeeded_run(tmp_path, "run-first")
    second_step = _seed_succeeded_run(tmp_path, "run-second")
    coordinator = OrchestrationContractCoordinator(tmp_path)
    receipt = _receipt(packet)
    first_manifest = _manifest(packet)
    coordinator.record_kandev_candidate(
        workflow_run_id="run-first",
        step_run_id=first_step,
        packet=packet,
        manifest=first_manifest,
        fixture=_fixture(packet, workflow_run_id="run-first", step_run_id=first_step),
    )
    coordinator.accept_verification(
        workflow_run_id="run-first",
        packet=packet,
        manifest=first_manifest,
        verification_receipt=receipt,
    )

    second_manifest = {**_manifest(packet), "assignment_id": "ASG-ORCH-002"}
    coordinator.record_kandev_candidate(
        workflow_run_id="run-second",
        step_run_id=second_step,
        packet=packet,
        manifest=second_manifest,
        fixture=_fixture(
            packet,
            workflow_run_id="run-second",
            assignment_id="ASG-ORCH-002",
            step_run_id=second_step,
        ),
    )
    with pytest.raises(OrchestrationContractError, match="manifest_conflict"):
        coordinator.accept_verification(
            workflow_run_id="run-second",
            packet=packet,
            manifest=second_manifest,
            verification_receipt=receipt,
        )
