from __future__ import annotations

import hashlib
import json

import pytest

from omo.cli import main as cli_main
from omo.omo_external_receipt import (
    ExternalReceiptError,
    record_external_receipt,
    record_native_execution_receipt,
)
from omo.workflow_mesh import (
    WorkflowMeshEventError,
    WorkflowMeshStore,
    new_workflow_event,
)

NOW = "2026-08-02T09:00:00Z"


def _grant(run_id: str, step_run_id: str) -> dict[str, object]:
    grant: dict[str, object] = {
        "admission_id": f"adm-{run_id}",
        "status": "admitted",
        "workflow_run_id": run_id,
        "trace_id": run_id,
        "backend": "external-receipt-test",
        "step_run_ids": [step_run_id],
        "capabilities": ["search"],
        "policy_digest": "external-connection-fabric/v1",
        "issued_at": NOW,
        "expires_at": "2026-08-02T10:00:00Z",
    }
    unsigned = json.dumps(grant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    grant["proof"] = hashlib.sha256(unsigned).hexdigest()
    return grant


def _seed_succeeded_run(tmp_path, run_id: str = "run-receipt") -> tuple[WorkflowMeshStore, str]:
    step_run_id = f"{run_id}:step-1"
    grant = _grant(run_id, step_run_id)
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", run_id))
    store.append(new_workflow_event("WorkflowAdmitted", run_id, payload={"admission": grant, **grant}))
    context = {"step_run_id": step_run_id, "admission_id": grant["admission_id"]}
    store.append(new_workflow_event("StepDispatched", run_id, payload=context))
    store.append(new_workflow_event("StepStarted", run_id, payload=context))
    store.append(new_workflow_event("WorkflowSucceeded", run_id))
    return store, step_run_id


def _receipt(result_state: str = "succeeded") -> dict[str, object]:
    return {
        "receipt_id": "receipt-1",
        "trace_id": "trace-1",
        "resource_id": "source:test",
        "operation": "search",
        "result_state": result_state,
        "observed_at": NOW,
        "provenance_ref": "test://source",
        "policy_digest": "external-connection-fabric/v1",
        "decision_factors": {"health": "healthy", "freshness": 1},
        "output_digest": "a" * 64,
    }


def _native_receipt(run_id: str = "run-receipt", step_run_id: str | None = None) -> dict[str, object]:
    step = step_run_id or f"{run_id}:step-1"
    return {
        "schema": "native-execution-receipt/v1",
        "status": "completed",
        "invocation_id": "sha256:" + "1" * 64,
        "material": {
            "schema": "native-execution-material/v1",
            "binding": {
                "correlation_id": "corr-1",
                "workflow_run_id": run_id,
                "packet_id": "packet-1",
                "packet_hash": "sha256:" + "2" * 64,
                "assignment_id": "assignment-1",
                "dispatch_id": "dispatch-1",
                "actor_id": "actor-1",
                "delivery_attempt_id": "attempt-1",
            },
            "capability": {"kind": "bos_service", "id": "bos-service:bos://governance/shared"},
            "inspection": {
                "receipt_digest": "sha256:" + "3" * 64,
                "source_digest": "sha256:" + "4" * 64,
            },
            "operation_id": "governance.shared.read",
            "request_digest": "sha256:" + "5" * 64,
            "admission": {
                "receipt_digest": "sha256:" + "6" * 64,
                "admission_id": "admission-1",
                "step_run_id": step,
                "worker": {"status": "not_applicable", "id": None},
            },
            "authorization_source": "bos-pep",
            "effect_classification": "read_only",
            "execution_attempt": 1,
        },
        "transport_state": "confirmed",
        "outcome": {
            "status": "succeeded",
            "failure_code": None,
            "result_digest": "sha256:" + "7" * 64,
        },
        "action_receipt": {"status": "not_applicable", "id": None, "digest": None},
        "cleanup_proof": {},
        "cleanup_digest": "sha256:" + "8" * 64,
        "fallback": {"used": False},
        "states": {"invoked": True, "evidenced": False, "independently_verified": False},
        "value_indicator_policy": False,
        "receipt_digest": "sha256:" + "9" * 64,
    }


def test_native_receipt_consumer_maps_verified_execution_to_mesh_evidence(tmp_path):
    store, step_run_id = _seed_succeeded_run(tmp_path)

    first = record_native_execution_receipt(
        tmp_path,
        _native_receipt(step_run_id=step_run_id),
        workflow_run_id="run-receipt",
        step_run_id=step_run_id,
    )
    repeated = record_native_execution_receipt(
        tmp_path,
        _native_receipt(step_run_id=step_run_id),
        workflow_run_id="run-receipt",
        step_run_id=step_run_id,
    )

    assert repeated == first
    evidence = store.evidence_snapshot("run-receipt", "external:bos-service:bos://governance/shared:sha256:" + "9" * 64)
    assert evidence is not None
    assert evidence["evidence_schema"] == "external-connection-receipt/v1"
    assert evidence["resource_id"] == "bos-service:bos://governance/shared"
    assert evidence["sha256"] == "7" * 64
    assert evidence["decision_factors"]["native_receipt_schema"] == "native-execution-receipt/v1"


def test_native_receipt_consumer_rejects_wrong_trace_or_unconfirmed_execution(tmp_path):
    _seed_succeeded_run(tmp_path)
    with pytest.raises(ExternalReceiptError, match="workflow_run_id"):
        record_native_execution_receipt(tmp_path, _native_receipt("other-run"), workflow_run_id="run-receipt")

    failed = _native_receipt()
    failed["transport_state"] = "uncertain"
    failed["outcome"] = {"status": "unknown", "failure_code": None, "result_digest": None}
    with pytest.raises(ExternalReceiptError, match="confirmed succeeded"):
        record_native_execution_receipt(tmp_path, failed, workflow_run_id="run-receipt")


def test_native_receipt_cli_uses_production_consumer(tmp_path, capsys):
    store, step_run_id = _seed_succeeded_run(tmp_path, "run-native-cli")
    receipt_file = tmp_path / "native-receipt.json"
    receipt_file.write_text(json.dumps(_native_receipt("run-native-cli", step_run_id)), encoding="utf-8")

    assert (
        cli_main(
            [
                "worker",
                "external-receipt",
                "run-native-cli",
                "--receipt-file",
                str(receipt_file),
                "--step-run-id",
                step_run_id,
                "--omo-dir",
                str(tmp_path),
                "--json",
            ]
        )
        == 0
    )
    event = json.loads(capsys.readouterr().out)
    assert event["payload"]["evidence_schema"] == "external-connection-receipt/v1"
    assert len(store.events()) == 6


def test_receipt_broker_records_safe_evidence_and_is_idempotent(tmp_path):
    store, step_run_id = _seed_succeeded_run(tmp_path)

    first = record_external_receipt(
        tmp_path,
        _receipt(),
        workflow_run_id="run-receipt",
        step_run_id=step_run_id,
    )
    repeated = record_external_receipt(
        tmp_path,
        _receipt(),
        workflow_run_id="run-receipt",
        step_run_id=step_run_id,
    )

    assert repeated == first
    assert len(store.events()) == 6
    evidence = store.evidence_snapshot("run-receipt", "external:source:test:receipt-1")
    assert evidence is not None
    assert evidence["sha256"] == "a" * 64
    assert evidence["decision_factors"] == {"health": "healthy", "freshness": 1}
    assert "output" not in evidence


def test_receipt_broker_rejects_failed_and_raw_receipts(tmp_path):
    _seed_succeeded_run(tmp_path)

    with pytest.raises(ExternalReceiptError, match="only succeeded/degraded"):
        record_external_receipt(tmp_path, _receipt("failed"), workflow_run_id="run-receipt")

    raw = _receipt()
    raw["raw_output"] = "must never enter the event"
    with pytest.raises(ExternalReceiptError, match="forbidden"):
        record_external_receipt(tmp_path, raw, workflow_run_id="run-receipt")


def test_receipt_retry_conflict_is_fail_closed(tmp_path):
    _seed_succeeded_run(tmp_path)
    record_external_receipt(tmp_path, _receipt(), workflow_run_id="run-receipt")
    changed = _receipt()
    changed["output_digest"] = "b" * 64

    with pytest.raises(WorkflowMeshEventError, match="Conflicting duplicate"):
        record_external_receipt(tmp_path, changed, workflow_run_id="run-receipt")


def test_receipt_broker_keeps_mesh_fail_closed_without_success(tmp_path):
    store = WorkflowMeshStore(tmp_path)
    store.append(new_workflow_event("WorkflowRequested", "run-incomplete"))

    with pytest.raises(WorkflowMeshEventError):
        record_external_receipt(tmp_path, _receipt(), workflow_run_id="run-incomplete")
    assert len(store.events()) == 1


def test_external_receipt_cli_records_json_event(tmp_path, capsys):
    store, step_run_id = _seed_succeeded_run(tmp_path, "run-cli")
    receipt_file = tmp_path / "receipt.json"
    receipt_file.write_text(json.dumps(_receipt()), encoding="utf-8")

    assert (
        cli_main(
            [
                "worker",
                "external-receipt",
                "run-cli",
                "--receipt-file",
                str(receipt_file),
                "--step-run-id",
                step_run_id,
                "--omo-dir",
                str(tmp_path),
                "--json",
            ]
        )
        == 0
    )
    event = json.loads(capsys.readouterr().out)
    assert event["event_type"] == "EvidenceRecorded"
    assert len(store.events()) == 6
