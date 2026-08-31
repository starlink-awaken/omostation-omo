import hashlib
import json
from pathlib import Path

import pytest

from omo.omo_cockpit_bridge import (
    approve_hitl_proposal_async,
    list_hitl_proposals,
    record_hitl_proposal,
    reject_hitl_proposal,
)
from omo.omo_shared import load_yaml


def _proposal(expected_change: str = "create controlled canary") -> dict[str, object]:
    base: dict[str, object] = {
        "id": "family-write-1",
        "type": "family_dashboard_document_write",
        "debt_id": "family-dashboard-content",
        "source": "family-dashboard",
        "target": "documents://family/_knowledge/canary.md",
        "expected_change": expected_change,
        "operation_level": "L3",
        "approval_required": True,
        "rollback": "remove canary after verified apply",
        "verification": "verify receipt digest",
        "auto_apply": "disabled",
        "operation": "replace_text",
        "target_relative": "_knowledge/canary.md",
        "payload_ref": "proposals/family-write-1/payload",
        "payload_sha256": "sha256:" + "a" * 64,
        "payload_bytes": 4,
    }
    canonical = json.dumps(base, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return {**base, "proposal_digest": "sha256:" + hashlib.sha256(canonical).hexdigest()}


def test_record_is_exclusive_idempotent_and_pathless(tmp_path: Path) -> None:
    omo = tmp_path / ".omo"
    first = record_hitl_proposal(
        omo,
        _proposal(),
        requested_by="service://family-dashboard",
        now="2026-08-30T23:00:00Z",
    )
    second = record_hitl_proposal(
        omo,
        _proposal(),
        requested_by="service://family-dashboard",
        now="2026-08-30T23:00:00Z",
    )
    assert first == second
    assert list_hitl_proposals(omo)[0]["status"] == "pending"
    assert "/Users/" not in json.dumps(first)
    with pytest.raises(ValueError, match="proposal id collision"):
        record_hitl_proposal(
            omo,
            _proposal("different controlled canary"),
            requested_by="service://family-dashboard",
            now="2026-08-30T23:00:00Z",
        )


@pytest.mark.asyncio
async def test_approve_binds_principal_and_archives_execution_receipt(tmp_path: Path) -> None:
    omo = tmp_path / ".omo"
    record_hitl_proposal(
        omo,
        _proposal(),
        requested_by="service://family-dashboard",
        now="2026-08-30T23:00:00Z",
    )

    def execute(proposal: dict[str, object]) -> dict[str, object]:
        processing = omo / "state" / "proposals" / "family-write-1.processing"
        assert load_yaml(processing) == proposal
        return {
            "status": "verified",
            "verify_receipt_ref": "mutations/1/verify.json",
            "verify_receipt_sha256": "sha256:" + "c" * 64,
        }

    success, error, receipt = await approve_hitl_proposal_async(
        omo,
        "family-write-1",
        principal_ref="operator://cockpit-api/abc",
        approved_at="2026-08-30T23:01:00Z",
        execute_mutation=execute,
    )
    assert success is True and error is None and receipt is not None
    assert receipt["status"] == "verified"
    assert receipt["approved_by"] == "operator://cockpit-api/abc"
    assert not (omo / "state" / "proposals" / "family-write-1.yaml").exists()
    assert (omo / "_delivery" / "hitl" / "family-dashboard" / "family-write-1.yaml").is_file()


@pytest.mark.asyncio
async def test_failed_execution_restores_pending_proposal(tmp_path: Path) -> None:
    omo = tmp_path / ".omo"
    record_hitl_proposal(omo, _proposal(), requested_by="service://family-dashboard", now="2026-08-30T23:00:00Z")
    success, error, receipt = await approve_hitl_proposal_async(
        omo,
        "family-write-1",
        principal_ref="operator://cockpit-api/abc",
        approved_at="2026-08-30T23:01:00Z",
        execute_mutation=lambda _proposal: {"status": "error"},
    )
    assert success is False and error and receipt is None
    assert (omo / "state" / "proposals" / "family-write-1.yaml").is_file()
    assert not (omo / "_delivery" / "hitl" / "family-dashboard" / "family-write-1.yaml").exists()


@pytest.mark.asyncio
async def test_approve_rejects_invalid_proposal_id_without_path_access(tmp_path: Path) -> None:
    success, error, receipt = await approve_hitl_proposal_async(
        tmp_path / ".omo",
        "../escape",
        principal_ref="operator://cockpit-api/abc",
        approved_at="2026-08-30T23:01:00Z",
        execute_mutation=lambda _proposal: {"status": "verified"},
    )
    assert success is False and error == "proposal id invalid" and receipt is None


@pytest.mark.asyncio
async def test_absolute_runtime_receipt_ref_is_rejected_and_pending_is_restored(tmp_path: Path) -> None:
    omo = tmp_path / ".omo"
    record_hitl_proposal(omo, _proposal(), requested_by="service://family-dashboard", now="2026-08-30T23:00:00Z")
    success, error, receipt = await approve_hitl_proposal_async(
        omo,
        "family-write-1",
        principal_ref="operator://cockpit-api/abc",
        approved_at="2026-08-30T23:01:00Z",
        execute_mutation=lambda _proposal: {
            "status": "verified",
            "verify_receipt_ref": "/Users/private/receipt.json",
            "verify_receipt_sha256": "sha256:" + "c" * 64,
        },
    )
    assert success is False and error == "runtime receipt invalid" and receipt is None
    assert (omo / "state" / "proposals" / "family-write-1.yaml").is_file()


def test_reject_archives_terminal_receipt_before_queue_cleanup(tmp_path: Path) -> None:
    omo = tmp_path / ".omo"
    record_hitl_proposal(omo, _proposal(), requested_by="service://family-dashboard", now="2026-08-30T23:00:00Z")
    receipt = reject_hitl_proposal(
        omo,
        "family-write-1",
        principal_ref="operator://cockpit-api/abc",
        rejected_at="2026-08-30T23:02:00Z",
    )
    assert receipt["status"] == "rejected"
    assert not (omo / "state" / "proposals" / "family-write-1.yaml").exists()
    assert load_yaml(omo / "_delivery" / "hitl" / "family-dashboard" / "family-write-1.yaml") == receipt


def test_record_rejects_secret_like_values(tmp_path: Path) -> None:
    proposal = _proposal()
    proposal["expected_change"] = "token=raw-secret"
    canonical = {key: value for key, value in proposal.items() if key != "proposal_digest"}
    encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    proposal["proposal_digest"] = "sha256:" + hashlib.sha256(encoded).hexdigest()
    with pytest.raises(ValueError, match="secret-like"):
        record_hitl_proposal(
            tmp_path / ".omo",
            proposal,
            requested_by="service://family-dashboard",
            now="2026-08-30T23:00:00Z",
        )
