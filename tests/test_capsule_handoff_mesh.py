from pathlib import Path

import pytest

from omo.workflow.capsule import CapsuleError, seal_capsule
from omo.workflow_mesh import WorkflowMeshStore, record_capsule_handoff


def _capsule():
    return seal_capsule(
        capsule_id="capsule:handoff-001",
        bet_id="BET-Y1Q4-T10-165",
        work_packet_hash="sha256:" + "a" * 64,
        receipt_digest="sha256:" + "b" * 64,
        producer_role="role:cell-alpha",
        consumer_role="role:cell-beta",
        payload_digest="sha256:" + "c" * 64,
    )


def test_verified_capsule_records_mesh_handoff(tmp_path: Path) -> None:
    store = WorkflowMeshStore(tmp_path / ".omo")
    capsule = _capsule()
    result = record_capsule_handoff(store, capsule)

    assert result["capsule_digest"] == capsule.digest
    assert result["receipt_digest"] == capsule.receipt_digest
    event = store.events()[0]
    assert event["event_type"] == "HandoffRecorded"
    assert event["payload"]["from_role"] == "role:cell-alpha"
    assert event["payload"]["to_role"] == "role:cell-beta"
    assert event["payload"]["handoff"]["capsule"]["digest"] == capsule.digest


def test_invalid_capsule_never_enters_mesh(tmp_path: Path) -> None:
    store = WorkflowMeshStore(tmp_path / ".omo")
    capsule = _capsule()
    tampered = capsule.__class__(**{**capsule.__dict__, "producer_role": "role:cell-evil"})

    with pytest.raises(CapsuleError, match="digest 不匹配"):
        record_capsule_handoff(store, tampered)
    assert store.events() == []
