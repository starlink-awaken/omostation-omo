import pytest

from omo.engineering_delivery_consumer_constants import MOS_PROJECTION_RECEIPT_LOG
from omo.engineering_delivery_consumer_projection import (
    _append_projection_status,
    _validate_primary_records,
    _validate_projection_receipt,
    _workspace_root,
)
from omo.engineering_delivery_consumer_shadow import _shadow_observer_relative_parts
from omo.omo_io import AppendOnlyLog
from omo.engineering_delivery_consumer_validators import EngineeringDeliveryConsumerError


def test_projection_status_writer_preserves_legacy_log_contract(tmp_path) -> None:
    log = AppendOnlyLog(tmp_path / MOS_PROJECTION_RECEIPT_LOG)

    receipt = _append_projection_status(
        log,
        [],
        decision_outcome_id="outcome-1",
        status="pending",
    )

    assert receipt["schema"] == "engineering-delivery-mos-projection/v1"
    assert receipt["decision_outcome_id"] == "outcome-1"
    assert receipt["status"] == "pending"
    assert receipt["mos_decision_id"] is None
    assert receipt["error_code"] is None
    assert _validate_projection_receipt(receipt) == receipt


def test_workspace_root_supports_omo_directory_and_temp_domain_root(tmp_path) -> None:
    assert _workspace_root(tmp_path) == tmp_path
    assert _workspace_root(tmp_path / ".omo") == tmp_path


def test_shadow_paths_normalize_workspace_symlink(tmp_path) -> None:
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)

    assert _shadow_observer_relative_parts(
        alias / "evidence.jsonl",
        workspace_root=tmp_path,
    ) == ("evidence.jsonl",)


def test_primary_records_fail_closed_on_malformed_record(tmp_path) -> None:
    with pytest.raises(EngineeringDeliveryConsumerError):
        _validate_primary_records([{"raw": "not-json"}], tmp_path)
