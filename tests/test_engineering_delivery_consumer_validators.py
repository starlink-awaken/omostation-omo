from datetime import UTC, datetime

from omo.engineering_delivery_consumer_validators import _utc_now


def test_validators_export_canonical_utc_now() -> None:
    value = _utc_now()
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    assert value.endswith("Z")
    assert parsed.tzinfo == UTC
    assert parsed.microsecond == 0
