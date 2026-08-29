import sys
from pathlib import Path
from types import ModuleType

from omo.event_ledger import LedgerBroker
from omo.event_ledger.publisher import publish_due, publish_to_bus, run_once


def _broker(tmp_path: Path) -> LedgerBroker:
    return LedgerBroker.connect(tmp_path / "ledger.db")


def _append(broker: LedgerBroker) -> str:
    broker.append(
        event_type="SignalObserved.v1",
        producer="publisher-test",
        principal_id="principal-1",
        space_id="space-1",
        correlation_id="correlation-1",
        idempotency_key="event-1",
        payload={"hello": "world"},
        occurred_at="2026-08-29T00:00:00Z",
        destinations=("bos://test",),
    )
    return str(broker.read()[0]["event_id"])


def test_publish_due_claims_and_persists_success_receipt(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    event_id = _append(broker)
    now = str(broker.outbox_entries()[0]["next_attempt_at"])
    calls: list[tuple[str, dict, str]] = []

    results = publish_due(
        broker,
        "bos://test",
        lambda current_event_id, payload, destination: (
            calls.append((current_event_id, payload, destination)) or "receipt-1"
        ),
        worker_id="worker-1",
        now=now,
    )

    assert len(calls) == 1
    assert calls[0][0] == event_id
    assert results[0].state == "sent"
    assert results[0].receipt_id == "receipt-1"
    entry = broker.outbox_entries()[0]
    assert entry["state"] == "sent"
    assert entry["receipt_id"] == "receipt-1"
    broker.close()


def test_publish_due_replays_sent_without_second_publish(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    _append(broker)
    now = str(broker.outbox_entries()[0]["next_attempt_at"])
    calls = 0

    def publish(_event_id: str, _payload: dict, _destination: str) -> str:
        nonlocal calls
        calls += 1
        return "receipt-1"

    first = publish_due(
        broker,
        "bos://test",
        publish,
        worker_id="worker-1",
        now=now,
    )
    second = publish_due(
        broker,
        "bos://test",
        publish,
        worker_id="worker-2",
        now="2026-08-29T00:00:01Z",
    )

    assert calls == 1
    assert first[0].receipt_id == second[0].receipt_id == "receipt-1"
    broker.close()


def test_publish_due_uses_strict_backoff_and_dead_letters_fifth_failure(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    _append(broker)
    now = str(broker.outbox_entries()[0]["next_attempt_at"])
    attempts = 0
    results = []

    def publish(_event_id: str, _payload: dict, _destination: str) -> str:
        nonlocal attempts
        attempts += 1
        raise ValueError("deterministic rejection")

    for expected_delay in (5, 30, 120, 600):
        result = publish_due(broker, "bos://test", publish, worker_id="worker-1", now=now)[0]
        results.append(result)
        assert result.state == "pending"
        next_time = result.next_attempt_at
        assert next_time == _add_seconds(now, expected_delay)
        now = next_time

    final = publish_due(broker, "bos://test", publish, worker_id="worker-1", now=now)[0]
    assert attempts == 5
    assert final.state == "failed"
    assert final.receipt_id is not None
    assert final.error_class == "ValueError"
    broker.close()


def test_publish_due_keeps_timeout_uncertain_then_retries(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    _append(broker)
    now = str(broker.outbox_entries()[0]["next_attempt_at"])
    calls = 0

    def publish(_event_id: str, _payload: dict, _destination: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("transport timeout")
        return "receipt-2"

    first = publish_due(broker, "bos://test", publish, worker_id="worker-1", now=now)[0]
    assert first.state == "uncertain"
    assert first.receipt_id is None
    assert first.error_class == "TimeoutError"

    second = publish_due(
        broker,
        "bos://test",
        publish,
        worker_id="worker-1",
        now=first.next_attempt_at,
    )[0]
    assert calls == 2
    assert second.state == "sent"
    assert second.receipt_id == "receipt-2"
    broker.close()


def test_publish_due_concurrent_workers_call_publish_once(tmp_path: Path) -> None:
    import threading
    import time

    broker = _broker(tmp_path)
    _append(broker)
    now = str(broker.outbox_entries()[0]["next_attempt_at"])
    broker.close()
    calls = 0
    calls_lock = threading.Lock()

    def publish(_event_id: str, _payload: dict, _destination: str) -> str:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.02)
        return "receipt-concurrent"

    def run(worker_id: str) -> None:
        local = _broker(tmp_path)
        publish_due(local, "bos://test", publish, worker_id=worker_id, now=now)
        local.close()

    first = threading.Thread(target=run, args=("worker-1",))
    second = threading.Thread(target=run, args=("worker-2",))
    first.start()
    second.start()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert calls == 1


def _add_seconds(value: str, seconds: int) -> str:
    from datetime import UTC, datetime, timedelta

    return (
        (datetime.fromisoformat(value.replace("Z", "+00:00")) + timedelta(seconds=seconds))
        .astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def test_run_once_reuses_canonical_publisher(tmp_path: Path) -> None:
    broker = _broker(tmp_path)
    _append(broker)
    now = str(broker.outbox_entries()[0]["next_attempt_at"])

    result = run_once(
        broker,
        "bos://test",
        lambda _event_id, _payload, _destination: "receipt-run-once",
        worker_id="worker-1",
        now=now,
    )

    assert result[0].receipt_id == "receipt-run-once"
    broker.close()


def test_publish_to_bus_returns_the_real_bus_event_id(monkeypatch) -> None:
    captured = {}

    class FakeEnvelope:
        def __init__(self, **kwargs):
            captured["envelope"] = kwargs

    class FakePlane:
        EVENT = "EVENT"

    bus_module = ModuleType("bus_foundation")
    bus_module.publish = lambda envelope: "bus-event-1"
    envelope_module = ModuleType("bus_foundation.envelope")
    envelope_module.OmniEnvelope = FakeEnvelope
    envelope_module.OmniPlane = FakePlane
    monkeypatch.setitem(sys.modules, "bus_foundation", bus_module)
    monkeypatch.setitem(sys.modules, "bus_foundation.envelope", envelope_module)

    receipt = publish_to_bus("event-1", {"hello": "world"}, "bos://test")

    assert receipt == "bus-event-1"
    assert captured["envelope"]["topic"] == "bos://test"
    assert captured["envelope"]["payload"]["event_id"] == "event-1"
