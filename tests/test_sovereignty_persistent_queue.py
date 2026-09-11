"""BET-Y1Q4-T10-147 — persistent, ledger-backed Queue.

Covers priority ordering, FIFO tiebreak, double-dequeue rejection, malformed
row rejection, kill/reopen durability and queue_id isolation. All writes go
through LedgerBroker.append via PersistentQueue; all reads replay the ledger.
"""

from __future__ import annotations

import pytest

from omo.event_ledger.broker import DuplicateEventError, LedgerBroker
from omo.sovereignty.persistent_queue import (
    EVT_ENQUEUED,
    PRODUCER,
    PersistentQueue,
    QueueReplayError,
)


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "queue.db"


@pytest.fixture()
def queue(db_path):
    q = PersistentQueue.open(db_path, "queue:test")
    yield q
    q.close()


def test_empty_queue_dequeue_and_peek_return_none(queue):
    assert queue.dequeue() is None
    assert queue.peek() is None
    assert queue.pending_count() == 0


def test_priority_ordering(queue):
    queue.enqueue("low", priority=1)
    queue.enqueue("high", priority=5)
    queue.enqueue("mid", priority=3)

    _, first = queue.dequeue()
    _, second = queue.dequeue()
    _, third = queue.dequeue()
    assert [first, second, third] == ["high", "mid", "low"]
    assert queue.dequeue() is None


def test_fifo_tiebreak_within_same_priority(queue):
    queue.enqueue("first", priority=2)
    queue.enqueue("second", priority=2)
    queue.enqueue("third", priority=2)

    _, a = queue.dequeue()
    _, b = queue.dequeue()
    _, c = queue.dequeue()
    assert [a, b, c] == ["first", "second", "third"]


def test_default_priority_is_zero_and_orders_below_positive(queue):
    queue.enqueue("default")
    queue.enqueue("boosted", priority=1)

    _, first = queue.dequeue()
    _, second = queue.dequeue()
    assert [first, second] == ["boosted", "default"]


def test_peek_does_not_mutate_state(queue):
    queue.enqueue("only")
    item_id, item = queue.peek()
    assert item == "only"
    assert queue.pending_count() == 1
    # peek again returns the exact same item, dequeue() then actually pops it
    assert queue.peek() == (item_id, item)
    assert queue.dequeue() == (item_id, item)
    assert queue.pending_count() == 0


def test_double_dequeue_of_same_item_is_rejected(queue):
    item_id = queue.enqueue("solo")
    assert queue.dequeue() == (item_id, "solo")

    # A normal second dequeue() call sees an empty pending set (the item is
    # already consumed) and returns None — it never re-examines a specific
    # item_id, so this is not where duplicate rejection is exercised.
    assert queue.dequeue() is None

    # The actual guard is at the ledger level: re-appending the exact same
    # ItemDequeued event for an item_id that was already dequeued (e.g. a
    # racing concurrent caller that computed the same pending head before
    # either commit) must raise, not silently double-pop.
    with pytest.raises(DuplicateEventError):
        queue._broker.append(
            event_type="PersistentQueue.ItemDequeued.v1",
            producer=PRODUCER,
            principal_id=queue.queue_id,
            space_id="persistent-queue",
            correlation_id=f"queue|{queue.queue_id}|{item_id}",
            idempotency_key=f"{item_id}|dequeue",
            payload={"queue_id": queue.queue_id, "item_id": item_id},
        )


def test_malformed_row_raises_queue_replay_error_not_silently_skipped(db_path):
    broker = LedgerBroker.connect(db_path)
    try:
        # Append a structurally-valid-JSON but semantically malformed
        # ItemEnqueued row directly (missing "priority").
        broker.append(
            event_type=EVT_ENQUEUED,
            producer=PRODUCER,
            principal_id="queue:malformed",
            space_id="persistent-queue",
            correlation_id="queue|queue:malformed|bad",
            idempotency_key="qitem:bad",
            payload={"queue_id": "queue:malformed", "item_id": "qitem:bad", "item": "x"},
        )
    finally:
        broker.close()

    q = PersistentQueue.open(db_path, "queue:malformed")
    try:
        with pytest.raises(QueueReplayError):
            q.pending_count()
    finally:
        q.close()


def test_kill_and_reopen_durability(db_path):
    q1 = PersistentQueue.open(db_path, "queue:durable")
    q1.enqueue("keep-1", priority=1)
    q1.enqueue("drop-me", priority=0)
    q1.enqueue("keep-2", priority=2)
    popped_id, popped_item = q1.dequeue()  # highest priority first: "keep-2"
    assert popped_item == "keep-2"
    # Simulate a hard kill: close the underlying connection abruptly, no
    # explicit flush/checkpoint call beyond what LedgerBroker.append already
    # guarantees per-call.
    q1.close()

    q2 = PersistentQueue.open(db_path, "queue:durable")
    try:
        # Exactly the 2 remaining items, in original priority order,
        # reconstructed purely from replay of a freshly opened broker.
        assert q2.pending_count() == 2
        _, first = q2.dequeue()
        _, second = q2.dequeue()
        assert [first, second] == ["keep-1", "drop-me"]
        assert q2.dequeue() is None
        # The popped item from before the kill stays popped.
        with pytest.raises(DuplicateEventError):
            q2._broker.append(
                event_type="PersistentQueue.ItemDequeued.v1",
                producer=PRODUCER,
                principal_id=q2.queue_id,
                space_id="persistent-queue",
                correlation_id=f"queue|{q2.queue_id}|{popped_id}",
                idempotency_key=f"{popped_id}|dequeue",
                payload={"queue_id": q2.queue_id, "item_id": popped_id},
            )
    finally:
        q2.close()


def test_independent_queue_ids_do_not_leak_items(db_path):
    a = PersistentQueue.open(db_path, "queue:a")
    b = PersistentQueue.open(db_path, "queue:b")
    try:
        a.enqueue("a-item")
        b.enqueue("b-item")

        assert a.pending_count() == 1
        assert b.pending_count() == 1
        assert a.peek()[1] == "a-item"
        assert b.peek()[1] == "b-item"
    finally:
        a.close()
        b.close()
