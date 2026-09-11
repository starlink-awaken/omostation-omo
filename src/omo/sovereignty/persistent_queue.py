"""Persistent Queue — ledger-backed durable priority queue.

Spec: ``docs/superpowers/specs/2026-09-11-persistent-queue-design.md``
(BET-Y1Q4-T10-147).

Mirrors the architecture already established by
:class:`omo.sovereignty.roles.SovereigntyService`: every write goes through
:meth:`LedgerBroker.append` (no second write path); every read reconstructs
state by replaying :meth:`LedgerBroker.read`. No queue state is cached on
the instance beyond ``queue_id`` and the broker handle, so durability across
a process crash falls directly out of the ledger's own WAL guarantee — there
is nothing queue-specific to flush.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from omo.event_ledger.broker import DuplicateEventError, LedgerBroker

PRODUCER = "omo-persistent-queue"
SPACE_ID = "persistent-queue"

EVT_ENQUEUED = "PersistentQueue.ItemEnqueued.v1"
EVT_DEQUEUED = "PersistentQueue.ItemDequeued.v1"


class QueueError(RuntimeError):
    """Typed error for PersistentQueue; ``code`` is a stable reason key."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


class QueueReplayError(QueueError):
    """A ledger row could not be strictly decoded during replay."""


@dataclass(frozen=True)
class QueueItem:
    item_id: str
    item: Any
    priority: int


def _new_item_id() -> str:
    return f"qitem:{uuid4()}"


class PersistentQueue:
    """A durable, priority-ordered, FIFO-tiebreak queue for one ``queue_id``.

    Multiple ``queue_id``s may share the same ledger database file; replay
    is always scoped to this instance's ``queue_id``, so independent queues
    never leak items into each other.
    """

    def __init__(self, broker: LedgerBroker, queue_id: str) -> None:
        self._broker = broker
        self.queue_id = queue_id

    @classmethod
    def open(cls, db_path: str | Path, queue_id: str) -> PersistentQueue:
        return cls(LedgerBroker.connect(db_path), queue_id)

    def close(self) -> None:
        self._broker.close()

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def enqueue(self, item: Any, *, priority: int = 0) -> str:
        """Append one ``ItemEnqueued`` event; returns the new ``item_id``."""
        item_id = _new_item_id()
        payload = {
            "queue_id": self.queue_id,
            "item_id": item_id,
            "item": item,
            "priority": int(priority),
        }
        self._broker.append(
            event_type=EVT_ENQUEUED,
            producer=PRODUCER,
            principal_id=self.queue_id,
            space_id=SPACE_ID,
            correlation_id=f"queue|{self.queue_id}|{item_id}",
            idempotency_key=item_id,
            payload=payload,
        )
        return item_id

    def dequeue(self) -> tuple[str, Any] | None:
        """Pop the highest-priority pending item (FIFO within a priority).

        Returns ``(item_id, item)`` or ``None`` if the queue is empty.
        Appends exactly one ``ItemDequeued`` event on success.
        """
        pending = self._pending_items()
        if not pending:
            return None
        item = pending[0]
        try:
            self._broker.append(
                event_type=EVT_DEQUEUED,
                producer=PRODUCER,
                principal_id=self.queue_id,
                space_id=SPACE_ID,
                correlation_id=f"queue|{self.queue_id}|{item.item_id}",
                idempotency_key=f"{item.item_id}|dequeue",
                payload={"queue_id": self.queue_id, "item_id": item.item_id},
            )
        except DuplicateEventError as exc:
            raise QueueError("ITEM_ALREADY_DEQUEUED", item.item_id) from exc
        return item.item_id, item.item

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def peek(self) -> tuple[str, Any] | None:
        """Same ordering as :meth:`dequeue` but never mutates state."""
        pending = self._pending_items()
        if not pending:
            return None
        return pending[0].item_id, pending[0].item

    def pending_count(self) -> int:
        return len(self._pending_items())

    def _pending_items(self) -> list[QueueItem]:
        rows = self._broker.read(producer=PRODUCER)
        enqueued: dict[str, QueueItem] = {}
        order: list[str] = []
        dequeued: set[str] = set()
        for row in rows:
            payload = self._decode_row(row)
            if payload.get("queue_id") != self.queue_id:
                continue
            event_type = row.get("event_type")
            item_id = payload.get("item_id")
            if not isinstance(item_id, str) or not item_id:
                raise QueueReplayError(
                    "MALFORMED_ROW", f"seq={row.get('sequence')} missing item_id"
                )
            if event_type == EVT_ENQUEUED:
                priority = payload.get("priority")
                if not isinstance(priority, int):
                    raise QueueReplayError(
                        "MALFORMED_ROW",
                        f"seq={row.get('sequence')} priority must be an integer",
                    )
                enqueued[item_id] = QueueItem(
                    item_id=item_id, item=payload.get("item"), priority=priority
                )
                order.append(item_id)
            elif event_type == EVT_DEQUEUED:
                dequeued.add(item_id)
            else:
                raise QueueReplayError(
                    "MALFORMED_ROW",
                    f"seq={row.get('sequence')} unknown event_type {event_type!r}",
                )
        pending = [enqueued[iid] for iid in order if iid not in dequeued]
        # Stable sort: `order` already preserves enqueue order, so equal
        # priorities keep FIFO tiebreak.
        pending.sort(key=lambda qi: -qi.priority)
        return pending

    @staticmethod
    def _decode_row(row: dict[str, Any]) -> dict[str, Any]:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError, KeyError) as exc:
            raise QueueReplayError(
                "MALFORMED_ROW",
                f"seq={row.get('sequence')} payload is not valid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise QueueReplayError(
                "MALFORMED_ROW", f"seq={row.get('sequence')} payload must be a JSON object"
            )
        return payload
