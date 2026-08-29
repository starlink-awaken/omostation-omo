"""Canonical, lease-based publisher for the Event Ledger outbox."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from .broker import (
    OUTBOX_FAILED,
    OUTBOX_PENDING,
    OUTBOX_SENT,
    OUTBOX_UNCERTAIN,
    LedgerBroker,
)

PublishFn = Callable[[str, dict[str, Any], str], str]
_BACKOFF_SECONDS = (5, 30, 120, 600)
_LEASE_SECONDS = 30


@dataclass(frozen=True)
class PublishResult:
    event_id: str
    destination: str
    state: str
    attempts: int
    receipt_id: str | None
    next_attempt_at: str
    error_class: str | None


class PublishUncertainError(RuntimeError):
    """The transport outcome is unknown; the row must not become sent."""


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _backoff_time(now: str, attempts: int) -> str:
    index = min(max(attempts - 1, 0), len(_BACKOFF_SECONDS) - 1)
    return _format_time(_parse_time(now) + timedelta(seconds=_BACKOFF_SECONDS[index]))


def _failure_receipt(event_id: str, destination: str, attempts: int) -> str:
    value = f"{event_id}\0{destination}\0{attempts}".encode()
    return "failure:" + hashlib.sha256(value).hexdigest()


def _result(row: dict[str, Any]) -> PublishResult:
    return PublishResult(
        event_id=str(row["event_id"]),
        destination=str(row["destination"]),
        state=str(row["state"]),
        attempts=int(row["attempts"]),
        receipt_id=row.get("receipt_id"),
        next_attempt_at=str(row["next_attempt_at"]),
        error_class=row.get("error_class"),
    )


def _sent_replays(broker: LedgerBroker, destination: str, limit: int) -> list[PublishResult]:
    rows = [
        row
        for row in broker.outbox_entries()
        if row.get("destination") == destination and row.get("state") == OUTBOX_SENT
    ]
    return [_result(row) for row in rows[:limit]]


def publish_due(
    broker: LedgerBroker,
    destination: str,
    publish: PublishFn,
    *,
    worker_id: str,
    now: str,
    limit: int = 100,
) -> list[PublishResult]:
    """Lease and publish due rows, preserving uncertain outcomes conservatively."""
    lease_expires_at = _format_time(_parse_time(now) + timedelta(seconds=_LEASE_SECONDS))
    claimed = broker.outbox_claim_due(
        destination,
        worker_id=worker_id,
        now=now,
        lease_expires_at=lease_expires_at,
        limit=limit,
    )
    if not claimed:
        return _sent_replays(broker, destination, limit)

    results: list[PublishResult] = []
    for row in claimed:
        event_id = str(row["event_id"])
        attempts = int(row["attempts"]) + 1
        try:
            payload = broker.outbox_event_payload(event_id)
            receipt_id = publish(event_id, payload, destination)
            if not isinstance(receipt_id, str) or not receipt_id.strip():
                raise ValueError("publisher returned an empty receipt")
            finalized = broker.outbox_finalize(
                event_id,
                destination,
                worker_id=worker_id,
                state=OUTBOX_SENT,
                attempts=attempts,
                next_attempt_at=now,
                receipt_id=receipt_id,
            )
        except (TimeoutError, ConnectionError, ConnectionResetError, PublishUncertainError) as exc:
            finalized = broker.outbox_finalize(
                event_id,
                destination,
                worker_id=worker_id,
                state=OUTBOX_UNCERTAIN,
                attempts=attempts,
                next_attempt_at=_backoff_time(now, attempts),
                error_class=type(exc).__name__,
            )
        except Exception as exc:
            deterministic_state = OUTBOX_FAILED if attempts >= len(_BACKOFF_SECONDS) + 1 else OUTBOX_PENDING
            finalized = broker.outbox_finalize(
                event_id,
                destination,
                worker_id=worker_id,
                state=deterministic_state,
                attempts=attempts,
                next_attempt_at=now if deterministic_state == OUTBOX_FAILED else _backoff_time(now, attempts),
                receipt_id=_failure_receipt(event_id, destination, attempts)
                if deterministic_state == OUTBOX_FAILED
                else None,
                error_class=type(exc).__name__,
            )
        results.append(_result(finalized))
    return results


__all__ = ["PublishFn", "PublishResult", "PublishUncertainError", "publish_due"]
