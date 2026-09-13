import json
import sqlite3
from pathlib import Path

from omo.resident.task_queue import TaskQueue, TaskStatus

_WORK_PACKET = "sha256:" + "a" * 64
_CLAIM_RECEIPT = "sha256:" + "b" * 64


def test_claim_bound_submit_persists_binding(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "claims.sqlite3")
    result = queue.submit_claim_bound(
        "bos://resident/sediment/trigger",
        {"path": "/tmp/a.md"},
        work_packet_digest=_WORK_PACKET,
        claim_receipt_digest=_CLAIM_RECEIPT,
        labels=["urgent"],
    )

    assert result.ok
    task = queue.get(result.task_id)
    assert task is not None
    assert task.work_packet_digest == _WORK_PACKET
    assert task.claim_receipt_digest == _CLAIM_RECEIPT
    assert task.labels == ["urgent", "claim-bound"]


def test_invalid_claim_binding_is_rejected_before_insert(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "invalid.sqlite3")
    bad_packet = queue.submit_claim_bound(
        "bos://resident/a", {}, work_packet_digest="not-a-digest", claim_receipt_digest=_CLAIM_RECEIPT
    )
    bad_receipt = queue.submit_claim_bound(
        "bos://resident/a", {}, work_packet_digest=_WORK_PACKET, claim_receipt_digest="sha256:bad"
    )

    assert not bad_packet.ok and bad_packet.reason == "invalid work_packet_digest"
    assert not bad_receipt.ok and bad_receipt.reason == "invalid claim_receipt_digest"
    assert queue.stats()[TaskStatus.QUEUED.value] == 0


def test_claim_columns_migrate_existing_database(tmp_path: Path) -> None:
    db = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(db) as connection:
        connection.execute(
            """
            CREATE TABLE resident_tasks (
                id TEXT PRIMARY KEY,
                uri TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                result TEXT,
                error_message TEXT NOT NULL DEFAULT '',
                priority INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                expires_at REAL NOT NULL DEFAULT 0,
                labels TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

    queue = TaskQueue(db)
    result = queue.submit_claim_bound(
        "bos://resident/a", {}, work_packet_digest=_WORK_PACKET, claim_receipt_digest=_CLAIM_RECEIPT
    )
    assert result.ok
    assert queue.get(result.task_id).claim_receipt_digest == _CLAIM_RECEIPT


def test_cli_accepts_claim_binding(tmp_path: Path, capsys) -> None:
    from omo.resident import task_queue as tq

    db = str(tmp_path / "cli.sqlite3")
    argv = [
        "submit",
        "--uri",
        "bos://resident/a",
        "--json",
        "{}",
        "--db",
        db,
        "--work-packet-digest",
        _WORK_PACKET,
        "--claim-receipt-digest",
        _CLAIM_RECEIPT,
    ]
    assert tq.main(argv) == 0
    task_id = json.loads(capsys.readouterr().out)["task_id"]
    task = TaskQueue(db).get(task_id)
    assert task.work_packet_digest == _WORK_PACKET
    assert task.claim_receipt_digest == _CLAIM_RECEIPT
