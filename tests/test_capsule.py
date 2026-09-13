"""test_capsule.py — T10-165 件二 Capsule 胶囊单元测试.

覆盖: 密封成功、坏 capsule 前缀/空 bet_id/坏 packet hash 拒绝、无 receipt
不交接、生产/消费 role: 前缀约束、digest 稳定性、篡改拒绝、未知字段拒绝、
append-only 存储 (重复拒绝/JSONL round-trip/篡改行加载拒绝)。
"""

from __future__ import annotations

import hashlib
import json

import pytest

from omo.workflow.capsule import (
    CapsuleError,
    CapsuleRecord,
    CapsuleStore,
    capsule_digest,
    seal_capsule,
    verify_capsule,
)

_PACKET = "sha256:" + "a" * 64
_RECEIPT = "sha256:" + "b" * 64
_PAYLOAD = "sha256:" + "c" * 64


def _seal(**over: object) -> CapsuleRecord:
    kw: dict[str, object] = {
        "capsule_id": "capsule:handoff-001",
        "bet_id": "BET-Y1Q4-T10-165",
        "work_packet_hash": _PACKET,
        "receipt_digest": _RECEIPT,
        "producer_role": "role:cell-alpha",
        "consumer_role": "role:cell-beta",
        "payload_digest": _PAYLOAD,
    }
    kw.update(over)
    return seal_capsule(**kw)  # type: ignore[arg-type]


def test_seal_ok() -> None:
    r = _seal()
    assert isinstance(r, CapsuleRecord)
    assert r.digest.startswith("sha256:")
    assert verify_capsule(r.to_dict()) == r


def test_seal_bad_capsule_id_rejected() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(capsule_id="handoff-001")
    assert exc.value.code == "invalid-capsule-id"


def test_seal_empty_bet_id_rejected() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(bet_id="  ")
    assert exc.value.code == "empty-bet-id"


def test_seal_bad_packet_hash_rejected() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(work_packet_hash="abc123")
    assert exc.value.code == "invalid-packet-hash"


def test_seal_missing_receipt_rejected() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(receipt_digest="")
    assert exc.value.code == "missing-receipt"


def test_seal_bad_receipt_shape_rejected() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(receipt_digest="sha256:zzz")
    assert exc.value.code == "missing-receipt"


def test_seal_role_prefix_enforced() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(producer_role="cell-alpha")
    assert exc.value.code == "invalid-role-id"
    with pytest.raises(CapsuleError) as exc:
        _seal(consumer_role="cell-beta")
    assert exc.value.code == "invalid-role-id"


def test_seal_bad_payload_digest_rejected() -> None:
    with pytest.raises(CapsuleError) as exc:
        _seal(payload_digest="not-a-digest")
    assert exc.value.code == "invalid-payload-digest"


def test_digest_stable() -> None:
    body = {"schema": "omo-capsule/v1", "capsule_id": "capsule:x"}
    assert capsule_digest(body) == capsule_digest(dict(body))


def test_verify_tamper_rejected() -> None:
    data = _seal().to_dict()
    data["receipt_digest"] = "sha256:" + "d" * 64
    with pytest.raises(CapsuleError) as exc:
        verify_capsule(data)
    assert exc.value.code == "digest-mismatch"


def test_verify_unknown_field_rejected() -> None:
    data = _seal().to_dict()
    data["mesh_trace"] = "trace-1"
    # 去掉 digest 影响前先补齐: 未知字段应在 digest 校验前拒绝
    with pytest.raises(CapsuleError) as exc:
        verify_capsule(data)
    assert exc.value.code == "unknown-field"


def test_store_append_and_list() -> None:
    store = CapsuleStore()
    store.append(_seal())
    store.append(_seal(capsule_id="capsule:handoff-002", bet_id="BET-Y1Q4-T10-164"))
    assert [r.capsule_id for r in store.list()] == ["capsule:handoff-001", "capsule:handoff-002"]
    assert [r.capsule_id for r in store.list(bet_id="BET-Y1Q4-T10-164")] == ["capsule:handoff-002"]
    assert store.get("capsule:nope") is None


def test_store_duplicate_rejected() -> None:
    store = CapsuleStore()
    store.append(_seal())
    with pytest.raises(CapsuleError) as exc:
        store.append(_seal())
    assert exc.value.code == "duplicate-capsule"


def test_store_jsonl_roundtrip(tmp_path) -> None:
    path = tmp_path / "capsules.jsonl"
    store = CapsuleStore(path)
    sealed = store.append(_seal())
    reloaded = CapsuleStore(path)
    got = reloaded.get("capsule:handoff-001")
    assert got == sealed
    assert got is not None and got.receipt_digest == _RECEIPT


def test_store_tampered_line_rejected(tmp_path) -> None:
    path = tmp_path / "capsules.jsonl"
    CapsuleStore(path).append(_seal())
    raw = json.loads(path.read_text(encoding="utf-8").strip())
    raw["producer_role"] = "role:cell-evil"
    path.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")
    with pytest.raises(CapsuleError) as exc:
        CapsuleStore(path)
    assert exc.value.code == "digest-mismatch"


def test_capsule_binds_real_packet_hash_shape() -> None:
    # 与 workspace 契约同形: sha256 hex (bin/plan/bet-ledger.py SHA256_REF_RE)
    blob = hashlib.sha256(b"packet").hexdigest()
    r = _seal(work_packet_hash=f"sha256:{blob}")
    assert r.work_packet_hash == f"sha256:{blob}"
