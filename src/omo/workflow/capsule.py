"""workflow/capsule.py — WorkPacket 胶囊 envelope 与 receipt 绑定 (BET-Y1Q4-T10-165 件二).

为 Agent Cell 跨 Cell 交接补齐可审计的胶囊层: 把 Workspace 编译的
WorkPacket 身份 (bet_id + work_packet_hash, 由 ``bin/plan/bet-ledger.py``
编译, 本模块只引用不重复编译) 与执行 Cell 产出的证据 receipt
(receipt_digest) 绑定为不可变 envelope, 供件三 Handoff 入 Mesh 消费。

设计决策:
- 不重复 WorkPacket 编译逻辑: 只校验 ``work_packet_hash`` 的
  ``sha256:<64 hex>`` 形状, 真值校验仍由 workspace 契约
  (``validate_work_packet_run``) 负责。
- 无 receipt 不交接 (fail closed): ``receipt_digest`` 必填且形状合法。
- 复用 sovereignty ``role:`` ID 前缀约束生产/消费方, 不重复身份模型;
  准入态核验 (admitted 才可交接) 留给件四 Role 级 verifier。
- 纯确定性逻辑, 零模型调用; 落盘路径由调用方传入, 本模块无默认写副作用。
- Capsule 不可变 + append-only 存储: 交接事件只增不改, 篡改行在加载时
  整体拒绝 (fail closed)。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

__all__ = (
    "CapsuleRecord",
    "CapsuleStore",
    "CapsuleError",
    "capsule_digest",
    "seal_capsule",
    "verify_capsule",
)

_CAPSULE_ID_RE = re.compile(r"^capsule:[A-Za-z0-9._-]{1,120}$")
_ROLE_ID_RE = re.compile(r"^role:[A-Za-z0-9._-]{1,120}$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

_SCHEMA = "omo-capsule/v1"

# to_dict 严格字段集: 未知字段拒绝 (防跨版本静默误读)
_FIELDS = frozenset(
    {
        "schema",
        "capsule_id",
        "bet_id",
        "work_packet_hash",
        "receipt_digest",
        "producer_role",
        "consumer_role",
        "payload_digest",
        "created_at",
        "digest",
    }
)


class CapsuleError(Exception):
    """胶囊失败 (附机器可读 code)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def capsule_digest(record: dict[str, Any]) -> str:
    """规范 JSON → sha256 digest (防篡改)."""
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CapsuleRecord:
    """一条密封胶囊 (不可变快照)."""

    capsule_id: str
    bet_id: str
    work_packet_hash: str
    receipt_digest: str
    producer_role: str
    consumer_role: str
    payload_digest: str
    created_at: str = ""
    digest: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": _SCHEMA,
            "capsule_id": self.capsule_id,
            "bet_id": self.bet_id,
            "work_packet_hash": self.work_packet_hash,
            "receipt_digest": self.receipt_digest,
            "producer_role": self.producer_role,
            "consumer_role": self.consumer_role,
            "payload_digest": self.payload_digest,
            "created_at": self.created_at,
            "digest": self.digest,
        }


def _now() -> str:
    return datetime.now(UTC).isoformat()


def seal_capsule(
    *,
    capsule_id: str,
    bet_id: str,
    work_packet_hash: str,
    receipt_digest: str,
    producer_role: str,
    consumer_role: str,
    payload_digest: str,
) -> CapsuleRecord:
    """校验并密封一条胶囊; 任一绑定缺失/形状非法即抛 CapsuleError."""
    if not _CAPSULE_ID_RE.match(capsule_id):
        raise CapsuleError(
            "invalid-capsule-id",
            f"capsule_id 必须匹配 {_CAPSULE_ID_RE.pattern}: {capsule_id!r}",
        )
    if not bet_id or not bet_id.strip():
        raise CapsuleError("empty-bet-id", "bet_id 不得为空")
    if not _DIGEST_RE.match(work_packet_hash):
        raise CapsuleError(
            "invalid-packet-hash",
            f"work_packet_hash 必须为 sha256:<64 hex>: {work_packet_hash!r}",
        )
    if not receipt_digest or not _DIGEST_RE.match(receipt_digest):
        raise CapsuleError(
            "missing-receipt",
            "无 receipt 不交接: receipt_digest 必须为 sha256:<64 hex>",
        )
    for label, role_id in (("producer_role", producer_role), ("consumer_role", consumer_role)):
        if not _ROLE_ID_RE.match(role_id):
            raise CapsuleError(
                "invalid-role-id",
                f"{label} 必须匹配 {_ROLE_ID_RE.pattern}: {role_id!r}",
            )
    if not _DIGEST_RE.match(payload_digest):
        raise CapsuleError(
            "invalid-payload-digest",
            f"payload_digest 必须为 sha256:<64 hex>: {payload_digest!r}",
        )
    created_at = _now()
    body = {
        "schema": _SCHEMA,
        "capsule_id": capsule_id,
        "bet_id": bet_id,
        "work_packet_hash": work_packet_hash,
        "receipt_digest": receipt_digest,
        "producer_role": producer_role,
        "consumer_role": consumer_role,
        "payload_digest": payload_digest,
        "created_at": created_at,
    }
    return CapsuleRecord(
        capsule_id=capsule_id,
        bet_id=bet_id,
        work_packet_hash=work_packet_hash,
        receipt_digest=receipt_digest,
        producer_role=producer_role,
        consumer_role=consumer_role,
        payload_digest=payload_digest,
        created_at=created_at,
        digest=capsule_digest(body),
    )


def verify_capsule(data: dict[str, Any]) -> CapsuleRecord:
    """重算 digest 并返回记录; 篡改/未知字段/形状非法即抛 CapsuleError."""
    if not isinstance(data, dict):
        raise CapsuleError("invalid-capsule", "capsule 必须为 mapping")
    unknown = set(data) - _FIELDS
    if unknown:
        raise CapsuleError(
            "unknown-field",
            f"未知字段拒绝 (防跨版本静默误读): {sorted(unknown)}",
        )
    missing = _FIELDS - set(data)
    if missing:
        raise CapsuleError("incomplete-capsule", f"缺字段: {sorted(missing)}")
    if data.get("schema") != _SCHEMA:
        raise CapsuleError(
            "schema-mismatch",
            f"schema 必须为 {_SCHEMA}: {data.get('schema')!r}",
        )
    body = {k: data[k] for k in _FIELDS if k != "digest"}
    if capsule_digest(body) != data.get("digest"):
        raise CapsuleError(
            "digest-mismatch",
            f"{data.get('capsule_id')!r} digest 不匹配 (疑似篡改)",
        )
    return CapsuleRecord(
        capsule_id=data["capsule_id"],
        bet_id=data["bet_id"],
        work_packet_hash=data["work_packet_hash"],
        receipt_digest=data["receipt_digest"],
        producer_role=data["producer_role"],
        consumer_role=data["consumer_role"],
        payload_digest=data["payload_digest"],
        created_at=data["created_at"],
        digest=data["digest"],
    )


class CapsuleStore:
    """胶囊 append-only 存储 (内存 + JSONL 落盘, 线程不安全, 单写者)."""

    def __init__(self, store: Path | None = None) -> None:
        self._store = store
        self._records: dict[str, CapsuleRecord] = {}
        if store is not None and store.is_file():
            self._load()

    # -- 查询 ----------------------------------------------------------

    def get(self, capsule_id: str) -> CapsuleRecord | None:
        return self._records.get(capsule_id)

    def list(self, *, bet_id: str | None = None) -> list[CapsuleRecord]:
        records = list(self._records.values())
        if bet_id is not None:
            records = [r for r in records if r.bet_id == bet_id]
        return sorted(records, key=lambda r: r.capsule_id)

    # -- 变更 (只增不改) ------------------------------------------------

    def append(self, record: CapsuleRecord) -> CapsuleRecord:
        """已密封记录入库; 先验 digest, 重复 capsule_id 拒绝."""
        verified = verify_capsule(record.to_dict())
        if verified.capsule_id in self._records:
            raise CapsuleError("duplicate-capsule", f"{verified.capsule_id!r} 已存在 (只增不改)")
        self._records[verified.capsule_id] = verified
        self._persist()
        return verified

    # -- 持久化 --------------------------------------------------------

    def _persist(self) -> None:
        if self._store is None:
            return
        self._store.parent.mkdir(parents=True, exist_ok=True)
        with self._store.open("w", encoding="utf-8") as fh:
            for record in self.list():
                fh.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")

    def _load(self) -> None:
        assert self._store is not None
        with self._store.open(encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = verify_capsule(json.loads(line))
                except CapsuleError as exc:
                    raise CapsuleError(
                        exc.code,
                        f"{self._store}:{lineno}: {exc}",
                    ) from exc
                self._records[record.capsule_id] = record
