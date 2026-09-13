"""workflow/role_registry.py — 持久 Role 注册表与准入状态机 (BET-Y1Q4-T10-165 件一).

为 Agent Cell 动态编排补齐持久身份的准入层: 在 ``omo.sovereignty`` 身份
模型 (``role:`` 前缀、assign/revoke) 之上, 增加面向调度的准入状态机
(pending → admitted ⇄ suspended, → revoked 终态) 与 capabilities 绑定。

设计决策:
- 复用 sovereignty ``role:`` ID 前缀, 不重复身份模型, 只加准入状态机层。
- 纯确定性逻辑, 零模型调用; 落盘路径由调用方传入, 本模块无默认写副作用。
- version 单调递增, stale-version 写入拒绝 (并发写冲突止血)。
- 未过门零写入/零自治/零扩并发 (T10-165 铁律): admit 要求 capabilities
  非空 (Role 级 verifier 接口雏形, 件四扩展)。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

__all__ = (
    "AdmissionState",
    "RoleRecord",
    "RoleRegistry",
    "RoleRegistryError",
    "admission_digest",
)

AdmissionState = Literal["pending", "admitted", "suspended", "revoked"]

_ROLE_ID_RE = re.compile(r"^role:[A-Za-z0-9._-]{1,120}$")

# 合法跃迁表 (终态 revoked 无出边)
_ALLOWED: dict[str, frozenset[str]] = {
    "pending": frozenset({"admitted", "revoked"}),
    "admitted": frozenset({"suspended", "revoked"}),
    "suspended": frozenset({"admitted", "revoked"}),
    "revoked": frozenset(),
}

_SCHEMA = "omo-role-registry/v1"


class RoleRegistryError(Exception):
    """注册表失败 (附机器可读 code)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def admission_digest(record: dict[str, Any]) -> str:
    """规范 JSON → sha256 digest (防篡改)."""
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RoleRecord:
    """一条持久 Role 注册 (不可变快照)."""

    role_id: str
    capabilities: frozenset[str] = field(default_factory=frozenset)
    admission_state: AdmissionState = "pending"
    version: int = 1
    updated_at: str = ""
    digest: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": _SCHEMA,
            "role_id": self.role_id,
            "capabilities": sorted(self.capabilities),
            "admission_state": self.admission_state,
            "version": self.version,
            "updated_at": self.updated_at,
            "digest": self.digest,
        }


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _seal(
    role_id: str, capabilities: frozenset[str], state: AdmissionState, version: int, updated_at: str
) -> RoleRecord:
    body = {
        "schema": _SCHEMA,
        "role_id": role_id,
        "capabilities": sorted(capabilities),
        "admission_state": state,
        "version": version,
        "updated_at": updated_at,
    }
    return RoleRecord(
        role_id=role_id,
        capabilities=capabilities,
        admission_state=state,
        version=version,
        updated_at=updated_at,
        digest=admission_digest(body),
    )


class RoleRegistry:
    """持久 Role 注册表 (内存 + JSONL 落盘, 线程不安全, 单写者)."""

    def __init__(self, store: Path | None = None) -> None:
        self._store = store
        self._records: dict[str, RoleRecord] = {}
        if store is not None and store.is_file():
            self._load()

    # -- 查询 ----------------------------------------------------------

    def get(self, role_id: str) -> RoleRecord | None:
        return self._records.get(role_id)

    def list(self, *, state: AdmissionState | None = None) -> list[RoleRecord]:
        records = list(self._records.values())
        if state is not None:
            records = [r for r in records if r.admission_state == state]
        return sorted(records, key=lambda r: r.role_id)

    def can_admit(self, role_id: str, capability: str) -> bool:
        """Role 级 verifier 雏形: 已准入 + 具备该 capability."""
        record = self._records.get(role_id)
        return record is not None and record.admission_state == "admitted" and capability in record.capabilities

    # -- 变更 ----------------------------------------------------------

    def register(self, role_id: str, capabilities: set[str] | frozenset[str] | list[str]) -> RoleRecord:
        if not _ROLE_ID_RE.match(role_id):
            raise RoleRegistryError(
                "invalid-role-id",
                f"role_id 必须匹配 {_ROLE_ID_RE.pattern}: {role_id!r}",
            )
        caps = frozenset(capabilities)
        if not caps:
            raise RoleRegistryError("empty-capabilities", f"{role_id!r} capabilities 不得为空")
        if role_id in self._records:
            raise RoleRegistryError("duplicate-role", f"{role_id!r} 已注册")
        record = _seal(role_id, caps, "pending", 1, _now())
        self._records[role_id] = record
        self._persist()
        return record

    def transition(self, role_id: str, to_state: AdmissionState, *, expected_version: int) -> RoleRecord:
        current = self._records.get(role_id)
        if current is None:
            raise RoleRegistryError("unknown-role", f"{role_id!r} 未注册")
        if expected_version != current.version:
            raise RoleRegistryError(
                "stale-version",
                f"{role_id!r} 期望 version={expected_version}, 实际 version={current.version}",
            )
        if to_state not in _ALLOWED[current.admission_state]:
            raise RoleRegistryError(
                "illegal-transition",
                f"{role_id!r} 不允许 {current.admission_state} → {to_state}",
            )
        record = _seal(role_id, current.capabilities, to_state, current.version + 1, _now())
        self._records[role_id] = record
        self._persist()
        return record

    def admit(self, role_id: str, *, expected_version: int) -> RoleRecord:
        return self.transition(role_id, "admitted", expected_version=expected_version)

    def suspend(self, role_id: str, *, expected_version: int) -> RoleRecord:
        return self.transition(role_id, "suspended", expected_version=expected_version)

    def revoke(self, role_id: str, *, expected_version: int) -> RoleRecord:
        return self.transition(role_id, "revoked", expected_version=expected_version)

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
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                record = RoleRecord(
                    role_id=data["role_id"],
                    capabilities=frozenset(data.get("capabilities", [])),
                    admission_state=data.get("admission_state", "pending"),
                    version=int(data.get("version", 1)),
                    updated_at=data.get("updated_at", ""),
                    digest=data.get("digest", ""),
                )
                self._records[record.role_id] = record
