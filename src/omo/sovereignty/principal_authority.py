"""Principal authority binding — spec BET-Y1Q3-T4-04 (Product P0 WP4).

OMO 是唯一 principal authority verifier (spec §2 权威边界)。
持久层只保存 authority reference / credential digest / membership version /
有效期 — 不保存 credential secret (spec §2)。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

PRINCIPAL_AUTHORITY_SCHEMA = "principal-authority-receipt/v1"


class PrincipalAuthorityError(Exception):
    """Authority 验证失败的统一错误 (拒绝矩阵的 OMO 侧出口)。"""


@dataclass(frozen=True)
class PrincipalAuthorityReceipt:
    """一次权威验证的回执 — digest 不含 secret (spec §2)。"""

    principal_id: str
    authority_ref: str
    credential_digest: str
    membership_version: int
    verified_at: str
    expires_at: str

    def receipt_digest(self) -> str:
        """Canonical receipt digest — 全链 (Cockpit/OMO/Agora) 重放锚。"""
        # 重放锚只覆盖不变字段 (verified_at 每次验证变化, 不进 digest)
        payload = json.dumps(
            {
                "principal_id": self.principal_id,
                "authority_ref": self.authority_ref,
                "credential_digest": str(self.credential_digest),
                "membership_version": self.membership_version,
                "expires_at": self.expires_at,
            },
            sort_keys=True,
        )
        return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


class PrincipalAuthority(Protocol):
    """窄 authority adapter 合同 (spec §3)。"""

    def verify(
        self,
        principal_id: str,
        credential_ref: str,
        *,
        now: str,
    ) -> PrincipalAuthorityReceipt: ...


class LocalPrincipalAuthority:
    """本地权威实现 — 权威源为 root 下 local-principal-credentials.json。

    文件格式 (不含 secret):
    {
      "authorities": {
        "<authority_ref>": {"type": "local-file", "path": "..."},
      },
      "principals": {
        "principal:xiamingxing": {
          "authority_ref": "local:default",
          "credential_digest": "sha256:<digest-of-credential-material>",
          "membership_version": 1,
          "expires_at": "2027-12-31T00:00:00Z"
        }
      }
    }
    """

    def __init__(self, credentials_path: Path) -> None:
        self._path = credentials_path

    def verify(
        self,
        principal_id: str,
        credential_ref: str,
        *,
        now: str,
    ) -> PrincipalAuthorityReceipt:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise PrincipalAuthorityError("authority_source_unavailable") from exc
        principals = data.get("principals") if isinstance(data, dict) else None
        if not isinstance(principals, dict):
            raise PrincipalAuthorityError("authority_source_invalid")
        record = principals.get(principal_id)
        if not isinstance(record, dict):
            raise PrincipalAuthorityError("principal_unknown")
        if str(record.get("authority_ref")) != credential_ref:
            raise PrincipalAuthorityError("authority_mismatch")
        digest = record.get("credential_digest")
        if not isinstance(digest, str) or not digest.startswith("sha256:"):
            raise PrincipalAuthorityError("credential_digest_invalid")
        version = record.get("membership_version")
        if not isinstance(version, int) or version < 1:
            raise PrincipalAuthorityError("membership_version_invalid")
        expires = str(record.get("expires_at", ""))
        if expires and str(now) >= expires:
            raise PrincipalAuthorityError("membership_expired")
        verified_at = str(now)
        return PrincipalAuthorityReceipt(
            principal_id=principal_id,
            authority_ref=credential_ref,
            credential_digest=digest,
            membership_version=version,
            verified_at=verified_at,
            expires_at=expires,
        )
