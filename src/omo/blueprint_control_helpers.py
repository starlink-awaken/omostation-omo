"""Shared helpers for blueprint control: hashing, time, validation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any


class BlueprintControlError(ValueError):
    """A blueprint cannot advance through the supervised control contract."""


@dataclass(frozen=True)
class CompiledBlueprintPacket:
    packet: dict[str, Any]
    packet_hash: str


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _utc(value: str | None = None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _stamp(value: str | None = None) -> str:
    return _utc(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_receipt_digest(receipt: Mapping[str, Any]) -> str:
    projected = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    canonical = json.dumps(projected, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _is_sha256(value: Any, *, prefixed: bool) -> bool:
    text = str(value or "")
    if prefixed:
        if not text.startswith("sha256:"):
            return False
        text = text.removeprefix("sha256:")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _safe_relative_path(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    path = PurePosixPath(text)
    canonical = path.as_posix() + ("/" if text.endswith("/") else "")
    if (
        not text
        or text in {".", "./"}
        or text.startswith("/")
        or "\\" in text
        or path.is_absolute()
        or ".." in path.parts
        or canonical != text
    ):
        raise BlueprintControlError(f"unsafe {field_name}: {text}")
    return text


def _required_string_list(container: Mapping[str, Any], field_name: str) -> list[str]:
    value = container.get(field_name)
    if not isinstance(value, list) or not value or any(not isinstance(item, str) or not item.strip() for item in value):
        raise BlueprintControlError(f"{field_name} must be a non-empty string list")
    return [item.strip() for item in value]


__all__ = [
    "BlueprintControlError",
    "CompiledBlueprintPacket",
    "_sha256",
    "_utc",
    "_stamp",
    "_canonical_receipt_digest",
    "_is_sha256",
    "_safe_relative_path",
    "_required_string_list",
]
