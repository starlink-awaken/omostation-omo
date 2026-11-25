from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

from .omo_debt_registry import DebtItem
from .omo_io import write_text_atomic

_SEED_ITEMS_HEADER_RE = re.compile(r"^seed_items:\s*(\[\s*\])?\s*(?:#.*)?$")
_TOP_LEVEL_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*:\s*(?:#.*)?$")


def append_history(payload: dict, action: str, note: str, actor: str = "", timestamp: str = "") -> None:
    entry: dict[str, str] = {
        "at": timestamp,
        "action": action,
        "note": note,
    }
    if actor:
        entry["actor"] = actor
    payload.setdefault("history", []).append(entry)


def register_item(args: argparse.Namespace, timestamp: str) -> dict:
    default_last = timestamp[:10] if timestamp else None
    default_next: str | None = None
    if timestamp:
        try:
            default_next = (datetime.fromisoformat(timestamp) + timedelta(days=14)).date().isoformat()
        except ValueError:
            default_next = None
    last_reviewed_at = getattr(args, "last_reviewed_at", None) or default_last
    next_review_at = getattr(args, "next_review_at", None) or default_next
    payload = {
        "id": args.id,
        "title": args.title,
        "dimension": args.dimension,
        "subdimension": args.subdimension,
        "domain": "workspace",
        "scope": "governance_kernel",
        "severity": args.severity,
        "weight": 0.05,
        "entropy_class": "classification",
        "lifecycle_state": "identified",
        "owner": args.owner,
        "affected_roots": [".omo"],
        "evidence_refs": [],
        "mitigation_refs": [],
        "opened_at": timestamp,
        "last_reviewed_at": last_reviewed_at,
        "next_review_at": next_review_at,
        "gate_level": "none",
        "history": [],
        "x1_policy_ref": getattr(args, "x1_policy_ref", ""),
        "x2_freshness": getattr(args, "x2_freshness", ""),
        "x3_tier": getattr(args, "x3_tier", ""),
    }
    actor = getattr(args, "actor", "")
    append_history(
        payload,
        "register",
        f"Registered debt item {args.id}.",
        actor=actor,
        timestamp=timestamp,
    )
    return payload


def _merge_ref_list(existing: Sequence[object], additions: Sequence[str], removals: Sequence[str]) -> list[str]:
    remove_set = {item.strip() for item in removals if item and item.strip()}
    merged: list[str] = []
    for item in existing:
        value = str(item).strip()
        if value and value not in remove_set and value not in merged:
            merged.append(value)
    for item in additions:
        value = item.strip()
        if value and value not in remove_set and value not in merged:
            merged.append(value)
    return merged


def update_item_refs(
    payload: dict,
    *,
    add_evidence: Sequence[str] = (),
    remove_evidence: Sequence[str] = (),
    add_mitigation: Sequence[str] = (),
    remove_mitigation: Sequence[str] = (),
) -> dict[str, list[str]]:
    """Update evidence_refs and mitigation_refs with order-preserving deduplication."""
    payload["evidence_refs"] = _merge_ref_list(
        payload.get("evidence_refs") or [],
        add_evidence,
        remove_evidence,
    )
    payload["mitigation_refs"] = _merge_ref_list(
        payload.get("mitigation_refs") or [],
        add_mitigation,
        remove_mitigation,
    )
    return {
        "evidence_refs": list(payload["evidence_refs"]),
        "mitigation_refs": list(payload["mitigation_refs"]),
    }


def _append_seed_item_preserving_comments(text: str, item_ref: str) -> str | None:
    lines = text.splitlines(keepends=True)
    header_idx: int | None = None
    inline_empty = False
    for idx, line in enumerate(lines):
        match = _SEED_ITEMS_HEADER_RE.match(line.rstrip("\r\n"))
        if match:
            header_idx = idx
            inline_empty = match.group(1) is not None
            break
    if header_idx is None:
        return None

    newline = "\r\n" if lines[header_idx].endswith("\r\n") else "\n"
    new_entry = f"- {item_ref}{newline}"

    if inline_empty:
        comment_idx = lines[header_idx].find("#")
        suffix = f"  {lines[header_idx][comment_idx:].rstrip()}" if comment_idx != -1 else ""
        lines[header_idx] = f"seed_items:{suffix}{newline}"
        lines.insert(header_idx + 1, new_entry)
        return "".join(lines)

    insert_after = header_idx
    for idx in range(header_idx + 1, len(lines)):
        raw = lines[idx].rstrip("\r\n")
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- "):
            insert_after = idx
            continue
        if raw.startswith((" ", "\t")):
            insert_after = idx
            continue
        if _TOP_LEVEL_KEY_RE.match(raw):
            break
        break

    if insert_after == len(lines) - 1 and not lines[insert_after].endswith(("\n", "\r")):
        lines[insert_after] = lines[insert_after] + newline
    lines.insert(insert_after + 1, new_entry)
    return "".join(lines)


def append_registry_ref(omo_dir: Path, item_ref: str, _load_yaml, _write_yaml) -> Path:
    registry_path = omo_dir / "_truth" / "registry" / "debt.yaml"
    registry = _load_yaml(registry_path)
    refs = list(registry.get("seed_items", []))
    if item_ref in refs:
        return registry_path
    if registry_path.exists():
        preserved = _append_seed_item_preserving_comments(
            registry_path.read_text(encoding="utf-8"),
            item_ref,
        )
        if preserved is not None:
            write_text_atomic(registry_path, preserved)
            return registry_path
    refs.append(item_ref)
    registry["seed_items"] = refs
    _write_yaml(registry_path, registry)
    return registry_path


def schedule_item(
    omo_dir: Path,
    item_id: str,
    next_review_at: str,
    _load_yaml,
    _write_yaml,
    timestamp: str,
) -> None:
    item_path = omo_dir / "debt" / "items" / f"{item_id}.yaml"
    if not item_path.exists():
        raise FileNotFoundError(f"unknown debt_id: {item_id}")
    payload = _load_yaml(item_path)
    payload["lifecycle_state"] = "scheduled"
    payload["next_review_at"] = next_review_at
    append_history(
        payload,
        "schedule",
        f"Next review set to {next_review_at}.",
        timestamp=timestamp,
    )
    _write_yaml(item_path, payload)


def update_item(omo_dir: Path, item_id: str, _load_yaml) -> tuple[Path, dict]:
    """Locate the canonical debt item YAML and return (path, payload).

    Search order (mirrors omo_debt_cli._debt_item_path):
      1. omo_dir/debt/items/{id}.yaml  (canonical SSOT)
      2. omo_dir.parent/omo/.omo/debt/items/{id}.yaml  (submodule-local state)

    Returns the first existing file; if none, returns the canonical path
    (which will then fail at the caller with a clear "not found" message).
    """
    candidates = [
        omo_dir / "debt" / "items" / f"{item_id}.yaml",
        omo_dir.parent / "omo" / ".omo" / "debt" / "items" / f"{item_id}.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path, _load_yaml(path)
    return candidates[0], _load_yaml(candidates[0])


def classify_review_sections(items: tuple[DebtItem, ...]) -> dict[str, list[str]]:
    sections = {
        "newly_registered": [],
        "closed": [],
        "drifted": [],
        "escalated": [],
        "reopened": [],
    }
    for item in items:
        actions = [entry["action"] for entry in item.history]
        if "register" in actions:
            sections["newly_registered"].append(item.id)
        if "close" in actions or item.lifecycle_state == "closed":
            sections["closed"].append(item.id)
        if item.entropy_class in {"pointer", "time"} and item.lifecycle_state != "closed":
            sections["drifted"].append(item.id)
        if "escalate" in actions or item.gate_level == "gate":
            sections["escalated"].append(item.id)
        if "reopen" in actions:
            sections["reopened"].append(item.id)
    return sections
