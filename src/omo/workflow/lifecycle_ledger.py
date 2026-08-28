"""Ledger helpers for workflow lifecycle."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .core import ledger_path, utc_now


def append_ledger_event(registry: dict[str, Any], event: dict[str, Any]) -> None:
    path = ledger_path(registry)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ts": utc_now(), **event}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _extract_run_timestamp(run_id: str) -> str | None:
    """Parse the timestamp embedded in a run_id like 20260723T062855Z-..."""
    if len(run_id) < 16 or not run_id[:4].isdigit():
        return None
    return f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}T{run_id[9:11]}:{run_id[11:13]}:{run_id[13:15]}Z"


def ledger_mentions_run(registry: dict[str, Any], run_id: str) -> bool:
    path = ledger_path(registry)
    if not path.exists() or path.stat().st_size == 0:
        return False
    needle = f'"run_id": "{run_id}"'
    needle_alt = f'"run_id":"{run_id}"'
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return needle in text or needle_alt in text


def heal_ledger_for_run(
    registry: dict[str, Any],
    run_id: str,
    payload: dict[str, Any],
) -> bool:
    """ADR-0209 A2: if ledger has no event for a known run, replay from run yaml.

    Reconstructs a minimal start (and close if terminal) event so observe/compliance
    do not warn forever after events.jsonl was trimmed externally.
    Returns True when a heal write happened.
    """
    if ledger_mentions_run(registry, run_id):
        return False
    original_ts = payload.get("created_at") or _extract_run_timestamp(run_id)
    append_ledger_event(
        registry,
        {
            "event": "agent_workflow_start",
            "run_id": run_id,
            "workflow_id": payload.get("workflow_id"),
            "actor": payload.get("actor"),
            "agent_profile": payload.get("agent_profile"),
            "objective": payload.get("objective"),
            "path": payload.get("path"),
            "locks": payload.get("locks") or [],
            "healed": True,
            "heal_reason": "ledger_missing_run_replay_from_run_yaml",
            "ts": original_ts or utc_now(),
        },
    )
    status = str(payload.get("status") or "")
    if status in {"ok", "failed", "blocked"}:
        close_ts = payload.get("closed_at") or payload.get("updated_at") or original_ts
        append_ledger_event(
            registry,
            {
                "event": "agent_workflow_close",
                "run_id": run_id,
                "workflow_id": payload.get("workflow_id"),
                "status": status,
                "evidence": payload.get("evidence") or [],
                "healed": True,
                "heal_reason": "ledger_missing_run_replay_from_run_yaml",
                "ts": close_ts or utc_now(),
            },
        )
    return True
