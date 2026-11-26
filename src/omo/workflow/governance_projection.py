"""Machine-neutral governance-ratio projection (one file per workflow start).

``.omo/_delivery/agent-workflows/**`` is anchored to the state root and
gitignored, so a CI checkout has no delivery ledger and
``check-governance-ratio.py`` can only report ``measured: false`` — the 40%
ceiling never blocks a merge on the plane where merges are decided.

This module mirrors each start event into the tracked
``.omo/_delivery/events/delivery-runs/`` plane, carrying only the fields the
classifier consumes. Free text and absolute paths stay out: the ledger holds
``actor``, ``objective`` and ``evidence`` with home-directory paths in them, and
publishing those to the repository is not what the metric needs.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from .core import registry_workspace_root, utc_now

SCHEMA_VERSION = "governance-ratio-event/v1"
PROJECTION_RELATIVE = Path(".omo") / "_delivery" / "events" / "delivery-runs"
_LEDGER_EVENT = "agent_workflow_start"


def projection_dir(registry: dict[str, Any]) -> Path:
    # Resolved per call: a module-level constant would freeze at import and
    # point at the wrong root inside a linked worktree (BET-Y2Q4-T10-233).
    return registry_workspace_root(registry) / PROJECTION_RELATIVE


_HOME_SHAPED = re.compile(r"^(?:/Users/[^/]+|/home/[^/]+)(?:/|$)")


def _strip_home(text: str) -> str:
    """Replace a home-directory prefix with `~/` so identity stays off the plane."""
    home = str(Path.home())
    if text == home:
        return "~"
    if text.startswith(home + os.sep):
        return "~/" + text[len(home) + 1 :]
    matched = _HOME_SHAPED.match(text)
    if matched:
        return "~/" + text[matched.end() :]
    return text


def _relative_to_root(value: Any, root: Path) -> str | None:
    """Repo-relative form of a path; foreign paths keep their tail minus identity.

    Dropping an out-of-root lock would change the verdict: the classifier
    substring-matches governance markers (`.omo/`) on the lock string, so the
    projection must preserve that tail.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        return candidate.as_posix()
    try:
        return candidate.resolve().relative_to(root).as_posix()
    except (ValueError, OSError):
        return _strip_home(candidate.as_posix())


def _sanitized_locks(locks: Any, root: Path) -> list[str]:
    if not isinstance(locks, (list, tuple)):
        return []
    out: set[str] = set()
    for lock in locks:
        rel = _relative_to_root(lock, root)
        if rel:
            out.add(rel)
    return sorted(out)


def build_projection_event(start_event: dict[str, Any], root: Path) -> dict[str, Any] | None:
    """Project a start event, or None when the event is not a workflow start."""
    if start_event.get("event") != _LEDGER_EVENT:
        return None
    objective = start_event.get("objective") or ""
    event: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event": _LEDGER_EVENT,
        "ts": start_event.get("ts") or utc_now(),
        "run_id": start_event.get("run_id") or "",
        "workflow_id": start_event.get("workflow_id") or "",
        "agent_profile": start_event.get("agent_profile") or "",
        "locks": _sanitized_locks(start_event.get("locks"), root),
        # Both flags are derived here so the classifier keeps its verdict
        # without the projection carrying the free text they came from.
        "human_approval": objective.startswith("[human]"),
        "is_collaboration": "collaboration" in objective.lower(),
    }
    if start_event.get("track"):
        event["track"] = str(start_event["track"])
    return event


def write_projection(registry: dict[str, Any], start_event: dict[str, Any]) -> Path | None:
    """Write one projection file per run; None when nothing was written.

    Never raises: a projection failure must not abort ``start``, but it does
    leave the run uncounted on the CI plane, which the ratio check surfaces as
    a projection gap rather than as compliance.
    """
    root = registry_workspace_root(registry)
    event = build_projection_event(start_event, root)
    if event is None or not event["run_id"]:
        return None
    target_dir = projection_dir(registry)
    target = target_dir / f"{event['run_id']}.json"
    if target.exists():
        return target
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(f".json.{os.getpid()}.tmp")
        tmp.write_text(
            json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(tmp, target)
    except OSError:
        return None
    return target
