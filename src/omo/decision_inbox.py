"""Decision-inbox broker — the authorized writer for ``.omo/state/decision-inbox.json``.

contract_gatekeeper (CR-DIRECT-IO) allows .omo mutations only inside
``src/omo/``.  cockpit's ``decide`` CLI therefore routes its inbox IO through
this module instead of touching the state plane directly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .omo_io import ensure_parent_dir, write_text_atomic

INBOX_REL = Path(".omo/state/decision-inbox.json")

EMPTY_INBOX: dict[str, Any] = {"items": [], "version": "1.0"}


def inbox_path(root: Path) -> Path:
    return root / INBOX_REL


def load(root: Path) -> dict[str, Any]:
    path = inbox_path(root)
    if not path.exists():
        return dict(EMPTY_INBOX)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return dict(EMPTY_INBOX)


def save(root: Path, data: dict[str, Any]) -> None:
    path = inbox_path(root)
    ensure_parent_dir(path)
    write_text_atomic(path, json.dumps(data, indent=2, ensure_ascii=False))
