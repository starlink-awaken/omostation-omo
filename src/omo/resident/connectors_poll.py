"""Resident Daemon connector-fetch poller (BET-Y1Q4-T5-04).

Periodically fetches incremental updates from registered connectors
using watermark-based tracking.  Each connector's last watermark is
persisted to a state file so the daemon can resume after restart.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import yaml

# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------


@dataclass
class Watermark:
    """Tracks the last-synced watermark for a single connector."""

    connector_id: str
    value: str = ""
    last_sync_ts: float = 0.0
    last_sync_ok: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> Watermark:
        return cls(
            connector_id=d.get("connector_id", ""),
            value=d.get("value", ""),
            last_sync_ts=d.get("last_sync_ts", 0.0),
            last_sync_ok=d.get("last_sync_ok", True),
        )


@dataclass
class PollerState:
    """Persisted state for the connector poller."""

    watermarks: dict[str, Watermark] = field(default_factory=dict)
    last_poll_ts: float = 0.0

    def to_dict(self) -> dict:
        return {
            "last_poll_ts": self.last_poll_ts,
            "watermarks": {k: v.to_dict() for k, v in self.watermarks.items()},
        }

    @classmethod
    def from_dict(cls, d: dict) -> PollerState:
        wms = {}
        for k, v in d.get("watermarks", {}).items():
            wms[k] = Watermark.from_dict(v)
        return cls(
            watermarks=wms,
            last_poll_ts=d.get("last_poll_ts", 0.0),
        )


def _state_path(ws: Path) -> Path:
    return ws / ".omo" / "state" / "connector-watermarks.json"


def _load_state(ws: Path) -> PollerState:
    path = _state_path(ws)
    if not path.is_file():
        return PollerState()
    try:
        return PollerState.from_dict(json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return PollerState()


def _save_state(ws: Path, state: PollerState) -> None:
    path = _state_path(ws)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Manifest loading
# ---------------------------------------------------------------------------


def _load_manifest(ws: Path) -> list[dict]:
    """Load the connector manifest and return only incremental connectors."""
    manifest_path = ws / ".omo" / "_truth" / "registry" / "connector-manifest.yaml"
    if not manifest_path.is_file():
        return []
    try:
        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    return [c for c in data.get("connectors", []) if c.get("incremental") and c.get("status") == "active"]


# ---------------------------------------------------------------------------
# Poller
# ---------------------------------------------------------------------------


@dataclass
class SyncResult:
    """Result of a single connector sync attempt."""

    connector_id: str
    ok: bool
    new_watermark: str
    events_emitted: int
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def poll_connectors(
    ws: Path,
    dry_run: bool = False,
    interval_seconds: float = 300.0,
) -> list[SyncResult]:
    """Poll all incremental connectors, respecting per-connector interval.

    Args:
        ws: Workspace root path.
        dry_run: If true, don't persist state or emit events.
        interval_seconds: Minimum seconds between syncs per connector.

    Returns:
        List of SyncResult for each connector polled this round.
    """
    state = _load_state(ws)
    connectors = _load_manifest(ws)
    now = time.time()

    results: list[SyncResult] = []

    for conn in connectors:
        cid = conn.get("id", "")
        if not cid:
            continue

        wm = state.watermarks.get(cid)
        if wm and (now - wm.last_sync_ts) < interval_seconds:
            continue  # not yet time to poll

        # Simulate sync: generate a new watermark based on current time
        new_wm = f"ts:{int(now)}"
        # In a real implementation, this would call the connector's fetch API
        events_emitted = 1  # placeholder

        result = SyncResult(
            connector_id=cid,
            ok=True,
            new_watermark=new_wm,
            events_emitted=events_emitted,
        )
        results.append(result)

        if not dry_run:
            state.watermarks[cid] = Watermark(
                connector_id=cid,
                value=new_wm,
                last_sync_ts=now,
                last_sync_ok=True,
            )

    if not dry_run:
        state.last_poll_ts = now
        _save_state(ws, state)

    return results


def list_watermarks(ws: Path) -> dict[str, Watermark]:
    """Return current watermark state for all tracked connectors."""
    state = _load_state(ws)
    return dict(state.watermarks)


def reset_watermark(ws: Path, connector_id: str) -> bool:
    """Clear the watermark for a connector, forcing a full re-sync."""
    state = _load_state(ws)
    if connector_id in state.watermarks:
        del state.watermarks[connector_id]
        _save_state(ws, state)
        return True
    return False
