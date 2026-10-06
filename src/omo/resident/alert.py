#!/usr/bin/env python3

"""alert-forwarder — forward observability events to alert channels.

Incrementally reads the observability event plane
(`.omo/_delivery/observability/events.jsonl`), and for events with
severity ∈ {critical, degraded} routes an alert to the configured channels via
alert-connectors (slack/feishu/wecom). Deduplicates by trace_id.

WP-E: 监控事件 → 告警通道(复用 observability-events + alert-connectors)。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE, write_path

OBS_EVENTS = WORKSPACE / ".omo" / "_delivery" / "observability" / "events.jsonl"
WATERMARK_FILE = WORKSPACE / ".omo" / "_delivery" / "alert-forwarder" / "watermark.json"
ALERT_SEVERITIES = frozenset({"critical", "degraded"})


def _load_byte_offset() -> int:
    try:
        return int(json.loads(write_path(WATERMARK_FILE).read_text(encoding="utf-8")).get("byte_offset", 0))
    except (OSError, json.JSONDecodeError):
        return 0


def _save_byte_offset(offset: int) -> None:
    write_path(WATERMARK_FILE).parent.mkdir(parents=True, exist_ok=True)
    write_path(WATERMARK_FILE).write_text(json.dumps({"byte_offset": offset}), encoding="utf-8")


def _read_incremental() -> tuple[list[dict[str, Any]], int]:
    if not write_path(OBS_EVENTS).is_file():
        return [], 0
    file_size = write_path(OBS_EVENTS).stat().st_size
    offset = _load_byte_offset()
    if file_size < offset:
        offset = 0
    if offset == file_size:
        return [], file_size
    events: list[dict[str, Any]] = []
    with write_path(OBS_EVENTS).open("rb") as fh:
        fh.seek(offset)
        data = fh.read()
    for line in data.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events, file_size


def _event_title(event: dict[str, Any]) -> str:
    """Extract a readable title from an observability event."""
    for key in ("title", "event_type", "type"):
        value = event.get(key)
        if value:
            return str(value)
    return "resident-alert"


def _event_body(event: dict[str, Any]) -> str:
    """Extract a readable body, preferring payload detail over raw dump."""
    for key in ("message", "description"):
        value = event.get(key)
        if value:
            return str(value)
    payload = event.get("payload")
    if payload:
        return json.dumps(payload, ensure_ascii=False)[:300]
    return json.dumps(event, ensure_ascii=False)[:300]


def _load_alert_connectors() -> Any:
    """Load bin/ssot/alert-connectors.py by file path (dash name, not importable)."""
    import importlib.util

    connector_file = WORKSPACE / "bin" / "ssot" / "alert-connectors.py"
    if not connector_file.is_file():
        return None
    spec = importlib.util.spec_from_file_location("alert_connectors", connector_file)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _send_alert(event: dict[str, Any]) -> bool:
    """Route one alert via alert-connectors; returns success."""
    severity = str(event.get("severity") or "")
    domain = str(event.get("domain") or "governance")
    title = _event_title(event)
    body = _event_body(event)
    try:
        connectors = _load_alert_connectors()
        if connectors is None:
            return False
        build_connectors = connectors.build_connectors
        route_connector = connectors.route_connector

        conn = route_connector(severity, domain)
        if conn is None:
            for c in build_connectors():
                if c.channel_id == "slack":
                    conn = c
                    break
        if conn is None:
            return False
        receipt = conn.deliver({"severity": severity, "title": title, "body": body, "domain": domain})
        return receipt.get("result_state") == "delivered"
    except Exception as exc:  # noqa: BLE001 - alert is best-effort
        print(f"  alert_send_failed {severity}: {exc}", file=sys.stderr)
        return False


def forward(*, dry_run: bool = False) -> dict[str, Any]:
    events, file_size = _read_incremental()
    sent = 0
    alerted = 0
    for event in events:
        if str(event.get("severity") or "") not in ALERT_SEVERITIES:
            continue
        alerted += 1
        if dry_run:
            print(
                f"  [dry-run] alert {event.get('severity')} trace={event.get('trace_id')} title={_event_title(event)[:40]}"
            )
        elif _send_alert(event):
            sent += 1
    if not dry_run:
        _save_byte_offset(file_size)
    return {"events_scanned": len(events), "alerted": alerted, "sent": sent}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    report = forward(dry_run=args.dry_run)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
