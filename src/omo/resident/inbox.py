#!/usr/bin/env python3
"""perception-inbox-adapter — poll 感知文件夹 and publish to the bus.

Watches the perception inbox folder (default ~/Documents/@感知信号) for
new/updated markdown files, publishes a `mesh:perception:inbox` bus event per
new file, and appends an `InboxSignal` event to the unified workflow-mesh
events.jsonl (so the resident daemon's sediment role can sediment them into
knowledge drafts). Tracks a content-digest watermark so re-runs never
re-publish.

Mirrors the personal-signals-adapter (omo.resident.signals) pattern (WP-D):
感知文件夹 → 事件中心 → resident agents 可订阅.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

DEFAULT_INBOX_DIR = Path.home() / "Documents" / "@感知信号"
WATERMARK_FILE = WORKSPACE / ".omo" / "_delivery" / "perception-inbox" / "watermark.json"
TOPIC = "mesh:perception:inbox"
# 感知信号事件进入 daemon 统一事件流 (workflow-mesh), 由 sediment 沉淀为知识
EVENTS_JSONL = WORKSPACE / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
INBOX_SIGNAL_TYPE = "InboxSignal"


def _load_watermark() -> dict[str, str]:
    try:
        data = json.loads(WATERMARK_FILE.read_text(encoding="utf-8"))
        return {k: v for k, v in data.items() if isinstance(v, str)}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_watermark(processed: dict[str, str]) -> None:
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(json.dumps(processed, indent=2), encoding="utf-8")


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _publish(topic: str, payload: dict[str, Any], trace_id: str) -> bool:
    try:
        from bus_foundation.facade import event as bus_event  # noqa: PLC0415

        bus_event.publish(
            topic=topic, payload=payload, source_uri="bos://capability/perception-inbox", trace_id=trace_id
        )
        return True
    except Exception as exc:  # noqa: BLE001 - best-effort publish
        print(f"  publish_failed {topic}: {exc}", file=sys.stderr)
        return False


def _append_to_events_jsonl(payload: dict[str, Any], trace_id: str) -> None:
    """将感知信号事件追加到 daemon 统一事件流 (workflow-mesh/events.jsonl)."""
    event = {
        "event_id": uuid.uuid4().hex,
        "event_type": INBOX_SIGNAL_TYPE,
        "idempotency_key": f"{trace_id}:InboxSignal",
        "occurred_at": _utc_now(),
        "payload": payload,
        "producer": "perception-inbox",
        "schema_version": "workflow-mesh/v1",
    }
    EVENTS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_JSONL.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def _utc_now() -> str:
    """UTC 毫秒级 ISO 时间戳 (time.strftime 的 %f 为字面量, 不能用于微秒)."""
    from datetime import datetime, timezone

    # noqa: UP017 -- cron 环境 python3=3.9 无 datetime.UTC, 须保持 timezone.utc 兼容
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # noqa: UP017


def poll(*, inbox_dir: Path, dry_run: bool = False) -> dict[str, Any]:
    watermark = _load_watermark()
    files = sorted(inbox_dir.glob("*.md"))
    new_files = [p for p in files if watermark.get(p.name) != _file_digest(p)]
    report: dict[str, Any] = {"scanned": len(files), "new": len(new_files), "published": 0}
    for path in new_files:
        digest = _file_digest(path)
        payload = {
            "source": "perception-inbox",
            "file": path.name,
            "content_digest": f"sha256:{digest}",
            "size_bytes": path.stat().st_size,
            "modified_at": path.stat().st_mtime,
        }
        trace_id = f"inbox:{path.stem}"
        if dry_run:
            print(f"  [dry-run] {path.name} → {TOPIC}")
        elif _publish(TOPIC, payload, trace_id):
            report["published"] += 1
            # 同时进入 daemon 统一事件流 (sediment 会沉淀为知识草稿)
            _append_to_events_jsonl(payload, trace_id)
        watermark[path.name] = digest
    if not dry_run:
        _save_watermark(watermark)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inbox-dir", type=Path, default=DEFAULT_INBOX_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    report = poll(inbox_dir=args.inbox_dir, dry_run=args.dry_run)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
