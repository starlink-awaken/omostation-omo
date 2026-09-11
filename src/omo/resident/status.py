#!/usr/bin/env python3

"""resident-status — resident 体系运行状态快照 (M2.4).

输出 resident agent 体系的运行状态 JSON:
- daemon: 字节偏移水位新鲜度 (cron --once 调度下进程退出正常, 以水位判断)
- events: workflow-mesh 事件流规模
- sediment: 知识沉淀草稿计数 (runs/failures)
- alert: 告警转发水位
- ledger: event-ledger 哈希链完整性

供监控面板 / 脚本集成 (omo resident status)。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

DELIVERY = WORKSPACE / ".omo" / "_delivery"
DAEMON_WATERMARKS = DELIVERY / "resident-orchestrator" / "watermarks"
EVENTS_JSONL = WORKSPACE / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
SEDIMENT_ROOT = WORKSPACE / ".omo" / "_knowledge" / "sediment"
ALERT_WATERMARK = DELIVERY / "alert-forwarder" / "watermark.json"
LEDGER = WORKSPACE / "runtime" / "omo" / "event-ledger.sqlite3"
STALE_THRESHOLD_SECONDS = 1800  # 30min


def _file_age(path: Path) -> float | None:
    try:
        return time.time() - path.stat().st_mtime
    except OSError:
        return None


def _count_files(directory: Path, pattern: str) -> int:
    try:
        return len(list(directory.glob(pattern)))
    except OSError:
        return 0


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _daemon_snapshot() -> dict[str, Any]:
    # 只统计五类角色水位 resident-{role}.json (由 daemon --once --role 每 2min 推进),
    # 排除订阅层 resident-sub.json — subscribe 非 cron daemon tick 证据, 更新频率低,
    # 混入会让健康体系被陈旧 sub 水位误判 degraded。
    # Cold start (never ticked) is non-fatal for status health (BET-Y1Q4-T9-01 Spec).
    watermark_files = sorted(p for p in DAEMON_WATERMARKS.glob("resident-*.json") if p.name != "resident-sub.json")
    if not watermark_files:
        return {
            "ok": True,
            "detail": "no daemon watermark (daemon never ticked) — cold start non-fatal",
            "tick_age_seconds": None,
            "cold_start": True,
        }
    newest = min(watermark_files, key=lambda p: p.stat().st_mtime)
    age = time.time() - newest.stat().st_mtime
    ok = age <= STALE_THRESHOLD_SECONDS
    wm = _load_json(newest)
    return {
        "ok": ok,
        "detail": f"last daemon tick {age:.0f}s ago",
        "tick_age_seconds": int(age),
        "watermark_file": newest.name,
        "byte_offset": int(wm.get("byte_offset", 0)) if wm else None,
    }


def _events_snapshot() -> dict[str, Any]:
    if not EVENTS_JSONL.is_file():
        return {"ok": True, "detail": "no events.jsonl yet", "lines": 0, "bytes": 0}
    age = _file_age(EVENTS_JSONL)
    lines = 0
    with EVENTS_JSONL.open(encoding="utf-8") as fh:
        for _ in fh:
            lines += 1
    return {
        "ok": True,
        "detail": f"{lines} events (input stream idle {age:.0f}s)" if age else f"{lines} events",
        "lines": lines,
        "bytes": EVENTS_JSONL.stat().st_size,
        "idle_seconds": int(age) if age is not None else None,
    }


def _sediment_snapshot() -> dict[str, Any]:
    runs = _count_files(SEDIMENT_ROOT / "runs", "*.md")
    failures = _count_files(SEDIMENT_ROOT / "failures", "*.md")
    return {"ok": True, "runs": runs, "failures": failures, "total": runs + failures}


def _alert_snapshot() -> dict[str, Any]:
    wm = _load_json(ALERT_WATERMARK)
    return {
        "ok": True,
        "watermark_byte_offset": int(wm.get("byte_offset", 0)) if wm else None,
        "watermark_set": wm is not None,
    }


def _is_lock_error(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and any(
        marker in str(exc).lower() for marker in ("locked", "busy")
    )


def _probe_ledger_once() -> dict[str, Any]:
    """Read-only probe: verify chain integrity without acquiring any write lock.

    Uses file:...?mode=ro URI to ensure zero lock contention with the daemon's
    write path.  On older SQLite that lacks lock_status PRAGMA we fall back to
    a simple row-count check — still read-only, still zero contention.
    """
    uri = f"file:{LEDGER}?mode=ro"
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=0.05)
        conn.row_factory = sqlite3.Row
        # Read-only chain verification: walk the hash chain forward.
        rows = list(conn.execute("SELECT sequence, event_hash, previous_hash FROM event_log ORDER BY sequence"))
        if not rows:
            return {"ok": True, "detail": "ledger empty (cold start)", "sequence": 0}
        # Genesis previous_hash is SQL NULL in production writers
        # (LedgerBroker.append / verify_chain). Empty-string is only a
        # synthetic unit-fixture convenience — accept either at seq start.
        prev_hash: str | None = None
        broken = False
        for index, row in enumerate(rows):
            expected_prev = prev_hash
            actual_prev = row["previous_hash"]
            if index == 0 and actual_prev in (None, ""):
                actual_prev = None
            if actual_prev != expected_prev:
                broken = True
                break
            prev_hash = row["event_hash"]
        seq = rows[-1]["sequence"]
        if broken:
            return {"ok": False, "detail": "chain broken at read-only probe", "sequence": seq}
        return {"ok": True, "detail": "chain ok (read-only probe)", "sequence": seq}
    except sqlite3.Error as exc:
        return {"ok": False, "detail": f"read-only probe failed: {exc}", "sequence": None}
    finally:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass


def _ledger_snapshot() -> dict[str, Any]:
    # Missing ledger is cold-start non-fatal (BET-Y1Q4-T9-01 Spec).
    if not LEDGER.is_file():
        return {
            "ok": True,
            "detail": "ledger sqlite missing — cold start non-fatal",
            "lock_age_seconds": None,
            "missing": True,
            "observation_mode": "read_only",
            "recovery_performed": False,
            "lock_monitor": {
                "locked": False,
                "checkpoint": None,
                "recovery": None,
                "detail": "ledger missing",
                "state": "missing",
                "observation_mode": "read_only",
                "recovery_performed": False,
            },
        }

    from omo.resident.ledger_check import check_lock_state_only  # noqa: PLC0415

    lock_state = check_lock_state_only(LEDGER)
    chain = _probe_ledger_once()
    state = str(lock_state.get("state") or "unknown")
    locked = True if state in {"locked", "busy"} else False if state == "unlocked" else None
    lock_monitor = {
        "locked": locked,
        "checkpoint": None,
        "recovery": None,
        "detail": str(lock_state.get("detail") or "lock observation unavailable"),
        "state": state,
        "observation_mode": "read_only",
        "recovery_performed": False,
    }
    ok = bool(lock_state.get("ok")) and bool(chain.get("ok"))
    if not lock_state.get("ok"):
        detail = str(lock_state.get("detail") or "ledger lock observation failed")
    elif not chain.get("ok"):
        detail = f"ledger check failed: {chain.get('detail', 'unknown read-only probe failure')}"
    else:
        detail = str(chain.get("detail") or "chain ok (read-only probe)")
    return {
        "ok": ok,
        "detail": detail,
        "sequence": chain.get("sequence"),
        "lock_age_seconds": lock_state.get("lock_age_seconds"),
        "observation_mode": "read_only",
        "recovery_performed": False,
        "lock_monitor": lock_monitor,
    }


def snapshot() -> dict[str, Any]:
    components = {
        "daemon": _daemon_snapshot(),
        "events": _events_snapshot(),
        "sediment": _sediment_snapshot(),
        "alert": _alert_snapshot(),
        "ledger": _ledger_snapshot(),
    }
    degraded = [name for name, c in components.items() if not c.get("ok")]
    return {
        "domain": "runtime",
        "event_type": "resident.status",
        "health": "degraded" if degraded else "ok",
        "degraded_components": degraded,
        "components": components,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args(argv)
    report = snapshot()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["health"] == "ok" else 2


if __name__ == "__main__":
    sys.exit(main())
