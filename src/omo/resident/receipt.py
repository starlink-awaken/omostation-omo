#!/usr/bin/env python3
"""resident receipt — append-only 审计轨迹.

为居民守护进程分发的每条事件写入一条不可篡改的 JSONL 收据,
供 dashboard / closeout / 合规审计消费. 对齐 ADR-0203 verify/closeout 证据链.

status 枚举:
  attempted  — handler 即将执行
  ok        — handler 成功返回
  blocked   — 非 safe handler, 等待人工批准 (--yes)
  error     — handler 抛异常
  skipped   — 条件不满足 (_condition_holds=false)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

RECEIPTS_FILE = WORKSPACE / ".omo" / "_delivery" / "resident-orchestrator" / "receipts.jsonl"
MAX_RECENT = 200  # recent() 单次上限


def record(
    event_type: str,
    action: str,
    handler: str,
    status: str,
    *,
    workflow_run_id: str | None = None,
    event_id: str | None = None,
    err: str | None = None,
    safe: bool = False,
) -> dict[str, Any]:
    """追加一条 receipt; 写入失败静默 (审计日志不应阻断主流程)."""
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event_type": event_type,
        "action": action,
        "handler": handler,
        "status": status,
        "run_id": workflow_run_id or None,
        "event_id": event_id or None,
        "safe": bool(safe),
        "err": err or None,
    }
    try:
        RECEIPTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with RECEIPTS_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass
    return entry


def recent(limit: int = 50) -> list[dict[str, Any]]:
    """读最近 N 条 receipt (最新在最后)."""
    limit = max(1, min(int(limit), MAX_RECENT))
    if not RECEIPTS_FILE.is_file():
        return []
    try:
        lines = RECEIPTS_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def stats() -> dict[str, Any]:
    """轻量统计: 总数 + 各 status 计数 + 最后写入时间."""
    total = ok = blocked = error = skipped = attempted = 0
    last_ts = None
    if RECEIPTS_FILE.is_file():
        try:
            for line in RECEIPTS_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                s = e.get("status")
                if s == "ok":
                    ok += 1
                elif s == "blocked":
                    blocked += 1
                elif s == "error":
                    error += 1
                elif s == "skipped":
                    skipped += 1
                elif s == "attempted":
                    attempted += 1
                last_ts = e.get("ts")
        except OSError:
            pass
    return {
        "total": total,
        "ok": ok,
        "blocked": blocked,
        "error": error,
        "skipped": skipped,
        "attempted": attempted,
        "last_ts": last_ts,
    }
