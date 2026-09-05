#!/usr/bin/env python3
"""resident-heartbeat — 心脏角色: 活性心跳发布 + 台账沉淀 (T10-16).

heartbeat 角色私有 tick: publish_heartbeat() 调 status.snapshot() 生成
system.alive 事件 → 追加统一事件流 → daemon --role heartbeat 下一 tick 路由 →
_heartbeat_handler 沉淀 `.omo/state/resident-heartbeat.jsonl` 活性台账 (幂等)。

复用 T10-15 inbox.py 的成熟模式: 外部数据源 → 统一事件流 → daemon 按 routes
路由 → handler。
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

# 统一事件流 (daemon 消费源)
EVENTS_JSONL = WORKSPACE / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
# 活性台账 (heartbeat handler 沉淀)
HEARTBEAT_LEDGER = WORKSPACE / ".omo" / "state" / "resident-heartbeat.jsonl"
SYSTEM_ALIVE_TYPE = "system.alive"


def _utc_now() -> str:
    """UTC 毫秒级 ISO 时间戳 (time.strftime 的 %f 为字面量, 不能用于微秒)."""
    # noqa: UP017 -- cron 环境 python3=3.9 无 datetime.UTC, 须保持 timezone.utc 兼容
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # noqa: UP017


def _append_to_events_jsonl(payload: dict[str, Any], snapshot: dict[str, Any]) -> None:
    """将 system.alive 事件追加到 daemon 统一事件流 (workflow-mesh/events.jsonl)."""
    event = {
        "event_id": uuid.uuid4().hex,
        "event_type": SYSTEM_ALIVE_TYPE,
        "idempotency_key": f"heartbeat:{snapshot.get('ts', '')}:{SYSTEM_ALIVE_TYPE}",
        "occurred_at": _utc_now(),
        "payload": payload,
        "producer": "resident-heartbeat",
        "schema_version": "workflow-mesh/v1",
    }
    EVENTS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_JSONL.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def _snapshot() -> dict[str, Any]:
    """当前 resident 体系活性快照 (模块级别名, 便于测试注入)."""
    from omo.resident.status import snapshot as status_snapshot  # noqa: PLC0415

    return status_snapshot()


def _write_ledger_direct(payload: dict[str, Any], snap: dict[str, Any]) -> None:
    """直接写活性台账 (不走统一事件流)。复用 _heartbeat_handler 的台账格式。"""
    idem = f"heartbeat:{snap.get('ts', '')}:{SYSTEM_ALIVE_TYPE}"
    entry = {
        "idempotency_key": idem,
        "ts": payload.get("ts") or snap.get("ts"),
        "health": payload.get("health"),
        "degraded_components": payload.get("degraded_components", []),
        "source": "resident-heartbeat",
    }
    HEARTBEAT_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _should_write_stream(payload: dict[str, Any]) -> bool:
    """源头脑电采样 (动脉B, 2026-08-28): 判断本次心跳是否写统一事件流。

    事件流信噪比实测 4% (480 心跳 : 20 真实信号) — 正常心跳只在整点写流
    (每小时 1 条存证), 降级/恢复状态每次都写 (保证告警链路可见)。
    台账粒度不变 (2min, 直接写台账绕过事件流)。
    """
    health = str(payload.get("health") or "")
    if health not in ("ok", "recovered"):
        return True  # 降级态: 高频写流, monitor/decision 链路需要看到
    now = datetime.now(timezone.utc)  # noqa: UP017 -- cron python3.9 兼容
    return now.minute < 2  # 整点窗口: 每小时第一次 tick 写 1 条存证


def _ledger_recover_best_effort() -> None:
    """Explicit T9-02 tick-path recover (cold-start non-fatal; status also recovers)."""
    try:
        from omo.resident.ledger_check import check_and_recover  # noqa: PLC0415
        from omo.resident.status import LEDGER  # noqa: PLC0415

        check_and_recover(LEDGER)
    except Exception:  # noqa: BLE001 - recover must not block heartbeat publish
        return


def publish_heartbeat(*, dry_run: bool = False) -> dict[str, Any]:
    """heartbeat 角色私有 tick 的发布侧: 生成 system.alive 事件进统一事件流.

    调用方 (daemon per-role publish hook / CLI) 每次 tick 调用一次; 事件在下一
    tick 被 heartbeat 角色消费 → 沉淀活性台账 (2min 节律)。
    脑电采样 (2026-08-28): 正常态只在整点写流 (存证), 其余 tick 直接写台账;
    降级态恢复每次写流 (告警链路可见)。
    """
    _ledger_recover_best_effort()
    snap = _snapshot()
    payload = {
        "health": snap.get("health"),
        "degraded_components": snap.get("degraded_components", []),
        "components_summary": {name: {"ok": bool(c.get("ok"))} for name, c in (snap.get("components") or {}).items()},
        "source": "resident-heartbeat",
        "ts": snap.get("ts"),
    }
    if dry_run:
        print(f"  [dry-run] {SYSTEM_ALIVE_TYPE} health={payload['health']}", file=sys.stderr)
        return {"published": 0, "health": payload["health"]}
    if _should_write_stream(payload):
        _append_to_events_jsonl(payload, snap)
        return {"published": 1, "health": payload["health"], "ts": snap.get("ts")}
    _write_ledger_direct(payload, snap)
    return {"published": 0, "ledger_only": True, "health": payload["health"], "ts": snap.get("ts")}


def _heartbeat_handler(event: dict[str, Any]) -> None:
    """system.alive 事件 → 追加活性台账 (幂等: 同 idempotency_key 不重复)."""
    if str(event.get("event_type") or "") != SYSTEM_ALIVE_TYPE:
        return
    payload = event.get("payload") or {}
    idem = str(event.get("idempotency_key") or f"{event.get('event_id')}:{SYSTEM_ALIVE_TYPE}")
    existing: set[str] = set()
    if HEARTBEAT_LEDGER.is_file():
        try:
            existing = {
                str(json.loads(line).get("idempotency_key") or "")
                for line in HEARTBEAT_LEDGER.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
        except (OSError, json.JSONDecodeError):
            existing = set()
    if idem in existing:
        return  # 幂等: 同一次心跳不重复落账
    entry = {
        "idempotency_key": idem,
        "ts": event.get("occurred_at") or payload.get("ts"),
        "health": payload.get("health"),
        "degraded_components": payload.get("degraded_components", []),
        "event_id": event.get("event_id"),
        "source": "resident-heartbeat",
    }
    HEARTBEAT_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def register_with_daemon(daemon_module: Any) -> None:
    """Wire heartbeat handler into resident-orchestrator-daemon.

    注册到 route action 名 ``heartbeat``; daemon 的规则表 (resident-routes.yaml)
    把 system.alive 事件 → 该 action。写活性台账是只读/非破坏性, 注册为 safe。
    """
    daemon_module.register_handler("heartbeat", _heartbeat_handler, safe=True)


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dump", action="store_true", help="查看已沉淀的活性台账")
    args = parser.parse_args(argv)
    if args.dump:
        for line in HEARTBEAT_LEDGER.read_text(encoding="utf-8").splitlines() if HEARTBEAT_LEDGER.is_file() else []:
            print(line)
        return 0
    report = publish_heartbeat(dry_run=args.dry_run)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
