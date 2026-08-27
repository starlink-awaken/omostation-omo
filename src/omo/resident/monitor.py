#!/usr/bin/env python3
"""resident-monitor — 眼睛角色: observability 严重事件 → 告警外发 (T10-16).

monitor 角色私有 tick: publish_monitor() 增量读 observability 平面
(`.omo/_delivery/observability/events.jsonl`, 复用 alert.py 的水位/读取逻辑) →
severity ∈ {critical, degraded} 的事件 → 构造 `alert` 事件 → 追加统一事件流 →
daemon --role monitor 下一 tick 路由 → `_alert_handler` 调 alert-connectors deliver
外发 (复用 T10-14 alert.py 交付) + 沉淀告警记录 `.omo/state/resident-monitor.jsonl` (幂等)。

与 alert.py CLI (forward) 的区别: alert.py 是独立 CLI (读 observability → 直发);
monitor 走 daemon 角色私有 tick 的发布→路由→处理链路, 是 cron 中告警外发的唯一路径。
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE
from omo.resident import alert as _alert

# 统一事件流 (daemon 消费源)
EVENTS_JSONL = WORKSPACE / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
# 告警记录台账 (alert handler 沉淀)
ALERT_LEDGER = WORKSPACE / ".omo" / "state" / "resident-monitor.jsonl"
ALERT_TYPE = "alert"
# 复用 alert.py 的 observability 平面/水位/严重度定义
OBS_EVENTS = _alert.OBS_EVENTS
WATERMARK_FILE = _alert.WATERMARK_FILE
ALERT_SEVERITIES = _alert.ALERT_SEVERITIES


def _utc_now() -> str:
    """UTC 毫秒级 ISO 时间戳 (time.strftime 的 %f 为字面量, 不能用于微秒)."""
    # noqa: UP017 -- cron 环境 python3=3.9 无 datetime.UTC, 须保持 timezone.utc 兼容
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # noqa: UP017


def _append_to_events_jsonl(payload: dict[str, Any], idempotency_key: str) -> None:
    """将 alert 事件追加到 daemon 统一事件流 (workflow-mesh/events.jsonl)."""
    event = {
        "event_id": uuid.uuid4().hex,
        "event_type": ALERT_TYPE,
        "idempotency_key": idempotency_key,
        "occurred_at": _utc_now(),
        "payload": payload,
        "producer": "resident-monitor",
        "schema_version": "workflow-mesh/v1",
    }
    EVENTS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_JSONL.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def _alert_payload(event: dict[str, Any]) -> dict[str, Any]:
    """从 observability 事件构造 alert 事件的 payload (保留外发所需的原始字段)."""
    return {
        "source": "resident-monitor",
        "observability": {
            "event_id": event.get("event_id"),
            "trace_id": event.get("trace_id"),
            "type": str(event.get("type") or event.get("event_type") or ""),
            "severity": str(event.get("severity") or ""),
            "domain": str(event.get("domain") or "governance"),
            "title": _alert._event_title(event),
            "body": _alert._event_body(event),
        },
        "ts": _utc_now(),
    }


def _idempotency_key(event: dict[str, Any]) -> str:
    trace = event.get("trace_id") or event.get("event_id")
    return f"alert:{trace}"


def publish_monitor(*, dry_run: bool = False) -> dict[str, Any]:
    """monitor 角色私有 tick 的发布侧: observability 严重事件 → alert 事件进统一事件流.

    增量读 observability (复用 alert.py 水位), severity ∈ {critical, degraded} 的事件
    构造为 alert 事件追加到统一事件流; 非 dry-run 推进水位。下一 tick 被 monitor 角色
    消费 → deliver 外发 (fail-closed)。
    """
    events, file_size = _alert._read_incremental()
    published = 0
    for event in events:
        severity = str(event.get("severity") or "")
        if severity not in ALERT_SEVERITIES:
            continue
        if dry_run:
            print(
                f"  [dry-run] alert {severity} trace={event.get('trace_id')} title={_alert._event_title(event)[:40]}",
                file=sys.stderr,
            )
            continue
        _append_to_events_jsonl(_alert_payload(event), _idempotency_key(event))
        published += 1
    if not dry_run:
        _alert._save_byte_offset(file_size)
        _write_alive_heartbeat(events_scanned=len(events), published=published)
    return {"events_scanned": len(events), "published": published, "severities": sorted(ALERT_SEVERITIES)}


def _write_alive_heartbeat(*, events_scanned: int, published: int) -> None:
    """自证心跳 (2026-08-28 P1b, 深度复盘 F5): 每小时一条 alive 记录写入台账.

    区分"安静"和"死亡": 无告警时 monitor 零痕迹, 台账超 N 小时无记录无法判断
    monitor 是否活着。心跳按小时幂等 (monitor-alive:YYYYMMDDTHH), kind=heartbeat
    与真实告警记录区分, 不触发外发。
    """
    now = datetime.now(timezone.utc)  # noqa: UP017 -- cron python3.9 兼容 (同 _utc_now)
    hour_key = f"monitor-alive:{now.strftime('%Y%m%dT%H')}"
    if ALERT_LEDGER.is_file():
        try:
            existing = {
                str(json.loads(line).get("idempotency_key") or "")
                for line in ALERT_LEDGER.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
            if hour_key in existing:
                return  # 本小时已写过
        except (OSError, json.JSONDecodeError):
            pass
    entry = {
        "idempotency_key": hour_key,
        "ts": _utc_now(),
        "kind": "heartbeat",
        "events_scanned": events_scanned,
        "published": published,
        "source": "resident-monitor",
    }
    ALERT_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _alert_handler(event: dict[str, Any]) -> None:
    """alert 事件 → 调 alert-connectors deliver 外发 + 沉淀告警记录 (幂等)."""
    if str(event.get("event_type") or "") != ALERT_TYPE:
        return
    payload = event.get("payload") or {}
    obs = payload.get("observability") or {}
    idem = str(event.get("idempotency_key") or f"{event.get('event_id')}:{ALERT_TYPE}")
    existing: set[str] = set()
    if ALERT_LEDGER.is_file():
        try:
            existing = {
                str(json.loads(line).get("idempotency_key") or "")
                for line in ALERT_LEDGER.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
        except (OSError, json.JSONDecodeError):
            existing = set()
    if idem in existing:
        return  # 幂等: 同一告警不重复外发
    severity = str(obs.get("severity") or "critical")
    # 重建 observability 形状交给 alert.py deliver (alert-connectors 契约)
    alert_event = {
        "severity": severity,
        "domain": obs.get("domain") or "governance",
        "title": obs.get("title") or _alert._event_title(event),
        "message": obs.get("body") or "",
        "type": obs.get("type") or ALERT_TYPE,
        "trace_id": obs.get("trace_id") or event.get("event_id"),
        "event_id": obs.get("event_id") or event.get("event_id"),
        "payload": payload,
    }
    delivered = _alert._send_alert(alert_event)
    entry = {
        "idempotency_key": idem,
        "ts": event.get("occurred_at") or payload.get("ts"),
        "severity": severity,
        "trace_id": obs.get("trace_id") or event.get("event_id"),
        "event_id": event.get("event_id"),
        "delivered": delivered,
        "source": "resident-monitor",
    }
    ALERT_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def register_with_daemon(daemon_module: Any) -> None:
    """Wire alert handler into resident-orchestrator-daemon.

    注册到 route action 名 ``alert``; daemon 的规则表 (resident-routes.yaml) 把
    alert 事件 → 该 action。告警外发是只读 observability + 网络推送, 注册为 safe。
    """
    daemon_module.register_handler("alert", _alert_handler, safe=True)


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dump", action="store_true", help="查看已沉淀的告警记录")
    args = parser.parse_args(argv)
    if args.dump:
        for line in ALERT_LEDGER.read_text(encoding="utf-8").splitlines() if ALERT_LEDGER.is_file() else []:
            print(line)
        return 0
    report = publish_monitor(dry_run=args.dry_run)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
