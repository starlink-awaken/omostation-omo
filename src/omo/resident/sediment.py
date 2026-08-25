#!/usr/bin/env python3

"""knowledge-sediment — turn workflow-mesh events into knowledge drafts.

Consumes ledger events (success → run retro draft; failure → failure pattern
draft) and writes them under `.omo/_knowledge/sediment/`. These are
event-driven drafts (traceable, verifiable) that a resident knowledge agent or
the human can later consolidate into full retros/patterns.

Wired into resident-orchestrator-daemon via register_with_daemon().
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

SEDIMENT_ROOT = WORKSPACE / ".omo" / "_knowledge" / "sediment"
SUCCESS_EVENTS = frozenset({"WorkflowSucceeded", "WorkflowClosed", "WorkflowAdmitted"})
FAILURE_EVENTS = frozenset({"WorkflowFailed", "StepFailed", "StepTimeout"})
# 个人文件信号 (personal-signals 渠道) — 沉淀为知识草稿 (M3.1 输入渠道激活)
SIGNAL_EVENTS = frozenset({"PersonalSignal"})
# workflow 生命周期事件 (T10-12): 按 run_id 聚合 → runs 草稿 (幂等, 同 run 多事件不覆盖)
LIFECYCLE_EVENTS = frozenset({"WorkflowRequested", "StepStarted", "StepDispatched"})
# 外部证据记录 (T10-12): → evidence 草稿 (带 event_id 溯源)
EVIDENCE_EVENTS = frozenset({"EvidenceRecorded"})


def _safe_slug(value: str, max_len: int = 80) -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]", "-", str(value))
    return value.strip("-")[:max_len] or "unknown"


def _utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _event_type(event: dict[str, Any]) -> str:
    return str(event.get("event_type") or "")


def _sediment_run(event: dict[str, Any], *, kind: str) -> Path | None:
    """Write a sediment draft for one event; returns the file path or None.

    Idempotent: if the target already exists (same run already sedimented) we
    return the path without rewriting, so a run's multiple lifecycle events
    never overwrite the first (usually WorkflowRequested, which carries the
    objective) draft.
    """
    run_id = str(event.get("workflow_run_id") or event.get("trace_id") or "unknown")
    event_id = str(event.get("event_id") or "")
    slug = _safe_slug(run_id)
    if kind == "failure":
        target = SEDIMENT_ROOT / "failures" / f"{slug}-{event_id[:8]}.md"
        title = "失败模式沉淀(事件驱动草稿)"
        section = "## 失败上下文"
    elif kind == "lifecycle":
        target = SEDIMENT_ROOT / "runs" / f"{slug}.md"
        title = "生命周期沉淀(事件驱动草稿)"
        section = "## 生命周期上下文"
    else:
        target = SEDIMENT_ROOT / "runs" / f"{slug}.md"
        title = "运行复盘沉淀(事件驱动草稿)"
        section = "## 运行上下文"
    if target.exists():
        return target  # 幂等: 同 run 多事件已归因, 不覆盖
    target.parent.mkdir(parents=True, exist_ok=True)
    body = (
        f"# {title}\n\n"
        f"- event_type: {_event_type(event)}\n"
        f"- workflow_run_id: {run_id}\n"
        f"- trace_id: {event.get('trace_id')}\n"
        f"- event_id: {event_id}\n"
        f"- occurred_at: {event.get('occurred_at')}\n"
        f"- generated_at: {_utc()}\n"
        f"- status: draft (事件驱动生成, 待运营 agent/人工完善为完整 retro/pattern)\n\n"
        f"{section}\n\n"
        f"- producer: {event.get('producer')}\n"
        f"- payload: 事件侧元数据见 ledger sequence(可通过 event_id 追溯)\n\n"
        f"## 待补充(五问/模式提炼)\n\n"
        f"- [ ] 计划 vs 实际\n- [ ] 结果与证据\n- [ ] 关键发现\n- [ ] 净增减\n- [ ] 交接建议\n"
    )
    target.write_text(body, encoding="utf-8")
    return target


def _evidence_run(event: dict[str, Any]) -> Path | None:
    """Write an evidence sediment draft (EvidenceRecorded) under evidence/."""
    run_id = str(event.get("workflow_run_id") or event.get("trace_id") or "unknown")
    event_id = str(event.get("event_id") or "")
    slug = _safe_slug(run_id)
    ev_slug = _safe_slug(event_id) or "unknown"
    target = SEDIMENT_ROOT / "evidence" / f"{slug}-{ev_slug[:8]}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    body = (
        f"# 证据沉淀(事件驱动草稿)\n\n"
        f"- event_type: {_event_type(event)}\n"
        f"- workflow_run_id: {run_id}\n"
        f"- trace_id: {event.get('trace_id')}\n"
        f"- event_id: {event_id}\n"
        f"- occurred_at: {event.get('occurred_at')}\n"
        f"- generated_at: {_utc()}\n"
        f"- status: draft (外部证据记录, 待完善为可复核证据条目)\n\n"
        f"## 证据上下文\n\n"
        f"- producer: {event.get('producer')}\n"
        f"- payload: 证据元数据见 ledger sequence(可通过 event_id 追溯)\n\n"
        f"## 待补充\n\n"
        f"- [ ] 证据要点\n- [ ] 复核结论\n- [ ] 关联决策/行动\n"
    )
    target.write_text(body, encoding="utf-8")
    return target


def consume_event(event: dict[str, Any]) -> Path | None:
    """Route one event to a sediment draft; returns path or None if ignored."""
    event_type = _event_type(event)
    if event_type in SUCCESS_EVENTS:
        return _sediment_run(event, kind="success")
    if event_type in FAILURE_EVENTS:
        return _sediment_run(event, kind="failure")
    if event_type in LIFECYCLE_EVENTS:
        return _sediment_run(event, kind="lifecycle")
    if event_type in EVIDENCE_EVENTS:
        return _evidence_run(event)
    if event_type in SIGNAL_EVENTS:
        # 个人文件信号 → 信号沉淀草稿 (slug 用文件名, 溯源 trace_id)
        filename = str((event.get("payload") or {}).get("file") or "unknown")
        slug = _safe_slug(filename.removesuffix(".md")) or "personal-signal"
        target = SEDIMENT_ROOT / "signals" / f"{slug}.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        body = (
            f"# 个人信号沉淀(事件驱动草稿)\n\n"
            f"- event_type: {event_type}\n"
            f"- trace_id: {event.get('trace_id')}\n"
            f"- event_id: {event.get('event_id')}\n"
            f"- occurred_at: {event.get('occurred_at')}\n"
            f"- generated_at: {_utc()}\n"
            f"- status: draft (个人文件信号, 待完善为笔记/决策/行动)\n\n"
            f"## 信号内容 (payload)\n\n"
            f"- source: personal-signals\n"
            f"- file: {filename}\n"
            f"- content_digest: {(event.get('payload') or {}).get('content_digest')}\n\n"
            f"## 待补充\n\n"
            f"- [ ] 信号要点\n- [ ] 关联上下文\n- [ ] 建议行动\n"
        )
        target.write_text(body, encoding="utf-8")
        return target
    return None


def register_with_daemon(daemon_module: Any) -> None:
    """Wire sediment handlers into resident-orchestrator-daemon.

    Registers under the route action name ``knowledge_sediment``; the daemon's
    rule table (resident-routes.yaml) maps event types → this action.
    Sediment handlers are read-only (write drafts under .omo/_knowledge/sediment)
    so they are registered as ``safe`` (no human-approval gate required).
    """
    daemon_module.register_handler("knowledge_sediment", _sediment_dispatch, safe=True)


def _sediment_dispatch(event: dict[str, Any]) -> None:
    """Route one event to the correct sediment kind based on its type."""
    event_type = _event_type(event)
    if event_type in SUCCESS_EVENTS:
        _success_handler(event)
    elif event_type in FAILURE_EVENTS:
        _failure_handler(event)
    elif event_type in LIFECYCLE_EVENTS:
        _lifecycle_handler(event)
    elif event_type in EVIDENCE_EVENTS:
        _evidence_handler(event)
    elif event_type in SIGNAL_EVENTS:
        # 个人文件信号 → 信号沉淀草稿
        path = consume_event(event)
        if path is not None:
            _log(f"sediment_written kind=signal file={path.name}")


def _success_handler(event: dict[str, Any]) -> None:
    path = _sediment_run(event, kind="success")
    if path is not None:
        _log(f"sediment_written kind=success run={event.get('workflow_run_id')} path={path.name}")


def _failure_handler(event: dict[str, Any]) -> None:
    path = _sediment_run(event, kind="failure")
    if path is not None:
        _log(f"sediment_written kind=failure run={event.get('workflow_run_id')} path={path.name}")


def _lifecycle_handler(event: dict[str, Any]) -> None:
    path = _sediment_run(event, kind="lifecycle")
    if path is not None:
        _log(f"sediment_written kind=lifecycle run={event.get('workflow_run_id')} path={path.name}")


def _evidence_handler(event: dict[str, Any]) -> None:
    path = _evidence_run(event)
    if path is not None:
        _log(f"sediment_written kind=evidence run={event.get('workflow_run_id')} path={path.name}")


def _log(msg: str) -> None:
    print(f"[knowledge-sediment] {msg}", file=sys.stderr)


def main(argv=None) -> int:
    """CLI: consume a JSON event from stdin and write a sediment draft."""
    import argparse

    argv = argv if argv is not None else None
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", help="event JSON string")
    args = parser.parse_args(argv)
    if args.json:
        event = json.loads(args.json)
    else:
        event = json.loads(sys.stdin.read())
    path = consume_event(event)
    print(json.dumps({"written": path is not None, "path": str(path) if path else None}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
