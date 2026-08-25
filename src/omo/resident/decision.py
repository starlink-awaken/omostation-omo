#!/usr/bin/env python3

"""decision-agent — event-driven decision proposals (WP-F).

Subscribes to failure/debt/swarm events and, when triggered, scans internal
state (reusing evolution-agent's scan_internal) and writes a proposal JSON under
`.omo/_knowledge/evolution-proposals/` carrying the triggering event's trace_id
for provenance.

T10-13: 提案可观测出口 — 除原始 JSON 外, 增量双写人读 md 收件箱
(`.omo/_knowledge/decision-proposals/`), 并提供 `omo resident decision
list/status/show` CLI 审计入口 (扫描 evolution-proposals 全量, 含历史)。

WP-F: 事件驱动决策 — 失败/债务事件 → 决策提案(可追溯)。
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

PROPOSAL_DIR = WORKSPACE / ".omo" / "_knowledge" / "evolution-proposals"
# T10-13: 人读 md 收件箱 (增量双写, 与 JSON 同 ts+slug 一一对应)
INBOX_DIR = WORKSPACE / ".omo" / "_knowledge" / "decision-proposals"
TRIGGER_EVENTS = frozenset({"WorkflowFailed", "StepFailed", "StepTimeout"})


def _safe_slug(value: str, max_len: int = 40) -> str:
    """Normalize an arbitrary trace/event id into a filesystem-safe slug.

    Keeps the *tail* (like the pre-T10-13 ``[-40:]`` behavior) since trace ids
    carry their run identifier at the end — the tail is the unique part.
    """
    value = re.sub(r"[^a-zA-Z0-9_-]", "-", str(value))
    return value.strip("-")[-max_len:] or "event"


def _render_proposal_md(result: dict[str, Any]) -> str:
    """Render a proposal dict into a human-readable markdown body (shared by
    the md-inbox writer and the ``show`` CLI command)."""
    trigger = result.get("trigger_event") or {}
    proposals = result.get("proposals") or []
    body = (
        "---\n"
        "schema: resident-decision/v1\n"
        "status: draft\n"
        f"trigger_event_type: {trigger.get('event_type') or ''}\n"
        f"trace_id: {trigger.get('trace_id') or ''}\n"
        f"workflow_run_id: {trigger.get('workflow_run_id') or ''}\n"
        f"event_id: {trigger.get('event_id') or ''}\n"
        f"proposal_count: {result.get('proposal_count') or 0}\n"
        f"generated_at: {datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        "---\n\n"
        "# 决策提案收件箱 (T10-13)\n\n"
        "## 触发事件\n\n"
        f"- event_type: {trigger.get('event_type') or ''}\n"
        f"- trace_id: {trigger.get('trace_id') or ''}\n"
        f"- workflow_run_id: {trigger.get('workflow_run_id') or ''}\n"
        f"- event_id: {trigger.get('event_id') or ''}\n\n"
        f"## 提案内容 ({len(proposals)} 条)\n\n"
    )
    if proposals:
        for i, p in enumerate(proposals, start=1):
            body += (
                f"### {i}. [{p.get('level') or '?'}] {p.get('action') or '?'}\n\n"
                f"- 类型: {p.get('type') or '?'} (severity: {p.get('severity') or '?'})\n"
                f"- 建议: {p.get('proposal') or ''}\n\n"
            )
    else:
        body += "_(无提案内容 — 仅触发事件溯源)_\n\n"
    return body


def _write_proposal_md(result: dict[str, Any], *, ts: str, slug: str) -> Path | None:
    """Write a human-readable markdown copy of a decision proposal.

    The md inbox mirrors the JSON proposal one-to-one (same ts+slug). Writing
    is idempotent — an existing file is never overwritten (each proposal is
    unique by timestamp; a retry of the same event keeps the first draft).
    """
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    target = INBOX_DIR / f"decision-{ts}-{slug}.md"
    if target.exists():
        return target
    target.write_text(_render_proposal_md(result), encoding="utf-8")
    return target


def _write_proposal(result: dict[str, Any], trace_id: str) -> str | None:
    PROPOSAL_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    slug = _safe_slug(trace_id or "event")
    path = PROPOSAL_DIR / f"decision-{ts}-{slug}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    # T10-13: 增量双写 md 收件箱 (同一提案的人读视图)
    _write_proposal_md(result, ts=ts, slug=slug)
    return str(path.relative_to(WORKSPACE))


def _scan_proposals() -> list[dict[str, Any]]:
    """Reuse evolution-agent's scan_internal to surface improvement opportunities."""
    try:
        agent_path = WORKSPACE / "bin" / "ssot" / "evolution-agent.py"
        spec = importlib.util.spec_from_file_location("evolution_agent", agent_path)
        assert spec is not None and spec.loader is not None
        agent = importlib.util.module_from_spec(spec)
        sys.modules["evolution_agent"] = agent
        spec.loader.exec_module(agent)
        return agent.scan_internal()
    except Exception:  # noqa: BLE001 - decision scan is best-effort
        return []


def _decide(event: dict[str, Any]) -> str | None:
    """One event → decision proposal (with trace_id provenance)."""
    trace_id = str(event.get("trace_id") or event.get("event_id") or "")
    event_type = str(event.get("event_type") or "")
    if event_type not in TRIGGER_EVENTS:
        return None
    proposals = _scan_proposals()
    result = {
        "schema": "resident-decision/v1",
        "trigger_event": {
            "event_type": event_type,
            "trace_id": trace_id,
            "workflow_run_id": event.get("workflow_run_id"),
            "event_id": event.get("event_id"),
        },
        "proposal_count": len(proposals),
        "proposals": proposals,
    }
    return _write_proposal(result, trace_id)


def register_with_daemon(daemon_module: Any) -> None:
    """Wire the decision handler into resident-orchestrator-daemon.

    Decision writes are read-only-ish (proposal JSON under evolution-proposals)
    so they register as ``safe``.
    """
    daemon_module.register_handler("decision_agent", _decision_handler, safe=True)


def _decision_handler(event: dict[str, Any]) -> None:
    path = _decide(event)
    if path is not None:
        print(f"[decision-agent] proposal_written {path}", file=sys.stderr)


# --- T10-13: 提案可观测出口 (CLI 审计) ---


def _iter_proposal_files() -> list[Path]:
    """All proposal JSONs under evolution-proposals, newest first."""
    if not PROPOSAL_DIR.exists():
        return []
    return sorted(PROPOSAL_DIR.glob("decision-*.json"), reverse=True)


def _load_proposal(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _proposal_ts(path: Path) -> str:
    """Extract the ``YYYYMMDD-HHMMSS`` timestamp from a decision-*.json name."""
    m = re.search(r"decision-(\d{8}-\d{6})", path.name)
    return m.group(1) if m else "?"


def _list_proposals(limit: int) -> str:
    """Newest-first listing of recent proposals plus event_type distribution."""
    files = _iter_proposal_files()
    total = len(files)
    lines = [f"decision proposals: {total} (最近 {limit} 条)", ""]
    types: dict[str, int] = {}
    for f in files[:limit]:
        data = _load_proposal(f)
        if data is None:
            continue
        trigger = data.get("trigger_event") or {}
        et = str(trigger.get("event_type") or "?")
        types[et] = types.get(et, 0) + 1
        lines.append(
            f"- {f.name}  [{et}] trace_id={trigger.get('trace_id') or '?'} proposals={data.get('proposal_count') or 0}"
        )
    lines.append("")
    lines.append(f"event_type 分布 (最近 {limit} 条): {json.dumps(types, ensure_ascii=False)}")
    return "\n".join(lines)


def _status_text() -> str:
    """Full-history snapshot: totals, trigger-event and proposal-type spreads."""
    files = _iter_proposal_files()
    total = len(files)
    trigger_types: dict[str, int] = {}
    action_levels: dict[str, int] = {}
    action_types: dict[str, int] = {}
    for f in files:
        data = _load_proposal(f)
        if data is None:
            continue
        trigger = data.get("trigger_event") or {}
        et = str(trigger.get("event_type") or "?")
        trigger_types[et] = trigger_types.get(et, 0) + 1
        for p in data.get("proposals") or []:
            lv = str(p.get("level") or "?")
            action_levels[lv] = action_levels.get(lv, 0) + 1
            ty = str(p.get("type") or "?")
            action_types[ty] = action_types.get(ty, 0) + 1
    latest = _proposal_ts(files[0]) if files else "n/a"
    return "\n".join(
        [
            f"decision proposals: {total}",
            f"latest proposal: {latest}",
            f"trigger event 分布: {json.dumps(trigger_types, ensure_ascii=False)}",
            f"建议 level 分布: {json.dumps(action_levels, ensure_ascii=False)}",
            f"建议 type 分布: {json.dumps(action_types, ensure_ascii=False)}",
        ]
    )


def _show_command(file_name: str) -> int:
    if "/" in file_name or ".." in file_name:
        print(f"[error] 只接受文件名 (非路径): {file_name}", file=sys.stderr)
        return 2
    path = PROPOSAL_DIR / file_name
    if not path.exists() or not path.name.startswith("decision-"):
        print(f"[error] 提案不存在: {file_name}", file=sys.stderr)
        return 2
    data = _load_proposal(path)
    if data is None:
        print(f"[error] 无法解析: {file_name}", file=sys.stderr)
        return 2
    print(_render_proposal_md(data))
    return 0


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")
    p_list = sub.add_parser("list", help="按时间倒序列出最近提案 (含 event_type 分布)")
    p_list.add_argument("--limit", type=int, default=10, help="条数 (默认 10)")
    sub.add_parser("status", help="全量统计快照 (总提案/触发事件/建议 level/type 分布)")
    p_show = sub.add_parser("show", help="渲染单条提案 JSON 为人读 md")
    p_show.add_argument("file", help="evolution-proposals 下 decision-*.json 文件名")
    parser.add_argument("--json", help="事件 JSON 字符串 (兼容原事件消费模式)")
    args = parser.parse_args(argv)

    if args.command == "list":
        print(_list_proposals(args.limit))
        return 0
    if args.command == "status":
        print(_status_text())
        return 0
    if args.command == "show":
        return _show_command(args.file)

    # 兼容原模式: 事件 JSON 消费 (daemon 实际走 _decision_handler, 此处供 CLI/管道)
    event = json.loads(args.json) if args.json else json.loads(sys.stdin.read())
    path = _decide(event)
    print(json.dumps({"written": path is not None, "path": path}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
