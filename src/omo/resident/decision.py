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

from omo.resident import WORKSPACE, write_path

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
    write_path(INBOX_DIR).mkdir(parents=True, exist_ok=True)
    target = write_path(INBOX_DIR) / f"decision-{ts}-{slug}.md"
    if target.exists():
        return target
    target.write_text(_render_proposal_md(result), encoding="utf-8")
    return target


def _write_proposal(result: dict[str, Any], trace_id: str) -> str | None:
    write_path(PROPOSAL_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    slug = _safe_slug(trace_id or "event")
    path = write_path(PROPOSAL_DIR) / f"decision-{ts}-{slug}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    # T10-13: 增量双写 md 收件箱 (同一提案的人读视图)
    _write_proposal_md(result, ts=ts, slug=slug)
    try:
        return str(path.relative_to(WORKSPACE))
    except ValueError:  # profile 声明后提案落在 state 根: 退回相对提案目录
        return str(path.relative_to(write_path(PROPOSAL_DIR)))


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
    # T10-57: drop provenance-free trigger events — without a trace/event id
    # the draft is an unreadable "?" placeholder and the raw event still lives
    # in the event stream for triage.
    if not trace_id:
        return None
    # T10-57: at most one draft per (event_type, trace_id) per UTC day —
    # retries of the same failure must not append near-identical files.
    today = datetime.now(UTC).strftime("%Y%m%d")
    slug = _safe_slug(trace_id)
    for existing in write_path(PROPOSAL_DIR).glob(f"decision-{today}-*-{slug}.json"):
        try:
            data = json.loads(existing.read_text(encoding="utf-8"))
        except (OSError, ValueError):  # unreadable draft → treat as absent
            continue
        if (data.get("trigger_event") or {}).get("event_type") == event_type:
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
    if not write_path(PROPOSAL_DIR).exists():
        return []
    return sorted(write_path(PROPOSAL_DIR).glob("decision-*.json"), reverse=True)


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
    path = write_path(PROPOSAL_DIR) / file_name
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
    parser.add_argument("--async", dest="enqueue", action="store_true", help="异步入队 (T10-125)")
    parser.add_argument("--db", default=None, help="队列 sqlite 路径 (仅 --async)")
    args = parser.parse_args(argv)

    if args.enqueue:
        from omo.resident.task_queue import TaskQueue, default_db_path  # noqa: PLC0415

        raw_event = json.loads(args.json) if args.json else json.loads(sys.stdin.read() or "{}")
        event = raw_event if isinstance(raw_event, dict) else {}
        queue = TaskQueue(Path(args.db) if args.db else write_path(default_db_path()))
        result = queue.submit("bos://resident/decision/trigger", event)
        print(json.dumps({"queued": result.ok, "task_id": result.task_id, "reason": result.reason}))
        return 0 if result.ok else 2

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


# --- BET-Y1Q4-T8-21: 状态标记与归档逻辑 ---

VALID_TRIAGE_STATUSES = ("reviewed", "promoted", "dismissed")


def _parse_frontmatter(content: str) -> dict[str, str]:
    """简单 YAML frontmatter 解析."""
    meta: dict[str, str] = {}
    if not content.startswith("---"):
        return meta
    end = content.find("---", 3)
    if end == -1:
        return meta
    for line in content[3:end].strip().splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip()
    return meta


def _update_frontmatter(content: str, updates: dict[str, str]) -> str:
    """更新 frontmatter 中的指定字段."""
    if not content.startswith("---"):
        return content
    end = content.find("---", 3)
    if end == -1:
        return content

    fm_lines = content[3:end].strip().splitlines()
    updated: dict[str, str] = {}
    new_lines: list[str] = []
    for line in fm_lines:
        if ":" in line:
            key, _, value = line.partition(":")
            k = key.strip()
            if k in updates:
                new_lines.append(f"{k}: {updates[k]}")
                updated[k] = updates[k]
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)

    # 添加新字段
    for k, v in updates.items():
        if k not in updated:
            new_lines.append(f"{k}: {v}")

    return "---\n" + "\n".join(new_lines) + "\n---" + content[end + 3 :]


def mark_proposal_status(file_path: str | Path, status: str) -> bool:
    """标记单个提案的状态.

    Args:
        file_path: 提案文件路径 (md 或 json)
        status: reviewed / promoted / dismissed

    Returns:
        是否成功标记
    """
    if status not in VALID_TRIAGE_STATUSES:
        return False

    path = Path(file_path)
    if not path.exists():
        return False

    content = path.read_text(encoding="utf-8", errors="replace")

    if path.suffix == ".json":
        try:
            data = json.loads(content)
            data["triage_status"] = status
            data["triage_updated_at"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except (json.JSONDecodeError, OSError):
            return False
    else:
        # markdown 文件
        new_content = _update_frontmatter(
            content,
            {
                "triage_status": status,
                "triage_updated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        path.write_text(new_content, encoding="utf-8")
        return True


def batch_archive_status(status: str = "reviewed", dry_run: bool = False) -> dict[str, Any]:
    """批量标记所有未归档提案.

    Args:
        status: 目标状态 (默认 reviewed)
        dry_run: 仅统计不写入

    Returns:
        统计结果 {"total": N, "marked": N, "skipped": N}
    """
    if status not in VALID_TRIAGE_STATUSES:
        return {"total": 0, "marked": 0, "skipped": 0, "error": "invalid_status"}

    inbox_dir = write_path(INBOX_DIR)
    if not inbox_dir.exists():
        return {"total": 0, "marked": 0, "skipped": 0}

    total = 0
    marked = 0
    skipped = 0

    for md_file in sorted(inbox_dir.glob("decision-*.md")):
        total += 1
        content = md_file.read_text(encoding="utf-8", errors="replace")
        meta = _parse_frontmatter(content)
        current = meta.get("triage_status")

        if current in VALID_TRIAGE_STATUSES:
            skipped += 1
            continue

        if not dry_run:
            mark_proposal_status(md_file, status)
        marked += 1

    return {"total": total, "marked": marked, "skipped": skipped}


def get_archive_progress() -> dict[str, Any]:
    """获取归档进度统计."""
    inbox_dir = write_path(INBOX_DIR)
    if not inbox_dir.exists():
        return {"total": 0, "reviewed": 0, "promoted": 0, "dismissed": 0, "unreviewed": 0}

    total = 0
    by_status: dict[str, int] = {}

    for md_file in sorted(inbox_dir.glob("decision-*.md")):
        total += 1
        content = md_file.read_text(encoding="utf-8", errors="replace")
        meta = _parse_frontmatter(content)
        s = meta.get("triage_status") or "unreviewed"
        by_status[s] = by_status.get(s, 0) + 1

    by_status["total"] = total
    by_status.setdefault("reviewed", 0)
    by_status.setdefault("promoted", 0)
    by_status.setdefault("dismissed", 0)
    by_status.setdefault("unreviewed", 0)
    return by_status


if __name__ == "__main__":
    sys.exit(main())
