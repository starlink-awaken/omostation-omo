#!/usr/bin/env python3

"""resident-promote — sediment 草稿 → 正式知识提升 (M4.1 阶段2).

扫描 `.omo/_knowledge/sediment/` 下的事件驱动草稿 (runs/ 复盘 + failures/
失败模式), 按 workflow 主题聚合, 生成结构化 retro 候选文档到
`.omo/_knowledge/retros/resident/`。完成"知识沉淀→能力"三阶段成长的
阶段2: 草稿从不可检索的散件 → 可检索/可统计的主题知识。

增强 (BET-Y1Q3-T10-11):
- 解析每篇草稿 frontmatter 元数据 (event_type / workflow_run_id / trace_id)
  提炼结构化指标, 而非仅列文件名。
- 产出 retro 带 YAML frontmatter (topic/counts/failure_rate/failure_breakdown),
  使指标可机器检索。
- 失败根因画像: 按 event_type 统计失败模式分布 + 关联 trace 溯源。

提升产物标记 candidate, 供运营 agent/人工完善为完整 retro/pattern。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE, ledger_trace, write_path

SEDIMENT_ROOT = WORKSPACE / ".omo" / "_knowledge" / "sediment"
RETRO_ROOT = WORKSPACE / ".omo" / "_knowledge" / "retros" / "resident"
EVENTS_PATH = WORKSPACE / ".omo" / "_knowledge" / "workflow-mesh" / "events.jsonl"
# 已聚合/超保留窗草稿的归档区 (gitignored, 可恢复)
ARCHIVE_ROOT = WORKSPACE / ".omo" / "_knowledge" / "sediment-archive"
# 文件名: {ts}Z-{workflow-type}-{run_id}[(-{event_id})].md
TOPIC_PATTERN = re.compile(r"^\d{8}T\d{6}Z-(.+?)(?:-[a-f0-9]{6,})?\.md$")
# 草稿顶部 frontmatter: `- key: value` 行 (事件侧元数据)
META_LINE = re.compile(r"^-\s+([a-z_]+):\s*(.*?)\s*$")
# 期望的 retro 输出 frontmatter schema
RETRO_SCHEMA = "resident-retro-candidate/v1"


def _utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _extract_topic(filename: str) -> str:
    """从草稿文件名提取 workflow 主题 (去时间戳/run_id/event_id 后缀)."""
    match = TOPIC_PATTERN.match(filename)
    if not match:
        return "unclassified"
    topic = match.group(1)
    # 循环去除尾部 hex 段 (run_id / event_id), 支持 multi-segment
    parts = topic.split("-")
    while parts and re.fullmatch(r"[a-f0-9]{6,}", parts[-1]):
        parts.pop()
    return "-".join(parts) or "unclassified"


def _parse_draft_meta(path: Path) -> dict[str, str]:
    """解析草稿顶部 `- key: value` frontmatter 行 (event_type/run_id/trace_id...)."""
    meta: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return meta
    for line in text.splitlines():
        m = META_LINE.match(line)
        if m:
            key, value = m.group(1), m.group(2).strip().strip('"')
            meta[key] = value
    return meta


def _scan(kind_dir: Path) -> list[tuple[str, str, Path]]:
    """返回 [(topic, filename, path)] 列表."""
    out: list[tuple[str, str, Path]] = []
    if not kind_dir.is_dir():
        return out
    for path in sorted(kind_dir.glob("*.md")):
        out.append((_extract_topic(path.name), path.name, path))
    return out


def _aggregate() -> dict[str, dict[str, Any]]:
    """按主题聚合 runs/failures 草稿, 附带解析后的 frontmatter 元数据."""
    topics: dict[str, dict[str, Any]] = {}
    for kind in ("runs", "failures"):
        entries = _scan(write_path(SEDIMENT_ROOT) / kind)
        for topic, filename, path in entries:
            bucket = topics.setdefault(
                topic, {"runs": [], "failures": [], "total": 0, "runs_meta": [], "failures_meta": []}
            )
            meta = _parse_draft_meta(path)
            bucket[kind].append(filename)
            bucket[f"{kind}_meta"].append({"filename": filename, "meta": meta})
            bucket["total"] += 1
    return topics


def _failure_breakdown(bucket: dict[str, Any]) -> dict[str, Any]:
    """失败根因画像: 按 event_type 分布 + 关联 trace (workflow_run_id) 去重溯源."""
    by_event_type: dict[str, int] = {}
    trace_ids: list[str] = []
    for item in bucket.get("failures_meta", []):
        meta = item.get("meta", {})
        et = meta.get("event_type") or "unknown"
        by_event_type[et] = by_event_type.get(et, 0) + 1
        trace = meta.get("workflow_run_id") or meta.get("trace_id")
        if trace:
            trace_ids.append(trace)
    unique_traces = sorted(set(trace_ids))
    return {
        "by_event_type": dict(sorted(by_event_type.items(), key=lambda kv: (-kv[1], kv[0]))),
        "trace_count": len(unique_traces),
        "trace_ids": unique_traces,
    }


def _counts_frontmatter(topic: str, bucket: dict[str, Any], failure_bd: dict[str, Any]) -> str:
    """生成 retro 的 YAML frontmatter (可机器检索的结构化指标)."""
    runs = len(bucket["runs"])
    failures = len(bucket["failures"])
    total = bucket["total"]
    failure_rate = round(failures / max(1, total), 4)
    lines = [
        "---",
        f"schema: {RETRO_SCHEMA}",
        f"topic: {topic}",
        f"generated_at: {_utc()}",
        "status: candidate",
        "counts:",
        f"  runs: {runs}",
        f"  failures: {failures}",
        f"  total: {total}",
        f"failure_rate: {failure_rate}",
        "failure_breakdown:",
        "  by_event_type:",
    ]
    for et, count in failure_bd["by_event_type"].items():
        lines.append(f"    {et}: {count}")
    lines.append(f"  trace_count: {failure_bd['trace_count']}")
    lines.append("---")
    return "\n".join(lines) + "\n"


def _write_retro(
    topic: str,
    bucket: dict[str, Any],
    dry_run: bool,
    fill_five_q: bool = True,
    skeletons: dict[str, dict[str, Any]] | None = None,
) -> Path | None:
    """为单个主题生成增强聚合 retro 文档 (frontmatter + 失败根因画像 + 五问骨架)."""
    runs = bucket["runs"]
    failures = bucket["failures"]
    failure_bd = _failure_breakdown(bucket)
    frontmatter = _counts_frontmatter(topic, bucket, failure_bd)
    body = (
        f"# {topic} 运行复盘聚合 (resident 事件驱动)\n\n"
        f"- generated_at: {_utc()}\n"
        f"- status: candidate (sediment 草稿聚合, 待运营 agent/人工完善为完整 retro)\n"
        f"- sediment 覆盖: {len(runs)} 成功运行 + {len(failures)} 失败模式 = {bucket['total']} 草稿\n"
        f"- 失败率: {len(failures) / max(1, bucket['total']):.2%}\n\n"
        f"## 成功运行 (runs/)\n\n"
    )
    if runs:
        body += "\n".join(f"- {name}" for name in runs) + "\n"
    else:
        body += "- (无)\n"
    body += "\n## 失败模式 (failures/)\n\n"
    if failures:
        body += "\n".join(f"- {name}" for name in failures) + "\n"
    else:
        body += "- (无)\n"
    body += "\n## 失败根因画像 (确定性启发式)\n\n"
    body += _render_failure_breakdown(failure_bd)
    five_q_filled_here = 0
    if fill_five_q and skeletons:
        five_q, five_q_filled_here = _render_deterministic_five_q(bucket, skeletons)
        if five_q_filled_here:
            body += "\n## 确定性五问骨架 (ledger 追溯, 自动填充)\n\n"
            body += five_q
            body += "\n> 上节为事件流确定性提取 (计划/实际/结果/失败/指标); 语义项见下待人工完善。\n"
    body += "\n## 待完善(运营 agent/人工)\n\n"
    if five_q_filled_here:
        body += "- [ ] 关键发现\n- [ ] 净增减\n- [ ] 交接建议\n"
    else:
        body += "- [ ] 计划 vs 实际\n- [ ] 结果与证据\n- [ ] 关键发现\n- [ ] 净增减\n- [ ] 交接建议\n"
    if dry_run:
        return None
    target = write_path(RETRO_ROOT) / f"{topic}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(frontmatter + body, encoding="utf-8")
    return target


def _render_failure_breakdown(failure_bd: dict[str, Any]) -> str:
    """渲染失败根因画像正文 (by_event_type 分布 + 溯源 trace 列表)."""
    by_event_type = failure_bd["by_event_type"]
    if not by_event_type:
        return "- (无失败模式沉淀)\n"
    rows = "\n".join(f"- {et}: {n} 篇" for et, n in by_event_type.items())
    trace_line = (
        f"- 关联工作流溯源: {failure_bd['trace_count']} 个 (trace_id 见下)\n"
        + "\n".join(f"  - `{t}`" for t in failure_bd["trace_ids"])
        if failure_bd["trace_ids"]
        else "- 关联工作流溯源: 0 个 (草稿缺 workflow_run_id 元数据)\n"
    )
    return rows + "\n" + trace_line + "\n"


def _topic_run_ids(bucket: dict[str, Any]) -> list[str]:
    """从 bucket 草稿 frontmatter 的 workflow_run_id 收集去重 run 列表."""
    seen: list[str] = []
    for kind in ("runs_meta", "failures_meta"):
        for item in bucket.get(kind, []):
            rid = str(item.get("meta", {}).get("workflow_run_id") or "")
            if rid and rid not in seen:
                seen.append(rid)
    return seen


def _render_deterministic_five_q(bucket: dict[str, Any], skeletons: dict[str, dict[str, Any]]) -> tuple[str, int]:
    """渲染确定性五问骨架段 (ledger 追溯); 返回 (markdown, 关联到骨架的 run 数).

    只渲染能在 events.jsonl 中定位到完整事件序列的 run; 语义项 (关键发现/交接建议)
    不在此渲染, 保持人工兜底。
    """
    lines: list[str] = []
    filled = 0
    for run_id in _topic_run_ids(bucket):
        sk = skeletons.get(run_id)
        if sk is None:
            continue
        filled += 1
        lines.append(f"- **{sk['run_id']}**")
        if sk["objective"]:
            lines.append(f"  - 计划 (objective): {sk['objective']}")
        if sk["workflow_id"]:
            lines.append(f"  - workflow: {sk['workflow_id']}")
        if sk["steps"]:
            lines.append(f"  - 实际步骤: {', '.join(sk['steps'])}")
        if sk["outcome"]:
            oc = sk["outcome"]
            lines.append(
                "  - 结果与证据: "
                f"ok={oc.get('ok')}, status={oc.get('status')}, "
                f"evidence_count={oc.get('evidence_count')}"
            )
        if sk["failure"]:
            fa = sk["failure"]
            lines.append(f"  - 失败根因: step={fa.get('step_name')}, error={fa.get('error')}")
        m = sk["metrics"]
        lines.append(f"  - 指标: event_count={m.get('event_count')}, duration_s={m.get('duration_s')}")
    if not lines:
        return "- (无 ledger 可确定骨架)\n", 0
    return "\n".join(lines) + "\n", filled


def _global_breakdown(topics: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """全局失败画像: 跨主题失败率 + top 失败 event_type."""
    total_runs = sum(len(b["runs"]) for b in topics.values())
    total_failures = sum(len(b["failures"]) for b in topics.values())
    global_by_event: dict[str, int] = {}
    for b in topics.values():
        for et, n in _failure_breakdown(b)["by_event_type"].items():
            global_by_event[et] = global_by_event.get(et, 0) + n
    return {
        "total_runs": total_runs,
        "total_failures": total_failures,
        "failure_rate": round(total_failures / max(1, total_runs + total_failures), 4),
        "top_failure_event_types": dict(sorted(global_by_event.items(), key=lambda kv: (-kv[1], kv[0]))),
    }


def _stale_draft_paths(retain_days: int) -> list[Path]:
    """返回 mtime 超过保留窗口 (retain_days) 的 sediment 草稿路径 (runs+failures).

    保留窗口从当前时间回退; 超窗草稿视为已被 promote 聚合消化, 可归档防无限堆积。
    """
    cutoff = time.time() - max(0, retain_days) * 86400
    stale: list[Path] = []
    for kind in ("runs", "failures"):
        kind_dir = write_path(SEDIMENT_ROOT) / kind
        if not kind_dir.is_dir():
            continue
        for path in kind_dir.glob("*.md"):
            try:
                if path.stat().st_mtime < cutoff:
                    stale.append(path)
            except OSError:
                continue
    return sorted(stale, key=lambda p: p.name)


def _archive_consumed_drafts(retain_days: int) -> int:
    """把超窗草稿移入 gitignored `.omo/_knowledge/sediment-archive/<kind>/`; 返回归档数.

    移动而非删除 (可恢复); 目标重名时加时间戳后缀避免覆盖。
    """
    archived = 0
    for path in _stale_draft_paths(retain_days):
        target_dir = write_path(ARCHIVE_ROOT) / path.parent.name
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / path.name
        if target.exists():
            target = target_dir / f"{path.stem}-{int(time.time())}{path.suffix}"
        try:
            path.rename(target)
            archived += 1
        except OSError:
            continue
    return archived


def _write_index(
    topics: dict[str, dict[str, Any]],
    five_q_filled: int,
    generated_at: str,
) -> Path | None:
    """生成 `retros/resident/index.md` (主题/草稿数/失败率/五问/生成时间), 供检索消费."""
    rows: list[str] = []
    total_drafts = 0
    for topic, bucket in sorted(topics.items(), key=lambda kv: (-kv[1]["total"], kv[0])):
        runs = len(bucket["runs"])
        failures = len(bucket["failures"])
        total = bucket["total"]
        total_drafts += total
        rate = round(failures / max(1, total), 4)
        rows.append(f"| {topic} | {total} | {runs} | {failures} | {rate} | {generated_at} |")
    content = (
        "# resident retro 索引 (promote 自动生成)\n\n"
        f"- generated_at: {generated_at}\n"
        f"- 主题数: {len(topics)} · 草稿总数: {total_drafts} · five_q_filled: {five_q_filled}\n\n"
        "| 主题 | 草稿数 | runs | failures | failure_rate | 生成时间 |\n"
        "|------|-------|------|----------|-------------|----------|\n" + "\n".join(rows) + "\n"
    )
    target = write_path(RETRO_ROOT) / "index.md"
    try:
        target.write_text(content, encoding="utf-8")
    except OSError:
        return None
    return target


def promote(
    *,
    dry_run: bool = False,
    limit: int | None = None,
    fill_five_q: bool = True,
    events_path: str | Path | None = None,
    retain_days: int = 30,
) -> dict[str, Any]:
    """聚合 sediment 草稿 → 主题 retro 文档; 返回统计报告 (含失败画像 + 五问骨架填充).

    retain_days>0 时: 落盘后把超过保留窗口的已聚合草稿移入 gitignored 归档区 (防无限堆积),
    并生成 `retros/resident/index.md` 索引 (dry-run 不落盘, 但报告含 archivable_count)。
    """
    topics = _aggregate()
    ordered = sorted(topics.items(), key=lambda kv: kv[1]["total"], reverse=True)
    if limit:
        ordered = ordered[:limit]
    skeletons: dict[str, dict[str, Any]] = {}
    if fill_five_q:
        try:
            path = Path(events_path) if events_path else write_path(EVENTS_PATH)
            skeletons = ledger_trace.load_run_skeletons(path)
        except OSError:
            skeletons = {}  # 事件流缺失时不阻断 promote, 退化为无骨架模式
    promoted = 0
    five_q_filled = 0
    written_topics: list[str] = []
    for topic, bucket in ordered:
        if _write_retro(topic, bucket, dry_run=dry_run, fill_five_q=fill_five_q, skeletons=skeletons) is not None:
            promoted += 1
            written_topics.append(topic)
        if fill_five_q and _render_deterministic_five_q(bucket, skeletons)[1] > 0:
            five_q_filled += 1
    total_drafts = sum(b["total"] for b in topics.values())
    archivable = len(_stale_draft_paths(retain_days))
    archived = 0
    index_written: Path | None = None
    if not dry_run:
        if retain_days > 0:
            archived = _archive_consumed_drafts(retain_days)
        index_written = _write_index(topics, five_q_filled, _utc())
    return {
        "drafts_scanned": total_drafts,
        "topics": len(topics),
        "promoted_topics": promoted,
        "five_q_filled": five_q_filled,
        "archivable_count": archivable,
        "archived_count": archived,
        "index_written": str(index_written) if index_written else None,
        "written_to": str(write_path(RETRO_ROOT)) if not dry_run else None,
        "topics_detail": {t: b["total"] for t, b in ordered},
        "coverage_ratio": round(min(1.0, len(topics) / max(1, total_drafts)), 4),
        "failure_breakdown": _global_breakdown(topics),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="只统计不落盘")
    parser.add_argument("--limit", type=int, help="只提升前 N 个主题")
    parser.add_argument("--json", action="store_true", help="输出 JSON 报告")
    parser.add_argument(
        "--fill-five-q",
        action="store_true",
        default=True,
        dest="fill_five_q",
        help="用 ledger 追溯填充确定性五问骨架 (默认开启)",
    )
    parser.add_argument(
        "--no-fill-five-q",
        action="store_false",
        dest="fill_five_q",
        help="不填充确定性五问骨架",
    )
    parser.add_argument(
        "--events-path",
        type=Path,
        default=None,
        help="events.jsonl 路径 (默认 worktree .omo/_knowledge/workflow-mesh/events.jsonl)",
    )
    parser.add_argument(
        "--retain-days",
        type=int,
        default=30,
        help="草稿保留窗口天数 (默认 30): 落盘时把超过该窗口的已聚合草稿移入 gitignored 归档区",
    )
    args = parser.parse_args(argv)
    report = promote(
        dry_run=args.dry_run,
        limit=args.limit,
        fill_five_q=args.fill_five_q,
        events_path=args.events_path,
        retain_days=args.retain_days,
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        fb = report["failure_breakdown"]
        print(
            f"promote: {report['drafts_scanned']} 草稿 → {report['topics']} 主题, "
            f"提升 {report['promoted_topics']} 篇 retro (coverage {report['coverage_ratio']}, "
            f"失败率 {fb['failure_rate']}, five_q_filled {report['five_q_filled']}, "
            f"archivable {report['archivable_count']}, archived {report['archived_count']})"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
