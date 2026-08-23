#!/usr/bin/env python3

"""resident-promote — sediment 草稿 → 正式知识提升 (M4.1 阶段2).

扫描 `.omo/_knowledge/sediment/` 下的事件驱动草稿 (runs/ 复盘 + failures/
失败模式), 按 workflow 主题聚合, 生成结构化 retro 候选文档到
`.omo/_knowledge/retros/resident/`。完成"知识沉淀→能力"三阶段成长的
阶段2: 草稿从不可检索的散件 → 可检索/可统计的主题知识。

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

from omo.resident import WORKSPACE

SEDIMENT_ROOT = WORKSPACE / ".omo" / "_knowledge" / "sediment"
RETRO_ROOT = WORKSPACE / ".omo" / "_knowledge" / "retros" / "resident"
# 文件名: {ts}Z-{workflow-type}-{run_id}[(-{event_id})].md
TOPIC_PATTERN = re.compile(r"^\d{8}T\d{6}Z-(.+?)(?:-[a-f0-9]{6,})?\.md$")


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


def _scan(kind_dir: Path) -> list[tuple[str, str, Path]]:
    """返回 [(topic, filename, path)] 列表."""
    out: list[tuple[str, str, Path]] = []
    if not kind_dir.is_dir():
        return out
    for path in sorted(kind_dir.glob("*.md")):
        out.append((_extract_topic(path.name), path.name, path))
    return out


def _aggregate() -> dict[str, dict[str, Any]]:
    """按主题聚合 runs/failures 草稿."""
    topics: dict[str, dict[str, Any]] = {}
    for kind in ("runs", "failures"):
        entries = _scan(SEDIMENT_ROOT / kind)
        for topic, filename, path in entries:
            bucket = topics.setdefault(topic, {"runs": [], "failures": [], "total": 0})
            bucket[kind].append(filename)
            bucket["total"] += 1
    return topics


def _write_retro(topic: str, bucket: dict[str, Any], dry_run: bool) -> Path | None:
    """为单个主题生成聚合 retro 文档."""
    runs = bucket["runs"]
    failures = bucket["failures"]
    body = (
        f"# {topic} 运行复盘聚合 (resident 事件驱动)\n\n"
        f"- generated_at: {_utc()}\n"
        f"- status: candidate (sediment 草稿聚合, 待运营 agent/人工完善为完整 retro)\n"
        f"- sediment 覆盖: {len(runs)} 成功运行 + {len(failures)} 失败模式 = {bucket['total']} 草稿\n\n"
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
    body += "\n## 待完善(运营 agent/人工)\n\n- [ ] 计划 vs 实际\n- [ ] 结果与证据\n- [ ] 关键发现\n- [ ] 净增减\n- [ ] 交接建议\n"
    if dry_run:
        return None
    target = RETRO_ROOT / f"{topic}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return target


def promote(*, dry_run: bool = False, limit: int | None = None) -> dict[str, Any]:
    """聚合 sediment 草稿 → 主题 retro 文档; 返回统计报告."""
    topics = _aggregate()
    ordered = sorted(topics.items(), key=lambda kv: kv[1]["total"], reverse=True)
    if limit:
        ordered = ordered[:limit]
    promoted = 0
    written_topics: list[str] = []
    for topic, bucket in ordered:
        if _write_retro(topic, bucket, dry_run=dry_run) is not None:
            promoted += 1
            written_topics.append(topic)
    total_drafts = sum(b["total"] for b in topics.values())
    return {
        "drafts_scanned": total_drafts,
        "topics": len(topics),
        "promoted_topics": promoted,
        "written_to": str(RETRO_ROOT) if not dry_run else None,
        "topics_detail": {t: b["total"] for t, b in ordered},
        "coverage_ratio": round(min(1.0, len(topics) / max(1, total_drafts)), 4),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="只统计不落盘")
    parser.add_argument("--limit", type=int, help="只提升前 N 个主题")
    parser.add_argument("--json", action="store_true", help="输出 JSON 报告")
    args = parser.parse_args(argv)
    report = promote(dry_run=args.dry_run, limit=args.limit)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(
            f"promote: {report['drafts_scanned']} 草稿 → {report['topics']} 主题, "
            f"提升 {report['promoted_topics']} 篇 retro (coverage {report['coverage_ratio']})"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
