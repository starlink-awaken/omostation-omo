#!/usr/bin/env python3
"""omo.resident.night_patrol — MBP 128G 统一内存电源感知夜间巡检引擎 (Phase 3 Sprint 3).

在接通 AC 电源且夜间系统闲置时，调动 MBP 128GB 本地重算力 (LM Studio :1234 Qwen-35B / MLAI3)
通宵走查全仓近 24 小时 Git 提交、隐蔽死锁、断言漏洞与架构漂移，自动起草 ADR/BET 提案。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import time
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from omo.resident import WORKSPACE, write_path

logger = logging.getLogger("omo.resident.night_patrol")

REPORTS_DIR = Path.home() / ".local/share/omostation/patrol"
PROPOSALS_DIR = WORKSPACE / ".omo/_knowledge/decision-proposals"
CARDS_DIR = Path.home() / ".local/share/omostation/cards"


def check_power_and_idle(force: bool = False) -> tuple[bool, str]:
    """电源感知: 必须处于 AC 电源接入状态，严禁电池续航时进行重模型推理."""
    if force:
        return True, "手动强制触发 (忽略电源与时间限制)"

    try:
        res = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=3)
        output = res.stdout.lower()
        # 严谨判定: 'battery power' 或 'discharging' 说明当前由电池供电
        is_battery = "battery power" in output or "discharging" in output
        has_ac = "ac power" in output or (" charging" in output and "discharging" not in output)
        if is_battery or not has_ac:
            return False, "当前由电池供电，自动跳过夜间重算力巡检以保电护温"
    except Exception as exc:
        logger.warning("Failed to check power status: %s", exc)

    # 检查时间窗口 (默认 02:00 ~ 06:00)
    current_hour = datetime.now().hour
    if not (2 <= current_hour <= 6):
        return False, f"当前时间 ({current_hour}:00) 不在夜间闲置窗口 (02:00 ~ 06:00)，请使用 --force 触发"

    return True, "AC 电源接入且处于夜间闲置窗口"


def collect_recent_commits(hours: int = 24) -> list[dict[str, str]]:
    """获取全仓最近 N 小时内的 Git Commit 清单."""
    try:
        res = subprocess.run(
            ["git", "log", f"--since={hours} hours ago", "--pretty=format:%h|%an|%s|%ad", "--date=short"],
            cwd=str(WORKSPACE),
            capture_output=True,
            text=True,
            timeout=5,
        )
        commits = []
        if res.returncode == 0 and res.stdout.strip():
            for line in res.stdout.strip().splitlines():
                parts = line.split("|", 3)
                if len(parts) == 4:
                    commits.append(
                        {
                            "hash": parts[0],
                            "author": parts[1],
                            "subject": parts[2],
                            "date": parts[3],
                        }
                    )
        return commits
    except Exception as exc:
        logger.warning("Failed to collect commits: %s", exc)
        return []


def run_code_review_with_heavy_model(commits: list[dict[str, str]], prompt_extra: str = "") -> str:
    """使用 MBP 128G 本地重模型 (LM Studio :1234 / AetherForge :4000) 执行代码复盘."""
    from omo.resident.interact import _invoke_llm_endpoint, _probe, _probe_lm_studio_active_model

    if not commits:
        return "近 24 小时无代码提交，无需代码审查。"

    commits_summary = "\n".join(f"- [{c['hash']}] {c['author']}: {c['subject']}" for c in commits[:15])
    review_prompt = (
        "你是夏明星主权操作系统的首席代码审查员。请对以下全仓最近 24 小时的代码提交执行夜间深度复盘：\n"
        f"{commits_summary}\n\n"
        f"{prompt_extra}\n"
        "审查准则：\n"
        "1. 是否存在绕过隔离 worktree 直接提交、破坏主工作区物理只读原则的痕迹？\n"
        "2. 是否存在未解决的 TODO/FIXME、破坏性硬编码绝对路径或类型断言漏洞？\n"
        "3. 是否有重复散落脚本、违背架构分层的坏味道？\n"
        "输出格式：分条陈述，严谨精炼，若一切健康直接输出「✅ 全域代码健康，未发现架构隐患与合规违规」。"
    )

    messages = [{"role": "user", "content": review_prompt}]

    # 优先路由到本地 LM Studio (:1234) 真实驻留的重模型 (如 Qwen 27B/35B)
    if _probe(1234):
        active_model = _probe_lm_studio_active_model(1234) or "swift-1.5-qwen3.8-27b-splash"
        raw = _invoke_llm_endpoint(1234, active_model, messages, timeout=60, stream=False)
        if raw:
            return raw

    # 回退到本地 AetherForge (:4000) 深度档
    if _probe(4000):
        raw = _invoke_llm_endpoint(4000, "deep", messages, timeout=40, stream=False)
        if raw:
            return raw

    return "⚠️ 本地重算力端口 (1234/4000) 离线，跳过深度大模型分析。"


def check_architecture_drift() -> dict[str, Any]:
    """走查架构漂移、游离脚本与 SSOT 一致性."""
    drift_items = []

    # 1. 检查根目录临时文件
    for p in WORKSPACE.glob("tmp*"):
        drift_items.append(f"发现根目录临时残留: {p.name}")
    for p in WORKSPACE.glob("test*.py"):
        drift_items.append(f"发现根目录散落测试脚本: {p.name}")

    # 2. 检查 .omo/_delivery 积压
    delivery_dir = WORKSPACE / ".omo/_delivery"
    if delivery_dir.exists():
        old_claims = list(delivery_dir.glob("branch-claims/*.json"))
        if len(old_claims) > 10:
            drift_items.append(f"分支认领台账较多 ({len(old_claims)}条)，建议适时归档")

    # 3. 检查并清理孤儿隔离工作树 (Q6 裁定)
    try:
        from omo.resident.worker import gc_orphan_worktrees

        cleaned_wts = gc_orphan_worktrees(max_age_hours=24, dry_run=False)
        if cleaned_wts:
            drift_items.append(
                f"清理超时孤儿工作树 {len(cleaned_wts)} 个: {', '.join(Path(w).name for w in cleaned_wts[:3])}"
            )
    except Exception as exc:
        logger.debug("Failed running worktree GC in patrol: %s", exc)

    return {
        "drift_detected": len(drift_items) > 0,
        "items": drift_items,
    }


def draft_decision_proposal(title: str, rationale: str, action: str) -> Path | None:
    """若发现系统级架构坏味道，自动在 decision-proposals 下起草提案草稿."""
    try:
        p_dir = write_path(PROPOSALS_DIR)
        p_dir.mkdir(parents=True, exist_ok=True)
        now_ts = int(time.time())
        filename = f"PROPOSAL-NIGHT-PATROL-{now_ts}.md"
        path = p_dir / filename
        body = (
            f"# 架构演进提案: {title}\n\n"
            f"- **起草者**: MBP 128G 夜间巡检 Taskforce (Qwen-35B)\n"
            f"- **日期**: {datetime.now(UTC).isoformat()[:10]}\n"
            f"- **状态**: PROPOSED\n"
            f"- **影响范围**: 全系统治理层\n\n"
            f"## 一、 发现的架构痛点与根因\n"
            f"{rationale}\n\n"
            f"## 二、 建议采取的演进自愈行动\n"
            f"{action}\n\n"
            f"## 三、 门禁与验收标准\n"
            f"- 运行 `make gac-local-gate` 保持 100% PASS。\n"
        )
        path.write_text(body, encoding="utf-8")
        return path
    except Exception as exc:
        logger.warning("Failed to draft proposal: %s", exc)
        return None


def run_night_patrol(force: bool = False, deep: bool = True) -> dict[str, Any]:
    """主巡检流程: 电源感知 -> 提交走查 -> 重模型审查 -> 漂移审计 -> 提案起草 -> 报告生成."""
    t0 = time.time()
    power_ok, power_msg = check_power_and_idle(force=force)
    if not power_ok:
        return {
            "ok": False,
            "skipped": True,
            "reason": power_msg,
        }

    # 1. 收集提交
    commits = collect_recent_commits(hours=24)

    # 2. 本地 128G 重模型审查
    review_output = ""
    if deep:
        review_output = run_code_review_with_heavy_model(commits)

    # 3. 架构漂移审计
    drift_report = check_architecture_drift()

    # 4. 自动起草演进提案 (若有漂移)
    drafted_proposal = None
    if drift_report["drift_detected"]:
        drafted_proposal = draft_decision_proposal(
            title="夜间巡检架构漂移自愈",
            rationale="\n".join(f"- {it}" for it in drift_report["items"]),
            action="运行 worktree 整理与孤立脚本归档流程。",
        )

    duration = round(time.time() - t0, 2)
    report = {
        "ok": True,
        "timestamp": datetime.now(UTC).isoformat(),
        "duration_seconds": duration,
        "power_status": power_msg,
        "commits_reviewed": len(commits),
        "review_summary": review_output,
        "drift_audit": drift_report,
        "drafted_proposal": str(drafted_proposal) if drafted_proposal else None,
    }

    # 持久化报告
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_file = REPORTS_DIR / "night-patrol-latest.json"
        report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    # 若有重大发现推入 SwiftBar Action Desk
    if drafted_proposal:
        try:
            CARDS_DIR.mkdir(parents=True, exist_ok=True)
            card_id = f"night-patrol-{int(t0)}"
            card_file = CARDS_DIR / f"{card_id}.json"
            card_data = {
                "id": card_id,
                "type": "night_patrol_finding",
                "title": f"夜间巡检简报: 发现 {len(drift_report['items'])} 项架构漂移",
                "category": "patrol",
                "summary": f"已自动起草演进提案: {drafted_proposal.name}",
                "details": f"Commit 审查: {len(commits)} 篇\n{review_output[:200]}",
                "status": "pending_approval",
                "created_at": datetime.now(UTC).isoformat(),
                "created_by": "mbp-night-patrol",
            }
            card_file.write_text(json.dumps(card_data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MBP 128G 电源感知夜间重模型深度巡检 CLI")
    parser.add_argument("--force", "-f", action="store_true", help="强制即时触发 (忽略电源与夜间窗口限制)")
    parser.add_argument("--no-deep", action="store_true", help="仅执行静态规则扫描，不调动重模型推理")
    parser.add_argument("--json", action="store_true", help="以 JSON 格式输出巡检报告")

    args = parser.parse_args(argv)
    res = run_night_patrol(force=args.force, deep=not args.no_deep)

    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        if not res.get("ok"):
            print(f"\n\033[33m⏸️  夜间巡检未触发: {res.get('reason')}\033[0m\n")
            return 0

        print("\n\033[1;32m🌅 MBP 128G 夜间深度巡检完成\033[0m")
        print(f"⏱️ 耗时: {res['duration_seconds']}s · 状态: {res['power_status']}")
        print(f"📊 走查 Commits: {res['commits_reviewed']} 项")
        print("\n\033[1;34m[重模型审查意见]\033[0m")
        print(res["review_summary"])
        if res["drafted_proposal"]:
            print(f"\n💡 \033[32m已自动起草演进提案: {res['drafted_proposal']}\033[0m")
            print("\033[2m   (已推入 SwiftBar Action Desk 待夏明星晨间审阅)\033[0m")
        print()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
