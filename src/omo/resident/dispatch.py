#!/usr/bin/env python3
"""omo.resident.dispatch — Agora 蜂群任务派发与 Worktree 隔离编排器 (Phase 3 Sprint 1).

将夏明星或常驻 Agent 的高层架构指令解构为标准 TaskObject v1.0，
分配受管隔离 Worktree，并路由派发给 Agora 蜂群内的专业 Agent 执行。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from omo.omo_paths import code_root
from omo.resident import WORKSPACE, write_path
from omo.resident.pitfall_rag import extract_relevant_pitfalls

logger = logging.getLogger("omo.resident.dispatch")

CARDS_DIR = Path.home() / ".local/share/omostation/cards"
TASKS_DIR = WORKSPACE / ".omo/_truth/task-center/dispatches"
PROVIDERS_FILE = code_root() / ".omo/_truth/registry/capability-providers.yaml"


def get_available_agents() -> list[dict[str, Any]]:
    """从能力注册表中动态提取 Agora 蜂群中可用的专业 Agent 列表."""
    if not PROVIDERS_FILE.exists():
        return [
            {
                "id": "claude-code",
                "capabilities": ["code_edit", "document_draft", "shell", "mcp"],
                "strengths": ["complex_reasoning", "multi_domain"],
            },
            {
                "id": "codex",
                "capabilities": ["code_edit", "shell", "file_system"],
                "strengths": ["implementation", "debugging", "structured_output"],
            },
            {"id": "crush", "capabilities": ["code_edit", "shell"], "strengths": ["speed", "lightweight"]},
            {
                "id": "opencode",
                "capabilities": ["code_edit", "shell", "local_model"],
                "strengths": ["open_source", "local_model"],
            },
        ]
    try:
        import yaml

        data = yaml.safe_load(PROVIDERS_FILE.read_text(encoding="utf-8"))
        providers = data.get("providers", [])
        return [
            {
                "id": p.get("id"),
                "capabilities": p.get("capabilities", []),
                "strengths": p.get("strengths", []),
                "description": p.get("description", ""),
            }
            for p in providers
        ]
    except Exception as exc:
        logger.warning("Failed to parse capability providers: %s", exc)
        return []


def match_agent_for_objective(objective: str) -> str:
    """根据目标意图自动推荐最贴切的 Agora 专业 Agent."""
    obj_lower = objective.lower()
    if any(k in obj_lower for k in ("重构", "refactor", "设计", "架构", "多文件", "complex", "mcp")):
        return "claude-code"
    elif any(k in obj_lower for k in ("修复", "fix", "单测", "test", "实现", "implement", "bug")):
        return "codex"
    elif any(k in obj_lower for k in ("格式化", "lint", "文档", "doc", "typo", "清理")):
        return "crush"
    return "codex"


def create_task_envelope(
    objective: str,
    target_agent: str | None = None,
    risk_tier: str = "R1",
    initiator: str = "chief-resident-copilot",
    session_id: str | None = None,
    target_paths: list[str] | None = None,
) -> dict[str, Any]:
    """构造标准 TaskObject v1.0 任务封套，自动注入防御性避坑硬约束 (Pitfall RAG)."""
    now_ts = int(time.time())
    short_uid = uuid.uuid4().hex[:6]
    task_id = f"TSK-{now_ts}-{short_uid}"
    agent_id = target_agent or match_agent_for_objective(objective)
    safe_session = session_id or f"task-{short_uid}"
    safe_session = re.sub(r"[^a-z0-9-]", "-", safe_session.lower()).strip("-")

    # 逆向检索 LifeOS 权威记忆与 AGENTS.md 避坑防御硬约束
    pitfalls = extract_relevant_pitfalls(objective, target_paths=target_paths, max_rules=3)
    pitfall_rules = [p["rule"] for p in pitfalls]

    envelope = {
        "$schema": "https://ecos.internal/schemas/task-object-v1.json",
        "task_id": task_id,
        "created_at": datetime.now(UTC).isoformat(),
        "initiator": initiator,
        "target_agent": agent_id,
        "risk_tier": risk_tier,
        "session_id": safe_session,
        "objective": objective,
        "status": "pending_isolation",
        "execution_context": {
            "workspace_root": str(WORKSPACE),
            "session": safe_session,
            "branch": f"work/{safe_session}",
            "worktree_path": None,
            "target_paths": target_paths or [],
        },
        "constraints": {
            "main_workspace_read_only": True,
            "worktree_isolation_required": True,
            "signoff_required": True,
            "pitfall_rules": pitfall_rules,
            "pitfall_metadata": pitfalls,
        },
        "system_guidance": ("【执行守则与防御约束】\n" + "\n".join(f"- {r}" for r in pitfall_rules)),
        "evidence": {},
    }
    return envelope


def prepare_isolated_worktree(session_id: str, dry_run: bool = False) -> tuple[bool, str, str | None]:
    """调用 gac-worktree.sh claim 准备物理隔离的工作区."""
    if dry_run:
        est_path = str(WORKSPACE.parent / f"Workspace-isolation-{session_id}")
        return True, f"[dry-run] 拟创建隔离工作树: {est_path}", est_path

    script = WORKSPACE / "bin/gac/gac-worktree.sh"
    if not script.exists():
        return False, "找不到 bin/gac/gac-worktree.sh 脚本", None

    try:
        env = dict(os.environ)
        env["SKIP_SUBMODULE_INIT"] = "1"  # 快速路径，防网络卡顿
        res = subprocess.run(
            ["bash", str(script), "claim", session_id],
            cwd=str(WORKSPACE),
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        if res.returncode == 0:
            # 探测实际建立的路径 (通常在 WS_PARENT/ws-<session> 或通过 claim 读出)
            # 尝试通过 git worktree list 解析
            wt_res = subprocess.run(
                ["git", "worktree", "list", "--porcelain"],
                cwd=str(WORKSPACE),
                capture_output=True,
                text=True,
                timeout=5,
            )
            wt_path = None
            if wt_res.returncode == 0:
                for block in wt_res.stdout.split("\n\n"):
                    if f"work/{session_id}" in block or f"{session_id}" in block:
                        for line in block.splitlines():
                            if line.startswith("worktree "):
                                wt_path = line.split(" ", 1)[1]
                                break
            return True, "隔离 Worktree 建立成功", wt_path
        return False, f"claim 失败: {res.stderr.strip()}", None
    except Exception as exc:
        return False, f"创建隔离工作树异常: {exc}", None


def dispatch_task(
    objective: str,
    target_agent: str | None = None,
    risk_tier: str = "R1",
    auto_worktree: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """主调度入口: 意图解构 -> 任务包封装 -> Worktree 隔离准备 -> Action Desk 卡片注册."""
    envelope = create_task_envelope(
        objective=objective,
        target_agent=target_agent,
        risk_tier=risk_tier,
    )
    session_id = envelope["session_id"]
    task_id = envelope["task_id"]

    wt_ok, wt_msg, wt_path = False, "未要求自动建立 worktree", None
    if auto_worktree:
        wt_ok, wt_msg, wt_path = prepare_isolated_worktree(session_id, dry_run=dry_run)
        envelope["execution_context"]["worktree_path"] = wt_path
        envelope["status"] = "worktree_ready" if wt_ok else "worktree_failed"
        envelope["evidence"]["worktree_log"] = wt_msg

    # 1. 持久化 TaskObject
    try:
        write_path(TASKS_DIR).mkdir(parents=True, exist_ok=True)
        task_file = write_path(TASKS_DIR) / f"{task_id}.json"
        task_file.write_text(json.dumps(envelope, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to save TaskObject: %s", exc)

    # 2. 推入 Action Desk 待办卡片 (供 SwiftBar / Web 审核查看)
    try:
        CARDS_DIR.mkdir(parents=True, exist_ok=True)
        card_id = f"task-dispatch-{session_id}"
        card_file = CARDS_DIR / f"{card_id}.json"
        card_data = {
            "id": card_id,
            "type": "agora_task",
            "title": f"蜂群任务: {envelope['target_agent']} · {envelope['objective'][:30]}",
            "category": "task_dispatch",
            "summary": f"派发给 {envelope['target_agent']} (隔离分支: work/{session_id})",
            "details": f"任务目标: {envelope['objective']}\n工作区: {wt_path or '待分配'}\n状态: {envelope['status']}",
            "status": "pending_approval",
            "created_at": envelope["created_at"],
            "created_by": "chief-resident-copilot",
            "task_id": task_id,
            "session_id": session_id,
        }
        card_file.write_text(json.dumps(card_data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to create Action Desk card: %s", exc)

    return {
        "ok": True,
        "task_id": task_id,
        "session_id": session_id,
        "target_agent": envelope["target_agent"],
        "objective": objective,
        "worktree": {
            "ok": wt_ok,
            "path": wt_path,
            "message": wt_msg,
        },
        "card_registered": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Agora 蜂群任务派发与 Worktree 隔离编排 CLI")
    parser.add_argument("objective", nargs="?", default="", help="任务目标或工程指令")
    parser.add_argument("--agent", "-a", dest="agent", help="指定目标 Agent (如 claude-code / codex / crush)")
    parser.add_argument("--risk", "-r", default="R1", choices=["R0", "R1", "R2", "R3"], help="风险分级 (默认 R1)")
    parser.add_argument("--dry-run", action="store_true", help="演练模式，不实际创建 worktree")
    parser.add_argument("--no-worktree", action="store_true", help="不自动拉起隔离 worktree")
    parser.add_argument("--json", action="store_true", help="以 JSON 格式输出调度结果")

    args = parser.parse_args(argv)
    if not args.objective.strip():
        parser.print_help()
        return 1

    res = dispatch_task(
        objective=args.objective.strip(),
        target_agent=args.agent,
        risk_tier=args.risk,
        auto_worktree=not args.no_worktree,
        dry_run=args.dry_run,
    )

    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        print("\n\033[1;32m⚡ Agora 蜂群任务派发成功\033[0m")
        print(f"📋 任务 ID: \033[36m{res['task_id']}\033[0m")
        print(f"🤖 委派 Agent: \033[33m{res['target_agent']}\033[0m")
        print(f"🎯 任务目标: {res['objective']}")
        print(f"📦 物理隔离状态: {res['worktree']['message']}")
        if res["worktree"]["path"]:
            print(f"📂 隔离工作树: \033[2m{res['worktree']['path']}\033[0m")
        print("\033[2m💡 已推入 SwiftBar Action Desk，待夏明星一键审核闭环。\033[0m\n")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
