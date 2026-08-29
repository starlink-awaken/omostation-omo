#!/usr/bin/env python3
"""Executor — Agent Cell 执行者. 按计划执行 → 工具调用 → 产出收集."""

from __future__ import annotations

import subprocess
import sys
import uuid
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


class Executor:
    def __init__(self, backend: str = "local", omo_dir: Path | None = None):
        self.backend = backend
        self.execution_log = []
        self.omo_dir = omo_dir or ROOT / ".omo"

    def execute_plan(self, plan: dict) -> dict:
        results = [self.execute_task(t) for t in plan.get("tasks", [])]
        result = {
            "schema": "execution-result/v1",
            "execution_id": f"exec-{uuid.uuid4().hex[:12]}",
            "plan_id": plan.get("plan_id", ""),
            "results": results,
            "completed": all(r.get("ok") for r in results),
            "completed_at": datetime.now(UTC).isoformat(),
        }
        self.execution_log.append(result)
        return result

    # BET-Y1Q3-T4-05 (WP2): fixed-success effectful actions — 需要 admitted
    # workflow context 才能执行; 无 context 一律 not_executed (spec §4)。
    EFFECTFUL_ACTIONS = frozenset({"generate_doc", "create_draft", "format_code", "run_tests", "backup", "snapshot"})

    def execute_task(self, task: dict) -> dict:
        action = task.get("action", "")
        target = task.get("target", "")
        context = task.get("admitted_context")
        if action in self.EFFECTFUL_ACTIONS:
            # spec §4: 无 admitted workflow context 的 effectful action 拒绝, 零副作用
            if not isinstance(context, dict) or not context:
                return {
                    "ok": False,
                    "effect": "not_executed",
                    "error": f"admitted workflow context required for effectful action: {action}",
                }
            return self._execute_effectful(action, target, context)
        try:
            if self.backend == "local":
                return self._execute_local(action, target)
            if self.backend == "pi-worker":
                return self._execute_pi_worker(action, target)
            if self.backend == "multica":
                return self._execute_multica(action, target)
            return {"ok": False, "error": f"Unsupported backend: {self.backend}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _execute_effectful(self, action: str, target: str, context: dict) -> dict:
        """Receipt-backed effectful 执行 — 消费已准入 context, 经 sandbox tool 产生
        durable receipt (幂等重放内建)。digest_ref 语义: 绑定 action+target 的
        canonical digest 作为执行证据。"""
        import hashlib
        import json as _json

        from omo.sandbox_tool_runner import SandboxToolError, run_sandbox_tool

        input_payload = _json.dumps({"action": action, "target": target}, sort_keys=True)
        input_digest = hashlib.sha256(input_payload.encode("utf-8")).hexdigest()
        try:
            result = run_sandbox_tool(
                self.omo_dir,
                workflow_run_id=context["workflow_run_id"],
                trace_id=context["trace_id"],
                dispatch_id=context["dispatch_id"],
                worker_id=context["worker_id"],
                step_run_id=context["step_run_id"],
                admission_id=context["admission_id"],
                input_ref=f"artifact://resident-effect/{action}/{target}",
                input_digest=input_digest,
                now=context.get("now"),
            )
        except SandboxToolError as exc:
            return {"ok": False, "effect": "not_executed", "error": f"sandbox_tool_rejected: {exc}"}
        # 幂等: 重放复用同一 invocation (sandbox runner 内建), replay 不产生第二次效果
        receipt_digest = result.get("invocation_id") or result.get("output_digest") or ""
        replayed = result.get("status") == "replayed"
        return {
            "ok": result.get("status") in {"executed", "replayed"},
            "effect": "executed",
            "replayed": replayed,
            "action": action,
            "target": target,
            "activation": result.get("activation"),
            "receipt_digest": receipt_digest,
            "result": result,
        }

    def _execute_local(self, action: str, target: str) -> dict:
        # BET-Y1Q3-T4-05: local backend 只保留真实只读操作;
        # effectful fixed-success 分支已移除 (WP2 spec §2/§3)。
        read_only = {"read_file", "list_files", "search", "query_status", "get_info", "scan", "check", "validate"}
        if action not in read_only:
            return {
                "ok": False,
                "effect": "not_executed",
                "error": f"Action '{action}' requires admitted workflow context (effectful not allowed in local mode)",
            }
        if action in ("scan", "list_files"):
            p = ROOT / target if not Path(target).is_absolute() else Path(target)
            if p.exists():
                files = list(p.rglob("*")) if p.is_dir() else [p]
                return {"ok": True, "output": [str(f.relative_to(ROOT)) for f in files[:50]], "count": len(files)}
            return {"ok": False, "error": f"Path not found: {target}"}
        if action == "read_file":
            p = ROOT / target if not Path(target).is_absolute() else Path(target)
            if p.exists():
                content = p.read_text(encoding="utf-8", errors="ignore")[:5000]
                return {"ok": True, "output": content, "size": len(content)}
            return {"ok": False, "error": f"File not found: {target}"}
        if action == "search":
            docs_dir = ROOT / "docs"
            results = (
                [
                    str(f.relative_to(ROOT))
                    for f in docs_dir.rglob("*.md")
                    if target.lower() in f.read_text(encoding="utf-8", errors="ignore").lower()
                ]
                if docs_dir.exists()
                else []
            )
            return {"ok": True, "output": results[:20], "count": len(results)}
        if action == "query_status":
            return {"ok": True, "output": "System operational", "status": "healthy"}
        return {"ok": False, "error": f"Unsupported local action: {action}"}

    def _execute_pi_worker(self, action: str, target: str) -> dict:
        """pi-worker 后端 — 执行 R2 可逆操作 (commit_code, create_pr, modify_config)."""
        r2_actions = {"commit_code", "create_pr", "modify_config", "deploy_staging"}
        if action not in r2_actions:
            return {"ok": False, "error": f"Action '{action}' not allowed in pi-worker mode"}
        # 通过 resident execute 角色执行
        try:
            result = subprocess.run(
                ["python3", str(ROOT / "bin" / "ssot" / "pi-worker.py"), "--action", action, "--target", target],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if result.returncode == 0:
                return {"ok": True, "output": result.stdout.strip(), "backend": "pi-worker"}
            return {"ok": False, "error": result.stderr.strip() or "pi-worker execution failed"}
        except (subprocess.TimeoutExpired, OSError) as e:
            return {"ok": False, "error": f"pi-worker error: {e}"}

    def _execute_multica(self, action: str, target: str) -> dict:
        """multica 后端 — 执行 R3 高危操作 (deploy_production, delete_data, push_main)."""
        r3_actions = {"deploy_production", "delete_data", "modify_permissions", "push_main"}
        if action not in r3_actions:
            return {"ok": False, "error": f"Action '{action}' not allowed in multica mode"}
        # R3 需要同步确认
        return {"ok": False, "error": "R3 actions require sync confirmation (use PDP/PEP first)"}


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("--plan")
    parser.add_argument("--task")
    parser.add_argument("--backend", default="local")
    args = parser.parse_args()
    e = Executor(backend=args.backend)
    if args.plan:
        r = e.execute_plan(json.loads(args.plan))
        print(json.dumps(r, ensure_ascii=False, indent=2))
    elif args.task:
        r = e.execute_task(json.loads(args.task))
        print(json.dumps(r, ensure_ascii=False, indent=2))
