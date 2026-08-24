#!/usr/bin/env python3
"""Executor — Agent Cell 执行者. 按计划执行 → 工具调用 → 产出收集."""

from __future__ import annotations
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


class Executor:
    def __init__(self, backend: str = "local"):
        self.backend = backend
        self.execution_log = []

    def execute_plan(self, plan: dict) -> dict:
        results = [self.execute_task(t) for t in plan.get("tasks", [])]
        result = {"schema": "execution-result/v1", "execution_id": f"exec-{uuid.uuid4().hex[:12]}", "plan_id": plan.get("plan_id", ""), "results": results, "completed": all(r.get("ok") for r in results), "completed_at": datetime.now(timezone.utc).isoformat()}
        self.execution_log.append(result)
        return result

    def execute_task(self, task: dict) -> dict:
        action = task.get("action", "")
        target = task.get("target", "")
        try:
            if self.backend == "local":
                return self._execute_local(action, target)
            return {"ok": False, "error": f"Unsupported backend: {self.backend}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _execute_local(self, action: str, target: str) -> dict:
        read_only = {"read_file", "list_files", "search", "query_status", "get_info", "scan", "check", "validate"}
        if action not in read_only:
            return {"ok": False, "error": f"Action '{action}' not allowed in local mode"}
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
            results = [str(f.relative_to(ROOT)) for f in docs_dir.rglob("*.md") if target.lower() in f.read_text(encoding="utf-8", errors="ignore").lower()] if docs_dir.exists() else []
            return {"ok": True, "output": results[:20], "count": len(results)}
        if action == "query_status":
            return {"ok": True, "output": "System operational", "status": "healthy"}
        return {"ok": False, "error": f"Unsupported local action: {action}"}


if __name__ == "__main__":
    import argparse, json
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
