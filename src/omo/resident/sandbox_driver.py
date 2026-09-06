#!/usr/bin/env python3
"""sandbox_driver — Git Worktree 物理沙箱受控执行驱动 (BET-Y1Q4-T10-133).

为持久化 Agent 的高危或未知探索任务提供秒级建立的隔离 worktree 物理沙箱。
三阶段驱动:

- create(): 调用主仓 bin/gac/resident-sandbox-timemachine.sh create
- run(cmd):  在沙箱内执行命令, 收集 exit code + 输出
- outcome(): 判定 success/failure; failure → 自动时光机回滚自毁 (防主工作区污染)

与 bdsk-shadow-sandbox (静态扫描) 正交: 本驱动是物理执行沙箱, 不是静态分析。

挂接: resident execute 通路 (execute.py) 的高危 ExecutionRequested 事件可走沙箱执行。
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from omo.resident import WORKSPACE

SANDBOX_SCRIPT = WORKSPACE / "bin" / "gac" / "resident-sandbox-timemachine.sh"
SANDBOX_PARENT = WORKSPACE.parent


@dataclass
class SandboxOutcome:
    """沙箱执行结果. failure → 调用方应触发 rollback."""

    ok: bool
    exit_code: int
    output: str = ""
    rollback_ran: bool = False
    detail: dict[str, Any] = field(default_factory=dict)


class SandboxDriver:
    """驱动沙箱 create → run → outcome/rollback 三阶段."""

    def __init__(self, session: str, workspace: Path = WORKSPACE) -> None:
        self.session = session
        self.workspace = workspace
        self.script = SANDBOX_SCRIPT
        self.sandbox_path = SANDBOX_PARENT / f"ws-sandbox-{session}"

    # ── create ─────────────────────────────────────────────
    def create(self, from_commit: str = "HEAD") -> tuple[bool, str]:
        """秒级创建隔离沙箱 worktree. 返回 (ok, message)."""
        if not self.script.is_file():
            return False, f"❌ sandbox script missing: {self.script}"
        res = subprocess.run(
            [
                "bash",
                str(self.script),
                "create",
                self.session,
                from_commit,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return (res.returncode == 0, (res.stdout + res.stderr).strip())

    # ── run ────────────────────────────────────────────────
    def run(self, cmd: str) -> SandboxOutcome:
        """在沙箱内执行命令串. 沙箱内禁止 push/merge/rebase (脚本硬约束)."""
        res = subprocess.run(
            ["bash", str(self.script), "run", self.session, "--", cmd],
            capture_output=True,
            text=True,
            check=False,
        )
        return SandboxOutcome(
            ok=(res.returncode == 0),
            exit_code=res.returncode,
            output=(res.stdout + res.stderr).strip(),
        )

    # ── outcome ────────────────────────────────────────────
    def outcome(self, run_result: SandboxOutcome) -> SandboxOutcome:
        """判定结果; failure → 自动时光机回滚自毁."""
        if run_result.ok:
            run_result.detail["rollback"] = "not_needed"
            return run_result
        # failure → 硬性回滚 (防主工作区污染)
        rollback_ok, msg = self.rollback()
        run_result.rollback_ran = rollback_ok
        run_result.detail["rollback"] = "ran" if rollback_ok else "failed"
        run_result.detail["rollback_msg"] = msg
        return run_result

    # ── rollback ───────────────────────────────────────────
    def rollback(self) -> tuple[bool, str]:
        """时光机回滚自毁 (worktree remove --force + prune)."""
        res = subprocess.run(
            ["bash", str(self.script), "rollback", self.session],
            capture_output=True,
            text=True,
            check=False,
        )
        return (res.returncode == 0, (res.stdout + res.stderr).strip())

    # ── 便捷: 一次完成 create→run→outcome ──────────────────
    def execute(self, cmd: str, from_commit: str = "HEAD") -> SandboxOutcome:
        """create + run + outcome 全链路. 失败自动回滚, 主工作区不受污染."""
        ok, msg = self.create(from_commit)
        if not ok:
            return SandboxOutcome(ok=False, exit_code=1, output=msg, detail={"stage": "create"})
        run_result = self.run(cmd)
        run_result.detail["stage"] = "run"
        return self.outcome(run_result)
