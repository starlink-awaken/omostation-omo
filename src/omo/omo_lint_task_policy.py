#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any


from .omo_task_policy import (
    OPC_P6_SELF_EVOLUTION_POLICY,
    TASK_POLICIES,
    check_task_policy,
    count_planned_matches,
    get_task_policy,
)


def cmd_lint_task_policy(policy_name: str, workspace_root: str = ".") -> int:
    root = Path(workspace_root).resolve()
    policy = get_task_policy(policy_name)
    issues = check_task_policy(root, policy)
    if issues:
        print(f"❌ omo lint {policy.name} fail: {len(issues)} issue(s)")
        for issue in issues:
            print(f"  - {issue}")
        return 1
    count = count_planned_matches(root, policy)
    print(f"✅ omo lint {policy.name} pass: matches={count}")
    return 0


def cmd_lint_all_task_policies(workspace_root: str = ".") -> int:
    root = Path(workspace_root).resolve()
    failures = 0
    for policy_name in sorted(TASK_POLICIES):
        failures += cmd_lint_task_policy(policy_name, str(root))
    return 0 if failures == 0 else 1


def cmd_lint_self_evolution_approval(workspace_root: str = ".") -> int:
    return cmd_lint_task_policy(OPC_P6_SELF_EVOLUTION_POLICY.name, workspace_root)
