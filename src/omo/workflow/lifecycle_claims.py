"""Claim 工具组 — 从 lifecycle.py 拆出 (2026-08-26 SRP 行数门 >1500L).

纯函数 claim 语义: 模式规范化 / 策略解析 / 已领路径与覆盖判定.
lifecycle.py 通过 re-export 保持对外接口不变.
"""

from __future__ import annotations

from typing import Any

from .core import (
    CLAIM_POLICY_MODES,
    WorkflowError,
    normalize_repo_path,
    path_matches,
    workflow_by_id,
)


def normalize_claim_mode(raw_mode: Any, default: str = "advisory") -> str:
    mode = str(raw_mode or default)
    return mode if mode in CLAIM_POLICY_MODES else default


def claim_policy(registry: dict[str, Any]) -> dict[str, Any]:
    policy = registry.get("claim_policy")
    if not isinstance(policy, dict):
        return {"mode": "advisory", "required_paths": [], "tiers": []}
    mode = normalize_claim_mode(policy.get("mode"))
    required_paths = policy.get("required_paths") or []
    normalized_required_paths = [str(item) for item in required_paths if isinstance(item, str)]
    tiers: list[dict[str, Any]] = []
    if normalized_required_paths:
        tiers.append(
            {
                "id": "legacy-required-paths",
                "mode": mode,
                "paths": normalized_required_paths,
            }
        )
    for index, tier in enumerate(policy.get("tiers") or []):
        if not isinstance(tier, dict):
            continue
        paths = [str(item) for item in tier.get("paths") or [] if isinstance(item, str)]
        if not paths:
            continue
        tier_mode = normalize_claim_mode(tier.get("mode"), default="advisory")
        if tier_mode == "off":
            continue
        tiers.append(
            {
                "id": str(tier.get("id") or f"tier-{index + 1}"),
                "mode": tier_mode,
                "paths": paths,
            }
        )
    return {
        "mode": mode,
        "required_paths": normalized_required_paths,
        "tiers": tiers,
    }


def claimed_paths(payload: dict[str, Any]) -> list[str]:
    paths: set[str] = set()
    for claim in payload.get("claims") or []:
        if not isinstance(claim, dict):
            continue
        for item in claim.get("paths") or []:
            if isinstance(item, str) and item.strip():
                paths.add(normalize_repo_path(item))
    return sorted(paths)


def claim_covers_path(claimed_path: str, changed_path: str) -> bool:
    normalized_claim = normalize_repo_path(claimed_path)
    normalized_changed = normalize_repo_path(changed_path)
    if normalized_claim == ".":
        return True
    if path_matches([normalized_claim], normalized_changed):
        return True
    return normalized_changed.startswith(normalized_claim.rstrip("/") + "/")


def is_read_only_workflow(registry: dict[str, Any], workflow_id: str) -> bool:
    """True when workflow declares empty write surfaces (ADR-0209 A4)."""
    if not workflow_id:
        return False
    try:
        workflow = workflow_by_id(registry, workflow_id)
    except WorkflowError:
        return False
    surfaces = workflow.get("surfaces") or {}
    write = surfaces.get("write")
    # Explicit empty write list => read-only. Missing write key is NOT exempt
    # (legacy workflows may omit surfaces entirely).
    return isinstance(write, list) and len(write) == 0
