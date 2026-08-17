"""Shared task_data builder — 消除 _import_bmad/_fast_track/_pitch 的重复构造"""

from __future__ import annotations

from typing import Any


def _governance_refs() -> list[str]:
    return [
        ".omo/standards/omo-governance-surfaces.md",
        ".omo/_truth/registry/omo-governance-surfaces.yaml",
        ".omo/_truth/x1-governance-policies.yaml",
        ".omo/_truth/x2-freshness-rules.yaml",
        ".omo/_truth/x3-value-stack.yaml",
        ".omo/_truth/x4-consistency-rules.yaml",
    ]


def build_ecos_task(
    task_id: str,
    title: str,
    *,
    task_type: str = "feature",
    risk_level: str = "L0",
    depends_on: list[str] | None = None,
    source_docs: list[str] | None = None,
    deliverables: list[str] | None = None,
    evidence_required: list[str] | None = None,
    test_plan: list[str] | None = None,
    imported_via: str = "omo_bridge",
    context_uri: str | None = None,
    entry_gate: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构造 ecos 适配器的 task_data dict，统一空位字段。"""
    task: dict[str, Any] = {
        "id": task_id,
        "title": title,
        "status": "candidate",
        "task_type": task_type,
        "risk_level": risk_level,
        "depends_on": depends_on or [],
        "source_docs": source_docs or [],
        "deliverables": deliverables or ["执行记录与源码修改"],
        "imported_via": imported_via,
        "context_uri": context_uri or f"bos://memory/tasks/{task_id}",
        "assigned_to": None,
        "dispatch_id": None,
        "run_ref": None,
        "approval_ref": None,
        "review_ref": None,
        "knowledge_refs": [],
        "handoff_refs": [],
        "governance_refs": _governance_refs(),
        "entry_gate": entry_gate or [],
        "evidence_required": evidence_required or [],
        "test_plan": test_plan or [],
        "allowed_operation_level": "L0",
        "human_approval_required": False,
        "metadata": {
            "governance_stack": "state_plane.kernel_plane.ingress_plane",
            "ingress_plane": "omo/_vendored/c2g",  # ADR-0412 内包,
        },
    }
    if extra:
        extra_payload = dict(extra)
        metadata_extra = extra_payload.pop("metadata", None)
        task.update(extra_payload)
        if isinstance(metadata_extra, dict):
            task["metadata"].update(metadata_extra)
    return task


def build_local_task(
    task_id: str,
    title: str,
    *,
    description: str = "",
    status: str = "planned",
    depends_on: list[str] | None = None,
    source_docs: list[str] | None = None,
    context_uri: str | None = None,
    imported_via: str = "omo_bridge",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构造 local 适配器的 task_data dict。"""
    from datetime import UTC, datetime

    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    task: dict[str, Any] = {
        "task_id": task_id,
        "title": title,
        "description": description,
        "status": status,
        "created_at": now,
        "updated_at": now,
        "metadata": {
            "depends_on": depends_on or [],
            "source_docs": source_docs or [],
            "context_uri": context_uri or f"bos://memory/tasks/{task_id}",
            "imported_via": imported_via,
            "governance_refs": _governance_refs(),
            "governance_stack": "state_plane.kernel_plane.ingress_plane",
            "ingress_plane": "omo/_vendored/c2g",  # ADR-0412 内包,
        },
    }
    if extra:
        task["metadata"].update(extra.pop("metadata", {}))
        task.update(extra)
    return task
