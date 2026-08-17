from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from .core import WorkflowError

SCHEMA = "affected-graph-receipt/v1"
WORKSPACE_ROOT_PROJECT = "workspace-root"


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _project_layers(layer_contract: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for layer, info in layer_contract.get("layers", {}).items():
        for project in info.get("projects", []):
            result[str(project)] = str(layer)
    return result


def _affected_projects(changed_projects: list[str], layer_contract: dict[str, Any]) -> list[str]:
    project_layers = _project_layers(layer_contract)
    graph = {project: set() for project in project_layers}
    downstream_layers = {layer: set() for layer in layer_contract.get("layers", {})}
    rules = layer_contract.get("dependency_rules", {}).get("allowed_directions", [])
    for rule in rules:
        for downstream in rule.get("from", []):
            for upstream in rule.get("to", []):
                downstream_layers.setdefault(str(upstream), set()).add(str(downstream))
    for upstream_project, upstream_layer in project_layers.items():
        for downstream_layer in downstream_layers.get(upstream_layer, set()):
            graph[upstream_project].update(
                project for project, layer in project_layers.items() if layer == downstream_layer
            )

    affected = set(changed_projects)
    queue = [project for project in changed_projects if project != WORKSPACE_ROOT_PROJECT]
    while queue:
        current = queue.pop(0)
        for downstream in graph[current]:
            if downstream not in affected:
                affected.add(downstream)
                queue.append(downstream)
    return sorted(affected)


def _claimed_projects(paths: list[str], known_projects: set[str]) -> set[str]:
    claimed: set[str] = set()
    for raw_path in paths:
        normalized_path = Path(raw_path).as_posix().lstrip("./").rstrip("/")
        if normalized_path == "projects":
            raise WorkflowError("ambiguous projects path cannot be bound to one affected project")
        parts = normalized_path.split("/")
        if len(parts) >= 2 and parts[0] == "projects":
            project = parts[1]
            if project not in known_projects:
                raise WorkflowError(f"claimed path references unknown project: {project}")
            claimed.add(project)
        else:
            claimed.add(WORKSPACE_ROOT_PROJECT)
    return claimed


def validate_affected_graph_receipt(
    receipt_reference: str | Path,
    claimed_paths: list[str],
    workspace_root: str | Path,
    claimed_surfaces: list[str] | None = None,
) -> dict[str, Any]:
    workspace = Path(workspace_root).resolve()
    receipt_ref = str(receipt_reference)
    relative_path = Path(receipt_ref)
    if (
        not receipt_ref
        or relative_path.is_absolute()
        or "//" in receipt_ref
        or any(part in {"", ".", ".."} for part in relative_path.parts)
        or relative_path.as_posix() != receipt_ref
    ):
        raise WorkflowError("affected graph receipt reference must be canonical workspace-relative")
    receipt_path = workspace / relative_path
    try:
        resolved_receipt = receipt_path.resolve(strict=True)
    except OSError as exc:
        raise WorkflowError(f"affected graph receipt file does not exist: {receipt_path}") from exc
    if resolved_receipt != receipt_path.absolute():
        raise WorkflowError("affected graph receipt reference must not traverse symlinks")
    try:
        resolved_receipt.relative_to(workspace)
    except ValueError as exc:
        raise WorkflowError("affected graph receipt reference must stay inside workspace") from exc
    if not resolved_receipt.is_file():
        raise WorkflowError(f"affected graph receipt is not a file: {receipt_ref}")

    try:
        receipt = json.loads(resolved_receipt.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowError(f"invalid affected graph receipt: {exc}") from exc
    if not isinstance(receipt, dict) or receipt.get("schema") != SCHEMA:
        raise WorkflowError(f"affected graph receipt must use schema {SCHEMA}")

    required = {
        "schema",
        "changed_projects",
        "affected_projects",
        "layer_contract_digest",
        "receipt_hash",
    }
    if set(receipt) != required:
        raise WorkflowError("affected graph receipt fields do not match schema")
    changed = receipt.get("changed_projects")
    affected = receipt.get("affected_projects")
    if (
        not isinstance(changed, list)
        or not changed
        or not all(isinstance(item, str) and item for item in changed)
        or changed != sorted(set(changed))
        or not isinstance(affected, list)
        or not all(isinstance(item, str) and item for item in affected)
        or affected != sorted(set(affected))
    ):
        raise WorkflowError("affected graph receipt project lists must be sorted and unique")

    unsigned = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    expected_hash = hashlib.sha256(_canonical_json(unsigned).encode()).hexdigest()
    if receipt.get("receipt_hash") != expected_hash:
        raise WorkflowError("affected graph receipt_hash mismatch")

    contract_path = workspace / "docs" / "layer-contract.yaml"
    if not contract_path.is_file():
        raise WorkflowError(f"layer contract does not exist: {contract_path}")
    contract_bytes = contract_path.read_bytes()
    current_digest = hashlib.sha256(contract_bytes).hexdigest()
    if receipt.get("layer_contract_digest") != current_digest:
        raise WorkflowError("affected graph layer contract digest mismatch")
    layer_contract = yaml.safe_load(contract_bytes) or {}
    known_projects = set(_project_layers(layer_contract))
    permitted_projects = known_projects | {WORKSPACE_ROOT_PROJECT}
    unknown = (set(changed) | set(affected)) - permitted_projects
    if unknown:
        raise WorkflowError("affected graph receipt contains unknown project(s): " + ", ".join(sorted(unknown)))
    expected_affected = _affected_projects(changed, layer_contract)
    if affected != expected_affected:
        raise WorkflowError("affected graph affected_projects do not match recomputation")

    claimed_projects = _claimed_projects(claimed_paths, known_projects)
    if claimed_surfaces:
        claimed_projects.add(WORKSPACE_ROOT_PROJECT)
    missing = claimed_projects - set(affected)
    if missing:
        raise WorkflowError("claimed projects missing from affected graph receipt: " + ", ".join(sorted(missing)))
    return {**receipt, "receipt_ref": receipt_ref}
