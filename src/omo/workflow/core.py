from __future__ import annotations

import argparse
import fnmatch
import re
import shlex
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

if __package__:
    from .delivery_anchor import DELIVERY_RELATIVE, ensure_delivery_anchor
else:
    # core.py is also loaded package-less by production code
    # (bin/plan/bet-ledger._external_workflow_core) and by the root test
    # loaders: a bare spec_from_file_location() gives __package__ == "" so a
    # relative import has no parent package.  Mirror the install_namespace
    # pattern from bin/agent-workflow.py: install the private package
    # namespaces pointing at the real source dirs, then import absolutely, so
    # every loader context binds the same omo.workflow.delivery_anchor module.
    import importlib.util as _ilu
    import sys as _sys

    _workflow_dir = Path(__file__).resolve().parent
    for _ns, _dir in (
        ("omo", _workflow_dir.parent),
        ("omo.workflow", _workflow_dir),
    ):
        if _ns not in _sys.modules:
            _ns_spec = _ilu.spec_from_loader(_ns, loader=None, is_package=True)
            if _ns_spec is None:
                raise ImportError(f"cannot create private namespace {_ns}")
            _ns_spec.submodule_search_locations = [str(_dir)]
            _ns_module = _ilu.module_from_spec(_ns_spec)
            _ns_module.__path__ = [str(_dir)]
            _sys.modules[_ns] = _ns_module
    from omo.workflow.delivery_anchor import DELIVERY_RELATIVE, ensure_delivery_anchor

WORKSPACE = Path(__file__).resolve().parents[5]
REGISTRY_PATH = WORKSPACE / ".omo/_truth/registry/agent-workflows"
AGENT_CLIS_PATH = WORKSPACE / ".omo/_truth/registry/agent-clis.yaml"
AGORA_BOS_REGISTRY_PATH = WORKSPACE / "projects/agora/etc/bos-services.yaml"
AGCP_MOF_WORKFLOW_PATH = (
    WORKSPACE / "projects/ecos/src/ecos/ssot/mof/m1/workflow/WORKFLOW-AGENT-GOVERNANCE-CONTROL-PLANE.yaml"
)
AGCP_MOF_BOSROUTE_PATH = (
    WORKSPACE / "projects/ecos/src/ecos/ssot/mof/m1/bosroute/BOSROUTE-GOVERNANCE-AGENT-WORKFLOW.yaml"
)
AGCP_BOS_ROUTES = {
    "bos://governance/agent-workflow/bootstrap",
    "bos://governance/agent-workflow/verify-plan",
    "bos://governance/agent-workflow/observe",
    "bos://governance/agent-workflow/compliance",
    "bos://governance/agent-workflow/doctor",
}
MOF_MODEL_PATH_PATTERN = "projects/ecos/src/ecos/ssot/mof/**"
MOF_DIFF_CHECK_IDS = {"mof-schema-validate", "mof-state-bridge", "mof-drift"}
ADAPTER_AUTHORITIES = {"discipline_layer", "input_adapter", "memory_adapter"}
INTEGRATION_AUTHORITIES = {
    "entrypoint",
    "governance_gate",
    "model_registry",
    "state_broker",
    "strategy_ingress",
}
CLAIM_POLICY_MODES = {"off", "advisory", "required"}
RUN_UPDATE_LOCK_TIMEOUT_SECONDS = 30.0
EXTERNAL_ROOT_REGISTRY_PATH = WORKSPACE / ".omo/_truth/registry/external-write-roots.yaml"


class WorkflowError(RuntimeError):
    """Raised when the workflow registry or run state is invalid."""


class SafeFormatDict(dict[str, str]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base``.

    Dict values are deep-merged; all other types use last-wins semantics.
    """
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, Any]:
    if not path.exists():
        raise WorkflowError(f"workflow registry not found: {path}")
    documents: list[dict[str, Any]] = []
    yaml_files: list[Path] = []
    if path.is_dir():
        yaml_files = sorted(path.glob("**/*.yaml"))
        if not yaml_files:
            raise WorkflowError(f"no YAML files found in registry directory: {path}")
        for yaml_file in yaml_files:
            try:
                for doc in yaml.safe_load_all(yaml_file.read_text(encoding="utf-8")):
                    if doc and isinstance(doc, dict):
                        documents.append(doc)
            except yaml.YAMLError as exc:
                raise WorkflowError(f"invalid YAML in {display_path(yaml_file)}: {exc}") from exc
    else:
        yaml_files = [path]
        try:
            for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
                if doc and isinstance(doc, dict):
                    documents.append(doc)
        except yaml.YAMLError as exc:
            raise WorkflowError(f"invalid YAML in {display_path(path)}: {exc}") from exc
    merged: dict[str, Any] = {}
    workflows_list: list[dict[str, Any]] = []
    external_patterns: dict[str, Any] = {}
    for yaml_file in yaml_files:
        for doc in yaml.safe_load_all(yaml_file.read_text(encoding="utf-8")):
            if not isinstance(doc, dict):
                continue
            is_adapter_file = "adapters" in yaml_file.parts
            is_workflow_file = "workflows" in yaml_file.parts
            if "workflows" in doc:
                workflows_list.extend(doc["workflows"])
                # ADR-0381 P0b: 单文件格式 (旧 monolithic / 测试 fixture) 中
                # runner/claim_policy/diff_checks 等与 workflows 同 doc — 必须一并合并,
                # 否则 runner 配置 (lock_state_dir 等) 丢失 → 锁落到默认目录 (v10 回归).
                for key, value in doc.items():
                    if key == "workflows":
                        continue
                    if key == "external_patterns" and isinstance(value, dict):
                        external_patterns.update(value)
                        continue
                    if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                        merged[key] = _deep_merge(merged[key], value)
                    else:
                        merged[key] = value
                continue
            if is_workflow_file and "id" in doc and "run_frequency" in doc:
                workflows_list.append(doc)
                continue
            for key, value in doc.items():
                if is_adapter_file:
                    external_patterns[key] = value
                elif key != "external_patterns":
                    if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                        merged[key] = _deep_merge(merged[key], value)
                    else:
                        merged[key] = value
    if not workflows_list:
        raise WorkflowError(f"workflow registry has no workflows: {path}")
    merged["workflows"] = workflows_list
    if external_patterns:
        merged["external_patterns"] = external_patterns
    return merged


def is_default_registry_path(path: Path) -> bool:
    candidate = path if path.is_absolute() else WORKSPACE / path
    try:
        resolved = candidate.resolve()
    except OSError:
        return False
    try:
        default_resolved = REGISTRY_PATH.resolve()
        return resolved == default_resolved
    except OSError:
        return False


def load_yaml_document_with(path: Path, key: str) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, f"missing file: {display_path(path)}"
    try:
        documents = [doc for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")) if doc]
    except yaml.YAMLError as exc:
        return None, f"invalid YAML: {display_path(path)} ({exc})"
    for document in documents:
        if isinstance(document, dict) and key in document:
            return document, None
    return None, f"{display_path(path)} has no YAML document with key '{key}'"


def workflow_by_id(registry: dict[str, Any], workflow_id: str) -> dict[str, Any]:
    for workflow in registry.get("workflows", []):
        if workflow.get("id") == workflow_id:
            return workflow
    raise WorkflowError(f"unknown workflow: {workflow_id}")


def workflow_roles(workflow: dict[str, Any]) -> list[str]:
    agents = workflow.get("agents") or {}
    roles = agents.get("roles") if isinstance(agents, dict) else []
    if not isinstance(roles, list):
        return []
    return [role for role in roles if isinstance(role, str)]


def validate_agent_profile(
    registry: dict[str, Any],
    workflow: dict[str, Any],
    profile_id: str,
    require: bool,
) -> None:
    roles = workflow_roles(workflow)
    workflow_id = str(workflow.get("id") or "")
    if not profile_id:
        if require and roles:
            raise WorkflowError(f"{workflow_id} requires --profile ({', '.join(roles)})")
        return
    profiles = registry.get("agent_profiles") or {}
    profile = profiles.get(profile_id) if isinstance(profiles, dict) else None
    if not isinstance(profile, dict):
        raise WorkflowError(f"unknown agent profile: {profile_id}")
    allowed = profile.get("allowed_workflows", [])
    if allowed != ["*"] and workflow_id not in allowed:
        raise WorkflowError(f"agent profile {profile_id} cannot run workflow {workflow_id}")
    if roles and profile_id not in roles:
        raise WorkflowError(f"agent profile {profile_id} is not listed in {workflow_id}.agents.roles")


def context_from_args(args: argparse.Namespace) -> dict[str, str]:
    return {
        "project": str(getattr(args, "project", "") or ""),
        "format": str(getattr(args, "format", "") or "openspec"),
        "source_file": str(getattr(args, "source_file", "") or ""),
        "run_id": str(getattr(args, "run_id", "") or ""),
        "actor": str(getattr(args, "actor", "") or "agent"),
        "profile": str(getattr(args, "profile", "") or ""),
    }


def substitute(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        return value.format_map(SafeFormatDict(context))
    if isinstance(value, list):
        return [substitute(item, context) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, context) for key, item in value.items()}
    return value


def command_display(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def load_external_write_roots(registry_path: Path | None = None) -> list[dict[str, Any]]:
    """Load explicit, fail-closed local-git roots that agents may write."""
    path = Path(registry_path or EXTERNAL_ROOT_REGISTRY_PATH).expanduser()
    if not path.is_file():
        return []
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise WorkflowError("external write-root registry is unreadable") from exc
    if not isinstance(payload, dict) or payload.get("schema") != "external-write-root-registry/v1":
        raise WorkflowError("external write-root registry schema is invalid")
    raw_roots = payload.get("roots")
    if not isinstance(raw_roots, list):
        raise WorkflowError("external write-root registry roots must be a list")

    roots: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw in raw_roots:
        if not isinstance(raw, dict):
            raise WorkflowError("external write root must be a mapping")
        root_id = raw.get("id")
        source_path = raw.get("path")
        kind = raw.get("kind")
        status = raw.get("status")
        patterns = raw.get("patterns")
        if (
            not isinstance(root_id, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,62}", root_id)
            or root_id in seen_ids
            or not isinstance(source_path, str)
            or kind != "local_git"
            or status != "admitted"
            or not isinstance(patterns, list)
            or not patterns
            or not all(isinstance(item, str) and item and "\\" not in item for item in patterns)
        ):
            raise WorkflowError("external write root entry is invalid")
        seen_ids.add(root_id)
        absolute = Path(source_path).expanduser()
        if not absolute.is_absolute():
            raise WorkflowError(f"external write root must be absolute after expansion: {root_id}")
        resolved = absolute.resolve()
        if resolved == WORKSPACE.resolve() or resolved.is_relative_to(WORKSPACE.resolve()):
            raise WorkflowError(f"external write root must stay outside Workspace: {root_id}")
        # A missing or non-git root is simply not admitted. Malformed registry
        # entries remain hard errors above so configuration drift stays visible.
        if absolute.is_symlink() or resolved.is_symlink() or not resolved.is_dir():
            continue
        git_entry = resolved / ".git"
        if not (git_entry.is_dir() or git_entry.is_file()):
            continue
        roots.append(
            {
                "id": root_id,
                "absolute": absolute,
                "resolved": resolved,
                "patterns": tuple(patterns),
            }
        )
    return sorted(roots, key=lambda item: item["id"])


def external_write_root_for_path(path: str | Path, registry_path: Path | None = None) -> dict[str, Any] | None:
    """Return the admitted root containing path, or None for another path."""
    candidate = Path(path).expanduser()
    try:
        resolved = candidate.resolve()
    except OSError as exc:
        raise WorkflowError(f"external write path is unreadable: {path}") from exc
    for root in load_external_write_roots(registry_path):
        if not resolved.is_relative_to(root["resolved"]):
            continue
        relative = resolved.relative_to(root["resolved"]).as_posix()
        if relative and any(fnmatch.fnmatchcase(relative, pattern) for pattern in root["patterns"]):
            return {**root, "relative": relative}
        return None
    return None


def external_synthetic_path(path: str | Path, registry_path: Path | None = None) -> str | None:
    matched = external_write_root_for_path(path, registry_path)
    if matched is None:
        return None
    return f"external/{matched['id']}/{matched['relative']}"


def normalize_external_surface(surface: str, registry_path: Path | None = None) -> str | None:
    """Map an admitted external surface to its canonical synthetic prefix."""
    raw = str(surface).strip()
    expanded = Path(raw).expanduser().resolve(strict=False)
    for root in load_external_write_roots(registry_path):
        root_text = root["resolved"].as_posix().rstrip("/")
        expanded_text = expanded.as_posix().rstrip("/")
        if expanded_text == root_text:
            return f"external/{root['id']}"
        if expanded_text.startswith(root_text + "/"):
            suffix = expanded_text[len(root_text) + 1 :]
            return f"external/{root['id']}/{suffix}"
    return None


def normalize_repo_path(raw_path: str) -> str:
    if not raw_path:
        raise WorkflowError("path cannot be empty")
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        try:
            path = path.resolve().relative_to(WORKSPACE)
        except ValueError as exc:
            synthetic = external_synthetic_path(raw_path)
            if synthetic is not None:
                return synthetic
            raise WorkflowError(f"path is outside workspace: {raw_path}") from exc
    normalized = path.as_posix().strip("/")
    if normalized in {"", "."}:
        return "."
    if normalized == ".." or normalized.startswith("../") or "/../" in normalized:
        raise WorkflowError(f"path escapes workspace: {raw_path}")
    return normalized


def changed_files_from_git(include_untracked: bool) -> list[str]:
    commands = [
        ["git", "diff", "--name-only"],
        ["git", "diff", "--cached", "--name-only"],
    ]
    if include_untracked:
        commands.append(["git", "ls-files", "--others", "--exclude-standard"])
    changed: set[str] = set()
    for command in commands:
        completed = subprocess.run(command, cwd=WORKSPACE, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            raise WorkflowError(f"failed to inspect changed files: {command_display(command)}")
        for line in completed.stdout.splitlines():
            if line.strip():
                changed.add(normalize_repo_path(line.strip()))

    status_command = ["git", "status", "--porcelain", "-z", "--", "."]
    for root in load_external_write_roots():
        completed = subprocess.run(status_command, cwd=root["resolved"], capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            continue
        entries = completed.stdout.split("\0")
        index = 0
        while index < len(entries):
            entry = entries[index]
            if not entry:
                index += 1
                continue
            status = entry[:2]
            path = entry[3:]
            index += 1
            if "R" in status and index < len(entries):
                path = entries[index]
                index += 1
            if not path:
                continue
            matched = external_write_root_for_path(root["resolved"] / path)
            if matched is not None:
                changed.add(f"external/{matched['id']}/{matched['relative']}")
    return sorted(changed)


def path_matches(patterns: list[str], path: str) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(WORKSPACE))
    except ValueError:
        return str(path)


def registry_workspace_root(registry: dict[str, Any]) -> Path:
    """Resolve the single workspace root that owns a registry's effects.

    Production registries retain the module-derived Workspace default. Tests
    and isolated runners can bind an explicit root without redirecting only a
    subset of run, lock, ledger, Mesh, or MOS writes.
    """
    configured = registry.get("runner", {}).get("workspace_root")
    if not configured:
        return WORKSPACE.resolve()
    root = Path(str(configured)).expanduser()
    if not root.is_absolute():
        root = WORKSPACE / root
    return root.resolve()


def _runner_path(registry: dict[str, Any], key: str, default: str) -> Path:
    configured = Path(str(registry.get("runner", {}).get(key, default))).expanduser()
    if configured.is_absolute():
        return configured
    workspace = registry_workspace_root(registry)
    # Delivery-state anchor (BET-Y2Q4-T10-208): only the production layout
    # bridges to the canonical checkout; custom runner configs never trigger.
    if configured.parts[: len(DELIVERY_RELATIVE.parts)] == DELIVERY_RELATIVE.parts:
        ensure_delivery_anchor(workspace)
    return workspace / configured


def run_state_dir(registry: dict[str, Any]) -> Path:
    return _runner_path(registry, "run_state_dir", ".omo/_delivery/agent-workflows/runs")


def lock_state_dir(registry: dict[str, Any]) -> Path:
    return _runner_path(registry, "lock_state_dir", ".omo/_delivery/agent-workflows/locks")


def ledger_path(registry: dict[str, Any]) -> Path:
    return _runner_path(registry, "ledger_path", ".omo/_delivery/agent-workflows/events.jsonl")


def workflow_rows(registry: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": workflow["id"],
            "title": workflow.get("title", ""),
            "lanes": workflow.get("allowed_lanes", []),
        }
        for workflow in registry.get("workflows", [])
    ]


def agent_rows(registry: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": profile_id,
            "purpose": profile.get("purpose", ""),
            "allowed_workflows": profile.get("allowed_workflows", []),
            "can_write_lanes": profile.get("can_write_lanes", []),
        }
        for profile_id, profile in sorted((registry.get("agent_profiles") or {}).items())
        if isinstance(profile, dict)
    ]


def integration_rows(registry: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, integration in (registry.get("internal_integrations") or {}).items():
        if not isinstance(integration, dict):
            continue
        rows.append(
            {
                "name": name,
                "status": integration.get("status"),
                "authority": integration.get("authority"),
                "owner": integration.get("owner"),
                "ssot_rule": integration.get("ssot_rule"),
                "gate_binding": integration.get("gate_binding"),
                "health_command": integration.get("health_command"),
                "health_required": bool(integration.get("health_required", False)),
                "commands": {
                    key: value
                    for key, value in integration.items()
                    if key
                    not in {
                        "status",
                        "authority",
                        "owner",
                        "ssot_rule",
                        "gate_binding",
                        "health_command",
                        "health_required",
                    }
                },
            }
        )
    return rows


def adapter_rows(registry: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, adapter in (registry.get("external_patterns") or {}).items():
        if not isinstance(adapter, dict):
            continue
        command = adapter.get("command")
        found = shutil.which(str(command)) if command else None
        rows.append(
            {
                "name": name,
                "status": adapter.get("status"),
                "authority": adapter.get("authority"),
                "ssot_rule": adapter.get("ssot_rule"),
                "ingress_workflow": adapter.get("ingress_workflow"),
                "skill": adapter.get("skill"),
                "command": command,
                "available": bool(found) if command else True,
                "path": found,
                "bridge": adapter.get("bridge"),
                "pattern": adapter.get("pattern"),
                "degrade_to": adapter.get("degrade_to"),
                "health_command": adapter.get("health_command"),
                "health_required": bool(adapter.get("health_required", False)),
            }
        )
    return rows
