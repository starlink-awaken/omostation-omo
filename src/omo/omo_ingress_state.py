from __future__ import annotations

import importlib.util
import io
import os
import sys
from contextlib import redirect_stdout
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from omo.omo_audit import record as record_audit
from omo.omo_governance_data import (
    build_governance_data,
    normalize_governance_data_json,
    serialize_governance_data,
)
from omo.omo_ingress_paths import (
    _audit_log_path,
    _delivery_root,
    _lock_path,
    _timestamp_slug,
    _utc_now,
    _workspace_relative,
)
from omo.omo_io import fcntl_lock, write_text_if_changed, write_yaml_atomic
from omo.omo_paths import STATE_ROOT_ENV
from omo.omo_shared import load_yaml

STATE_SYNC_TARGET = ".omo/state/health.yaml + .omo/state/system.yaml + BRIEF.md + .omo/_control/governance-data.json"


def _resolve_write_root(code_root: Path, state_root: Path | None) -> Path:
    """ADR-0456 C2 — the write root is declared, never inferred from the caller.

    Precedence: explicit ``state_root`` > env ``OMOSTATION_STATE_ROOT`` > ``code_root``.
    The last arm keeps an undeclared profile byte-identical to the historical layout
    without letting a fixture-rooted caller write into the host checkout.
    """
    candidate = state_root if state_root is not None else os.environ.get(STATE_ROOT_ENV)
    if candidate is not None and str(candidate):
        return Path(str(candidate)).expanduser().absolute()
    return code_root


def _load_root_module(workspace_root: Path, name: str, relative_path: str):
    module_path = workspace_root / relative_path
    module_key = f"_workspace_{name.replace('-', '_')}"
    cached = sys.modules.get(module_key)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(module_key, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_key] = module
    spec.loader.exec_module(module)
    return module


def normalize_health_yaml(payload: str) -> str:
    """Compare health projection semantically, ignoring generation timestamps."""
    lines = []
    for line in payload.splitlines():
        if line.startswith(("# generated_at:", "generated_at:")):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def normalize_system_yaml(payload: str) -> str:
    data = yaml.safe_load(payload) or {}
    if isinstance(data, dict):
        data = deepcopy(data)
        for field in (
            "health_score_generated_at",
            "governance_feedback_last_run",
            "updated_at",
        ):
            data.pop(field, None)
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=True)


def normalize_brief_md(payload: str) -> str:
    lines = []
    for line in payload.splitlines():
        if line.startswith("> **Generated**:"):
            lines.append("> **Generated**: `<runtime>`")
        else:
            lines.append(line)
    return "\n".join(lines).strip()


def _build_health_projection(code_root: Path) -> tuple[str, dict[str, Any]]:
    compass_radar = _load_root_module(code_root, "compass_radar", "bin/compass_radar.py")
    omo_dir = code_root / ".omo"
    output = omo_dir / "state" / "health.yaml"
    with redirect_stdout(io.StringIO()):
        report, _runtime_summary, _age_desc = compass_radar.build_health_projection(
            omo_dir=omo_dir,
            output=output,
        )
    return compass_radar.render_yaml(report), compass_radar.build_system_projection_updates(
        workspace_root=code_root,
        report=report,
    )


def _build_brief_content(code_root: Path) -> str:
    generate_brief = _load_root_module(code_root, "generate-brief", "bin/mof/generate-brief.py")
    return generate_brief.generate_brief_content()


def _system_payload(
    system_path: Path,
    system_updates: dict[str, Any],
) -> str:
    payload = load_yaml(system_path)
    if not isinstance(payload, dict):
        raise TypeError(f"state/system.yaml top-level must be a mapping: {system_path}")
    payload.update(deepcopy(system_updates))
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)


def _preview_write(
    path: Path,
    payload: str,
    *,
    normalize,
) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "changed": True, "reason": "missing"}
    current = path.read_text(encoding="utf-8")
    changed = normalize(current) != normalize(payload)
    return {
        "path": str(path),
        "changed": changed,
        "reason": "content" if changed else "unchanged",
    }


def _write_or_preview(
    path: Path,
    payload: str,
    *,
    normalize,
    dry_run: bool,
) -> dict[str, Any]:
    preview = _preview_write(path, payload, normalize=normalize)
    if not dry_run and preview["changed"]:
        changed = write_text_if_changed(path, payload, normalize=normalize)
        preview["changed"] = changed
        preview["reason"] = "written" if changed else "unchanged"
    return preview


def _record_state_sync(
    state_omo_dir: Path,
    *,
    display_root: Path,
    actor: str,
    source_ref: str,
    timestamp: str,
    writes: list[dict[str, Any]],
) -> str:
    from omo.omo_ingress import _record_mutation, _record_trail

    changed_paths = [
        _workspace_relative(Path(item["path"]), workspace_root=display_root) for item in writes if item.get("changed")
    ]
    artifact = {
        "kind": "state_projection_sync",
        "actor": actor,
        "source_ref": source_ref,
        "created_at": timestamp,
        "target": STATE_SYNC_TARGET,
        "changed_paths": changed_paths,
        "write_count": len(changed_paths),
        "writes": [
            {
                **item,
                "path": _workspace_relative(Path(str(item["path"])), workspace_root=display_root),
            }
            for item in writes
        ],
    }
    artifact_path = _delivery_root(state_omo_dir) / "state" / f"state-sync-{_timestamp_slug(timestamp)}.yaml"
    write_yaml_atomic(artifact_path, artifact)
    artifact_ref = _workspace_relative(artifact_path, workspace_root=display_root)
    details = (
        f"actor={actor} source_ref={source_ref or '-'} "
        f"changed={','.join(changed_paths) if changed_paths else '-'} artifact={artifact_ref}"
    )
    record_audit(
        action="ingress_sync_state_projection",
        debt_id="",
        actor=actor,
        details=details,
        audit_file=_audit_log_path(state_omo_dir),
    )
    _record_trail(  # type: ignore[reportUndefinedVariable]  # rebound at module load from omo.omo_ingress
        state_omo_dir,
        actor=f"broker:{actor}",
        action="sync_state_projection",
        target=STATE_SYNC_TARGET,
        parent_step_id=f"ingress:state-sync:{timestamp}",
    )
    _record_mutation(  # type: ignore[reportUndefinedVariable]  # rebound at module load from omo.omo_ingress
        state_omo_dir,
        actor=actor,
        action="sync_state_projection",
        target=STATE_SYNC_TARGET,
        artifact_ref=artifact_ref,
        source_ref=source_ref,
        broker_ref="projects/omo/src/omo/omo_ingress_state.py:sync_state_projection",
        created_at=timestamp,
        extra={"changed_paths": changed_paths, "write_count": len(changed_paths)},
    )
    return artifact_ref


def sync_state_projection(
    code_root: Path,
    *,
    state_root: Path | None = None,
    dry_run: bool = False,
    actor: str = "omo state sync",
    source_ref: str = "omo-state:sync",
    health_content: str | None = None,
    system_updates: dict[str, Any] | None = None,
    brief_content: str | None = None,
    governance_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Synchronize high-churn runtime projections through one OMO writer.

    ``code_root`` is the read plane (governance SSOT, root modules, ``.omo`` existence);
    every write target — the four projections and the runtime mirror root holding delivery,
    audit, trail, lock and mutation logs — resolves from ``state_root`` (ADR-0456 C1/C2).
    """
    code_root = code_root.resolve()
    write_root = _resolve_write_root(code_root, state_root)
    if not (code_root / ".omo").is_dir():
        raise FileNotFoundError(f"missing .omo directory: {code_root / '.omo'}")

    timestamp = _utc_now()
    state_omo_dir = write_root / ".omo"
    runtime_state_dir = state_omo_dir / "state" / "runtime"
    runtime_state_dir.mkdir(parents=True, exist_ok=True)

    # Canonical paths under .omo/state/runtime/ (ADR-0129)
    health_path = runtime_state_dir / "health.yaml"
    brief_path = runtime_state_dir / "brief.md"
    governance_data_path = runtime_state_dir / "governance-data.json"

    system_path = state_omo_dir / "state" / "system.yaml"

    with fcntl_lock(_lock_path(state_omo_dir)):
        if health_content is None or system_updates is None:
            health_content, system_updates = _build_health_projection(code_root)
        system_updates = dict(system_updates)
        system_updates["governance_feedback_last_run"] = timestamp
        system_updates["updated_at"] = timestamp
        system_payload = _system_payload(system_path, system_updates)
        writes = [
            _write_or_preview(
                health_path,
                health_content,
                normalize=normalize_health_yaml,
                dry_run=dry_run,
            ),
            _write_or_preview(
                system_path,
                system_payload,
                normalize=normalize_system_yaml,
                dry_run=dry_run,
            ),
        ]
        if brief_content is None:
            brief_content = _build_brief_content(code_root)
        if governance_data is None:
            governance_data = build_governance_data(code_root)
        writes.extend(
            [
                _write_or_preview(
                    brief_path,
                    brief_content,
                    normalize=normalize_brief_md,
                    dry_run=dry_run,
                ),
                _write_or_preview(
                    governance_data_path,
                    serialize_governance_data(governance_data),
                    normalize=normalize_governance_data_json,
                    dry_run=dry_run,
                ),
            ]
        )

        changed_count = sum(1 for item in writes if item.get("changed"))
        artifact_ref = ""
        if not dry_run and changed_count:
            artifact_ref = _record_state_sync(
                state_omo_dir,
                display_root=code_root,
                actor=actor,
                source_ref=source_ref,
                timestamp=timestamp,
                writes=writes,
            )

    return {
        "ok": True,
        "dry_run": dry_run,
        "actor": actor,
        "source_ref": source_ref,
        "target": STATE_SYNC_TARGET,
        "code_root": str(code_root),
        "state_root": str(write_root),
        "changed_count": changed_count,
        "artifact_ref": artifact_ref,
        "writes": [
            {
                **item,
                "path": _workspace_relative(Path(str(item["path"])), workspace_root=code_root),
            }
            for item in writes
        ],
    }


# --- Lazy indirection helpers from omo.omo_ingress (avoids static cycle) ---
# At module load, copy omo.omo_ingress's private helpers into our globals
# so LOAD_GLOBAL inside our functions finds them directly. Use a deferred
# try/except to handle the cycle: omo.omo_ingress may not be fully
# initialized yet when our module loads (it imports us).
import sys as _sys


def _bind_helpers() -> None:
    mod = _sys.modules.get("omo.omo_ingress")
    if mod is None:
        return
    for _name in (
        "_record_trail",
        "_record_mutation",
        "_load_registry",
        "_register_ingress",
        "_write_registry",
    ):
        if hasattr(mod, _name) and _name not in globals():
            globals()[_name] = getattr(mod, _name)


try:
    _bind_helpers()
except Exception:
    pass
