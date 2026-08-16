from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _import_omo_module(module_name: str):
    try:
        return importlib.import_module(module_name)
    except ImportError:
        workspace_projects = Path(__file__).resolve().parents[3]
        omo_src = workspace_projects / "omo" / "src"
        leaf = module_name.rsplit(".", 1)[-1]
        module_path = omo_src / "omo" / f"{leaf}.py"
        if not module_path.exists():
            raise
        inserted = False
        if str(omo_src) not in sys.path:
            sys.path.insert(0, str(omo_src))
            inserted = True
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            return module
        finally:
            if inserted and sys.path and sys.path[0] == str(omo_src):
                sys.path.pop(0)


def validate_planned_task_data(task_data: dict[str, Any]) -> list[str]:
    validator = _import_omo_module("omo.omo_task_schema").validate_task_data
    return validator(task_data, group="planned")


def create_goal_via_broker(
    omo_dir: Path,
    *,
    goal_id: str,
    title: str,
    description: str,
    source_ref: str,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ingress = _import_omo_module("omo.omo_ingress")
    return ingress.create_goal(
        omo_dir,
        goal_id=goal_id,
        title=title,
        description=description,
        ingress_plane="projects/c2g",
        source_ref=source_ref,
        extra_fields=extra_fields,
    )


def create_planned_task_via_broker(
    omo_dir: Path,
    *,
    task_data: dict[str, Any],
    source_ref: str,
) -> dict[str, Any]:
    ingress = _import_omo_module("omo.omo_ingress")
    return ingress.create_planned_task(
        omo_dir,
        task_data=task_data,
        ingress_plane="projects/c2g",
        source_ref=source_ref,
    )
