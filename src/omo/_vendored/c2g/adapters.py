from pathlib import Path

import yaml

from .domain import BetSchema, PitchSchema, TaskSchema
from .omo_client import (
    _import_omo_module,
    create_goal_via_broker,
    create_planned_task_via_broker,
    validate_planned_task_data,
)
from .ports import IGovernanceProvider, IStorageProvider


class EcosGovernanceProvider(IGovernanceProvider):
    def __init__(self, omo_dir_path: str | None = None):
        self.omo_dir = Path(omo_dir_path) if omo_dir_path else None
        try:
            _import_omo_module("omo.omo_task_schema")
        except ImportError:
            raise ImportError("The 'omo' package is required to use EcosGovernanceProvider. Install c2g[ecos].")

    def validate_pitch(self, pitch: PitchSchema) -> bool:
        if not pitch.upstream_ref:
            print("  ❌ [CR-STRATEGY-01 孤儿拦截] Pitch缺乏Upstream锚点，拒绝转化为Bet。")
            return False
        return True

    def validate_task(self, task: TaskSchema) -> bool:
        errors = validate_planned_task_data(task.model_dump())
        if errors:
            print("  ❌ M2 防腐层拦截 (Schema Validation Failed)")
            for err in errors:
                print(f"     - {err}")
            return False
        return True

    def get_current_phase(self) -> str:
        if not self.omo_dir:
            return "unknown"
        system_file = self.omo_dir / "state" / "system.yaml"
        if not system_file.exists():
            return "unknown"
        data = yaml.safe_load(system_file.read_text(encoding="utf-8")) or {}
        phase = data.get("current_phase") or data.get("phase") or data.get("phase_id")
        return str(phase) if phase is not None else "unknown"


class EcosStorageProvider(IStorageProvider):
    def __init__(self, omo_dir_path):
        from pathlib import Path

        self.omo_dir = Path(omo_dir_path)
        try:
            _import_omo_module("omo.omo_ingress")
        except ImportError:
            raise ImportError("The 'omo' package is required to use EcosStorageProvider. Install c2g[ecos].")

    def save_bet(self, bet: BetSchema) -> str:
        create_goal_via_broker(
            self.omo_dir,
            goal_id=bet.goal_id,
            title=bet.title,
            description=f"Bet: {bet.title} (Appetite: {bet.appetite})",
            source_ref=f"c2g:bet:{bet.goal_id}",
            extra_fields={
                "appetite": bet.appetite,
                "vector": bet.vector,
                "created_at": bet.created_at,
                "bet_status": bet.status,
            },
        )
        return bet.goal_id

    def save_task(self, task: TaskSchema) -> str:
        metadata = dict(task.metadata or {})
        task_data = {
            "id": task.task_id,
            "title": task.title,
            "status": "candidate",
            "task_type": metadata.get("task_type", "feature"),
            "risk_level": metadata.get("risk_level", "L0"),
            "depends_on": metadata.get("depends_on", []),
            "source_docs": metadata.get("source_docs", ["c2g:task-without-source-doc"]),
            "deliverables": metadata.get("deliverables", ["执行记录与源码修改"]),
            "imported_via": metadata.get("imported_via", "projects/c2g"),
            "context_uri": metadata.get("context_uri", f"bos://memory/tasks/{task.task_id}"),
            "assigned_to": None,
            "dispatch_id": None,
            "run_ref": None,
            "approval_ref": None,
            "review_ref": None,
            "knowledge_refs": [],
            "handoff_refs": [],
            "governance_refs": metadata.get("governance_refs", []),
            "entry_gate": metadata.get("entry_gate", []),
            "evidence_required": metadata.get("evidence_required", []),
            "test_plan": metadata.get("test_plan", ["c2g adapter default test plan"]),
            "allowed_operation_level": metadata.get("allowed_operation_level", "L0"),
            "human_approval_required": metadata.get("human_approval_required", False),
            "metadata": metadata,
        }
        create_planned_task_via_broker(
            self.omo_dir,
            task_data=task_data,
            source_ref=f"c2g:task:{task.task_id}",
        )
        return task.task_id

    def get_pitches(self) -> list[PitchSchema]:
        sandbox_dir = self.omo_dir.parent / "runtime" / "sandbox" / "pitches"
        if not sandbox_dir.exists():
            return []

        pitches = []
        for pf in sandbox_dir.glob("*.md"):
            content = pf.read_text(encoding="utf-8")
            upstream = None
            appetite = "Unknown"
            for line in content.split("\n"):
                if "> **Upstream**" in line:
                    upstream = line.split(":", 1)[1].strip() if ":" in line else line.strip()
                if "**Appetite:**" in line:
                    appetite = line.replace("**Appetite:**", "").strip()

            p = PitchSchema(
                pitch_id=pf.name,
                title=pf.stem,
                content=content,
                upstream_ref=upstream,
                appetite=appetite,
                created_at="Unknown",
            )
            pitches.append(p)
        return pitches

    def delete_pitch(self, pitch_id: str) -> bool:
        pf = self.omo_dir.parent / "runtime" / "sandbox" / "pitches" / pitch_id
        if pf.exists():
            pf.unlink()
            return True
        return False

    def get_active_bets(self) -> list[BetSchema]:
        goals_file = self.omo_dir / "goals" / "current.yaml"
        if not goals_file.exists():
            return []
        data = yaml.safe_load(goals_file.read_text(encoding="utf-8")) or {}
        bets = []
        for g in data.get("goals", []):
            gid = g.get("id", "")
            if not gid.startswith("BET-"):
                continue
            bets.append(
                BetSchema(
                    goal_id=gid,
                    title=g.get("title") or g.get("desc", gid),
                    description=g.get("desc", ""),
                    vector=g.get("vector", "V1"),
                    created_at=g.get("created_at", ""),
                    appetite=g.get("appetite", ""),
                )
            )
        return bets


def get_providers(base_dir_path: str | None = None, adapter_type: str = "ecos"):
    """
    Dependency Injection point.
    adapter_type can be 'ecos' (default for eCOS workspace) or 'local' (standalone usage).
    """
    if adapter_type == "ecos":
        try:
            return EcosGovernanceProvider(base_dir_path), EcosStorageProvider(base_dir_path)
        except ImportError as e:
            print(f"⚠️ eCOS Adapter not available ({e}). Falling back to 'local' adapter.")
            adapter_type = "local"

    if adapter_type == "local":
        from .adapters_local import LocalGovernanceProvider, LocalStorageProvider
        from .bridge_utils import get_c2g_data_dir

        # 锚定修复 (2026-07-02): base_dir 缺省 / 或来自 ecos 回退时的 .omo 路径,
        # 一律落到 <repo_root>/.c2g_data — 曾把 bets.json 写进 .omo/ 造成三处分叉
        local_dir = base_dir_path
        if not local_dir or Path(local_dir).name == ".omo":
            local_dir = str(get_c2g_data_dir())
        return LocalGovernanceProvider(), LocalStorageProvider(local_dir)

    raise ValueError(f"Unknown adapter type: {adapter_type}")
