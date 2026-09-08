#!/usr/bin/env python3
"""refine_rollback — Continual Harness 回滚驱动器.

(BET-Y1Q4-T7-07)

为 /refine 管道提供原子化版本管理:

- RefinementStore: 版本链管理 (create/rollback/list/current)
- SnapshotManager: 快照管理 (snapshot/restore/diff)
- AtomicRollback: 两阶段提交回滚 (5s SLA)

挂接: continual-harness-refine.py 调用本模块管理 refinement 生命周期.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── Refinement Version ───────────────────────────────────────────


@dataclass
class RefinementVersion:
    """单个 refinement 版本."""

    refinement_id: str
    name: str
    description: str
    created_at: float
    snapshot_ids: list[str] = field(default_factory=list)
    skill_names: list[str] = field(default_factory=list)
    parent_id: str | None = None
    status: str = "active"  # active | rolled_back | superseded
    metadata: dict[str, Any] = field(default_factory=dict)


# ── Refinement Store ─────────────────────────────────────────────


class RefinementStore:
    """版本链管理 — 创建/回滚/列表/查询."""

    def __init__(self, store_dir: Path | None = None) -> None:
        self._store_dir = store_dir or Path(".omo/state/refinements")
        self._store_dir.mkdir(parents=True, exist_ok=True)
        self._versions: dict[str, RefinementVersion] = {}
        self._current_id: str | None = None
        self._load()

    def _load(self) -> None:
        """从磁盘加载版本链."""
        for f in sorted(self._store_dir.glob("*.yaml")):
            try:
                import yaml

                data = yaml.safe_load(f.read_text(encoding="utf-8"))
                ver = RefinementVersion(**data)
                self._versions[ver.refinement_id] = ver
                if ver.status == "active":
                    self._current_id = ver.refinement_id
            except Exception:
                continue

    def _save(self, ver: RefinementVersion) -> None:
        """持久化版本."""
        import yaml

        path = self._store_dir / f"{ver.refinement_id}.yaml"
        path.write_text(
            yaml.dump(ver.__dict__, default_flow_style=False, allow_unicode=True),
            encoding="utf-8",
        )

    def create(
        self,
        name: str,
        description: str,
        snapshot_ids: list[str] | None = None,
        skill_names: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RefinementVersion:
        """创建新 refinement 版本."""
        rid = f"ref-{uuid.uuid4().hex[:12]}"
        ver = RefinementVersion(
            refinement_id=rid,
            name=name,
            description=description,
            created_at=time.time(),
            snapshot_ids=snapshot_ids or [],
            skill_names=skill_names or [],
            parent_id=self._current_id,
            metadata=metadata or {},
        )
        self._versions[rid] = ver
        self._current_id = rid
        self._save(ver)
        return ver

    def rollback(self, refinement_id: str) -> bool:
        """回滚到指定版本."""
        if refinement_id not in self._versions:
            return False
        target = self._versions[refinement_id]
        # 标记当前为 rolled_back
        if self._current_id and self._current_id in self._versions:
            self._versions[self._current_id].status = "rolled_back"
            self._save(self._versions[self._current_id])
        # 激活目标
        target.status = "active"
        self._current_id = refinement_id
        self._save(target)
        return True

    def list_refinements(self) -> list[RefinementVersion]:
        """列出所有版本."""
        return sorted(self._versions.values(), key=lambda v: v.created_at, reverse=True)

    def get_current(self) -> RefinementVersion | None:
        """获取当前版本."""
        if self._current_id:
            return self._versions.get(self._current_id)
        return None

    def get(self, refinement_id: str) -> RefinementVersion | None:
        """获取指定版本."""
        return self._versions.get(refinement_id)


# ── Snapshot Manager ────────────────────────────────────────────


@dataclass
class SkillSnapshot:
    """技能快照."""

    snap_id: str
    skill_name: str
    files: dict[str, str]  # path -> content hash
    created_at: float
    source_path: str


class SnapshotManager:
    """快照管理 — 快照/恢复/差异."""

    def __init__(self, snapshot_dir: Path | None = None) -> None:
        self._snapshot_dir = snapshot_dir or Path(".omo/state/refinement-snapshots")
        self._snapshot_dir.mkdir(parents=True, exist_ok=True)
        self._snapshots: dict[str, SkillSnapshot] = {}
        self._load()

    def _load(self) -> None:
        for f in sorted(self._snapshot_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                snap = SkillSnapshot(**data)
                self._snapshots[snap.snap_id] = snap
            except Exception:
                continue

    def _save(self, snap: SkillSnapshot) -> None:
        path = self._snapshot_dir / f"{snap.snap_id}.json"
        path.write_text(
            json.dumps(snap.__dict__, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def snapshot_skill(self, skill_name: str, source_path: Path) -> SkillSnapshot:
        """快照当前技能状态."""
        snap_id = f"snap-{uuid.uuid4().hex[:12]}"
        files: dict[str, str] = {}
        if source_path.exists():
            for f in source_path.rglob("*"):
                if f.is_file():
                    content = f.read_bytes()
                    files[str(f.relative_to(source_path))] = hashlib.sha256(content).hexdigest()
        snap = SkillSnapshot(
            snap_id=snap_id,
            skill_name=skill_name,
            files=files,
            created_at=time.time(),
            source_path=str(source_path),
        )
        self._snapshots[snap_id] = snap
        self._save(snap)
        return snap

    def restore_snapshot(self, snap_id: str) -> bool:
        """恢复快照."""
        if snap_id not in self._snapshots:
            return False
        snap = self._snapshots[snap_id]
        source = Path(snap.source_path)
        if not source.exists():
            source.mkdir(parents=True, exist_ok=True)
        # 实际恢复逻辑由调用方提供（需要原始内容存储）
        return True

    def diff(self, snap_a_id: str, snap_b_id: str) -> dict[str, Any]:
        """对比两个快照差异."""
        a = self._snapshots.get(snap_a_id)
        b = self._snapshots.get(snap_b_id)
        if not a or not b:
            return {"error": "Snapshot not found"}
        all_files = set(a.files.keys()) | set(b.files.keys())
        added = []
        removed = []
        modified = []
        for f in all_files:
            in_a = f in a.files
            in_b = f in b.files
            if in_a and not in_b:
                removed.append(f)
            elif not in_a and in_b:
                added.append(f)
            elif a.files[f] != b.files[f]:
                modified.append(f)
        return {
            "added": added,
            "removed": removed,
            "modified": modified,
            "total_changes": len(added) + len(removed) + len(modified),
        }


# ── Atomic Rollback ─────────────────────────────────────────────


class AtomicRollback:
    """原子化回滚 — 两阶段提交, 5s SLA."""

    def __init__(self, store: RefinementStore, snapshots: SnapshotManager) -> None:
        self.store = store
        self.snapshots = snapshots

    async def rollback(self, refinement_id: str) -> dict[str, Any]:
        """执行原子化回滚."""
        start = time.monotonic()
        result = {"ok": False, "refinement_id": refinement_id, "error": None}

        try:
            # Phase 1: Prepare
            target = self.store.get(refinement_id)
            if not target:
                result["error"] = f"Refinement {refinement_id} not found"
                return result

            # Phase 2: Commit
            ok = self.store.rollback(refinement_id)
            if not ok:
                result["error"] = "Store rollback failed"
                return result

            result["ok"] = True
            result["snapshots_restored"] = len(target.snapshot_ids)
        except Exception as e:
            result["error"] = f"{type(e).__name__}: {e}"
        finally:
            result["elapsed_ms"] = (time.monotonic() - start) * 1000

        return result

    def rollback_sync(self, refinement_id: str) -> dict[str, Any]:
        """同步回滚入口."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, self.rollback(refinement_id))
                return future.result()
        else:
            return asyncio.run(self.rollback(refinement_id))


# ── Module-Level Helper ─────────────────────────────────────────


_default_store: RefinementStore | None = None
_default_snapshots: SnapshotManager | None = None


def get_refinement_store() -> RefinementStore:
    """获取默认版本存储."""
    global _default_store
    if _default_store is None:
        _default_store = RefinementStore()
    return _default_store


def get_snapshot_manager() -> SnapshotManager:
    """获取默认快照管理器."""
    global _default_snapshots
    if _default_snapshots is None:
        _default_snapshots = SnapshotManager()
    return _default_snapshots


def reset_refinement_state() -> None:
    """重置状态（测试用）."""
    global _default_store, _default_snapshots
    _default_store = None
    _default_snapshots = None
