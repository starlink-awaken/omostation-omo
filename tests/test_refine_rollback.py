#!/usr/bin/env python3
"""test_refine_rollback — Continual Harness 回滚驱动器单元测试.

(BET-Y1Q4-T7-07)

覆盖:
- RefinementStore (创建/回滚/列表/查询)
- SnapshotManager (快照/恢复/差异)
- AtomicRollback (原子化回滚)
- 集成测试
- 压力测试
"""

from __future__ import annotations

import asyncio
import pytest

from omo.resident.refine_rollback import (
    AtomicRollback,
    RefinementStore,
    RefinementVersion,
    SnapshotManager,
    SkillSnapshot,
    get_refinement_store,
    get_snapshot_manager,
    reset_refinement_state,
)


# ── RefinementStore Tests ────────────────────────────────────────


class TestRefinementStore:
    def test_create(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        ver = store.create("test-refine", "测试精炼")
        assert ver.refinement_id.startswith("ref-")
        assert ver.name == "test-refine"
        assert ver.status == "active"

    def test_get_current(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        ver = store.create("current-test", "当前版本")
        current = store.get_current()
        assert current is not None
        assert current.refinement_id == ver.refinement_id

    def test_rollback(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        v1 = store.create("v1", "版本1")
        v2 = store.create("v2", "版本2")
        assert store.get_current().refinement_id == v2.refinement_id
        ok = store.rollback(v1.refinement_id)
        assert ok
        assert store.get_current().refinement_id == v1.refinement_id
        assert store.get(v2.refinement_id).status == "rolled_back"

    def test_rollback_nonexistent(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        ok = store.rollback("nonexistent-id")
        assert not ok

    def test_list_refinements(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        store.create("a", "A")
        store.create("b", "B")
        store.create("c", "C")
        versions = store.list_refinements()
        assert len(versions) == 3

    def test_get(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        ver = store.create("get-test", "查询测试")
        fetched = store.get(ver.refinement_id)
        assert fetched is not None
        assert fetched.name == "get-test"


# ── SnapshotManager Tests ───────────────────────────────────────


class TestSnapshotManager:
    def test_snapshot_skill(self, tmp_path):
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()
        skill_dir = skills_dir / "test-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("# Test skill")

        manager = SnapshotManager(tmp_path / "snapshots")
        snap = manager.snapshot_skill("test-skill", skill_dir)
        assert snap.snap_id.startswith("snap-")
        assert snap.skill_name == "test-skill"
        assert "SKILL.md" in snap.files

    def test_restore_snapshot(self, tmp_path):
        manager = SnapshotManager(tmp_path / "snapshots")
        snap = SkillSnapshot(
            snap_id="snap-test",
            skill_name="test",
            files={},
            created_at=0.0,
            source_path=str(tmp_path / "target"),
        )
        manager._snapshots["snap-test"] = snap
        ok = manager.restore_snapshot("snap-test")
        assert ok

    def test_restore_nonexistent(self, tmp_path):
        manager = SnapshotManager(tmp_path / "snapshots")
        ok = manager.restore_snapshot("nonexistent")
        assert not ok

    def test_diff(self, tmp_path):
        manager = SnapshotManager(tmp_path / "snapshots")
        snap_a = SkillSnapshot(
            snap_id="snap-a",
            skill_name="test",
            files={"f1.txt": "hash1", "f2.txt": "hash2"},
            created_at=0.0,
            source_path="",
        )
        snap_b = SkillSnapshot(
            snap_id="snap-b",
            skill_name="test",
            files={"f1.txt": "hash1", "f3.txt": "hash3"},
            created_at=1.0,
            source_path="",
        )
        manager._snapshots["snap-a"] = snap_a
        manager._snapshots["snap-b"] = snap_b
        diff = manager.diff("snap-a", "snap-b")
        assert "f2.txt" in diff["removed"]
        assert "f3.txt" in diff["added"]
        assert "f1.txt" not in diff["modified"]


# ── AtomicRollback Tests ─────────────────────────────────────────


class TestAtomicRollback:
    def test_rollback_success(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        snaps = SnapshotManager(tmp_path / "snapshots")
        rollback = AtomicRollback(store, snaps)

        v1 = store.create("v1", "版本1")
        v2 = store.create("v2", "版本2")

        result = rollback.rollback_sync(v1.refinement_id)
        assert result["ok"]
        assert store.get_current().refinement_id == v1.refinement_id

    def test_rollback_nonexistent(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        snaps = SnapshotManager(tmp_path / "snapshots")
        rollback = AtomicRollback(store, snaps)

        result = rollback.rollback_sync("nonexistent")
        assert not result["ok"]

    def test_rollback_measures_latency(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        snaps = SnapshotManager(tmp_path / "snapshots")
        rollback = AtomicRollback(store, snaps)

        v1 = store.create("v1", "版本1")
        result = rollback.rollback_sync(v1.refinement_id)
        assert "elapsed_ms" in result
        assert result["elapsed_ms"] >= 0


# ── Singleton Tests ─────────────────────────────────────────────


class TestSingleton:
    def test_get_refinement_store_returns_same(self):
        reset_refinement_state()
        s1 = get_refinement_store()
        s2 = get_refinement_store()
        assert s1 is s2

    def test_reset_refinement_state(self):
        reset_refinement_state()
        s1 = get_refinement_store()
        reset_refinement_state()
        s2 = get_refinement_store()
        assert s1 is not s2


# ── Integration Tests ───────────────────────────────────────────


class TestIntegration:
    def test_full_refine_then_rollback(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        snaps = SnapshotManager(tmp_path / "snapshots")

        # 创建初始版本
        v1 = store.create("initial", "初始版本", skill_names=["skill-a"])

        # 模拟精炼（parent_id 自动设置为当前版本）
        v2 = store.create("refine-1", "第一次精炼", skill_names=["skill-a-fixed"])
        assert v2.parent_id == v1.refinement_id

        # 回滚
        rollback = AtomicRollback(store, snaps)
        result = rollback.rollback_sync(v1.refinement_id)
        assert result["ok"]
        assert store.get_current().refinement_id == v1.refinement_id

    def test_version_chain(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        v1 = store.create("v1", "V1")
        v2 = store.create("v2", "V2")
        v3 = store.create("v3", "V3")

        # 验证 parent_id 自动链接
        assert v2.parent_id == v1.refinement_id
        assert v3.parent_id == v2.refinement_id

        versions = store.list_refinements()
        assert len(versions) == 3
        current = store.get_current()
        assert current.refinement_id == v3.refinement_id


# ── Stress Tests ────────────────────────────────────────────────


class TestStress:
    def test_many_refinements(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        for i in range(100):
            store.create(f"ref-{i}", f"Refinement {i}")
        assert len(store.list_refinements()) == 100

    def test_rapid_rollback(self, tmp_path):
        store = RefinementStore(tmp_path / "refinements")
        snaps = SnapshotManager(tmp_path / "snapshots")
        rollback = AtomicRollback(store, snaps)

        versions = []
        for i in range(10):
            v = store.create(f"v{i}", f"V{i}")
            versions.append(v)

        for v in reversed(versions):
            result = rollback.rollback_sync(v.refinement_id)
            assert result["ok"]

    def test_concurrent_snapshots(self, tmp_path):
        import concurrent.futures
        manager = SnapshotManager(tmp_path / "snapshots")
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        def make_snapshot(i: int) -> str:
            skill_dir = skills_dir / f"skill-{i}"
            skill_dir.mkdir(exist_ok=True)
            (skill_dir / "SKILL.md").write_text(f"# Skill {i}")
            snap = manager.snapshot_skill(f"skill-{i}", skill_dir)
            return snap.snap_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(make_snapshot, i) for i in range(20)]
            snap_ids = [f.result() for f in futures]

        assert len(snap_ids) == 20
        assert len(set(snap_ids)) == 20
