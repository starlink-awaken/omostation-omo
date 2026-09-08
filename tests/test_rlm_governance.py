#!/usr/bin/env python3
"""test_rlm_governance — RLM 变量命名空间 GC、资源核算与 GaC 安全门禁单元测试.

(BET-Y1Q4-T6-31)

覆盖:
- NamespaceGC (TTL/LRU/脏状态)
- ResourceAccountant (步数/Token/内存)
- ASTSecurityGate (危险调用/导入/属性)
- GaCGovernor (统一入口/治理)
- 集成测试
- 压力测试
"""

from __future__ import annotations

import asyncio

import pytest

from omo.resident.rlm_governance import (
    ASTSecurityGate,
    GaCGovernor,
    GCPolicy,
    GovernorMetrics,
    NamespaceGC,
    ResourceAccountant,
    ResourceExhaustedError,
    ResourceLimits,
    ResourceSnapshot,
    SecurityViolation,
    get_governor,
    reset_governor,
)
from omo.resident.rlm_kernel import RLMKernel, VariableStore, get_kernel, reset_kernel

# ── NamespaceGC Tests ───────────────────────────────────────────


class TestNamespaceGC:
    def test_should_gc_by_ttl(self):
        policy = GCPolicy(ttl_seconds=0.01)  # 10ms TTL
        gc = NamespaceGC(policy)
        store = VariableStore()
        store.set("old_var", "value")
        # 不立即触发
        assert not gc.should_gc(store)
        # 等待 TTL
        import time

        time.sleep(0.02)
        assert gc.should_gc(store)

    def test_should_gc_by_lru(self):
        policy = GCPolicy(ttl_seconds=99999, lru_threshold=3)
        gc = NamespaceGC(policy)
        store = VariableStore()
        for i in range(4):
            store.set(f"var_{i}", i)
        assert gc.should_gc(store)

    def test_run_gc_expire_by_ttl(self):
        policy = GCPolicy(ttl_seconds=0.01)
        gc = NamespaceGC(policy)
        store = VariableStore()
        store.set("temp", "data")
        import time

        time.sleep(0.02)
        result = asyncio.run(gc.run_gc(store))
        assert result["expired"] >= 1
        assert not store.has("temp")

    def test_run_gc_clean_dirty(self):
        policy = GCPolicy(ttl_seconds=99999, dirty_check=True)
        gc = NamespaceGC(policy)
        store = VariableStore()
        store.set("dirty", None)
        store.set("clean", "value")
        result = asyncio.run(gc.run_gc(store))
        assert result["dirty"] >= 1
        assert not store.has("dirty")
        assert store.has("clean")

    def test_reset_dirty_state(self):
        gc = NamespaceGC()
        store = VariableStore()
        store.set("dirty", None)
        assert gc.reset_dirty_state(store, "dirty")
        assert not store.has("dirty")

    def test_reset_clean_state_untouched(self):
        gc = NamespaceGC()
        store = VariableStore()
        store.set("clean", "value")
        assert not gc.reset_dirty_state(store, "clean")
        assert store.has("clean")


# ── ResourceAccountant Tests ────────────────────────────────────


class TestResourceAccountant:
    def test_record_step(self):
        accountant = ResourceAccountant()
        accountant.record_step()
        assert accountant._step_count == 1

    def test_record_step_limit(self):
        limits = ResourceLimits(max_steps=2)
        accountant = ResourceAccountant(limits)
        accountant.record_step()
        accountant.record_step()
        with pytest.raises(ResourceExhaustedError):
            accountant.record_step()

    def test_record_tokens(self):
        accountant = ResourceAccountant()
        accountant.record_tokens(100)
        assert accountant._token_consumed == 100

    def test_check_memory_ok(self):
        accountant = ResourceAccountant()
        assert accountant.check_memory("small value")

    def test_check_memory_exceeds(self):
        limits = ResourceLimits(max_variable_bytes=10)
        accountant = ResourceAccountant(limits)
        with pytest.raises(ResourceExhaustedError):
            accountant.check_memory("x" * 100)

    def test_snapshot(self):
        accountant = ResourceAccountant()
        store = VariableStore()
        store.set("x", 42)
        snap = accountant.snapshot(store)
        assert snap.variable_count == 1
        assert snap.step_count == 0

    def test_get_usage(self):
        accountant = ResourceAccountant()
        store = VariableStore()
        store.set("a", [1, 2, 3])
        usage = accountant.get_usage(store)
        assert usage["variables"] == 1
        assert "steps" in usage
        assert "tokens" in usage


# ── ASTSecurityGate Tests ───────────────────────────────────────


class TestASTSecurityGate:
    def test_safe_code(self):
        gate = ASTSecurityGate()
        report = gate.scan("x = 1 + 2\ny = [i for i in range(10)]")
        assert report.safe

    def test_dangerous_eval(self):
        gate = ASTSecurityGate()
        report = gate.scan("eval('1 + 1')")
        assert not report.safe
        assert any("eval" in v for v in report.violations)

    def test_dangerous_exec(self):
        gate = ASTSecurityGate()
        report = gate.scan("exec('print(1)')")
        assert not report.safe

    def test_dangerous_os_import(self):
        gate = ASTSecurityGate()
        report = gate.scan("import os")
        assert not report.safe
        assert any("os" in v for v in report.violations)

    def test_dangerous_subprocess(self):
        gate = ASTSecurityGate()
        report = gate.scan("from subprocess import run")
        assert not report.safe

    def test_dangerous_open(self):
        gate = ASTSecurityGate()
        report = gate.scan("open('/etc/passwd')")
        assert not report.safe

    def test_dangerous_attribute(self):
        gate = ASTSecurityGate()
        report = gate.scan("obj.system('ls')")
        assert not report.safe

    def test_whitelist_bypass(self):
        gate = ASTSecurityGate()
        gate.add_to_whitelist("eval")
        report = gate.scan("eval('1+1')")
        assert report.safe

    def test_syntax_error(self):
        gate = ASTSecurityGate()
        report = gate.scan("def foo(")
        assert not report.safe

    def test_multiple_violations(self):
        gate = ASTSecurityGate()
        report = gate.scan("import os; eval('1'); exec('2')")
        assert not report.safe
        assert len(report.violations) >= 2


# ── GaCGovernor Tests ───────────────────────────────────────────


class TestGaCGovernor:
    def test_governed_put(self):
        reset_kernel()
        governor = GaCGovernor()
        result = governor.governed_put("x", [1, 2, 3])
        assert result["name"] == "x"
        assert governor.store.has("x")

    def test_governed_put_memory_limit(self):
        reset_kernel()
        governor = GaCGovernor(limits=ResourceLimits(max_variable_bytes=10))
        with pytest.raises(ResourceExhaustedError):
            governor.governed_put("big", "x" * 100)

    def test_governed_execute_safe(self):
        reset_kernel()
        governor = GaCGovernor()
        result = governor.governed_execute("y = 42")
        assert result["error"] is None
        assert governor.store.has("y")

    def test_governed_execute_blocks_eval(self):
        reset_kernel()
        governor = GaCGovernor()
        with pytest.raises(SecurityViolation):
            governor.governed_execute("eval('1')")

    def test_governed_execute_blocks_import(self):
        reset_kernel()
        governor = GaCGovernor()
        with pytest.raises(SecurityViolation):
            governor.governed_execute("import os")

    def test_force_gc(self):
        reset_kernel()
        governor = GaCGovernor(gc_policy=GCPolicy(ttl_seconds=0.01))
        governor.governed_put("temp", "data")
        import time

        time.sleep(0.02)
        result = governor.force_gc()
        assert result["expired"] >= 1

    def test_get_metrics(self):
        reset_kernel()
        governor = GaCGovernor()
        governor.governed_put("a", 1)
        metrics = governor.get_metrics()
        assert metrics["resources"]["variables"] == 1
        assert "gc_runs" in metrics

    def test_gc_background(self):
        reset_kernel()
        governor = GaCGovernor(gc_policy=GCPolicy(ttl_seconds=0.01))
        governor.governed_put("temp", "data")

        async def run():
            task = asyncio.create_task(governor.start())
            await asyncio.sleep(0.05)
            governor.stop()
            try:
                await asyncio.wait_for(task, timeout=1.0)
            except (TimeoutError, asyncio.CancelledError):
                pass

        asyncio.run(run())
        assert governor.metrics.gc_runs >= 1


# ── Singleton Tests ─────────────────────────────────────────────


class TestSingleton:
    def test_get_governor_returns_same(self):
        reset_governor()
        g1 = get_governor()
        g2 = get_governor()
        assert g1 is g2

    def test_reset_governor(self):
        reset_governor()
        g1 = get_governor()
        reset_governor()
        g2 = get_governor()
        assert g1 is not g2


# ── Integration Tests ───────────────────────────────────────────


class TestIntegration:
    def test_full_workflow(self):
        reset_kernel()
        governor = GaCGovernor()

        # 1. 安全存储
        governor.governed_put("data", list(range(100)))

        # 2. 安全执行
        result = governor.governed_execute("filtered = [x for x in data if x > 50]")
        assert result["error"] is None
        assert governor.store.has("filtered")

        # 3. 危险代码被拦截
        with pytest.raises(SecurityViolation):
            governor.governed_execute("import os; os.system('ls')")

        # 4. 资源追踪
        metrics = governor.get_metrics()
        assert metrics["resources"]["variables"] >= 2
        assert metrics["security_violations"] == 1

    def test_gc_with_kernel_vars(self):
        reset_kernel()
        governor = GaCGovernor(gc_policy=GCPolicy(ttl_seconds=0.01))
        for i in range(10):
            governor.governed_put(f"var_{i}", f"value_{i}")
        import time

        time.sleep(0.02)
        result = governor.force_gc()
        assert result["expired"] == 10


# ── Stress Tests ────────────────────────────────────────────────


class TestStress:
    def test_mass_variable_gc(self):
        reset_kernel()
        governor = GaCGovernor(gc_policy=GCPolicy(ttl_seconds=0.01, lru_threshold=10000))
        for i in range(500):
            governor.governed_put(f"var_{i}", i)
        import time

        time.sleep(0.02)
        result = governor.force_gc()
        assert result["expired"] == 500

    def test_rapid_security_scans(self):
        gate = ASTSecurityGate()
        for _ in range(100):
            report = gate.scan("x = 1 + 2")
            assert report.safe

    def test_memory_tracking_accuracy(self):
        reset_kernel()
        governor = GaCGovernor()
        for i in range(50):
            governor.governed_put(f"var_{i}", "x" * 100)
        metrics = governor.get_metrics()
        assert metrics["resources"]["variables"] == 50
