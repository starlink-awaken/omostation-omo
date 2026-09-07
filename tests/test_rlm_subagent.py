#!/usr/bin/env python3
"""test_rlm_subagent — 异步递归子代理编排与低阻保护膜单元测试.

(BET-Y1Q4-T10-137)

覆盖:
- RetryPolicy / classify_error
- LowFrictionMembrane (retry / non-retry)
- AsyncSubagentSpawner (spawn / depth limit / concurrency)
- SubagentPool (batch / serial)
- 集成: rlm_kernel 上下文注入
- 压力测试: 并发/递归
"""

from __future__ import annotations

import asyncio
import pytest

from omo.resident.rlm_kernel import RLMKernel, VariableStore, get_kernel, reset_kernel
from omo.resident.rlm_subagent import (
    AsyncSubagentSpawner,
    LowFrictionMembrane,
    RetryCategory,
    RetryPolicy,
    SubagentPool,
    SubagentResult,
    SubagentTask,
    classify_error,
    run_async,
    spawn_subagent,
)


# ── RetryPolicy Tests ───────────────────────────────────────────


class TestRetryPolicy:
    def test_delay_exponential(self):
        policy = RetryPolicy(base_delay=0.1, exponential_base=2.0)
        assert policy.delay_for(0) == pytest.approx(0.1)
        assert policy.delay_for(1) == pytest.approx(0.2)
        assert policy.delay_for(2) == pytest.approx(0.4)

    def test_delay_capped_at_max(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0, exponential_base=10.0)
        assert policy.delay_for(0) == pytest.approx(1.0)
        assert policy.delay_for(1) == pytest.approx(5.0)  # capped
        assert policy.delay_for(2) == pytest.approx(5.0)  # capped

    def test_default_config(self):
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.base_delay == pytest.approx(0.1)


# ── classify_error Tests ────────────────────────────────────────


class TestClassifyError:
    def test_retryable_timeout(self):
        err = TimeoutError("connection timeout")
        assert classify_error(err) == RetryCategory.RETRYABLE

    def test_retryable_network(self):
        err = ConnectionError("network reset")
        assert classify_error(err) == RetryCategory.RETRYABLE

    def test_retryable_503(self):
        err = RuntimeError("503 service unavailable")
        assert classify_error(err) == RetryCategory.RETRYABLE

    def test_non_retryable_permission(self):
        err = PermissionError("denied")
        assert classify_error(err) == RetryCategory.NON_RETRYABLE

    def test_non_retryable_syntax(self):
        err = SyntaxError("invalid syntax")
        assert classify_error(err) == RetryCategory.NON_RETRYABLE

    def test_non_retryable_generic(self):
        err = RuntimeError("unknown error")
        assert classify_error(err) == RetryCategory.NON_RETRYABLE


# ── LowFrictionMembrane Tests ──────────────────────────────────


class TestLowFrictionMembrane:
    @pytest.mark.asyncio
    async def test_execute_success_no_retry(self):
        membrane = LowFrictionMembrane()

        async def fn():
            return "ok"

        result, attempts = await membrane.execute_with_retry("test", fn)
        assert result == "ok"
        assert attempts == 1

    @pytest.mark.asyncio
    async def test_execute_retry_then_success(self):
        membrane = LowFrictionMembrane(
            RetryPolicy(max_attempts=3, base_delay=0.01)
        )
        call_count = 0

        async def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("timeout")
            return "recovered"

        result, attempts = await membrane.execute_with_retry("test", fn)
        assert result == "recovered"
        assert attempts == 3

    @pytest.mark.asyncio
    async def test_execute_non_retryable_raises_immediately(self):
        membrane = LowFrictionMembrane(
            RetryPolicy(max_attempts=3, base_delay=0.01)
        )

        async def fn():
            raise PermissionError("access denied")

        with pytest.raises(PermissionError):
            await membrane.execute_with_retry("test", fn)

    @pytest.mark.asyncio
    async def test_execute_max_attempts_exhausted(self):
        membrane = LowFrictionMembrane(
            RetryPolicy(max_attempts=2, base_delay=0.01)
        )

        async def fn():
            raise TimeoutError("always timeout")

        with pytest.raises(TimeoutError):
            await membrane.execute_with_retry("test", fn)


# ── AsyncSubagentSpawner Tests ──────────────────────────────────


class TestAsyncSubagentSpawner:
    @pytest.mark.asyncio
    async def test_spawn_basic(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner()
        task = SubagentTask(name="test", prompt="do something")
        result = await spawner.spawn(task)
        assert result.ok
        assert result.output is not None
        assert result.output["task"] == "test"

    @pytest.mark.asyncio
    async def test_spawn_registers_result_in_kernel(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner()
        task = SubagentTask(name="test", prompt="do something")
        result = await spawner.spawn(task)
        # 结果应注册到 kernel store
        assert spawner.kernel.store.has(f"result_{result.task_id}")

    @pytest.mark.asyncio
    async def test_depth_limit(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner(max_depth=2)
        task = SubagentTask(name="deep", prompt="recurse", depth=3)
        result = await spawner.spawn(task)
        assert not result.ok
        assert "Max recursion depth" in result.error

    @pytest.mark.asyncio
    async def test_concurrent_spawn(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner(max_concurrency=3)
        tasks = [SubagentTask(name=f"t{i}", prompt=f"task {i}") for i in range(5)]
        results = await asyncio.gather(*[spawner.spawn(t) for t in tasks])
        assert all(r.ok for r in results)
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_latency_tracking(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner()
        task = SubagentTask(name="latency", prompt="measure")
        result = await spawner.spawn(task)
        assert result.latency_ms > 0

    @pytest.mark.asyncio
    async def test_active_count(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner()
        assert spawner.active_count == 0
        task = SubagentTask(name="count", prompt="test")
        await spawner.spawn(task)
        assert spawner.active_count == 0  # completed


# ── SubagentPool Tests ──────────────────────────────────────────


class TestSubagentPool:
    @pytest.mark.asyncio
    async def test_run_batch(self):
        reset_kernel()
        pool = SubagentPool(max_concurrency=3)
        tasks = [SubagentTask(name=f"batch_{i}", prompt=f"batch {i}") for i in range(5)]
        results = await pool.run_batch(tasks)
        assert len(results) == 5
        assert all(r.ok for r in results)

    @pytest.mark.asyncio
    async def test_run_serial(self):
        reset_kernel()
        pool = SubagentPool()
        tasks = [SubagentTask(name=f"serial_{i}", prompt=f"serial {i}") for i in range(3)]
        results = await pool.run_serial(tasks)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_get_results(self):
        reset_kernel()
        pool = SubagentPool()
        tasks = [SubagentTask(name="r1", prompt="t1"), SubagentTask(name="r2", prompt="t2")]
        await pool.run_batch(tasks)
        results = pool.get_results()
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_clear_results(self):
        reset_kernel()
        pool = SubagentPool()
        tasks = [SubagentTask(name="c1", prompt="t1")]
        await pool.run_batch(tasks)
        pool.clear_results()
        assert len(pool.get_results()) == 0


# ── Integration Tests ───────────────────────────────────────────


class TestIntegration:
    @pytest.mark.asyncio
    async def test_spawn_with_kernel_args(self):
        reset_kernel()
        spawner = AsyncSubagentSpawner()
        task = SubagentTask(
            name="with_args",
            prompt="use args",
            args={"data": [1, 2, 3], "label": "test"},
        )
        result = await spawner.spawn(task)
        assert result.ok
        # 检查 args 注入到 kernel store
        assert spawner.kernel.store.has("data")
        assert spawner.kernel.store.has("label")

    @pytest.mark.asyncio
    async def test_spawn_subagent_helper(self):
        reset_kernel()
        result = await spawn_subagent("helper", "test prompt", {"x": 42})
        assert result.ok
        assert result.attempts >= 1

    def test_run_async_entry(self):
        reset_kernel()
        result = run_async(spawn_subagent("sync_entry", "sync test"))
        assert result.ok


# ── Stress Tests ────────────────────────────────────────────────


class TestStress:
    @pytest.mark.asyncio
    async def test_many_concurrent_tasks(self):
        """压力测试: 50 并发任务."""
        reset_kernel()
        pool = SubagentPool(max_concurrency=5)
        tasks = [SubagentTask(name=f"stress_{i}", prompt=f"stress {i}") for i in range(50)]
        results = await pool.run_batch(tasks)
        assert len(results) == 50
        assert all(r.ok for r in results)

    @pytest.mark.asyncio
    async def test_deep_recursion_blocked(self):
        """压力测试: 递归深度限制生效."""
        reset_kernel()
        spawner = AsyncSubagentSpawner(max_depth=1)
        task = SubagentTask(name="deep", prompt="recurse", depth=2)
        result = await spawner.spawn(task)
        assert not result.ok
        assert "Max recursion depth" in result.error

    @pytest.mark.asyncio
    async def test_rapid_spawn_cycle(self):
        """压力测试: 快速 spawn 循环."""
        reset_kernel()
        spawner = AsyncSubagentSpawner(max_concurrency=5)
        for i in range(20):
            task = SubagentTask(name=f"rapid_{i}", prompt=f"rapid {i}")
            result = await spawner.spawn(task)
            assert result.ok
