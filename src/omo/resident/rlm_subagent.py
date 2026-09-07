#!/usr/bin/env python3
"""rlm_subagent — 异步递归子代理编排契约与 Low-Friction 低阻保护膜机制.

(BET-Y1Q4-T10-137)

基于 Python 异步调用的原生子代理派生契约:

- AsyncSubagentSpawner: 异步派生子代理, 独立沙箱执行, 强类型 Python 对象返回值
- LowFrictionMembrane: 低阻保护膜, 拦截网络抖动/超时/语法错误, 自动重试
- SubagentPool: 并发子代理管理, 硬限 <= 5
- RetryPolicy: 可重试 vs 不可重试错误分类

与 T10-136 rlm_kernel 集成: 子代理执行上下文自动注入 sandbox_locals.
"""

from __future__ import annotations

import asyncio
import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from omo.resident.rlm_kernel import RLMKernel, get_kernel


# ── Retry Policy ────────────────────────────────────────────────


class RetryCategory(enum.Enum):
    """错误分类."""
    RETRYABLE = "retryable"        # 网络抖动/超时 — 可重试
    NON_RETRYABLE = "non_retryable"  # 权限/幻觉/语法 — 不可重试


# 可重试错误关键词
_RETRYABLE_KEYWORDS = frozenset([
    "timeout", "connection", "network", "503", "502", "504",
    "unavailable", "reset", "refused", "temporarily",
])

# 不可重试错误类型
_NON_RETRYABLE_TYPES = frozenset([
    "PermissionError", "AuthenticationError", "SyntaxError",
    "ValidationError", "NotFoundError", "ValueError",
])


def classify_error(error: Exception) -> RetryCategory:
    """分类错误是否可重试."""
    type_name = type(error).__name__
    error_msg = str(error).lower()

    if type_name in _NON_RETRYABLE_TYPES:
        return RetryCategory.NON_RETRYABLE
    if any(kw in error_msg for kw in _RETRYABLE_KEYWORDS):
        return RetryCategory.RETRYABLE
    return RetryCategory.NON_RETRYABLE


# ── Retry Policy Config ─────────────────────────────────────────


@dataclass
class RetryPolicy:
    """重试策略配置."""
    max_attempts: int = 3
    base_delay: float = 0.1  # 100ms
    max_delay: float = 5.0   # 5s
    exponential_base: float = 2.0

    def delay_for(self, attempt: int) -> float:
        """计算退避延迟."""
        delay = self.base_delay * (self.exponential_base ** attempt)
        return min(delay, self.max_delay)


# ── Subagent Task ───────────────────────────────────────────────


@dataclass
class SubagentTask:
    """子代理任务."""
    name: str
    prompt: str
    args: dict[str, Any] = field(default_factory=dict)
    depth: int = 0
    max_depth: int = 3
    timeout_seconds: float = 30.0
    task_id: str = field(default_factory=lambda: f"sub-{uuid.uuid4().hex[:8]}")


@dataclass
class SubagentResult:
    """子代理执行结果."""
    task_id: str
    ok: bool
    output: Any = None
    error: str | None = None
    attempts: int = 1
    latency_ms: float = 0.0
    from_cache: bool = False


# ── Low-Friction Membrane ──────────────────────────────────────


class LowFrictionMembrane:
    """低阻保护膜 — 拦截基础设施异常并自动重试.

    隔离机架基础设施异常与模型认知失败:
    - 网络抖动/超时/503 → 自动重试
    - 权限拒绝/语法错误/模型幻觉 → 直接上报, 不重试
    """

    def __init__(self, policy: RetryPolicy | None = None) -> None:
        self.policy = policy or RetryPolicy()
        self._retry_counts: dict[str, int] = {}

    async def execute_with_retry(
        self,
        task_id: str,
        fn: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Any, int]:
        """带重试的执行. 返回 (result, attempts).

        Raises:
            最后一次异常（可重试或不可重试）
        """
        last_error: Exception | None = None
        for attempt in range(self.policy.max_attempts):
            try:
                if asyncio.iscoroutinefunction(fn):
                    result = await fn(*args, **kwargs)
                else:
                    result = fn(*args, **kwargs)
                return result, attempt + 1
            except Exception as e:
                last_error = e
                category = classify_error(e)
                if category == RetryCategory.NON_RETRYABLE:
                    raise
                if attempt < self.policy.max_attempts - 1:
                    delay = self.policy.delay_for(attempt)
                    await asyncio.sleep(delay)
        raise last_error  # type: ignore


# ── Async Subagent Spawner ─────────────────────────────────────


class AsyncSubagentSpawner:
    """异步子代理派生器.

    在独立沙箱中派生子代理:
    - 异步非阻塞
    - 强类型 Python 对象返回值
    - 自动注入 sandbox_locals
    - 递归深度硬限 <= 3
    """

    def __init__(
        self,
        kernel: RLMKernel | None = None,
        membrane: LowFrictionMembrane | None = None,
        max_depth: int = 3,
        max_concurrency: int = 5,
    ) -> None:
        self.kernel = kernel or get_kernel()
        self.membrane = membrane or LowFrictionMembrane()
        self.max_depth = max_depth
        self.max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._active_count = 0

    async def spawn(self, task: SubagentTask) -> SubagentResult:
        """异步派生子代理.

        Args:
            task: 子代理任务

        Returns:
            执行结果
        """
        start = time.monotonic()

        # 深度检查
        if task.depth > self.max_depth:
            return SubagentResult(
                task_id=task.task_id,
                ok=False,
                error=f"Max recursion depth ({self.max_depth}) exceeded",
                latency_ms=(time.monotonic() - start) * 1000,
            )

        # 并发限制
        async with self._semaphore:
            self._active_count += 1
            try:
                result = await self._run_task(task)
            finally:
                self._active_count -= 1

        latency = (time.monotonic() - start) * 1000
        result.latency_ms = latency
        return result

    async def _run_task(self, task: SubagentTask) -> SubagentResult:
        """执行子代理任务（带保护膜重试）."""
        try:
            output, attempts = await self.membrane.execute_with_retry(
                task.task_id,
                self._execute_in_sandbox,
                task,
            )
            return SubagentResult(
                task_id=task.task_id,
                ok=True,
                output=output,
                attempts=attempts,
            )
        except Exception as e:
            return SubagentResult(
                task_id=task.task_id,
                ok=False,
                error=f"{type(e).__name__}: {e}",
                attempts=self.membrane.policy.max_attempts,
            )

    async def _execute_in_sandbox(self, task: SubagentTask) -> Any:
        """在沙箱中执行任务（模拟）.

        实际实现中会:
        1. 创建隔离 worktree
        2. 注入 sandbox_locals
        3. 执行任务 prompt
        4. 捕获返回值
        5. 清理沙箱
        """
        # 模拟异步执行延迟
        await asyncio.sleep(0.01)

        # 使用 RLM kernel 执行上下文
        if task.args:
            for k, v in task.args.items():
                self.kernel.put(k, v)

        # 模拟执行结果
        result = {"task": task.name, "status": "completed", "depth": task.depth}

        # 注册结果到命名空间
        self.kernel.put(f"result_{task.task_id}", result)

        return result

    @property
    def active_count(self) -> int:
        """当前活跃子代理数."""
        return self._active_count


# ── Subagent Pool ──────────────────────────────────────────────


class SubagentPool:
    """并发子代理池.

    管理多个子代理的并发执行:
    - 并发数硬限 <= 5
    - 超出限制时排队
    - 任务完成后自动回收
    """

    def __init__(
        self,
        max_concurrency: int = 5,
        kernel: RLMKernel | None = None,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.kernel = kernel or get_kernel()
        self.spawner = AsyncSubagentSpawner(
            kernel=self.kernel,
            max_concurrency=max_concurrency,
        )
        self._results: list[SubagentResult] = []

    async def run_batch(self, tasks: list[SubagentTask]) -> list[SubagentResult]:
        """批量并发执行任务."""
        coros = [self.spawner.spawn(task) for task in tasks]
        results = await asyncio.gather(*coros, return_exceptions=False)
        self._results.extend(results)
        return results

    async def run_serial(self, tasks: list[SubagentTask]) -> list[SubagentTask]:
        """串行执行任务（降级模式）."""
        results = []
        for task in tasks:
            result = await self.spawner.spawn(task)
            results.append(result)
        return results

    def get_results(self) -> list[SubagentResult]:
        """获取所有结果."""
        return list(self._results)

    def clear_results(self) -> None:
        """清空结果."""
        self._results.clear()


# ── Module-Level Helpers ───────────────────────────────────────


async def spawn_subagent(
    name: str,
    prompt: str,
    args: dict[str, Any] | None = None,
    depth: int = 0,
) -> SubagentResult:
    """便捷函数: 派生子代理."""
    task = SubagentTask(
        name=name,
        prompt=prompt,
        args=args or {},
        depth=depth,
    )
    spawner = AsyncSubagentSpawner()
    return await spawner.spawn(task)


def run_async(coro: Any) -> Any:
    """运行异步协程的同步入口."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # 已在事件循环中, 创建新线程运行
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    else:
        return asyncio.run(coro)
