#!/usr/bin/env python3
"""rlm_governance — RLM 变量命名空间生命周期 GC、资源核算与 GaC 安全门禁.

(BET-Y1Q4-T6-31)

弥补原生 RLM 权限裸奔与长周期命名空间泄漏短板:

- NamespaceGC: 基于 TTL/LRU 的自动 GC 与脏状态重置
- ResourceAccountant: 步数/Token/内存硬核算体系
- ASTSecurityGate: AST 静态安全审查, 拦截破坏性系统调用
- GaCGovernor: 统一治理入口

与 T10-136 rlm_kernel 集成: 治理自动启用.
"""

from __future__ import annotations

import ast
import asyncio
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from omo.resident.rlm_kernel import RLMKernel, VariableStore, get_kernel, reset_kernel

# ── Resource Limits ─────────────────────────────────────────────


@dataclass
class ResourceLimits:
    """资源限制配置."""
    max_variable_bytes: int = 10 * 1024 * 1024    # 10MB per variable
    max_namespace_bytes: int = 100 * 1024 * 1024  # 100MB total
    max_token_budget: int = 8192                   # default token budget
    max_steps: int = 100000                         # max execution steps
    gc_ttl_seconds: float = 3600.0                 # 1 hour default TTL


# ── Resource Accountant ─────────────────────────────────────────


@dataclass
class ResourceSnapshot:
    """资源快照."""
    step_count: int
    token_consumed: int
    memory_bytes: int
    variable_count: int
    timestamp: float


class ResourceAccountant:
    """资源核算 — 步数/Token/内存硬核算."""

    def __init__(self, limits: ResourceLimits | None = None) -> None:
        self.limits = limits or ResourceLimits()
        self._step_count = 0
        self._token_consumed = 0
        self._snapshots: list[ResourceSnapshot] = []

    def record_step(self, tokens: int = 0) -> None:
        """记录执行步数."""
        self._step_count += 1
        self._token_consumed += tokens
        if self._step_count > self.limits.max_steps:
            raise ResourceExhaustedError(
                f"Step limit exceeded: {self._step_count}/{self.limits.max_steps}"
            )

    def record_tokens(self, tokens: int) -> None:
        """记录 Token 消耗."""
        self._token_consumed += tokens

    def check_memory(self, value: Any) -> bool:
        """检查变量内存是否超限."""
        size = sys.getsizeof(value)
        if size > self.limits.max_variable_bytes:
            raise ResourceExhaustedError(
                f"Variable size {size} bytes exceeds limit {self.limits.max_variable_bytes}"
            )
        return True

    def snapshot(self, store: VariableStore) -> ResourceSnapshot:
        """创建资源快照."""
        snap = ResourceSnapshot(
            step_count=self._step_count,
            token_consumed=self._token_consumed,
            memory_bytes=self._estimate_memory(store),
            variable_count=len(store.list_vars()),
            timestamp=time.time(),
        )
        self._snapshots.append(snap)
        return snap

    def _estimate_memory(self, store: VariableStore) -> int:
        """估算总内存占用."""
        total = 0
        for meta in store.list_vars():
            total += meta.size_estimate * 4  # rough estimate
        return total

    def get_usage(self, store: VariableStore) -> dict[str, Any]:
        """获取资源使用报告."""
        return {
            "steps": self._step_count,
            "tokens": self._token_consumed,
            "memory_bytes": self._estimate_memory(store),
            "variables": len(store.list_vars()),
            "step_limit": self.limits.max_steps,
            "token_budget": self.limits.max_token_budget,
            "memory_limit": self.limits.max_variable_bytes,
        }


class ResourceExhaustedError(Exception):
    """资源耗尽错误."""
    pass


# ── Namespace GC ────────────────────────────────────────────────


@dataclass
class GCPolicy:
    """GC 策略配置."""
    ttl_seconds: float = 3600.0     # 1 hour
    lru_threshold: int = 1000       # trigger GC when vars exceed this
    dirty_check: bool = True        # enable dirty state detection


class NamespaceGC:
    """命名空间垃圾回收 — TTL/LRU/脏状态."""

    def __init__(self, policy: GCPolicy | None = None) -> None:
        self.policy = policy or GCPolicy()
        self._last_gc = time.time()
        self._gc_count = 0
        self._reclaimed = 0

    def should_gc(self, store: VariableStore) -> bool:
        """判断是否需要 GC."""
        now = time.time()
        vars = store.list_vars()
        # TTL check
        if now - self._last_gc > self.policy.ttl_seconds:
            return True
        # LRU check
        if len(vars) > self.policy.lru_threshold:
            return True
        return False

    async def run_gc(self, store: VariableStore) -> dict[str, Any]:
        """执行 GC. 返回回收统计."""
        start = time.time()
        before = len(store.list_vars())

        # 1. TTL 过期清理
        expired = self._expire_by_ttl(store)

        # 2. 脏状态清理
        dirty = 0
        if self.policy.dirty_check:
            dirty = self._clean_dirty(store)

        self._last_gc = time.time()
        self._gc_count += 1

        elapsed = self._last_gc - start
        after = len(store.list_vars())
        reclaimed = before - after

        return {
            "gc_count": self._gc_count,
            "before": before,
            "after": after,
            "reclaimed": reclaimed,
            "expired": expired,
            "dirty": dirty,
            "elapsed_ms": elapsed * 1000,
        }

    def _expire_by_ttl(self, store: VariableStore) -> int:
        """清理过期变量."""
        now = time.time()
        expired = 0
        for meta in store.list_vars():
            if now - meta.last_accessed > self.policy.ttl_seconds:
                store.delete(meta.name)
                expired += 1
        return expired

    def _clean_dirty(self, store: VariableStore) -> int:
        """清理脏状态变量."""
        dirty = 0
        for meta in store.list_vars():
            val = store.get(meta.name)
            # 检查 None 值（常见脏状态）
            if val is None:
                store.delete(meta.name)
                dirty += 1
        return dirty

    def reset_dirty_state(self, store: VariableStore, name: str) -> bool:
        """重置单个变量的脏状态."""
        try:
            val = store.get(name)
            if val is None:
                store.delete(name)
                return True
        except KeyError:
            pass
        return False


# ── AST Security Gate ───────────────────────────────────────────


# 危险的内置函数和属性
_DANGEROUS_BUILTINS = frozenset([
    "eval", "exec", "__import__", "compile", "open", "input",
    "globals", "locals", "vars", "dir", "getattr", "setattr", "delattr",
    "breakpoint", "exit", "quit",
])

_DANGEROUS_MODULES = frozenset([
    "os", "sys", "subprocess", "shutil", "pathlib", "importlib",
    "socket", "http", "urllib", "ftplib", "telnetlib",
    "pickle", "shelve", "marshal",
    "ctypes", "multiprocessing", "threading",
])

_DANGEROUS_ATTRS = frozenset([
    "system", "popen", "spawn", "fork", "kill",
    "remove", "unlink", "rmdir", "rename",
    "write", "read", "send", "recv",
    "__class__", "__bases__", "__subclasses__", "__globals__",
])


class SecurityViolationError(Exception):
    """安全违规错误."""
    pass


@dataclass
class SecurityReport:
    """安全审查报告."""
    safe: bool
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class ASTSecurityGate:
    """AST 静态安全审查门禁."""

    def __init__(self) -> None:
        self._whitelist: set[str] = set()

    def add_to_whitelist(self, name: str) -> None:
        """添加白名单."""
        self._whitelist.add(name)

    def scan(self, code: str) -> SecurityReport:
        """扫描代码安全性."""
        violations: list[str] = []
        warnings: list[str] = []

        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return SecurityReport(
                safe=False,
                violations=[f"Syntax error: {e}"],
            )

        for node in ast.walk(tree):
            self._check_node(node, violations, warnings)

        return SecurityReport(
            safe=len(violations) == 0,
            violations=violations,
            warnings=warnings,
        )

    def _check_node(self, node: ast.AST, violations: list[str], warnings: list[str]) -> None:
        """检查单个 AST 节点."""
        if isinstance(node, ast.Call):
            self._check_call(node, violations, warnings)
        elif isinstance(node, ast.Import):
            self._check_import(node, violations, warnings)
        elif isinstance(node, ast.ImportFrom):
            self._check_import_from(node, violations, warnings)

    def _check_call(self, node: ast.Call, violations: list[str], warnings: list[str]) -> None:
        """检查函数调用."""
        func_name = self._get_func_name(node.func)
        if func_name is None:
            return

        base = func_name.split(".")[0]
        if base in self._whitelist:
            return

        if func_name in _DANGEROUS_BUILTINS or base in _DANGEROUS_BUILTINS:
            violations.append(f"Dangerous builtin call: {func_name}")
        elif "." in func_name:
            attr = func_name.split(".")[-1]
            if attr in _DANGEROUS_ATTRS:
                violations.append(f"Dangerous attribute access: {func_name}")

    def _check_import(self, node: ast.Import, violations: list[str], warnings: list[str]) -> None:
        """检查 import."""
        for alias in node.names:
            base = alias.name.split(".")[0]
            if base in _DANGEROUS_MODULES:
                violations.append(f"Dangerous import: {alias.name}")

    def _check_import_from(self, node: ast.ImportFrom, violations: list[str], warnings: list[str]) -> None:
        """检查 from import."""
        if node.module:
            base = node.module.split(".")[0]
            if base in _DANGEROUS_MODULES:
                violations.append(f"Dangerous import from: {node.module}")

    def _get_func_name(self, node: ast.expr) -> str | None:
        """获取函数名."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            value = self._get_func_name(node.value)
            if value:
                return f"{value}.{node.attr}"
        return None


# ── GaC Governor ────────────────────────────────────────────────


@dataclass
class GovernorMetrics:
    """治理指标."""
    gc_runs: int = 0
    variables_reclaimed: int = 0
    security_violations: int = 0
    resource_errors: int = 0


class GaCGovernor:
    """GaC 统一治理入口.

    集成 GC + 资源核算 + AST 安全门禁.
    """

    def __init__(
        self,
        kernel: RLMKernel | None = None,
        limits: ResourceLimits | None = None,
        gc_policy: GCPolicy | None = None,
    ) -> None:
        self.kernel = kernel or get_kernel()
        self.accountant = ResourceAccountant(limits)
        self.gc = NamespaceGC(gc_policy)
        self.gate = ASTSecurityGate()
        self.metrics = GovernorMetrics()
        self._running = False

    async def start(self) -> None:
        """启动后台 GC 循环."""
        self._running = True
        while self._running:
            await asyncio.sleep(self.gc.policy.ttl_seconds / 2)
            if self.gc.should_gc(self.kernel.store):
                result = await self.gc.run_gc(self.kernel.store)
                self.metrics.gc_runs += 1
                self.metrics.variables_reclaimed += result["reclaimed"]

    def stop(self) -> None:
        """停止后台 GC."""
        self._running = False

    def governed_put(self, name: str, value: Any) -> dict[str, Any]:
        """带治理的变量存储."""
        # 1. 内存核算
        self.accountant.check_memory(value)
        # 2. 步数记录
        self.accountant.record_step(tokens=len(str(value)) // 4)
        # 3. 执行存储
        result = self.kernel.put(name, value)
        return result

    def governed_execute(self, code: str) -> dict[str, Any]:
        """带治理的代码执行."""
        # 1. AST 安全扫描
        report = self.gate.scan(code)
        if not report.safe:
            self.metrics.security_violations += 1
            raise SecurityViolationError(
                f"Security violation: {'; '.join(report.violations)}"
            )
        # 2. 资源记录
        self.accountant.record_step(tokens=len(code) // 4)
        # 3. 执行
        result = self.kernel.execute_with_context(code)
        if result.get("error"):
            self.metrics.resource_errors += 1
        return result

    def force_gc(self) -> dict[str, Any]:
        """强制 GC."""
        result = asyncio.run(self.gc.run_gc(self.kernel.store))
        self.metrics.gc_runs += 1
        self.metrics.variables_reclaimed += result["reclaimed"]
        return result

    def get_metrics(self) -> dict[str, Any]:
        """获取治理指标."""
        return {
            "gc_runs": self.metrics.gc_runs,
            "variables_reclaimed": self.metrics.variables_reclaimed,
            "security_violations": self.metrics.security_violations,
            "resource_errors": self.metrics.resource_errors,
            "resources": self.accountant.get_usage(self.kernel.store),
            "kernel": self.kernel.summary(),
        }

    @property
    def store(self) -> VariableStore:
        """便捷访问 store."""
        return self.kernel.store


# ── Module-Level Helper ─────────────────────────────────────────


_default_governor: GaCGovernor | None = None


def get_governor() -> GaCGovernor:
    """获取默认治理器."""
    global _default_governor
    if _default_governor is None:
        _default_governor = GaCGovernor()
    return _default_governor


def reset_governor() -> None:
    """重置治理器."""
    global _default_governor
    _default_governor = None
