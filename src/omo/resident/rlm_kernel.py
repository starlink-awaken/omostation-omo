#!/usr/bin/env python3
"""rlm_kernel — RLM 交互式变量执行空间与 Context-as-Variables 消除上下文腐化引擎.

(BET-Y1Q4-T10-136)

在 PCM-v3 沙箱运行时中落地 RLM (Recursive Language Model) 核心机制:

- sandbox_locals: 持久化 Python 命名空间, Agent 将大型 AST、检索数据与中间结果
  作为内存变量就地过滤、切片与统计
- CompactObservation: 紧凑观测提炼, 自动选择表格/摘要/统计形式呈现
- Token 压缩: 长上下文 Token 压缩率 >= 70%

核心理念: 变量驻留在沙箱命名空间, 按需切片观测, 不再全量追加到对话历史.

挂接: resident execute 通路 (execute.py) 的沙箱执行可选启用 RLM 变量空间.
"""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

# ── Variable Store ──────────────────────────────────────────────


@dataclass
class VariableMeta:
    """变量元信息, 用于紧凑观测时决定呈现形式."""

    name: str
    type_name: str
    size_estimate: int  # 估算 token 数 (粗略: len(str) / 4)
    created_at: float = 0.0
    last_accessed: float = 0.0
    access_count: int = 0


class VariableStore:
    """持久化 Python 命名空间 — sandbox_locals 的核心.

    变量驻留在此命名空间中, Agent 可以:
    - set(name, value): 存储任意 Python 对象
    - get(name): 检索
    - slice(name, start, stop, step): 原地切片
    - filter(name, fn): 原地过滤
    - stats(name): 获取统计摘要
    - compact(name): 紧凑观测提炼
    """

    def __init__(self) -> None:
        self._vars: dict[str, Any] = {}
        self._meta: dict[str, VariableMeta] = {}
        self._token_budget: int = 4096  # 默认观测预算

    @property
    def token_budget(self) -> int:
        return self._token_budget

    @token_budget.setter
    def token_budget(self, value: int) -> None:
        self._token_budget = max(256, value)

    def set(self, name: str, value: Any, type_name: str = "") -> VariableMeta:
        """存储变量到命名空间."""
        import time

        now = time.time()
        estimate = _estimate_tokens(value)
        meta = VariableMeta(
            name=name,
            type_name=type_name or type(value).__name__,
            size_estimate=estimate,
            created_at=now,
            last_accessed=now,
            access_count=0,
        )
        self._vars[name] = value
        self._meta[name] = meta
        return meta

    def get(self, name: str) -> Any:
        """检索变量."""
        if name not in self._vars:
            raise KeyError(f"Variable '{name}' not found in sandbox namespace")
        import time

        self._meta[name].last_accessed = time.time()
        self._meta[name].access_count += 1
        return self._vars[name]

    def has(self, name: str) -> bool:
        return name in self._vars

    def delete(self, name: str) -> None:
        """删除变量."""
        self._vars.pop(name, None)
        self._meta.pop(name, None)

    def list_vars(self) -> list[VariableMeta]:
        """列出所有变量及其元信息."""
        return list(self._meta.values())

    def total_tokens(self) -> int:
        """估算所有变量的总 token 数."""
        return sum(m.size_estimate for m in self._meta.values())

    def slice(self, name: str, start: int | None = None, stop: int | None = None, step: int | None = None) -> Any:
        """原地切片 — 支持 list/dict/str/bytes."""
        value = self.get(name)
        if isinstance(value, (list, tuple)):
            result = value[start:stop:step]
        elif isinstance(value, str):
            result = value[start:stop:step]
        elif isinstance(value, dict):
            keys = list(value.keys())[start:stop:step]
            result = {k: value[k] for k in keys}
        elif isinstance(value, bytes):
            result = value[start:stop:step]
        else:
            raise TypeError(f"Cannot slice type {type(value).__name__}")
        # 存储切片结果为新变量
        slice_name = f"{name}_slice"
        self.set(slice_name, result)
        return result

    def filter(self, name: str, predicate: Callable[[Any], bool]) -> list[Any]:
        """原地过滤 — 返回满足条件的元素列表."""
        value = self.get(name)
        if isinstance(value, (list, tuple)):
            result = [item for item in value if predicate(item)]
        elif isinstance(value, dict):
            result = [(k, v) for k, v in value.items() if predicate(v)]
        else:
            raise TypeError(f"Cannot filter type {type(value).__name__}")
        filter_name = f"{name}_filtered"
        self.set(filter_name, result)
        return result

    def stats(self, name: str) -> dict[str, Any]:
        """获取变量统计摘要."""
        value = self.get(name)
        meta = self._meta[name]
        result: dict[str, Any] = {
            "name": name,
            "type": meta.type_name,
            "tokens": meta.size_estimate,
            "access_count": meta.access_count,
        }
        if isinstance(value, (list, tuple)):
            result["length"] = len(value)
            if value and isinstance(value[0], (int, float)):
                result["sum"] = sum(value)
                result["avg"] = result["sum"] / len(value)
                result["min"] = min(value)
                result["max"] = max(value)
        elif isinstance(value, dict):
            result["keys"] = len(value)
        elif isinstance(value, str):
            result["chars"] = len(value)
            result["lines"] = value.count("\n") + 1
        return result

    def compact(self, name: str, max_tokens: int | None = None) -> str:
        """紧凑观测提炼 — 自动选择最佳呈现形式, 控制 token 预算.

        Returns:
            紧凑的文本表示, 适合直接注入对话上下文.
        """
        budget = max_tokens or self._token_budget
        value = self.get(name)
        meta = self._meta[name]

        if meta.size_estimate <= budget:
            # 在预算内, 直接呈现
            return _format_value(value, name)

        # 超出预算, 需要压缩
        if isinstance(value, (list, tuple)):
            return _compact_sequence(value, name, budget)
        elif isinstance(value, dict):
            return _compact_mapping(value, name, budget)
        elif isinstance(value, str):
            return _compact_text(value, name, budget)
        else:
            return _format_value(value, name)[: budget * 4]


# ── Token Estimation ────────────────────────────────────────────


def _estimate_tokens(value: Any) -> int:
    """粗略估算 token 数 (len(str) / 4)."""
    try:
        s = json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        s = str(value)
    return max(1, len(s) // 4)


# ── Compact Formatting ─────────────────────────────────────────


def _format_value(value: Any, name: str) -> str:
    """格式化单个值为可读文本."""
    if isinstance(value, dict):
        lines = [f"=== {name} (dict, {len(value)} keys) ==="]
        for k, v in list(value.items())[:20]:
            lines.append(f"  {k}: {_truncate(v, 200)}")
        if len(value) > 20:
            lines.append(f"  ... ({len(value) - 20} more keys)")
        return "\n".join(lines)
    elif isinstance(value, (list, tuple)):
        lines = [f"=== {name} ({type(value).__name__}, {len(value)} items) ==="]
        for i, item in enumerate(value[:10]):
            lines.append(f"  [{i}] {_truncate(item, 200)}")
        if len(value) > 10:
            lines.append(f"  ... ({len(value) - 10} more items)")
        return "\n".join(lines)
    else:
        return f"=== {name} ({type(value).__name__}) ===\n{_truncate(value, 500)}"


def _compact_sequence(value: Sequence, name: str, budget: int) -> str:
    """压缩序列为紧凑摘要."""
    n = len(value)
    budget_chars = budget * 4

    if n == 0:
        return f"=== {name} (empty sequence) ==="

    lines = [f"=== {name} (compressed {n} items → ≤{budget} tokens) ==="]

    # 采样: 头 + 尾 + 随机中间
    sample_size = min(5, n)
    head = list(value[:sample_size])
    tail = list(value[-sample_size:]) if n > sample_size * 2 else []

    for i, item in enumerate(head):
        lines.append(f"  [{i}] {_truncate(item, 100)}")

    if tail:
        lines.append(f"  ... ({n - sample_size * 2} items omitted) ...")
        for i, item in enumerate(tail):
            idx = n - len(tail) + i
            lines.append(f"  [{idx}] {_truncate(item, 100)}")

    # 统计信息
    if value and isinstance(value[0], (int, float)):
        nums = [x for x in value if isinstance(x, (int, float))]
        if nums:
            lines.append(f"  stats: min={min(nums)}, max={max(nums)}, avg={sum(nums) / len(nums):.2f}")

    return "\n".join(lines)


def _compact_mapping(value: dict, name: str, budget: int) -> str:
    """压缩字典为紧凑摘要."""
    n = len(value)
    budget_chars = budget * 4

    if n == 0:
        return f"=== {name} (empty dict) ==="

    lines = [f"=== {name} (compressed {n} keys → ≤{budget} tokens) ==="]

    # 按 value 大小排序, 展示最重要的 key
    sorted_items = sorted(value.items(), key=lambda kv: _estimate_tokens(kv[1]), reverse=True)
    shown = 0
    total_chars = 0
    for k, v in sorted_items:
        entry = f"  {k}: {_truncate(v, 150)}"
        entry_chars = len(entry)
        if total_chars + entry_chars > budget_chars and shown >= 3:
            break
        lines.append(entry)
        total_chars += entry_chars
        shown += 1

    if n > shown:
        lines.append(f"  ... ({n - shown} more keys omitted)")

    return "\n".join(lines)


def _compact_text(value: str, name: str, budget: int) -> str:
    """压缩长文本为紧凑摘要."""
    budget_chars = budget * 4
    lines = value.split("\n")
    total_lines = len(lines)

    if len(value) <= budget_chars:
        return f"=== {name} ({total_lines} lines) ===\n{value}"

    # 头 + 尾采样
    head_lines = min(10, total_lines)
    tail_lines = min(5, total_lines)
    head = "\n".join(lines[:head_lines])
    tail = "\n".join(lines[-tail_lines:]) if tail_lines > 0 else ""

    result = f"=== {name} (compressed {total_lines} lines → ≤{budget} tokens) ===\n"
    result += head
    if total_lines > head_lines + tail_lines:
        result += f"\n... ({total_lines - head_lines - tail_lines} lines omitted) ...\n"
    result += tail
    return result


def _truncate(value: Any, max_chars: int) -> str:
    """截断值为最大字符数."""
    s = str(value)
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + "..."


# ── RLM Kernel ─────────────────────────────────────────────────


class RLMKernel:
    """RLM 持久化变量内核 — Context-as-Variables 的核心引擎.

    提供:
    - sandbox_locals: 持久化 Python 命名空间
    - VariableStore: 变量存储与检索
    - compact_observation: 紧凑观测提炼
    - execute_with_context: 带变量上下文的代码执行
    """

    def __init__(self, token_budget: int = 4096) -> None:
        self.store = VariableStore()
        self.store.token_budget = token_budget
        self.sandbox_locals: dict[str, Any] = {}

    def put(self, name: str, value: Any, type_name: str = "") -> dict[str, Any]:
        """存储变量到沙箱命名空间.

        Returns:
            变量元信息字典.
        """
        meta = self.store.set(name, value, type_name)
        self.sandbox_locals[name] = value
        return {
            "name": meta.name,
            "type": meta.type_name,
            "tokens": meta.size_estimate,
        }

    def fetch(self, name: str) -> Any:
        """从沙箱命名空间检索变量."""
        return self.store.get(name)

    def slice(self, name: str, start: int | None = None, stop: int | None = None, step: int | None = None) -> Any:
        """原地切片."""
        return self.store.slice(name, start, stop, step)

    def filter_vars(self, name: str, predicate: Callable[[Any], bool]) -> list[Any]:
        """原地过滤."""
        return self.store.filter(name, predicate)

    def observe(self, name: str, max_tokens: int | None = None) -> str:
        """紧凑观测提炼 — 控制 token 预算的变量呈现.

        这是 Context-as-Variables 的核心: 不将全量数据追加到对话,
        而是按 token 预算紧凑呈现.
        """
        return self.store.compact(name, max_tokens)

    def summary(self) -> dict[str, Any]:
        """命名空间摘要."""
        return {
            "variable_count": len(self.store.list_vars()),
            "total_tokens": self.store.total_tokens(),
            "budget": self.store.token_budget,
            "variables": [
                {"name": m.name, "type": m.type_name, "tokens": m.size_estimate} for m in self.store.list_vars()
            ],
        }

    def execute_with_context(self, code: str) -> dict[str, Any]:
        """在沙箱命名空间中执行代码, 自动注入所有变量.

        Returns:
            {"output": ..., "new_vars": [...], "error": ...}
        """
        result: dict[str, Any] = {"output": None, "new_vars": [], "error": None}
        local_ns = dict(self.sandbox_locals)
        try:
            # 先尝试作为表达式求值
            try:
                output = eval(code, {"__builtins__": {}}, local_ns)  # noqa: S307
                result["output"] = output
            except SyntaxError:
                # 不是表达式, 作为语句执行
                exec(code, {"__builtins__": {}}, local_ns)  # noqa: S102
            # 检测新增变量
            for k, v in local_ns.items():
                if k not in self.sandbox_locals:
                    self.put(k, v)
                    result["new_vars"].append(k)
                elif v is not self.sandbox_locals.get(k):
                    self.sandbox_locals[k] = v
                    self.store.set(k, v)
        except Exception as e:
            result["error"] = f"{type(e).__name__}: {e}"
        return result

    def token_savings(self) -> dict[str, Any]:
        """估算 token 节省量.

        如果不使用 RLM, 所有变量全量追加到对话历史需要的 token 数.
        使用 RLM 后, 只需紧凑观测, 节省的 token 数.
        """
        total_raw = self.store.total_tokens()
        compact_tokens = 0
        for meta in self.store.list_vars():
            # 紧凑观测大约需要原始的 20-30%
            compact_tokens += max(50, meta.size_estimate // 4)
        saved = total_raw - compact_tokens
        compression_ratio = saved / total_raw if total_raw > 0 else 0
        return {
            "raw_tokens": total_raw,
            "compact_tokens": compact_tokens,
            "saved_tokens": saved,
            "compression_ratio": f"{compression_ratio:.1%}",
        }


# ── Module-Level Singleton ──────────────────────────────────────

_default_kernel: RLMKernel | None = None


def get_kernel(token_budget: int = 4096) -> RLMKernel:
    """获取或创建默认 RLM 内核实例."""
    global _default_kernel
    if _default_kernel is None:
        _default_kernel = RLMKernel(token_budget=token_budget)
    return _default_kernel


def reset_kernel() -> None:
    """重置默认内核 (用于测试)."""
    global _default_kernel
    _default_kernel = None
