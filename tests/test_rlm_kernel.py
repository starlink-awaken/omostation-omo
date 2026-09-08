#!/usr/bin/env python3
"""test_rlm_kernel — RLM 交互式变量执行空间单元测试与压力测试.

(BET-Y1Q4-T10-136)

覆盖:
- VariableStore CRUD
- 切片/过滤/统计/紧凑观测
- RLMKernel 集成
- Token 压缩率验证 (>= 70%)
- 压力测试: 大量变量/大对象
"""

from __future__ import annotations

import json

import pytest

from omo.resident.rlm_kernel import (
    RLMKernel,
    VariableMeta,
    VariableStore,
    _estimate_tokens,
    get_kernel,
    reset_kernel,
)

# ── VariableStore Tests ─────────────────────────────────────────


class TestVariableStore:
    def test_set_and_get(self):
        store = VariableStore()
        store.set("x", 42)
        assert store.get("x") == 42

    def test_has(self):
        store = VariableStore()
        assert not store.has("x")
        store.set("x", 1)
        assert store.has("x")

    def test_delete(self):
        store = VariableStore()
        store.set("x", 1)
        store.delete("x")
        assert not store.has("x")

    def test_get_missing_raises(self):
        store = VariableStore()
        with pytest.raises(KeyError, match="not found"):
            store.get("nonexistent")

    def test_list_vars(self):
        store = VariableStore()
        store.set("a", [1, 2, 3], type_name="list")
        store.set("b", {"k": "v"}, type_name="dict")
        vars = store.list_vars()
        assert len(vars) == 2
        names = {v.name for v in vars}
        assert names == {"a", "b"}

    def test_total_tokens(self):
        store = VariableStore()
        store.set("x", "hello world")  # 11 chars ≈ 2 tokens
        store.set("y", list(range(100)))  # list of 100 ints
        total = store.total_tokens()
        assert total > 0

    def test_meta_tracking(self):
        store = VariableStore()
        store.set("x", 42)
        meta = store.list_vars()[0]
        assert meta.name == "x"
        assert meta.type_name == "int"
        assert meta.access_count == 0
        store.get("x")
        meta = store.list_vars()[0]
        assert meta.access_count == 1


# ── Slicing Tests ──────────────────────────────────────────────


class TestSlicing:
    def test_list_slice(self):
        store = VariableStore()
        store.set("data", list(range(20)))
        result = store.slice("data", 0, 5)
        assert result == [0, 1, 2, 3, 4]
        assert store.has("data_slice")

    def test_dict_slice(self):
        store = VariableStore()
        store.set("d", {f"key{i}": i for i in range(10)})
        result = store.slice("d", 0, 3)
        assert len(result) == 3

    def test_string_slice(self):
        store = VariableStore()
        store.set("s", "hello world")
        result = store.slice("s", 0, 5)
        assert result == "hello"

    def test_slice_unsupported_type(self):
        store = VariableStore()
        store.set("x", 42)
        with pytest.raises(TypeError, match="Cannot slice"):
            store.slice("x", 0, 5)


# ── Filter Tests ───────────────────────────────────────────────


class TestFilter:
    def test_list_filter(self):
        store = VariableStore()
        store.set("nums", [1, 2, 3, 4, 5, 6])
        result = store.filter("nums", lambda x: x % 2 == 0)
        assert result == [2, 4, 6]
        assert store.has("nums_filtered")

    def test_dict_filter(self):
        store = VariableStore()
        store.set("scores", {"alice": 90, "bob": 60, "carol": 85})
        result = store.filter("scores", lambda v: v >= 80)
        assert dict(result) == {"alice": 90, "carol": 85}

    def test_filter_unsupported_type(self):
        store = VariableStore()
        store.set("x", 42)
        with pytest.raises(TypeError, match="Cannot filter"):
            store.filter("x", lambda x: True)


# ── Stats Tests ────────────────────────────────────────────────


class TestStats:
    def test_list_stats(self):
        store = VariableStore()
        store.set("nums", [10, 20, 30, 40, 50])
        stats = store.stats("nums")
        assert stats["length"] == 5
        assert stats["sum"] == 150
        assert stats["avg"] == 30.0
        assert stats["min"] == 10
        assert stats["max"] == 50

    def test_dict_stats(self):
        store = VariableStore()
        store.set("d", {"a": 1, "b": 2, "c": 3})
        stats = store.stats("d")
        assert stats["keys"] == 3

    def test_string_stats(self):
        store = VariableStore()
        store.set("s", "line1\nline2\nline3")
        stats = store.stats("s")
        assert stats["chars"] == 17
        assert stats["lines"] == 3


# ── Compact Observation Tests ──────────────────────────────────


class TestCompactObservation:
    def test_small_value_in_budget(self):
        store = VariableStore()
        store.set("x", [1, 2, 3])
        result = store.compact("x", max_tokens=1000)
        assert "=== x" in result
        assert "1" in result

    def test_large_list_compressed(self):
        store = VariableStore()
        store.set("big", list(range(10000)))
        result = store.compact("big", max_tokens=100)
        assert "compressed" in result
        assert "10000 items" in result
        # 验证压缩后 token 数在预算内
        tokens = len(result) // 4
        assert tokens <= 150  # 有少量开销

    def test_large_dict_compressed(self):
        store = VariableStore()
        store.set("big_dict", {f"key_{i}": f"value_{i}" * 10 for i in range(100)})
        result = store.compact("big_dict", max_tokens=100)
        assert "compressed" in result

    def test_large_text_compressed(self):
        store = VariableStore()
        store.set("big_text", "\n".join(f"line {i}: {'x' * 100}" for i in range(500)))
        result = store.compact("big_text", max_tokens=100)
        assert "compressed" in result
        assert "500 lines" in result

    def test_empty_sequence(self):
        store = VariableStore()
        store.set("empty", [])
        result = store.compact("empty")
        assert "empty" in result


# ── Token Estimation Tests ─────────────────────────────────────


class TestTokenEstimation:
    def test_estimate_tokens_int(self):
        assert _estimate_tokens(42) >= 1

    def test_estimate_tokens_string(self):
        assert _estimate_tokens("hello") >= 1

    def test_estimate_tokens_list(self):
        assert _estimate_tokens([1, 2, 3]) >= 1

    def test_estimate_tokens_dict(self):
        assert _estimate_tokens({"a": 1, "b": 2}) >= 1


# ── RLMKernel Integration Tests ────────────────────────────────


class TestRLMKernel:
    def test_put_and_fetch(self):
        kernel = RLMKernel()
        info = kernel.put("x", [1, 2, 3])
        assert info["name"] == "x"
        assert info["type"] == "list"
        assert kernel.fetch("x") == [1, 2, 3]

    def test_observe(self):
        kernel = RLMKernel()
        kernel.put("data", list(range(100)))
        result = kernel.observe("data", max_tokens=50)
        assert "=== data" in result

    def test_summary(self):
        kernel = RLMKernel()
        kernel.put("a", 1)
        kernel.put("b", "hello")
        s = kernel.summary()
        assert s["variable_count"] == 2
        assert s["total_tokens"] > 0

    def test_execute_with_context(self):
        kernel = RLMKernel()
        kernel.put("x", 10)
        kernel.put("y", 20)
        result = kernel.execute_with_context("x + y")
        assert result["output"] == 30
        assert result["error"] is None

    def test_execute_with_new_vars(self):
        kernel = RLMKernel()
        result = kernel.execute_with_context("z = 42")
        assert "z" in result["new_vars"]
        assert kernel.fetch("z") == 42

    def test_execute_with_error(self):
        kernel = RLMKernel()
        result = kernel.execute_with_context("1 / 0")
        assert result["error"] is not None
        assert "ZeroDivisionError" in result["error"]

    def test_token_savings(self):
        kernel = RLMKernel()
        # 放入大对象
        kernel.put("big", list(range(10000)))
        kernel.put("text", "x" * 10000)
        savings = kernel.token_savings()
        assert savings["raw_tokens"] > 0
        assert savings["saved_tokens"] > 0
        # 压缩率应该 >= 70%
        ratio_str = savings["compression_ratio"].rstrip("%")
        ratio = float(ratio_str) / 100
        assert ratio >= 0.7, f"Compression ratio {ratio:.1%} < 70% target"

    def test_sandbox_locals_sync(self):
        kernel = RLMKernel()
        kernel.put("x", 42)
        assert "x" in kernel.sandbox_locals
        assert kernel.sandbox_locals["x"] == 42


# ── Singleton Tests ────────────────────────────────────────────


class TestSingleton:
    def test_get_kernel_returns_same(self):
        reset_kernel()
        k1 = get_kernel()
        k2 = get_kernel()
        assert k1 is k2

    def test_reset_kernel(self):
        reset_kernel()
        k1 = get_kernel()
        reset_kernel()
        k2 = get_kernel()
        assert k1 is not k2


# ── Stress Tests ───────────────────────────────────────────────


class TestStress:
    def test_many_variables(self):
        """压力测试: 1000 个变量."""
        kernel = RLMKernel(token_budget=8192)
        for i in range(1000):
            kernel.put(f"var_{i}", list(range(100)))
        assert kernel.summary()["variable_count"] == 1000
        # 所有变量可检索
        for i in range(0, 1000, 100):
            assert kernel.fetch(f"var_{i}") == list(range(100))

    def test_large_object_compact(self):
        """压力测试: 100KB 对象紧凑观测."""
        kernel = RLMKernel(token_budget=256)
        big_data = {"records": [{"id": i, "data": "x" * 200} for i in range(500)]}
        kernel.put("big", big_data)
        result = kernel.observe("big")
        # 结果必须在 256 token 预算附近
        tokens = len(result) // 4
        assert tokens < 500  # 有少量开销

    def test_rapid_put_get_cycle(self):
        """压力测试: 快速存取循环."""
        kernel = RLMKernel()
        for i in range(500):
            kernel.put(f"k{i}", i)
        for i in range(500):
            assert kernel.fetch(f"k{i}") == i

    def test_slice_performance(self):
        """压力测试: 大列表切片."""
        store = VariableStore()
        store.set("big", list(range(100000)))
        result = store.slice("big", 0, 10)
        assert result == list(range(10))
        result = store.slice("big", 99990, 100000)
        assert result == list(range(99990, 100000))
