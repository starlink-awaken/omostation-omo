"""test_rlm_kernel.py — RLM 持久化变量内核测试 (BET-Y1Q4-T10-136).

覆盖:
  1. 持久化命名空间（跨多次 execute 调用）
  2. 受限 AST 求值（拒绝 import / 函数定义 / 私有属性）
  3. 切片 + 紧凑观测
  4. **压缩率 >= 70%**（done_when 关键验收指标）
  5. session 隔离（不同 session_id 独立命名空间）
  6. rollback 时清空（drop_session）
  7. 危险 builtin 黑名单（os.system / __import__ 等）
"""

from __future__ import annotations

import pytest

from omo.resident.rlm_kernel import (
    DEFAULT_MAX_OBS_TOKENS,
    RLMCompactObservation,
    RLMSession,
    drop_session,
    get_session,
    list_sessions,
)


@pytest.fixture(autouse=True)
def _clean_sessions():
    """每个测试前后清空 session 注册表，确保隔离."""
    yield
    for sid in list(list_sessions()):
        drop_session(sid)


# ── 1. 持久化命名空间 ──────────────────────────────────
class TestPersistence:
    def test_set_and_get_var(self):
        s = get_session("s1")
        s.set_var("x", 42)
        assert s.get_var("x") == 42
        assert s.variables["x"].access_count == 1  # get 已 touch

    def test_set_invalid_name_rejected(self):
        s = get_session("s1")
        with pytest.raises(ValueError, match="invalid variable name"):
            s.set_var("1bad-name", 1)

    def test_get_unknown_raises(self):
        s = get_session("s1")
        with pytest.raises(KeyError, match="unknown variable"):
            s.get_var("missing")

    def test_exec_writes_back_to_namespace(self):
        """exec('y = 100') 应当把 y 注册到 namespaces."""
        s = get_session("s1")
        s.exec_expr("y = 100")
        assert s.get_var("y") == 100

    def test_persistence_across_callsites(self):
        """变量跨多次 execute 调用保持."""
        s = get_session("s1")
        s.set_var("counter", 0)
        for i in range(5):
            s.exec_expr("counter = counter + 1")
        assert s.get_var("counter") == 5


# ── 2. 受限 AST 求值 ─────────────────────────────────────
class TestSafeAST:
    def test_eval_simple_subscript(self):
        s = get_session("s1")
        s.set_var("data", {"a": 1, "b": 2})
        assert s.eval_var("data['a']") == 1

    def test_eval_rejects_import(self):
        s = get_session("s1")
        # __import__ is caught as a disallowed function call (not via Import AST node)
        with pytest.raises(ValueError, match="blocked function call"):
            s.eval_var("__import__('os')")

    def test_eval_rejects_function_def(self):
        s = get_session("s1")
        with pytest.raises(ValueError, match="blocked AST node"):
            s.exec_expr("def evil(): pass")

    def test_eval_rejects_private_attr(self):
        s = get_session("s1")
        s.set_var("d", {"_private": 1})
        with pytest.raises(ValueError, match="blocked private attribute"):
            s.eval_var("d._private")

    def test_eval_rejects_lambda(self):
        s = get_session("s1")
        with pytest.raises(ValueError, match="blocked AST node"):
            s.eval_var("(lambda x: x)(1)")

    def test_eval_rejects_disallowed_call(self):
        s = get_session("s1")
        with pytest.raises(ValueError, match="blocked function call"):
            s.eval_var("open('/etc/passwd')")


# ── 3. 切片 + 紧凑观测 ──────────────────────────────────
class TestSliceAndObserve:
    def test_slice_list(self):
        s = get_session("s1")
        s.set_var("hits", list(range(100)))
        obs = s.slice_var("hits", "[:5]")
        assert isinstance(obs, RLMCompactObservation)
        assert obs.kind == "sequence"
        assert obs.expr == "vars['hits'][:5]"
        assert "0" in obs.obs and "4" in obs.obs

    def test_slice_dict_by_key(self):
        s = get_session("s1")
        s.set_var("meta", {"policy": "卫生健康", "year": 2026})
        obs = s.slice_var("meta", "['policy']")
        assert obs.kind == "scalar"
        assert "卫生健康" in obs.obs

    def test_observe_long_string_truncated(self):
        s = get_session("s1")
        huge = "A" * 10000
        obs = s.observe("big", huge, max_tokens=64)
        # 256 chars cap (64 tokens × 4 chars/token) → 截断
        assert obs.obs_chars <= 64 * 4 + 50  # 容差 (含 truncation 标记)
        assert "<truncated" in obs.obs


# ── 4. 压缩率 >= 70% — done_when 关键验收 ────────────────────
class TestCompressionRatio:
    def test_compression_ratio_long_list(self):
        """典型检索结果 list of dict (100 项) 压缩率应 >= 70%."""
        s = get_session("s1")
        # 模拟 KEMS-v2 检索结果
        hits = [
            {"id": i, "title": f"政策文档 {i}", "score": 0.99 - i * 0.01}
            for i in range(100)
        ]
        s.set_var("hits", hits)
        obs = s.observe("vars['hits']", hits)
        # 100 项 dict 列表用 reprlib 截前 8 项 + 大量 ...
        assert obs.compression_ratio >= 0.70, (
            f"❌ compression_ratio={obs.compression_ratio:.3f} < 0.70 "
            f"(raw_chars={obs.raw_chars}, obs_chars={obs.obs_chars})"
        )

    def test_compression_ratio_huge_string(self):
        s = get_session("s1")
        # 模拟 AST dump 或长文档
        ast_dump = "{'node': 'Call', 'args': [" + ", ".join(["'arg'"] * 500) + "]}"
        s.set_var("ast_dump", ast_dump)
        obs = s.observe("vars['ast_dump']", ast_dump, max_tokens=64)
        assert obs.compression_ratio >= 0.70

    def test_compression_ratio_deep_nested(self):
        s = get_session("s1")
        nested = {"level1": {"level2": {"level3": {"data": list(range(1000))}}}}
        s.set_var("deep", nested)
        obs = s.observe("vars['deep']", nested)
        # deep mapping 用 reprlib 应至少 70% 压缩
        assert obs.compression_ratio >= 0.70

    def test_session_cumulative_compression(self):
        """多次 observe 累计压缩率仍应达标."""
        s = get_session("s1")
        for i in range(10):
            data = [{"k": j, "v": "x" * 100} for j in range(50)]
            s.observe(f"batch_{i}", data)
        stats = s.session_stats()
        assert stats["session_compression_ratio"] >= 0.70


# ── 5. session 隔离 ─────────────────────────────────────
class TestSessionIsolation:
    def test_separate_sessions_have_separate_namespaces(self):
        s1 = get_session("alpha")
        s2 = get_session("beta")
        s1.set_var("x", 100)
        s2.set_var("x", 200)
        assert s1.get_var("x") == 100
        assert s2.get_var("x") == 200

    def test_get_session_returns_same_instance(self):
        a = get_session("zeta")
        b = get_session("zeta")
        assert a is b


# ── 6. rollback 协同 ───────────────────────────────────
class TestRollback:
    def test_drop_session_removes_namespace(self):
        s = get_session("to_drop")
        s.set_var("data", [1, 2, 3])
        assert drop_session("to_drop") is True
        # 重新 get_session 应当返回全新实例
        s2 = get_session("to_drop")
        assert "data" not in s2.namespaces

    def test_drop_nonexistent_returns_false(self):
        assert drop_session("never_existed") is False


# ── 7. 危险 builtin 黑名单 ──────────────────────────────
class TestBlockedBuiltins:
    def test_eval_blocks_dunder_import(self):
        s = get_session("s1")
        with pytest.raises(ValueError):
            s.eval_var("__import__('os').system('echo PWNED')")

    def test_exec_blocks_import_statement(self):
        s = get_session("s1")
        with pytest.raises(ValueError):
            s.exec_expr("import os")

    def test_exec_blocks_from_import(self):
        s = get_session("s1")
        with pytest.raises(ValueError):
            s.exec_expr("from os import system")


# ── 8. session_stats 证据 ───────────────────────────────
class TestSessionStats:
    def test_initial_stats(self):
        s = get_session("s1")
        stats = s.session_stats()
        assert stats["session_id"] == "s1"
        assert stats["variable_count"] == 0
        assert stats["callsite_count"] == 0
        assert stats["cumulative_raw_chars"] == 0

    def test_stats_after_work(self):
        s = get_session("s1")
        s.set_var("hits", list(range(1000)))
        s.observe("hits", s.get_var("hits"))
        stats = s.session_stats()
        assert stats["variable_count"] == 1
        assert stats["cumulative_raw_chars"] > 0
        assert stats["cumulative_obs_chars"] > 0
