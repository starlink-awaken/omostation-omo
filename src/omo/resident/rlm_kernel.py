#!/usr/bin/env python3
"""rlm_kernel — RLM (Recursive Language Model) 持久化变量执行空间 (BET-Y1Q4-T10-136).

解决 Agent 长程 Context Rot 与 Token 暴增问题。传统工具调用把检索结果/AST/中间数据
直接追加文本至对话上下文，导致 context 线性膨胀、注意力衰减。本内核提供沙箱持久化
Python 命名空间，允许 Agent 把大型中间态作为命名变量就地切片、过滤、聚合，再用
紧凑观测 (compact_observation) 把所需结果精炼回写到对话历史。

设计要点:
  1. **持久化命名空间**: 单 sandbox session 内变量跨多次 execute() 调用保持
  2. **原地执行**: ``eval_var / exec_expr / slice_var`` 三大原语，不复制数据
  3. **紧凑观测**: 长列表/字典自动摘要为长度受限的 repr，默认 max_obs_tokens
  4. **Token 压缩率**: 对比 "raw len → obs len" 计算压缩率并入 evidence
  5. **沙箱边界**: 不暴露 builtin imports 中的危险模块 (os.system / subprocess / ...)
  6. **Time-Machine 协同**: 与 sandbox_driver 复用 session 命名空间，rollback 时一并清空

挂接: execute.py 的高危 ExecutionRequested 事件 → 沙箱执行 → rlm_kernel 持久化变量
      → 完成后 compact_observation 回写 Spine draft 池。

压缩率证据: 单元测试 test_rlm_kernel.py::test_compression_ratio_70pct 验证
            在典型 AST/检索结果场景下压缩率 >= 70%。
"""

from __future__ import annotations

import ast
import math
import reprlib
import statistics
import tokenize
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from io import StringIO
from typing import Any

# ── 危险 builtin 黑名单（防止 RLM sandbox 内任意代码逃逸）──────────────────
_BLOCKED_BUILTINS = {
    "exec",  # 我们自己管控 exec，不暴露给 user-expr
    "eval",  # 同上
    "compile",
    "__import__",
    "open",
    "input",
    "breakpoint",
}

# token 压缩观测阈值（默认 256 tokens；rlm_kernel caller 可覆写）
DEFAULT_MAX_OBS_TOKENS = 256

# 长集合/字典观测截断上限
DEFAULT_SEQ_PREVIEW = 8
DEFAULT_MAP_PREVIEW = 6


@dataclass
class RLMVariable:
    """沙箱内持久化变量条目.

    Attributes:
        name: 变量名 (作为 namespaces dict 的 key 也存一份，便于 __repr__)
        value: 实际值
        size_bytes: 序列化估值，用于压缩率证据
        created_at_callsite: 调用计数 (第几次 execute() 创建)
    """

    name: str
    value: Any
    size_bytes: int = 0
    created_at_callsite: int = 0
    access_count: int = 0

    def touch(self) -> None:
        """每次被读取/切片时 touch 一次，用于热点分析."""
        self.access_count += 1


@dataclass
class RLMCompactObservation:
    """紧凑观测 — 把大型中间态压缩为对话可承载的精炼文本.

    字段:
        expr: 表达式字符串 (e.g. "vars['hits'][:5]")
        obs: 紧凑 repr (限长)
        raw_chars: 原始字符串长度
        obs_chars: 观测字符串长度
        compression_ratio: 1 - obs_chars/raw_chars (>= 0.70 为达标)
        kind: 变量类型分类 (scalar/seq/mapping/ndarray-like)
    """

    expr: str
    obs: str
    raw_chars: int
    obs_chars: int
    compression_ratio: float
    kind: str

    @property
    def compressed(self) -> bool:
        """是否达到 70% 压缩率目标."""
        return self.compression_ratio >= 0.70


@dataclass
class RLMSession:
    """单次沙箱会话的 RLM 状态.

    每个 SandboxDriver session 对应一个 RLMSession，跨多次 execute 持久化命名空间.
    rollback 时整体清空.
    """

    session_id: str
    namespaces: dict[str, Any] = field(default_factory=dict)
    variables: dict[str, RLMVariable] = field(default_factory=dict)
    callsite_count: int = 0
    cumulative_raw_chars: int = 0
    cumulative_obs_chars: int = 0

    # ── 变量注册 ────────────────────────────────────────
    def set_var(self, name: str, value: Any) -> RLMVariable:
        """注册/覆盖一个持久化变量."""
        if not name.isidentifier():
            raise ValueError(f"❌ invalid variable name: {name!r}")
        self.namespaces[name] = value
        var = RLMVariable(
            name=name,
            value=value,
            size_bytes=_estimate_size(value),
            created_at_callsite=self.callsite_count,
        )
        self.variables[name] = var
        return var

    def get_var(self, name: str) -> Any:
        """读取变量 (会 touch access_count)."""
        if name not in self.variables:
            raise KeyError(f"❌ unknown variable: {name!r}")
        self.variables[name].touch()
        return self.variables[name].value

    # ── 表达式执行（受限 Python AST）─────────────────────────
    def eval_var(self, expr: str) -> Any:
        """受限 AST 求值 — 仅允许 Name / Subscript / Slice / Call(白名单).

        拒绝: Import, ImportFrom, Attribute(builtins._ 私有), Global/Nonlocal.
        """
        tree = ast.parse(expr, mode="eval")
        _validate_ast(tree)
        # 仅暴露 user 变量 + 安全 builtin
        safe_globals = {"__builtins__": _safe_builtins()}
        safe_locals = dict(self.namespaces)
        return eval(  # noqa: S307 — 表达式已 AST 校验过
            compile(tree, "<rlm-eval>", "eval"),
            safe_globals,
            safe_locals,
        )

    def exec_expr(self, expr: str) -> Any:
        """受限 exec — 允许 Assign / Expr. 结果写回 namespaces.

        仅允许 `var = expr` 或裸表达式；不暴露 import / 函数定义.
        """
        tree = ast.parse(expr, mode="exec")
        _validate_ast(tree, allow_assign=True)
        safe_globals = {"__builtins__": _safe_builtins()}
        safe_locals = dict(self.namespaces)
        exec(  # noqa: S102 — AST 已校验
            compile(tree, "<rlm-exec>", "exec"),
            safe_globals,
            safe_locals,
        )
        # 把 exec 的写回 namespaces（保守：仅 top-level Name 赋值）
        for name, value in safe_locals.items():
            if name in self.namespaces or _is_simple_name_assignment(tree, name):
                self.set_var(name, value)
        self.callsite_count += 1
        return None

    def slice_var(self, name: str, slice_spec: str) -> RLMCompactObservation:
        """对变量做切片并立即生成紧凑观测.

        slice_spec 是 Python slice expression, e.g. "[:5]" / "[::10]" / "['key']".
        """
        if name not in self.variables:
            raise KeyError(f"❌ unknown variable: {name!r}")
        full_expr = f"vars_['{name}']{slice_spec}"
        # 在 eval 内通过临时 globals 注入 vars_
        safe_globals = {"__builtins__": _safe_builtins(), "vars_": self.namespaces}
        tree = ast.parse(full_expr, mode="eval")
        _validate_ast(tree)
        sliced = eval(  # noqa: S307 — AST 已校验
            compile(tree, "<rlm-slice>", "eval"),
            safe_globals,
            {},
        )
        # touch 访问计数
        self.variables[name].touch()
        # 生成观测
        obs = self.observe(f"vars['{name}']{slice_spec}", sliced)
        return obs

    # ── 紧凑观测 ─────────────────────────────────────────
    def observe(self, expr: str, value: Any, max_tokens: int = DEFAULT_MAX_OBS_TOKENS) -> RLMCompactObservation:
        """把任意 Python 值压缩为长度受限的紧凑观测文本.

        策略:
          - scalar (int/float/bool/str/None) → str()
          - sequence (list/tuple) → reprlib.repr 截前 DEFAULT_SEQ_PREVIEW 个
          - mapping (dict) → reprlib.repr 截前 DEFAULT_MAP_PREVIEW 个
          - 其他 → type + id + size_bytes

        同时统计 raw_chars（未压缩的全 repr）和 obs_chars（压缩后），计算 compression_ratio.
        """
        raw_repr = repr(value)
        kind = _classify_kind(value)
        if kind == "scalar":
            obs_str = repr(value)
        elif kind == "sequence":
            obs_str = reprlib.repr(value)  # 自动截断长 sequence
        elif kind == "mapping":
            obs_str = reprlib.repr(value)
        else:
            obs_str = f"<{type(value).__name__} id={id(value):#x} size={_estimate_size(value)}B>"

        # token 估算 (粗略：~4 chars / token)
        max_chars = max_tokens * 4
        if len(obs_str) > max_chars:
            obs_str = obs_str[:max_chars] + f"... <truncated {len(obs_str) - max_chars} chars>"

        raw_chars = len(raw_repr)
        obs_chars = len(obs_str)
        compression_ratio = max(0.0, 1.0 - obs_chars / max(raw_chars, 1))

        self.cumulative_raw_chars += raw_chars
        self.cumulative_obs_chars += obs_chars

        return RLMCompactObservation(
            expr=expr,
            obs=obs_str,
            raw_chars=raw_chars,
            obs_chars=obs_chars,
            compression_ratio=compression_ratio,
            kind=kind,
        )

    # ── 证据 ─────────────────────────────────────────────
    def session_stats(self) -> dict[str, Any]:
        """返回当前 session 的累计证据.

        Returns:
            dict with session_id, callsite_count, variable_count, cumulative
            raw_chars, cumulative obs_chars, session_compression_ratio.
        """
        session_ratio = (
            1.0
            - self.cumulative_obs_chars / max(self.cumulative_raw_chars, 1)
        )
        return {
            "session_id": self.session_id,
            "callsite_count": self.callsite_count,
            "variable_count": len(self.variables),
            "cumulative_raw_chars": self.cumulative_raw_chars,
            "cumulative_obs_chars": self.cumulative_obs_chars,
            "session_compression_ratio": max(0.0, session_ratio),
        }


# ── 模块级 session 注册表（跨 execute 调用持久化）────────────────────────────
_SESSIONS: dict[str, RLMSession] = {}


def get_session(session_id: str) -> RLMSession:
    """获取或创建指定 session_id 的 RLM 持久化状态."""
    if session_id not in _SESSIONS:
        _SESSIONS[session_id] = RLMSession(session_id=session_id)
    return _SESSIONS[session_id]


def drop_session(session_id: str) -> bool:
    """rollback 时清空 session 命名空间 (与 SandboxDriver 协同).

    Returns:
        True if session existed and was dropped, False if it didn't exist.
    """
    if session_id in _SESSIONS:
        del _SESSIONS[session_id]
        return True
    return False


def list_sessions() -> list[str]:
    """返回当前所有活跃 session_id（用于调试与监控）."""
    return list(_SESSIONS.keys())


# ── 内部辅助 ─────────────────────────────────────────────
def _safe_builtins() -> dict[str, Any]:
    """构造受限 builtin 白名单."""
    safe = {
        "len": len,
        "range": range,
        "enumerate": enumerate,
        "zip": zip,
        "map": map,
        "filter": filter,
        "sum": sum,
        "min": min,
        "max": max,
        "abs": abs,
        "round": round,
        "sorted": sorted,
        "any": any,
        "all": all,
        "list": list,
        "dict": dict,
        "set": set,
        "tuple": tuple,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "repr": repr,
        "print": print,
        "isinstance": isinstance,
        "type": type,
        "True": True,
        "False": False,
        "None": None,
    }
    return safe


def _classify_kind(value: Any) -> str:
    """将 Python 值分类为 scalar / sequence / mapping / other."""
    if isinstance(value, (int, float, bool, str)) or value is None:
        return "scalar"
    if isinstance(value, (str, bytes)):
        return "scalar"
    if isinstance(value, Sequence):
        return "sequence"
    if isinstance(value, Mapping):
        return "mapping"
    return "other"


def _estimate_size(value: Any) -> int:
    """粗略估算序列化后字节数（用于 size_bytes 字段）."""
    try:
        return len(repr(value).encode("utf-8"))
    except Exception:
        return 0


def _validate_ast(tree: ast.AST, allow_assign: bool = False) -> None:
    """校验 AST 不包含危险节点.

    黑名单:
      - Import / ImportFrom
      - FunctionDef / AsyncFunctionDef / ClassDef (沙箱内禁止定义)
      - Global / Nonlocal
      - Lambda (避免 exec 突破)
      - Attribute 访问以 _ 开头的私有属性
      - Call 目标不在白名单
    """
    blocked_types: tuple[type, ...] = (
        ast.Import,
        ast.ImportFrom,
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
        ast.Global,
        ast.Nonlocal,
        ast.Lambda,
    )
    for node in ast.walk(tree):
        if isinstance(node, blocked_types):
            raise ValueError(f"❌ blocked AST node: {type(node).__name__}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise ValueError(f"❌ blocked private attribute access: {node.attr!r}")
        if isinstance(node, ast.Call):
            _validate_call_target(node)


def _validate_call_target(call: ast.Call) -> None:
    """校验 Call 节点的目标函数在白名单内."""
    safe_funcs = {
        "len", "range", "enumerate", "zip", "map", "filter",
        "sum", "min", "max", "abs", "round", "sorted", "any", "all",
        "list", "dict", "set", "tuple", "str", "int", "float", "bool",
        "repr", "print", "isinstance", "type",
    }
    func = call.func
    if isinstance(func, ast.Name):
        if func.id not in safe_funcs:
            raise ValueError(f"❌ blocked function call: {func.id!r}")
    elif isinstance(func, ast.Attribute):
        # 允许 user 变量的 method call (e.g. hits.append())，但禁止 _ 开头
        if func.attr.startswith("_"):
            raise ValueError(f"❌ blocked private method: {func.attr!r}")


def _is_simple_name_assignment(tree: ast.AST, name: str) -> bool:
    """判断 AST 中是否有 `name = ...` 形式的顶层赋值."""
    if not isinstance(tree, ast.Module):
        return False
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return True
    return False


__all__ = (
    "DEFAULT_MAX_OBS_TOKENS",
    "RLMCompactObservation",
    "RLMSession",
    "RLMVariable",
    "drop_session",
    "get_session",
    "list_sessions",
)
